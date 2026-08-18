# Desplegar AML GOLD en otro entorno

Este procedimiento permite desplegar el snapshot en un workspace Fabric propio usando
un fork de GitHub y autenticacion OIDC. No reutiliza identidades, capacidades ni IDs del
entorno original. El workspace, el Lakehouse `GOLD` y sus tablas permanecen disponibles
al terminar; solo se cierra la sesion temporal de Spark.

## Resultado

El workflow:

1. descarga aproximadamente 1.10 GB desde Git LFS a un runner de GitHub;
2. crea o reutiliza el Lakehouse `GOLD`;
3. carga el snapshot Parquet/Zstandard en OneLake;
4. materializa ocho tablas Delta;
5. valida conteos, fechas y relaciones.

Como referencia, en una capacidad F64 el despliegue medido tardo unos 2 minutos y
46 segundos. La descarga Git LFS cloud-to-cloud tardo entre 6 y 12 segundos.

## Requisitos

- Acceso a una capacidad Microsoft Fabric activa.
- Permiso para crear un workspace en esa capacidad.
- Rol Admin en el workspace de destino.
- Permiso para crear una aplicacion de Microsoft Entra o ayuda de un administrador.
- GitHub CLI (`gh`) y Azure CLI (`az`) autenticados para la configuracion inicial.
- Git LFS solo es necesario si se clona el snapshot localmente; GitHub Actions lo
  instala en el runner.

El administrador de Fabric debe permitir que el grupo o service principal usado pueda
llamar a las API publicas de Fabric. Si la politica del tenant restringe los service
principals, debe incluirse la identidad en el grupo autorizado.

## 1. Crear un fork

Desde GitHub, crear un fork de:

```text
https://github.com/paakos/AntiMoneyLaundryDataset
```

En los ejemplos siguientes se usan estas variables. Sustituir sus valores:

```powershell
$GitHubOwner = "<USUARIO_U_ORGANIZACION>"
$GitHubRepo = "AntiMoneyLaundryDataset"
$TenantId = "<TENANT_ID>"
$SubscriptionId = "<SUBSCRIPTION_ID>"
$CapacityId = "<FABRIC_CAPACITY_ID>"
$WorkspaceName = "amldemo-target"
```

El ID Fabric de la capacidad se obtiene con:

```powershell
az login --tenant $TenantId
az rest --method get `
  --resource "https://api.fabric.microsoft.com" `
  --url "https://api.fabric.microsoft.com/v1/capacities" `
  --query "value[].{id:id,name:displayName,state:state,sku:sku}" `
  --output table
```

## 2. Crear el workspace persistente

Se recomienda que el propietario del entorno cree el workspace con su identidad y
conceda al workflow acceso solo a ese workspace. Esto evita dar permisos de
aprovisionamiento sobre toda la capacidad.

```powershell
$FabricResource = "https://api.fabric.microsoft.com"
$Token = az account get-access-token `
  --resource $FabricResource `
  --query accessToken `
  --output tsv
$Headers = @{ Authorization = "Bearer $Token" }

$WorkspaceBody = @{
  displayName = $WorkspaceName
  description = "AML GOLD deployed from GitHub"
  capacityId = $CapacityId
} | ConvertTo-Json -Compress

$Workspace = Invoke-RestMethod `
  -Method Post `
  -Uri "$FabricResource/v1/workspaces" `
  -Headers $Headers `
  -ContentType "application/json" `
  -Body $WorkspaceBody

$WorkspaceId = $Workspace.id
$WorkspaceId
```

Si el workspace ya existe, obtener su ID en lugar de crearlo:

```powershell
$Response = Invoke-RestMethod `
  -Method Get `
  -Uri "$FabricResource/v1/workspaces" `
  -Headers $Headers
$WorkspaceId = @($Response.value) |
  Where-Object { $_.displayName -ieq $WorkspaceName } |
  Select-Object -ExpandProperty id -First 1
```

## 3. Crear la identidad OIDC

Crear una aplicacion y su service principal, sin secretos ni contrasenas:

```powershell
$App = az ad app create `
  --display-name "github-$GitHubOwner-$GitHubRepo" |
  ConvertFrom-Json
$ServicePrincipal = az ad sp create --id $App.appId | ConvertFrom-Json

$ClientId = $App.appId
$ServicePrincipalObjectId = $ServicePrincipal.id
```

Obtener los IDs inmutables que GitHub incluye en el token OIDC:

```powershell
$Repository = gh api "repos/$GitHubOwner/$GitHubRepo" | ConvertFrom-Json
$OwnerId = $Repository.owner.id
$RepositoryId = $Repository.id
$OidcSubject = "repo:$GitHubOwner@$OwnerId/$GitHubRepo@$RepositoryId`:ref:refs/heads/main"
```

Crear la credencial federada:

```powershell
$GraphToken = az account get-access-token `
  --resource-type ms-graph `
  --query accessToken `
  --output tsv

$FederatedBody = @{
  name = "github-main"
  issuer = "https://token.actions.githubusercontent.com"
  subject = $OidcSubject
  description = "GitHub Actions main branch deployment"
  audiences = @("api://AzureADTokenExchange")
} | ConvertTo-Json -Depth 5

Invoke-RestMethod `
  -Method Post `
  -Uri "https://graph.microsoft.com/v1.0/applications/$($App.id)/federatedIdentityCredentials" `
  -Headers @{ Authorization = "Bearer $GraphToken" } `
  -ContentType "application/json" `
  -Body $FederatedBody
```

El formato con IDs inmutables es importante cuando la organizacion GitHub tiene
habilitada esa proteccion. Si Azure devuelve `AADSTS700213`, revisar en el log de
`azure/login` el `subject claim` presentado y usarlo exactamente.

## 4. Conceder acceso al workspace

Con la identidad del propietario del entorno, agregar el service principal como
`Contributor` solamente en el workspace creado:

```powershell
$RoleBody = @{
  principal = @{
    id = $ServicePrincipalObjectId
    type = "ServicePrincipal"
  }
  role = "Contributor"
} | ConvertTo-Json -Depth 5 -Compress

Invoke-RestMethod `
  -Method Post `
  -Uri "$FabricResource/v1/workspaces/$WorkspaceId/roleAssignments" `
  -Headers $Headers `
  -ContentType "application/json" `
  -Body $RoleBody
```

No basta con asignar Azure RBAC `Contributor` al recurso ARM de la capacidad. Azure
RBAC, permisos de capacidad Fabric y roles del workspace son planos distintos. Al usar
un workspace precreado, el workflow solo necesita el rol del workspace.

## 5. Configurar el fork

Configurar los secretos de GitHub Actions:

```powershell
$RepositoryName = "$GitHubOwner/$GitHubRepo"

gh secret set AZURE_CLIENT_ID `
  --repo $RepositoryName `
  --body $ClientId
gh secret set AZURE_TENANT_ID `
  --repo $RepositoryName `
  --body $TenantId
gh secret set AZURE_SUBSCRIPTION_ID `
  --repo $RepositoryName `
  --body $SubscriptionId
```

Configurar las variables:

```powershell
gh variable set FABRIC_WORKSPACE_NAME `
  --repo $RepositoryName `
  --body $WorkspaceName
gh variable set FABRIC_CAPACITY_ID `
  --repo $RepositoryName `
  --body $CapacityId
```

Comprobar solamente los nombres, sin mostrar valores secretos:

```powershell
gh secret list --repo $RepositoryName
gh variable list --repo $RepositoryName
```

## 6. Ejecutar el despliegue

Desde GitHub:

1. Abrir **Actions**.
2. Seleccionar **Deploy AML Gold to Fabric**.
3. Pulsar **Run workflow**.
4. Indicar el workspace precreado o aceptar la variable configurada.

Tambien puede lanzarse desde la terminal:

```powershell
gh workflow run deploy-fabric.yml `
  --repo $RepositoryName `
  --ref main `
  -f workspace_name=$WorkspaceName
```

Consultar el progreso:

```powershell
gh run list --repo $RepositoryName --workflow deploy-fabric.yml --limit 5
```

## 7. Validar el resultado

La ejecucion debe terminar con `DEPLOY_RESULT_JSON` y estos valores:

- `status`: `ok`;
- ocho tablas desplegadas;
- `dateKeyMismatches`: `0`;
- `missingDates`: `0`;
- `orphanPatterns`: `0`.

Conteos esperados:

| Tabla | Filas |
|---|---:|
| `dim_account` | 2,087,762 |
| `dim_bank` | 122,333 |
| `dim_currency` | 15 |
| `dim_date` | 56 |
| `dim_laundering_pattern` | 8 |
| `dim_payment_format` | 7 |
| `fact_laundering_pattern_txn` | 22,743 |
| `fact_transaction` | 31,898,238 |

## Reejecuciones y conservacion

El proceso es idempotente: reutiliza el workspace y `GOLD`, vuelve a cargar el snapshot
y sobrescribe las tablas Delta. No contiene pasos para eliminar el workspace,
Lakehouse, tablas o archivos. Solo elimina la sesion Livy temporal al finalizar.

Los secretos OIDC del repositorio original no se copian al fork. Cada entorno debe
crear y configurar su propia aplicacion Entra y sus propios secretos de GitHub.