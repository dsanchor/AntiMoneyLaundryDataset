# AntiMoneyLaundryDataset

Repositorio reproducible para aprovisionar un workspace Microsoft Fabric y restaurar el
Lakehouse `GOLD` con las ocho tablas Delta del modelo AML.

## Arquitectura de despliegue

Fabric Git Integration sincroniza la definición de `data/Gold.Lakehouse`, pero no
versiona ni ejecuta los datos de las tablas. La automatización completa se realiza con
`.github/workflows/deploy-fabric.yml`:

1. localiza el workspace `amldemo` o lo crea cuando se proporciona `FABRIC_CAPACITY_ID`;
2. crea o reutiliza el Lakehouse `GOLD`;
3. descarga los snapshots Parquet/Zstandard mediante Git LFS;
4. los carga en OneLake;
5. recrea las tablas Delta y valida conteos, fechas y relaciones.

El proceso es idempotente: una nueva ejecución sobrescribe cada tabla con el snapshot
versionado y vuelve a ejecutar todas las validaciones.

## Datos versionados

Los datos se guardan en `data/snapshots/gold/<tabla>/*.parquet` usando compresión
Zstandard. Parquet evita la sobrecarga de CSV y conserva los tipos; los archivos se
gestionan con Git LFS para no superar el límite de 100 MiB de GitHub.

El contrato del paquete está en `data/snapshots/gold/manifest.json`.

## Configuración de GitHub

Crear una aplicación de Microsoft Entra o identidad de servicio con credencial federada
para este repositorio. Debe tener permisos para crear items y ejecutar Spark en el
workspace, además de acceso de escritura a OneLake.

Configurar estos secretos del repositorio:

- `AZURE_CLIENT_ID`
- `AZURE_TENANT_ID`
- `AZURE_SUBSCRIPTION_ID`

Configurar estas variables:

- `FABRIC_WORKSPACE_NAME`: opcional; valor predeterminado `amldemo`.
- `FABRIC_CAPACITY_ID`: obligatorio solamente si el workflow debe crear el workspace o
	asignarle capacidad.

Después de hacer push a `main`, el workflow despliega automáticamente. También puede
ejecutarse manualmente desde **Actions > Deploy AML Gold to Fabric**.

## Integración Git de Fabric

Para usar también Source Control en Fabric, conectar el workspace al repositorio,
rama `main`, carpeta Git `/data`, y ejecutar **Update all**. Esto crea el item `GOLD`
y sincroniza los notebooks. La hidratación de tablas sigue a cargo del workflow, ya que
Git Integration no transporta contenido de OneLake ni ejecuta notebooks.

## Regenerar el snapshot

`src/export_gold_snapshot.py` se ejecuta por Livy contra el Lakehouse fuente. Genera
Parquet/Zstandard bajo `Files/github_exports/gold`; después se descarga esa carpeta a
`data/snapshots/gold`, se valida el manifiesto y se publica con Git LFS.
