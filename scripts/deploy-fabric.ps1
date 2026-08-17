param(
    [string]$WorkspaceName = "amldemo",
    [string]$LakehouseName = "GOLD",
    [string]$CapacityId = "",
    [string]$SnapshotRoot = "$PSScriptRoot/../data/snapshots/gold"
)

$ErrorActionPreference = "Stop"
$FabricResource = "https://api.fabric.microsoft.com"
$FabricApi = "$FabricResource/v1"
$deploymentTimer = [System.Diagnostics.Stopwatch]::StartNew()
$phaseTimings = [ordered]@{}

function Complete-Phase {
    param([string]$Name, [System.Diagnostics.Stopwatch]$Timer)
    $Timer.Stop()
    $phaseTimings[$Name] = [math]::Round($Timer.Elapsed.TotalSeconds, 3)
    Write-Host "TIMING $Name=$($phaseTimings[$Name])s"
}

function Get-FabricHeaders {
    $token = az account get-access-token --resource $FabricResource --query accessToken --output tsv
    if (-not $token) { throw "Unable to acquire a Fabric access token." }
    return @{ Authorization = "Bearer $token" }
}

function Invoke-FabricApi {
    param([string]$Method, [string]$Uri, $Body = $null)
    $parameters = @{
        Method = $Method
        Uri = $Uri
        Headers = Get-FabricHeaders
        ContentType = "application/json"
    }
    if ($null -ne $Body) {
        $parameters.Body = $Body | ConvertTo-Json -Depth 20 -Compress
    }
    return Invoke-RestMethod @parameters
}

function Wait-LivyIdle {
    param([string]$BaseUri, [string]$SessionId)
    for ($attempt = 0; $attempt -lt 120; $attempt++) {
        $session = Invoke-FabricApi -Method Get -Uri "$BaseUri/sessions/$SessionId"
        if ($session.state -eq "idle") { return }
        if ($session.state -in @("dead", "error", "killed", "shutting_down")) {
            throw "Livy session $SessionId entered state $($session.state)."
        }
        Start-Sleep -Seconds 5
    }
    throw "Timed out waiting for Livy session $SessionId."
}

$phaseTimer = [System.Diagnostics.Stopwatch]::StartNew()
$workspaceResponse = Invoke-FabricApi -Method Get -Uri "$FabricApi/workspaces"
$workspace = @($workspaceResponse.value) | Where-Object {
    $_.displayName -ieq $WorkspaceName
} | Select-Object -First 1

if (-not $workspace) {
    if (-not $CapacityId) {
        throw "Workspace '$WorkspaceName' does not exist. Set FABRIC_CAPACITY_ID to create it."
    }
    $workspace = Invoke-FabricApi -Method Post -Uri "$FabricApi/workspaces" -Body @{
        displayName = $WorkspaceName
        capacityId = $CapacityId
    }
}

$workspaceId = $workspace.id
$workspaceDetails = Invoke-FabricApi -Method Get -Uri "$FabricApi/workspaces/$workspaceId"
if (-not $workspaceDetails.capacityId) {
    if (-not $CapacityId) { throw "Workspace '$WorkspaceName' has no Fabric capacity assigned." }
    Invoke-FabricApi -Method Post -Uri "$FabricApi/workspaces/$workspaceId/assignToCapacity" -Body @{
        capacityId = $CapacityId
    } | Out-Null
}
Complete-Phase -Name "workspace" -Timer $phaseTimer

$phaseTimer = [System.Diagnostics.Stopwatch]::StartNew()
$items = Invoke-FabricApi -Method Get -Uri "$FabricApi/workspaces/$workspaceId/items?type=Lakehouse"
$lakehouse = @($items.value) | Where-Object {
    $_.displayName -ieq $LakehouseName
} | Select-Object -First 1

if (-not $lakehouse) {
    $lakehouse = Invoke-FabricApi -Method Post -Uri "$FabricApi/workspaces/$workspaceId/items" -Body @{
        displayName = $LakehouseName
        type = "Lakehouse"
    }
} elseif ($lakehouse.displayName -cne $LakehouseName) {
    Invoke-FabricApi -Method Patch -Uri "$FabricApi/workspaces/$workspaceId/items/$($lakehouse.id)" -Body @{
        displayName = $LakehouseName
    } | Out-Null
}

$lakehouseId = $lakehouse.id
Complete-Phase -Name "lakehouse" -Timer $phaseTimer
if (-not (Test-Path "$SnapshotRoot/manifest.json")) {
    throw "Snapshot manifest not found at $SnapshotRoot/manifest.json"
}
if (-not (Get-Command azcopy -ErrorAction SilentlyContinue)) {
    throw "azcopy is required to upload the snapshot to OneLake."
}

$env:AZCOPY_AUTO_LOGIN_TYPE = "AZCLI"
$destination = "https://onelake.dfs.fabric.microsoft.com/$workspaceId/$lakehouseId/Files/bootstrap/gold"
$phaseTimer = [System.Diagnostics.Stopwatch]::StartNew()
azcopy copy "$SnapshotRoot/*" $destination --recursive=true --overwrite=true
if ($LASTEXITCODE -ne 0) { throw "AzCopy failed with exit code $LASTEXITCODE." }
Complete-Phase -Name "onelake_upload" -Timer $phaseTimer

$livyBase = "$FabricApi/workspaces/$workspaceId/lakehouses/$lakehouseId/livyapi/versions/2023-12-01"
$phaseTimer = [System.Diagnostics.Stopwatch]::StartNew()
$session = Invoke-FabricApi -Method Post -Uri "$livyBase/sessions" -Body @{
    name = "github_gold_bootstrap"
    conf = @{
        "spark.fabric.pool.name" = "Starter Pool"
        "spark.dynamicAllocation.enabled" = "true"
    }
}
$sessionId = $session.id

try {
    Wait-LivyIdle -BaseUri $livyBase -SessionId $sessionId
    Complete-Phase -Name "spark_startup" -Timer $phaseTimer
    $code = Get-Content "$PSScriptRoot/../src/bootstrap_gold.py" -Raw
    $code = $code.Replace("__WORKSPACE_ID__", $workspaceId).Replace("__LAKEHOUSE_ID__", $lakehouseId)
    $phaseTimer = [System.Diagnostics.Stopwatch]::StartNew()
    $statement = Invoke-FabricApi -Method Post -Uri "$livyBase/sessions/$sessionId/statements" -Body @{
        code = $code
        kind = "pyspark"
    }

    for ($attempt = 0; $attempt -lt 360; $attempt++) {
        $result = Invoke-FabricApi -Method Get -Uri "$livyBase/sessions/$sessionId/statements/$($statement.id)"
        if ($result.state -eq "available") {
            if ($result.output.status -ne "ok") {
                throw "$($result.output.ename): $($result.output.evalue)`n$($result.output.traceback -join "`n")"
            }
            $text = $result.output.data.'text/plain' -join "`n"
            if ($text -notmatch "DEPLOY_RESULT_JSON=") {
                throw "Deployment completed without the expected validation marker. Output: $text"
            }
            Write-Host $text
            Complete-Phase -Name "delta_materialization_and_validation" -Timer $phaseTimer
            break
        }
        if ($result.state -in @("error", "cancelled")) {
            throw "Livy statement entered state $($result.state)."
        }
        Start-Sleep -Seconds 10
    }
    if ($result.state -ne "available") { throw "Timed out waiting for the Gold deployment statement." }
} finally {
    Invoke-FabricApi -Method Delete -Uri "$livyBase/sessions/$sessionId" | Out-Null
}

$deploymentTimer.Stop()
$phaseTimings["total"] = [math]::Round($deploymentTimer.Elapsed.TotalSeconds, 3)
$timingResult = [ordered]@{
    workspaceName = $WorkspaceName
    workspaceId = $workspaceId
    lakehouseName = $LakehouseName
    lakehouseId = $lakehouseId
    seconds = $phaseTimings
}
Write-Host "DEPLOY_TIMING_JSON=$($timingResult | ConvertTo-Json -Depth 5 -Compress)"
Write-Host "GOLD deployment completed in workspace '$WorkspaceName' ($workspaceId), Lakehouse $lakehouseId."
