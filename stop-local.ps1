$ErrorActionPreference = "Stop"
$processFile = Join-Path $PSScriptRoot ".local-data\processes.json"
if (-not (Test-Path $processFile)) {
    Write-Host "No local stack PID file found."
    exit 0
}

$processes = @(Get-Content $processFile -Raw | ConvertFrom-Json)
$allProcesses = @(Get-CimInstance Win32_Process)

function Stop-LocalProcessTree($processId, $expectedPath, $isRoot) {
    $children = @($allProcesses | Where-Object ParentProcessId -eq $processId)
    foreach ($child in $children) {
        Stop-LocalProcessTree $child.ProcessId $expectedPath $false
    }
    $process = $allProcesses | Where-Object ProcessId -eq $processId | Select-Object -First 1
    if ($process -and (-not $isRoot -or $process.ExecutablePath -eq $expectedPath)) {
        Stop-Process -Id $processId -Force -ErrorAction SilentlyContinue
    }
}

foreach ($entry in $processes) {
    Stop-LocalProcessTree $entry.Id $entry.Path $true
    Write-Host "$($entry.Name) stopped"
}
Remove-Item $processFile