param([switch]$RunDvc)

$ErrorActionPreference = "Stop"
$root = $PSScriptRoot
$envFile = Join-Path $root ".env"
if (Test-Path $envFile) {
    foreach ($line in Get-Content $envFile) {
        $trimmed = $line.Trim()
        if (-not $trimmed -or $trimmed.StartsWith("#")) { continue }
        if ($trimmed -match '^(?<name>[A-Za-z_][A-Za-z0-9_]*)=(?<value>.*)$') {
            $name = $Matches['name']
            $value = $Matches['value'].Trim()
            if (($value.StartsWith('"') -and $value.EndsWith('"')) -or ($value.StartsWith("'") -and $value.EndsWith("'"))) {
                $value = $value.Substring(1, $value.Length - 2)
            }
            [Environment]::SetEnvironmentVariable($name, $value, "Process")
        }
    }
}
$python = Join-Path $root ".venv\Scripts\python.exe"
$localData = Join-Path $root ".local-data"
$tools = Join-Path $localData "native-tools"
$logs = Join-Path $localData "logs"
$processFile = Join-Path $localData "processes.json"
$kafkaHome = Join-Path $tools "kafka\kafka_2.13-3.9.1"
$kafkaUiJar = Join-Path $tools "kafka-ui\kafka-ui-api-v0.7.2.jar"
$java = Get-ChildItem (Join-Path $tools "jdk") -Filter java.exe -File -Recurse -ErrorAction SilentlyContinue | Select-Object -First 1 -ExpandProperty FullName
$kafkaClasspath = "$kafkaHome\libs\*;$kafkaHome\config"
$prometheus = Get-ChildItem (Join-Path $tools "prometheus") -Filter prometheus.exe -File -Recurse -ErrorAction SilentlyContinue | Select-Object -First 1 -ExpandProperty FullName
$grafana = Get-ChildItem (Join-Path $tools "grafana") -Filter grafana-server.exe -File -Recurse -ErrorAction SilentlyContinue | Select-Object -First 1 -ExpandProperty FullName

if (-not (Test-Path $python)) { throw "Python environment not found at $python." }
if (-not $java -or -not (Test-Path (Join-Path $kafkaHome "libs\kafka_2.13-3.9.1.jar")) -or -not (Test-Path $kafkaUiJar) -or -not $prometheus -or -not $grafana) {
    throw "Native services are not installed. Run .\install-local-tools.ps1 first."
}
if (Test-Path $processFile) { throw "Local services are already tracked. Run .\stop-local.ps1 first." }
$env:OPENBLAS_NUM_THREADS = "1"
$env:OMP_NUM_THREADS = "1"
$env:MKL_NUM_THREADS = "1"
$env:NUMEXPR_NUM_THREADS = "1"
foreach ($port in 9092, 9093) {
    if (Get-NetTCPConnection -LocalPort $port -State Listen -ErrorAction SilentlyContinue) {
        throw "Kafka port $port is already in use."
    }
}

if ($RunDvc) {
    & $python -m dvc repro
    if ($LASTEXITCODE -ne 0) { throw "DVC pipeline failed." }
}

function Find-FreePort($startPort) {
    for ($port = $startPort; $port -lt $startPort + 50; $port++) {
        if (-not (Get-NetTCPConnection -LocalPort $port -State Listen -ErrorAction SilentlyContinue)) {
            return $port
        }
    }
    throw "No free port found starting at $startPort."
}

function Wait-ForTcpPort($port, $serviceName) {
    $deadline = [DateTime]::UtcNow.AddSeconds(60)
    while ([DateTime]::UtcNow -lt $deadline) {
        $client = [System.Net.Sockets.TcpClient]::new()
        $waitHandle = $null
        try {
            $connect = $client.BeginConnect("127.0.0.1", $port, $null, $null)
            $waitHandle = $connect.AsyncWaitHandle
            if ($waitHandle.WaitOne(250)) {
                $client.EndConnect($connect)
                return
            }
        } catch {
        } finally {
            if ($waitHandle) { $waitHandle.Dispose() }
            $client.Dispose()
        }
    }
    throw "$serviceName did not open port $port within 60 seconds. Check $logs."
}

$mlflowPort = Find-FreePort 5000
$kafkaUiPort = Find-FreePort 8080
$apiPort = Find-FreePort 8000
$streamlitPort = Find-FreePort 8501
$prometheusPort = Find-FreePort 9090
$grafanaPort = Find-FreePort 3000
$grafanaHome = Split-Path (Split-Path $grafana -Parent) -Parent
$jdkHome = Split-Path (Split-Path $java -Parent) -Parent
$kafkaData = Join-Path $localData "kafka-data"
$kafkaConfig = Join-Path $localData "kafka.properties"
$provisioning = Join-Path $localData "grafana\provisioning"
$dashboardPath = (Join-Path $root "monitoring\grafana\dashboards").Replace("\", "/")

New-Item -ItemType Directory -Force -Path $logs, $kafkaData, `
    (Join-Path $localData "prometheus-data"), `
    (Join-Path $localData "grafana\data"), `
    (Join-Path $localData "grafana\logs"), `
    (Join-Path $localData "grafana\plugins"), `
    (Join-Path $provisioning "datasources"), `
    (Join-Path $provisioning "dashboards"), `
    (Join-Path $localData "mlflow") | Out-Null

$kafkaDataPath = $kafkaData.Replace("\", "/")
@"
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@127.0.0.1:9093
listeners=PLAINTEXT://127.0.0.1:9092,CONTROLLER://127.0.0.1:9093
advertised.listeners=PLAINTEXT://127.0.0.1:9092
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
controller.listener.names=CONTROLLER
inter.broker.listener.name=PLAINTEXT
log.dirs=$kafkaDataPath
num.partitions=3
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
group.initial.rebalance.delay.ms=0
"@ | Set-Content -Encoding ascii $kafkaConfig

$env:JAVA_HOME = $jdkHome
$env:PATH = "$jdkHome\bin;$env:PATH"
if (-not (Test-Path (Join-Path $kafkaData "meta.properties"))) {
    $clusterId = (& $java -cp $kafkaClasspath kafka.tools.StorageTool random-uuid | Select-Object -Last 1).Trim()
    if ($LASTEXITCODE -ne 0 -or -not $clusterId) { throw "Could not generate the Kafka cluster ID." }
    & $java -cp $kafkaClasspath kafka.tools.StorageTool format --standalone -t $clusterId -c $kafkaConfig
    if ($LASTEXITCODE -ne 0) { throw "Could not format Kafka's local storage." }
}

$env:RECSYS_KAFKA_BOOTSTRAP_SERVERS = "127.0.0.1:9092"
$env:RECSYS_TRACKING_URI = "http://127.0.0.1:$mlflowPort"
$env:MLFLOW_TRACKING_URI = "http://127.0.0.1:$mlflowPort"
$env:RECSYS_API_URL = "http://127.0.0.1:$apiPort"
$env:RECSYS_MLFLOW_URL = "http://localhost:$mlflowPort"
$env:RECSYS_MLFLOW_MODELS_URL = "http://localhost:$mlflowPort/#/models"
$env:RECSYS_KAFKA_UI_URL = "http://localhost:$kafkaUiPort"
$env:RECSYS_PROMETHEUS_URL = "http://localhost:$prometheusPort"
$env:RECSYS_GRAFANA_URL = "http://localhost:$grafanaPort"
$processes = [System.Collections.Generic.List[object]]::new()

function Save-ProcessList {
    $processes | ConvertTo-Json -Depth 3 | Set-Content -Encoding utf8 $processFile
}

function Track-Process($name, $process, $path) {
    $processes.Add([pscustomobject]@{ Name = $name; Id = $process.Id; Path = $path })
    Save-ProcessList
    Write-Host "$name started (PID $($process.Id))"
}

function Start-LocalProcess($name, $arguments, $port) {
    $process = Start-Process -FilePath $python -ArgumentList $arguments -WorkingDirectory $root `
        -RedirectStandardOutput (Join-Path $logs "$name.out.log") `
        -RedirectStandardError (Join-Path $logs "$name.err.log") -PassThru -WindowStyle Hidden
    Track-Process $name $process $python
    if ($port) { Write-Host "$name port: $port" }
}

$kafkaLogConfig = "-Dlog4j.configuration=file:$kafkaHome/config/log4j.properties"
$kafkaProcess = Start-Process -FilePath $java -ArgumentList @(
    "-Xms256m", "-Xmx512m", $kafkaLogConfig, "-cp", "`"$kafkaClasspath`"", "kafka.Kafka", $kafkaConfig
) `
    -WorkingDirectory $root -RedirectStandardOutput (Join-Path $logs "kafka.out.log") `
    -RedirectStandardError (Join-Path $logs "kafka.err.log") -PassThru -WindowStyle Hidden
Track-Process "kafka" $kafkaProcess $java
Wait-ForTcpPort 9092 "Kafka"

$previousKafkaUiEnvironment = @{}
$kafkaUiEnvironment = @{
    DYNAMIC_CONFIG_ENABLED = "true"
    KAFKA_CLUSTERS_0_NAME = "ABMLOPS Local"
    KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS = "127.0.0.1:9092"
    SERVER_PORT = "$kafkaUiPort"
}
foreach ($name in $kafkaUiEnvironment.Keys) {
    $previousKafkaUiEnvironment[$name] = [Environment]::GetEnvironmentVariable($name, "Process")
    [Environment]::SetEnvironmentVariable($name, $kafkaUiEnvironment[$name], "Process")
}
try {
    $kafkaUiProcess = Start-Process -FilePath $java -ArgumentList @("-Xms96m", "-Xmx256m", "-jar", $kafkaUiJar) `
        -WorkingDirectory $localData -RedirectStandardOutput (Join-Path $logs "kafka-ui.out.log") `
        -RedirectStandardError (Join-Path $logs "kafka-ui.err.log") -PassThru -WindowStyle Hidden
} finally {
    foreach ($name in $kafkaUiEnvironment.Keys) {
        [Environment]::SetEnvironmentVariable($name, $previousKafkaUiEnvironment[$name], "Process")
    }
}
Track-Process "kafka-ui" $kafkaUiProcess $java
Wait-ForTcpPort $kafkaUiPort "Kafka UI"

$mlflowBackend = "sqlite:///./.local-data/mlflow/backend.db"
$mlflowArtifacts = "file:./.local-data/mlflow/artifacts"
Start-LocalProcess "mlflow" @("-m", "mlflow", "server", "--host", "127.0.0.1", "--port", "$mlflowPort", "--backend-store-uri", $mlflowBackend, "--default-artifact-root", $mlflowArtifacts) $mlflowPort
Wait-ForTcpPort $mlflowPort "MLflow"
$registryCheck = "import mlflow, os; mlflow.set_tracking_uri(os.environ['RECSYS_TRACKING_URI']); models=mlflow.MlflowClient().search_registered_models(); print('present' if any(model.name == 'retailrocket-item-neighbors' for model in models) else 'missing')"
$registryState = & $python -c $registryCheck
if ($LASTEXITCODE -ne 0) { throw "Could not check the MLflow model registry." }
if ($registryState -contains "missing") {
    Write-Host "Registering the recommender model in MLflow..."
    & $python -m recsys.mlflow_log
    if ($LASTEXITCODE -ne 0) { throw "MLflow model registration failed. Check $logs\mlflow-register.err.log." }
} else {
    Write-Host "MLflow model already registered; keeping run and model history."
}

Start-LocalProcess "api" @("-m", "uvicorn", "recsys.api:app", "--host", "127.0.0.1", "--port", "$apiPort") $apiPort
Wait-ForTcpPort $apiPort "API"
Start-LocalProcess "event-sink" @("-m", "recsys.event_sink") $null
Start-LocalProcess "loadgen" @("-m", "recsys.loadgen") $null
Start-LocalProcess "streamlit" @("-m", "streamlit", "run", "streamlit_app.py", "--server.address=127.0.0.1", "--server.port=$streamlitPort", "--server.headless=true") $streamlitPort

@"
global:
  scrape_interval: 15s
  evaluation_interval: 15s
scrape_configs:
  - job_name: recommendation-api
    metrics_path: /metrics
    static_configs:
      - targets: [127.0.0.1:$apiPort]
"@ | Set-Content -Encoding utf8 (Join-Path $localData "prometheus.yml")
$prometheusArgs = @(
    "--config.file=$(Join-Path $localData 'prometheus.yml')",
    "--storage.tsdb.path=$(Join-Path $localData 'prometheus-data')",
    "--web.listen-address=127.0.0.1:$prometheusPort"
)
$prometheusProcess = Start-Process -FilePath $prometheus -ArgumentList $prometheusArgs -WorkingDirectory $root `
    -RedirectStandardOutput (Join-Path $logs "prometheus.out.log") `
    -RedirectStandardError (Join-Path $logs "prometheus.err.log") -PassThru -WindowStyle Hidden
Track-Process "prometheus" $prometheusProcess $prometheus

@"
apiVersion: 1
datasources:
  - name: Prometheus
    uid: Prometheus
    type: prometheus
    access: proxy
    url: http://127.0.0.1:$prometheusPort
    isDefault: true
"@ | Set-Content -Encoding utf8 (Join-Path $provisioning "datasources\prometheus.yml")
@"
apiVersion: 1
providers:
  - name: Recommender
    orgId: 1
    folder: Recommender
    type: file
    disableDeletion: true
    options:
      path: $dashboardPath
"@ | Set-Content -Encoding utf8 (Join-Path $provisioning "dashboards\dashboards.yml")

$grafanaUser = [Environment]::GetEnvironmentVariable("GRAFANA_ADMIN_USER", "Process")
if (-not $grafanaUser) { $grafanaUser = "admin" }
$grafanaPassword = [Environment]::GetEnvironmentVariable("GRAFANA_ADMIN_PASSWORD", "Process")
if (-not $grafanaPassword) { $grafanaPassword = "localdev" }
$grafanaDb = Join-Path $localData "grafana\data\grafana.db"
if ((Test-Path $grafanaDb) -and (($grafanaUser -ne "admin") -or ($grafanaPassword -ne "localdev"))) {
    Remove-Item $grafanaDb -Force -ErrorAction SilentlyContinue
    Remove-Item (Join-Path $localData "grafana\data\grafana.db-shm") -Force -ErrorAction SilentlyContinue
    Remove-Item (Join-Path $localData "grafana\data\grafana.db-wal") -Force -ErrorAction SilentlyContinue
}
$grafanaEnvironment = @{
    GF_PATHS_HOME = $grafanaHome
    GF_PATHS_CONFIG = Join-Path $grafanaHome "conf\defaults.ini"
    GF_PATHS_DATA = Join-Path $localData "grafana\data"
    GF_PATHS_LOGS = Join-Path $localData "grafana\logs"
    GF_PATHS_PLUGINS = Join-Path $localData "grafana\plugins"
    GF_PATHS_PROVISIONING = $provisioning
    GF_SERVER_HTTP_ADDR = "127.0.0.1"
    GF_SERVER_HTTP_PORT = "$grafanaPort"
    GF_SERVER_ROOT_URL = "http://localhost:$grafanaPort"
    GF_SECURITY_ADMIN_USER = $grafanaUser
    GF_SECURITY_ADMIN_PASSWORD = $grafanaPassword
    GF_USERS_ALLOW_SIGN_UP = "false"
}
$oldGrafanaEnvironment = @{}
foreach ($name in $grafanaEnvironment.Keys) {
    $oldGrafanaEnvironment[$name] = [Environment]::GetEnvironmentVariable($name, "Process")
    [Environment]::SetEnvironmentVariable($name, $grafanaEnvironment[$name], "Process")
}
try {
    $grafanaProcess = Start-Process -FilePath $grafana -WorkingDirectory $grafanaHome `
        -RedirectStandardOutput (Join-Path $logs "grafana.out.log") `
        -RedirectStandardError (Join-Path $logs "grafana.err.log") -PassThru -WindowStyle Hidden
} finally {
    foreach ($name in $grafanaEnvironment.Keys) {
        [Environment]::SetEnvironmentVariable($name, $oldGrafanaEnvironment[$name], "Process")
    }
}
Track-Process "grafana" $grafanaProcess $grafana

Write-Host "Dashboard: http://localhost:$streamlitPort"
Write-Host "API: http://localhost:$apiPort (health: /healthz, readiness: /readyz, metrics: /metrics)"
Write-Host "MLflow: http://localhost:$mlflowPort"
Write-Host "MLflow models: http://localhost:$mlflowPort/#/models"
Write-Host "Kafka UI: http://localhost:$kafkaUiPort"
Write-Host "Prometheus: http://localhost:$prometheusPort"
Write-Host "Grafana: http://localhost:$grafanaPort ($grafanaUser / $grafanaPassword)"
Write-Host "Kafka: 127.0.0.1:9092; event sink writes to data/events"
Write-Host "Live traffic is generated by the local loadgen process so Prometheus/Grafana stay active."
Write-Host "Run the DVC pipeline with .\.venv\Scripts\dvc.exe repro or start with -RunDvc."
Write-Host "Logs: $logs"