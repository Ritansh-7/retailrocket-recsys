$ErrorActionPreference = "Stop"
$root = $PSScriptRoot
$tools = Join-Path $root ".local-data\native-tools"
$downloads = Join-Path $root ".local-data\downloads"
New-Item -ItemType Directory -Force -Path $tools, $downloads | Out-Null

function Install-PortableArchive($name, $url, $fileName, $destination, $marker) {
    if (Test-Path (Join-Path $destination ".installed")) {
        Write-Host "$name already installed"
        return
    }

    $archive = Join-Path $downloads $fileName
    $partial = "$archive.part"
    if (-not (Test-Path $archive)) {
        Remove-Item $partial -Force -ErrorAction SilentlyContinue
        Write-Host "Downloading $name..."
        Invoke-WebRequest -Uri $url -OutFile $partial
        Move-Item $partial $archive
    }

    New-Item -ItemType Directory -Force -Path $destination | Out-Null
    Write-Host "Extracting $name..."
    $tarArguments = if ($fileName.EndsWith(".tgz")) { @("-xzf", $archive, "-C", $destination) } else { @("-xf", $archive, "-C", $destination) }
    & tar.exe @tarArguments
    if ($LASTEXITCODE -ne 0) { throw "Could not extract $name." }
    Remove-Item $archive -Force
    Set-Content -Path (Join-Path $destination ".installed") -Value "ready" -Encoding ascii
}

$jdkRoot = Join-Path $tools "jdk"
$kafkaRoot = Join-Path $tools "kafka"
$kafkaUiRoot = Join-Path $tools "kafka-ui"
$prometheusRoot = Join-Path $tools "prometheus"
$grafanaRoot = Join-Path $tools "grafana"

Install-PortableArchive "Eclipse Temurin JDK 17" `
    "https://api.adoptium.net/v3/binary/latest/17/ga/windows/x64/jdk/hotspot/normal/eclipse" `
    "temurin-17.zip" $jdkRoot (Join-Path $jdkRoot ".installed")
Install-PortableArchive "Apache Kafka 3.9.1" `
    "https://archive.apache.org/dist/kafka/3.9.1/kafka_2.13-3.9.1.tgz" `
    "kafka-3.9.1.tgz" $kafkaRoot (Join-Path $kafkaRoot ".installed")
$kafkaUiJar = Join-Path $kafkaUiRoot "kafka-ui-api-v0.7.2.jar"
if (-not (Test-Path $kafkaUiJar)) {
    New-Item -ItemType Directory -Force -Path $kafkaUiRoot | Out-Null
    $partialJar = "$kafkaUiJar.part"
    Invoke-WebRequest `
        -Uri "https://github.com/provectus/kafka-ui/releases/download/v0.7.2/kafka-ui-api-v0.7.2.jar" `
        -OutFile $partialJar
    Move-Item $partialJar $kafkaUiJar
}
Install-PortableArchive "Prometheus 2.53.3" `
    "https://github.com/prometheus/prometheus/releases/download/v2.53.3/prometheus-2.53.3.windows-amd64.zip" `
    "prometheus-2.53.3.zip" $prometheusRoot (Join-Path $prometheusRoot ".installed")
Install-PortableArchive "Grafana 11.3.1" `
    "https://dl.grafana.com/enterprise/release/grafana-enterprise-11.3.1.windows-amd64.zip" `
    "grafana-11.3.1.zip" $grafanaRoot (Join-Path $grafanaRoot ".installed")

Write-Host "Native Kafka, Kafka UI, Prometheus, and Grafana tools are ready under $tools"