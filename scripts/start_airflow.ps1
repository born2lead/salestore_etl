<#
.SYNOPSIS
    Starts local Apache Airflow via Docker Compose.
.DESCRIPTION
    Checks Docker daemon status, initializes the environment if needed, and launches Airflow webserver & scheduler.
#>

$ErrorActionPreference = "Stop"

Write-Host "=== Starting Local Apache Airflow ===" -ForegroundColor Cyan

function Test-DockerDaemon {
    $prev = $ErrorActionPreference
    try {
        $ErrorActionPreference = "SilentlyContinue"
        & docker info 2>$null | Out-Null
        return ($LASTEXITCODE -eq 0)
    } catch {
        return $false
    } finally {
        $ErrorActionPreference = $prev
    }
}

# 1. Check if Docker engine is running
if (-not (Test-DockerDaemon)) {
    Write-Host "Docker daemon is not running. Attempting to start Docker Desktop..." -ForegroundColor Yellow
    $dockerPath = "C:\Users\Amal Raj\AppData\Local\Programs\DockerDesktop\Docker Desktop.exe"
    
    if (Test-Path $dockerPath) {
        Start-Process -FilePath $dockerPath
        Write-Host "Docker Desktop launched. Waiting for engine to initialize (can take 30-60 seconds)..." -ForegroundColor Yellow
        $retries = 25
        while ($retries -gt 0) {
            Start-Sleep -Seconds 3
            if (Test-DockerDaemon) {
                Write-Host "Docker daemon is now ready!" -ForegroundColor Green
                break
            }
            $retries--
            Write-Host -NoNewline "."
        }
        Write-Host ""
        if (-not (Test-DockerDaemon)) {
            Write-Warning "Docker Desktop is still initializing or needs manual interaction (e.g., UAC / Terms prompt)."
            Write-Host "Please ensure Docker Desktop is open and showing 'Engine running', then re-run this script." -ForegroundColor Cyan
            exit 1
        }
    } else {
        Write-Error "Docker Desktop executable not found at '$dockerPath'. Please launch Docker Desktop manually."
        exit 1
    }
} else {
    Write-Host "Docker daemon is active." -ForegroundColor Green
}

# 2. Run docker compose up
Write-Host "Launching Airflow services with Docker Compose..." -ForegroundColor Cyan
docker compose up -d

Write-Host "`n=== Airflow Environment Status ===" -ForegroundColor Green
Write-Host "Web UI URL   : http://localhost:8080" -ForegroundColor Cyan
Write-Host "Username     : admin" -ForegroundColor White
Write-Host "Password     : admin" -ForegroundColor White
Write-Host "DAG File     : dags/aws_de_pipeline_dag.py" -ForegroundColor White
Write-Host "`nTo check logs: docker compose logs -f" -ForegroundColor Gray
Write-Host "To stop      : powershell scripts/stop_airflow.ps1" -ForegroundColor Gray
