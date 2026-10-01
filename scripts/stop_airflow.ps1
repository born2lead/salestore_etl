<#
.SYNOPSIS
    Stops local Apache Airflow containers.
#>

Write-Host "Stopping Airflow containers..." -ForegroundColor Yellow
docker compose down
Write-Host "Airflow stopped." -ForegroundColor Green
