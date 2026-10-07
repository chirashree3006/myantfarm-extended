# Start the whole stack on Windows (Docker Desktop) and open the shop + ops console.
#   cd C:\Users\HP\Documents\myantfarm-extended
#   powershell -ExecutionPolicy Bypass -File scripts\start.ps1          # full (2 TinyLlama replicas)
#   powershell -ExecutionPolicy Bypass -File scripts\start.ps1 -Lite    # 1 TinyLlama replica (8 GB laptops)

param([switch]$Lite)

$ErrorActionPreference = "Stop"
Set-Location (Split-Path $PSScriptRoot -Parent)

docker info *> $null
if ($LASTEXITCODE -ne 0) { Write-Host "Docker Desktop is not running. Start it and retry." -ForegroundColor Red; exit 1 }

if (-not (Test-Path .env)) { Copy-Item .env.example .env }
$port = (Select-String -Path .env -Pattern '^GATEWAY_PORT=(\d+)').Matches.Groups[1].Value
if (-not $port) { $port = "80" }
$base = if ($port -eq "80") { "http://localhost" } else { "http://localhost:$port" }

$files = @("-f", "docker-compose.yml")
if ($Lite) { $files += @("-f", "docker-compose.lite.yml") }

Write-Host "Building and starting containers (first run downloads ~2 GB + TinyLlama)..." -ForegroundColor Cyan
docker compose @files up -d --build
if ($LASTEXITCODE -ne 0) { exit 1 }

Write-Host "Waiting for the gateway..." -ForegroundColor Cyan
for ($i = 0; $i -lt 60; $i++) {
  try { Invoke-RestMethod "$base/lb/health" -TimeoutSec 3 | Out-Null; break } catch { Start-Sleep 2 }
}

Write-Host "Waiting for TinyLlama to be loaded (first run: a few minutes)..." -ForegroundColor Cyan
for ($i = 0; $i -lt 120; $i++) {
  try {
    $m = Invoke-RestMethod "$base/api/multi/metrics" -TimeoutSec 5
    if (($m.llm_health.PSObject.Properties.Value | Where-Object { $_.model_loaded }).Count -gt 0) { break }
  } catch {}
  Start-Sleep 5
}

docker compose @files ps
Write-Host "`nShop        : $base/" -ForegroundColor Green
Write-Host "Ops console : $base/ops/" -ForegroundColor Green
Write-Host "Backend     : $base/backend/   (traffic-surge demo)" -ForegroundColor Green
Write-Host "Website LB  : http://localhost:8080/" -ForegroundColor Green
Start-Process "$base/ops/"
Start-Process "$base/"
