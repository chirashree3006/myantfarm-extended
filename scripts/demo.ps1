# Command-line demo of one incident, end to end (stack must be running).
#   powershell -ExecutionPolicy Bypass -File scripts\demo.ps1 -Scenario leak
#   powershell -ExecutionPolicy Bypass -File scripts\demo.ps1 -Scenario slow_db -Base http://<VM-IP>

param(
  [ValidateSet("auth_regression", "leak", "slow_db", "healthy")] [string]$Scenario = "auth_regression",
  [string]$Base = "http://localhost",
  [int]$Traffic = 60
)
$ErrorActionPreference = "Stop"
$H = @{ "Content-Type" = "application/json" }
function Post($path, $body) { Invoke-RestMethod -Method Post -Uri "$Base$path" -Headers $H -Body ($body | ConvertTo-Json) -TimeoutSec 900 }

Write-Host "== 1. Reset all replicas, inject '$Scenario'" -ForegroundColor Cyan
Post "/api/multi/control/reset" @{} | Out-Null
Post "/api/multi/control/restock" @{} | Out-Null
if ($Scenario -ne "healthy") { Post "/api/multi/control/fault" @{ mode = $Scenario } | Out-Null }

Write-Host "== 2. Send $Traffic requests through the load balancer" -ForegroundColor Cyan
$t = Post "/api/multi/control/traffic" @{ count = $Traffic; concurrency = 10; mix = "mixed" }
Write-Host ("   success {0}%  p95 {1}ms  distribution: {2}" -f $t.success_rate_pct, $t.latency_ms.p95, ($t.by_instance | ConvertTo-Json -Compress))

$sc = if ($Scenario -eq "healthy") { $null } else { $Scenario }
Write-Host "== 3. C2 single-agent copilot (one TinyLlama call)..." -ForegroundColor Cyan
$s = Post "/api/single/analyze" @{ scenario = $sc }
Write-Host $s.answer -ForegroundColor DarkGray
Write-Host ("   DQ {0}  actionable={1}  ({2:N1}s)" -f $s.dq.dq, $s.dq.actionable, ($s.elapsed_ms / 1000)) -ForegroundColor Yellow

Write-Host "== 4. C3x multi-agent (4 agents in parallel + coordinator)..." -ForegroundColor Cyan
$m = Post "/api/multi/incident/analyze" @{ scenario = $sc; mode = "extended" }
$b = $m.brief
Write-Host ("   ROOT CAUSE: {0} ({1}% confidence, agents: {2})" -f $b.diagnosis.title, $b.diagnosis.confidence_pct, ($b.diagnosis.supporting_agents -join ", "))
Write-Host "   $($b.summary)"
foreach ($a in $b.action_plan) { Write-Host ("   {0}. [{1}] ({2}) {3}`n        $ {4}" -f $a.priority, $a.type, $a.owner, $a.action, $a.command) }
Write-Host ("   Teams paged: {0}" -f ($b.teams_paged -join ", "))
Write-Host ("   DQ {0}  actionable={1}  ({2:N1}s, served by {3})" -f $m.dq.dq, $m.dq.actionable, ($m.timing_ms.total / 1000), $m.served_by) -ForegroundColor Green
