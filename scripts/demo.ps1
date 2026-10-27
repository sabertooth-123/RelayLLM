<#
.SYNOPSIS
  End-to-end RelayLLM demo on this machine (no Docker needed).

.DESCRIPTION
  1-9: `relay demo` starts 1 coordinator + 3 worker processes, submits jobs,
       kills a worker mid-job, restarts the coordinator, and checks
       idempotency and stale-attempt fencing. It exits non-zero if any check
       fails.
  10:  runs the full automated test suite.

  Works in Windows PowerShell 5.1 and PowerShell 7. Requires Rust + MSVC
  Build Tools (see README).

.EXAMPLE
  powershell -ExecutionPolicy Bypass -File scripts\demo.ps1
  powershell -ExecutionPolicy Bypass -File scripts\demo.ps1 -Jobs 30 -SkipTests
#>
param(
    [int]$Jobs = 100,
    [switch]$SkipTests
)
$ErrorActionPreference = 'Stop'
Set-Location (Split-Path -Parent $PSScriptRoot)

Write-Host "== Building relay (release) ==" -ForegroundColor Cyan
cargo build --release --locked -p relay-cli
if ($LASTEXITCODE -ne 0) { throw "cargo build failed" }

Write-Host "`n== relay demo --jobs $Jobs ==" -ForegroundColor Cyan
& .\target\release\relay.exe demo --jobs $Jobs
$demoExit = $LASTEXITCODE

$testExit = 0
if ($SkipTests) {
    Write-Host "`n== Step 10 skipped (-SkipTests). Run: cargo test --workspace ==" -ForegroundColor Yellow
} else {
    Write-Host "`n== Step 10: cargo test --workspace ==" -ForegroundColor Cyan
    cargo test --workspace --locked
    $testExit = $LASTEXITCODE
}

Write-Host ""
if ($demoExit -eq 0 -and $testExit -eq 0) {
    Write-Host "DEMO PASSED: every demo check and every test succeeded." -ForegroundColor Green
    exit 0
}
Write-Host "DEMO FAILED: relay demo exit=$demoExit, cargo test exit=$testExit" -ForegroundColor Red
exit 1
