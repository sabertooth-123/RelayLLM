<#
.SYNOPSIS
  Reproduce the published benchmark: release build, both durability levels,
  3 repetitions each, 5 worker-kill recovery trials each.

.DESCRIPTION
  Results land in bench\results\<timestamp>\ (CSV + JSON + environment).
  Close other heavy programs first: the client, coordinator and workers all
  share this machine's CPUs and disk, and the numbers are sensitive to that.

.EXAMPLE
  powershell -ExecutionPolicy Bypass -File scripts\bench.ps1
#>
param(
    [int]$Repeat = 3,
    [int]$RecoveryTrials = 5
)
$ErrorActionPreference = 'Stop'
Set-Location (Split-Path -Parent $PSScriptRoot)

cargo build --release --locked -p relay-cli
if ($LASTEXITCODE -ne 0) { throw "cargo build failed" }
rustc --version

foreach ($level in 'full', 'normal') {
    & .\target\release\relay.exe bench --sqlite-synchronous $level --repeat $Repeat --recovery-trials $RecoveryTrials --out bench\results
    if ($LASTEXITCODE -ne 0) { throw "benchmark ($level) failed" }
}
