<#
.SYNOPSIS
  The ten-step failure-recovery demo against the Docker Compose deployment.

.DESCRIPTION
  Needs only Docker and PowerShell (5.1 or 7): all API calls are plain
  HTTP from PowerShell, and failures are injected with `docker kill`.
  Each step prints [ok]/[FAIL]; the script exits 1 if any check fails.
  The stack is torn down (including its volume) at the end unless -Keep.

.EXAMPLE
  powershell -ExecutionPolicy Bypass -File scripts\demo-compose.ps1
  powershell -ExecutionPolicy Bypass -File scripts\demo-compose.ps1 -Jobs 50 -Port 7480 -SkipTests
#>
param(
    [int]$Jobs = 100,
    [int]$Port = 7070,
    [switch]$SkipTests,
    [switch]$Keep
)
$ErrorActionPreference = 'Stop'
Set-Location (Split-Path -Parent $PSScriptRoot)
$Base = "http://127.0.0.1:$Port"
$script:Passed = 0
$script:Failed = @()

# Short lease so recovery takes seconds; request timeout must stay below it.
$env:RELAY_PORT = "$Port"
$env:RELAY_LEASE_SECS = "3"
$env:RELAY_REQUEST_TIMEOUT_SECS = "2"
$env:RELAY_MOCK_DELAY_MS = "300"

function Step([int]$n, [string]$title) { Write-Host "`n[$n/10] $title" -ForegroundColor Cyan }
function Note([string]$text) { Write-Host "         $text" }
function Check([bool]$ok, [string]$what) {
    if ($ok) { Write-Host "  [ok]   $what" -ForegroundColor Green; $script:Passed++ }
    else { Write-Host "  [FAIL] $what" -ForegroundColor Red; $script:Failed += $what }
}

# One HTTP call; returns Status and parsed Body, including for 4xx/5xx.
function Api([string]$Method, [string]$Path, $Body = $null) {
    $params = @{ Method = $Method; Uri = "$Base$Path"; UseBasicParsing = $true; ContentType = 'application/json'; TimeoutSec = 30 }
    if ($null -ne $Body) { $params.Body = ($Body | ConvertTo-Json -Depth 8 -Compress) }
    try {
        $r = Invoke-WebRequest @params
        $status = [int]$r.StatusCode
        $text = $r.Content
    } catch {
        $resp = $_.Exception.Response
        if ($null -eq $resp) { throw }
        $status = [int]$resp.StatusCode
        if ($_.ErrorDetails -and $_.ErrorDetails.Message) { $text = $_.ErrorDetails.Message }
        else { $text = (New-Object System.IO.StreamReader($resp.GetResponseStream())).ReadToEnd() }
    }
    $json = $null
    if ($text) { $json = $text | ConvertFrom-Json }
    [pscustomobject]@{ Status = $status; Body = $json }
}

function WaitFor([int]$Seconds, [scriptblock]$Probe) {
    $deadline = (Get-Date).AddSeconds($Seconds)
    while ((Get-Date) -lt $deadline) {
        try { $v = & $Probe; if ($v) { return $v } } catch { }
        Start-Sleep -Milliseconds 250
    }
    return $null
}

function Submit([string]$Prompt, [int]$DelayMs = -1, [string]$Kind = 'mock', [string]$Key = $null) {
    $payload = @{ prompt = $Prompt }
    if ($DelayMs -ge 0) { $payload.delay_ms = $DelayMs }
    $body = @{ kind = $Kind; payload = $payload; max_attempts = 5 }
    if ($Key) { $body.idempotency_key = $Key }
    Api POST /v1/jobs $body
}

function WorkerName([string]$Id) { return ($Id -split '-')[0] }

# Run docker and judge it by its exit code. Windows PowerShell 5.1 turns
# native stderr into terminating errors under 'Stop' as soon as it is
# redirected, and docker writes normal progress output to stderr.
function Invoke-Docker([string[]]$DockerArgs, [switch]$Capture) {
    $previous = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    try {
        $output = & docker @DockerArgs 2>&1 | Out-String
        $code = $LASTEXITCODE
    } finally {
        $ErrorActionPreference = $previous
    }
    if ($Capture) { return $output }
    if ($code -ne 0) { throw "docker $($DockerArgs -join ' ') failed (exit $code): $output" }
}

try {
    # ----------------------------------------------------------------- 1
    Step 1 "Start one coordinator and three workers (docker compose)"
    Invoke-Docker compose, up, --build, -d
    $healthy = WaitFor 60 { (Api GET /healthz).Status -eq 200 }
    $workers = WaitFor 30 { $w = (Api GET /v1/workers).Body; if (@($w).Count -eq 3) { @($w) } }
    Check ($healthy -and @($workers).Count -eq 3) "coordinator healthy at $Base with 3 registered workers (lease 3 s)"

    # ----------------------------------------------------------------- 2
    Step 2 "Submit $Jobs mock LLM jobs"
    $ids = @()
    for ($i = 0; $i -lt $Jobs; $i++) { $ids += (Submit "compose demo job $i").Body.job.id }
    # The marker goes *last*: sequential submits from PowerShell take a few
    # seconds, and a marker submitted first could finish before step 4.
    $marker = (Submit "the job whose worker we will kill" 8000).Body.job.id
    Check ($ids.Count -eq $Jobs) "$($ids.Count) jobs accepted (plus 1 long marker job)"

    # ----------------------------------------------------------------- 3
    Step 3 "Workers process jobs concurrently"
    $maxRunning = 0; $maxBusy = 0; $sample = ''
    for ($i = 0; $i -lt 20; $i++) {
        $w = @((Api GET /v1/workers).Body)
        $running = ($w | Measure-Object -Property running_jobs -Sum).Sum
        $busy = @($w | Where-Object { $_.running_jobs -gt 0 }).Count
        if ($busy -gt $maxBusy -or ($busy -eq $maxBusy -and $running -gt $maxRunning)) {
            $sample = ($w | ForEach-Object { "$($_.id.Substring(0, 15))..: $($_.running_jobs)" }) -join ', '
        }
        if ($running -gt $maxRunning) { $maxRunning = $running }
        if ($busy -gt $maxBusy) { $maxBusy = $busy }
        Start-Sleep -Milliseconds 150
    }
    Check ($maxBusy -ge 2 -and $maxRunning -ge 2) "up to $maxRunning jobs running at once across $maxBusy workers"
    Note "sample: $sample"

    # ----------------------------------------------------------------- 4
    Step 4 "Kill a worker container while it is processing a job"
    $owner = WaitFor 60 { $s = (Api GET "/v1/jobs/$marker").Body; if ($s.state -eq 'running') { $s.worker_id } }
    # Never kill a container we didn't positively identify: an empty pattern
    # would match every container's logs.
    if (-not $owner) { throw "marker job $marker was never observed running; refusing to kill an arbitrary worker" }
    $victim = $null
    $containers = (Invoke-Docker compose, ps, -q, worker -Capture) -split "`r?`n" | Where-Object { $_ }
    foreach ($c in $containers) {
        if ((Invoke-Docker logs, $c -Capture) -match [regex]::Escape($owner)) { $victim = $c }
    }
    if (-not $victim) { throw "could not find the container running worker $owner" }
    $victimName = (Invoke-Docker inspect, --format, '{{.Name}}', $victim -Capture).Trim().TrimStart('/')
    Invoke-Docker kill, $victim
    $killedAt = Get-Date
    Check $true "SIGKILLed container $victimName while it ran job $marker"

    # ----------------------------------------------------------------- 5
    Step 5 "The lease expires and another worker recovers the job"
    $done = WaitFor 120 { $s = (Api GET "/v1/jobs/$marker").Body; if ($s.state -eq 'succeeded') { $s } }
    $elapsed = ((Get-Date) - $killedAt).TotalSeconds
    $history = @((Api GET "/v1/jobs/$marker/attempts").Body)
    foreach ($a in $history) { Note "attempt $($a.attempt) on $($a.worker_id.Substring(0, 15)).. -> $($a.outcome)" }
    $shape = $history.Count -eq 2 -and $history[0].worker_id -eq $owner -and $history[0].outcome -eq 'lease_expired' -and $history[1].worker_id -ne $owner -and $history[1].outcome -eq 'succeeded'
    Check ($done -and $shape) ("marker recovered and finished as attempt 2, {0:N1} s after the kill" -f $elapsed)
    $total = $Jobs + 1
    $allDone = WaitFor 180 {
        $m = (Invoke-WebRequest -UseBasicParsing "$Base/metrics").Content
        if ($m -match 'relay_jobs\{state="succeeded"\} (\d+)' -and [int]$Matches[1] -eq $total) { $true }
    }
    Check ($null -ne $allDone) "all $total jobs succeeded"

    # ----------------------------------------------------------------- 6
    Step 6 "Restart the coordinator (SIGKILL, then start again)"
    $sample5 = $ids[0..4]
    $before = $sample5 | ForEach-Object { ((Api GET "/v1/jobs/$_/result").Body | ConvertTo-Json -Compress -Depth 8) }
    Invoke-Docker compose, kill, coordinator
    Start-Sleep -Seconds 1
    Invoke-Docker compose, start, coordinator
    $back = WaitFor 60 { (Api GET /healthz).Status -eq 200 }
    Check ($null -ne $back) "coordinator container killed and restarted on the same volume"

    # ----------------------------------------------------------------- 7
    Step 7 "Persisted jobs and completed results are still there"
    $m = (Invoke-WebRequest -UseBasicParsing "$Base/metrics").Content
    $succ = 0; if ($m -match 'relay_jobs\{state="succeeded"\} (\d+)') { $succ = [int]$Matches[1] }
    Check ($succ -eq $total) "$succ succeeded jobs after restart"
    $after = $sample5 | ForEach-Object { ((Api GET "/v1/jobs/$_/result").Body | ConvertTo-Json -Compress -Depth 8) }
    Check (($before -join '|') -eq ($after -join '|')) "sampled results are identical to before the crash"
    $fresh = (Submit "after restart").Body.job.id
    $ran = WaitFor 60 { (Api GET "/v1/jobs/$fresh").Body.state -eq 'succeeded' }
    Check ($null -ne $ran) "surviving workers reconnected and processed a new job"

    # ----------------------------------------------------------------- 8
    Step 8 "Submit the same idempotency key twice"
    $key = "compose-demo-" + [guid]::NewGuid()
    $first = Submit "idempotent request" -Key $key
    $second = Submit "idempotent request" -Key $key
    Check ($first.Status -eq 201 -and $second.Status -eq 200 -and $first.Body.job.id -eq $second.Body.job.id) "one logical job: 201 then 200, same id $($first.Body.job.id)"
    $conflict = Submit "a different request" -Key $key
    Check ($conflict.Status -eq 409 -and $conflict.Body.error.code -eq 'idempotency_conflict') "same key + different request -> 409 idempotency_conflict"

    # ----------------------------------------------------------------- 9
    Step 9 "A stale attempt cannot overwrite a newer result"
    $suffix = [guid]::NewGuid().ToString('N').Substring(0, 8)
    $zombie = "zombie-$suffix"; $rescuer = "rescuer-$suffix"
    foreach ($w in $zombie, $rescuer) {
        Api PUT "/v1/workers/$w" @{ name = (WorkerName $w); concurrency = 1; kinds = @('demo-stale') } | Out-Null
    }
    $job = (Submit "who answers last?" -Kind 'demo-stale').Body.job.id
    $a1 = (Api POST "/v1/workers/$zombie/claims" @{ claim_id = "c-zombie" }).Body
    Note "zombie claimed attempt $($a1.attempt) and went silent (no heartbeats)"
    $requeued = WaitFor 30 { (Api GET "/v1/jobs/$job").Body.state -eq 'retry_waiting' }
    Check ($null -ne $requeued) "the zombie's lease expired and the job was requeued"
    $n = 0
    $a2 = WaitFor 30 { $script:n++; $r = Api POST "/v1/workers/$rescuer/claims" @{ claim_id = "c-rescuer-$($script:n)" }; if ($r.Status -eq 200) { $r.Body } }
    Api POST "/v1/jobs/$job/attempts/$($a2.attempt)/complete" @{ worker_id = $rescuer; result = @{ answer = 'fresh, from attempt 2' } } | Out-Null
    Note "rescuer claimed attempt $($a2.attempt) and completed it"
    $late = Api POST "/v1/jobs/$job/attempts/$($a1.attempt)/complete" @{ worker_id = $zombie; result = @{ answer = 'stale, from attempt 1' } }
    Check ($late.Status -eq 409 -and $late.Body.error.code -eq 'stale_attempt') "zombie's late completion of attempt 1 -> 409 stale_attempt"
    $result = (Api GET "/v1/jobs/$job/result").Body.result
    Check ($result.answer -eq 'fresh, from attempt 2') "the stored result is still attempt 2's"
} catch {
    Check $false "demo aborted: $($_.Exception.Message)"
} finally {
    if ($Keep) { Note "stack left running (-Keep); stop with: docker compose down -v" }
    else { Invoke-Docker compose, down, -v -Capture | Out-Null; Note "stack torn down (docker compose down -v)" }
}

# --------------------------------------------------------------------- 10
Step 10 "Run the automated tests"
$testExit = 0
if ($SkipTests) { Note "skipped (-SkipTests). Run: cargo test --workspace" }
elseif (-not (Get-Command cargo -ErrorAction SilentlyContinue)) { Note "cargo not found; run 'cargo test --workspace' where Rust is installed" }
else { cargo test --workspace --locked; $testExit = $LASTEXITCODE; Check ($testExit -eq 0) "cargo test --workspace" }

Write-Host "`n$($script:Passed) checks passed, $($script:Failed.Count) failed."
if ($script:Failed.Count -gt 0) { exit 1 }
exit 0
