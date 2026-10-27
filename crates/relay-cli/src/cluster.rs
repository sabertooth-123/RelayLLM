//! A local cluster of real `relay` processes (one coordinator, N workers),
//! spawned from this same binary. Used by `relay demo` and `relay bench`.
//! Processes are hard-killed when the cluster is dropped, even on panic.

use std::fs::File;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use anyhow::{Context, bail};
use relay_client::RelayClient;

pub struct Cluster {
    exe: PathBuf,
    dir: PathBuf,
    port: u16,
    coordinator_args: Vec<String>,
    coordinator: Option<Child>,
    workers: Vec<(String, Child)>,
    /// Bumped per coordinator start, so restarts get separate log files.
    coordinator_runs: u32,
}

impl Cluster {
    /// `coordinator_args` are extra flags (lease, retry, ...); the database
    /// and bind address are managed here.
    pub fn new(dir: &Path, coordinator_args: &[&str]) -> anyhow::Result<Self> {
        std::fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
        let port = std::net::TcpListener::bind("127.0.0.1:0")?
            .local_addr()?
            .port();
        Ok(Self {
            exe: std::env::current_exe().context("locating the relay binary")?,
            dir: dir.to_owned(),
            port,
            coordinator_args: coordinator_args.iter().map(|s| (*s).to_owned()).collect(),
            coordinator: None,
            workers: Vec::new(),
            coordinator_runs: 0,
        })
    }

    pub fn url(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    pub fn coordinator_pid(&self) -> Option<u32> {
        self.coordinator.as_ref().map(Child::id)
    }

    fn spawn(&self, log_name: &str, args: &[String]) -> anyhow::Result<Child> {
        let log = File::create(self.dir.join(log_name))?;
        Command::new(&self.exe)
            .args(args)
            .env("RELAY_LOG", "info")
            .env_remove("RELAY_URL")
            .stdin(Stdio::null())
            .stdout(log.try_clone()?)
            .stderr(log)
            .spawn()
            .with_context(|| format!("starting {log_name}"))
    }

    /// Start the coordinator on the cluster's port and database, and wait
    /// until it answers `/healthz`.
    pub async fn start_coordinator(&mut self) -> anyhow::Result<()> {
        self.coordinator_runs += 1;
        let mut args = vec![
            "coordinator".to_owned(),
            "--db".to_owned(),
            self.dir.join("relay.db").display().to_string(),
            "--bind".to_owned(),
            format!("127.0.0.1:{}", self.port),
        ];
        args.extend(self.coordinator_args.iter().cloned());
        let log = format!("coordinator-{}.log", self.coordinator_runs);
        self.coordinator = Some(self.spawn(&log, &args)?);

        let client = RelayClient::with_timeout(self.url(), Duration::from_secs(2))?;
        let deadline = Instant::now() + Duration::from_secs(20);
        while client.health().await.is_err() {
            if let Some(child) = &mut self.coordinator {
                if let Some(status) = child.try_wait()? {
                    bail!(
                        "coordinator exited with {status}; see {}",
                        self.dir.join(&log).display()
                    );
                }
            }
            if Instant::now() > deadline {
                bail!(
                    "coordinator not healthy after 20 s; see {}",
                    self.dir.join(&log).display()
                );
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        Ok(())
    }

    /// Hard kill (TerminateProcess / SIGKILL): no drain, no cleanup.
    pub fn kill_coordinator(&mut self) -> anyhow::Result<()> {
        if let Some(mut child) = self.coordinator.take() {
            child.kill()?;
            child.wait()?;
        }
        Ok(())
    }

    /// Start a worker named `name` (its id will be `name-<uuid>`).
    pub fn start_worker(&mut self, name: &str, extra: &[&str]) -> anyhow::Result<()> {
        let mut args = vec![
            "--server".to_owned(),
            self.url(),
            "worker".to_owned(),
            "--name".to_owned(),
            name.to_owned(),
        ];
        args.extend(extra.iter().map(|s| (*s).to_owned()));
        let child = self.spawn(&format!("{name}.log"), &args)?;
        self.workers.push((name.to_owned(), child));
        Ok(())
    }

    /// Hard kill a worker mid-flight.
    pub fn kill_worker(&mut self, name: &str) -> anyhow::Result<()> {
        let index = self
            .workers
            .iter()
            .position(|(n, _)| n == name)
            .with_context(|| format!("no worker named {name}"))?;
        let (_, mut child) = self.workers.remove(index);
        child.kill()?;
        child.wait()?;
        Ok(())
    }

    /// Peak resident memory of the coordinator process, from the OS:
    /// `PeakWorkingSet64` on Windows, `VmHWM` on Linux. `None` if the
    /// platform isn't supported or the query fails; never a guess.
    pub fn coordinator_peak_memory_bytes(&self) -> Option<u64> {
        let pid = self.coordinator_pid()?;
        peak_memory_bytes(pid)
    }
}

#[cfg(windows)]
fn peak_memory_bytes(pid: u32) -> Option<u64> {
    let output = Command::new("powershell")
        .args([
            "-NoProfile",
            "-NonInteractive",
            "-Command",
            &format!("(Get-Process -Id {pid}).PeakWorkingSet64"),
        ])
        .output()
        .ok()?;
    String::from_utf8_lossy(&output.stdout).trim().parse().ok()
}

#[cfg(target_os = "linux")]
fn peak_memory_bytes(pid: u32) -> Option<u64> {
    let status = std::fs::read_to_string(format!("/proc/{pid}/status")).ok()?;
    let kb: u64 = status
        .lines()
        .find(|l| l.starts_with("VmHWM:"))?
        .split_whitespace()
        .nth(1)?
        .parse()
        .ok()?;
    Some(kb * 1024)
}

#[cfg(not(any(windows, target_os = "linux")))]
fn peak_memory_bytes(_pid: u32) -> Option<u64> {
    None
}

impl Drop for Cluster {
    fn drop(&mut self) {
        for (_, child) in &mut self.workers {
            let _ = child.kill();
            let _ = child.wait();
        }
        let _ = self.kill_coordinator();
    }
}
