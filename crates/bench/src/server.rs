//! Spawning `matier-bench-server` as a child process (used by `matrix --spawn-server`).

use std::{
    io::{Read, Write},
    net::TcpStream,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail};

/// A running server child process; killed when the last handle is dropped or `kill` is called.
#[derive(Clone)]
pub struct SpawnedServer(Arc<Mutex<Child>>);

impl Drop for SpawnedServer {
    fn drop(&mut self) {
        if Arc::strong_count(&self.0) == 1 {
            self.kill();
        }
    }
}

impl SpawnedServer {
    /// Starts the server and blocks until `/health` answers (and the cert file exists, if any).
    ///
    /// `bin` defaults to `matier-bench-server` next to the current executable.
    /// `tls_addr` of `None` disables the TLS listener.
    pub fn spawn(
        bin: Option<&Path>,
        addr: &str,
        tls_addr: Option<&str>,
        cert: &Path,
    ) -> Result<Self> {
        let bin = match bin {
            Some(b) => b.to_owned(),
            None => {
                let exe = std::env::current_exe()?;
                exe.with_file_name(format!(
                    "matier-bench-server{}",
                    std::env::consts::EXE_SUFFIX
                ))
            }
        };
        if let Some(dir) = cert.parent().filter(|d| !d.as_os_str().is_empty()) {
            std::fs::create_dir_all(dir)?;
        }
        let _ = std::fs::remove_file(cert);
        let child = Command::new(&bin)
            .args([
                "--addr",
                addr,
                "--tls-addr",
                tls_addr.unwrap_or("off"),
                "--cert-out",
            ])
            .arg(cert)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .spawn()
            .with_context(|| format!("starting {bin:?} (use --server-bin)"))?;
        let server = Self(Arc::new(Mutex::new(child)));

        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            let cert_ready =
                tls_addr.is_none() || std::fs::metadata(cert).is_ok_and(|m| m.len() > 0);
            if cert_ready && health(addr) {
                return Ok(server);
            }
            if let Some(status) = server.0.lock().unwrap().try_wait()? {
                bail!("server exited early: {status}");
            }
            if Instant::now() > deadline {
                bail!("server did not become healthy on {addr}");
            }
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    /// Kills the child (idempotent).
    pub fn kill(&self) {
        if let Ok(mut c) = self.0.lock() {
            let _ = c.kill();
            let _ = c.wait();
        }
    }
}

/// Minimal blocking `GET /health` probe.
fn health(addr: &str) -> bool {
    let probe = || -> std::io::Result<bool> {
        let mut s = TcpStream::connect(addr)?;
        s.set_read_timeout(Some(Duration::from_secs(1)))?;
        s.write_all(format!("GET /health HTTP/1.0\r\nHost: {addr}\r\n\r\n").as_bytes())?;
        let mut buf = String::new();
        let _ = s.read_to_string(&mut buf);
        Ok(buf.starts_with("HTTP/1.") && buf.contains(" 200"))
    };
    probe().unwrap_or(false)
}

/// `host:port` of a base URL like `http://127.0.0.1:8080`.
pub fn authority(url: &str) -> Result<String> {
    let uri: http::Uri = url.parse()?;
    uri.authority()
        .map(|a| a.to_string())
        .with_context(|| format!("no host:port in {url:?}"))
}

/// Default cert location shared with `scripts/bench.*`.
pub fn default_cert() -> PathBuf {
    PathBuf::from("target/bench-server-cert.pem")
}
