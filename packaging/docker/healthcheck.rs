// rt-healthcheck: the container HEALTHCHECK probe for images without a shell or curl (distroless).
//
//   rt-healthcheck [ADDR] [PATH]     defaults: $RT_HEALTHCHECK_ADDR or 127.0.0.1:9090, /healthz
//
// Sends `GET PATH HTTP/1.0` to ADDR and exits 0 on a 2xx status, 1 otherwise. std only, built with
// `rustc` in the image's build stage; it is not part of the Cargo workspace.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::process::ExitCode;
use std::time::Duration;

fn probe(addr: &str, path: &str) -> Result<u16, String> {
    let sock: SocketAddr = addr.parse().map_err(|e| format!("bad address {addr}: {e}"))?;
    let timeout = Duration::from_secs(2);
    let mut stream = TcpStream::connect_timeout(&sock, timeout).map_err(|e| format!("connect {addr}: {e}"))?;
    stream.set_read_timeout(Some(timeout)).map_err(|e| e.to_string())?;
    stream.set_write_timeout(Some(timeout)).map_err(|e| e.to_string())?;
    let request = format!("GET {path} HTTP/1.0\r\nHost: {addr}\r\nUser-Agent: rt-healthcheck\r\n\r\n");
    stream.write_all(request.as_bytes()).map_err(|e| format!("write: {e}"))?;
    let mut head = [0u8; 64];
    let n = stream.read(&mut head).map_err(|e| format!("read: {e}"))?;
    // "HTTP/1.1 200 OK": the status code is the second space-separated field.
    let line = String::from_utf8_lossy(&head[..n]);
    let code = line.split_whitespace().nth(1).ok_or("no status line")?;
    code.parse::<u16>().map_err(|_| format!("bad status line: {line:?}"))
}

fn main() -> ExitCode {
    let mut args = std::env::args().skip(1);
    let addr = args
        .next()
        .or_else(|| std::env::var("RT_HEALTHCHECK_ADDR").ok())
        .unwrap_or_else(|| "127.0.0.1:9090".to_string());
    let path = args.next().unwrap_or_else(|| "/healthz".to_string());
    match probe(&addr, &path) {
        Ok(code) if (200..300).contains(&code) => ExitCode::SUCCESS,
        Ok(code) => {
            eprintln!("rt-healthcheck: {addr}{path} answered {code}");
            ExitCode::FAILURE
        }
        Err(e) => {
            eprintln!("rt-healthcheck: {e}");
            ExitCode::FAILURE
        }
    }
}
