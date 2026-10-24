//! Phase-0 spike of the WebSocket plan (docs/plan/2026-10-22-websockets-durable.md): memory per
//! idle held socket with the socket-task shape the node uses (one tokio task per socket that
//! selects over the codec and an outbound channel, a small entry in a shared map, a ping timer).
//!
//! ```text
//! cargo run -p rt-triggers --example ws_spike -- server 127.0.0.1:9400 [read_buffer_bytes]
//! cargo run -p rt-triggers --example ws_spike -- client 127.0.0.1:9400 10000
//! ```
//!
//! The server prints its RSS (`ps -o rss`) and the socket count every 2 s; the client opens and
//! holds `n` sockets. Results are in the plan (section 11, phase 0).
//!
//! Against a real node (`bench/ws.sh`): `hold <ws-url> <n>` opens and holds `n` sockets of a
//! service; `latency <ws-url> <n>` sends `n` messages one after another on one socket and prints
//! the echo round-trip percentiles.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tokio_tungstenite::tungstenite::Message;

struct Entry {
    _tags: Vec<String>,
    _out: tokio::sync::mpsc::UnboundedSender<Message>,
}

fn rss_kib() -> u64 {
    let pid = std::process::id().to_string();
    std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &pid])
        .output()
        .ok()
        .and_then(|o| String::from_utf8_lossy(&o.stdout).trim().parse().ok())
        .unwrap_or(0)
}

async fn server(addr: &str, read_buf: usize) {
    let listener = TcpListener::bind(addr).await.expect("bind");
    let live = Arc::new(AtomicUsize::new(0));
    let map: Arc<dashmap::DashMap<u64, Entry>> = Arc::new(dashmap::DashMap::new());
    {
        let live = live.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(2)).await;
                println!("sockets={} rss_kib={}", live.load(Ordering::Relaxed), rss_kib());
            }
        });
    }
    let mut next = 0u64;
    loop {
        let Ok((stream, _)) = listener.accept().await else {
            continue;
        };
        next += 1;
        let id = next;
        let (live, map) = (live.clone(), map.clone());
        tokio::spawn(async move {
            let cfg = WebSocketConfig::default()
                .read_buffer_size(read_buf)
                .write_buffer_size(0)
                .max_message_size(Some(1 << 20))
                .max_frame_size(Some(1 << 20));
            let Ok(mut ws) = tokio_tungstenite::accept_async_with_config(stream, Some(cfg)).await else {
                return;
            };
            let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
            map.insert(
                id,
                Entry {
                    _tags: vec!["room:1".into()],
                    _out: tx,
                },
            );
            live.fetch_add(1, Ordering::Relaxed);
            let mut ping = tokio::time::interval(Duration::from_secs(30));
            loop {
                tokio::select! {
                    m = ws.next() => match m {
                        Some(Ok(Message::Text(t))) => { let _ = ws.send(Message::Text(t)).await; }
                        Some(Ok(_)) => {}
                        _ => break,
                    },
                    Some(out) = rx.recv() => { let _ = ws.send(out).await; }
                    _ = ping.tick() => { let _ = ws.send(Message::Ping(Default::default())).await; }
                }
            }
            map.remove(&id);
            live.fetch_sub(1, Ordering::Relaxed);
        });
    }
}

async fn client(addr: &str, n: usize) {
    hold(&format!("ws://{addr}/"), n).await
}

async fn hold(url: &str, n: usize) {
    let addr = url
        .trim_start_matches("ws://")
        .split('/')
        .next()
        .unwrap_or_default()
        .to_string();
    let mut held = Vec::with_capacity(n);
    for i in 0..n {
        let stream = match TcpStream::connect(&addr).await {
            Ok(s) => s,
            Err(e) => {
                eprintln!("connect {i}: {e}");
                break;
            }
        };
        match tokio_tungstenite::client_async(url, stream).await {
            Ok((ws, _)) => held.push(ws),
            Err(e) => {
                eprintln!("handshake {i}: {e}");
                break;
            }
        }
    }
    println!("holding {} sockets", held.len());
    tokio::time::sleep(Duration::from_secs(3600)).await;
}

async fn latency(url: &str, n: usize) {
    let addr = url
        .trim_start_matches("ws://")
        .split('/')
        .next()
        .unwrap_or_default()
        .to_string();
    let stream = TcpStream::connect(&addr).await.expect("connect");
    stream.set_nodelay(true).expect("nodelay");
    let (mut ws, _) = tokio_tungstenite::client_async(url, stream).await.expect("handshake");
    let mut rtt = Vec::with_capacity(n);
    for i in 0..n {
        let t = std::time::Instant::now();
        ws.send(Message::text(format!("m{i}"))).await.expect("send");
        loop {
            match ws.next().await {
                Some(Ok(Message::Text(_))) => break,
                Some(Ok(_)) => continue,
                other => panic!("echo: {other:?}"),
            }
        }
        rtt.push(t.elapsed().as_secs_f64() * 1000.0);
    }
    rtt.sort_by(f64::total_cmp);
    let at = |q: f64| rtt[((rtt.len() as f64 * q) as usize).min(rtt.len() - 1)];
    println!("n={n} p50={:.2}ms p90={:.2}ms p99={:.2}ms", at(0.5), at(0.9), at(0.99));
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    match args.get(1).map(String::as_str) {
        Some("server") => server(&args[2], args.get(3).and_then(|s| s.parse().ok()).unwrap_or(4096)).await,
        Some("client") => client(&args[2], args[3].parse().expect("n")).await,
        Some("hold") => hold(&args[2], args[3].parse().expect("n")).await,
        Some("latency") => latency(&args[2], args[3].parse().expect("n")).await,
        _ => eprintln!("usage: ws_spike server <addr> [read_buf] | client <addr> <n>"),
    }
}
