//! `fs_socket` fixture (WSM-02): probes the sandbox.
//!
//! The guest asks for `wasi:filesystem` preopens, tries to open a file through `std::fs` and
//! through every preopen it finds, then starts a TCP connect to `127.0.0.1:<port>` (`?port=N`,
//! default 80). Every failure is handled by the guest, which then answers `200` with:
//!
//! ```text
//! preopens=<count>
//! fs_open=<no-preopens|denied:...|opened>
//! std_fs=<denied:...|opened>
//! connect=<denied:...|allowed>
//! ```
//!
//! Under the runtime: `preopens=0`, `fs_open=no-preopens`, `std_fs=denied:...`,
//! `connect=denied:...` (all socket addresses are denied).

#[allow(dead_code)]
mod http {
    include!("../../_shared/http.rs");
}

use wasip2::exports::http::incoming_handler::Guest;
use wasip2::filesystem::types::{DescriptorFlags, OpenFlags, PathFlags};
use wasip2::http::types::{IncomingRequest, ResponseOutparam};
use wasip2::sockets::network::{IpAddressFamily, IpSocketAddress, Ipv4SocketAddress};

struct Component;

wasip2::http::proxy::export!(Component);

fn probe_filesystem() -> (usize, String, String) {
    let dirs = wasip2::filesystem::preopens::get_directories();
    let count = dirs.len();
    let fs_open = match dirs.first() {
        None => "no-preopens".to_string(),
        Some((dir, _name)) => {
            match dir.open_at(PathFlags::empty(), "etc/passwd", OpenFlags::empty(), DescriptorFlags::READ) {
                Ok(_) => "opened".to_string(),
                Err(e) => format!("denied:{e:?}"),
            }
        }
    };
    let std_fs = match std::fs::File::open("/etc/passwd") {
        Ok(_) => "opened".to_string(),
        Err(e) => format!("denied:{:?}", e.kind()),
    };
    (count, fs_open, std_fs)
}

fn probe_connect(port: u16) -> String {
    let network = wasip2::sockets::instance_network::instance_network();
    let socket = match wasip2::sockets::tcp_create_socket::create_tcp_socket(IpAddressFamily::Ipv4) {
        Ok(s) => s,
        Err(e) => return format!("denied:create:{e:?}"),
    };
    let addr = IpSocketAddress::Ipv4(Ipv4SocketAddress { port, address: (127, 0, 0, 1) });
    match socket.start_connect(&network, addr) {
        Ok(()) => "allowed".to_string(),
        Err(e) => format!("denied:{e:?}"),
    }
}

impl Guest for Component {
    fn handle(request: IncomingRequest, response_out: ResponseOutparam) {
        let pq = http::path_and_query(&request);
        let (_path, query) = http::split_query(&pq);
        let port: u16 = http::query_param(query, "port").and_then(|v| v.parse().ok()).unwrap_or(80);

        let (count, fs_open, std_fs) = probe_filesystem();
        let connect = probe_connect(port);
        let body = format!("preopens={count}\nfs_open={fs_open}\nstd_fs={std_fs}\nconnect={connect}\n");
        http::respond_text(response_out, 200, &body);
    }
}
