//! The socket hub (docs/handlers.md "WebSockets"): the node owns every WebSocket.
//!
//! - **Accept.** A guest's `fetch` calls `sockets.accept(req)`: the hub reserves a socket (plan
//!   and node caps) and hands back a ticket. The task's response carries the ticket to ingress
//!   (`x-rt-ws-ticket`, removed before the client sees it), which answers `101`, upgrades the
//!   connection and binds it to the reserved socket ([`SocketHub::claim`], [`SocketHub::bind_io`]).
//! - **Events.** One tokio task per socket reads frames (size cap, rate limit, ping/pong, the
//!   object's auto-response) and queues an event in its object's inbox. The hub runs one task per
//!   event, one object at a time, in order ([`SocketTaskFactory::event_task`] builds it). A failed
//!   event is delivered again with backoff (at-least-once); after the last attempt a message's
//!   socket is closed with 1011 and the event is logged.
//! - **Turns.** A request to a durable object waits for its turn in the same inbox
//!   ([`SocketHub::turn`]), so an object's events and requests never overlap.
//! - **Sends** from guests arrive through [`rt_host::SocketHost`] and are written by the socket's
//!   task; a client that does not read is closed with 1013 once its buffer is full.

mod conn;
mod hub;

pub use hub::{
    Claim, HubConfig, HubStats, ObjectTurn, SocketHub, SocketLimits, SocketTaskFactory, Submitter, TurnError,
};

/// `Sec-WebSocket-Accept` for a client's `Sec-WebSocket-Key` (RFC 6455 §4.2.2).
pub fn accept_key(key: &[u8]) -> String {
    tokio_tungstenite::tungstenite::handshake::derive_accept_key(key)
}

/// A WebSocket upgrade request (RFC 6455 §4.1): `GET`, `Upgrade: websocket`, `Connection` naming
/// `upgrade`, a `Sec-WebSocket-Key` and version 13.
pub fn is_upgrade_request(method: &http::Method, headers: &http::HeaderMap) -> bool {
    let has_token = |name: http::header::HeaderName, token: &str| {
        headers
            .get_all(name)
            .iter()
            .filter_map(|v| v.to_str().ok())
            .flat_map(|v| v.split(','))
            .any(|t| t.trim().eq_ignore_ascii_case(token))
    };
    method == http::Method::GET
        && has_token(http::header::UPGRADE, "websocket")
        && has_token(http::header::CONNECTION, "upgrade")
        && headers.contains_key(http::header::SEC_WEBSOCKET_KEY)
        && headers
            .get(http::header::SEC_WEBSOCKET_VERSION)
            .is_some_and(|v| v.as_bytes() == b"13")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_accept_key_is_the_rfc_example() {
        assert_eq!(accept_key(b"dGhlIHNhbXBsZSBub25jZQ=="), "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=");
    }

    #[test]
    fn an_upgrade_request_needs_every_part() {
        let mut h = http::HeaderMap::new();
        h.insert(http::header::UPGRADE, "WebSocket".parse().unwrap());
        h.insert(http::header::CONNECTION, "keep-alive, Upgrade".parse().unwrap());
        h.insert(
            http::header::SEC_WEBSOCKET_KEY,
            "dGhlIHNhbXBsZSBub25jZQ==".parse().unwrap(),
        );
        h.insert(http::header::SEC_WEBSOCKET_VERSION, "13".parse().unwrap());
        assert!(is_upgrade_request(&http::Method::GET, &h));
        assert!(!is_upgrade_request(&http::Method::POST, &h));
        let mut v8 = h.clone();
        v8.insert(http::header::SEC_WEBSOCKET_VERSION, "8".parse().unwrap());
        assert!(!is_upgrade_request(&http::Method::GET, &v8));
        let mut no_key = h.clone();
        no_key.remove(http::header::SEC_WEBSOCKET_KEY);
        assert!(!is_upgrade_request(&http::Method::GET, &no_key));
    }
}
