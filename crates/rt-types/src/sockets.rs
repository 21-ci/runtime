//! WebSockets and durable objects (docs/handlers.md "WebSockets"):
//! the shapes shared by the node's socket hub, the host layer, the executors and the
//! control API.
//!
//! The node owns every socket. A socket belongs to an **object** of a service: a durable
//! service's object is chosen in code (`durable.get(name)`); every other socket, and every
//! direct request to a durable service, uses the object [`DEFAULT_OBJECT`]. Events of one object
//! run one at a time, each as its own task.

use std::sync::Arc;

use bytes::Bytes;
use serde::{Deserialize, Serialize};

/// The object of a socket accepted without one, and of a request that reached a durable service
/// directly (not through `durable.get(name)`).
pub const DEFAULT_OBJECT: &str = "default";

/// Longest object name, in bytes.
pub const MAX_OBJECT_NAME_BYTES: usize = 256;

/// Tags one socket may carry, and the longest tag in bytes.
pub const MAX_SOCKET_TAGS: usize = 10;
pub const MAX_TAG_BYTES: usize = 256;

/// Largest socket attachment (serialized), as Cloudflare's `serializeAttachment`.
pub const MAX_ATTACHMENT_BYTES: usize = 16 * 1024;

/// Longest close reason (a close frame's payload is at most 125 bytes, 2 of them the code).
pub const MAX_CLOSE_REASON_BYTES: usize = 123;

/// The response header that carries an accept ticket from the accepting task to ingress. The
/// runtime sets it and removes it before the client sees the response.
pub const TICKET_HEADER: &str = "x-rt-ws-ticket";

/// Control characters, and the invisible or text-direction format characters that would let one
/// name look like another in listings and logs (TYPES-003).
fn hidden_char(c: char) -> bool {
    c.is_control()
        || matches!(c, '\u{00AD}' | '\u{061C}' | '\u{180E}' | '\u{200B}'..='\u{200F}' | '\u{202A}'..='\u{202E}'
            | '\u{2060}'..='\u{2064}' | '\u{2066}'..='\u{2069}' | '\u{FEFF}')
}

/// An object name: 1 to [`MAX_OBJECT_NAME_BYTES`] bytes of UTF-8 without control, invisible or
/// text-direction characters.
pub fn valid_object_name(name: &str) -> bool {
    !name.is_empty() && name.len() <= MAX_OBJECT_NAME_BYTES && !name.chars().any(hidden_char)
}

/// A socket tag: 1 to [`MAX_TAG_BYTES`] bytes without control, invisible or text-direction
/// characters.
pub fn valid_tag(tag: &str) -> bool {
    !tag.is_empty() && tag.len() <= MAX_TAG_BYTES && !tag.chars().any(hidden_char)
}

/// A socket id as the runtime makes them: `ws_` and a ULID.
pub fn valid_socket_id(id: &str) -> bool {
    id.strip_prefix("ws_")
        .is_some_and(|u| u.len() == 26 && u.bytes().all(|b| b.is_ascii_alphanumeric()))
}

/// A close code a guest may send: 1000, or 3000-4999 (application codes). The runtime itself
/// also sends 1001, 1008, 1009, 1011, 1012 and 1013.
pub fn valid_app_close_code(code: u16) -> bool {
    code == 1000 || (3000..=4999).contains(&code)
}

/// One WebSocket message.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WsData {
    Text(String),
    Binary(Bytes),
}

impl WsData {
    pub fn len(&self) -> usize {
        match self {
            WsData::Text(t) => t.len(),
            WsData::Binary(b) => b.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// The object a task acts for: its name and its place in the object's sequence of turns. Every
/// task of an object (event, or a request to a durable object) is one turn; `prev` is the turn
/// before it (0 = none). An isolate that ran turn `prev` of the object may reuse what it built
/// for it (a warm object); any other must not (another isolate ran the object in between).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectRef {
    pub name: Arc<str>,
    pub seq: u64,
    pub prev: u64,
}

impl ObjectRef {
    /// An object reference outside the turn sequence (tests, tasks the hub does not order).
    pub fn named(name: &str) -> Self {
        Self {
            name: Arc::from(name),
            seq: 0,
            prev: 0,
        }
    }
}

/// What happened on a socket (or to an object), delivered to the guest as one task.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SocketEventKind {
    /// `webSocketMessage(ws, message)`.
    Message(WsData),
    /// `webSocketClose(ws, code, reason, wasClean)`: the socket is gone.
    Close { code: u16, reason: String, was_clean: bool },
    /// `webSocketError(ws, error)`: the socket failed (protocol error, slow consumer); a close
    /// event follows.
    Error { code: String, message: String },
    /// `alarm({ scheduledTime, retryCount })` of a durable object.
    Alarm { scheduled_ms: i64, retry: u32 },
}

impl SocketEventKind {
    pub const fn label(&self) -> &'static str {
        match self {
            SocketEventKind::Message(_) => "message",
            SocketEventKind::Close { .. } => "close",
            SocketEventKind::Error { .. } => "error",
            SocketEventKind::Alarm { .. } => "alarm",
        }
    }
}

/// One event task's input.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SocketEvent {
    /// The socket the event is about (`None` for an alarm).
    pub socket: Option<SocketInfo>,
    pub kind: SocketEventKind,
    /// 1 for the first delivery; a failed handler gets the event again with the next number.
    pub attempt: u32,
}

/// A socket as guests and the control API see it (`SocketInfo` of `std:sockets`).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SocketInfo {
    pub id: String,
    pub object: String,
    pub tags: Vec<String>,
    /// The subprotocol the guest chose at accept.
    pub protocol: Option<String>,
    pub created_at_ms: i64,
    pub last_message_at_ms: Option<i64>,
    pub auto_response_at_ms: Option<i64>,
    /// Bytes queued for the client and not written yet.
    pub buffered_amount: u64,
    pub remote_ip: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_tags_ids_and_codes() {
        assert!(valid_object_name("room-42") && valid_object_name(DEFAULT_OBJECT) && valid_object_name("ü"));
        assert!(!valid_object_name("") && !valid_object_name("a\nb") && !valid_object_name(&"x".repeat(257)));
        assert!(valid_tag("user:1") && !valid_tag("") && !valid_tag(&"t".repeat(257)));
        assert!(valid_socket_id("ws_01ARZ3NDEKTSV4RRFFQ69G5FAV"));
        assert!(!valid_socket_id("ws_short") && !valid_socket_id("01ARZ3NDEKTSV4RRFFQ69G5FAV"));
        assert!(valid_app_close_code(1000) && valid_app_close_code(4000));
        assert!(!valid_app_close_code(1011) && !valid_app_close_code(5000) && !valid_app_close_code(2999));
    }

    /// TYPES-003: names and tags never carry invisible or text-direction characters (they show up
    /// in API listings and logs and could disguise one name as another).
    #[test]
    fn types_003_invisible_and_bidi_characters_are_refused() {
        for c in [
            '\u{202E}', '\u{202A}', '\u{2066}', '\u{2069}', '\u{200B}', '\u{200D}', '\u{200E}', '\u{FEFF}', '\u{00AD}',
            '\u{061C}', '\u{2060}',
        ] {
            let s = format!("room{c}42");
            assert!(!valid_object_name(&s), "{:?}", c);
            assert!(!valid_tag(&s), "{:?}", c);
        }
        assert!(valid_object_name("комната-42") && valid_tag("用户:1") && valid_object_name("🙂"));
    }

    #[test]
    fn ws_data_reports_its_byte_length_not_its_char_count() {
        assert_eq!(WsData::Text("héllo".into()).len(), 6);
        assert_eq!(WsData::Binary(Bytes::from_static(b"ab")).len(), 2);
        assert!(WsData::Text(String::new()).is_empty());
        assert!(!WsData::Binary(Bytes::from_static(b"x")).is_empty());
    }

    #[test]
    fn named_object_refs_have_no_turn_order_and_events_are_labelled() {
        let o = ObjectRef::named("room-1");
        assert_eq!((&*o.name, o.seq, o.prev), ("room-1", 0, 0));
        assert_eq!(SocketEventKind::Message(WsData::Text("x".into())).label(), "message");
        assert_eq!(
            SocketEventKind::Close {
                code: 1000,
                reason: String::new(),
                was_clean: true
            }
            .label(),
            "close"
        );
        assert_eq!(
            SocketEventKind::Error {
                code: "e".into(),
                message: "m".into()
            }
            .label(),
            "error"
        );
        assert_eq!(
            SocketEventKind::Alarm {
                scheduled_ms: 5,
                retry: 0
            }
            .label(),
            "alarm"
        );
    }
}
