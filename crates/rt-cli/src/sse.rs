//! Minimal Server-Sent Events reader (the control API streams `logs?follow=1` this way).

use std::io::{BufRead, Read};

#[derive(Debug, Default, PartialEq, Eq)]
pub struct Event {
    pub id: Option<String>,
    /// `message` when the stream names no event.
    pub event: String,
    pub data: String,
}

/// Largest event (and line) read: the runtime's log records are far smaller; a stream that sends
/// more is broken or hostile and ends with an error instead of growing memory (CLINET-003).
pub const MAX_EVENT_BYTES: usize = 8 * 1024 * 1024;

/// Feed every complete event to `on_event` until the stream ends (`Ok`) or `on_event` fails.
/// Comments (`: keep-alive`) and events without data are skipped; an event cut off by the end
/// of the stream is dropped, as the SSE spec says.
pub fn read_events<R, E>(mut r: R, mut on_event: impl FnMut(Event) -> Result<(), E>) -> Result<(), ReadError<E>>
where
    R: BufRead,
{
    let mut ev = Event::default();
    let mut has_data = false;
    let mut line = String::new();
    loop {
        line.clear();
        let n = (&mut r)
            .take(MAX_EVENT_BYTES as u64 + 2)
            .read_line(&mut line)
            .map_err(ReadError::Io)?;
        if n == 0 {
            return Ok(());
        }
        if line.len() > MAX_EVENT_BYTES + 1 || ev.data.len() + line.len() > MAX_EVENT_BYTES + 8 {
            return Err(ReadError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "an event is larger than 8 MiB",
            )));
        }
        let l = line.trim_end_matches(['\n', '\r']);
        if l.is_empty() {
            if has_data {
                let done = std::mem::take(&mut ev);
                has_data = false;
                on_event(Event {
                    event: if done.event.is_empty() {
                        "message".into()
                    } else {
                        done.event
                    },
                    ..done
                })
                .map_err(ReadError::Handler)?;
            } else {
                ev = Event::default();
            }
            continue;
        }
        if l.starts_with(':') {
            continue;
        }
        let (field, value) = match l.split_once(':') {
            Some((f, v)) => (f, v.strip_prefix(' ').unwrap_or(v)),
            None => (l, ""),
        };
        match field {
            "event" => ev.event = value.to_string(),
            "id" => ev.id = Some(value.to_string()),
            "data" => {
                if has_data {
                    ev.data.push('\n');
                }
                ev.data.push_str(value);
                has_data = true;
            }
            _ => {}
        }
    }
}

#[derive(Debug)]
pub enum ReadError<E> {
    Io(std::io::Error),
    Handler(E),
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(s: &str) -> Vec<Event> {
        let mut out = Vec::new();
        read_events::<_, ()>(s.as_bytes(), |e| {
            out.push(e);
            Ok(())
        })
        .unwrap();
        out
    }

    #[test]
    fn parses_id_event_and_data() {
        let evs = parse("id: 1\nevent: log\ndata: {\"a\":1}\n\nid: 2\ndata: x\n\n");
        assert_eq!(
            evs,
            vec![
                Event {
                    id: Some("1".into()),
                    event: "log".into(),
                    data: "{\"a\":1}".into()
                },
                Event {
                    id: Some("2".into()),
                    event: "message".into(),
                    data: "x".into()
                },
            ]
        );
    }

    #[test]
    fn skips_comments_and_dataless_events_and_handles_crlf() {
        let evs = parse(": keep-alive\n\nevent: ping\n\r\nevent: log\r\ndata: a\r\n\r\n");
        assert_eq!(
            evs,
            vec![Event {
                id: None,
                event: "log".into(),
                data: "a".into()
            }]
        );
    }

    #[test]
    fn multi_line_data_is_joined_with_newlines() {
        let evs = parse("data: one\ndata: two\n\n");
        assert_eq!(evs[0].data, "one\ntwo");
    }

    #[test]
    fn a_truncated_event_is_dropped() {
        assert!(parse("data: cut off").is_empty());
    }

    /// CLINET-003: chunk boundaries anywhere (a 1-byte read buffer), `retry:` and unknown fields
    /// ignored, and an event or a line larger than the bound ends the stream with an error
    /// instead of growing memory without limit.
    #[test]
    fn clinet_003_chunking_unknown_fields_and_the_size_bound() {
        let text = "retry: 3000\r\nid: 7\r\nda";
        let rest = "ta: {\"m\":\"x\"}\r\n: keepalive\r\nfoo: bar\r\n\r\n";
        let joined = format!("{text}{rest}");
        let mut out = Vec::new();
        read_events::<_, ()>(std::io::BufReader::with_capacity(1, joined.as_bytes()), |e| {
            out.push(e);
            Ok(())
        })
        .unwrap();
        assert_eq!(
            out,
            vec![Event {
                id: Some("7".into()),
                event: "message".into(),
                data: "{\"m\":\"x\"}".into()
            }]
        );

        let huge_line = format!("data: {}\n\n", "x".repeat(MAX_EVENT_BYTES + 1));
        assert!(matches!(
            read_events::<_, ()>(huge_line.as_bytes(), |_| Ok(())),
            Err(ReadError::Io(_))
        ));
        let many_lines = "data: aaaaaaaaaa\n".repeat(MAX_EVENT_BYTES / 10 + 10);
        assert!(matches!(
            read_events::<_, ()>(many_lines.as_bytes(), |_| Ok(())),
            Err(ReadError::Io(_))
        ));
    }

    #[test]
    fn handler_errors_stop_the_stream() {
        let r = read_events("data: 1\n\ndata: 2\n\n".as_bytes(), |_| Err::<(), _>("stop"));
        assert!(matches!(r, Err(ReadError::Handler("stop"))));
    }
}
