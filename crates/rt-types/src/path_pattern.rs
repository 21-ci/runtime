//! The one path pattern syntax of the platform, and the canonical form of a request path.
//!
//! A pattern starts with `/`. `*` matches within one segment (at most one per segment), `**` is a
//! whole segment and matches zero or more segments, `:name` matches one segment. Everything else
//! is literal and matches exactly (case-sensitive; [`PathPattern::matches_folded`] ignores ASCII
//! case). Matching allocates nothing and costs at most segments of path × segments of pattern,
//! as only the latest `**` is ever retried.

use std::borrow::Cow;

/// Longest pattern, in bytes.
pub const MAX_PATTERN_LEN: usize = 256;
/// Most segments in a pattern.
pub const MAX_PATTERN_SEGMENTS: usize = 32;
/// Most `**` segments in a pattern.
pub const MAX_PATTERN_DOUBLE_STARS: usize = 3;
/// Percent-decoding passes before a path counts as malformed.
const DECODE_ROUNDS: usize = 3;

#[derive(Clone, Debug, PartialEq, Eq)]
enum Seg {
    Literal(Box<[u8]>),
    /// `*` or `:name`.
    Any,
    /// `prefix*suffix`.
    Glob(Box<[u8]>, Box<[u8]>),
    /// `**`.
    Deep,
}

impl Seg {
    fn eq(&self, s: &[u8], fold: bool) -> bool {
        let same = |a: &[u8], b: &[u8]| if fold { a.eq_ignore_ascii_case(b) } else { a == b };
        match self {
            Seg::Literal(l) => same(l, s),
            Seg::Any => !s.is_empty(),
            Seg::Glob(p, x) => {
                s.len() >= p.len() + x.len() && same(p, &s[..p.len()]) && same(x, &s[s.len() - x.len()..])
            }
            Seg::Deep => false,
        }
    }
}

/// A compiled pattern.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PathPattern {
    segs: Box<[Seg]>,
}

impl PathPattern {
    /// Compile `pattern`; the error says what is wrong with it, for an API caller.
    pub fn parse(pattern: &str) -> Result<Self, String> {
        if pattern.len() > MAX_PATTERN_LEN {
            return Err(format!("longer than {MAX_PATTERN_LEN} bytes"));
        }
        let Some(rest) = pattern.strip_prefix('/') else {
            return Err("must start with `/`".into());
        };
        if pattern
            .bytes()
            .any(|b| b.is_ascii_control() || matches!(b, b' ' | b'\\' | b'%' | b'?' | b'#'))
        {
            return Err("must be a decoded path: no spaces, controls, `\\`, `%`, `?` or `#`".into());
        }
        let mut segs = Vec::new();
        if !rest.is_empty() {
            for s in rest.split('/') {
                segs.push(parse_segment(s)?);
            }
        }
        if segs.len() > MAX_PATTERN_SEGMENTS {
            return Err(format!("more than {MAX_PATTERN_SEGMENTS} segments"));
        }
        if segs.iter().filter(|s| **s == Seg::Deep).count() > MAX_PATTERN_DOUBLE_STARS {
            return Err(format!("more than {MAX_PATTERN_DOUBLE_STARS} `**`"));
        }
        Ok(Self { segs: segs.into() })
    }

    /// Matches every path: what a stored rule that no longer compiles falls back to.
    pub fn everything() -> Self {
        Self {
            segs: Box::new([Seg::Deep]),
        }
    }

    pub fn matches(&self, path: &[u8]) -> bool {
        self.run(path, false)
    }

    /// As [`Self::matches`], ignoring ASCII case.
    pub fn matches_folded(&self, path: &[u8]) -> bool {
        self.run(path, true)
    }

    fn run(&self, path: &[u8], fold: bool) -> bool {
        if path.first() != Some(&b'/') {
            return false;
        }
        let segs = &self.segs;
        let (mut si, mut pos) = (0usize, 1usize);
        // Where to resume after the latest `**`: pattern index and path position.
        let mut retry: Option<(usize, usize)> = None;
        loop {
            if segs.get(si) == Some(&Seg::Deep) {
                si += 1;
                retry = Some((si, pos));
                continue;
            }
            if pos >= path.len() {
                return segs[si..].iter().all(|s| *s == Seg::Deep);
            }
            let end = segment_end(path, pos);
            if segs.get(si).is_some_and(|s| s.eq(&path[pos..end], fold)) {
                si += 1;
                pos = end + 1;
                continue;
            }
            let Some((resume, from)) = retry else {
                return false;
            };
            if from >= path.len() {
                return false;
            }
            let next = segment_end(path, from) + 1;
            retry = Some((resume, next));
            si = resume;
            pos = next;
        }
    }
}

fn segment_end(path: &[u8], from: usize) -> usize {
    path[from..]
        .iter()
        .position(|b| *b == b'/')
        .map_or(path.len(), |i| from + i)
}

fn parse_segment(s: &str) -> Result<Seg, String> {
    match s {
        "" => return Err("has an empty segment (`//` or a trailing `/`)".into()),
        "." | ".." => return Err("has a dot segment".into()),
        "*" => return Ok(Seg::Any),
        "**" => return Ok(Seg::Deep),
        _ => {}
    }
    if let Some(name) = s.strip_prefix(':') {
        let mut chars = name.bytes();
        let starts = chars.next().is_some_and(|b| b.is_ascii_alphabetic() || b == b'_');
        if !starts || !chars.all(|b| b.is_ascii_alphanumeric() || b == b'_') {
            return Err(format!("`{s}` is not a valid `:name`"));
        }
        return Ok(Seg::Any);
    }
    match s.split_once('*') {
        None => Ok(Seg::Literal(s.as_bytes().into())),
        Some((_, x)) if x.contains('*') => Err(format!("`{s}`: at most one `*` per segment, and `**` stands alone")),
        Some((p, x)) => Ok(Seg::Glob(p.as_bytes().into(), x.as_bytes().into())),
    }
}

/// A path no canonical form exists for: not absolute, bad or too deeply nested percent-encoding,
/// or a control byte once decoded.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MalformedPath;

/// `path` as any lenient reader could take it: percent-decoded repeatedly, `\` as `/`, dot segments
/// resolved, empty segments and the trailing slash gone. Over-matching a limit is safe; a form that
/// escapes one is not. Case is left to the matcher. An already canonical path is not copied.
pub fn canonical_path(path: &str) -> Result<Cow<'_, [u8]>, MalformedPath> {
    let raw = path.as_bytes();
    if raw.first() != Some(&b'/') {
        return Err(MalformedPath);
    }
    if is_canonical(raw) {
        return Ok(Cow::Borrowed(raw));
    }
    let mut bytes = decode_once(raw, true)?;
    let mut rounds = 1;
    while has_escape(&bytes) {
        if rounds == DECODE_ROUNDS {
            return Err(MalformedPath);
        }
        bytes = decode_once(&bytes, false)?;
        rounds += 1;
    }
    if bytes.iter().any(u8::is_ascii_control) {
        return Err(MalformedPath);
    }
    let mut out: Vec<&[u8]> = Vec::new();
    for seg in bytes.split(|b| *b == b'/' || *b == b'\\') {
        match seg {
            b"" | b"." => {}
            b".." => {
                out.pop();
            }
            s => out.push(s),
        }
    }
    if out.is_empty() {
        return Ok(Cow::Owned(vec![b'/']));
    }
    let mut joined = Vec::with_capacity(bytes.len());
    for s in out {
        joined.push(b'/');
        joined.extend_from_slice(s);
    }
    Ok(Cow::Owned(joined))
}

fn is_canonical(raw: &[u8]) -> bool {
    if raw == b"/" {
        return true;
    }
    if raw.last() == Some(&b'/') {
        return false;
    }
    let mut prev = 0u8;
    for &b in raw {
        if b == b'%' || b == b'\\' || b.is_ascii_control() || (prev == b'/' && (b == b'/' || b == b'.')) {
            return false;
        }
        prev = b;
    }
    true
}

fn hex(b: u8) -> Option<u8> {
    (b as char).to_digit(16).map(|d| d as u8)
}

fn escape_at(bytes: &[u8], i: usize) -> Option<u8> {
    Some(hex(*bytes.get(i + 1)?)? << 4 | hex(*bytes.get(i + 2)?)?)
}

fn has_escape(bytes: &[u8]) -> bool {
    (0..bytes.len()).any(|i| bytes[i] == b'%' && escape_at(bytes, i).is_some())
}

/// One percent-decoding pass. `strict`: a `%` that starts no escape is an error (the wire form);
/// later passes keep it, as `%25` legitimately leaves one.
fn decode_once(bytes: &[u8], strict: bool) -> Result<Vec<u8>, MalformedPath> {
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            match escape_at(bytes, i) {
                Some(b) => {
                    out.push(b);
                    i += 3;
                    continue;
                }
                None if strict => return Err(MalformedPath),
                None => {}
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    Ok(out)
}
