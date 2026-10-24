//! SQL values between V8 and SQLite without a serde round trip (shared by `std:sql` in `ops.rs`
//! and `std:sqlite` in `ops_sqlite.rs`).
//!
//! Arguments are read straight from the JS values ([`Bind`]), which JS snapshots first so that no
//! guest code runs inside an op; results are built straight as JS values (own data properties): `NULL` -> `null`, `INTEGER` -> number (`bigint` for `safeIntegers`), `REAL` -> number,
//! `TEXT` -> string, `BLOB` -> `Uint8Array`.

use std::convert::Infallible;

use deno_core::{v8, FromV8, ToV8};
use deno_error::JsErrorBox;
use rt_store::{SqlRef, SqlResult, SqlStatement, SqlValue};

/// Integers beyond this are not exactly representable as a JS number (2^53).
const MAX_EXACT: f64 = 9_007_199_254_740_992.0;

/// A value bound to a statement parameter, owned until the statement has run.
pub(crate) enum Bind {
    Null,
    Int(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
}

impl Bind {
    pub(crate) fn as_ref(&self) -> SqlRef<'_> {
        match self {
            Bind::Null => SqlRef::Null,
            Bind::Int(i) => SqlRef::Integer(*i),
            Bind::Real(r) => SqlRef::Real(*r),
            Bind::Text(t) => SqlRef::Text(t.as_bytes()),
            Bind::Blob(b) => SqlRef::Blob(b),
        }
    }

    pub(crate) fn into_value(self) -> SqlValue {
        match self {
            Bind::Null => SqlValue::Null,
            Bind::Int(i) => SqlValue::Integer(i),
            Bind::Real(r) => SqlValue::Real(r),
            Bind::Text(t) => SqlValue::Text(t),
            Bind::Blob(b) => SqlValue::Blob(b),
        }
    }
}

fn bytes_of_view(view: v8::Local<'_, v8::ArrayBufferView>) -> Vec<u8> {
    let mut out = vec![0u8; view.byte_length()];
    let n = view.copy_contents(&mut out);
    out.truncate(n);
    out
}

/// One JS value as a bind parameter. `null`/`undefined` -> NULL; a number -> INTEGER when it is a
/// whole number within +-2^53, else REAL (NaN -> NULL, as SQLite stores it); `bigint` -> INTEGER
/// (out of the 64-bit range: `RangeError`); `boolean` -> 0/1; string -> TEXT; `Uint8Array` or any
/// buffer -> BLOB; anything else is a `TypeError` (bun:sqlite). `std:sql` binds an object as its
/// JSON text: JS makes it before the op (`snapshotBinds` in `js/sqlite.js`).
///
/// Never runs guest code: only primitives and buffers are read, through their internal slots. A
/// SQL op holds the isolate's `OpState` (and `std:sqlite` the connection) while it converts, so a
/// getter, `toString`, `toJSON` or proxy trap that re-entered an op would abort the node or
/// deadlock the Machine thread.
pub(crate) fn bind_of(scope: &mut v8::PinScope<'_, '_>, v: v8::Local<'_, v8::Value>) -> Result<Bind, JsErrorBox> {
    if v.is_null_or_undefined() {
        return Ok(Bind::Null);
    }
    if v.is_int32() {
        return Ok(v.int32_value(scope).map_or(Bind::Null, |i| Bind::Int(i64::from(i))));
    }
    if v.is_number() {
        let n = v.number_value(scope).unwrap_or(f64::NAN);
        return Ok(if n.is_nan() {
            Bind::Null
        } else if n.fract() == 0.0 && n.abs() <= MAX_EXACT {
            Bind::Int(n as i64)
        } else {
            Bind::Real(n)
        });
    }
    if v.is_string() {
        return Ok(Bind::Text(v.to_rust_string_lossy(scope)));
    }
    if v.is_boolean() {
        return Ok(Bind::Int(i64::from(v.is_true())));
    }
    if v.is_big_int() {
        let b = v8::Local::<v8::BigInt>::try_from(v).map_err(|_| JsErrorBox::type_error("not a bigint"))?;
        let (i, lossless) = b.i64_value();
        return if lossless {
            Ok(Bind::Int(i))
        } else {
            Err(JsErrorBox::range_error("bigint does not fit in 64 bits"))
        };
    }
    if let Ok(view) = v8::Local::<v8::ArrayBufferView>::try_from(v) {
        return Ok(Bind::Blob(bytes_of_view(view)));
    }
    if let Ok(buf) = v8::Local::<v8::ArrayBuffer>::try_from(v) {
        let store = buf.get_backing_store();
        return Ok(Bind::Blob(store.iter().map(std::cell::Cell::get).collect()));
    }
    Err(JsErrorBox::type_error(
        "unsupported binding: expected null, number, bigint, boolean, string or Uint8Array",
    ))
}

/// A JS array of bind values (`undefined`/`null` = none), as `snapshotBinds` makes it: a plain
/// array of own data elements, so reading them runs no guest code. (A proxy is not an array
/// here; the snapshot is.)
pub(crate) fn binds_of(scope: &mut v8::PinScope<'_, '_>, v: v8::Local<'_, v8::Value>) -> Result<Vec<Bind>, JsErrorBox> {
    if v.is_null_or_undefined() {
        return Ok(Vec::new()); // LCOV_EXCL_LINE: the JS side hands over an array (`snapshotBinds`)
    }
    let arr = v8::Local::<v8::Array>::try_from(v).map_err(|_| JsErrorBox::type_error("args must be an array"))?;
    let n = arr.length();
    let mut out = Vec::with_capacity(n as usize);
    for i in 0..n {
        let item = arr.get_index(scope, i).unwrap_or_else(|| v8::undefined(scope).into());
        out.push(bind_of(scope, item)?);
    }
    Ok(out)
}

// ---- results ------------------------------------------------------------------------------------

/// `Uint8Array` holding a copy of `bytes`.
pub(crate) fn bytes_to_v8<'s>(scope: &v8::PinScope<'s, '_>, bytes: &[u8]) -> v8::Local<'s, v8::Value> {
    let buf = v8::ArrayBuffer::new(scope, bytes.len());
    if let Some(dst) = buf.get_backing_store().data() {
        // SAFETY: the backing store was just allocated with `bytes.len()` bytes and nothing else
        // can reach it yet.
        unsafe { std::ptr::copy_nonoverlapping(bytes.as_ptr(), dst.as_ptr().cast::<u8>(), bytes.len()) };
    }
    match v8::Uint8Array::new(scope, buf, 0, bytes.len()) {
        Some(a) => a.into(),
        None => v8::null(scope).into(), // LCOV_EXCL_LINE: V8 failing to allocate
    }
}

/// One column value as a JS value (`safe`: `INTEGER` as `bigint`).
pub(crate) fn cell_to_v8<'s>(scope: &v8::PinScope<'s, '_>, v: SqlRef<'_>, safe: bool) -> v8::Local<'s, v8::Value> {
    match v {
        SqlRef::Null => v8::null(scope).into(),
        SqlRef::Integer(i) => {
            if safe {
                v8::BigInt::new_from_i64(scope, i).into()
            } else if let Ok(small) = i32::try_from(i) {
                v8::Integer::new(scope, small).into()
            } else {
                v8::Number::new(scope, i as f64).into()
            }
        }
        SqlRef::Real(r) => v8::Number::new(scope, r).into(),
        SqlRef::Text(t) => text_to_v8(scope, t),
        SqlRef::Blob(b) => bytes_to_v8(scope, b),
    }
}

fn text_to_v8<'s>(scope: &v8::PinScope<'s, '_>, t: &[u8]) -> v8::Local<'s, v8::Value> {
    match v8::String::new_from_utf8(scope, t, v8::NewStringType::Normal) {
        Some(s) => s.into(),
        None => v8::null(scope).into(), // LCOV_EXCL_LINE: V8 failing to allocate
    }
}

fn str_key<'s>(scope: &v8::PinScope<'s, '_>, name: &str) -> v8::Local<'s, v8::Name> {
    match v8::String::new_from_utf8(scope, name.as_bytes(), v8::NewStringType::Internalized) {
        Some(s) => s.into(),
        None => v8::String::empty(scope).into(), // LCOV_EXCL_LINE: V8 failing to allocate
    }
}

/// `{ k0: v0, k1: v1, ... }` with a plain `Object.prototype`, as own data properties (defined,
/// never set: a setter a guest put on `Object.prototype` does not run).
pub(crate) fn object_of<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    entries: &[(&str, v8::Local<'s, v8::Value>)],
) -> v8::Local<'s, v8::Object> {
    let obj = v8::Object::new(scope);
    for (k, v) in entries {
        let key = str_key(scope, k);
        obj.create_data_property(scope, key, *v);
    }
    obj
}

fn value_of<'s>(scope: &v8::PinScope<'s, '_>, v: &SqlValue) -> v8::Local<'s, v8::Value> {
    let r = match v {
        SqlValue::Null => SqlRef::Null,
        SqlValue::Integer(i) => SqlRef::Integer(*i),
        SqlValue::Real(r) => SqlRef::Real(*r),
        SqlValue::Text(t) => SqlRef::Text(t.as_bytes()),
        SqlValue::Blob(b) => SqlRef::Blob(b),
    };
    cell_to_v8(scope, r, false)
}

/// `{ columns, rows, rowsAffected, lastInsertRowid }` of an `std:sql` call, built as JS values.
pub(crate) struct SqlResultV8(pub SqlResult);

impl<'a> ToV8<'a> for SqlResultV8 {
    type Error = Infallible;

    fn to_v8<'i>(self, scope: &mut v8::PinScope<'a, 'i>) -> Result<v8::Local<'a, v8::Value>, Self::Error> {
        let r = self.0;
        let columns: Vec<v8::Local<'a, v8::Value>> =
            r.columns.iter().map(|c| text_to_v8(scope, c.as_bytes())).collect();
        let columns = v8::Array::new_with_elements(scope, &columns);
        let rows: Vec<v8::Local<'a, v8::Value>> = r
            .rows
            .iter()
            .map(|row| {
                let cells: Vec<v8::Local<'a, v8::Value>> = row.iter().map(|c| value_of(scope, c)).collect();
                v8::Array::new_with_elements(scope, &cells).into()
            })
            .collect();
        let rows = v8::Array::new_with_elements(scope, &rows);
        let affected = v8::Number::new(scope, r.rows_affected as f64);
        let last = match r.last_insert_rowid {
            Some(id) => v8::Number::new(scope, id as f64).into(),
            None => v8::null(scope).into(),
        };
        let fields = [
            ("columns", columns.into()),
            ("rows", rows.into()),
            ("rowsAffected", affected.into()),
            ("lastInsertRowid", last),
        ];
        Ok(object_of(scope, &fields).into())
    }
}

/// The results of a batch, as an array.
pub(crate) struct SqlResultsV8(pub Vec<SqlResult>);

impl<'a> ToV8<'a> for SqlResultsV8 {
    type Error = Infallible;

    fn to_v8<'i>(self, scope: &mut v8::PinScope<'a, 'i>) -> Result<v8::Local<'a, v8::Value>, Self::Error> {
        let items: Vec<v8::Local<'a, v8::Value>> = self
            .0
            .into_iter()
            .map(|r| SqlResultV8(r).to_v8(scope).unwrap_or_else(|never| match never {}))
            .collect();
        Ok(v8::Array::new_with_elements(scope, &items).into())
    }
}

// ---- statements in -----------------------------------------------------------------------------------

/// Own data property `name` of a statement object made by `snapshotStmts` (null prototype), as
/// a string. Nothing is converted: a value that is not a string is `None`.
fn string_prop(scope: &mut v8::PinScope<'_, '_>, obj: v8::Local<'_, v8::Object>, name: &str) -> Option<String> {
    let key = str_key(scope, name);
    let v = obj.get(scope, key.into())?;
    v.is_string().then(|| v.to_rust_string_lossy(scope))
}

/// `{ sql, args }` (or a bare SQL string) from JS, with its arguments decoded. The object is the
/// snapshot `snapshotStmts` (`js/bootstrap.js`) makes: null prototype, own data properties, so
/// reading it runs no guest code; anything else (a proxy, an object with a prototype) is refused.
pub(crate) struct StmtV8(pub SqlStatement);

impl<'a> FromV8<'a> for StmtV8 {
    type Error = JsErrorBox;

    fn from_v8<'i>(scope: &mut v8::PinScope<'a, 'i>, value: v8::Local<'a, v8::Value>) -> Result<Self, Self::Error> {
        if value.is_string() {
            return Ok(StmtV8(SqlStatement {
                sql: value.to_rust_string_lossy(scope),
                args: Vec::new(),
            }));
        }
        let bad = || JsErrorBox::type_error("a statement is a string or { sql, args }");
        if value.is_proxy() {
            return Err(bad()); // LCOV_EXCL_LINE: the JS side hands over a null-prototype snapshot (`snapshotStmts`)
        }
        let obj = v8::Local::<v8::Object>::try_from(value).map_err(|_| bad())?;
        if !obj.get_prototype(scope).is_some_and(|p| p.is_null()) {
            return Err(bad()); // LCOV_EXCL_LINE: the JS side hands over a null-prototype snapshot (`snapshotStmts`)
        }
        let sql = string_prop(scope, obj, "sql").ok_or_else(|| JsErrorBox::type_error("a statement needs `sql`"))?;
        let key = str_key(scope, "args");
        let args = match obj.get(scope, key.into()) {
            Some(a) => binds_of(scope, a)?,
            None => Vec::new(), // LCOV_EXCL_LINE: a plain object always answers its `args` key
        };
        Ok(StmtV8(SqlStatement {
            sql,
            args: args.into_iter().map(Bind::into_value).collect(),
        }))
    }
}

/// The statements of a batch.
pub(crate) struct StmtsV8(pub Vec<SqlStatement>);

impl<'a> FromV8<'a> for StmtsV8 {
    type Error = JsErrorBox;

    fn from_v8<'i>(scope: &mut v8::PinScope<'a, 'i>, value: v8::Local<'a, v8::Value>) -> Result<Self, Self::Error> {
        let arr =
            v8::Local::<v8::Array>::try_from(value).map_err(|_| JsErrorBox::type_error("stmts must be an array"))?;
        let mut out = Vec::with_capacity(arr.length() as usize);
        for i in 0..arr.length() {
            let item = arr.get_index(scope, i).unwrap_or_else(|| v8::undefined(scope).into());
            out.push(StmtV8::from_v8(scope, item)?.0);
        }
        Ok(StmtsV8(out))
    }
}

/// Positional arguments of an `std:sql` call, decoded from a JS array.
pub(crate) struct ArgsV8(pub Vec<SqlValue>);

impl<'a> FromV8<'a> for ArgsV8 {
    type Error = JsErrorBox;

    fn from_v8<'i>(scope: &mut v8::PinScope<'a, 'i>, value: v8::Local<'a, v8::Value>) -> Result<Self, Self::Error> {
        Ok(ArgsV8(
            binds_of(scope, value)?.into_iter().map(Bind::into_value).collect(),
        ))
    }
}
