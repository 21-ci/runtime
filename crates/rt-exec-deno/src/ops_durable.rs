//! Ops behind `std:durable` storage: the task's object's keys
//! (V8 structured clones) and its alarm. Writes wait in the task's handle and commit with the
//! handler (`op_rt_task_commit`); reads see them.

use std::cell::RefCell;
use std::rc::Rc;

use bytes::Bytes;
use deno_core::serde_v8::ToJsBuffer;
use deno_core::{op2, JsBuffer, OpState};
use deno_error::JsErrorBox;
use rt_host::{HostError, HostHandle};
use rt_store::{DurableListOptions, DurableValue};
use serde::{Deserialize, Serialize};

use crate::ops::TaskOps;

fn host(state: &OpState) -> Result<HostHandle, JsErrorBox> {
    state
        .try_borrow::<TaskOps>()
        .map(|t| t.host.clone())
        .ok_or_else(|| JsErrorBox::new("Error", "unavailable"))
}

fn err(e: HostError) -> JsErrorBox {
    JsErrorBox::new("Error", e.code)
}

#[derive(Serialize)]
pub struct ValueOut {
    format: u8,
    data: ToJsBuffer,
}

fn value_out(v: DurableValue) -> ValueOut {
    ValueOut {
        format: v.format,
        data: v.data.to_vec().into(),
    }
}

#[derive(Serialize)]
pub struct EntryOut {
    key: String,
    format: u8,
    data: ToJsBuffer,
}

/// `storage.get(key | keys)`.
#[op2]
#[serde]
pub async fn op_rt_storage_get(
    state: Rc<RefCell<OpState>>,
    #[serde] keys: Vec<String>,
) -> Result<Vec<Option<ValueOut>>, JsErrorBox> {
    let h = host(&state.borrow())?;
    let keys: Vec<Bytes> = keys.into_iter().map(Bytes::from).collect();
    Ok(h.storage_get(&keys)
        .await
        .map_err(err)?
        .into_iter()
        .map(|v| v.map(value_out))
        .collect())
}

/// `storage.put(key, value)`: `format` 1 = structured clone.
#[op2]
pub fn op_rt_storage_put(
    state: &mut OpState,
    #[string] key: String,
    #[smi] format: u32,
    #[buffer] data: JsBuffer,
) -> Result<(), JsErrorBox> {
    let value = DurableValue {
        format: u8::try_from(format).unwrap_or(0),
        data: Bytes::copy_from_slice(&data),
    };
    host(state)?.storage_put(Bytes::from(key), value).map_err(err)
}

/// `storage.delete(key | keys)`: how many existed.
#[op2]
pub async fn op_rt_storage_delete(state: Rc<RefCell<OpState>>, #[serde] keys: Vec<String>) -> Result<u32, JsErrorBox> {
    let h = host(&state.borrow())?;
    let keys: Vec<Bytes> = keys.into_iter().map(Bytes::from).collect();
    h.storage_delete(&keys).await.map_err(err)
}

#[derive(Deserialize)]
pub struct ListIn {
    #[serde(default)]
    prefix: String,
    start: Option<String>,
    end: Option<String>,
    limit: Option<u32>,
    #[serde(default)]
    reverse: bool,
}

/// `storage.list({ prefix, start, end, limit, reverse })`.
#[op2]
#[serde]
pub async fn op_rt_storage_list(state: Rc<RefCell<OpState>>, #[serde] q: ListIn) -> Result<Vec<EntryOut>, JsErrorBox> {
    let h = host(&state.borrow())?;
    let opts = DurableListOptions {
        prefix: Bytes::from(q.prefix),
        start: q.start.map(Bytes::from),
        end: q.end.map(Bytes::from),
        limit: q.limit.unwrap_or(1000) as usize,
        reverse: q.reverse,
    };
    let page = h.storage_list(opts).await.map_err(err)?;
    Ok(page
        .into_iter()
        .map(|(k, v)| EntryOut {
            key: String::from_utf8_lossy(&k).into_owned(),
            format: v.format,
            data: v.data.to_vec().into(),
        })
        .collect())
}

/// `storage.deleteAll()`.
#[op2(fast)]
pub fn op_rt_storage_delete_all(state: &mut OpState) -> Result<(), JsErrorBox> {
    host(state)?.storage_delete_all().map_err(err)
}

/// `storage.getAlarm()`: Unix ms, or -1 for none (the JS side answers null).
#[op2]
pub async fn op_rt_alarm_get(state: Rc<RefCell<OpState>>) -> Result<f64, JsErrorBox> {
    let h = host(&state.borrow())?;
    Ok(h.alarm_get().await.map_err(err)?.map_or(-1.0, |t| t as f64))
}

/// `storage.setAlarm(time)` (`has`) / `storage.deleteAlarm()`.
#[op2(fast)]
pub fn op_rt_alarm_set(state: &mut OpState, has: bool, at_ms: f64) -> Result<(), JsErrorBox> {
    host(state)?.alarm_set(has.then_some(at_ms as i64)).map_err(err)
}
