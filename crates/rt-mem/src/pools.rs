//! IO buffer pools: size-class pools (4 KB / 64 KB / 1 MB), capped, trimmed on purge.
//!
//! Buffers are charged to the governor's `io_buffers` category while in use *and* while idle in
//! a pool (the memory is still resident), so `trim` is what hands idle memory back.
//!
//! **Reserved:** no I/O path of the node borrows from these pools yet (request and response
//! bodies are `bytes::Bytes` from hyper, guest I/O buffers belong to the executors), so the
//! `io_buffers` category stays 0. The pools are complete and tested for when a body path
//! adopts them.

use std::sync::Arc;

use parking_lot::Mutex;

use crate::governor::MemoryGovernor;
use crate::reservation::Reservation;
use crate::types::{MemCategory, Owner};

/// Pool size classes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum SizeClass {
    K4,
    K64,
    M1,
}

impl SizeClass {
    pub const fn bytes(self) -> usize {
        match self {
            SizeClass::K4 => 4 * 1024,
            SizeClass::K64 => 64 * 1024,
            SizeClass::M1 => 1024 * 1024,
        }
    }

    /// Smallest class that fits `len`, or `None` when larger than 1 MB.
    pub fn for_len(len: usize) -> Option<SizeClass> {
        [SizeClass::K4, SizeClass::K64, SizeClass::M1]
            .into_iter()
            .find(|c| c.bytes() >= len)
    }

    const fn idx(self) -> usize {
        match self {
            SizeClass::K4 => 0,
            SizeClass::K64 => 1,
            SizeClass::M1 => 2,
        }
    }
}

/// An idle pooled buffer: always zeroed, still charged to the governor.
struct Idle {
    buf: Vec<u8>,
    charge: Reservation,
}

#[derive(Default)]
struct State {
    idle: [Vec<Idle>; 3],
    /// Bytes held by idle buffers.
    retained: u64,
}

struct Shared {
    governor: Arc<dyn MemoryGovernor>,
    max_bytes: u64,
    state: Mutex<State>,
}

/// A buffer borrowed from a pool; returned (or freed when the pool is full) on drop.
pub struct PooledBuf {
    buf: Vec<u8>,
    class: Option<SizeClass>,
    /// `None` when the governor refused the charge (the buffer is then never pooled).
    charge: Option<Reservation>,
    pool: Arc<Shared>,
}

impl PooledBuf {
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.buf
    }
    pub fn len(&self) -> usize {
        self.buf.len()
    }
    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }
}

impl Drop for PooledBuf {
    fn drop(&mut self) {
        let (Some(class), Some(charge)) = (self.class, self.charge.take()) else {
            return; // oversize or uncharged: freed
        };
        let mut buf = std::mem::take(&mut self.buf);
        let size = class.bytes() as u64;
        let mut st = self.pool.state.lock();
        if st.retained + size > self.pool.max_bytes {
            return; // pool full: buffer and charge are dropped (outside the zeroing cost)
        }
        // idle buffers never hold previous contents
        buf.fill(0);
        st.retained += size;
        st.idle[class.idx()].push(Idle { buf, charge });
    }
}

/// Capped, governor-accounted (`io_buffers` category) buffer pools.
pub struct IoBufferPools {
    shared: Arc<Shared>,
}

impl IoBufferPools {
    /// `max_bytes` bounds the memory retained in the pools.
    pub fn new(governor: Arc<dyn MemoryGovernor>, max_bytes: u64) -> Self {
        Self {
            shared: Arc::new(Shared {
                governor,
                max_bytes,
                state: Mutex::new(State::default()),
            }),
        }
    }

    /// Borrow a zeroed buffer of at least `len` bytes.
    pub fn get(&self, len: usize) -> PooledBuf {
        let class = SizeClass::for_len(len);
        if let Some(class) = class {
            let mut st = self.shared.state.lock();
            if let Some(idle) = st.idle[class.idx()].pop() {
                st.retained -= class.bytes() as u64;
                return PooledBuf {
                    buf: idle.buf,
                    class: Some(class),
                    charge: Some(idle.charge),
                    pool: self.shared.clone(),
                };
            }
        }
        let size = class.map_or(len, SizeClass::bytes);
        let charge = self
            .shared
            .governor
            .reserve(MemCategory::IoBuffers, size as u64, Owner::NONE)
            .ok();
        PooledBuf {
            buf: vec![0u8; size],
            class,
            charge,
            pool: self.shared.clone(),
        }
    }

    /// Drop pooled buffers (called on purge).
    pub fn trim(&self) {
        let drained: Vec<Idle> = {
            let mut st = self.shared.state.lock();
            st.retained = 0;
            st.idle.iter_mut().flat_map(std::mem::take).collect()
        };
        drop(drained);
    }
}
