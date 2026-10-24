//! rt-cache (L3): tiered prepared cache, single-flight loading, cost-aware W-TinyLFU
//! eviction, MAC-verified T2 on disk.

pub mod cache;
pub mod policy;
pub mod t2;

pub use cache::{
    pack_snapshots, unpack_snapshots, CacheSettings, CacheStats, EvictReason, EvictReport, LoadError, Loader,
    PreparedCache, T2Usage, T2Use,
};
pub use policy::{EntryMeta, EvictionPolicy, FrequencySketch, WTinyLfu};
pub use rt_sched::{Lookup, PreparedSource};
pub use t2::{NoTamperSink, T2Entry, T2Error, T2Prepared, T2Store, TamperSink, T2_LAYOUT};
