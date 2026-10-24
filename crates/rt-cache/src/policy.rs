//! Cost-aware W-TinyLFU.
//!
//! | Element | Spec |
//! |---|---|
//! | Frequency | Count-Min Sketch, 4-bit counters, doorkeeper Bloom filter; halve all counters every `10 × capacity_entries` accesses |
//! | Structure | Window LRU (1 %) → main SLRU (probation 20 % / protected 80 %) |
//! | Admission | Candidate beats victim if `freq × cost / size` is higher |
//! | `cost` | EWMA of measured reload time from the next tier |
//! | `size` | Accounted bytes |

use std::time::Duration;

/// What the policy knows about a cache entry.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct EntryMeta {
    pub key_hash: u64,
    /// Accounted bytes.
    pub size: u64,
    /// EWMA of reload time from the next tier (load ms, or compile ms when T2 is missing).
    pub cost: Duration,
    /// `keep_resident` services are exempt from eviction.
    pub pinned: bool,
}

/// Admission/eviction policy for T1 (T2 reuses the frequency data).
pub trait EvictionPolicy: Send {
    /// Count an access (lookups, hits, misses).
    fn record_access(&mut self, key_hash: u64);

    /// Estimated frequency of `key_hash` (0..=15 plus doorkeeper bit).
    fn frequency(&self, key_hash: u64) -> u32;

    /// Should `candidate` replace `victim`? `freq × cost / size` of the candidate must be
    /// strictly higher (CCH-04: equal frequency and size, 200 ms vs 2 ms reload ⇒ the cheap one is
    /// evicted).
    fn admit(&self, candidate: &EntryMeta, victim: &EntryMeta) -> bool;

    /// Halve all counters (aging); also called by sysmon every second for decay.
    fn decay(&mut self);

    /// Serialize the sketch (persisted to `meta.db` `cache_freq` every 5 min, CCH-11).
    fn snapshot(&self) -> Vec<u8>;

    /// Restore a sketch produced by `snapshot`. Unknown/corrupt data is ignored (cold start).
    fn restore(&mut self, data: &[u8]);
}

const ROWS: usize = 4;
const MAX_COUNT: u8 = 15;
const SNAPSHOT_MAGIC: &[u8; 4] = b"WTL1";
const SNAPSHOT_HEADER: usize = 4 + 8 + 8 + 4 + 4;

/// SplitMix64 finalizer: spreads small / structured hashes over all 64 bits.
fn mix(mut z: u64) -> u64 {
    z = z.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

fn fnv(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in bytes {
        h ^= u64::from(*b);
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

/// Count-Min Sketch with 4-bit counters and a doorkeeper filter.
///
/// The first access of a key only sets its doorkeeper bits (one-hit wonders never reach the
/// counters); later accesses bump the counters (conservative update). Every `10 × capacity`
/// accesses all counters are halved and the doorkeeper is cleared (aging).
pub struct FrequencySketch {
    capacity_entries: usize,
    /// Counters per row (power of two).
    width: usize,
    /// `ROWS × width` 4-bit counters, two per byte.
    counters: Vec<u8>,
    /// Doorkeeper Bloom filter (bit array, power-of-two bit count, two hashes).
    door: Vec<u64>,
    door_mask: u64,
    additions: u64,
    sample_size: u64,
}

impl FrequencySketch {
    /// Sized for `capacity_entries` cache entries (halves every `10 × capacity_entries` accesses).
    pub fn new(capacity_entries: usize) -> Self {
        let cap = capacity_entries.max(1);
        let width = cap.next_power_of_two().clamp(64, 1 << 22);
        let door_bits = (cap.saturating_mul(8)).next_power_of_two().clamp(1024, 1 << 26);
        Self {
            capacity_entries: cap,
            width,
            counters: vec![0; ROWS * width / 2],
            door: vec![0; door_bits / 64],
            door_mask: (door_bits - 1) as u64,
            additions: 0,
            sample_size: (cap as u64).saturating_mul(10).max(10),
        }
    }

    fn index(&self, key_hash: u64, row: usize) -> usize {
        row * self.width
            + (mix(key_hash ^ (row as u64 + 1).wrapping_mul(0xA24B_AED4_963E_E407)) as usize & (self.width - 1))
    }

    fn get(&self, idx: usize) -> u8 {
        let b = self.counters[idx >> 1];
        if idx & 1 == 0 {
            b & 0x0F
        } else {
            b >> 4
        }
    }

    fn set(&mut self, idx: usize, v: u8) {
        let b = &mut self.counters[idx >> 1];
        if idx & 1 == 0 {
            *b = (*b & 0xF0) | (v & 0x0F);
        } else {
            *b = (*b & 0x0F) | (v << 4);
        }
    }

    fn door_bits(&self, key_hash: u64) -> [u64; 2] {
        let h = mix(key_hash ^ 0xD6E8_FEB8_6659_FD93);
        [h & self.door_mask, (h >> 32 ^ mix(h)) & self.door_mask]
    }

    fn door_contains(&self, key_hash: u64) -> bool {
        self.door_bits(key_hash)
            .iter()
            .all(|b| self.door[(b >> 6) as usize] & (1u64 << (b & 63)) != 0)
    }

    fn door_insert(&mut self, key_hash: u64) {
        for b in self.door_bits(key_hash) {
            self.door[(b >> 6) as usize] |= 1u64 << (b & 63);
        }
    }

    pub fn increment(&mut self, key_hash: u64) {
        if !self.door_contains(key_hash) {
            self.door_insert(key_hash);
        } else {
            let idx: [usize; ROWS] = std::array::from_fn(|r| self.index(key_hash, r));
            let vals: [u8; ROWS] = std::array::from_fn(|r| self.get(idx[r]));
            let min = *vals.iter().min().unwrap_or(&0);
            if min < MAX_COUNT {
                // Conservative update: only the minimal counters move.
                for (v, i) in vals.iter().zip(idx) {
                    if *v == min {
                        self.set(i, min + 1);
                    }
                }
            }
        }
        self.additions += 1;
        if self.additions >= self.sample_size {
            self.halve();
        }
    }

    pub fn estimate(&self, key_hash: u64) -> u32 {
        let counted = (0..ROWS).map(|r| self.get(self.index(key_hash, r))).min().unwrap_or(0);
        u32::from(counted) + u32::from(self.door_contains(key_hash))
    }

    pub fn halve(&mut self) {
        for b in &mut self.counters {
            // Both nibbles: (x >> 1) without bleeding the low bit of the high nibble downwards.
            *b = (*b >> 1) & 0x77;
        }
        self.door.iter_mut().for_each(|w| *w = 0);
        self.additions /= 2;
    }

    fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(SNAPSHOT_HEADER + self.counters.len() + self.door.len() * 8 + 8);
        out.extend_from_slice(SNAPSHOT_MAGIC);
        out.extend_from_slice(&(self.capacity_entries as u64).to_le_bytes());
        out.extend_from_slice(&self.additions.to_le_bytes());
        out.extend_from_slice(&(self.width as u32).to_le_bytes());
        out.extend_from_slice(&(self.door.len() as u32).to_le_bytes());
        out.extend_from_slice(&self.counters);
        for w in &self.door {
            out.extend_from_slice(&w.to_le_bytes());
        }
        let sum = fnv(&out);
        out.extend_from_slice(&sum.to_le_bytes());
        out
    }

    /// Parse a snapshot; `None` for anything that is not exactly a snapshot we wrote.
    fn decode(data: &[u8], sample_size: u64) -> Option<Self> {
        if data.len() < SNAPSHOT_HEADER + 8 || &data[..4] != SNAPSHOT_MAGIC {
            return None;
        }
        let (body, tail) = data.split_at(data.len() - 8);
        if fnv(body) != u64::from_le_bytes(tail.try_into().ok()?) {
            return None;
        }
        let capacity_entries = u64::from_le_bytes(body[4..12].try_into().ok()?) as usize;
        let additions = u64::from_le_bytes(body[12..20].try_into().ok()?);
        let width = u32::from_le_bytes(body[20..24].try_into().ok()?) as usize;
        let door_words = u32::from_le_bytes(body[24..28].try_into().ok()?) as usize;
        if !width.is_power_of_two() || !(64..=(1 << 22)).contains(&width) || !(door_words * 64).is_power_of_two() {
            return None;
        }
        let counters_len = ROWS * width / 2;
        let rest = &body[SNAPSHOT_HEADER..];
        if rest.len() != counters_len + door_words * 8 {
            return None;
        }
        let door = rest[counters_len..]
            .chunks_exact(8)
            .map(|c| u64::from_le_bytes(c.try_into().unwrap_or([0; 8])))
            .collect();
        Some(Self {
            capacity_entries,
            width,
            counters: rest[..counters_len].to_vec(),
            door,
            door_mask: (door_words as u64 * 64) - 1,
            additions,
            sample_size,
        })
    }
}

/// Window LRU (1 %) → main SLRU (probation 20 % / protected 80 %) with cost-aware admission.
///
/// The segmented structure itself lives in `PreparedCache` (it owns the entries); this type is
/// the *policy*: frequency estimation and the admission rule `freq × cost / size`.
pub struct WTinyLfu {
    sketch: FrequencySketch,
}

impl WTinyLfu {
    pub fn new(capacity_entries: usize) -> Self {
        Self {
            sketch: FrequencySketch::new(capacity_entries),
        }
    }

    /// `freq × cost / size` (cost in seconds, size in bytes; a zero size counts as one byte).
    fn score(&self, m: &EntryMeta) -> f64 {
        f64::from(self.sketch.estimate(m.key_hash)) * m.cost.as_secs_f64() / m.size.max(1) as f64
    }
}

impl EvictionPolicy for WTinyLfu {
    fn record_access(&mut self, key_hash: u64) {
        self.sketch.increment(key_hash);
    }

    fn frequency(&self, key_hash: u64) -> u32 {
        self.sketch.estimate(key_hash)
    }

    fn admit(&self, candidate: &EntryMeta, victim: &EntryMeta) -> bool {
        if victim.pinned {
            return false;
        }
        candidate.pinned || self.score(candidate) > self.score(victim)
    }

    fn decay(&mut self) {
        self.sketch.halve();
    }

    fn snapshot(&self) -> Vec<u8> {
        self.sketch.encode()
    }

    fn restore(&mut self, data: &[u8]) {
        if let Some(s) = FrequencySketch::decode(data, self.sketch.sample_size) {
            self.sketch = s;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Body (everything before the trailing checksum) of a snapshot.
    fn body(snapshot: &[u8]) -> Vec<u8> {
        snapshot[..snapshot.len() - 8].to_vec()
    }

    /// Append a checksum that matches `body`, so only the layout checks can refuse it.
    fn sealed(mut body: Vec<u8>) -> Vec<u8> {
        let sum = fnv(&body);
        body.extend_from_slice(&sum.to_le_bytes());
        body
    }

    #[test]
    fn a_snapshot_with_a_bad_width_or_body_length_is_refused() {
        let sketch = FrequencySketch::new(1000);
        let good = sketch.encode();
        assert!(
            FrequencySketch::decode(&good, 1).is_some(),
            "the snapshot we wrote decodes"
        );

        // Width is the u32 at body[20..24]; 3 is not a power of two.
        let mut width = body(&good);
        width[20..24].copy_from_slice(&3u32.to_le_bytes());
        assert!(FrequencySketch::decode(&sealed(width), 1).is_none());

        // One byte short of the counter table.
        let mut short = body(&good);
        short.remove(SNAPSHOT_HEADER);
        assert!(FrequencySketch::decode(&sealed(short), 1).is_none());
    }
}
