//! Host facts for operators (`GET /v1/node/status`, `GET /v1/node/series`).
//!
//! Sources are `getloadavg`, `statvfs` on the data dir, `/proc/stat`, `/proc/net/dev` and the
//! cgroup CPU counters. A source that is absent (macOS, no cgroup) gives `None`, never an error.
//! A sample every minute feeds a fixed 24 h ring; live values are deltas from the last sample.
//! Everything here is operator-only, and no host path is ever reported: the disk carries a label.

use std::collections::VecDeque;
use std::ffi::CString;
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::time::Instant;

use parking_lot::Mutex;
use serde::Serialize;

/// 24 h of 1-minute points.
pub const RING_POINTS: usize = 24 * 60;
const DISK_LABEL: &str = "data";
const MINUTE_MS: i64 = 60_000;

/// Where the cgroup tree says this process runs.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Container {
    Cgroup2,
    Cgroup1,
    None,
}

impl Container {
    pub const fn as_str(self) -> &'static str {
        match self {
            Container::Cgroup2 => "cgroup2",
            Container::Cgroup1 => "cgroup1",
            Container::None => "none",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct DiskInfo {
    pub path_label: &'static str,
    pub total_bytes: u64,
    pub free_bytes: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Serialize)]
pub struct NetRates {
    pub rx_bytes_per_s: f64,
    pub tx_bytes_per_s: f64,
}

/// Live values for `status.host`.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct HostStatus {
    pub uptime_s: u64,
    pub container: Container,
    pub cores: u32,
    pub load: Option<[f64; 3]>,
    pub cpu_pct: Option<f64>,
    pub disk: Option<DiskInfo>,
    pub net: Option<NetRates>,
}

/// One point of the node series. Fixed size: the ring stays about 100 KB.
#[derive(Clone, Copy, Debug, PartialEq, Serialize)]
pub struct HostPoint {
    pub ts: i64,
    pub cpu_pct: Option<f64>,
    pub load1: Option<f64>,
    pub mem_committed: u64,
    pub disk_free: Option<u64>,
    pub rx: Option<f64>,
    pub tx: Option<f64>,
}

/// `range` of the node series.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SeriesRange {
    H1,
    H6,
    H24,
}

impl SeriesRange {
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "1h" => Some(Self::H1),
            "6h" => Some(Self::H6),
            "24h" => Some(Self::H24),
            _ => None,
        }
    }

    /// `(minutes covered, minutes per point)`: at most 300 points.
    const fn shape(self) -> (i64, i64) {
        match self {
            Self::H1 => (60, 1),
            Self::H6 => (6 * 60, 5),
            Self::H24 => (24 * 60, 5),
        }
    }

    /// Seconds between two points.
    pub const fn step_s(self) -> u32 {
        (self.shape().1 * 60) as u32
    }
}

/// Parse the aggregate `cpu` line of `/proc/stat`: `(busy, total)` jiffies. Idle and iowait are
/// not busy; guest time is already part of user time.
pub fn parse_proc_stat(text: &str) -> Option<(u64, u64)> {
    let line = text.lines().find(|l| l.starts_with("cpu "))?;
    let v: Vec<u64> = line
        .split_whitespace()
        .skip(1)
        .take(8)
        .map(str::parse)
        .collect::<Result<_, _>>()
        .ok()?;
    if v.len() < 4 {
        return None;
    }
    let total: u64 = v.iter().sum();
    let idle = v[3] + v.get(4).copied().unwrap_or(0);
    Some((total - idle, total))
}

/// Parse `/proc/net/dev`: `(rx, tx)` bytes over every interface but loopback.
pub fn parse_net_dev(text: &str) -> Option<(u64, u64)> {
    let mut sum = None;
    for line in text.lines() {
        let Some((name, rest)) = line.split_once(':') else {
            continue;
        };
        if name.trim() == "lo" {
            continue;
        }
        let mut cols = rest.split_whitespace();
        let (Some(rx), Some(tx)) = (cols.next(), cols.nth(7)) else {
            continue;
        };
        if let (Ok(rx), Ok(tx)) = (rx.parse::<u64>(), tx.parse::<u64>()) {
            let (r, t) = sum.unwrap_or((0, 0));
            sum = Some((r + rx, t + tx));
        }
    }
    sum
}

/// `usage_usec` of a cgroup v2 `cpu.stat`.
pub fn parse_cgroup2_usage_us(text: &str) -> Option<u64> {
    text.lines()
        .find_map(|l| l.strip_prefix("usage_usec "))
        .and_then(|v| v.trim().parse().ok())
}

/// The paths read; tests point them at a temporary tree.
#[derive(Clone, Debug)]
pub struct HostSources {
    pub proc_root: PathBuf,
    pub cgroup_root: PathBuf,
    pub data_dir: PathBuf,
}

impl HostSources {
    /// The real host, with disk figures for the filesystem of `data_dir`.
    pub fn system(data_dir: PathBuf) -> Self {
        Self {
            proc_root: PathBuf::from("/proc"),
            cgroup_root: PathBuf::from("/sys/fs/cgroup"),
            data_dir,
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum CpuRaw {
    /// CPU microseconds used by the cgroup.
    Cgroup(u64),
    /// `(busy, total)` jiffies of the host.
    Proc(u64, u64),
}

#[derive(Clone, Copy, Debug)]
struct Raw {
    at_ms: i64,
    cpu: Option<CpuRaw>,
    net: Option<(u64, u64)>,
}

#[derive(Default)]
struct State {
    last: Option<Raw>,
    ring: VecDeque<HostPoint>,
}

/// Samples the host and keeps the node series. Takes its lock once a minute and per operator
/// read; nothing on the request path touches it.
pub struct HostSampler {
    src: HostSources,
    started: Instant,
    cores: u32,
    state: Mutex<State>,
}

impl HostSampler {
    pub fn new(src: HostSources) -> Self {
        Self {
            src,
            started: Instant::now(),
            // honours affinity and the cgroup CPU quota
            cores: std::thread::available_parallelism().map_or(1, |n| n.get() as u32),
            state: Mutex::new(State::default()),
        }
    }

    pub fn container(&self) -> Container {
        let root = &self.src.cgroup_root;
        if root.join("cgroup.controllers").exists() {
            Container::Cgroup2
        } else if cgroup1_usage_path(root).is_some() {
            Container::Cgroup1
        } else {
            Container::None
        }
    }

    fn read_cpu(&self) -> Option<CpuRaw> {
        let root = &self.src.cgroup_root;
        let read = |p: &Path| std::fs::read_to_string(p).ok();
        if let Some(us) = read(&root.join("cpu.stat")).and_then(|t| parse_cgroup2_usage_us(&t)) {
            return Some(CpuRaw::Cgroup(us));
        }
        if let Some(ns) = cgroup1_usage_path(root)
            .and_then(|p| read(&p))
            .and_then(|t| t.trim().parse::<u64>().ok())
        {
            return Some(CpuRaw::Cgroup(ns / 1_000));
        }
        read(&self.src.proc_root.join("stat"))
            .and_then(|t| parse_proc_stat(&t))
            .map(|(busy, total)| CpuRaw::Proc(busy, total))
    }

    fn read_raw(&self, at_ms: i64) -> Raw {
        Raw {
            at_ms,
            cpu: self.read_cpu(),
            net: std::fs::read_to_string(self.src.proc_root.join("net/dev"))
                .ok()
                .and_then(|t| parse_net_dev(&t)),
        }
    }

    /// CPU % (of all cores) and network rates between two readings; `None` without elapsed time
    /// or without the source on both sides.
    fn deltas(&self, a: &Raw, b: &Raw) -> (Option<f64>, Option<NetRates>) {
        let ms = b.at_ms - a.at_ms;
        if ms <= 0 {
            return (None, None);
        }
        let cpu = match (a.cpu, b.cpu) {
            (Some(CpuRaw::Cgroup(x)), Some(CpuRaw::Cgroup(y))) => {
                let wall_us = ms as f64 * 1_000.0 * f64::from(self.cores);
                Some(y.saturating_sub(x) as f64 / wall_us * 100.0)
            }
            (Some(CpuRaw::Proc(bx, tx)), Some(CpuRaw::Proc(by, ty))) if ty > tx => {
                Some(by.saturating_sub(bx) as f64 / (ty - tx) as f64 * 100.0)
            }
            _ => None,
        };
        let net = a.net.zip(b.net).map(|((rx0, tx0), (rx1, tx1))| {
            let per_s = |d: u64| d as f64 * 1_000.0 / ms as f64;
            NetRates {
                rx_bytes_per_s: per_s(rx1.saturating_sub(rx0)),
                tx_bytes_per_s: per_s(tx1.saturating_sub(tx0)),
            }
        });
        (cpu.map(|c| c.clamp(0.0, 100.0)), net)
    }

    /// Take the minute's sample (reads files: run it off the async threads).
    pub fn sample(&self, now_ms: i64, mem_committed: u64) {
        let raw = self.read_raw(now_ms);
        let load1 = load_average().map(|l| l[0]);
        let disk_free = disk_info(&self.src.data_dir).map(|d| d.free_bytes);
        let mut st = self.state.lock();
        let (cpu_pct, net) = st.last.map_or((None, None), |last| self.deltas(&last, &raw));
        st.last = Some(raw);
        if st.ring.len() == RING_POINTS {
            st.ring.pop_front();
        }
        st.ring.push_back(HostPoint {
            ts: now_ms,
            cpu_pct,
            load1,
            mem_committed,
            disk_free,
            rx: net.map(|n| n.rx_bytes_per_s),
            tx: net.map(|n| n.tx_bytes_per_s),
        });
    }

    /// Live values: deltas from the last sample (reads files: run it off the async threads).
    pub fn live(&self, now_ms: i64) -> HostStatus {
        let raw = self.read_raw(now_ms);
        let last = self.state.lock().last;
        let (cpu_pct, net) = last.map_or((None, None), |l| self.deltas(&l, &raw));
        HostStatus {
            uptime_s: self.started.elapsed().as_secs(),
            container: self.container(),
            cores: self.cores,
            load: load_average(),
            cpu_pct,
            disk: disk_info(&self.src.data_dir),
            net,
        }
    }

    pub fn ring_len(&self) -> usize {
        self.state.lock().ring.len()
    }

    /// The points of `range`, oldest first, averaged per step (the disk keeps the step's last
    /// value). Starts at the process start after a restart: the ring lives in memory.
    pub fn series(&self, range: SeriesRange, now_ms: i64) -> Vec<HostPoint> {
        let (minutes, per_point) = range.shape();
        let from = now_ms - minutes * MINUTE_MS;
        let step_ms = per_point * MINUTE_MS;
        let st = self.state.lock();
        let mut out: Vec<HostPoint> = Vec::new();
        let mut group: Vec<HostPoint> = Vec::with_capacity(per_point as usize);
        let mut slot = None;
        for p in st.ring.iter().filter(|p| p.ts > from) {
            let s = p.ts.div_euclid(step_ms);
            if slot.is_some_and(|x| x != s) {
                out.push(average(&group));
                group.clear();
            }
            slot = Some(s);
            group.push(*p);
        }
        if !group.is_empty() {
            out.push(average(&group));
        }
        out
    }
}

fn cgroup1_usage_path(root: &Path) -> Option<PathBuf> {
    ["cpuacct/cpuacct.usage", "cpu,cpuacct/cpuacct.usage"]
        .iter()
        .map(|p| root.join(p))
        .find(|p| p.exists())
}

fn average(group: &[HostPoint]) -> HostPoint {
    let avg = |f: fn(&HostPoint) -> Option<f64>| {
        let vals: Vec<f64> = group.iter().filter_map(f).collect();
        (!vals.is_empty()).then(|| vals.iter().sum::<f64>() / vals.len() as f64)
    };
    let last = group[group.len() - 1];
    HostPoint {
        ts: group[0].ts,
        cpu_pct: avg(|p| p.cpu_pct),
        load1: avg(|p| p.load1),
        mem_committed: group.iter().map(|p| p.mem_committed).sum::<u64>() / group.len() as u64,
        disk_free: last.disk_free,
        rx: avg(|p| p.rx),
        tx: avg(|p| p.tx),
    }
}

fn load_average() -> Option<[f64; 3]> {
    let mut l = [0f64; 3];
    // SAFETY: `l` is a valid, exclusively borrowed buffer of the 3 doubles the call is told about.
    let n = unsafe { libc::getloadavg(l.as_mut_ptr(), 3) };
    (n == 3).then_some(l)
}

fn disk_info(path: &Path) -> Option<DiskInfo> {
    let c = CString::new(path.as_os_str().as_bytes()).ok()?;
    // SAFETY: `statvfs` is a plain C struct of integers, for which all-zero bytes are a valid value.
    let mut st: libc::statvfs = unsafe { std::mem::zeroed() };
    // SAFETY: `c` is a valid NUL-terminated path that outlives the call, and `st` is a valid,
    // exclusively borrowed out-pointer.
    if unsafe { libc::statvfs(c.as_ptr(), &mut st) } != 0 {
        return None;
    }
    #[allow(clippy::unnecessary_cast)] // the field widths differ between platforms
    let (blocks, avail, frsize) = (st.f_blocks as u64, st.f_bavail as u64, st.f_frsize as u64);
    Some(DiskInfo {
        path_label: DISK_LABEL,
        total_bytes: blocks.saturating_mul(frsize),
        free_bytes: avail.saturating_mul(frsize),
    })
}
