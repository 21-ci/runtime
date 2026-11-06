//! Host sampling: parsers, deltas, the 24 h ring, absent sources.

use std::fs;
use std::path::Path;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

use rt_obs::hostinfo::{
    parse_cgroup2_usage_us, parse_net_dev, parse_proc_stat, Container, HostSampler, HostSources, SeriesRange,
    RING_POINTS,
};

const MIN: i64 = 60_000;
const T0: i64 = 1_760_000_000_000;

fn write(root: &Path, rel: &str, text: &str) {
    let p = root.join(rel);
    fs::create_dir_all(p.parent().unwrap()).unwrap();
    fs::write(p, text).unwrap();
}

fn proc_stat(busy: u64, idle: u64) -> String {
    format!("cpu  {busy} 0 0 {idle} 0 0 0 0 0 0\ncpu0 1 1 1 1\nintr 5\n")
}

fn net_dev(rx: u64, tx: u64) -> String {
    format!(
        "Inter-|   Receive |  Transmit\n face |bytes packets|bytes\n    lo: 999 1 0 0 0 0 0 0 999 1 0 0 0 0 0 0\n  eth0: {rx} 1 0 0 0 0 0 0 {tx} 1 0 0 0 0 0 0\n"
    )
}

struct Fx {
    dir: tempfile::TempDir,
    now: Arc<AtomicI64>,
    sampler: HostSampler,
}

fn fx() -> Fx {
    let dir = tempfile::tempdir().unwrap();
    let now = Arc::new(AtomicI64::new(T0));
    let sampler = HostSampler::new(HostSources {
        proc_root: dir.path().join("proc"),
        cgroup_root: dir.path().join("cgroup"),
        data_dir: dir.path().to_path_buf(),
    });
    Fx { dir, now, sampler }
}

impl Fx {
    fn sample(&self) {
        self.sampler.sample(self.now.load(Ordering::SeqCst), 7);
    }
    fn at(&self) -> i64 {
        self.now.load(Ordering::SeqCst)
    }
}

#[test]
fn proc_stat_gives_busy_and_total_jiffies() {
    assert_eq!(
        parse_proc_stat("cpu  10 20 30 40 50 60 70 80 0 0\ncpu0 1 1 1 1"),
        Some((10 + 20 + 30 + 60 + 70 + 80, 360))
    );
    assert_eq!(parse_proc_stat("intr 1"), None);
    assert_eq!(parse_proc_stat("cpu  x y"), None);
}

#[test]
fn net_dev_sums_every_interface_but_loopback() {
    let t = format!(
        "{}  eth1: 5 0 0 0 0 0 0 0 6 0 0 0 0 0 0 0\nbroken line\n",
        net_dev(100, 200)
    );
    assert_eq!(parse_net_dev(&t), Some((105, 206)));
    assert_eq!(parse_net_dev("nothing here"), None);
}

#[test]
fn cgroup2_usage_is_read_from_cpu_stat() {
    assert_eq!(parse_cgroup2_usage_us("usage_usec 123\nuser_usec 1\n"), Some(123));
    assert_eq!(parse_cgroup2_usage_us("user_usec 1\n"), None);
}

#[test]
fn container_kind_comes_from_the_cgroup_tree() {
    let f = fx();
    assert_eq!(f.sampler.container(), Container::None);
    write(f.dir.path(), "cgroup/cpuacct/cpuacct.usage", "1");
    assert_eq!(f.sampler.container(), Container::Cgroup1);
    write(f.dir.path(), "cgroup/cgroup.controllers", "cpu io");
    assert_eq!(f.sampler.container(), Container::Cgroup2);
    assert_eq!(Container::Cgroup2.as_str(), "cgroup2");
    assert_eq!(Container::Cgroup1.as_str(), "cgroup1");
    assert_eq!(Container::None.as_str(), "none");
}

#[test]
fn missing_sources_are_none_not_errors() {
    let f = fx();
    f.sample();
    f.now.fetch_add(MIN, Ordering::SeqCst);
    f.sample();
    let live = f.sampler.live(f.at());
    assert_eq!(live.cpu_pct, None);
    assert_eq!(live.net, None);
    assert_eq!(live.container, Container::None);
    assert!(live.cores >= 1);
    assert!(live.disk.is_some(), "the data dir exists");
    let last = *f.sampler.series(SeriesRange::H1, f.at()).last().unwrap();
    assert_eq!((last.cpu_pct, last.rx, last.tx), (None, None, None));
    assert_eq!(last.mem_committed, 7);
}

#[test]
fn the_disk_label_never_carries_the_host_path() {
    let f = fx();
    f.sample();
    let d = f.sampler.live(f.at()).disk.expect("disk");
    assert_eq!(d.path_label, "data");
    assert!(d.total_bytes >= d.free_bytes && d.total_bytes > 0);
    let text = serde_json::to_string(&f.sampler.live(f.at())).unwrap();
    assert!(
        !text.contains(f.dir.path().to_str().unwrap()),
        "no host path in the status: {text}"
    );
}

#[test]
fn proc_cpu_and_net_rates_are_deltas_from_the_last_sample() {
    let f = fx();
    write(f.dir.path(), "proc/stat", &proc_stat(100, 900));
    write(f.dir.path(), "proc/net/dev", &net_dev(1_000, 2_000));
    f.sample();
    assert_eq!(f.sampler.live(f.at()).cpu_pct, None, "no time has passed");

    f.now.fetch_add(MIN, Ordering::SeqCst);
    write(f.dir.path(), "proc/stat", &proc_stat(150, 950)); // 50 busy of 100
    write(f.dir.path(), "proc/net/dev", &net_dev(7_000, 2_600));
    let live = f.sampler.live(f.at());
    assert_eq!(live.cpu_pct, Some(50.0));
    let net = live.net.unwrap();
    assert_eq!((net.rx_bytes_per_s, net.tx_bytes_per_s), (100.0, 10.0));

    f.sample();
    let p = *f.sampler.series(SeriesRange::H1, f.at()).last().unwrap();
    assert_eq!((p.cpu_pct, p.rx, p.tx), (Some(50.0), Some(100.0), Some(10.0)));
    assert_eq!(p.ts, f.at());
}

#[test]
fn counters_that_went_backwards_give_zero_not_a_huge_rate() {
    let f = fx();
    write(f.dir.path(), "proc/net/dev", &net_dev(1_000, 1_000));
    f.sample();
    f.now.fetch_add(MIN, Ordering::SeqCst);
    write(f.dir.path(), "proc/net/dev", &net_dev(10, 10));
    let net = f.sampler.live(f.at()).net.unwrap();
    assert_eq!((net.rx_bytes_per_s, net.tx_bytes_per_s), (0.0, 0.0));
}

#[test]
fn cgroup_cpu_wins_over_proc_stat_and_is_relative_to_all_cores() {
    let f = fx();
    let cores = f.sampler.live(f.at()).cores as f64;
    write(f.dir.path(), "proc/stat", &proc_stat(100, 900));
    write(f.dir.path(), "cgroup/cgroup.controllers", "cpu");
    write(f.dir.path(), "cgroup/cpu.stat", "usage_usec 1000000\n");
    f.sample();
    f.now.fetch_add(MIN, Ordering::SeqCst);
    write(f.dir.path(), "cgroup/cpu.stat", "usage_usec 31000000\n"); // 30 s of CPU in 60 s
    let pct = f.sampler.live(f.at()).cpu_pct.unwrap();
    assert!((pct - 50.0 / cores).abs() < 1e-6, "{pct}");
}

#[test]
fn cgroup1_cpu_is_read_from_cpuacct() {
    let f = fx();
    let cores = f.sampler.live(f.at()).cores as f64;
    write(f.dir.path(), "cgroup/cpuacct/cpuacct.usage", "0\n");
    f.sample();
    f.now.fetch_add(MIN, Ordering::SeqCst);
    write(f.dir.path(), "cgroup/cpuacct/cpuacct.usage", "60000000000\n"); // 60 s
    let pct = f.sampler.live(f.at()).cpu_pct.unwrap();
    assert!((pct - 100.0 / cores).abs() < 1e-6, "{pct}");
}

#[test]
fn the_ring_is_bounded_and_ranges_cap_at_three_hundred_points() {
    let f = fx();
    for _ in 0..RING_POINTS + 50 {
        f.sample();
        f.now.fetch_add(MIN, Ordering::SeqCst);
    }
    assert_eq!(f.sampler.ring_len(), RING_POINTS);
    let now = f.at();
    assert_eq!(SeriesRange::parse("1h"), Some(SeriesRange::H1));
    assert_eq!(SeriesRange::parse("6h"), Some(SeriesRange::H6));
    assert_eq!(SeriesRange::parse("24h"), Some(SeriesRange::H24));
    assert_eq!(SeriesRange::parse("2h"), None);
    let h1 = f.sampler.series(SeriesRange::H1, now);
    assert!((59..=61).contains(&h1.len()), "{}", h1.len());
    let h24 = f.sampler.series(SeriesRange::H24, now);
    assert!(h24.len() <= 300 && h24.len() >= 280, "{}", h24.len());
    let h6 = f.sampler.series(SeriesRange::H6, now);
    assert!(h6.len() <= 300 && h6.len() >= 70, "{}", h6.len());
    assert!(h24.windows(2).all(|w| w[0].ts < w[1].ts), "oldest first");
    assert!(h24.iter().all(|p| p.mem_committed == 7), "averages of equal values");
}
