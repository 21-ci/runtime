//! Memory budget auto-detection: `memory.budget_mb`, else 85 % of cgroup v2
//! `memory.max`, else 85 % of physical RAM.

use std::path::Path;

/// Percentage of the container/host limit used as the default budget.
pub const BUDGET_FRACTION_PERCENT: u64 = 85;

/// Where a budget came from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BudgetSource {
    /// `memory.budget_mb` was set.
    Config,
    /// cgroup v2 `memory.max`.
    CgroupV2,
    /// cgroup v1 `memory.limit_in_bytes`.
    CgroupV1,
    /// Physical RAM (`sysconf`).
    PhysicalRam,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Budget {
    pub bytes: u64,
    pub source: BudgetSource,
}

impl Budget {
    /// A budget the node can run with: `Err` when nothing was configured and neither a cgroup
    /// limit nor physical RAM could be read (0 bytes would refuse every reservation, GOV-006).
    pub fn check(&self) -> Result<(), String> {
        if self.bytes == 0 {
            Err(
                "could not detect the memory budget (no cgroup limit, physical RAM unknown): set memory.budget_mb"
                    .into(),
            )
        } else {
            Ok(())
        }
    }
}

/// 85 % of `limit_bytes` (MEM-08: `memory.max = 2g` ⇒ 1,740 MB).
pub fn budget_from_limit(limit_bytes: u64) -> u64 {
    limit_bytes / 100 * BUDGET_FRACTION_PERCENT + (limit_bytes % 100) * BUDGET_FRACTION_PERCENT / 100
}

/// Parse the content of a cgroup limit file. `max` / empty / unparsable ⇒ `None` (unlimited).
pub fn parse_cgroup_limit(content: &str) -> Option<u64> {
    let t = content.trim();
    if t.is_empty() || t == "max" {
        return None;
    }
    let v: u64 = t.parse().ok()?;
    // cgroup v1 reports "unlimited" as a huge page-aligned number.
    if v >= (1 << 60) {
        None
    } else {
        Some(v)
    }
}

/// Detect the budget using the real `/sys/fs/cgroup` and `sysconf`.
/// `configured_mb == 0` means auto.
pub fn detect_budget(configured_mb: u64) -> Budget {
    detect_budget_with(
        Path::new("/proc/self/cgroup"),
        Path::new("/sys/fs/cgroup"),
        configured_mb,
        physical_ram_bytes(),
    )
}

/// Testable core of [`detect_budget`]: `proc_cgroup` is `/proc/self/cgroup`, `cgroup_root` is
/// `/sys/fs/cgroup`.
pub fn detect_budget_with(proc_cgroup: &Path, cgroup_root: &Path, configured_mb: u64, physical_ram: u64) -> Budget {
    if configured_mb == 0 {
        // Outside a cgroup namespace the limit lives in the process's own (nested) cgroup.
        if let Some(limit) = own_cgroup_v2_limit(proc_cgroup, cgroup_root) {
            return Budget {
                bytes: budget_from_limit(limit),
                source: BudgetSource::CgroupV2,
            };
        }
    }
    detect_budget_in(cgroup_root, configured_mb, physical_ram)
}

/// `memory.max` of the cgroup named by the `0::<path>` line of `proc_cgroup`, if numeric.
fn own_cgroup_v2_limit(proc_cgroup: &Path, root: &Path) -> Option<u64> {
    let table = std::fs::read_to_string(proc_cgroup).ok()?;
    let rel = table
        .lines()
        .find_map(|l| l.strip_prefix("0::"))?
        .trim()
        .trim_start_matches('/');
    if rel.is_empty() || rel.split('/').any(|c| c == "..") {
        return None;
    }
    parse_cgroup_limit(&std::fs::read_to_string(root.join(rel).join("memory.max")).ok()?)
}

/// Testable core of [`detect_budget`]: `cgroup_root` is the directory containing `memory.max`
/// (v2) or `memory/memory.limit_in_bytes` (v1); `physical_ram` is used when no cgroup limit exists.
pub fn detect_budget_in(cgroup_root: &Path, configured_mb: u64, physical_ram: u64) -> Budget {
    if configured_mb > 0 {
        return Budget {
            bytes: configured_mb.saturating_mul(1024 * 1024),
            source: BudgetSource::Config,
        };
    }
    if let Some(limit) = std::fs::read_to_string(cgroup_root.join("memory.max"))
        .ok()
        .as_deref()
        .and_then(parse_cgroup_limit)
    {
        return Budget {
            bytes: budget_from_limit(limit),
            source: BudgetSource::CgroupV2,
        };
    }
    if let Some(limit) = std::fs::read_to_string(cgroup_root.join("memory/memory.limit_in_bytes"))
        .ok()
        .as_deref()
        .and_then(parse_cgroup_limit)
    {
        return Budget {
            bytes: budget_from_limit(limit),
            source: BudgetSource::CgroupV1,
        };
    }
    Budget {
        bytes: budget_from_limit(physical_ram),
        source: BudgetSource::PhysicalRam,
    }
}

/// Physical RAM in bytes via `sysconf` (0 if unavailable).
pub fn physical_ram_bytes() -> u64 {
    // SAFETY: sysconf has no preconditions.
    let (pages, page) = unsafe { (libc::sysconf(libc::_SC_PHYS_PAGES), libc::sysconf(libc::_SC_PAGESIZE)) };
    ram_bytes(pages, page)
}

/// `pages * page`; 0 when either `sysconf` value is unknown (reported as -1).
fn ram_bytes(pages: libc::c_long, page: libc::c_long) -> u64 {
    if pages > 0 && page > 0 {
        (pages as u64).saturating_mul(page as u64)
    } else {
        0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_file(dir: &Path, name: &str, content: &str) -> std::path::PathBuf {
        let p = dir.join(name);
        std::fs::create_dir_all(p.parent().unwrap()).unwrap();
        std::fs::write(&p, content).unwrap();
        p
    }

    #[test]
    fn the_own_cgroup_limit_is_read_from_the_nested_cgroup_named_in_proc() {
        let dir = tempfile::tempdir().unwrap();
        let proc = temp_file(dir.path(), "proc/self/cgroup", "0::/app/task\n");
        temp_file(dir.path(), "root/app/task/memory.max", "2147483648\n");
        assert_eq!(
            own_cgroup_v2_limit(&proc, &dir.path().join("root")),
            Some(2_147_483_648)
        );
    }

    #[test]
    fn an_unlimited_own_cgroup_or_a_missing_proc_file_gives_no_limit() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("root");
        let unlimited = temp_file(dir.path(), "proc/unlimited", "0::/app\n");
        temp_file(dir.path(), "root/app/memory.max", "max\n");
        assert_eq!(own_cgroup_v2_limit(&unlimited, &root), None);
        assert_eq!(own_cgroup_v2_limit(&dir.path().join("no-such-file"), &root), None);
    }

    #[test]
    fn an_empty_or_parent_relative_cgroup_path_is_never_followed() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("root");
        temp_file(dir.path(), "root/memory.max", "1024\n");
        temp_file(dir.path(), "escape/memory.max", "1024\n");
        let at_root = temp_file(dir.path(), "proc/root", "0::/\n");
        let escaping = temp_file(dir.path(), "proc/escape", "0::/../escape\n");
        assert_eq!(own_cgroup_v2_limit(&at_root, &root), None);
        assert_eq!(own_cgroup_v2_limit(&escaping, &root), None);
    }

    #[test]
    fn a_host_with_an_own_cgroup_limit_gets_a_cgroup_v2_budget_before_physical_ram() {
        let dir = tempfile::tempdir().unwrap();
        let proc = temp_file(dir.path(), "proc/cgroup", "0::/app\n");
        let root = dir.path().join("root");
        temp_file(dir.path(), "root/app/memory.max", "1000000000\n");
        let b = detect_budget_with(&proc, &root, 0, 64 << 30);
        assert_eq!(
            b,
            Budget {
                bytes: budget_from_limit(1_000_000_000),
                source: BudgetSource::CgroupV2
            }
        );
    }

    #[test]
    fn a_configured_budget_ignores_the_own_cgroup_limit() {
        let dir = tempfile::tempdir().unwrap();
        let proc = temp_file(dir.path(), "proc/cgroup", "0::/app\n");
        let root = dir.path().join("root");
        temp_file(dir.path(), "root/app/memory.max", "1000000000\n");
        let b = detect_budget_with(&proc, &root, 512, 64 << 30);
        assert_eq!(
            b,
            Budget {
                bytes: 512 * 1024 * 1024,
                source: BudgetSource::Config
            }
        );
    }

    #[test]
    fn ram_bytes_is_pages_times_page_size_and_zero_when_sysconf_fails() {
        assert_eq!(ram_bytes(4, 4096), 16_384);
        assert_eq!(ram_bytes(-1, 4096), 0);
        assert_eq!(ram_bytes(4, -1), 0);
        assert_eq!(ram_bytes(0, 4096), 0);
    }
}
