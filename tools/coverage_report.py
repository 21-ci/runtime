#!/usr/bin/env python3
"""Coverage report: per-module line and function coverage from an lcov file.

Reads the lcov.info that scripts/coverage.sh writes (Rust through cargo-llvm-cov, Python through coverage.py),
applies lcov's exclusion markers, groups files by module (crates/<name>, tools) and prints a table. It also
writes summary.md and, with --missing, the uncovered line ranges of every file.

Exclusion markers (the lcov convention) sit in a `//` or `#` comment and carry a mandatory reason:
LCOV_EXCL_LINE excludes its own line; LCOV_EXCL_START ... LCOV_EXCL_STOP excludes every line in between.
Each LINE and START is followed by `: <reason>`; a marker without one is an error, because an exclusion
has to say why the line cannot be tested.
"""

import argparse
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path

# Only in a comment: the marker names in this file's docs and code are not markers.
MARKER = re.compile(r"(?://|#)\s*LCOV_EXCL_(LINE|START|STOP)(?::\s*(\S.*))?")


@dataclass
class FileCov:
    path: str
    lines: dict = field(default_factory=dict)  # line -> hit count
    functions: dict = field(default_factory=dict)  # name -> hit count
    excluded: int = 0


@dataclass
class Totals:
    lines: int = 0
    lines_hit: int = 0
    functions: int = 0
    functions_hit: int = 0
    excluded: int = 0
    files: int = 0

    def add(self, f: FileCov):
        self.lines += len(f.lines)
        self.lines_hit += sum(1 for c in f.lines.values() if c > 0)
        self.functions += len(f.functions)
        self.functions_hit += sum(1 for c in f.functions.values() if c > 0)
        self.excluded += f.excluded
        self.files += 1

    @staticmethod
    def pct(hit: int, total: int) -> float:
        return 100.0 if total == 0 else 100.0 * hit / total

    @property
    def line_pct(self) -> float:
        return self.pct(self.lines_hit, self.lines)

    @property
    def function_pct(self) -> float:
        return self.pct(self.functions_hit, self.functions)


def parse_lcov(path: Path) -> list:
    """Parse lcov records; the same source file may appear several times (one per test binary), so merge."""
    files: dict = {}
    cur = None
    for raw in path.read_text(encoding="utf-8", errors="replace").splitlines():
        if raw.startswith("SF:"):
            p = raw[3:]
            cur = files.setdefault(p, FileCov(p))
        elif cur is None:
            continue
        elif raw.startswith("DA:"):
            line, count = raw[3:].split(",")[:2]
            n = int(line)
            cur.lines[n] = cur.lines.get(n, 0) + int(float(count))
        elif raw.startswith("FNDA:"):
            count, name = raw[5:].split(",", 1)
            cur.functions[name] = cur.functions.get(name, 0) + int(float(count))
        elif raw.startswith("FN:"):
            name = raw[3:].split(",", 1)[1]
            cur.functions.setdefault(name, 0)
        elif raw == "end_of_record":
            cur = None
    return list(files.values())


def apply_exclusions(f: FileCov, errors: list, listing: list):
    """Drop lines covered by LCOV_EXCL markers; record every exclusion with its reason."""
    try:
        source = Path(f.path).read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        return
    excluded = set()
    start = None
    for i, text in enumerate(source, 1):
        m = MARKER.search(text)
        if not m:
            if start is not None:
                excluded.add(i)
            continue
        kind, reason = m.group(1), m.group(2)
        if kind in ("LINE", "START") and not reason:
            errors.append(f"{f.path}:{i}: LCOV_EXCL_{kind} needs a reason (LCOV_EXCL_{kind}: <why>)")
        if kind == "LINE":
            excluded.add(i)
            listing.append(f"{f.path}:{i}  {reason or '(no reason)'}")
        elif kind == "START":
            if start is not None:
                errors.append(f"{f.path}:{i}: LCOV_EXCL_START inside another START (line {start[0]})")
            start = (i, reason)
            excluded.add(i)
        else:
            if start is None:
                errors.append(f"{f.path}:{i}: LCOV_EXCL_STOP without START")
            else:
                listing.append(f"{f.path}:{start[0]}-{i}  {start[1] or '(no reason)'}")
            excluded.add(i)
            start = None
    if start is not None:
        errors.append(f"{f.path}:{start[0]}: LCOV_EXCL_START without STOP")
    before = len(f.lines)
    f.lines = {n: c for n, c in f.lines.items() if n not in excluded}
    f.excluded = before - len(f.lines)


def module_of(path: str, root: Path) -> str:
    try:
        rel = Path(path).resolve().relative_to(root)
    except ValueError:
        return "(outside repo)"
    parts = rel.parts
    if len(parts) >= 2 and parts[0] == "crates":
        return parts[1]
    return parts[0] if parts else "(root)"


def ranges(numbers: list) -> str:
    """[1,2,3,7,9,10] -> '1-3, 7, 9-10'."""
    out = []
    for n in sorted(numbers):
        if out and n == out[-1][1] + 1:
            out[-1][1] = n
        else:
            out.append([n, n])
    return ", ".join(str(a) if a == b else f"{a}-{b}" for a, b in out)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("lcov", type=Path, help="lcov.info to read")
    ap.add_argument("--root", type=Path, default=Path(__file__).resolve().parent.parent, help="repository root")
    ap.add_argument("--module", action="append", default=[], help="only report this module (repeatable)")
    ap.add_argument("--missing", action="store_true", help="list the uncovered lines of every file")
    ap.add_argument("--exclusions", action="store_true", help="list every LCOV_EXCL exclusion with its reason")
    ap.add_argument("--fail-under", type=float, default=None, help="exit 2 if any module's line %% is below this")
    ap.add_argument("--floor", action="append", default=[], metavar="MODULE=PCT",
                    help="a module's own --fail-under (repeatable), e.g. tools=45")
    ap.add_argument("--markdown", type=Path, default=None, help="also write the summary as markdown here")
    args = ap.parse_args(argv)

    root = args.root.resolve()
    files = parse_lcov(args.lcov)
    errors: list = []
    listing: list = []
    by_module: dict = {}
    for f in files:
        mod = module_of(f.path, root)
        if args.module and mod not in args.module:
            continue
        apply_exclusions(f, errors, listing)
        by_module.setdefault(mod, []).append(f)

    if not by_module:
        print("no coverage data for the selected modules", file=sys.stderr)
        return 1

    rows = []
    total = Totals()
    for mod in sorted(by_module):
        t = Totals()
        for f in by_module[mod]:
            t.add(f)
            total.add(f)
        rows.append((mod, t))

    header = f"{'module':<16} {'files':>5} {'lines':>8} {'missed':>7} {'line %':>7} {'fn %':>7} {'excl':>5}"
    table = [header, "-" * len(header)]
    for mod, t in rows + [("TOTAL", total)]:
        if mod == "TOTAL":
            table.append("-" * len(header))
        table.append(
            f"{mod:<16} {t.files:>5} {t.lines:>8} {t.lines - t.lines_hit:>7} "
            f"{t.line_pct:>6.2f}% {t.function_pct:>6.2f}% {t.excluded:>5}"
        )
    print("\n".join(table))

    if args.missing:
        print("\nUncovered lines")
        for mod, _ in rows:
            for f in sorted(by_module[mod], key=lambda f: f.path):
                missed = [n for n, c in f.lines.items() if c == 0]
                if missed:
                    rel = Path(f.path).resolve()
                    rel = rel.relative_to(root) if rel.is_relative_to(root) else rel
                    print(f"  {rel}: {ranges(missed)}")

    if args.exclusions and listing:
        print(f"\nExclusions ({len(listing)})")
        for item in listing:
            print(f"  {item}")

    if args.markdown:
        md = ["| module | files | lines | missed | line % | fn % | excluded |", "|---|---:|---:|---:|---:|---:|---:|"]
        for mod, t in rows + [("**TOTAL**", total)]:
            md.append(
                f"| {mod} | {t.files} | {t.lines} | {t.lines - t.lines_hit} | {t.line_pct:.2f}% "
                f"| {t.function_pct:.2f}% | {t.excluded} |"
            )
        args.markdown.write_text("# Coverage\n\n" + "\n".join(md) + "\n", encoding="utf-8")

    rc = 0
    if errors:
        print("\nExclusion marker errors", file=sys.stderr)
        for e in errors:
            print(f"  {e}", file=sys.stderr)
        rc = 2
    floors = dict(f.split("=", 1) for f in args.floor)
    if args.fail_under is not None or floors:
        low = []
        for m, t in rows:
            need = float(floors[m]) if m in floors else args.fail_under
            if need is not None and t.line_pct < need:
                low.append((m, t.line_pct, need))
        for m, p, need in low:
            print(f"below {need:.2f}%: {m} ({p:.2f}%)", file=sys.stderr)
        if low:
            rc = 2
    return rc


if __name__ == "__main__":
    sys.exit(main())
