#!/usr/bin/env python3
"""Tests for coverage_report.py: lcov parsing, exclusions, grouping and the exit code."""

import io
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path

import coverage_report as cr


class CoverageReportTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        src = self.root / "crates" / "rt-demo" / "src"
        src.mkdir(parents=True)
        self.lib = src / "lib.rs"
        self.lib.write_text(
            "fn a() {}\n"  # 1 hit
            "fn b() {}\n"  # 2 missed
            "unreachable!(); // LCOV_EXCL_LINE: the type system rules this out\n"  # 3 excluded
            "// LCOV_EXCL_START: OS failure path\n"  # 4
            "panic!();\n"  # 5
            "// LCOV_EXCL_STOP\n"  # 6
            "fn c() {}\n",  # 7 hit
            encoding="utf-8",
        )
        (self.root / "tools").mkdir()
        self.py = self.root / "tools" / "x.py"
        self.py.write_text("a = 1\n", encoding="utf-8")

    def tearDown(self):
        self.tmp.cleanup()

    def lcov(self, body: str) -> Path:
        p = self.root / "lcov.info"
        p.write_text(body, encoding="utf-8")
        return p

    def run_report(self, *args):
        out, err = io.StringIO(), io.StringIO()
        with redirect_stdout(out), redirect_stderr(err):
            rc = cr.main([*map(str, args), "--root", str(self.root)])
        return rc, out.getvalue(), err.getvalue()

    def demo_lcov(self, hit_b: int = 0) -> Path:
        return self.lcov(
            f"SF:{self.lib}\nFN:1,a\nFN:2,b\nFNDA:1,a\nFNDA:{hit_b},b\n"
            f"DA:1,1\nDA:2,{hit_b}\nDA:3,0\nDA:5,0\nDA:7,4\nend_of_record\n"
            f"SF:{self.py}\nDA:1,1\nend_of_record\n"
        )

    def test_exclusions_drop_lines_and_missed_lines_count(self):
        rc, out, _ = self.run_report(self.demo_lcov(), "--missing", "--exclusions")
        self.assertEqual(rc, 0)
        demo = next(line for line in out.splitlines() if line.startswith("rt-demo"))
        # 3 counted lines (1, 2, 7), 1 missed, 2 excluded (3 and 5).
        self.assertEqual(demo.split()[1:4], ["1", "3", "1"])
        self.assertTrue(demo.rstrip().endswith("2"))
        self.assertIn("crates/rt-demo/src/lib.rs: 2", out)
        self.assertIn("the type system rules this out", out)
        self.assertIn("tools", out)

    def test_fail_under_and_module_filter(self):
        rc, _, err = self.run_report(self.demo_lcov(), "--fail-under", "100")
        self.assertEqual(rc, 2)
        self.assertIn("rt-demo", err)
        rc, out, _ = self.run_report(self.demo_lcov(), "--module", "tools", "--fail-under", "100")
        self.assertEqual(rc, 0)
        self.assertNotIn("rt-demo", out)

    def test_a_module_floor_overrides_the_global_one(self):
        rc, _, err = self.run_report(self.demo_lcov(), "--fail-under", "100", "--floor", "rt-demo=60")
        self.assertEqual(rc, 0, err)
        rc, _, err = self.run_report(self.demo_lcov(), "--fail-under", "50", "--floor", "rt-demo=90")
        self.assertEqual(rc, 2)
        self.assertIn("below 90.00%: rt-demo", err)

    def test_records_of_the_same_file_merge(self):
        once = f"SF:{self.lib}\nDA:2,0\nend_of_record\n"
        p = self.lcov(once + f"SF:{self.lib}\nDA:2,3\nend_of_record\n")
        files = cr.parse_lcov(p)
        self.assertEqual(len(files), 1)
        self.assertEqual(files[0].lines[2], 3)

    def test_marker_without_reason_is_an_error(self):
        self.lib.write_text("x(); // LCOV_EXCL_LINE\n", encoding="utf-8")
        rc, _, err = self.run_report(self.lcov(f"SF:{self.lib}\nDA:1,0\nend_of_record\n"))
        self.assertEqual(rc, 2)
        self.assertIn("needs a reason", err)

    def test_unbalanced_markers_are_errors(self):
        self.lib.write_text("// LCOV_EXCL_STOP\n// LCOV_EXCL_START: why\nx();\n", encoding="utf-8")
        rc, _, err = self.run_report(self.lcov(f"SF:{self.lib}\nDA:3,0\nend_of_record\n"))
        self.assertEqual(rc, 2)
        self.assertIn("STOP without START", err)
        self.assertIn("START without STOP", err)

    def test_markdown_and_ranges(self):
        md = self.root / "summary.md"
        self.run_report(self.demo_lcov(hit_b=1), "--markdown", md)
        text = md.read_text(encoding="utf-8")
        self.assertIn("| rt-demo | 1 | 3 | 0 | 100.00%", text)
        self.assertEqual(cr.ranges([1, 2, 3, 7, 9, 10]), "1-3, 7, 9-10")

    def test_no_data_for_selection(self):
        rc, _, err = self.run_report(self.demo_lcov(), "--module", "rt-none")
        self.assertEqual(rc, 1)
        self.assertIn("no coverage data", err)


if __name__ == "__main__":
    unittest.main()
