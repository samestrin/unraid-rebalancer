"""Tests for terminal display."""

import os
from unittest.mock import patch

import pytest

from rebalancer import (
    DiskInfo,
    PlanEntry,
    format_bytes,
    format_disk_table,
    format_plan_summary,
    ANSI,
)


class TestFormatBytes:
    def test_bytes(self):
        assert format_bytes(500) == "500 B"

    def test_kilobytes(self):
        assert "1.5 KB" in format_bytes(1536)

    def test_megabytes(self):
        assert "MB" in format_bytes(1_500_000)

    def test_gigabytes(self):
        assert "GB" in format_bytes(1_500_000_000)

    def test_terabytes(self):
        assert "TB" in format_bytes(1_500_000_000_000)

    def test_zero(self):
        assert format_bytes(0) == "0 B"

    def test_does_not_return_float_for_bytes(self):
        """C2: format_bytes should return clean integer for byte-range values."""
        result = format_bytes(500)
        assert result == "500 B"
        # Must not contain a decimal point for bytes
        assert "." not in result

    def test_large_value_returns_clean_format(self):
        """C2: ensure no floating point artifacts in formatted output."""
        # 1.5 TB
        result = format_bytes(1_649_267_441_664)
        assert "TB" in result
        # Should be a clean decimal like "1.5 TB", not "1.4999999..."
        parts = result.split()
        num = float(parts[0])
        assert num == round(num, 1)


class TestFormatDiskTable:
    def test_includes_all_disks(self):
        disks = [
            DiskInfo("/mnt/disk1", 16_000_000_000_000, 14_000_000_000_000, 2_000_000_000_000, 87),
            DiskInfo("/mnt/disk2", 16_000_000_000_000, 4_000_000_000_000, 12_000_000_000_000, 25),
        ]
        table = format_disk_table(disks)
        assert "disk1" in table
        assert "disk2" in table
        assert "87%" in table
        assert "25%" in table

    def test_no_color_env(self):
        disks = [DiskInfo("/mnt/disk1", 1000, 900, 100, 90)]
        with patch.dict(os.environ, {"NO_COLOR": "1"}):
            table = format_disk_table(disks)
            assert "\033[" not in table

    def test_color_thresholds_match_max_used(self):
        """Disks above max_used should be red, below should be green/yellow."""
        disks = [
            DiskInfo("/mnt/disk1", 1000, 900, 100, 90),  # above 80 -> red
            DiskInfo("/mnt/disk2", 1000, 750, 250, 75),  # 70-80 -> yellow
            DiskInfo("/mnt/disk3", 1000, 500, 500, 50),  # below 70 -> green
        ]
        table = format_disk_table(disks, max_used=80)
        assert "\033[31m" in table  # red for 90%
        assert "\033[32m" in table  # green for 50%

    def test_empty_disks(self):
        table = format_disk_table([])
        assert "Disk" in table  # header should still appear


class TestFormatPlanSummary:
    def test_counts_by_status(self):
        entries = [
            PlanEntry("/a", 100, "/s", "/t", status="pending"),
            PlanEntry("/b", 200, "/s", "/t", status="pending"),
            PlanEntry("/c", 300, "/s", "/t", status="completed"),
            PlanEntry("/d", 400, "/s", "/t", status="cleaned"),
        ]
        summary = format_plan_summary(entries)
        assert "pending" in summary.lower()
        assert "2" in summary

    def test_empty_plan(self):
        summary = format_plan_summary([])
        assert "No plan" in summary

    def test_shows_total_bytes(self):
        entries = [
            PlanEntry("/a", 1_000_000_000_000, "/s", "/t", status="pending"),
        ]
        summary = format_plan_summary(entries)
        assert "TB" in summary or "GB" in summary
