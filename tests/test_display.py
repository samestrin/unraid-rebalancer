"""Tests for terminal display."""

import os
from unittest.mock import patch

import pytest

from rebalancer import (
    DiskInfo,
    PlanEntry,
    PlanDB,
    format_bytes,
    format_disk_table,
    format_plan_summary,
    format_plan_summary_db,
    _format_status_breakdown,
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

    def test_disk_summary_header_appears(self):
        disks = [DiskInfo("/mnt/disk1", 1000, 900, 100, 90)]
        table = format_disk_table(disks)
        assert "Disk Summary:" in table

    def test_blank_line_after_disk_summary_header(self):
        disks = [DiskInfo("/mnt/disk1", 1000, 900, 100, 90)]
        table = format_disk_table(disks)
        lines = table.split("\n")
        assert "Disk Summary:" in lines[0]
        assert lines[1] == ""

    def test_columns_aligned_with_ansi_colors(self):
        """ANSI escape codes should not break Use% column alignment with header."""
        import re
        disks = [
            DiskInfo("/mnt/disk1", 16_000_000_000_000, 14_000_000_000_000, 2_000_000_000_000, 97),
            DiskInfo("/mnt/disk2", 16_000_000_000_000, 4_000_000_000_000, 12_000_000_000_000, 25),
        ]
        table = format_disk_table(disks, max_used=80)
        lines = table.split("\n")
        ansi_re = re.compile(r'\033\[[0-9;]*m')
        sep_idx = next(i for i, l in enumerate(lines) if l.startswith("-"))
        header_stripped = ansi_re.sub('', lines[sep_idx - 1])
        data_lines = [l for l in lines[sep_idx + 1:] if l.strip()]
        stripped_data = [ansi_re.sub('', l) for l in data_lines]
        # Data lines should match header width
        for i, line in enumerate(stripped_data):
            assert len(line) == len(header_stripped), (
                f"Data line {i} width {len(line)} != header width {len(header_stripped)}: "
                f"'{line}' vs '{header_stripped}'"
            )

    def test_separator_line_at_least_52_chars(self):
        disks = [DiskInfo("/mnt/disk1", 1000, 900, 100, 90)]
        table = format_disk_table(disks)
        sep_lines = [l for l in table.split("\n") if l.startswith("-")]
        assert len(sep_lines) > 0
        assert len(sep_lines[0]) >= 52


class TestFormatPlanSummary:
    def test_plan_summary_header_with_colon(self):
        entries = [PlanEntry("/a", 100, "/s", "/t", status="pending")]
        summary = format_plan_summary(entries)
        assert "Plan Summary:" in summary

    def test_blank_line_after_header(self):
        entries = [PlanEntry("/a", 100, "/s", "/t", status="pending")]
        summary = format_plan_summary(entries)
        lines = summary.split("\n")
        assert any("Plan Summary:" in l for l in lines)
        # Find header line index
        hdr_idx = next(i for i, l in enumerate(lines) if "Plan Summary:" in l)
        assert lines[hdr_idx + 1] == ""

    def test_status_label_title_case(self):
        entries = [
            PlanEntry("/a", 100, "/s", "/t", status="pending"),
            PlanEntry("/b", 200, "/s", "/t", status="in_progress"),
            PlanEntry("/c", 300, "/s", "/t", status="cleaned"),
        ]
        summary = format_plan_summary(entries)
        # Labels should be Title Case, not snake_case
        assert "  Pending" in summary
        assert "  In Progress" in summary
        assert "  Cleaned" in summary
        # Raw snake_case should not appear as a label
        assert "\n  pending" not in summary
        assert "\n  in_progress" not in summary

    def test_percentage_format(self):
        entries = [
            PlanEntry("/a", 100, "/s", "/t", status="pending"),
            PlanEntry("/b", 200, "/s", "/t", status="pending"),
            PlanEntry("/c", 300, "/s", "/t", status="cleaned"),
        ]
        summary = format_plan_summary(entries)
        # 2/3 = 66.7%, 1/3 = 33.3%
        assert "( 66.7%)" in summary
        assert "( 33.3%)" in summary

    def test_remaining_before_status_breakdown(self):
        entries = [
            PlanEntry("/a", 1_000_000_000, "/s", "/t", status="pending"),
            PlanEntry("/b", 2_000_000_000, "/s", "/t", status="cleaned"),
        ]
        summary = format_plan_summary(entries)
        lines = summary.split("\n")
        remaining_idx = next(i for i, l in enumerate(lines) if "Remaining:" in l)
        pending_idx = next(i for i, l in enumerate(lines) if "  Pending" in l)
        assert remaining_idx < pending_idx

    def test_zero_count_always_show_statuses(self):
        entries = [PlanEntry("/a", 100, "/s", "/t", status="pending")]
        summary = format_plan_summary(entries)
        # pending, in_progress, cleaned always shown
        assert "  Pending" in summary
        assert "  In Progress" in summary
        assert "  Cleaned" in summary

    def test_zero_count_skipped_statuses_omitted(self):
        entries = [PlanEntry("/a", 100, "/s", "/t", status="pending")]
        summary = format_plan_summary(entries)
        assert "Skipped" not in summary
        assert "Error" not in summary

    def test_remaining_includes_in_progress_bytes(self):
        """Remaining should count both pending and in_progress entries."""
        entries = [
            PlanEntry("/a", 1_000_000_000, "/s", "/t", status="pending"),
            PlanEntry("/b", 2_000_000_000, "/s", "/t", status="in_progress"),
            PlanEntry("/c", 3_000_000_000, "/s", "/t", status="cleaned"),
        ]
        summary = format_plan_summary(entries)
        # pending + in_progress = 3 GB = 2.8 GB formatted
        assert "Remaining:" in summary
        assert "2.8 GB" in summary

    def test_empty_plan_returns_no_plan_message(self):
        summary = format_plan_summary([])
        assert "No plan entries." in summary

    def test_shows_total_bytes(self):
        entries = [PlanEntry("/a", 1_000_000_000_000, "/s", "/t", status="pending")]
        summary = format_plan_summary(entries)
        assert "TB" in summary or "GB" in summary


class TestFormatPlanSummaryDB:
    def test_header_with_colon(self, state_dir, db_path):
        db = PlanDB(db_path)
        db.write_plan([PlanEntry("/a", 100, "/s", "/t", status="pending")])
        summary = format_plan_summary_db(db)
        assert "Plan Summary:" in summary
        db.close()

    def test_session_transfer_limit_shown(self, state_dir, db_path):
        db = PlanDB(db_path)
        db.write_plan([
            PlanEntry("/a", 100, "/s", "/t", status="in_progress"),
            PlanEntry("/b", 200, "/s", "/t", status="pending"),
        ])
        db.set_meta("session_transfer_limit", "3")
        summary = format_plan_summary_db(db)
        assert "[limit: 3]" in summary
        db.close()

    def test_limit_suffix_only_on_in_progress(self, state_dir, db_path):
        db = PlanDB(db_path)
        db.write_plan([PlanEntry("/a", 100, "/s", "/t", status="pending")])
        db.set_meta("session_transfer_limit", "2")
        summary = format_plan_summary_db(db)
        assert "[limit: 2]" not in summary
        db.close()

    def test_no_meta_key_no_limit_suffix(self, state_dir, db_path):
        db = PlanDB(db_path)
        db.write_plan([PlanEntry("/a", 100, "/s", "/t", status="in_progress")])
        summary = format_plan_summary_db(db)
        assert "  In Progress" in summary
        assert "[limit:" not in summary
        db.close()

    def test_percentages_sum_to_100(self, state_dir, db_path):
        import re
        db = PlanDB(db_path)
        db.write_plan([
            PlanEntry("/a", 100, "/s", "/t", status="pending"),
            PlanEntry("/b", 200, "/s", "/t", status="cleaned"),
            PlanEntry("/c", 300, "/s", "/t", status="error_copy"),
        ])
        summary = format_plan_summary_db(db)
        pcts = [float(m) for m in re.findall(r"\(\s*([\d.]+)%\)", summary)]
        assert sum(pcts) == pytest.approx(100.0, abs=0.2)
        db.close()

    def test_remaining_before_estimated_time(self, state_dir, db_path):
        db = PlanDB(db_path)
        db.write_plan([PlanEntry("/a", 1_000_000_000, "/s", "/t", status="pending")])
        # Record throughput so ETA shows up
        db.record_copy_throughput(1_000_000_000, 100.0)
        summary = format_plan_summary_db(db)
        lines = summary.split("\n")
        remaining_idx = next(i for i, l in enumerate(lines) if "Remaining:" in l)
        eta_idx = next(i for i, l in enumerate(lines) if "Estimated time:" in l)
        assert remaining_idx < eta_idx
        db.close()

    def test_remaining_includes_in_progress_bytes_db(self, state_dir, db_path):
        """DB-based remaining should count both pending and in_progress."""
        db = PlanDB(db_path)
        db.write_plan([
            PlanEntry("/a", 1_000_000_000, "/s", "/t", status="pending"),
            PlanEntry("/b", 2_000_000_000, "/s", "/t", status="in_progress"),
            PlanEntry("/c", 3_000_000_000, "/s", "/t", status="cleaned"),
        ])
        summary = format_plan_summary_db(db)
        assert "Remaining:" in summary
        assert "2.8 GB" in summary
        db.close()

    def test_empty_plan_returns_no_plan_message(self, state_dir, db_path):
        db = PlanDB(db_path)
        summary = format_plan_summary_db(db)
        assert "No plan entries." in summary
        db.close()


class TestFormatStatusBreakdown:
    def test_zero_total_entries_with_nonzero_count_no_crash(self):
        """Should not raise ZeroDivisionError when total_entries is 0."""
        result = _format_status_breakdown({"pending": 1}, total_entries=0)
        assert result == []

    def test_zero_total_entries_with_zero_counts(self):
        """Zero total with zero counts should return empty list."""
        result = _format_status_breakdown({"pending": 0}, total_entries=0)
        assert result == []
