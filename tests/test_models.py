"""Tests for data models and CSV/JSON I/O."""

import json

import pytest

from rebalancer import (
    DiskInfo,
    PlanEntry,
    read_drives_json,
    read_plan_csv,
    write_drives_json,
    write_plan_csv,
)


class TestPlanEntry:
    def test_default_status_is_pending(self):
        e = PlanEntry("/mnt/disk1/TV_Shows/X", 100, "/mnt/disk1", "/mnt/disk10")
        assert e.status == "pending"


# --- CSV I/O ---

class TestPlanCSV:
    def test_write_and_read_roundtrip(self, state_dir):
        path = state_dir / "plan.csv"
        entries = [
            PlanEntry("/mnt/disk4/TV_Shows/Breaking Bad (2008)", 200_000_000_000, "/mnt/disk4", "/mnt/disk11"),
            PlanEntry("/mnt/disk6/Anime/Naruto", 300_000_000_000, "/mnt/disk6", "/mnt/disk10", status="completed"),
        ]
        write_plan_csv(entries, path)
        loaded = read_plan_csv(path)
        assert len(loaded) == 2
        assert loaded[0].path == "/mnt/disk4/TV_Shows/Breaking Bad (2008)"
        assert loaded[0].size_bytes == 200_000_000_000
        assert loaded[0].source_disk == "/mnt/disk4"
        assert loaded[0].target_disk == "/mnt/disk11"
        assert loaded[0].status == "pending"
        assert loaded[1].status == "completed"

    def test_write_is_atomic(self, state_dir):
        """Write uses temp file + rename, so partial writes don't corrupt."""
        path = state_dir / "plan.csv"
        entries = [PlanEntry("/mnt/disk1/X", 100, "/mnt/disk1", "/mnt/disk10")]
        write_plan_csv(entries, path)
        # File should exist and be valid
        assert path.exists()
        loaded = read_plan_csv(path)
        assert len(loaded) == 1

    def test_read_empty_file(self, state_dir):
        """Empty CSV (header only or truly empty) returns empty list."""
        path = state_dir / "plan.csv"
        path.write_text("path,size_bytes,source_disk,target_disk,status\n")
        loaded = read_plan_csv(path)
        assert loaded == []

    def test_read_nonexistent_file(self, state_dir):
        path = state_dir / "nonexistent.csv"
        loaded = read_plan_csv(path)
        assert loaded == []

    def test_csv_with_commas_in_path(self, state_dir):
        """Paths with commas should be handled by proper CSV quoting."""
        path = state_dir / "plan.csv"
        entries = [PlanEntry("/mnt/disk1/TV_Shows/Show, The (2020)", 100, "/mnt/disk1", "/mnt/disk10")]
        write_plan_csv(entries, path)
        loaded = read_plan_csv(path)
        assert loaded[0].path == "/mnt/disk1/TV_Shows/Show, The (2020)"

    def test_csv_preserves_all_fields(self, state_dir):
        path = state_dir / "plan.csv"
        entry = PlanEntry("/mnt/disk4/Movies/2024", 500_000_000_000, "/mnt/disk4", "/mnt/disk11", status="verified")
        write_plan_csv([entry], path)
        loaded = read_plan_csv(path)
        assert loaded[0].path == entry.path
        assert loaded[0].size_bytes == entry.size_bytes
        assert loaded[0].source_disk == entry.source_disk
        assert loaded[0].target_disk == entry.target_disk
        assert loaded[0].status == entry.status

    def test_csv_with_unicode_path(self, state_dir):
        """Anime titles may have Unicode characters."""
        path = state_dir / "plan.csv"
        entries = [PlanEntry("/mnt/disk1/Anime/\u9032\u6483\u306e\u5de8\u4eba", 100, "/mnt/disk1", "/mnt/disk10")]
        write_plan_csv(entries, path)
        loaded = read_plan_csv(path)
        assert loaded[0].path == "/mnt/disk1/Anime/\u9032\u6483\u306e\u5de8\u4eba"

    def test_write_empty_plan(self, state_dir):
        """Empty entries should produce header-only CSV."""
        path = state_dir / "plan.csv"
        write_plan_csv([], path)
        loaded = read_plan_csv(path)
        assert loaded == []
        content = path.read_text()
        assert "path,size_bytes" in content

    def test_write_overwrites_existing(self, state_dir):
        """Writing a new plan replaces the old one atomically."""
        path = state_dir / "plan.csv"
        write_plan_csv([PlanEntry("/old", 1, "/s", "/t")], path)
        write_plan_csv([PlanEntry("/new", 2, "/s", "/t")], path)
        loaded = read_plan_csv(path)
        assert len(loaded) == 1
        assert loaded[0].path == "/new"


# --- JSON I/O ---

class TestDrivesJSON:
    def test_write_and_read_roundtrip(self, state_dir, sample_disks):
        path = state_dir / "drives.json"
        write_drives_json(sample_disks, path)
        loaded = read_drives_json(path)
        assert len(loaded) == len(sample_disks)
        assert loaded[0].path == sample_disks[0].path
        assert loaded[0].total_bytes == sample_disks[0].total_bytes
        assert loaded[0].used_pct == sample_disks[0].used_pct

    def test_read_nonexistent_file(self, state_dir):
        path = state_dir / "nonexistent.json"
        loaded = read_drives_json(path)
        assert loaded == []

    def test_json_is_valid(self, state_dir, sample_disks):
        path = state_dir / "drives.json"
        write_drives_json(sample_disks, path)
        data = json.loads(path.read_text())
        assert isinstance(data, list)
        assert len(data) == len(sample_disks)
        assert "path" in data[0]
        assert "total_bytes" in data[0]
