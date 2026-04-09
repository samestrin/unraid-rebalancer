"""Tests for duplicate detection and resolution."""

from unittest.mock import MagicMock

import pytest

from rebalancer import (
    MovableUnit,
    find_duplicates,
    format_duplicates_report,
)


class TestFindDuplicates:
    def test_no_duplicates(self):
        units = [
            MovableUnit("/mnt/disk1/TV/ShowA", "TV", "ShowA", 100, "/mnt/disk1"),
            MovableUnit("/mnt/disk2/TV/ShowB", "TV", "ShowB", 200, "/mnt/disk2"),
        ]
        assert find_duplicates(units) == []

    def test_simple_duplicate_pair(self):
        units = [
            MovableUnit("/mnt/disk1/TV/ShowA", "TV", "ShowA", 100, "/mnt/disk1"),
            MovableUnit("/mnt/disk2/TV/ShowA", "TV", "ShowA", 100, "/mnt/disk2"),
        ]
        groups = find_duplicates(units, disk_usage={"/mnt/disk1": 90, "/mnt/disk2": 50})
        assert len(groups) == 1
        assert len(groups[0]) == 2
        # Fuller disk first (deletion candidate)
        assert groups[0][0].disk == "/mnt/disk1"
        assert groups[0][1].disk == "/mnt/disk2"

    def test_triple_duplicate(self):
        units = [
            MovableUnit("/mnt/disk1/TV/ShowA", "TV", "ShowA", 100, "/mnt/disk1"),
            MovableUnit("/mnt/disk2/TV/ShowA", "TV", "ShowA", 100, "/mnt/disk2"),
            MovableUnit("/mnt/disk3/TV/ShowA", "TV", "ShowA", 100, "/mnt/disk3"),
        ]
        groups = find_duplicates(units, disk_usage={
            "/mnt/disk1": 95, "/mnt/disk2": 80, "/mnt/disk3": 30,
        })
        assert len(groups) == 1
        assert len(groups[0]) == 3
        assert groups[0][0].disk == "/mnt/disk1"  # fullest first
        assert groups[0][2].disk == "/mnt/disk3"  # emptiest last (keep)

    def test_different_shares_same_name_not_duplicate(self):
        units = [
            MovableUnit("/mnt/disk1/Movies/2024", "Movies", "2024", 100, "/mnt/disk1"),
            MovableUnit("/mnt/disk2/Anime/2024", "Anime", "2024", 100, "/mnt/disk2"),
        ]
        assert find_duplicates(units) == []

    def test_mixed_duplicates_and_unique(self):
        units = [
            MovableUnit("/mnt/disk1/TV/ShowA", "TV", "ShowA", 100, "/mnt/disk1"),
            MovableUnit("/mnt/disk2/TV/ShowA", "TV", "ShowA", 100, "/mnt/disk2"),
            MovableUnit("/mnt/disk1/TV/ShowB", "TV", "ShowB", 200, "/mnt/disk1"),
        ]
        groups = find_duplicates(units, disk_usage={"/mnt/disk1": 90, "/mnt/disk2": 50})
        assert len(groups) == 1
        assert groups[0][0].share == "TV" and groups[0][0].name == "ShowA"

    def test_empty_units(self):
        assert find_duplicates([]) == []

    def test_no_disk_usage_sorts_by_path(self):
        units = [
            MovableUnit("/mnt/disk2/TV/ShowA", "TV", "ShowA", 100, "/mnt/disk2"),
            MovableUnit("/mnt/disk1/TV/ShowA", "TV", "ShowA", 100, "/mnt/disk1"),
        ]
        groups = find_duplicates(units)
        assert len(groups) == 1
        # Without disk_usage, sorted by path
        assert groups[0][0].disk == "/mnt/disk1"


class TestFormatDuplicatesReport:
    def test_empty_groups(self):
        result = format_duplicates_report([], {})
        assert "No duplicates found." in result

    def test_single_group_shows_disks(self):
        units = [
            MovableUnit("/mnt/disk1/TV/ShowA", "TV", "ShowA", 1_000_000_000, "/mnt/disk1"),
            MovableUnit("/mnt/disk2/TV/ShowA", "TV", "ShowA", 1_000_000_000, "/mnt/disk2"),
        ]
        groups = find_duplicates(units, disk_usage={"/mnt/disk1": 90, "/mnt/disk2": 50})
        report = format_duplicates_report(groups, {"/mnt/disk1": 90, "/mnt/disk2": 50})
        assert "TV/ShowA" in report
        assert "disk1" in report
        assert "disk2" in report
        assert "DELETE" in report
        assert "KEEP" in report

    def test_shows_reclaimable_summary(self):
        units = [
            MovableUnit("/mnt/disk1/TV/ShowA", "TV", "ShowA", 1_000_000_000, "/mnt/disk1"),
            MovableUnit("/mnt/disk2/TV/ShowA", "TV", "ShowA", 1_000_000_000, "/mnt/disk2"),
        ]
        groups = find_duplicates(units, disk_usage={"/mnt/disk1": 90, "/mnt/disk2": 50})
        report = format_duplicates_report(groups, {"/mnt/disk1": 90, "/mnt/disk2": 50})
        assert "1 duplicate" in report.lower()
        assert "reclaimable" in report.lower()
