"""Tests for duplicate detection and resolution."""

from unittest.mock import MagicMock

import pytest

from rebalancer import (
    DiskInfo,
    MovableUnit,
    find_duplicates,
    format_duplicates_report,
    resolve_duplicate,
    main,
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


class TestCheckDuplicatesCLI:
    def _setup_mocks(self, mocker, state_dir, units_by_disk=None):
        mocker.patch("rebalancer.STATE_DIR", state_dir)
        disks = [
            DiskInfo("/mnt/disk1", 1_000_000, 900_000, 100_000, 90),
            DiskInfo("/mnt/disk2", 1_000_000, 500_000, 500_000, 50),
        ]
        mocker.patch("rebalancer.discover_disks", return_value=disks)
        if units_by_disk is None:
            units_by_disk = {
                "/mnt/disk1": [
                    MovableUnit("/mnt/disk1/TV/ShowA", "TV", "ShowA", 50_000, "/mnt/disk1"),
                    MovableUnit("/mnt/disk1/TV/ShowB", "TV", "ShowB", 30_000, "/mnt/disk1"),
                ],
                "/mnt/disk2": [
                    MovableUnit("/mnt/disk2/TV/ShowA", "TV", "ShowA", 50_000, "/mnt/disk2"),
                ],
            }

        def scan_side_effect(disk, excludes, remote=None):
            return units_by_disk.get(disk.path, [])

        mocker.patch("rebalancer.scan_movable_units", side_effect=scan_side_effect)

    def test_check_duplicates_prints_report(self, state_dir, mocker, capsys):
        self._setup_mocks(mocker, state_dir)
        result = main(["--check-duplicates"])
        assert result == 0
        output = capsys.readouterr().out
        assert "TV/ShowA" in output
        assert "DELETE" in output
        assert "KEEP" in output

    def test_check_duplicates_no_duplicates(self, state_dir, mocker, capsys):
        unique_by_disk = {
            "/mnt/disk1": [MovableUnit("/mnt/disk1/TV/ShowA", "TV", "ShowA", 50_000, "/mnt/disk1")],
            "/mnt/disk2": [MovableUnit("/mnt/disk2/TV/ShowB", "TV", "ShowB", 30_000, "/mnt/disk2")],
        }
        self._setup_mocks(mocker, state_dir, units_by_disk=unique_by_disk)
        result = main(["--check-duplicates"])
        assert result == 0
        output = capsys.readouterr().out
        assert "No duplicates found." in output

    def test_check_duplicates_no_lock_needed(self, state_dir, mocker, capsys):
        """--check-duplicates should not acquire a lock."""
        self._setup_mocks(mocker, state_dir)
        lock_mock = mocker.patch("rebalancer.acquire_lock")
        result = main(["--check-duplicates"])
        assert result == 0
        lock_mock.assert_not_called()


class TestResolveDuplicate:
    def _mock_run(self, mocker, verify_stdout="", verify_rc=0, rm_rc=0,
                  in_use=False, is_symlink=False):
        """Mock run_cmd for resolve_duplicate tests."""
        def side_effect(cmd, **kwargs):
            cmd_str = " ".join(cmd) if isinstance(cmd, list) else cmd
            result = MagicMock()
            result.stdout = ""
            result.stderr = ""
            result.returncode = 0
            if "--itemize-changes" in cmd_str:
                result.stdout = verify_stdout
                result.returncode = verify_rc
            elif "rm -rf" in cmd_str:
                result.returncode = rm_rc
            elif "test -L" in cmd_str:
                result.returncode = 0 if is_symlink else 1
            elif "lsof" in cmd_str:
                result.returncode = 0 if in_use else 1
                result.stdout = "COMMAND PID" if in_use else ""
            return result
        mocker.patch("rebalancer.run_cmd", side_effect=side_effect)

    def test_verified_match_deletes_source(self, mocker):
        self._mock_run(mocker)
        source = MovableUnit("/mnt/disk1/TV/ShowA", "TV", "ShowA", 100, "/mnt/disk1")
        target = MovableUnit("/mnt/disk2/TV/ShowA", "TV", "ShowA", 100, "/mnt/disk2")
        status = resolve_duplicate(source, target)
        assert status == "resolved"

    def test_mismatch_does_not_delete(self, mocker):
        self._mock_run(mocker, verify_stdout=">f..t...... file.mkv\n")
        source = MovableUnit("/mnt/disk1/TV/ShowA", "TV", "ShowA", 100, "/mnt/disk1")
        target = MovableUnit("/mnt/disk2/TV/ShowA", "TV", "ShowA", 100, "/mnt/disk2")
        status = resolve_duplicate(source, target)
        assert status == "mismatch"

    def test_in_use_does_not_delete(self, mocker):
        self._mock_run(mocker, in_use=True)
        source = MovableUnit("/mnt/disk1/TV/ShowA", "TV", "ShowA", 100, "/mnt/disk1")
        target = MovableUnit("/mnt/disk2/TV/ShowA", "TV", "ShowA", 100, "/mnt/disk2")
        status = resolve_duplicate(source, target)
        assert status == "in_use"

    def test_symlink_does_not_delete(self, mocker):
        self._mock_run(mocker, is_symlink=True)
        source = MovableUnit("/mnt/disk1/TV/ShowA", "TV", "ShowA", 100, "/mnt/disk1")
        target = MovableUnit("/mnt/disk2/TV/ShowA", "TV", "ShowA", 100, "/mnt/disk2")
        status = resolve_duplicate(source, target)
        assert status == "error"

    def test_dry_run_does_not_delete(self, mocker):
        self._mock_run(mocker)
        source = MovableUnit("/mnt/disk1/TV/ShowA", "TV", "ShowA", 100, "/mnt/disk1")
        target = MovableUnit("/mnt/disk2/TV/ShowA", "TV", "ShowA", 100, "/mnt/disk2")
        status = resolve_duplicate(source, target, dry_run=True)
        assert status == "dry_run"

    def test_invalid_path_rejected(self, mocker):
        self._mock_run(mocker)
        source = MovableUnit("/tmp/bad", "TV", "ShowA", 100, "/tmp")
        target = MovableUnit("/mnt/disk2/TV/ShowA", "TV", "ShowA", 100, "/mnt/disk2")
        status = resolve_duplicate(source, target)
        assert status == "error"

    def test_rm_failure_returns_error(self, mocker):
        self._mock_run(mocker, rm_rc=1)
        source = MovableUnit("/mnt/disk1/TV/ShowA", "TV", "ShowA", 100, "/mnt/disk1")
        target = MovableUnit("/mnt/disk2/TV/ShowA", "TV", "ShowA", 100, "/mnt/disk2")
        status = resolve_duplicate(source, target)
        assert status == "error"


class TestResolveDuplicatesCLI:
    def _setup_mocks(self, mocker, state_dir):
        mocker.patch("rebalancer.STATE_DIR", state_dir)
        mocker.patch("rebalancer.setup_signal_handlers")
        disks = [
            DiskInfo("/mnt/disk1", 1_000_000, 900_000, 100_000, 90),
            DiskInfo("/mnt/disk2", 1_000_000, 500_000, 500_000, 50),
        ]
        mocker.patch("rebalancer.discover_disks", return_value=disks)

        def scan_side_effect(disk, excludes, remote=None):
            if disk.path == "/mnt/disk1":
                return [MovableUnit("/mnt/disk1/TV/ShowA", "TV", "ShowA", 50_000, "/mnt/disk1")]
            elif disk.path == "/mnt/disk2":
                return [MovableUnit("/mnt/disk2/TV/ShowA", "TV", "ShowA", 50_000, "/mnt/disk2")]
            return []
        mocker.patch("rebalancer.scan_movable_units", side_effect=scan_side_effect)

    def test_resolve_duplicates_full_flow(self, state_dir, mocker, capsys):
        self._setup_mocks(mocker, state_dir)
        mocker.patch("rebalancer.resolve_duplicate", return_value="resolved")
        result = main(["--resolve-duplicates", "--yes"])
        assert result == 0
        output = capsys.readouterr().out
        assert "resolved" in output.lower() or "Resolved" in output

    def test_resolve_duplicates_dry_run(self, state_dir, mocker, capsys):
        self._setup_mocks(mocker, state_dir)
        resolve_mock = mocker.patch("rebalancer.resolve_duplicate", return_value="dry_run")
        result = main(["--resolve-duplicates", "--dry-run", "--yes"])
        assert result == 0
        call_kwargs = resolve_mock.call_args.kwargs
        assert call_kwargs.get("dry_run") is True

    def test_resolve_duplicates_requires_confirmation(self, state_dir, mocker, capsys):
        self._setup_mocks(mocker, state_dir)
        mocker.patch("builtins.input", return_value="n")
        mocker.patch("rebalancer.resolve_duplicate")
        result = main(["--resolve-duplicates"])
        assert result == 0
        output = capsys.readouterr().out
        assert "Aborted" in output

    def test_resolve_duplicates_no_duplicates(self, state_dir, mocker, capsys):
        mocker.patch("rebalancer.STATE_DIR", state_dir)
        mocker.patch("rebalancer.setup_signal_handlers")
        disks = [DiskInfo("/mnt/disk1", 1_000_000, 500_000, 500_000, 50)]
        mocker.patch("rebalancer.discover_disks", return_value=disks)
        mocker.patch("rebalancer.scan_movable_units", return_value=[
            MovableUnit("/mnt/disk1/TV/ShowA", "TV", "ShowA", 50_000, "/mnt/disk1"),
        ])
        result = main(["--resolve-duplicates", "--yes"])
        assert result == 0
        output = capsys.readouterr().out
        assert "No duplicates found." in output

    def test_mutual_exclusivity(self, state_dir, mocker, capsys):
        mocker.patch("rebalancer.STATE_DIR", state_dir)
        result = main(["--check-duplicates", "--resolve-duplicates"])
        assert result == 1
        output = capsys.readouterr().out
        assert "mutually exclusive" in output

    def test_resolve_triple_deletes_two(self, state_dir, mocker, capsys):
        """Triple duplicate should delete copies on the two fuller disks."""
        mocker.patch("rebalancer.STATE_DIR", state_dir)
        mocker.patch("rebalancer.setup_signal_handlers")
        disks = [
            DiskInfo("/mnt/disk1", 1_000_000, 950_000, 50_000, 95),
            DiskInfo("/mnt/disk2", 1_000_000, 800_000, 200_000, 80),
            DiskInfo("/mnt/disk3", 1_000_000, 300_000, 700_000, 30),
        ]
        mocker.patch("rebalancer.discover_disks", return_value=disks)

        def scan_side_effect(disk, excludes, remote=None):
            return [MovableUnit(f"{disk.path}/TV/ShowA", "TV", "ShowA", 50_000, disk.path)]
        mocker.patch("rebalancer.scan_movable_units", side_effect=scan_side_effect)

        resolve_mock = mocker.patch("rebalancer.resolve_duplicate", return_value="resolved")
        result = main(["--resolve-duplicates", "--yes"])
        assert result == 0
        # Should be called twice: delete from disk1 (95%) and disk2 (80%), keep disk3 (30%)
        assert resolve_mock.call_count == 2
        # Verify the kept copy is on disk3
        for call in resolve_mock.call_args_list:
            keep = call.args[1]  # target (kept copy)
            assert keep.disk == "/mnt/disk3"
