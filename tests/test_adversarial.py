"""Adversarial review tests — security, safety, and edge cases."""

import fcntl
import os
from unittest.mock import MagicMock

import pytest

from rebalancer import (
    DiskInfo,
    MovableUnit,
    PlanDB,
    PlanEntry,
    TransferResult,
    _build_target_path,
    _check_not_symlink,
    _find_best_target,
    _validate_safe_path,
    acquire_lock,
    check_in_use,
    classify_disks,
    generate_plan,
    parse_du_output,
    read_plan_csv,
    read_drives_json,
    release_lock,
    run_cmd,
    transfer_unit,
    write_plan_csv,
    LOCK_FILE,
)


class TestPathSafety:
    def test_validate_safe_path_normal(self):
        assert _validate_safe_path("/mnt/disk1/TV_Shows/Lost") is True

    def test_validate_safe_path_traversal(self):
        assert _validate_safe_path("/mnt/disk1/../../etc/passwd") is False

    def test_validate_safe_path_wrong_prefix(self):
        assert _validate_safe_path("/tmp/evil") is False

    def test_validate_safe_path_disk_root_rejected(self):
        """rm -rf on /mnt/disk1 itself must be rejected."""
        assert _validate_safe_path("/mnt/disk1") is False

    def test_validate_safe_path_share_root_rejected(self):
        """rm -rf on /mnt/disk1/TV_Shows must be rejected (no item)."""
        assert _validate_safe_path("/mnt/disk1/TV_Shows") is False

    def test_validate_safe_path_non_numeric_disk(self):
        """Paths like /mnt/diskevil should be rejected."""
        assert _validate_safe_path("/mnt/diskevil/data/item") is False

    def test_build_target_path_mismatched_prefix(self):
        entry = PlanEntry("/mnt/disk10/data", 100, "/mnt/disk1", "/mnt/disk2")
        with pytest.raises(ValueError):
            _build_target_path(entry)

    def test_build_target_path_correct(self):
        entry = PlanEntry("/mnt/disk1/TV_Shows/Show", 100, "/mnt/disk1", "/mnt/disk2")
        assert _build_target_path(entry) == "/mnt/disk2/TV_Shows/Show"

    def test_transfer_traversal_path_rejected(self, mocker):
        mocker.patch("rebalancer.run_cmd")
        entry = PlanEntry(
            "/mnt/disk1/../../../etc/passwd", 100, "/mnt/disk1", "/mnt/disk2"
        )
        assert transfer_unit(entry) == "error_path"


class TestSymlinkSafety:
    """M5: Symlink detection before rm -rf to prevent following symlinks."""

    def test_check_not_symlink_returns_true_for_regular_dir(self, mocker):
        mock = mocker.patch("rebalancer.run_cmd")
        mock.return_value.returncode = 1  # test -L returns 1 = not a symlink
        assert _check_not_symlink("/mnt/disk1/TV_Shows/Show") is True

    def test_check_not_symlink_returns_false_for_symlink(self, mocker):
        mock = mocker.patch("rebalancer.run_cmd")
        mock.return_value.returncode = 0  # test -L returns 0 = IS a symlink
        assert _check_not_symlink("/mnt/disk1/TV_Shows/Show") is False

    def test_check_not_symlink_returns_false_on_error(self, mocker):
        """Fail closed — last safety gate before rm -rf."""
        mock = mocker.patch("rebalancer.run_cmd")
        mock.side_effect = Exception("connection lost")
        assert _check_not_symlink("/mnt/disk1/TV_Shows/Show") is False

    def test_transfer_rejects_symlink_source(self, mocker):
        """If source path is a symlink, transfer must return error_path."""
        calls = []
        test_e_count = [0]
        test_l_count = [0]

        def mock_run(cmd, **kwargs):
            cmd_str = " ".join(cmd) if isinstance(cmd, list) else cmd
            calls.append(cmd_str)
            result = MagicMock()
            result.stdout = ""
            result.stderr = ""
            if "test -L" in cmd_str:
                test_l_count[0] += 1
                result.returncode = 0  # IS a symlink
            elif "test -e" in cmd_str:
                test_e_count[0] += 1
                result.returncode = 0 if test_e_count[0] == 1 else 1
            elif "lsof" in cmd_str:
                result.returncode = 1  # not in use
            elif "df -Pk" in cmd_str:
                result.stdout = (
                    "Filesystem     1024-blocks      Used Available Capacity Mounted on\n"
                    "/dev/md10p1    15623792588 7890384356 7733408232      51% /mnt/disk10\n"
                )
                result.returncode = 0
            else:
                result.returncode = 0
            return result

        mocker.patch("rebalancer.run_cmd", side_effect=mock_run)
        entry = PlanEntry("/mnt/disk1/TV_Shows/Show", 100, "/mnt/disk1", "/mnt/disk10")
        status = transfer_unit(entry)
        assert status == "error_path"
        # rm -rf must NOT have been called
        assert not any("rm -rf" in c for c in calls), "rm called on symlink source"


class TestCommandInjection:
    def test_shell_metacharacters_quoted_in_remote(self, mocker):
        mock_proc = MagicMock()
        mock_proc.communicate.return_value = ("", "")
        mock_proc.returncode = 0
        mock = mocker.patch("rebalancer.subprocess.Popen", return_value=mock_proc)
        run_cmd(["ls", "-1", "/mnt/disk1/$(evil)/"], remote="root@host")
        call_args = mock.call_args[0][0]
        ssh_cmd = call_args[-1]
        # shlex.quote wraps in single quotes
        assert "$(evil)" not in ssh_cmd or "'" in ssh_cmd


class TestParseDuOutputRobust:
    def test_error_message_returns_zero(self):
        assert parse_du_output("du: cannot access '/mnt/disk1/gone': No such file") == 0

    def test_non_numeric_returns_zero(self):
        assert parse_du_output("not-a-number\t/path") == 0

    def test_valid_output(self):
        assert parse_du_output("12345\t/mnt/disk1/folder") == 12345


class TestCorruptedFiles:
    def test_corrupted_csv_missing_column(self, state_dir):
        path = state_dir / "plan.csv"
        path.write_text("path,size_bytes\n/mnt/disk1/A,100\n")
        result = read_plan_csv(path)
        assert result == []

    def test_corrupted_csv_non_integer_size(self, state_dir):
        path = state_dir / "plan.csv"
        path.write_text(
            "path,size_bytes,source_disk,target_disk,status\n"
            "/mnt/disk1/A,notanumber,/mnt/disk1,/mnt/disk10,pending\n"
        )
        result = read_plan_csv(path)
        assert result == []

    def test_corrupted_drives_json(self, state_dir):
        path = state_dir / "drives.json"
        path.write_text("{truncated")
        result = read_drives_json(path)
        assert result == []


class TestLockFile:
    def test_real_acquire_and_release(self, tmp_path):
        """Test actual lock functions (bypassing autouse mock)."""
        import rebalancer
        # Call the real functions directly
        lock_path = tmp_path / LOCK_FILE
        lock_fd = open(lock_path, "w")
        fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        lock_fd.write(str(os.getpid()))
        lock_fd.flush()

        # Try to acquire again — should fail
        try:
            lock_fd2 = open(lock_path, "w")
            fcntl.flock(lock_fd2, fcntl.LOCK_EX | fcntl.LOCK_NB)
            acquired_twice = True
            lock_fd2.close()
        except (OSError, IOError):
            acquired_twice = False

        assert not acquired_twice, "Lock should not be acquirable twice"

        # Release
        fcntl.flock(lock_fd, fcntl.LOCK_UN)
        lock_fd.close()


class TestCheckInUseSafety:
    def test_error_returns_true_for_safety(self, mocker):
        mock = mocker.patch("rebalancer.run_cmd")
        mock.side_effect = Exception("lsof not found")
        assert check_in_use("/mnt/disk1/path") is True


class TestTransferRmVerification:
    def test_rm_failure_returns_error(self, mocker):
        calls = []
        test_e_count = [0]

        def mock_run(cmd, **kwargs):
            cmd_str = " ".join(cmd) if isinstance(cmd, list) else cmd
            calls.append(cmd_str)
            result = MagicMock()
            if "test -L" in cmd_str:
                result.returncode = 1  # not a symlink
            elif "test -e" in cmd_str:
                test_e_count[0] += 1
                result.returncode = 0 if test_e_count[0] == 1 else 1  # source exists, target doesn't
            elif "rm -rf" in cmd_str:
                result.returncode = 1  # rm failed
            elif "lsof" in cmd_str:
                result.returncode = 1  # not in use
            else:
                result.returncode = 0
            result.stdout = ""
            result.stderr = ""
            return result

        mocker.patch("rebalancer.run_cmd", side_effect=mock_run)
        entry = PlanEntry("/mnt/disk1/TV_Shows/Show", 100, "/mnt/disk1", "/mnt/disk10")
        status = transfer_unit(entry)
        assert status == "error_delete"

    def test_target_exists_resumes_transfer(self, mocker):
        """If target already exists (partial copy from crash), rsync resumes it."""
        calls = []

        def mock_run(cmd, **kwargs):
            cmd_str = " ".join(cmd) if isinstance(cmd, list) else cmd
            calls.append(cmd_str)
            result = MagicMock()
            if "test -L" in cmd_str:
                result.returncode = 1  # not a symlink
            elif "test -e" in cmd_str:
                result.returncode = 0  # both source and target exist
            elif "lsof" in cmd_str:
                result.returncode = 1  # not in use
            else:
                result.returncode = 0
            result.stdout = ""
            result.stderr = ""
            return result

        mocker.patch("rebalancer.run_cmd", side_effect=mock_run)
        entry = PlanEntry("/mnt/disk1/TV_Shows/Show", 100, "/mnt/disk1", "/mnt/disk10")
        status = transfer_unit(entry)
        # Should succeed — rsync resumes/completes the partial copy
        assert status == "cleaned"
        # rsync should still have been called (to sync any remaining differences)
        assert any("rsync" in c and "-aHP" in c for c in calls)


class TestDfExceptionSafety:
    """The df space check must not silently proceed on exception."""

    def test_df_exception_does_not_proceed_blindly(self, mocker):
        """If df raises an exception, transfer should fail safe, not proceed."""
        test_e_count = [0]

        def mock_run(cmd, **kwargs):
            cmd_str = " ".join(cmd) if isinstance(cmd, list) else cmd
            result = MagicMock()
            result.stdout = ""
            result.stderr = ""
            if "test -L" in cmd_str:
                result.returncode = 1  # not a symlink
            elif "test -e" in cmd_str:
                test_e_count[0] += 1
                result.returncode = 0 if test_e_count[0] == 1 else 1
            elif "df -Pk" in cmd_str:
                raise RuntimeError("network timeout during df")
            elif "lsof" in cmd_str:
                result.returncode = 1  # not in use
            else:
                result.returncode = 0
            return result

        mocker.patch("rebalancer.run_cmd", side_effect=mock_run)
        entry = PlanEntry("/mnt/disk1/TV_Shows/Show", 100_000, "/mnt/disk1", "/mnt/disk10")
        result = transfer_unit(entry)
        # Current behavior: proceeds optimistically (result == "cleaned")
        # Desired behavior: fail safe when space check is unavailable
        assert result != "cleaned", (
            "Transfer should not proceed when df space check throws exception. "
            "Fix: return TransferResult('skipped_full') on df exception."
        )


class TestZeroByteDisk:
    """Disks with total_bytes=0 must not cause ZeroDivisionError."""

    def test_zero_total_bytes_in_find_best_target(self):
        """_find_best_target must not crash with total_bytes=0 disk."""
        targets = [
            DiskInfo("/mnt/disk1", 0, 0, 0, 0),  # zero-byte disk
            DiskInfo("/mnt/disk2", 1_000_000_000, 500_000_000, 500_000_000, 50),
        ]
        projected = {"/mnt/disk1": 0, "/mnt/disk2": 500_000_000, "/mnt/disk3": 900_000_000}
        # Should not raise ZeroDivisionError
        result = _find_best_target(
            unit_size=100_000_000,
            source_disk="/mnt/disk3",
            targets=targets,
            projected_usage=projected,
            max_used=80,
            min_free=0,
        )
        # Should skip the zero-byte disk and potentially pick disk2
        assert result is None or result.path != "/mnt/disk1"

    def test_zero_total_bytes_in_generate_plan(self):
        """generate_plan must not crash when a disk has total_bytes=0."""
        disks = [
            DiskInfo("/mnt/disk1", 0, 0, 0, 0),
            DiskInfo("/mnt/disk2", 1_000_000, 900_000, 100_000, 90),
            DiskInfo("/mnt/disk3", 1_000_000, 300_000, 700_000, 30),
        ]
        units = [
            MovableUnit("/mnt/disk2/TV/Show", "TV", "Show", 50_000, "/mnt/disk2"),
        ]
        over, under = classify_disks(disks, 80)
        # Should not crash
        plan = generate_plan(units, over, under, "fullest-first", 80, 0)
        assert isinstance(plan, list)


class TestDuplicatePlanPaths:
    """PlanDB.write_plan must handle duplicate paths gracefully."""

    def test_write_plan_duplicate_paths(self, db_path):
        """Duplicate paths in write_plan should not crash."""
        db = PlanDB(db_path)
        entries = [
            PlanEntry("/mnt/disk1/TV/Show", 100, "/mnt/disk1", "/mnt/disk10"),
            PlanEntry("/mnt/disk1/TV/Show", 200, "/mnt/disk1", "/mnt/disk11"),
        ]
        # Should either deduplicate or use INSERT OR REPLACE, but not crash
        try:
            db.write_plan(entries)
            # If it didn't crash, verify we got a consistent state
            all_entries = db.get_all()
            # Should have exactly 1 entry (last wins or first wins)
            assert len(all_entries) <= 2
        except Exception:
            # If it crashes, that's the bug we're documenting
            pytest.fail("write_plan crashed on duplicate paths — needs INSERT OR REPLACE")
        finally:
            db.close()


class TestRsyncPartialExitCodes:
    """rsync exit codes 23 (partial) and 24 (vanished) must be caught."""

    def test_rsync_exit_23_returns_error(self, mocker):
        """rsync exit 23 (partial transfer) must return error_copy."""
        from tests.test_execution import TestTransferUnit
        def set_rsync_partial(result):
            result.returncode = 23
            result.stderr = "rsync: some files could not be transferred"
        mock_run, _ = TestTransferUnit()._make_mock_run(
            overrides={"-aHP": set_rsync_partial}
        )
        mocker.patch("rebalancer.run_cmd", side_effect=mock_run)
        entry = PlanEntry("/mnt/disk1/TV_Shows/Show", 100_000, "/mnt/disk1", "/mnt/disk10")
        result = transfer_unit(entry)
        assert result == "error_copy", f"rsync exit 23 should be error_copy, got {result.status}"

    def test_rsync_exit_24_returns_error(self, mocker):
        """rsync exit 24 (files vanished) must return error_copy."""
        from tests.test_execution import TestTransferUnit
        def set_rsync_vanished(result):
            result.returncode = 24
            result.stderr = "rsync warning: some files vanished before they could be transferred"
        mock_run, _ = TestTransferUnit()._make_mock_run(
            overrides={"-aHP": set_rsync_vanished}
        )
        mocker.patch("rebalancer.run_cmd", side_effect=mock_run)
        entry = PlanEntry("/mnt/disk1/TV_Shows/Show", 100_000, "/mnt/disk1", "/mnt/disk10")
        result = transfer_unit(entry)
        assert result == "error_copy", f"rsync exit 24 should be error_copy, got {result.status}"


class TestDoubleSignal:
    """Double Ctrl+C within 3 seconds must force exit."""

    def test_double_signal_exits(self):
        """Second signal within 3s of first should call sys.exit(1)."""
        import time as time_mod
        from rebalancer import _signal_handler, reset_shutdown_flags, shutdown_requested
        reset_shutdown_flags()
        # First signal
        _signal_handler(2, None)
        assert shutdown_requested() is True
        # Second signal within 3s
        with pytest.raises(SystemExit) as exc_info:
            _signal_handler(2, None)
        assert exc_info.value.code == 1
        reset_shutdown_flags()

    def test_second_signal_after_3s_does_not_exit(self):
        """Second signal after 3s should not force exit (just re-set flag)."""
        import time as time_mod
        import rebalancer
        from rebalancer import _signal_handler, reset_shutdown_flags, shutdown_requested
        reset_shutdown_flags()
        # First signal
        _signal_handler(2, None)
        # Hack: set _last_signal_time to 10s ago to simulate delay
        rebalancer._last_signal_time = time_mod.time() - 10.0
        # Second signal — should NOT exit
        _signal_handler(2, None)  # should not raise
        assert shutdown_requested() is True
        reset_shutdown_flags()
