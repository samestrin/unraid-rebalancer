"""Tests for execution engine — Phase 4 RED."""

import os
from datetime import datetime
from unittest.mock import MagicMock

import pytest

from rebalancer import (
    PlanEntry,
    TransferResult,
    _truncate_stderr,
    check_in_use,
    log_transfer,
    transfer_unit,
)


# --- check_in_use ---

class TestCheckInUse:
    def test_returns_true_when_lsof_finds_open_files(self, mocker):
        mock = mocker.patch("rebalancer.run_cmd")
        mock.return_value.returncode = 0
        mock.return_value.stdout = "python3  1234 user  4r  REG  /mnt/disk1/TV_Shows/Show/file.mkv\n"
        assert check_in_use("/mnt/disk1/TV_Shows/Show") is True

    def test_returns_false_when_lsof_finds_nothing(self, mocker):
        mock = mocker.patch("rebalancer.run_cmd")
        mock.return_value.returncode = 1
        mock.return_value.stdout = ""
        mock.return_value.stderr = ""
        assert check_in_use("/mnt/disk1/TV_Shows/Show") is False

    def test_returns_true_on_error(self, mocker):
        """On error, assume in-use for safety."""
        mock = mocker.patch("rebalancer.run_cmd")
        mock.side_effect = Exception("timeout")
        assert check_in_use("/mnt/disk1/TV_Shows/Show") is True

    def test_passes_remote(self, mocker):
        mock = mocker.patch("rebalancer.run_cmd")
        mock.return_value.returncode = 1
        mock.return_value.stdout = ""
        mock.return_value.stderr = ""
        check_in_use("/mnt/disk1/TV_Shows/Show", remote="root@unraid.lan")
        call_kwargs = mock.call_args[1]
        assert call_kwargs.get("remote") == "root@unraid.lan"

    def test_lsof_does_not_use_b_flag(self, mocker):
        """CRITICAL: -b flag breaks on Unraid lsof 4.99.5, causing the
        pre-delete safety check to silently pass."""
        mock = mocker.patch("rebalancer.run_cmd")
        mock.return_value.returncode = 1
        mock.return_value.stdout = ""
        mock.return_value.stderr = ""
        check_in_use("/mnt/disk1/TV_Shows/Show")
        cmd = mock.call_args[0][0]
        assert "-b" not in cmd

    def test_lsof_error_with_stderr_returns_true(self, mocker):
        """If lsof returns non-zero with stderr, assume in-use (safe default).
        This catches Unraid's lsof -b 'can't stat()' warning pattern."""
        mock = mocker.patch("rebalancer.run_cmd")
        mock.return_value.returncode = 1
        mock.return_value.stdout = ""
        mock.return_value.stderr = "lsof: WARNING: can't stat(): Resource temporarily unavailable"
        assert check_in_use("/mnt/disk1/TV_Shows/Show") is True

    def test_lsof_returncode_zero_empty_stdout_returns_false(self, mocker):
        """lsof returns 0 with empty stdout on some versions when no files open."""
        mock = mocker.patch("rebalancer.run_cmd")
        mock.return_value.returncode = 0
        mock.return_value.stdout = ""
        mock.return_value.stderr = ""
        assert check_in_use("/mnt/disk1/TV_Shows/Show") is False

    def test_accepts_timeout_parameter(self, mocker):
        """check_in_use should forward custom timeout to run_cmd."""
        mock = mocker.patch("rebalancer.run_cmd")
        mock.return_value.returncode = 1
        mock.return_value.stdout = ""
        mock.return_value.stderr = ""
        check_in_use("/mnt/disk1/TV_Shows/Show", timeout=60)
        call_kwargs = mock.call_args[1]
        assert call_kwargs.get("timeout") == 60

    def test_default_timeout_is_120(self, mocker):
        """Default timeout for lsof should be 120 seconds."""
        mock = mocker.patch("rebalancer.run_cmd")
        mock.return_value.returncode = 1
        mock.return_value.stdout = ""
        mock.return_value.stderr = ""
        check_in_use("/mnt/disk1/TV_Shows/Show")
        call_kwargs = mock.call_args[1]
        assert call_kwargs.get("timeout") == 120

    def test_timeout_prints_warning(self, mocker, capsys):
        """When lsof times out, print a diagnostic warning."""
        import subprocess
        mock = mocker.patch("rebalancer.run_cmd")
        mock.side_effect = subprocess.TimeoutExpired(cmd="lsof", timeout=120)
        result = check_in_use("/mnt/disk1/TV_Shows/Show")
        assert result is True
        output = capsys.readouterr().out
        assert "lsof timed out" in output.lower() or "timed out" in output.lower()


# --- transfer_unit ---

class TestTransferUnit:
    def _make_mock_run(self, calls=None, overrides=None):
        """Create a mock_run for transfer_unit tests.

        Default behavior:
        - test -e on source path → 0 (exists)
        - test -e on target path → 1 (not found)
        - test -L → 1 (not a symlink)
        - lsof → 1 (not in use, for pre-delete check)
        - Everything else → 0 (success)
        """
        if calls is None:
            calls = []
        if overrides is None:
            overrides = {}
        test_e_count = [0]

        def mock_run(cmd, **kwargs):
            cmd_str = " ".join(cmd) if isinstance(cmd, list) else cmd
            calls.append(cmd_str)
            result = MagicMock()
            result.stdout = ""
            result.stderr = ""
            # test -e: first call = source (exists=0), second call = target (not found=1)
            if "test -L" in cmd_str:
                result.returncode = 1  # not a symlink
            elif "test -e" in cmd_str:
                test_e_count[0] += 1
                result.returncode = 0 if test_e_count[0] == 1 else 1
            elif "lsof" in cmd_str:
                result.returncode = 1  # not in use
            elif "df -Pk" in cmd_str:
                # Default: target disk has plenty of space
                result.stdout = (
                    "Filesystem     1024-blocks      Used Available Capacity Mounted on\n"
                    "/dev/md10p1    15623792588 7890384356 7733408232      51% /mnt/disk10\n"
                )
                result.returncode = 0
            else:
                result.returncode = 0
            # Apply overrides
            for pattern, rc_or_fn in overrides.items():
                if pattern in cmd_str:
                    if callable(rc_or_fn):
                        rc_or_fn(result)
                    else:
                        result.returncode = rc_or_fn
            return result

        return mock_run, calls

    def test_successful_three_phase_transfer(self, mocker):
        """Copy -> verify -> delete source."""
        mock_run, calls = self._make_mock_run()
        mocker.patch("rebalancer.run_cmd", side_effect=mock_run)
        entry = PlanEntry(
            "/mnt/disk1/TV_Shows/Show",
            100_000,
            "/mnt/disk1",
            "/mnt/disk10",
        )
        status = transfer_unit(entry)
        assert status == "cleaned"
        assert any("rsync" in c and "-aHP" in c for c in calls), "Missing rsync copy"
        assert any("rsync" in c and "--itemize-changes" in c for c in calls), "Missing rsync verify"
        assert any("rm -rf" in c for c in calls), "Missing rm"

    def test_checksum_mismatch_blocks_delete(self, mocker):
        def set_verify_diff(result):
            result.stdout = ">f..t...... file.mkv\n"
        mock_run, calls = self._make_mock_run(overrides={"--itemize-changes": set_verify_diff})
        mocker.patch("rebalancer.run_cmd", side_effect=mock_run)
        entry = PlanEntry("/mnt/disk1/TV_Shows/Show", 100_000, "/mnt/disk1", "/mnt/disk10")
        status = transfer_unit(entry)
        assert status == "error_verify"
        assert not any("rm -rf" in c for c in calls), "rm called despite checksum mismatch"

    def test_rsync_copy_failure_returns_error(self, mocker):
        mock_run, _ = self._make_mock_run(overrides={"rsync": 1})
        mocker.patch("rebalancer.run_cmd", side_effect=mock_run)
        entry = PlanEntry("/mnt/disk1/TV_Shows/Show", 100_000, "/mnt/disk1", "/mnt/disk10")
        status = transfer_unit(entry)
        assert status == "error_copy"

    def test_delete_path_must_be_under_mnt_disk(self, mocker):
        mock_run, calls = self._make_mock_run()
        mocker.patch("rebalancer.run_cmd", side_effect=mock_run)
        entry = PlanEntry("/mnt/disk1/TV_Shows/Show", 100_000, "/mnt/disk1", "/mnt/disk10")
        transfer_unit(entry)
        rm_calls = [c for c in calls if "rm -rf" in c]
        for c in rm_calls:
            assert "/mnt/disk" in c

    def test_unsafe_path_rejected(self, mocker):
        mocker.patch("rebalancer.run_cmd")
        entry = PlanEntry("/tmp/evil/path", 100_000, "/tmp/evil", "/mnt/disk10")
        status = transfer_unit(entry)
        assert status == "error_path"

    def test_directory_timestamp_diff_does_not_block_transfer(self, mocker):
        """C1: rsync --itemize-changes emits .d..t...... for directory timestamp
        diffs, which are normal after copy. These should NOT cause error_verify."""
        def set_dir_timestamp_diff(result):
            # Directory-only timestamp changes — not a data integrity issue
            result.stdout = ".d..t...... ./\n.d..t...... subdir/\n"
        mock_run, calls = self._make_mock_run(
            overrides={"--itemize-changes": set_dir_timestamp_diff}
        )
        mocker.patch("rebalancer.run_cmd", side_effect=mock_run)
        entry = PlanEntry("/mnt/disk1/TV_Shows/Show", 100_000, "/mnt/disk1", "/mnt/disk10")
        status = transfer_unit(entry)
        assert status == "cleaned", (
            f"Directory timestamp diffs should not block transfer, got {status}"
        )

    def test_real_file_diff_still_blocks_transfer(self, mocker):
        """C1 safety: actual file differences must still block deletion."""
        def set_mixed_diff(result):
            result.stdout = ".d..t...... ./\n>f.st...... changed_file.mkv\n"
        mock_run, calls = self._make_mock_run(
            overrides={"--itemize-changes": set_mixed_diff}
        )
        mocker.patch("rebalancer.run_cmd", side_effect=mock_run)
        entry = PlanEntry("/mnt/disk1/TV_Shows/Show", 100_000, "/mnt/disk1", "/mnt/disk10")
        status = transfer_unit(entry)
        assert status == "error_verify"

    def test_directory_permission_diff_blocks_transfer(self, mocker):
        """Directory permission differences (.d...p.....) must NOT be filtered out."""
        def set_dir_perm_diff(result):
            result.stdout = ".d...p..... ./\n"
        mock_run, calls = self._make_mock_run(
            overrides={"--itemize-changes": set_dir_perm_diff}
        )
        mocker.patch("rebalancer.run_cmd", side_effect=mock_run)
        entry = PlanEntry("/mnt/disk1/TV_Shows/Show", 100_000, "/mnt/disk1", "/mnt/disk10")
        status = transfer_unit(entry)
        assert status == "error_verify", (
            "Directory permission diffs should block transfer"
        )

    def test_directory_owner_diff_blocks_transfer(self, mocker):
        """Directory owner differences (.d....o....) must NOT be filtered out."""
        def set_dir_owner_diff(result):
            result.stdout = ".d....o.... ./\n"
        mock_run, calls = self._make_mock_run(
            overrides={"--itemize-changes": set_dir_owner_diff}
        )
        mocker.patch("rebalancer.run_cmd", side_effect=mock_run)
        entry = PlanEntry("/mnt/disk1/TV_Shows/Show", 100_000, "/mnt/disk1", "/mnt/disk10")
        status = transfer_unit(entry)
        assert status == "error_verify", (
            "Directory owner diffs should block transfer"
        )

    def test_rsync_verify_uses_archive_mode(self, mocker):
        """Verify phase must use -anc (archive) not -rnc to catch metadata diffs."""
        mock_run, calls = self._make_mock_run()
        mocker.patch("rebalancer.run_cmd", side_effect=mock_run)
        entry = PlanEntry("/mnt/disk1/TV_Shows/Show", 100_000, "/mnt/disk1", "/mnt/disk10")
        transfer_unit(entry)
        verify_calls = [c for c in calls if "--itemize-changes" in c]
        assert len(verify_calls) >= 1
        assert "-anc" in verify_calls[0], f"Expected -anc in verify call, got: {verify_calls[0]}"

    def test_rsync_target_path_construction(self, mocker):
        """Target rsync path should replace source disk with target disk."""
        mock_run, calls = self._make_mock_run()
        mocker.patch("rebalancer.run_cmd", side_effect=mock_run)
        entry = PlanEntry(
            "/mnt/disk4/TV_Shows/Breaking Bad (2008)",
            200_000,
            "/mnt/disk4",
            "/mnt/disk11",
        )
        transfer_unit(entry)
        rsync_calls = [c for c in calls if "rsync" in c and "-aHP" in c]
        assert len(rsync_calls) >= 1
        assert "/mnt/disk11/TV_Shows/Breaking Bad (2008)" in rsync_calls[0]

    def test_skips_transfer_when_target_disk_full(self, mocker):
        """Pre-transfer df check should skip if target has insufficient space."""
        def set_full_disk(result):
            result.stdout = (
                "Filesystem     1024-blocks         Used Available Capacity Mounted on\n"
                "/dev/md10p1    15623792588  15623791564      1024     100% /mnt/disk10\n"
            )
        mock_run, calls = self._make_mock_run(
            overrides={"df -Pk": set_full_disk}
        )
        mocker.patch("rebalancer.run_cmd", side_effect=mock_run)
        entry = PlanEntry("/mnt/disk1/TV_Shows/Show", 100_000_000_000, "/mnt/disk1", "/mnt/disk10")
        status = transfer_unit(entry, min_free=0)
        assert status == "skipped_full"
        assert not any("rsync" in c for c in calls), "rsync should not run when target is full"

    def test_proceeds_when_target_has_space(self, mocker):
        """Normal flow: target has enough space, transfer proceeds."""
        mock_run, calls = self._make_mock_run()
        mocker.patch("rebalancer.run_cmd", side_effect=mock_run)
        entry = PlanEntry("/mnt/disk1/TV_Shows/Show", 100_000, "/mnt/disk1", "/mnt/disk10")
        status = transfer_unit(entry, min_free=0)
        assert status == "cleaned"
        assert any("rsync" in c and "-aHP" in c for c in calls)

    def test_proceeds_when_df_check_fails(self, mocker):
        """If df fails, proceed optimistically — rsync will fail cleanly."""
        def df_fails(result):
            result.returncode = 1
            result.stdout = ""
        mock_run, calls = self._make_mock_run(
            overrides={"df -Pk": df_fails}
        )
        mocker.patch("rebalancer.run_cmd", side_effect=mock_run)
        entry = PlanEntry("/mnt/disk1/TV_Shows/Show", 100_000, "/mnt/disk1", "/mnt/disk10")
        status = transfer_unit(entry, min_free=0)
        assert status == "cleaned"

    def test_bwlimit_passed_to_rsync_copy(self, mocker):
        """--bwlimit value should appear in rsync copy command."""
        mock_run, calls = self._make_mock_run()
        mocker.patch("rebalancer.run_cmd", side_effect=mock_run)
        entry = PlanEntry("/mnt/disk1/TV_Shows/Show", 100_000, "/mnt/disk1", "/mnt/disk10")
        transfer_unit(entry, bwlimit="50000")
        rsync_copy = [c for c in calls if "rsync" in c and "-aHP" in c]
        assert len(rsync_copy) >= 1
        assert "--bwlimit=50000" in rsync_copy[0]

    def test_bwlimit_not_in_verify(self, mocker):
        """--bwlimit should NOT appear in rsync verify command (read-only)."""
        mock_run, calls = self._make_mock_run()
        mocker.patch("rebalancer.run_cmd", side_effect=mock_run)
        entry = PlanEntry("/mnt/disk1/TV_Shows/Show", 100_000, "/mnt/disk1", "/mnt/disk10")
        transfer_unit(entry, bwlimit="50000")
        verify_calls = [c for c in calls if "--itemize-changes" in c]
        assert len(verify_calls) >= 1
        assert "--bwlimit" not in verify_calls[0]

    def test_custom_copy_timeout(self, mocker):
        """Custom copy timeout passed to rsync copy run_cmd call."""
        calls_with_kwargs = []
        def mock_run(cmd, **kwargs):
            cmd_str = " ".join(cmd) if isinstance(cmd, list) else cmd
            calls_with_kwargs.append((cmd_str, kwargs))
            result = MagicMock()
            result.stdout = ""
            result.stderr = ""
            if "test -L" in cmd_str:
                result.returncode = 1  # not a symlink
            elif "test -e" in cmd_str:
                # source exists, target doesn't
                result.returncode = 0 if len([c for c in calls_with_kwargs if "test -e" in c[0]]) == 1 else 1
            elif "lsof" in cmd_str:
                result.returncode = 1
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
        entry = PlanEntry("/mnt/disk1/TV_Shows/Show", 100_000, "/mnt/disk1", "/mnt/disk10")
        transfer_unit(entry, copy_timeout=3600, verify_timeout=1800)
        rsync_copy = [(c, k) for c, k in calls_with_kwargs if "rsync" in c and "-aHP" in c]
        assert len(rsync_copy) >= 1
        assert rsync_copy[0][1].get("timeout") == 3600
        rsync_verify = [(c, k) for c, k in calls_with_kwargs if "--itemize-changes" in c]
        assert len(rsync_verify) >= 1
        assert rsync_verify[0][1].get("timeout") == 1800

    def test_no_bwlimit_by_default(self, mocker):
        """No --bwlimit in rsync when not specified."""
        mock_run, calls = self._make_mock_run()
        mocker.patch("rebalancer.run_cmd", side_effect=mock_run)
        entry = PlanEntry("/mnt/disk1/TV_Shows/Show", 100_000, "/mnt/disk1", "/mnt/disk10")
        transfer_unit(entry)
        rsync_calls = [c for c in calls if "rsync" in c]
        assert not any("--bwlimit" in c for c in rsync_calls)

    def test_skips_when_space_below_min_free(self, mocker):
        """Skip if free space after transfer would be below min_free threshold."""
        def set_low_space(result):
            # 100GB free
            result.stdout = (
                "Filesystem     1024-blocks         Used   Available Capacity Mounted on\n"
                "/dev/md10p1    15623792588  15519029388   104763200       99% /mnt/disk10\n"
            )
        mock_run, calls = self._make_mock_run(
            overrides={"df -Pk": set_low_space}
        )
        mocker.patch("rebalancer.run_cmd", side_effect=mock_run)
        # Transfer 50GB — would leave ~50GB free, below 60GB min_free
        entry = PlanEntry("/mnt/disk1/TV_Shows/Show", 50_000_000_000, "/mnt/disk1", "/mnt/disk10")
        min_free_60g = 60_000_000_000
        status = transfer_unit(entry, min_free=min_free_60g)
        assert status == "skipped_full"


# --- PlanDB.update_status ---

class TestUpdateStatus:
    def test_updates_status(self, db_path):
        from rebalancer import PlanDB
        db = PlanDB(db_path)
        db.write_plan([
            PlanEntry("/mnt/disk1/A", 100, "/mnt/disk1", "/mnt/disk10", status="pending"),
            PlanEntry("/mnt/disk1/B", 200, "/mnt/disk1", "/mnt/disk10", status="pending"),
        ])
        db.update_status("/mnt/disk1/A", "in_progress")
        loaded = db.get_all()
        assert loaded[0].status == "in_progress"
        assert loaded[1].status == "pending"
        db.close()

    def test_updates_only_matching_entry(self, db_path):
        from rebalancer import PlanDB
        db = PlanDB(db_path)
        db.write_plan([
            PlanEntry("/mnt/disk1/A", 100, "/mnt/disk1", "/mnt/disk10", status="pending"),
            PlanEntry("/mnt/disk1/B", 200, "/mnt/disk1", "/mnt/disk10", status="pending"),
        ])
        db.update_status("/mnt/disk1/B", "completed")
        loaded = db.get_all()
        assert loaded[0].status == "pending"
        assert loaded[1].status == "completed"
        db.close()


# --- log_transfer ---

class TestLogTransfer:
    def test_appends_to_log_file(self, state_dir):
        log_path = state_dir / "transfers.log"
        entry = PlanEntry("/mnt/disk1/A", 100, "/mnt/disk1", "/mnt/disk10")
        log_transfer(log_path, entry, "cleaned")
        content = log_path.read_text()
        assert "/mnt/disk1/A" in content
        assert "cleaned" in content
        assert "/mnt/disk10" in content

    def test_appends_multiple_entries(self, state_dir):
        log_path = state_dir / "transfers.log"
        e1 = PlanEntry("/mnt/disk1/A", 100, "/mnt/disk1", "/mnt/disk10")
        e2 = PlanEntry("/mnt/disk1/B", 200, "/mnt/disk1", "/mnt/disk11")
        log_transfer(log_path, e1, "cleaned")
        log_transfer(log_path, e2, "cleaned")
        lines = log_path.read_text().strip().splitlines()
        assert len(lines) == 2

    def test_log_includes_timestamp(self, state_dir):
        log_path = state_dir / "transfers.log"
        entry = PlanEntry("/mnt/disk1/A", 100, "/mnt/disk1", "/mnt/disk10")
        log_transfer(log_path, entry, "cleaned")
        content = log_path.read_text()
        # Should have ISO-ish timestamp
        assert "20" in content  # year prefix

    def test_log_write_failure_does_not_raise(self, state_dir):
        """H4: log_transfer should not propagate exceptions on write failure."""
        log_path = state_dir / "nonexistent_dir" / "transfers.log"
        entry = PlanEntry("/mnt/disk1/A", 100, "/mnt/disk1", "/mnt/disk10")
        # Should not raise — log failure is non-fatal
        log_transfer(log_path, entry, "cleaned")

    def test_log_with_detail_has_7_columns(self, state_dir):
        """When detail is provided, log line has 7 TSV columns."""
        log_path = state_dir / "transfers.log"
        entry = PlanEntry("/mnt/disk1/A", 100, "/mnt/disk1", "/mnt/disk10")
        log_transfer(log_path, entry, "error_copy", detail="rsync: connection refused")
        content = log_path.read_text().strip()
        columns = content.split("\t")
        assert len(columns) == 7
        assert columns[6] == "rsync: connection refused"

    def test_log_without_detail_has_6_columns(self, state_dir):
        """Without detail, log line stays at 6 TSV columns (backward compat)."""
        log_path = state_dir / "transfers.log"
        entry = PlanEntry("/mnt/disk1/A", 100, "/mnt/disk1", "/mnt/disk10")
        log_transfer(log_path, entry, "cleaned")
        content = log_path.read_text().strip()
        columns = content.split("\t")
        assert len(columns) == 6


class TestTransferResult:
    def test_returns_transfer_result_type(self, mocker):
        """transfer_unit must return TransferResult, not str."""
        mock_run, _ = TestTransferUnit()._make_mock_run()
        mocker.patch("rebalancer.run_cmd", side_effect=mock_run)
        entry = PlanEntry("/mnt/disk1/TV_Shows/Show", 100_000, "/mnt/disk1", "/mnt/disk10")
        result = transfer_unit(entry)
        assert isinstance(result, TransferResult)

    def test_transfer_result_equals_string(self, mocker):
        """TransferResult must compare equal to status string."""
        mock_run, _ = TestTransferUnit()._make_mock_run()
        mocker.patch("rebalancer.run_cmd", side_effect=mock_run)
        entry = PlanEntry("/mnt/disk1/TV_Shows/Show", 100_000, "/mnt/disk1", "/mnt/disk10")
        result = transfer_unit(entry)
        assert result == "cleaned"
        assert result != "error_copy"

    def test_cleaned_has_empty_detail(self, mocker):
        """Successful transfer has no stderr detail."""
        mock_run, _ = TestTransferUnit()._make_mock_run()
        mocker.patch("rebalancer.run_cmd", side_effect=mock_run)
        entry = PlanEntry("/mnt/disk1/TV_Shows/Show", 100_000, "/mnt/disk1", "/mnt/disk10")
        result = transfer_unit(entry)
        assert result.detail == ""

    def test_error_copy_contains_stderr(self, mocker):
        """On rsync copy failure, detail contains stderr text."""
        def set_rsync_error(result):
            result.returncode = 1
            result.stderr = "rsync: connection unexpectedly closed"
        mock_run, _ = TestTransferUnit()._make_mock_run(
            overrides={"-aHP": set_rsync_error}
        )
        mocker.patch("rebalancer.run_cmd", side_effect=mock_run)
        entry = PlanEntry("/mnt/disk1/TV_Shows/Show", 100_000, "/mnt/disk1", "/mnt/disk10")
        result = transfer_unit(entry)
        assert result == "error_copy"
        assert "connection unexpectedly closed" in result.detail

    def test_error_verify_contains_stderr(self, mocker):
        """On verify failure with stderr, detail captures it."""
        def set_verify_fail(result):
            result.returncode = 1
            result.stderr = "rsync: read errors"
            result.stdout = ">f.st...... bad.mkv\n"
        mock_run, _ = TestTransferUnit()._make_mock_run(
            overrides={"--itemize-changes": set_verify_fail}
        )
        mocker.patch("rebalancer.run_cmd", side_effect=mock_run)
        entry = PlanEntry("/mnt/disk1/TV_Shows/Show", 100_000, "/mnt/disk1", "/mnt/disk10")
        result = transfer_unit(entry)
        assert result == "error_verify"
        assert "read errors" in result.detail

    def test_error_delete_contains_stderr(self, mocker):
        """On rm -rf failure, detail captures stderr."""
        def set_rm_fail(result):
            result.returncode = 1
            result.stderr = "rm: cannot remove: Permission denied"
        mock_run, _ = TestTransferUnit()._make_mock_run(
            overrides={"rm -rf": set_rm_fail}
        )
        mocker.patch("rebalancer.run_cmd", side_effect=mock_run)
        entry = PlanEntry("/mnt/disk1/TV_Shows/Show", 100_000, "/mnt/disk1", "/mnt/disk10")
        result = transfer_unit(entry)
        assert result == "error_delete"
        assert "Permission denied" in result.detail


    def test_in_use_before_delete_returns_skipped_in_use(self, mocker):
        """Pre-delete in-use check should return skipped_in_use, not error_verify."""
        def set_in_use(result):
            result.returncode = 0
            result.stdout = "COMMAND  PID USER   FD   TYPE DEVICE SIZE/OFF NODE NAME\nplex    1234 root    4r   REG  0,38      100 file.mkv\n"
        mock_run, calls = TestTransferUnit()._make_mock_run(overrides={"lsof": set_in_use})
        mocker.patch("rebalancer.run_cmd", side_effect=mock_run)
        entry = PlanEntry("/mnt/disk1/TV_Shows/Show", 100_000, "/mnt/disk1", "/mnt/disk10")
        result = transfer_unit(entry)
        assert result == "skipped_in_use"
        assert not any("rm -rf" in c for c in calls), "rm called despite files in use"

    def test_timeout_returns_transfer_result_not_string(self, mocker):
        """CRITICAL: outer TimeoutExpired must return TransferResult, not str."""
        import subprocess as sp
        calls = [0]
        def mock_run(cmd, **kwargs):
            cmd_str = " ".join(cmd) if isinstance(cmd, list) else cmd
            calls[0] += 1
            result = MagicMock()
            result.stdout = ""
            result.stderr = ""
            # Let the first call (test -e) succeed, then timeout on mkdir
            if calls[0] == 1:
                result.returncode = 0  # source exists
            elif calls[0] == 2:
                result.returncode = 1  # target doesn't exist
            elif calls[0] == 3:
                # df check
                result.stdout = (
                    "Filesystem     1024-blocks      Used Available Capacity Mounted on\n"
                    "/dev/md10p1    15623792588 7890384356 7733408232      51% /mnt/disk10\n"
                )
                result.returncode = 0
            else:
                raise sp.TimeoutExpired(cmd=cmd, timeout=1)
            return result

        mocker.patch("rebalancer.run_cmd", side_effect=mock_run)
        entry = PlanEntry("/mnt/disk1/TV_Shows/Show", 100_000, "/mnt/disk1", "/mnt/disk10")
        result = transfer_unit(entry)
        assert isinstance(result, TransferResult), f"Expected TransferResult, got {type(result)}"
        assert result == "error_timeout"
        assert hasattr(result, "detail")

    def test_transfer_result_not_hashable(self):
        """TransferResult should not be hashable (no __hash__)."""
        result = TransferResult("cleaned")
        with pytest.raises(TypeError):
            hash(result)

    def test_transfer_result_eq_with_detail(self):
        """Two TransferResults with same status but different detail are not equal."""
        a = TransferResult("error_copy", "detail1")
        b = TransferResult("error_copy", "detail2")
        assert a != b  # different detail
        c = TransferResult("error_copy", "detail1")
        assert a == c  # same status and detail


class TestTransferResultTimingFields:
    def test_timing_defaults_none(self):
        result = TransferResult("cleaned")
        assert result.copy_seconds is None
        assert result.verify_seconds is None
        assert result.delete_seconds is None

    def test_timing_fields_settable(self):
        result = TransferResult("cleaned", copy_seconds=10.5, verify_seconds=5.2, delete_seconds=0.3)
        assert result.copy_seconds == 10.5
        assert result.verify_seconds == 5.2
        assert result.delete_seconds == 0.3

    def test_string_eq_still_works_with_timing(self):
        result = TransferResult("cleaned", copy_seconds=100.0)
        assert result == "cleaned"

    def test_successful_transfer_has_phase_timings(self, mocker):
        """A successful transfer should return non-None timing for all phases."""
        mock_run, _ = TestTransferUnit()._make_mock_run()
        mocker.patch("rebalancer.run_cmd", side_effect=mock_run)
        entry = PlanEntry("/mnt/disk1/TV_Shows/Show", 100_000, "/mnt/disk1", "/mnt/disk10")
        result = transfer_unit(entry)
        assert result == "cleaned"
        assert result.copy_seconds is not None and result.copy_seconds >= 0
        assert result.verify_seconds is not None and result.verify_seconds >= 0
        assert result.delete_seconds is not None and result.delete_seconds >= 0

    def test_error_copy_has_partial_timings(self, mocker):
        """Failed copy should have copy_seconds but not verify/delete."""
        mock_run, _ = TestTransferUnit()._make_mock_run(overrides={"-aHP": 1})
        mocker.patch("rebalancer.run_cmd", side_effect=mock_run)
        entry = PlanEntry("/mnt/disk1/TV_Shows/Show", 100_000, "/mnt/disk1", "/mnt/disk10")
        result = transfer_unit(entry)
        assert result == "error_copy"
        assert result.copy_seconds is not None
        assert result.verify_seconds is None
        assert result.delete_seconds is None


class TestTruncateStderr:
    def test_empty_string(self):
        assert _truncate_stderr("") == ""

    def test_short_string_unchanged(self):
        assert _truncate_stderr("error message") == "error message"

    def test_truncated_at_500(self):
        long_msg = "x" * 600
        result = _truncate_stderr(long_msg)
        assert len(result) == 503  # 500 + "..."
        assert result.endswith("...")

    def test_tabs_sanitized(self):
        result = _truncate_stderr("col1\tcol2\tcol3")
        assert "\t" not in result
        assert "col1 col2 col3" == result

    def test_newlines_sanitized(self):
        result = _truncate_stderr("line1\nline2\r\nline3")
        assert "\n" not in result
        assert "\r" not in result

    def test_none_input_returns_empty(self):
        assert _truncate_stderr(None) == ""


class TestProgressMode:
    def test_progress_adds_info_progress2(self, mocker):
        """rsync copy includes --info=progress2 when progress=True."""
        mock_run, calls = TestTransferUnit()._make_mock_run()
        mocker.patch("rebalancer.run_cmd", side_effect=mock_run)
        entry = PlanEntry("/mnt/disk1/TV_Shows/Show", 100_000, "/mnt/disk1", "/mnt/disk10")
        transfer_unit(entry, progress=True)
        rsync_copy = [c for c in calls if "rsync" in c and "-aHP" in c]
        assert len(rsync_copy) >= 1
        assert "--info=progress2" in rsync_copy[0]

    def test_progress_not_in_verify(self, mocker):
        """Verify phase never gets --info=progress2."""
        mock_run, calls = TestTransferUnit()._make_mock_run()
        mocker.patch("rebalancer.run_cmd", side_effect=mock_run)
        entry = PlanEntry("/mnt/disk1/TV_Shows/Show", 100_000, "/mnt/disk1", "/mnt/disk10")
        transfer_unit(entry, progress=True)
        verify_calls = [c for c in calls if "--itemize-changes" in c]
        assert len(verify_calls) >= 1
        assert "--info=progress2" not in verify_calls[0]

    def test_no_progress_by_default(self, mocker):
        """rsync copy omits --info=progress2 by default."""
        mock_run, calls = TestTransferUnit()._make_mock_run()
        mocker.patch("rebalancer.run_cmd", side_effect=mock_run)
        entry = PlanEntry("/mnt/disk1/TV_Shows/Show", 100_000, "/mnt/disk1", "/mnt/disk10")
        transfer_unit(entry)
        rsync_calls = [c for c in calls if "rsync" in c]
        assert not any("--info=progress2" in c for c in rsync_calls)


class TestPhaseStatusOutput:
    def test_copy_phase_shows_eta_and_rate(self, mocker, capsys):
        """Copying line should show Est. ETA and rate when copy_rate provided."""
        mock_run, _ = TestTransferUnit()._make_mock_run()
        mocker.patch("rebalancer.run_cmd", side_effect=mock_run)
        entry = PlanEntry("/mnt/disk1/TV_Shows/Show", 1_000_000_000, "/mnt/disk1", "/mnt/disk10")
        transfer_unit(entry, phase_status=True, copy_rate=50_000_000.0)
        output = capsys.readouterr().out
        assert "Copying..." in output
        assert "Est." in output
        assert "/s" in output

    def test_verify_phase_shows_eta_and_rate(self, mocker, capsys):
        """Verifying line should show Est. ETA and rate when verify_rate provided."""
        mock_run, _ = TestTransferUnit()._make_mock_run()
        mocker.patch("rebalancer.run_cmd", side_effect=mock_run)
        entry = PlanEntry("/mnt/disk1/TV_Shows/Show", 1_000_000_000, "/mnt/disk1", "/mnt/disk10")
        transfer_unit(entry, phase_status=True, verify_rate=80_000_000.0)
        output = capsys.readouterr().out
        assert "Verifying..." in output
        assert "Est." in output

    def test_delete_phase_no_eta(self, mocker, capsys):
        """Delete line should not show ETA (no throughput data)."""
        mock_run, _ = TestTransferUnit()._make_mock_run()
        mocker.patch("rebalancer.run_cmd", side_effect=mock_run)
        entry = PlanEntry("/mnt/disk1/TV_Shows/Show", 100_000, "/mnt/disk1", "/mnt/disk10")
        transfer_unit(entry, phase_status=True)
        output = capsys.readouterr().out
        assert "Deleting source..." in output
        # Delete line should NOT have Est.
        delete_line = [l for l in output.split("\n") if "Deleting" in l][0]
        assert "Est." not in delete_line

    def test_no_rates_no_eta(self, mocker, capsys):
        """Without rates, phase lines should not show Est."""
        mock_run, _ = TestTransferUnit()._make_mock_run()
        mocker.patch("rebalancer.run_cmd", side_effect=mock_run)
        entry = PlanEntry("/mnt/disk1/TV_Shows/Show", 100_000, "/mnt/disk1", "/mnt/disk10")
        transfer_unit(entry, phase_status=True)
        output = capsys.readouterr().out
        copy_line = [l for l in output.split("\n") if "Copying" in l][0]
        assert "Est." not in copy_line

    def test_copy_rate_only_no_verify_eta(self, mocker, capsys):
        """With copy_rate but no verify_rate, only copy shows Est."""
        mock_run, _ = TestTransferUnit()._make_mock_run()
        mocker.patch("rebalancer.run_cmd", side_effect=mock_run)
        entry = PlanEntry("/mnt/disk1/TV_Shows/Show", 1_000_000_000, "/mnt/disk1", "/mnt/disk10")
        transfer_unit(entry, phase_status=True, copy_rate=50_000_000.0)
        output = capsys.readouterr().out
        copy_line = [l for l in output.split("\n") if "Copying" in l][0]
        verify_line = [l for l in output.split("\n") if "Verifying" in l][0]
        assert "Est." in copy_line
        assert "Est." not in verify_line

    def test_copy_actual_appended_same_line(self, mocker, capsys):
        """After copy completes, actual timing appended with arrow on same line."""
        mock_run, _ = TestTransferUnit()._make_mock_run()
        mocker.patch("rebalancer.run_cmd", side_effect=mock_run)
        entry = PlanEntry("/mnt/disk1/TV_Shows/Show", 1_000_000_000, "/mnt/disk1", "/mnt/disk10")
        transfer_unit(entry, phase_status=True, copy_rate=50_000_000.0)
        output = capsys.readouterr().out
        copy_line = [l for l in output.split("\n") if "Copying" in l][0]
        # Should have arrow separator with actual timing
        assert "\u2192" in copy_line  # →
        assert "Est." in copy_line

    def test_verify_actual_appended_same_line(self, mocker, capsys):
        """After verify completes, actual timing appended on same line."""
        mock_run, _ = TestTransferUnit()._make_mock_run()
        mocker.patch("rebalancer.run_cmd", side_effect=mock_run)
        entry = PlanEntry("/mnt/disk1/TV_Shows/Show", 1_000_000_000, "/mnt/disk1", "/mnt/disk10")
        transfer_unit(entry, phase_status=True, verify_rate=80_000_000.0)
        output = capsys.readouterr().out
        verify_line = [l for l in output.split("\n") if "Verifying" in l][0]
        assert "\u2192" in verify_line

    def test_actual_without_est_still_shows(self, mocker, capsys):
        """Even without rate estimates, actual timing should appear after arrow."""
        mock_run, _ = TestTransferUnit()._make_mock_run()
        mocker.patch("rebalancer.run_cmd", side_effect=mock_run)
        entry = PlanEntry("/mnt/disk1/TV_Shows/Show", 100_000, "/mnt/disk1", "/mnt/disk10")
        transfer_unit(entry, phase_status=True)
        output = capsys.readouterr().out
        copy_line = [l for l in output.split("\n") if "Copying" in l][0]
        assert "\u2192" in copy_line

    def test_progress_mode_copy_actual_on_separate_line(self, mocker, capsys):
        """With --progress, actual timing goes on a separate 'Copied.' line."""
        mock_run, _ = TestTransferUnit()._make_mock_run()
        mocker.patch("rebalancer.run_cmd", side_effect=mock_run)
        entry = PlanEntry("/mnt/disk1/TV_Shows/Show", 1_000_000_000, "/mnt/disk1", "/mnt/disk10")
        transfer_unit(entry, phase_status=True, progress=True, copy_rate=50_000_000.0)
        output = capsys.readouterr().out
        assert "Copied." in output
        # Copying line should NOT have the arrow (progress breaks same-line)
        copy_line = [l for l in output.split("\n") if "Copying" in l][0]
        assert "\u2192" not in copy_line

    def test_progress_mode_verify_actual_on_separate_line(self, mocker, capsys):
        """With --progress, verify actual goes on 'Verified.' line."""
        mock_run, _ = TestTransferUnit()._make_mock_run()
        mocker.patch("rebalancer.run_cmd", side_effect=mock_run)
        entry = PlanEntry("/mnt/disk1/TV_Shows/Show", 1_000_000_000, "/mnt/disk1", "/mnt/disk10")
        transfer_unit(entry, phase_status=True, progress=True, verify_rate=80_000_000.0)
        output = capsys.readouterr().out
        assert "Verified." in output

    def test_copy_error_still_shows_actual(self, mocker, capsys):
        """Even on error_copy, actual timing should be printed."""
        mock_run, _ = TestTransferUnit()._make_mock_run(overrides={"rsync": 1})
        mocker.patch("rebalancer.run_cmd", side_effect=mock_run)
        entry = PlanEntry("/mnt/disk1/TV_Shows/Show", 100_000, "/mnt/disk1", "/mnt/disk10")
        result = transfer_unit(entry, phase_status=True, copy_rate=50_000_000.0)
        assert result == "error_copy"
        output = capsys.readouterr().out
        copy_line = [l for l in output.split("\n") if "Copying" in l][0]
        assert "\u2192" in copy_line

    def test_progress_passthrough_passes_to_run_cmd(self, mocker):
        """With progress=True, run_cmd is called with passthrough=True for copy phase."""
        calls_with_kwargs = []
        test_e_count = [0]

        def mock_run(cmd, **kwargs):
            cmd_str = " ".join(cmd) if isinstance(cmd, list) else cmd
            calls_with_kwargs.append((cmd_str, kwargs))
            result = MagicMock()
            result.stdout = ""
            result.stderr = ""
            if "test -L" in cmd_str:
                result.returncode = 1
            elif "test -e" in cmd_str:
                test_e_count[0] += 1
                result.returncode = 0 if test_e_count[0] == 1 else 1
            elif "lsof" in cmd_str:
                result.returncode = 1
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
        entry = PlanEntry("/mnt/disk1/TV_Shows/Show", 100_000, "/mnt/disk1", "/mnt/disk10")
        transfer_unit(entry, progress=True)

        # Copy phase should have passthrough=True
        rsync_copy = [(c, k) for c, k in calls_with_kwargs if "rsync" in c and "-aHP" in c]
        assert len(rsync_copy) >= 1
        assert rsync_copy[0][1].get("passthrough") is True

        # Verify phase should NOT have passthrough=True
        verify = [(c, k) for c, k in calls_with_kwargs if "--itemize-changes" in c]
        assert len(verify) >= 1
        assert verify[0][1].get("passthrough") is not True
