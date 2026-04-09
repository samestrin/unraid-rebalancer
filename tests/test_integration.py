"""Integration tests — full pipeline end-to-end."""

from unittest.mock import MagicMock

import pytest

from rebalancer import (
    DiskInfo,
    MovableUnit,
    PlanDB,
    PlanEntry,
    TransferResult,
    PLAN_DB_FILE,
    main,
)


class TestFullPipeline:
    def _setup_mocks(self, mocker, state_dir):
        mocker.patch("rebalancer.STATE_DIR", state_dir)
        mocker.patch("rebalancer.setup_signal_handlers")
        mocker.patch("rebalancer.shutdown_requested", return_value=False)
        mocker.patch("rebalancer.is_within_active_hours", return_value=True)
        mocker.patch("rebalancer.check_in_use", return_value=False)
        mocker.patch("rebalancer.transfer_unit", return_value=TransferResult("cleaned"))

        disks = [
            DiskInfo("/mnt/disk1", 1_000_000, 900_000, 100_000, 90),
            DiskInfo("/mnt/disk2", 1_000_000, 850_000, 150_000, 85),
            DiskInfo("/mnt/disk3", 1_000_000, 300_000, 700_000, 30),
        ]
        mocker.patch("rebalancer.discover_disks", return_value=disks)

        units = [
            MovableUnit("/mnt/disk1/TV_Shows/ShowA", "TV_Shows", "ShowA", 50_000, "/mnt/disk1"),
            MovableUnit("/mnt/disk1/TV_Shows/ShowB", "TV_Shows", "ShowB", 30_000, "/mnt/disk1"),
            MovableUnit("/mnt/disk2/Anime/Naruto", "Anime", "Naruto", 40_000, "/mnt/disk2"),
        ]
        mocker.patch("rebalancer.scan_movable_units", return_value=units)

    def test_full_run_creates_plan_and_executes(self, state_dir, mocker, capsys):
        self._setup_mocks(mocker, state_dir)
        result = main(["--yes", "--min-free-space", "0"])
        assert result == 0

        db = PlanDB(state_dir / PLAN_DB_FILE)
        assert db.has_plan()

        log_path = state_dir / "transfers.log"
        assert log_path.exists()
        db.close()

        output = capsys.readouterr().out
        assert "Discovering" in output

    def test_dry_run_creates_plan_no_execute(self, state_dir, mocker, capsys):
        self._setup_mocks(mocker, state_dir)
        transfer_mock = mocker.patch("rebalancer.transfer_unit")

        result = main(["--dry-run", "--min-free-space", "0"])
        assert result == 0
        transfer_mock.assert_not_called()

        db = PlanDB(state_dir / PLAN_DB_FILE)
        entries = db.get_all()
        assert all(e.status == "pending" for e in entries)
        db.close()

    def test_resume_after_interrupt(self, state_dir, db_path, mocker, capsys):
        mocker.patch("rebalancer.STATE_DIR", state_dir)
        mocker.patch("rebalancer.setup_signal_handlers")
        mocker.patch("rebalancer.shutdown_requested", return_value=False)
        mocker.patch("rebalancer.is_within_active_hours", return_value=True)
        mocker.patch("rebalancer.check_in_use", return_value=False)
        mocker.patch("rebalancer.transfer_unit", return_value=TransferResult("cleaned"))

        db = PlanDB(db_path)
        db.write_plan([
            PlanEntry("/mnt/disk1/TV_Shows/ShowA", 50_000, "/mnt/disk1", "/mnt/disk3", status="in_progress"),
            PlanEntry("/mnt/disk1/TV_Shows/ShowB", 30_000, "/mnt/disk1", "/mnt/disk3", status="pending"),
        ])
        db.close()

        mocker.patch("rebalancer.discover_disks")
        mocker.patch("rebalancer.scan_movable_units")

        result = main(["--yes"])
        assert result == 0
        output = capsys.readouterr().out
        assert "Recovered 1" in output

    def test_force_rescan_rebuilds_plan(self, state_dir, db_path, mocker, capsys):
        self._setup_mocks(mocker, state_dir)

        db = PlanDB(db_path)
        db.write_plan([
            PlanEntry("/mnt/disk1/old", 100, "/mnt/disk1", "/mnt/disk3", status="completed"),
        ])
        db.close()

        result = main(["--force-rescan", "--yes", "--min-free-space", "0"])
        assert result == 0
        output = capsys.readouterr().out
        assert "Discovering" in output

    def test_force_rescan_warns_with_pending(self, state_dir, db_path, mocker, capsys):
        """Force rescan with pending entries prompts for confirmation."""
        self._setup_mocks(mocker, state_dir)

        db = PlanDB(db_path)
        db.write_plan([
            PlanEntry("/mnt/disk1/A", 100, "/mnt/disk1", "/mnt/disk3", status="pending"),
        ])
        db.close()

        # User says no
        mocker.patch("builtins.input", return_value="n")
        result = main(["--force-rescan", "--min-free-space", "0"])
        assert result == 0
        output = capsys.readouterr().out
        assert "Warning" in output
        assert "Aborted" in output

    def test_force_rescan_yes_flag_skips_prompt(self, state_dir, db_path, mocker, capsys):
        """--yes flag skips the confirmation prompt."""
        self._setup_mocks(mocker, state_dir)

        db = PlanDB(db_path)
        db.write_plan([
            PlanEntry("/mnt/disk1/A", 100, "/mnt/disk1", "/mnt/disk3", status="pending"),
        ])
        db.close()

        result = main(["--force-rescan", "--yes", "--min-free-space", "0"])
        assert result == 0
        output = capsys.readouterr().out
        assert "Discovering" in output
        assert "Warning" not in output

    def test_force_rescan_no_pending_no_prompt(self, state_dir, db_path, mocker, capsys):
        """Force rescan with only completed entries doesn't prompt."""
        self._setup_mocks(mocker, state_dir)

        db = PlanDB(db_path)
        db.write_plan([
            PlanEntry("/mnt/disk1/A", 100, "/mnt/disk1", "/mnt/disk3", status="completed"),
        ])
        db.close()

        result = main(["--force-rescan", "--yes", "--min-free-space", "0"])
        assert result == 0
        output = capsys.readouterr().out
        assert "Warning" not in output
        assert "Discovering" in output

    def test_all_balanced_exits_early(self, state_dir, mocker, capsys):
        mocker.patch("rebalancer.STATE_DIR", state_dir)
        mocker.patch("rebalancer.setup_signal_handlers")

        disks = [
            DiskInfo("/mnt/disk1", 1_000_000, 500_000, 500_000, 50),
            DiskInfo("/mnt/disk2", 1_000_000, 600_000, 400_000, 60),
        ]
        mocker.patch("rebalancer.discover_disks", return_value=disks)
        mocker.patch("rebalancer.scan_movable_units", return_value=[])

        result = main(["--yes"])
        assert result == 0
        output = capsys.readouterr().out
        assert "Nothing to do" in output

    def test_skip_in_use_files(self, state_dir, db_path, mocker, capsys):
        mocker.patch("rebalancer.STATE_DIR", state_dir)
        mocker.patch("rebalancer.setup_signal_handlers")
        mocker.patch("rebalancer.shutdown_requested", return_value=False)
        mocker.patch("rebalancer.is_within_active_hours", return_value=True)
        mocker.patch("rebalancer.check_in_use", return_value=True)

        db = PlanDB(db_path)
        db.write_plan([
            PlanEntry("/mnt/disk1/TV_Shows/A", 100, "/mnt/disk1", "/mnt/disk3", status="pending"),
        ])
        db.close()

        mocker.patch("rebalancer.discover_disks")
        mocker.patch("rebalancer.scan_movable_units")

        transfer_mock = mocker.patch("rebalancer.transfer_unit")
        result = main(["--yes"])
        assert result == 0
        transfer_mock.assert_not_called()
        output = capsys.readouterr().out
        assert "SKIP" in output

    def test_status_mode(self, state_dir, db_path, mocker, capsys):
        mocker.patch("rebalancer.STATE_DIR", state_dir)

        db = PlanDB(db_path)
        db.write_plan([
            PlanEntry("/mnt/disk1/A", 100_000_000, "/mnt/disk1", "/mnt/disk3", status="pending"),
            PlanEntry("/mnt/disk1/B", 200_000_000, "/mnt/disk1", "/mnt/disk3", status="cleaned"),
        ])
        db.close()

        result = main(["--status"])
        assert result == 0
        output = capsys.readouterr().out
        assert "Plan Summary:" in output
        assert "Pending" in output
        assert "Cleaned" in output
        assert "Total entries:" in output
        assert "%" in output

    def test_status_shows_current_transfer(self, state_dir, db_path, mocker, capsys):
        mocker.patch("rebalancer.STATE_DIR", state_dir)

        db = PlanDB(db_path)
        db.write_plan([
            PlanEntry("/mnt/disk1/Movies/2023", 5_000_000_000, "/mnt/disk1", "/mnt/disk3", status="in_progress"),
            PlanEntry("/mnt/disk2/TV/ShowA", 1_000_000_000, "/mnt/disk2", "/mnt/disk5", status="pending"),
        ])
        db.close()

        result = main(["--status"])
        assert result == 0
        output = capsys.readouterr().out
        assert "Current Transfer:" in output
        assert "Movies/2023" in output
        assert "disk1" in output

    def test_status_shows_up_next(self, state_dir, db_path, mocker, capsys):
        mocker.patch("rebalancer.STATE_DIR", state_dir)

        db = PlanDB(db_path)
        entries = [
            PlanEntry(f"/mnt/disk1/TV/Show{i}", 100_000_000 * i, "/mnt/disk1", "/mnt/disk3", status="pending")
            for i in range(1, 8)
        ]
        db.write_plan(entries)
        db.close()

        result = main(["--status"])
        assert result == 0
        output = capsys.readouterr().out
        assert "Up Next:" in output
        # Should show first 5 only
        assert "Show1" in output
        assert "Show5" in output
        assert "Show6" not in output

    def test_status_no_pending_no_up_next(self, state_dir, db_path, mocker, capsys):
        mocker.patch("rebalancer.STATE_DIR", state_dir)

        db = PlanDB(db_path)
        db.write_plan([
            PlanEntry("/mnt/disk1/A", 100, "/mnt/disk1", "/mnt/disk3", status="cleaned"),
        ])
        db.close()

        result = main(["--status"])
        assert result == 0
        output = capsys.readouterr().out
        assert "Up Next:" not in output
        assert "Current Transfer:" not in output

    def test_limit_stops_after_n_transfers(self, state_dir, db_path, mocker, capsys):
        mocker.patch("rebalancer.STATE_DIR", state_dir)
        mocker.patch("rebalancer.setup_signal_handlers")
        mocker.patch("rebalancer.shutdown_requested", return_value=False)
        mocker.patch("rebalancer.is_within_active_hours", return_value=True)
        mocker.patch("rebalancer.check_in_use", return_value=False)
        mocker.patch("rebalancer.transfer_unit", return_value=TransferResult("cleaned"))

        db = PlanDB(db_path)
        db.write_plan([
            PlanEntry("/mnt/disk1/TV_Shows/A", 100, "/mnt/disk1", "/mnt/disk3", status="pending"),
            PlanEntry("/mnt/disk1/TV_Shows/B", 200, "/mnt/disk1", "/mnt/disk3", status="pending"),
            PlanEntry("/mnt/disk1/TV_Shows/C", 300, "/mnt/disk1", "/mnt/disk3", status="pending"),
        ])
        db.close()

        mocker.patch("rebalancer.discover_disks")
        mocker.patch("rebalancer.scan_movable_units")

        result = main(["--limit", "1", "--yes"])
        assert result == 0
        output = capsys.readouterr().out
        assert "Limit reached" in output

        db = PlanDB(db_path)
        cleaned = db.get_all(status_filter="cleaned")
        pending = db.get_pending()
        assert len(cleaned) == 1
        assert len(pending) == 2
        db.close()

    def test_limit_counts_only_successful_transfers(self, state_dir, db_path, mocker, capsys):
        mocker.patch("rebalancer.STATE_DIR", state_dir)
        mocker.patch("rebalancer.setup_signal_handlers")
        mocker.patch("rebalancer.shutdown_requested", return_value=False)
        mocker.patch("rebalancer.is_within_active_hours", return_value=True)

        check_calls = iter([True, False, False])
        mocker.patch("rebalancer.check_in_use", side_effect=check_calls)
        mocker.patch("rebalancer.transfer_unit", return_value=TransferResult("cleaned"))

        db = PlanDB(db_path)
        db.write_plan([
            PlanEntry("/mnt/disk1/TV_Shows/InUse", 100, "/mnt/disk1", "/mnt/disk3", status="pending"),
            PlanEntry("/mnt/disk1/TV_Shows/Ready", 200, "/mnt/disk1", "/mnt/disk3", status="pending"),
            PlanEntry("/mnt/disk1/TV_Shows/Also", 300, "/mnt/disk1", "/mnt/disk3", status="pending"),
        ])
        db.close()

        mocker.patch("rebalancer.discover_disks")
        mocker.patch("rebalancer.scan_movable_units")

        result = main(["--limit", "1", "--yes"])
        assert result == 0

        db = PlanDB(db_path)
        skipped = db.get_all(status_filter="skipped")
        cleaned = db.get_all(status_filter="cleaned")
        assert len(skipped) == 1
        assert len(cleaned) == 1
        db.close()


class TestInputValidation:
    def test_max_used_too_low(self, state_dir, mocker, capsys):
        mocker.patch("rebalancer.STATE_DIR", state_dir)
        result = main(["--max-used", "0"])
        assert result == 1
        assert "must be between" in capsys.readouterr().out

    def test_max_used_too_high(self, state_dir, mocker, capsys):
        mocker.patch("rebalancer.STATE_DIR", state_dir)
        result = main(["--max-used", "100"])
        assert result == 1
        assert "must be between" in capsys.readouterr().out

    def test_negative_limit(self, state_dir, mocker, capsys):
        mocker.patch("rebalancer.STATE_DIR", state_dir)
        result = main(["--limit", "-5"])
        assert result == 1
        assert "must be >= 0" in capsys.readouterr().out

    def test_negative_min_free_space(self, state_dir, mocker, capsys):
        mocker.patch("rebalancer.STATE_DIR", state_dir)
        result = main(["--min-free-space", "-1"])
        assert result == 1
        assert "must be >= 0" in capsys.readouterr().out

    def test_invalid_active_hours_format(self, state_dir, mocker, capsys):
        mocker.patch("rebalancer.STATE_DIR", state_dir)
        result = main(["--active-hours", "noon-midnight"])
        assert result == 1
        assert "Invalid" in capsys.readouterr().out

    def test_zero_copy_timeout_rejected(self, state_dir, mocker, capsys):
        mocker.patch("rebalancer.STATE_DIR", state_dir)
        result = main(["--copy-timeout", "0"])
        assert result == 1
        assert "must be > 0" in capsys.readouterr().out

    def test_zero_verify_timeout_rejected(self, state_dir, mocker, capsys):
        mocker.patch("rebalancer.STATE_DIR", state_dir)
        result = main(["--verify-timeout", "0"])
        assert result == 1
        assert "must be > 0" in capsys.readouterr().out


class TestSessionTransferLimit:
    def test_session_transfer_limit_set_during_limited_run(self, state_dir, db_path, mocker, capsys):
        """When --limit is used, session_transfer_limit should be stored in meta."""
        mocker.patch("rebalancer.STATE_DIR", state_dir)
        mocker.patch("rebalancer.setup_signal_handlers")
        mocker.patch("rebalancer.shutdown_requested", return_value=False)
        mocker.patch("rebalancer.is_within_active_hours", return_value=True)
        mocker.patch("rebalancer.check_in_use", return_value=False)
        mocker.patch("rebalancer.transfer_unit", return_value=TransferResult("cleaned"))

        db = PlanDB(db_path)
        db.write_plan([
            PlanEntry("/mnt/disk1/A", 100, "/mnt/disk1", "/mnt/disk3", status="pending"),
            PlanEntry("/mnt/disk1/B", 200, "/mnt/disk1", "/mnt/disk3", status="pending"),
        ])
        db.close()

        mocker.patch("rebalancer.discover_disks")
        mocker.patch("rebalancer.scan_movable_units")

        result = main(["--limit", "1", "--yes"])
        assert result == 0

        db = PlanDB(db_path)
        assert db.get_meta("session_transfer_limit") is None  # cleared after run
        db.close()

    def test_session_transfer_limit_cleared_after_full_run(self, state_dir, db_path, mocker, capsys):
        """After all transfers complete, session_transfer_limit should be cleared."""
        mocker.patch("rebalancer.STATE_DIR", state_dir)
        mocker.patch("rebalancer.setup_signal_handlers")
        mocker.patch("rebalancer.shutdown_requested", return_value=False)
        mocker.patch("rebalancer.is_within_active_hours", return_value=True)
        mocker.patch("rebalancer.check_in_use", return_value=False)
        mocker.patch("rebalancer.transfer_unit", return_value=TransferResult("cleaned"))

        db = PlanDB(db_path)
        db.write_plan([
            PlanEntry("/mnt/disk1/A", 100, "/mnt/disk1", "/mnt/disk3", status="pending"),
            PlanEntry("/mnt/disk1/B", 200, "/mnt/disk1", "/mnt/disk3", status="pending"),
        ])
        db.close()

        mocker.patch("rebalancer.discover_disks")
        mocker.patch("rebalancer.scan_movable_units")

        result = main(["--yes"])
        assert result == 0

        db = PlanDB(db_path)
        assert db.get_meta("session_transfer_limit") is None
        db.close()

    def test_no_limit_flag_does_not_set_meta(self, state_dir, db_path, mocker, capsys):
        """Without --limit, session_transfer_limit should never be set."""
        mocker.patch("rebalancer.STATE_DIR", state_dir)
        mocker.patch("rebalancer.setup_signal_handlers")
        mocker.patch("rebalancer.shutdown_requested", return_value=False)
        mocker.patch("rebalancer.is_within_active_hours", return_value=True)
        mocker.patch("rebalancer.check_in_use", return_value=False)

        # Capture set_meta calls to verify the key is never written
        original_set_meta = PlanDB.set_meta
        set_meta_keys = []

        def tracking_set_meta(self_db, key, value):
            set_meta_keys.append(key)
            return original_set_meta(self_db, key, value)

        mocker.patch.object(PlanDB, "set_meta", tracking_set_meta)
        mocker.patch("rebalancer.transfer_unit", return_value=TransferResult("cleaned"))

        db = PlanDB(db_path)
        db.write_plan([
            PlanEntry("/mnt/disk1/A", 100, "/mnt/disk1", "/mnt/disk3", status="pending"),
        ])
        db.close()

        mocker.patch("rebalancer.discover_disks")
        mocker.patch("rebalancer.scan_movable_units")

        result = main(["--yes"])
        assert result == 0
        assert "session_transfer_limit" not in set_meta_keys
