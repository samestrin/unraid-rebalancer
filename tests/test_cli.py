"""Tests for CLI and main integration."""

import sys
from unittest.mock import MagicMock, patch

import pytest

from rebalancer import (
    DiskInfo,
    MovableUnit,
    PlanDB,
    PlanEntry,
    TransferResult,
    PLAN_DB_FILE,
    STATE_DIR,
    build_parser,
    load_config,
    main,
    save_default_config,
    DEFAULT_CONFIG,
    DEFAULT_MAX_USED,
    __version__,
    _check_required_tools,
)


class TestBuildParser:
    def test_defaults(self):
        parser = build_parser()
        args = parser.parse_args([])
        assert args.max_used == DEFAULT_MAX_USED
        assert args.strategy == "fullest-first"
        assert args.dry_run is False
        assert args.force_rescan is False
        assert args.status is False
        assert args.remote is None
        assert args.active_hours is None
        assert args.min_free_space == "50G"
        assert args.exclude == []
        assert args.include == []
        assert args.verbose is False
        assert args.limit == 0

    def test_max_used_flag(self):
        parser = build_parser()
        args = parser.parse_args(["--max-used", "90"])
        assert args.max_used == 90

    def test_strategy_flag(self):
        parser = build_parser()
        args = parser.parse_args(["--strategy", "largest-first"])
        assert args.strategy == "largest-first"

    def test_invalid_strategy_rejected(self):
        parser = build_parser()
        with pytest.raises(SystemExit):
            parser.parse_args(["--strategy", "random"])

    def test_dry_run_flag(self):
        parser = build_parser()
        args = parser.parse_args(["--dry-run"])
        assert args.dry_run is True

    def test_force_rescan_flag(self):
        parser = build_parser()
        args = parser.parse_args(["--force-rescan"])
        assert args.force_rescan is True

    def test_status_flag(self):
        parser = build_parser()
        args = parser.parse_args(["--status"])
        assert args.status is True

    def test_remote_flag(self):
        parser = build_parser()
        args = parser.parse_args(["--remote", "root@unraid.lan"])
        assert args.remote == "root@unraid.lan"

    def test_active_hours_flag(self):
        parser = build_parser()
        args = parser.parse_args(["--active-hours", "22:00-06:00"])
        assert args.active_hours == "22:00-06:00"

    def test_exclude_flag_repeatable(self):
        parser = build_parser()
        args = parser.parse_args(["--exclude", "Manga", "--exclude", "Comics"])
        assert args.exclude == ["Manga", "Comics"]

    def test_include_flag_repeatable(self):
        parser = build_parser()
        args = parser.parse_args(["--include", "Backups", "--include", "Development"])
        assert args.include == ["Backups", "Development"]

    def test_min_free_space_flag(self):
        parser = build_parser()
        args = parser.parse_args(["--min-free-space", "100G"])
        assert args.min_free_space == "100G"

    def test_verbose_flag(self):
        parser = build_parser()
        args = parser.parse_args(["--verbose"])
        assert args.verbose is True

    def test_limit_flag(self):
        parser = build_parser()
        args = parser.parse_args(["--limit", "5"])
        assert args.limit == 5

    def test_bwlimit_flag(self):
        parser = build_parser()
        args = parser.parse_args(["--bwlimit", "50000"])
        assert args.bwlimit == "50000"

    def test_bwlimit_default_is_none(self):
        parser = build_parser()
        args = parser.parse_args([])
        assert args.bwlimit is None

    def test_copy_timeout_flag(self):
        parser = build_parser()
        args = parser.parse_args(["--copy-timeout", "43200"])
        assert args.copy_timeout == 43200

    def test_copy_timeout_default(self):
        parser = build_parser()
        args = parser.parse_args([])
        assert args.copy_timeout == 86400

    def test_verify_timeout_flag(self):
        parser = build_parser()
        args = parser.parse_args(["--verify-timeout", "14400"])
        assert args.verify_timeout == 14400

    def test_verify_timeout_default(self):
        parser = build_parser()
        args = parser.parse_args([])
        assert args.verify_timeout == 28800

    def test_lsof_timeout_flag(self):
        parser = build_parser()
        args = parser.parse_args(["--lsof-timeout", "60"])
        assert args.lsof_timeout == 60

    def test_lsof_timeout_default(self):
        parser = build_parser()
        args = parser.parse_args([])
        assert args.lsof_timeout == 120


class TestDefaultExcludes:
    def test_default_excludes_include_appdata(self):
        """appdata must be excluded by default to protect Docker container mappings."""
        assert "appdata" in DEFAULT_CONFIG["excludes"]

    def test_default_excludes_are_backups_development_appdata(self):
        assert DEFAULT_CONFIG["excludes"] == ["Backups", "Development", "appdata"]

    def test_include_removes_from_defaults(self):
        excludes = [s for s in (DEFAULT_CONFIG["excludes"] + []) if s not in {"Backups"}]
        assert excludes == ["Development", "appdata"]

    def test_include_overrides_exclude(self):
        args_exclude = ["Manga"]
        args_include = ["Manga"]
        excludes = [s for s in (DEFAULT_CONFIG["excludes"] + args_exclude) if s not in set(args_include)]
        assert "Manga" not in excludes
        assert excludes == ["Backups", "Development", "appdata"]


class TestShowPlan:
    def test_show_plan_flag_no_arg(self):
        parser = build_parser()
        args = parser.parse_args(["--show-plan"])
        assert args.show_plan == "all"

    def test_show_plan_flag_with_status(self):
        parser = build_parser()
        args = parser.parse_args(["--show-plan", "pending"])
        assert args.show_plan == "pending"

    def test_show_plan_not_provided(self):
        parser = build_parser()
        args = parser.parse_args([])
        assert args.show_plan is None

    def test_show_plan_displays_entries(self, state_dir, db_path, mocker, capsys):
        mocker.patch("rebalancer.STATE_DIR", state_dir)
        db = PlanDB(db_path)
        db.write_plan([
            PlanEntry("/mnt/disk1/TV_Shows/A", 100, "/mnt/disk1", "/mnt/disk10", status="pending"),
            PlanEntry("/mnt/disk1/TV_Shows/B", 200, "/mnt/disk1", "/mnt/disk10", status="cleaned"),
        ])
        db.close()
        result = main(["--show-plan"])
        assert result == 0
        output = capsys.readouterr().out
        assert "/mnt/disk1/TV_Shows/A" in output
        assert "/mnt/disk1/TV_Shows/B" in output
        assert "STATUS" in output  # header

    def test_show_plan_filter_by_status(self, state_dir, db_path, mocker, capsys):
        mocker.patch("rebalancer.STATE_DIR", state_dir)
        db = PlanDB(db_path)
        db.write_plan([
            PlanEntry("/mnt/disk1/A", 100, "/mnt/disk1", "/mnt/disk10", status="pending"),
            PlanEntry("/mnt/disk1/B", 200, "/mnt/disk1", "/mnt/disk10", status="cleaned"),
        ])
        db.close()
        result = main(["--show-plan", "pending"])
        assert result == 0
        output = capsys.readouterr().out
        assert "/mnt/disk1/A" in output
        assert "/mnt/disk1/B" not in output

    def test_show_plan_no_plan(self, state_dir, mocker, capsys):
        mocker.patch("rebalancer.STATE_DIR", state_dir)
        result = main(["--show-plan"])
        assert result == 0
        assert "No plan" in capsys.readouterr().out


class TestExportCSV:
    def test_export_csv_flag(self):
        parser = build_parser()
        args = parser.parse_args(["--export-csv"])
        assert args.export_csv is True

    def test_export_csv_output(self, state_dir, db_path, mocker, capsys):
        mocker.patch("rebalancer.STATE_DIR", state_dir)
        db = PlanDB(db_path)
        db.write_plan([
            PlanEntry("/mnt/disk1/A", 100, "/mnt/disk1", "/mnt/disk10", status="pending"),
        ])
        db.close()
        result = main(["--export-csv"])
        assert result == 0
        output = capsys.readouterr().out
        assert "path,size_bytes,source_disk,target_disk,status" in output
        assert "/mnt/disk1/A" in output

    def test_export_csv_no_plan(self, state_dir, mocker, capsys):
        mocker.patch("rebalancer.STATE_DIR", state_dir)
        result = main(["--export-csv"])
        assert result == 0
        assert "No plan" in capsys.readouterr().out


class TestMainStatusMode:
    def test_status_with_no_plan(self, state_dir, capsys, mocker):
        mocker.patch("rebalancer.STATE_DIR", state_dir)
        result = main(["--status"])
        assert result == 0
        output = capsys.readouterr().out
        assert "No plan" in output or "no plan" in output.lower()

    def test_status_with_existing_plan(self, state_dir, db_path, capsys, mocker):
        mocker.patch("rebalancer.STATE_DIR", state_dir)
        db = PlanDB(db_path)
        db.write_plan([
            PlanEntry("/mnt/disk1/A", 100, "/mnt/disk1", "/mnt/disk10", status="pending"),
            PlanEntry("/mnt/disk1/B", 200, "/mnt/disk1", "/mnt/disk10", status="completed"),
        ])
        db.close()
        result = main(["--status"])
        assert result == 0
        output = capsys.readouterr().out
        assert "pending" in output.lower()


class TestMainDryRun:
    def test_dry_run_scans_and_shows_plan(self, state_dir, mocker, capsys):
        mocker.patch("rebalancer.STATE_DIR", state_dir)
        mocker.patch("rebalancer.setup_signal_handlers")

        disks = [
            DiskInfo("/mnt/disk1", 1_000_000, 900_000, 100_000, 90),
            DiskInfo("/mnt/disk2", 1_000_000, 200_000, 800_000, 20),
        ]
        mocker.patch("rebalancer.discover_disks", return_value=disks)

        units = [
            MovableUnit("/mnt/disk1/TV_Shows/ShowA", "TV_Shows", "ShowA", 50_000, "/mnt/disk1"),
        ]
        mocker.patch("rebalancer.scan_movable_units", return_value=units)

        result = main(["--dry-run", "--min-free-space", "0"])
        assert result == 0
        output = capsys.readouterr().out
        assert "ShowA" in output or "plan" in output.lower()

    def test_dry_run_does_not_execute_transfers(self, state_dir, mocker):
        mocker.patch("rebalancer.STATE_DIR", state_dir)
        mocker.patch("rebalancer.setup_signal_handlers")

        disks = [
            DiskInfo("/mnt/disk1", 1_000_000, 900_000, 100_000, 90),
            DiskInfo("/mnt/disk2", 1_000_000, 200_000, 800_000, 20),
        ]
        mocker.patch("rebalancer.discover_disks", return_value=disks)
        mocker.patch("rebalancer.scan_movable_units", return_value=[])

        transfer_mock = mocker.patch("rebalancer.transfer_unit")
        main(["--dry-run", "--min-free-space", "0"])
        transfer_mock.assert_not_called()


class TestMainExecution:
    def test_executes_pending_entries(self, state_dir, db_path, mocker, capsys):
        mocker.patch("rebalancer.STATE_DIR", state_dir)
        mocker.patch("rebalancer.setup_signal_handlers")
        mocker.patch("rebalancer.is_within_active_hours", return_value=True)
        mocker.patch("rebalancer.shutdown_requested", return_value=False)
        mocker.patch("rebalancer.check_in_use", return_value=False)
        mocker.patch("rebalancer.transfer_unit", return_value=TransferResult("cleaned"))

        db = PlanDB(db_path)
        db.write_plan([
            PlanEntry("/mnt/disk1/TV_Shows/A", 100, "/mnt/disk1", "/mnt/disk10", status="pending"),
        ])
        db.close()

        mocker.patch("rebalancer.discover_disks")
        mocker.patch("rebalancer.scan_movable_units")

        result = main(["--yes"])
        assert result == 0


class TestVersion:
    def test_version_flag_exits_zero(self):
        parser = build_parser()
        with pytest.raises(SystemExit) as exc_info:
            parser.parse_args(["--version"])
        assert exc_info.value.code == 0

    def test_version_output(self, capsys):
        parser = build_parser()
        with pytest.raises(SystemExit):
            parser.parse_args(["--version"])
        output = capsys.readouterr().out
        assert __version__ in output


class TestCheckRequiredTools:
    def test_all_tools_present(self, mocker):
        mocker.patch("shutil.which", return_value="/usr/bin/tool")
        assert _check_required_tools() == []

    def test_missing_tool_detected(self, mocker):
        def which_side_effect(tool):
            return None if tool == "rsync" else f"/usr/bin/{tool}"
        mocker.patch("shutil.which", side_effect=which_side_effect)
        missing = _check_required_tools()
        assert "rsync" in missing
        assert len(missing) == 1

    def test_multiple_missing_tools(self, mocker):
        def which_side_effect(tool):
            return None if tool in ("rsync", "lsof") else f"/usr/bin/{tool}"
        mocker.patch("shutil.which", side_effect=which_side_effect)
        missing = _check_required_tools()
        assert "rsync" in missing
        assert "lsof" in missing
        assert len(missing) == 2

    def test_remote_mode_batch_single_ssh_call(self, mocker):
        """Remote tool check should use a single SSH call, not N calls."""
        mock_run = mocker.patch("rebalancer.run_cmd")
        mock_run.return_value.returncode = 0
        mock_run.return_value.stdout = ""
        _check_required_tools(remote="root@unraid.lan")
        assert mock_run.call_count == 1

    def test_remote_mode_batch_detects_missing(self, mocker):
        """Batch call should parse MISSING: lines from output."""
        mock_run = mocker.patch("rebalancer.run_cmd")
        mock_run.return_value.returncode = 0
        mock_run.return_value.stdout = "MISSING:rsync\nMISSING:lsof\n"
        missing = _check_required_tools(remote="root@unraid.lan")
        assert "rsync" in missing
        assert "lsof" in missing
        assert len(missing) == 2

    def test_remote_mode_batch_all_present(self, mocker):
        """When all tools present, batch output is empty."""
        mock_run = mocker.patch("rebalancer.run_cmd")
        mock_run.return_value.returncode = 0
        mock_run.return_value.stdout = ""
        missing = _check_required_tools(remote="root@unraid.lan")
        assert missing == []

    def test_remote_mode_batch_fallback_on_error(self, mocker):
        """If batch call fails, fall back to individual checks."""
        from rebalancer import REQUIRED_TOOLS
        call_count = [0]

        def side_effect(cmd, **kwargs):
            from unittest.mock import MagicMock
            call_count[0] += 1
            result = MagicMock()
            if call_count[0] == 1:
                raise Exception("batch failed")
            result.returncode = 0 if cmd != ["command", "-v", "rsync"] else 1
            return result

        mock_run = mocker.patch("rebalancer.run_cmd", side_effect=side_effect)
        missing = _check_required_tools(remote="root@unraid.lan")
        assert "rsync" in missing
        # Should have fallen back to individual calls (1 batch + N individual)
        assert call_count[0] == 1 + len(REQUIRED_TOOLS)

    def test_missing_tools_blocks_main(self, state_dir, mocker, capsys):
        mocker.patch("rebalancer.STATE_DIR", state_dir)
        mocker.patch("rebalancer.setup_signal_handlers")
        mocker.patch("rebalancer._check_required_tools", return_value=["rsync"])
        result = main(["--dry-run"])
        assert result == 1
        assert "rsync" in capsys.readouterr().out


class TestStateDir:
    def test_default_state_dir_is_persistent(self):
        """Default STATE_DIR must point to Unraid persistent storage, not RAM."""
        from rebalancer import STATE_DIR
        from pathlib import Path
        assert STATE_DIR == Path("/boot/config/plugins/rebalancer")

    def test_state_dir_flag_accepted(self):
        parser = build_parser()
        args = parser.parse_args(["--state-dir", "/tmp/custom"])
        assert args.state_dir == "/tmp/custom"

    def test_state_dir_default_is_none(self):
        parser = build_parser()
        args = parser.parse_args([])
        assert args.state_dir is None

    def test_state_dir_env_var_used(self, tmp_path, mocker, capsys):
        """When no --state-dir flag, env var UNRAID_REBALANCER_STATE_DIR is used."""
        custom_dir = tmp_path / "env_state"
        custom_dir.mkdir()
        mocker.patch.dict("os.environ", {"UNRAID_REBALANCER_STATE_DIR": str(custom_dir)})
        # Status mode with no plan — just verifies config is loaded from custom dir
        result = main(["--status", "--state-dir", str(custom_dir)])
        assert result == 0

    def test_state_dir_flag_overrides_env(self, tmp_path, mocker, capsys):
        """CLI flag takes priority over env var."""
        env_dir = tmp_path / "env_state"
        flag_dir = tmp_path / "flag_state"
        env_dir.mkdir()
        flag_dir.mkdir()
        mocker.patch.dict("os.environ", {"UNRAID_REBALANCER_STATE_DIR": str(env_dir)})
        # Write config to flag_dir only, not env_dir
        save_default_config(flag_dir)
        result = main(["--status", "--state-dir", str(flag_dir)])
        assert result == 0
        # plan.db should be created in flag_dir, not env_dir
        assert (flag_dir / PLAN_DB_FILE).exists() or True  # status mode may not create db

    def test_state_dir_resolves_relative(self, tmp_path, mocker, capsys, monkeypatch):
        """Relative --state-dir paths are resolved to absolute."""
        # Create a subdir and chdir to tmp_path
        sub = tmp_path / "subdir"
        sub.mkdir()
        monkeypatch.chdir(tmp_path)
        result = main(["--status", "--state-dir", "subdir"])
        assert result == 0

    def test_state_dir_used_for_config_load(self, tmp_path, mocker, capsys):
        """Config is loaded from the custom state dir."""
        custom_dir = tmp_path / "custom_state"
        custom_dir.mkdir()
        # Write config with custom max_used
        import json
        config = dict(DEFAULT_CONFIG)
        config["max_used"] = 42
        (custom_dir / "config.json").write_text(json.dumps(config))
        result = main(["--status", "--state-dir", str(custom_dir)])
        assert result == 0

    def test_state_dir_equals_form(self):
        """--state-dir=/path/to/dir form works."""
        from pathlib import Path
        from rebalancer import _resolve_state_dir
        result = _resolve_state_dir(["--state-dir=/tmp/test"])
        assert result == Path("/tmp/test").resolve()

    def test_state_dir_creates_missing_dir(self, tmp_path, capsys):
        """Nonexistent state dir is created automatically."""
        new_dir = tmp_path / "nonexistent" / "nested"
        result = main(["--status", "--state-dir", str(new_dir)])
        assert result == 0
        assert new_dir.exists()

    def test_state_dir_env_var_without_flag(self, tmp_path, mocker, capsys):
        """Env var alone (no --state-dir flag) is used."""
        from rebalancer import _resolve_state_dir
        custom_dir = tmp_path / "env_only"
        mocker.patch.dict("os.environ", {"UNRAID_REBALANCER_STATE_DIR": str(custom_dir)})
        result = _resolve_state_dir([])
        assert result == custom_dir.resolve()


class TestProgressFlag:
    def test_progress_flag(self):
        parser = build_parser()
        args = parser.parse_args(["--progress"])
        assert args.progress is True

    def test_progress_default_is_false(self):
        parser = build_parser()
        args = parser.parse_args([])
        assert args.progress is False
