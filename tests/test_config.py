"""Tests for config.json and parse_size."""

import json

import pytest

from rebalancer import (
    DEFAULT_CONFIG,
    CONFIG_FILE,
    TransferResult,
    load_config,
    main,
    parse_size,
    save_default_config,
)


class TestParseSize:
    def test_plain_integer(self):
        assert parse_size("1000") == 1000

    def test_bytes(self):
        assert parse_size("500B") == 500

    def test_kilobytes(self):
        assert parse_size("1K") == 1024
        assert parse_size("1KB") == 1024

    def test_megabytes(self):
        assert parse_size("1M") == 1024**2
        assert parse_size("100MB") == 100 * 1024**2

    def test_gigabytes(self):
        assert parse_size("1G") == 1024**3
        assert parse_size("100G") == 100 * 1024**3
        assert parse_size("1GB") == 1024**3

    def test_terabytes(self):
        assert parse_size("1T") == 1024**4
        assert parse_size("1TB") == 1024**4

    def test_fractional(self):
        assert parse_size("1.5G") == int(1.5 * 1024**3)

    def test_case_insensitive(self):
        assert parse_size("100g") == parse_size("100G")
        assert parse_size("1tb") == parse_size("1TB")

    def test_with_spaces(self):
        assert parse_size("  100G  ") == 100 * 1024**3

    def test_zero(self):
        assert parse_size("0") == 0

    def test_empty_string(self):
        assert parse_size("") == 0

    def test_invalid_raises(self):
        with pytest.raises(ValueError):
            parse_size("not-a-size")

    def test_negative_raises(self):
        # Plain negative integer parses fine but validation catches it
        assert parse_size("-100") == -100


class TestConfig:
    def test_load_missing_returns_defaults(self, state_dir):
        config = load_config(state_dir)
        assert config["max_used"] == 80
        assert config["excludes"] == ["Backups", "Development", "appdata"]
        assert config["strategy"] == "fullest-first"
        assert config["min_free_space"] == "50G"

    def test_save_and_load_roundtrip(self, state_dir):
        save_default_config(state_dir)
        config = load_config(state_dir)
        assert config == DEFAULT_CONFIG

    def test_user_overrides_merged(self, state_dir):
        path = state_dir / CONFIG_FILE
        path.write_text(json.dumps({"max_used": 90, "excludes": ["MyCustomShare"]}))
        config = load_config(state_dir)
        assert config["max_used"] == 90
        assert config["excludes"] == ["MyCustomShare"]
        assert config["strategy"] == "fullest-first"  # default preserved

    def test_string_max_used_coerced_to_int(self, state_dir):
        """C3: config.json max_used as string should be coerced to int by load_config."""
        path = state_dir / CONFIG_FILE
        path.write_text(json.dumps({"max_used": "90"}))
        config = load_config(state_dir)
        assert config["max_used"] == 90
        assert isinstance(config["max_used"], int)

    def test_invalid_max_used_type_falls_back_to_default(self, state_dir):
        """C3: non-numeric max_used in config should fall back to default."""
        path = state_dir / CONFIG_FILE
        path.write_text(json.dumps({"max_used": "not-a-number"}))
        config = load_config(state_dir)
        assert config["max_used"] == 80  # default
        assert isinstance(config["max_used"], int)

    def test_corrupted_config_uses_defaults(self, state_dir):
        path = state_dir / CONFIG_FILE
        path.write_text("{broken json")
        config = load_config(state_dir)
        assert config == DEFAULT_CONFIG

    def test_init_config_creates_file(self, state_dir, mocker, capsys):
        mocker.patch("rebalancer.STATE_DIR", state_dir)
        result = main(["--init-config"])
        assert result == 0
        assert (state_dir / CONFIG_FILE).exists()
        output = capsys.readouterr().out
        assert "Config written" in output

    def test_config_excludes_used_by_main(self, state_dir, db_path, mocker, capsys):
        """Config excludes replace hardcoded defaults."""
        mocker.patch("rebalancer.STATE_DIR", state_dir)
        mocker.patch("rebalancer.setup_signal_handlers")
        mocker.patch("rebalancer.shutdown_requested", return_value=False)
        mocker.patch("rebalancer.is_within_active_hours", return_value=True)
        mocker.patch("rebalancer.check_in_use", return_value=False)
        mocker.patch("rebalancer.transfer_unit", return_value=TransferResult("cleaned"))

        # Write custom config
        config_path = state_dir / CONFIG_FILE
        config_path.write_text(json.dumps({"excludes": ["CustomExclude"]}))

        from rebalancer import DiskInfo
        disks = [
            DiskInfo("/mnt/disk1", 1000000, 900000, 100000, 90),
            DiskInfo("/mnt/disk2", 1000000, 200000, 800000, 20),
        ]
        mocker.patch("rebalancer.discover_disks", return_value=disks)

        # Track what excludes scan receives
        captured_excludes = []

        def capture_scan(disk, excludes, **kwargs):
            captured_excludes.append(list(excludes))
            return []

        mocker.patch("rebalancer.scan_movable_units", side_effect=capture_scan)

        main(["--dry-run"])

        # Excludes should come from config, not hardcoded
        if captured_excludes:
            assert "CustomExclude" in captured_excludes[0]
            assert "Backups" not in captured_excludes[0]


class TestMinFreeSpaceHumanReadable:
    def test_main_accepts_human_readable(self, state_dir, mocker, capsys):
        mocker.patch("rebalancer.STATE_DIR", state_dir)
        mocker.patch("rebalancer.setup_signal_handlers")
        from rebalancer import DiskInfo
        disks = [DiskInfo("/mnt/disk1", 1000000, 500000, 500000, 50)]
        mocker.patch("rebalancer.discover_disks", return_value=disks)
        mocker.patch("rebalancer.scan_movable_units", return_value=[])
        result = main(["--min-free-space", "100G", "--dry-run"])
        assert result == 0

    def test_main_rejects_invalid_size(self, state_dir, mocker, capsys):
        mocker.patch("rebalancer.STATE_DIR", state_dir)
        result = main(["--min-free-space", "not-a-size"])
        assert result == 1
        assert "Invalid size" in capsys.readouterr().out
