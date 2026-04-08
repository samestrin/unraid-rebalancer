"""Tests for disk discovery — Phase 2 RED."""

from unittest.mock import MagicMock

import pytest

from rebalancer import (
    DiskInfo,
    MovableUnit,
    discover_disks,
    is_year_folder,
    parse_df_output,
    parse_du_output,
    parse_ls_output,
    run_cmd,
    scan_movable_units,
    DEFAULT_CONFIG,
)


# --- run_cmd ---

class TestRunCmd:
    def _mock_popen(self, mocker, stdout="", stderr="", returncode=0):
        """Create a mock Popen that returns the given stdout/stderr/returncode."""
        mock_proc = MagicMock()
        mock_proc.communicate.return_value = (stdout, stderr)
        mock_proc.returncode = returncode
        mock = mocker.patch("rebalancer.subprocess.Popen", return_value=mock_proc)
        return mock, mock_proc

    def test_run_cmd_returns_stdout(self, mocker):
        mock, _ = self._mock_popen(mocker, stdout="hello\n")
        result = run_cmd(["echo", "hello"])
        assert result.stdout == "hello\n"

    def test_run_cmd_passes_args(self, mocker):
        mock, _ = self._mock_popen(mocker)
        run_cmd(["df", "-B1", "/mnt/disk*"])
        mock.assert_called_once()
        args = mock.call_args[0][0]
        assert args == ["df", "-B1", "/mnt/disk*"]

    def test_run_cmd_kills_on_timeout(self, mocker):
        """M6: Timed-out child processes must be killed, not left orphaned."""
        import subprocess
        mock_proc = MagicMock()
        mock_proc.communicate.side_effect = subprocess.TimeoutExpired(cmd="test", timeout=1)
        mocker.patch("rebalancer.subprocess.Popen", return_value=mock_proc)
        with pytest.raises(subprocess.TimeoutExpired):
            run_cmd(["sleep", "999"], timeout=1)
        mock_proc.kill.assert_called_once()
        mock_proc.wait.assert_called_once()

    def test_passthrough_returns_empty_stdout_but_captures_stderr(self, mocker):
        """run_cmd with passthrough=True returns empty stdout but captures stderr."""
        mock_proc = MagicMock()
        mock_proc.communicate.return_value = (None, "rsync warning: something")
        mock_proc.returncode = 0
        mocker.patch("rebalancer.subprocess.Popen", return_value=mock_proc)
        result = run_cmd(["echo", "test"], passthrough=True)
        assert result.stdout == ""
        assert result.stderr == "rsync warning: something"
        assert result.returncode == 0

    def test_passthrough_pipes_stderr_only(self, mocker):
        """Passthrough mode pipes stderr (for diagnostics) but not stdout (for terminal)."""
        import subprocess
        mock_proc = MagicMock()
        mock_proc.communicate.return_value = (None, "")
        mock_proc.returncode = 0
        mock = mocker.patch("rebalancer.subprocess.Popen", return_value=mock_proc)
        run_cmd(["echo", "test"], passthrough=True)
        call_kwargs = mock.call_args[1]
        assert "stdout" not in call_kwargs or call_kwargs["stdout"] is None
        assert call_kwargs.get("stderr") == subprocess.PIPE

    def test_passthrough_kills_on_timeout(self, mocker):
        """Passthrough mode still kills on timeout."""
        import subprocess
        mock_proc = MagicMock()
        mock_proc.communicate.side_effect = subprocess.TimeoutExpired(cmd="test", timeout=1)
        mocker.patch("rebalancer.subprocess.Popen", return_value=mock_proc)
        with pytest.raises(subprocess.TimeoutExpired):
            run_cmd(["sleep", "999"], timeout=1, passthrough=True)
        mock_proc.kill.assert_called_once()
        mock_proc.wait.assert_called_once()


# --- parse_df_output ---

class TestParseDfOutput:
    def test_parse_standard_output(self, sample_df_output):
        disks = parse_df_output(sample_df_output)
        assert len(disks) == 4
        assert disks[0].path == "/mnt/disk1"
        assert disks[0].used_pct == 96

    def test_parse_sorts_by_path_numerically(self, sample_df_output):
        """Disks should be sorted numerically: disk1, disk2, disk10, disk11."""
        disks = parse_df_output(sample_df_output)
        paths = [d.path for d in disks]
        assert paths == ["/mnt/disk1", "/mnt/disk2", "/mnt/disk10", "/mnt/disk11"]

    def test_parse_calculates_bytes_from_1k_blocks(self):
        output = (
            "Filesystem      1K-blocks       Used  Available Use% Mounted on\n"
            "/dev/md1p1    1000000  800000  200000  80% /mnt/disk1\n"
        )
        disks = parse_df_output(output)
        assert disks[0].total_bytes == 1_000_000 * 1024
        assert disks[0].used_bytes == 800_000 * 1024
        assert disks[0].free_bytes == 200_000 * 1024

    def test_parse_empty_output(self):
        output = "Filesystem      1K-blocks       Used  Available Use% Mounted on\n"
        disks = parse_df_output(output)
        assert disks == []

    def test_parse_filters_non_disk_mounts(self):
        """Only /mnt/disk[N] paths should be included."""
        output = (
            "Filesystem      1K-blocks       Used  Available Use% Mounted on\n"
            "/dev/md1p1    1000000  800000  200000  80% /mnt/disk1\n"
            "tmpfs          1000000  100000  900000  10% /tmp\n"
            "/dev/sda1      1000000  500000  500000  50% /mnt/cache\n"
        )
        disks = parse_df_output(output)
        assert len(disks) == 1
        assert disks[0].path == "/mnt/disk1"

    def test_parse_handles_disk_numbers_above_9(self):
        output = (
            "Filesystem      1K-blocks       Used  Available Use% Mounted on\n"
            "/dev/md10p1   1000000  800000  200000  80% /mnt/disk10\n"
            "/dev/md11p1   1000000  400000  600000  40% /mnt/disk11\n"
        )
        disks = parse_df_output(output)
        assert len(disks) == 2
        assert disks[0].path == "/mnt/disk10"
        assert disks[1].path == "/mnt/disk11"

    def test_parse_numeric_sort_order(self):
        """disk2 should sort before disk10 (numeric, not lexicographic)."""
        output = (
            "Filesystem      1K-blocks       Used  Available Use% Mounted on\n"
            "/dev/md10p1   1000000  800000  200000  80% /mnt/disk10\n"
            "/dev/md2p1    1000000  800000  200000  80% /mnt/disk2\n"
            "/dev/md1p1    1000000  800000  200000  80% /mnt/disk1\n"
        )
        disks = parse_df_output(output)
        paths = [d.path for d in disks]
        assert paths == ["/mnt/disk1", "/mnt/disk2", "/mnt/disk10"]

    def test_parse_malformed_line_skipped(self):
        """H1: malformed df lines (non-numeric fields) should be skipped, not crash."""
        output = (
            "Filesystem      1K-blocks       Used  Available Use% Mounted on\n"
            "/dev/md1p1    1000000  800000  200000  80% /mnt/disk1\n"
            "/dev/md2p1    -  -  -  -% /mnt/disk2\n"
            "/dev/md3p1    1000000  400000  600000  40% /mnt/disk3\n"
        )
        disks = parse_df_output(output)
        assert len(disks) == 2  # disk2 skipped, disk1 and disk3 ok
        assert disks[0].path == "/mnt/disk1"
        assert disks[1].path == "/mnt/disk3"

    def test_parse_percentage_strip(self):
        """The Use% column has a % suffix that must be stripped."""
        output = (
            "Filesystem      1K-blocks       Used  Available Use% Mounted on\n"
            "/dev/md1p1    1000000  999000  1000  100% /mnt/disk1\n"
        )
        disks = parse_df_output(output)
        assert disks[0].used_pct == 100

    def test_parse_real_unraid_df_output(self):
        """Fixture from actual Unraid server (Slackware 15.0+, 11 XFS disks).
        Verified via SSH to root@unraid.lan on 2026-04-07.
        Must parse only /mnt/diskN, excluding cache/backup/user/user0."""
        real_output = (
            "Filesystem     1024-blocks        Used   Available Capacity Mounted on\n"
            "/dev/md1p1     15623792588 14995396292   628396296      96% /mnt/disk1\n"
            "/dev/md10p1    15623792588  7890384356  7733408232      51% /mnt/disk10\n"
            "/dev/md11p1    15623792588  4609540836 11014251752      30% /mnt/disk11\n"
            "/dev/md2p1     15623587800 14551145436  1072442364      94% /mnt/disk2\n"
            "/dev/md3p1     15623587800 14853371648   770216152      96% /mnt/disk3\n"
            "/dev/md4p1     15623792588 15623776740       15848     100% /mnt/disk4\n"
            "/dev/md5p1     15623792588 15100358164   523434424      97% /mnt/disk5\n"
            "/dev/md6p1     15623792588 15623791544        1044     100% /mnt/disk6\n"
            "/dev/md7p1     15623792588 15228325744   395466844      98% /mnt/disk7\n"
            "/dev/md8p1     15623792588 14983087644   640704944      96% /mnt/disk8\n"
            "/dev/md9p1     15623792588 14006043096  1617749492      90% /mnt/disk9\n"
            # These must be excluded by _DISK_PATH_RE
            "shfs           15623792588 10000000000  5623792588      64% /mnt/user\n"
            "shfs           15623792588 10000000000  5623792588      64% /mnt/user0\n"
        )
        disks = parse_df_output(real_output)
        assert len(disks) == 11
        paths = [d.path for d in disks]
        # Numeric sort: disk1, disk2, ..., disk10, disk11
        assert paths == [f"/mnt/disk{i}" for i in range(1, 12)]
        # Excluded mounts
        assert not any("user" in d.path for d in disks)
        assert not any("cache" in d.path for d in disks)
        # Spot-check values
        disk4 = next(d for d in disks if d.path == "/mnt/disk4")
        assert disk4.used_pct == 100
        assert disk4.free_bytes == 15848 * 1024
        disk11 = next(d for d in disks if d.path == "/mnt/disk11")
        assert disk11.used_pct == 30


# --- is_year_folder ---

class TestIsYearFolder:
    def test_valid_years(self):
        for y in ("1900", "1999", "2000", "2024", "2025", "2099"):
            assert is_year_folder(y), f"{y} should be a year folder"

    def test_invalid_non_year_numbers(self):
        for name in ("1080", "1899", "2100", "3000", "0000"):
            assert not is_year_folder(name), f"{name} should not be a year folder"

    def test_invalid_non_numeric(self):
        for name in ("Extras", "Featurettes", "2024a", "abcd", ""):
            assert not is_year_folder(name), f"'{name}' should not be a year folder"


# --- parse_ls_output ---

class TestParseLsOutput:
    def test_parse_basic_listing(self):
        output = "Breaking Bad (2008)\nThe Wire (2002)\nLost (2004)\n"
        names = parse_ls_output(output)
        assert names == ["Breaking Bad (2008)", "The Wire (2002)", "Lost (2004)"]

    def test_parse_empty_output(self):
        assert parse_ls_output("") == []
        assert parse_ls_output("\n") == []

    def test_parse_strips_whitespace(self):
        output = "  show1  \nshow2\n"
        names = parse_ls_output(output)
        assert names == ["show1", "show2"]


# --- parse_du_output ---

class TestParseDuOutput:
    def test_parse_single_entry(self):
        output = "500000000\t/mnt/disk1/TV_Shows/Lost (2004)\n"
        size = parse_du_output(output)
        assert size == 500_000_000

    def test_parse_empty_returns_zero(self):
        assert parse_du_output("") == 0
        assert parse_du_output("\n") == 0


# --- discover_disks ---

class TestDiscoverDisks:
    def test_discover_remote_returns_disk_infos(self, mocker, sample_df_output):
        """Remote mode uses run_cmd with glob — test the remote path."""
        mock = mocker.patch("rebalancer.run_cmd")
        mock.return_value.stdout = sample_df_output
        mock.return_value.returncode = 0
        disks = discover_disks(remote="root@unraid.lan")
        assert len(disks) == 4
        assert all(isinstance(d, DiskInfo) for d in disks)

    def test_discover_local_uses_glob(self, mocker, sample_df_output):
        """Local mode uses glob.glob to expand /mnt/disk*."""
        mocker.patch("glob.glob", return_value=["/mnt/disk1", "/mnt/disk2"])
        mock = mocker.patch("rebalancer.run_cmd")
        mock.return_value.stdout = sample_df_output
        mock.return_value.returncode = 0
        disks = discover_disks()
        assert len(disks) == 4

    def test_discover_local_no_disks(self, mocker):
        """When glob returns empty, should return empty list without calling df."""
        mocker.patch("glob.glob", return_value=[])
        mock = mocker.patch("rebalancer.run_cmd")
        disks = discover_disks()
        assert disks == []
        mock.assert_not_called()

    def test_discover_passes_remote(self, mocker, sample_df_output):
        mock = mocker.patch("rebalancer.run_cmd")
        mock.return_value.stdout = sample_df_output
        mock.return_value.returncode = 0
        discover_disks(remote="root@unraid.lan")
        call_kwargs = mock.call_args
        assert call_kwargs[1].get("remote") == "root@unraid.lan" or "root@unraid.lan" in str(call_kwargs)


# --- scan_movable_units ---

class TestScanMovableUnits:
    @staticmethod
    def _du_batch_output(cmd):
        """Generate realistic du output: one line per path argument after 'du -sb'."""
        # cmd is a list like ["du", "-sb", "/path/a", "/path/b"]
        paths = cmd[2:]  # everything after the flags
        return "\n".join(f"200000000000\t{p}" for p in paths) + "\n"

    def test_scan_tv_shows_returns_show_folders(self, mocker):
        """TV_Shows children are individual show folders = movable units."""
        disk = DiskInfo("/mnt/disk1", 16_000_000_000_000, 14_000_000_000_000, 2_000_000_000_000, 87)

        def mock_run(cmd, **kwargs):
            result = type("R", (), {"stdout": "", "returncode": 0})()
            if cmd[0] == "ls" and cmd[-1] == "/mnt/disk1/":
                result.stdout = "TV_Shows\nMovies\n"
            elif cmd[0] == "ls" and "TV_Shows" in cmd[-1]:
                result.stdout = "Breaking Bad (2008)\nLost (2004)\n"
            elif cmd[0] == "ls" and "Movies" in cmd[-1]:
                result.stdout = "2024\n2025\n"
            elif cmd[0] == "du":
                result.stdout = TestScanMovableUnits._du_batch_output(cmd)
            return result

        mocker.patch("rebalancer.run_cmd", side_effect=mock_run)
        units = scan_movable_units(disk, DEFAULT_CONFIG["excludes"])
        tv_units = [u for u in units if u.share == "TV_Shows"]
        assert len(tv_units) == 2
        assert tv_units[0].name == "Breaking Bad (2008)"
        assert tv_units[0].disk == "/mnt/disk1"

    def test_scan_movies_returns_year_folders(self, mocker):
        """Movies children should be year folders as movable units."""
        disk = DiskInfo("/mnt/disk1", 16_000_000_000_000, 14_000_000_000_000, 2_000_000_000_000, 87)

        def mock_run(cmd, **kwargs):
            result = type("R", (), {"stdout": "", "returncode": 0})()
            if cmd[0] == "ls" and cmd[-1] == "/mnt/disk1/":
                result.stdout = "Movies\n"
            elif cmd[0] == "ls" and "Movies" in cmd[-1]:
                result.stdout = "2024\n2025\n1999\nExtras\n"
            elif cmd[0] == "du":
                result.stdout = TestScanMovableUnits._du_batch_output(cmd)
            return result

        mocker.patch("rebalancer.run_cmd", side_effect=mock_run)
        units = scan_movable_units(disk, DEFAULT_CONFIG["excludes"])
        assert len(units) == 3  # 2024, 2025, 1999 (not Extras)
        names = [u.name for u in units]
        assert "2024" in names
        assert "Extras" not in names

    def test_scan_excludes_default_shares(self, mocker):
        """Backups and Development should be excluded by default."""
        disk = DiskInfo("/mnt/disk1", 16_000_000_000_000, 14_000_000_000_000, 2_000_000_000_000, 87)

        def mock_run(cmd, **kwargs):
            result = type("R", (), {"stdout": "", "returncode": 0})()
            if cmd[0] == "ls" and cmd[-1] == "/mnt/disk1/":
                result.stdout = "TV_Shows\nBackups\nDevelopment\n"
            elif cmd[0] == "ls" and "TV_Shows" in cmd[-1]:
                result.stdout = "Show1\n"
            elif cmd[0] == "du":
                result.stdout = TestScanMovableUnits._du_batch_output(cmd)
            return result

        mocker.patch("rebalancer.run_cmd", side_effect=mock_run)
        units = scan_movable_units(disk, DEFAULT_CONFIG["excludes"])
        shares = {u.share for u in units}
        assert "Backups" not in shares
        assert "Development" not in shares
        assert "TV_Shows" in shares

    def test_scan_empty_disk(self, mocker):
        """A disk with no shares returns empty list."""
        disk = DiskInfo("/mnt/disk12", 16_000_000_000_000, 0, 16_000_000_000_000, 0)

        def mock_run(cmd, **kwargs):
            result = type("R", (), {"stdout": "", "returncode": 0})()
            return result

        mocker.patch("rebalancer.run_cmd", side_effect=mock_run)
        units = scan_movable_units(disk, DEFAULT_CONFIG["excludes"])
        assert units == []

    def test_scan_inner_ls_failure_skips_share(self, mocker):
        """H2: if ls of a share subdirectory fails, skip that share gracefully."""
        disk = DiskInfo("/mnt/disk1", 16_000_000_000_000, 14_000_000_000_000, 2_000_000_000_000, 87)

        def mock_run(cmd, **kwargs):
            result = type("R", (), {"stdout": "", "returncode": 0})()
            if cmd[0] == "ls" and cmd[-1] == "/mnt/disk1/":
                result.stdout = "TV_Shows\nBrokenShare\n"
            elif cmd[0] == "ls" and "BrokenShare" in cmd[-1]:
                result.returncode = 2  # ls failure
                result.stdout = "ls: cannot access '/mnt/disk1/BrokenShare/': I/O error\n"
            elif cmd[0] == "ls" and "TV_Shows" in cmd[-1]:
                result.stdout = "Show1\n"
            elif cmd[0] == "du":
                result.stdout = TestScanMovableUnits._du_batch_output(cmd)
            return result

        mocker.patch("rebalancer.run_cmd", side_effect=mock_run)
        units = scan_movable_units(disk, [])
        # BrokenShare should be skipped, TV_Shows should still work
        shares = {u.share for u in units}
        assert "TV_Shows" in shares
        assert "BrokenShare" not in shares

    def test_scan_empty_share(self, mocker):
        """A share directory with no children returns no units."""
        disk = DiskInfo("/mnt/disk1", 16_000_000_000_000, 14_000_000_000_000, 2_000_000_000_000, 87)

        def mock_run(cmd, **kwargs):
            cmd_str = " ".join(cmd) if isinstance(cmd, list) else cmd
            result = type("R", (), {"stdout": "", "returncode": 0})()
            if "ls -1" in cmd_str and cmd_str.endswith("/mnt/disk1/"):
                result.stdout = "TV_Shows\n"
            elif "ls -1" in cmd_str and "TV_Shows" in cmd_str:
                result.stdout = ""
            return result

        mocker.patch("rebalancer.run_cmd", side_effect=mock_run)
        units = scan_movable_units(disk, DEFAULT_CONFIG["excludes"])
        assert units == []
