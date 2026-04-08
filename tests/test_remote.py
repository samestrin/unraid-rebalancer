"""Tests for remote mode — Phase 9 RED."""

from unittest.mock import MagicMock

import pytest

from rebalancer import run_cmd, validate_remote_connection


class TestRunCmdRemote:
    def _mock_popen(self, mocker, stdout="", stderr="", returncode=0):
        mock_proc = MagicMock()
        mock_proc.communicate.return_value = (stdout, stderr)
        mock_proc.returncode = returncode
        mock = mocker.patch("rebalancer.subprocess.Popen", return_value=mock_proc)
        return mock, mock_proc

    def test_wraps_command_in_ssh(self, mocker):
        mock, _ = self._mock_popen(mocker, stdout="ok\n")
        run_cmd(["echo", "hello"], remote="root@unraid.lan")
        call_args = mock.call_args[0][0]
        assert call_args[0] == "ssh"
        assert "root@unraid.lan" in call_args
        assert "echo hello" in call_args[-1]

    def test_local_command_no_ssh(self, mocker):
        mock, _ = self._mock_popen(mocker)
        run_cmd(["echo", "hello"])
        call_args = mock.call_args[0][0]
        assert call_args == ["echo", "hello"]

    def test_ssh_connect_timeout(self, mocker):
        mock, _ = self._mock_popen(mocker)
        run_cmd(["df"], remote="root@host")
        call_args = mock.call_args[0][0]
        assert "-o" in call_args
        assert "ConnectTimeout=10" in call_args


class TestValidateRemoteConnection:
    def test_success(self, mocker):
        mock = mocker.patch("rebalancer.run_cmd")
        mock.return_value.returncode = 0
        mock.return_value.stdout = "ok\n"
        assert validate_remote_connection("root@unraid.lan") is True

    def test_failure(self, mocker):
        mock = mocker.patch("rebalancer.run_cmd")
        mock.return_value.returncode = 255
        mock.return_value.stdout = ""
        assert validate_remote_connection("root@bad.host") is False

    def test_exception(self, mocker):
        mock = mocker.patch("rebalancer.run_cmd")
        mock.side_effect = Exception("timeout")
        assert validate_remote_connection("root@bad.host") is False
