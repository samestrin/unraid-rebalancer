"""Tests for signal handling and active hours."""

from datetime import time as dt_time
from unittest.mock import patch

import pytest

import signal

from rebalancer import (
    is_within_active_hours,
    parse_time_range,
    setup_signal_handlers,
    shutdown_requested,
    reset_shutdown_flags,
)


class TestParseTimeRange:
    def test_simple_range(self):
        start, end = parse_time_range("09:00-17:00")
        assert start == dt_time(9, 0)
        assert end == dt_time(17, 0)

    def test_overnight_range(self):
        start, end = parse_time_range("22:00-06:00")
        assert start == dt_time(22, 0)
        assert end == dt_time(6, 0)

    def test_midnight_start(self):
        start, end = parse_time_range("00:00-08:00")
        assert start == dt_time(0, 0)
        assert end == dt_time(8, 0)

    def test_invalid_format_raises(self):
        with pytest.raises(ValueError):
            parse_time_range("9-17")

    def test_invalid_time_raises(self):
        with pytest.raises(ValueError):
            parse_time_range("25:00-06:00")

    def test_equal_start_end_raises(self):
        """H5: start == end creates a zero-width window that silently blocks all transfers."""
        with pytest.raises(ValueError):
            parse_time_range("09:00-09:00")


class TestIsWithinActiveHours:
    def test_none_returns_true(self):
        assert is_within_active_hours(None) is True

    def test_within_daytime_range(self):
        with patch("rebalancer.datetime") as mock_dt:
            mock_dt.now.return_value.time.return_value = dt_time(12, 0)
            assert is_within_active_hours("09:00-17:00") is True

    def test_outside_daytime_range(self):
        with patch("rebalancer.datetime") as mock_dt:
            mock_dt.now.return_value.time.return_value = dt_time(20, 0)
            assert is_within_active_hours("09:00-17:00") is False

    def test_within_overnight_range_late(self):
        with patch("rebalancer.datetime") as mock_dt:
            mock_dt.now.return_value.time.return_value = dt_time(23, 0)
            assert is_within_active_hours("22:00-06:00") is True

    def test_within_overnight_range_early(self):
        with patch("rebalancer.datetime") as mock_dt:
            mock_dt.now.return_value.time.return_value = dt_time(3, 0)
            assert is_within_active_hours("22:00-06:00") is True

    def test_outside_overnight_range(self):
        with patch("rebalancer.datetime") as mock_dt:
            mock_dt.now.return_value.time.return_value = dt_time(12, 0)
            assert is_within_active_hours("22:00-06:00") is False

    def test_at_exact_start_boundary(self):
        with patch("rebalancer.datetime") as mock_dt:
            mock_dt.now.return_value.time.return_value = dt_time(9, 0)
            assert is_within_active_hours("09:00-17:00") is True

    def test_at_exact_end_boundary(self):
        with patch("rebalancer.datetime") as mock_dt:
            mock_dt.now.return_value.time.return_value = dt_time(17, 0)
            assert is_within_active_hours("09:00-17:00") is False


class TestShutdownFlags:
    def setup_method(self):
        reset_shutdown_flags()

    def test_initial_state(self):
        assert shutdown_requested() is False

    def test_sigterm_handler_installed(self):
        """H3: SIGTERM should be handled for graceful shutdown on kill."""
        old_handler = signal.getsignal(signal.SIGTERM)
        try:
            setup_signal_handlers()
            handler = signal.getsignal(signal.SIGTERM)
            assert handler is not signal.SIG_DFL, "SIGTERM handler not installed"
        finally:
            signal.signal(signal.SIGTERM, old_handler)
            reset_shutdown_flags()
