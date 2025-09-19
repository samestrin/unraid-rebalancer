#!/usr/bin/env python3
"""
Unit tests for ETA enhancement functionality in Unraid Rebalancer.

Tests the performance models, ETA calculations, smoothing algorithms,
and integration with the PerformanceMonitor class.
"""

import unittest
from unittest.mock import patch, MagicMock, Mock
import sys
import os
import time
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from unraid_rebalancer import (
    PerformanceMonitor, OperationMetrics, TransferMetrics, Plan, Move, Unit, format_duration
)


class TestETAEnhancement(unittest.TestCase):
    """Test suite for ETA enhancement functionality."""

    def setUp(self):
        """Set up test data for ETA calculations."""
        # Create mock plan for testing
        self.mock_unit1 = Unit(
            share="Movies",
            rel_path="Movie1",
            size_bytes=100 * 1024**3,  # 100GB
            src_disk="disk1"
        )
        self.mock_unit2 = Unit(
            share="Movies",
            rel_path="Movie2",
            size_bytes=200 * 1024**3,  # 200GB
            src_disk="disk2"
        )

        self.mock_moves = [
            Move(unit=self.mock_unit1, dest_disk="disk2"),
            Move(unit=self.mock_unit2, dest_disk="disk3")
        ]

        self.mock_plan = Plan(
            moves=self.mock_moves,
            summary={"total_moves": 2, "total_bytes": 300.0 * 1024**3}
        )

        # Create performance monitor for testing
        self.monitor = PerformanceMonitor(
            operation_id="test_operation",
            rsync_mode="fast",
            metrics_enabled=False  # Disable database for testing
        )

    def test_format_duration_seconds(self):
        """Test duration formatting for seconds."""
        self.assertEqual(format_duration(30.5), "30.5s")
        self.assertEqual(format_duration(45.0), "45.0s")

    def test_format_duration_minutes(self):
        """Test duration formatting for minutes."""
        self.assertEqual(format_duration(90), "1m 30s")
        self.assertEqual(format_duration(150), "2m 30s")
        self.assertEqual(format_duration(3540), "59m 0s")

    def test_format_duration_hours(self):
        """Test duration formatting for hours."""
        self.assertEqual(format_duration(3600), "1h 0m")
        self.assertEqual(format_duration(3720), "1h 2m")
        self.assertEqual(format_duration(7200), "2h 0m")

    @patch('unraid_rebalancer.get_conservative_write_rate')
    def test_calculate_initial_eta_with_models(self, mock_get_rate):
        """Test initial ETA calculation with performance models."""
        # Mock conservative write rate (80 MB/s)
        mock_get_rate.return_value = 80.0

        eta = self.monitor.calculate_initial_eta(self.mock_plan)

        # 300GB at 80 MB/s = 300*1024/80 = 3840 seconds
        expected_eta = (300 * 1024**3) / (80 * 1024**2)
        self.assertAlmostEqual(eta, expected_eta, places=0)

    def test_calculate_initial_eta_fallback(self):
        """Test initial ETA calculation with fallback when models unavailable."""
        # Test with plan that has total bytes
        eta = self.monitor.calculate_initial_eta(self.mock_plan)

        # Should get some positive ETA value
        self.assertGreater(eta, 0)
        self.assertIsInstance(eta, float)

    def test_calculate_initial_eta_empty_plan(self):
        """Test initial ETA calculation with empty plan."""
        empty_plan = Plan(moves=[], summary={"total_moves": 0, "total_bytes": 0})
        eta = self.monitor.calculate_initial_eta(empty_plan)

        self.assertEqual(eta, 0.0)

    def test_update_real_time_eta_no_transfers(self):
        """Test real-time ETA update with no transfer history."""
        eta = self.monitor.update_real_time_eta(0, 1000)
        self.assertIsNone(eta)

    def test_update_real_time_eta_with_transfers(self):
        """Test real-time ETA update with transfer history."""
        # Add some mock transfers to the operation
        transfer1 = TransferMetrics(
            unit_path="/mnt/disk1/Movies/Movie1",
            src_disk="disk1",
            dest_disk="disk2",
            size_bytes=50 * 1024**3,
            start_time=time.time() - 100,
            end_time=time.time() - 50,
            success=True,
            transfer_rate_bps=100 * 1024**2  # 100 MB/s
        )

        transfer2 = TransferMetrics(
            unit_path="/mnt/disk2/Movies/Movie2",
            src_disk="disk2",
            dest_disk="disk3",
            size_bytes=75 * 1024**3,
            start_time=time.time() - 50,
            end_time=time.time(),
            success=True,
            transfer_rate_bps=120 * 1024**2  # 120 MB/s
        )

        self.monitor.operation.transfers = [transfer1, transfer2]

        # Test ETA calculation
        remaining_bytes = 200 * 1024**3  # 200GB remaining
        eta = self.monitor.update_real_time_eta(100 * 1024**3, remaining_bytes)

        self.assertIsNotNone(eta)
        self.assertGreater(eta, 0)

    def test_update_real_time_eta_zero_remaining(self):
        """Test real-time ETA update with zero remaining bytes."""
        eta = self.monitor.update_real_time_eta(1000, 0)
        self.assertEqual(eta, 0.0)

    def test_get_eta_info(self):
        """Test getting ETA information."""
        # Set some test ETA values
        self.monitor.initial_eta_seconds = 3600.0  # 1 hour
        self.monitor.current_eta_seconds = 1800.0  # 30 minutes

        eta_info = self.monitor.get_eta_info()

        self.assertEqual(eta_info['initial_eta_seconds'], 3600.0)
        self.assertEqual(eta_info['current_eta_seconds'], 1800.0)

    def test_get_eta_info_none_values(self):
        """Test getting ETA information when values are None."""
        eta_info = self.monitor.get_eta_info()

        self.assertIsNone(eta_info['initial_eta_seconds'])
        self.assertIsNone(eta_info['current_eta_seconds'])

    def test_weighted_moving_average_calculation(self):
        """Test the weighted moving average calculation in update_real_time_eta."""
        # Create transfers with known rates
        transfers = []
        rates = [80, 90, 100, 110, 120]  # MB/s converted to bytes/s

        for i, rate_mbps in enumerate(rates):
            transfer = TransferMetrics(
                unit_path=f"/mnt/disk1/test{i}",
                src_disk="disk1",
                dest_disk="disk2",
                size_bytes=1024**3,
                start_time=time.time() - (len(rates) - i) * 10,
                end_time=time.time() - (len(rates) - i - 1) * 10,
                success=True,
                transfer_rate_bps=rate_mbps * 1024**2
            )
            transfers.append(transfer)

        self.monitor.operation.transfers = transfers

        # Test that weighted average favors recent transfers
        remaining_bytes = 1024**3
        eta = self.monitor.update_real_time_eta(0, remaining_bytes)

        self.assertIsNotNone(eta)
        # Should be less than if using simple average due to higher recent rates
        simple_avg_eta = remaining_bytes / (100 * 1024**2)  # Simple average rate
        self.assertLess(eta, simple_avg_eta)

    def test_performance_monitor_integration(self):
        """Test integration of ETA features with PerformanceMonitor."""
        # Test that monitor can be created successfully
        self.assertIsInstance(self.monitor, PerformanceMonitor)

        # Test that ETA methods exist and are callable
        self.assertTrue(hasattr(self.monitor, 'calculate_initial_eta'))
        self.assertTrue(hasattr(self.monitor, 'update_real_time_eta'))
        self.assertTrue(hasattr(self.monitor, 'get_eta_info'))

        # Test that methods return expected types
        eta = self.monitor.calculate_initial_eta(self.mock_plan)
        self.assertIsInstance(eta, (int, float))

        eta_info = self.monitor.get_eta_info()
        self.assertIsInstance(eta_info, dict)
        self.assertIn('initial_eta_seconds', eta_info)
        self.assertIn('current_eta_seconds', eta_info)


class TestPerformanceModels(unittest.TestCase):
    """Test suite for performance models functionality."""

    def setUp(self):
        """Set up test data for performance models."""
        try:
            from performance_models import (
                DRIVE_PERFORMANCE_MODELS, get_performance_model,
                estimate_transfer_rate_mbps, get_conservative_write_rate,
                detect_drive_type
            )
            self.models_available = True
            self.get_performance_model = get_performance_model
            self.estimate_transfer_rate_mbps = estimate_transfer_rate_mbps
            self.get_conservative_write_rate = get_conservative_write_rate
            self.detect_drive_type = detect_drive_type
            self.DRIVE_PERFORMANCE_MODELS = DRIVE_PERFORMANCE_MODELS
        except ImportError:
            self.models_available = False

    def test_performance_models_available(self):
        """Test that performance models module is available."""
        self.assertTrue(self.models_available, "Performance models module should be available")

    @unittest.skipUnless(lambda self: self.models_available, "Performance models not available")
    def test_get_performance_model_default(self):
        """Test getting default performance model."""
        model = self.get_performance_model()
        self.assertIn('sequential_write_mbps', model)
        self.assertIn('reliability_factor', model)

    @unittest.skipUnless(lambda self: self.models_available, "Performance models not available")
    def test_get_performance_model_7200_rpm(self):
        """Test getting 7200 RPM drive model."""
        model = self.get_performance_model("7200_rpm_sata")
        self.assertEqual(model['sequential_write_mbps'], 140)
        self.assertEqual(model['reliability_factor'], 0.85)

    @unittest.skipUnless(lambda self: self.models_available, "Performance models not available")
    def test_estimate_transfer_rate(self):
        """Test transfer rate estimation."""
        rate = self.estimate_transfer_rate_mbps("7200_rpm_sata", "sequential_write")
        expected_rate = 140 * 0.85  # base rate * reliability factor
        self.assertAlmostEqual(rate, expected_rate, places=1)

    @unittest.skipUnless(lambda self: self.models_available, "Performance models not available")
    def test_conservative_write_rate(self):
        """Test conservative write rate calculation."""
        rate = self.get_conservative_write_rate("7200_rpm_sata")
        # Should be 80% of estimated rate
        estimated = self.estimate_transfer_rate_mbps("7200_rpm_sata", "sequential_write")
        expected = estimated * 0.8
        self.assertAlmostEqual(rate, expected, places=1)

    @unittest.skipUnless(lambda self: self.models_available, "Performance models not available")
    def test_detect_drive_type_large_drive(self):
        """Test drive type detection for large drives."""
        # > 8TB should be detected as 5400 RPM
        large_size = 10 * 1024**4  # 10TB
        drive_type = self.detect_drive_type("/dev/sda", large_size)
        self.assertEqual(drive_type, "5400_rpm_sata")

    @unittest.skipUnless(lambda self: self.models_available, "Performance models not available")
    def test_detect_drive_type_small_drive(self):
        """Test drive type detection for small drives."""
        # < 500GB should be detected as SSD
        small_size = 256 * 1024**3  # 256GB
        drive_type = self.detect_drive_type("/dev/sda", small_size)
        self.assertEqual(drive_type, "ssd")

    @unittest.skipUnless(lambda self: self.models_available, "Performance models not available")
    def test_detect_drive_type_medium_drive(self):
        """Test drive type detection for medium drives."""
        # Medium size should default to 7200 RPM
        medium_size = 2 * 1024**4  # 2TB
        drive_type = self.detect_drive_type("/dev/sda", medium_size)
        self.assertEqual(drive_type, "7200_rpm_sata")


if __name__ == '__main__':
    unittest.main()