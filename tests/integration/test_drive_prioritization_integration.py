#!/usr/bin/env python3
"""
Integration tests for drive prioritization functionality in Unraid Rebalancer.

Tests the integration of drive prioritization with existing CLI options,
rebalancing scenarios, and overall system functionality.
"""

import unittest
from unittest.mock import patch, MagicMock, Mock
import sys
import os
import tempfile
import argparse
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from unraid_rebalancer import (
    Disk, Unit, Plan, Move, build_plan, format_duration, PerformanceMonitor
)


class TestDrivePrioritizationIntegration(unittest.TestCase):
    """Test suite for drive prioritization integration."""

    def setUp(self):
        """Set up test environment with mock disks and units."""
        # Create realistic test disks
        self.disks = [
            # High-fill disk (donor)
            Disk(
                name="disk1",
                path=Path("/mnt/disk1"),
                size_bytes=4000 * 1024**3,  # 4TB
                used_bytes=3600 * 1024**3,  # 90% full
                free_bytes=400 * 1024**3    # 10% free
            ),
            # Medium-fill disk (balanced)
            Disk(
                name="disk2",
                path=Path("/mnt/disk2"),
                size_bytes=4000 * 1024**3,  # 4TB
                used_bytes=2400 * 1024**3,  # 60% full
                free_bytes=1600 * 1024**3   # 40% free
            ),
            # Low-fill disk (recipient)
            Disk(
                name="disk3",
                path=Path("/mnt/disk3"),
                size_bytes=4000 * 1024**3,  # 4TB
                used_bytes=800 * 1024**3,   # 20% full
                free_bytes=3200 * 1024**3   # 80% free
            ),
            # Another low-fill disk (recipient)
            Disk(
                name="disk4",
                path=Path("/mnt/disk4"),
                size_bytes=4000 * 1024**3,  # 4TB
                used_bytes=1200 * 1024**3,  # 30% full
                free_bytes=2800 * 1024**3   # 70% free
            )
        ]

        # Create diverse allocation units
        self.units = [
            # Large units from high-fill disk
            Unit(share="Movies", rel_path="LargeMovie1", size_bytes=200 * 1024**3, src_disk="disk1"),
            Unit(share="Movies", rel_path="LargeMovie2", size_bytes=150 * 1024**3, src_disk="disk1"),
            Unit(share="TV", rel_path="TVSeries1", size_bytes=100 * 1024**3, src_disk="disk1"),

            # Medium units from medium-fill disk
            Unit(share="Movies", rel_path="MediumMovie1", size_bytes=80 * 1024**3, src_disk="disk2"),
            Unit(share="TV", rel_path="TVSeries2", size_bytes=60 * 1024**3, src_disk="disk2"),

            # Small units from various disks
            Unit(share="Photos", rel_path="PhotoSet1", size_bytes=20 * 1024**3, src_disk="disk1"),
            Unit(share="Photos", rel_path="PhotoSet2", size_bytes=15 * 1024**3, src_disk="disk2"),
        ]

    def test_integration_size_vs_space_strategy(self):
        """Test that size and space strategies produce different but valid plans."""
        target_percent = 75.0
        headroom_percent = 5.0

        # Generate plan with size strategy
        plan_size = build_plan(self.disks, self.units, target_percent, headroom_percent, 'size')

        # Generate plan with space strategy
        plan_space = build_plan(self.disks, self.units, target_percent, headroom_percent, 'space')

        # Both plans should have moves (disk1 is over target)
        self.assertGreater(len(plan_size.moves), 0)
        self.assertGreater(len(plan_space.moves), 0)

        # Plans should be valid (no moves to same disk)
        for plan in [plan_size, plan_space]:
            for move in plan.moves:
                self.assertNotEqual(move.unit.src_disk, move.dest_disk)

        # Space strategy should prioritize moves from disk1 (highest fill)
        if len(plan_space.moves) > 0:
            first_move_space = plan_space.moves[0]
            self.assertEqual(first_move_space.unit.src_disk, "disk1")

    def test_integration_with_include_disks_filter(self):
        """Test drive prioritization with disk inclusion filters."""
        # Simulate including only specific disks
        included_disks = [self.disks[0], self.disks[2]]  # disk1 and disk3
        filtered_units = [u for u in self.units if any(u.src_disk == d.name for d in included_disks)]

        plan = build_plan(included_disks, filtered_units, 75.0, 5.0, 'space')

        # Verify only included disks are used
        for move in plan.moves:
            self.assertIn(move.unit.src_disk, ['disk1', 'disk3'])
            self.assertIn(move.dest_disk, ['disk1', 'disk3'])

    def test_integration_with_exclude_disks_filter(self):
        """Test drive prioritization with disk exclusion filters."""
        # Simulate excluding specific disks
        excluded_disk_names = ['disk4']
        included_disks = [d for d in self.disks if d.name not in excluded_disk_names]
        filtered_units = [u for u in self.units if u.src_disk not in excluded_disk_names]

        plan = build_plan(included_disks, filtered_units, 75.0, 5.0, 'space')

        # Verify excluded disk is not used
        for move in plan.moves:
            self.assertNotEqual(move.unit.src_disk, 'disk4')
            self.assertNotEqual(move.dest_disk, 'disk4')

    def test_integration_with_target_percent_variations(self):
        """Test drive prioritization with different target percentages."""
        test_targets = [70.0, 80.0, 90.0]

        for target in test_targets:
            with self.subTest(target=target):
                plan = build_plan(self.disks, self.units, target, 5.0, 'space')

                # Higher targets should generally result in fewer moves
                if target == 90.0:
                    # Very high target - disk1 at 90% is right at target
                    moves_from_disk1 = [m for m in plan.moves if m.unit.src_disk == "disk1"]
                    # Should have few or no moves from disk1
                    self.assertLessEqual(len(moves_from_disk1), 2)
                elif target == 70.0:
                    # Lower target - disk1 at 90% is well over target
                    moves_from_disk1 = [m for m in plan.moves if m.unit.src_disk == "disk1"]
                    # Should have more moves from disk1
                    self.assertGreater(len(moves_from_disk1), 0)

    def test_integration_error_handling(self):
        """Test error handling in integrated scenarios."""
        # Test with empty disk list
        plan_empty_disks = build_plan([], self.units, 80.0, 5.0, 'space')
        self.assertEqual(len(plan_empty_disks.moves), 0)

        # Test with empty units list
        plan_empty_units = build_plan(self.disks, [], 80.0, 5.0, 'space')
        self.assertEqual(len(plan_empty_units.moves), 0)

        # Test with invalid strategy
        with self.assertRaises(ValueError):
            build_plan(self.disks, self.units, 80.0, 5.0, 'invalid_strategy')

    def test_integration_plan_serialization(self):
        """Test that plans with drive prioritization can be serialized and deserialized."""
        plan = build_plan(self.disks, self.units, 75.0, 5.0, 'space')

        # Serialize to JSON
        plan_json = plan.to_json()
        self.assertIsInstance(plan_json, str)

        # Deserialize from JSON
        plan_restored = Plan.from_json(plan_json)

        # Verify plans are equivalent
        self.assertEqual(len(plan.moves), len(plan_restored.moves))
        for orig_move, restored_move in zip(plan.moves, plan_restored.moves):
            self.assertEqual(orig_move.unit.src_disk, restored_move.unit.src_disk)
            self.assertEqual(orig_move.dest_disk, restored_move.dest_disk)
            self.assertEqual(orig_move.unit.size_bytes, restored_move.unit.size_bytes)

    def test_integration_performance_monitoring(self):
        """Test integration of drive prioritization with performance monitoring."""
        plan = build_plan(self.disks, self.units, 75.0, 5.0, 'space')

        # Create performance monitor
        monitor = PerformanceMonitor(
            operation_id="integration_test",
            rsync_mode="fast",
            metrics_enabled=False
        )

        # Test initial ETA calculation
        initial_eta = monitor.calculate_initial_eta(plan)
        self.assertGreater(initial_eta, 0)
        self.assertIsInstance(initial_eta, float)

        # Test ETA info retrieval
        eta_info = monitor.get_eta_info()
        self.assertIn('initial_eta_seconds', eta_info)
        self.assertIn('current_eta_seconds', eta_info)

    def test_integration_large_scale_scenario(self):
        """Test drive prioritization with larger number of units and disks."""
        # Create more disks with varying fill levels
        large_disks = []
        for i in range(10):
            fill_percent = 30 + (i * 7)  # 30% to 93% fill
            size_bytes = 8000 * 1024**3  # 8TB
            used_bytes = int(size_bytes * fill_percent / 100)
            free_bytes = size_bytes - used_bytes

            disk = Disk(
                name=f"disk{i+1}",
                path=Path(f"/mnt/disk{i+1}"),
                size_bytes=size_bytes,
                used_bytes=used_bytes,
                free_bytes=free_bytes
            )
            large_disks.append(disk)

        # Create many units
        large_units = []
        for i in range(50):
            size_gb = 10 + (i % 40)  # 10GB to 50GB units
            src_disk_idx = i % len(large_disks)

            unit = Unit(
                share=f"Share{i % 5}",
                rel_path=f"Item{i}",
                size_bytes=size_gb * 1024**3,
                src_disk=f"disk{src_disk_idx + 1}"
            )
            large_units.append(unit)

        # Test both strategies with large dataset
        plan_size = build_plan(large_disks, large_units, 80.0, 5.0, 'size')
        plan_space = build_plan(large_disks, large_units, 80.0, 5.0, 'space')

        # Both should handle large datasets
        self.assertIsInstance(plan_size, Plan)
        self.assertIsInstance(plan_space, Plan)

        # Space strategy should prioritize high-fill disks
        if len(plan_space.moves) > 0:
            # Check that early moves come from high-fill disks
            first_moves = plan_space.moves[:5]  # First 5 moves
            high_fill_moves = [m for m in first_moves if int(m.unit.src_disk.replace('disk', '')) >= 7]
            self.assertGreater(len(high_fill_moves), 0, "Should prioritize moves from high-fill disks")


class TestETAIntegration(unittest.TestCase):
    """Test suite for ETA enhancement integration."""

    def setUp(self):
        """Set up test environment for ETA integration."""
        self.test_plan = Plan(
            moves=[
                Move(
                    unit=Unit("Movies", "Movie1", 100 * 1024**3, "disk1"),
                    dest_disk="disk2"
                ),
                Move(
                    unit=Unit("TV", "Show1", 50 * 1024**3, "disk2"),
                    dest_disk="disk3"
                )
            ],
            summary={"total_moves": 2, "total_bytes": 150.0 * 1024**3}
        )

    def test_eta_format_duration_integration(self):
        """Test that duration formatting works correctly in integrated scenarios."""
        test_cases = [
            (30, "30.0s"),
            (90, "1m 30s"),
            (3660, "1h 1m"),
            (7320, "2h 2m")
        ]

        for seconds, expected in test_cases:
            with self.subTest(seconds=seconds):
                formatted = format_duration(seconds)
                self.assertEqual(formatted, expected)

    def test_eta_performance_monitor_integration(self):
        """Test ETA functionality integration with PerformanceMonitor."""
        monitor = PerformanceMonitor(
            operation_id="eta_integration_test",
            rsync_mode="balanced",
            metrics_enabled=False
        )

        # Test initial ETA calculation
        eta = monitor.calculate_initial_eta(self.test_plan)
        self.assertGreater(eta, 0)

        # Test that ETA info can be retrieved
        eta_info = monitor.get_eta_info()
        self.assertIsNotNone(eta_info['initial_eta_seconds'])

    @patch('unraid_rebalancer.get_conservative_write_rate')
    def test_eta_with_performance_models_integration(self, mock_rate):
        """Test ETA calculation integration with performance models."""
        mock_rate.return_value = 100.0  # 100 MB/s

        monitor = PerformanceMonitor(
            operation_id="model_integration_test",
            metrics_enabled=False
        )

        eta = monitor.calculate_initial_eta(self.test_plan)

        # 150GB at 100 MB/s should be 150*1024/100 = 1536 seconds
        expected_eta = (150 * 1024**3) / (100 * 1024**2)
        self.assertAlmostEqual(eta, expected_eta, places=0)


if __name__ == '__main__':
    unittest.main()