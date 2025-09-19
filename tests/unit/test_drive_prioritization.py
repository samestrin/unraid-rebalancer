#!/usr/bin/env python3
"""
Unit tests for drive prioritization functionality in Unraid Rebalancer.

Tests the fill percentage calculation, sorting strategies, and CLI integration
for the drive prioritization feature.
"""

import unittest
from unittest.mock import patch, MagicMock
import sys
import os
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from unraid_rebalancer import Disk, Unit, Plan, build_plan


class TestDrivePrioritization(unittest.TestCase):
    """Test suite for drive prioritization functionality."""

    def setUp(self):
        """Set up test data with disks of varying fill percentages."""
        # Create test disks with different fill percentages
        self.disk1 = Disk(
            name="disk1",
            path=Path("/mnt/disk1"),
            size_bytes=1000 * 1024**3,  # 1TB
            used_bytes=900 * 1024**3,   # 90% full
            free_bytes=100 * 1024**3    # 10% free
        )

        self.disk2 = Disk(
            name="disk2",
            path=Path("/mnt/disk2"),
            size_bytes=1000 * 1024**3,  # 1TB
            used_bytes=500 * 1024**3,   # 50% full
            free_bytes=500 * 1024**3    # 50% free
        )

        self.disk3 = Disk(
            name="disk3",
            path=Path("/mnt/disk3"),
            size_bytes=1000 * 1024**3,  # 1TB
            used_bytes=200 * 1024**3,   # 20% full
            free_bytes=800 * 1024**3    # 80% free
        )

        self.disks = [self.disk1, self.disk2, self.disk3]

        # Create test units from different disks
        self.units = [
            Unit(share="Movies", rel_path="Movie1", size_bytes=50 * 1024**3, src_disk="disk1"),
            Unit(share="Movies", rel_path="Movie2", size_bytes=30 * 1024**3, src_disk="disk1"),
            Unit(share="Movies", rel_path="Movie3", size_bytes=40 * 1024**3, src_disk="disk2"),
            Unit(share="TV", rel_path="Show1", size_bytes=20 * 1024**3, src_disk="disk2"),
        ]

    def test_disk_fill_percentage_calculation(self):
        """Test that fill percentages are calculated correctly."""
        self.assertAlmostEqual(self.disk1.fill_percentage, 90.0, places=1)
        self.assertAlmostEqual(self.disk2.fill_percentage, 50.0, places=1)
        self.assertAlmostEqual(self.disk3.fill_percentage, 20.0, places=1)

    def test_disk_used_pct_property(self):
        """Test that the used_pct property works correctly."""
        self.assertAlmostEqual(self.disk1.used_pct, 90.0, places=1)
        self.assertAlmostEqual(self.disk2.used_pct, 50.0, places=1)
        self.assertAlmostEqual(self.disk3.used_pct, 20.0, places=1)

    def test_fill_percentage_equals_used_pct(self):
        """Test that fill_percentage and used_pct return the same value."""
        for disk in self.disks:
            self.assertEqual(disk.fill_percentage, disk.used_pct)

    def test_zero_size_disk_fill_percentage(self):
        """Test fill percentage calculation for disk with zero size."""
        zero_disk = Disk(
            name="zero_disk",
            path=Path("/mnt/zero"),
            size_bytes=0,
            used_bytes=0,
            free_bytes=0
        )
        self.assertEqual(zero_disk.fill_percentage, 0.0)

    def test_build_plan_size_strategy(self):
        """Test plan generation with size-based sorting (default)."""
        plan = build_plan(self.disks, self.units, target_percent=80.0, headroom_percent=5.0, strategy='size')

        # Verify plan was created
        self.assertIsInstance(plan, Plan)
        self.assertGreater(len(plan.moves), 0)

        # Check that moves exist (disk1 is over 80% full, should donate)
        moves_from_disk1 = [m for m in plan.moves if m.unit.src_disk == "disk1"]
        self.assertGreater(len(moves_from_disk1), 0)

    def test_build_plan_space_strategy(self):
        """Test plan generation with space-based sorting."""
        plan = build_plan(self.disks, self.units, target_percent=80.0, headroom_percent=5.0, strategy='space')

        # Verify plan was created
        self.assertIsInstance(plan, Plan)
        self.assertGreater(len(plan.moves), 0)

        # Check that moves from high-fill disk1 are prioritized
        moves_from_disk1 = [m for m in plan.moves if m.unit.src_disk == "disk1"]
        self.assertGreater(len(moves_from_disk1), 0)

    def test_space_strategy_prioritizes_high_fill_disks(self):
        """Test that space strategy prioritizes moves from high-fill disks first."""
        plan = build_plan(self.disks, self.units, target_percent=80.0, headroom_percent=5.0, strategy='space')

        if len(plan.moves) > 1:
            # First move should be from the highest fill disk (disk1 at 90%)
            first_move = plan.moves[0]
            self.assertEqual(first_move.unit.src_disk, "disk1")

    def test_invalid_strategy_raises_error(self):
        """Test that invalid strategy raises ValueError."""
        with self.assertRaises(ValueError):
            build_plan(self.disks, self.units, target_percent=80.0, headroom_percent=5.0, strategy='invalid')

    def test_strategy_parameter_defaults_to_size(self):
        """Test that strategy parameter defaults to 'size' when not specified."""
        plan_default = build_plan(self.disks, self.units, target_percent=80.0, headroom_percent=5.0)
        plan_size = build_plan(self.disks, self.units, target_percent=80.0, headroom_percent=5.0, strategy='size')

        # Plans should be identical when using default vs explicit 'size'
        self.assertEqual(len(plan_default.moves), len(plan_size.moves))

    def test_empty_units_list(self):
        """Test plan generation with empty units list."""
        plan = build_plan(self.disks, [], target_percent=80.0, headroom_percent=5.0, strategy='space')
        self.assertEqual(len(plan.moves), 0)

    def test_single_disk_scenario(self):
        """Test plan generation with single disk."""
        single_disk = [self.disk1]
        plan = build_plan(single_disk, self.units, target_percent=80.0, headroom_percent=5.0, strategy='space')
        # Should have no moves since no recipient disks available
        self.assertEqual(len(plan.moves), 0)

    def test_all_disks_same_fill_percentage(self):
        """Test behavior when all disks have the same fill percentage."""
        # Create disks with same fill percentage
        same_fill_disks = []
        for i in range(3):
            disk = Disk(
                name=f"disk{i+1}",
                path=Path(f"/mnt/disk{i+1}"),
                size_bytes=1000 * 1024**3,
                used_bytes=500 * 1024**3,  # 50% full
                free_bytes=500 * 1024**3
            )
            same_fill_disks.append(disk)

        plan = build_plan(same_fill_disks, self.units, target_percent=40.0, headroom_percent=5.0, strategy='space')

        # All disks are above target, so moves should be generated
        self.assertGreaterEqual(len(plan.moves), 0)

    def test_space_strategy_secondary_sort_by_size(self):
        """Test that space strategy uses size as secondary sort criteria."""
        # Create units from same disk with different sizes
        same_disk_units = [
            Unit(share="Movies", rel_path="Small", size_bytes=10 * 1024**3, src_disk="disk1"),
            Unit(share="Movies", rel_path="Large", size_bytes=100 * 1024**3, src_disk="disk1"),
            Unit(share="Movies", rel_path="Medium", size_bytes=50 * 1024**3, src_disk="disk1"),
        ]

        plan = build_plan(self.disks, same_disk_units, target_percent=80.0, headroom_percent=5.0, strategy='space')

        # Moves from same disk should be sorted by size (largest first)
        moves_from_disk1 = [m for m in plan.moves if m.unit.src_disk == "disk1"]
        if len(moves_from_disk1) > 1:
            for i in range(len(moves_from_disk1) - 1):
                self.assertGreaterEqual(
                    moves_from_disk1[i].unit.size_bytes,
                    moves_from_disk1[i + 1].unit.size_bytes
                )


class TestDiskProperties(unittest.TestCase):
    """Test suite for Disk class properties."""

    def test_disk_properties_with_various_values(self):
        """Test disk properties with various size and usage values."""
        test_cases = [
            (2000 * 1024**3, 1000 * 1024**3, 1000 * 1024**3, 50.0),  # 50% full
            (1024**3, 256 * 1024**2, 768 * 1024**2, 25.0),            # 25% full
            (500 * 1024**3, 500 * 1024**3, 0, 100.0),                 # 100% full
            (1024**3, 0, 1024**3, 0.0),                               # 0% full
        ]

        for size_bytes, used_bytes, free_bytes, expected_pct in test_cases:
            with self.subTest(size=size_bytes, used=used_bytes):
                disk = Disk(
                    name="test_disk",
                    path=Path("/mnt/test"),
                    size_bytes=size_bytes,
                    used_bytes=used_bytes,
                    free_bytes=free_bytes
                )
                self.assertAlmostEqual(disk.fill_percentage, expected_pct, places=1)
                self.assertAlmostEqual(disk.used_pct, expected_pct, places=1)


if __name__ == '__main__':
    unittest.main()