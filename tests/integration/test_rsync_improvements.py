#!/usr/bin/env python3
"""
Integration Tests for Rsync Improvements

This module contains comprehensive integration tests for end-to-end functionality
of the rsync improvements including atomic operations, error recovery, and
complete transfer workflows.
"""

import os
import sys
import time
import tempfile
import unittest
import subprocess
import shutil
from unittest.mock import Mock, patch, MagicMock
from pathlib import Path

# Add project root to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import unraid_rebalancer
from unraid_rebalancer import (
    Plan, Move, Unit, Disk,
    perform_plan,
    get_rsync_flags,
    RSYNC_MODES
)


class TestAtomicTransferWorkflow(unittest.TestCase):
    """Integration tests for complete atomic transfer workflows."""

    def setUp(self):
        """Set up test environment with mock Unraid structure."""
        self.test_root = Path(tempfile.mkdtemp())

        # Create mock Unraid disk structure
        self.disk1_path = self.test_root / "mnt" / "disk1"
        self.disk2_path = self.test_root / "mnt" / "disk2"

        self.disk1_path.mkdir(parents=True)
        self.disk2_path.mkdir(parents=True)

        # Create test shares and content
        self.create_test_content()

    def tearDown(self):
        """Clean up test environment."""
        if self.test_root.exists():
            shutil.rmtree(self.test_root)

    def create_test_content(self):
        """Create test content for transfer testing."""
        # Create Movies share on disk1
        movies_disk1 = self.disk1_path / "Movies"
        movies_disk1.mkdir()

        # Create test movie directories with files
        movie1 = movies_disk1 / "TestMovie1"
        movie1.mkdir()
        (movie1 / "movie.mkv").write_text("fake movie content " * 1000)  # ~20KB
        (movie1 / "subtitles.srt").write_text("fake subtitles")

        movie2 = movies_disk1 / "TestMovie2"
        movie2.mkdir()
        (movie2 / "movie.mp4").write_text("another fake movie " * 800)  # ~16KB
        (movie2 / "info.nfo").write_text("movie info")

        # Create some content on disk2 as well
        music_disk2 = self.disk2_path / "Music"
        music_disk2.mkdir()
        album = music_disk2 / "TestAlbum"
        album.mkdir()
        (album / "track1.mp3").write_text("fake audio data " * 500)  # ~8KB

    def create_test_plan(self) -> Plan:
        """Create a test plan for moving content."""
        # Create disk objects
        disk1 = Disk(name="disk1", path=self.disk1_path, used_gb=1.0, free_gb=99.0)
        disk2 = Disk(name="disk2", path=self.disk2_path, used_gb=0.5, free_gb=99.5)

        # Create unit for TestMovie1
        movie1_path = self.disk1_path / "Movies" / "TestMovie1"
        unit1 = Unit(
            share="Movies",
            rel_path="TestMovie1",
            src_disk="disk1",
            size_bytes=movie1_path.stat().st_size if movie1_path.exists() else 20000
        )
        unit1._src_abs_cache = movie1_path

        # Create move operation
        move1 = Move(unit=unit1, dest_disk="disk2")

        return Plan(moves=[move1])

    @patch('unraid_rebalancer.run')
    def test_complete_atomic_transfer_workflow(self, mock_run):
        """Test complete atomic transfer workflow from plan to completion."""
        # Mock successful rsync execution
        mock_run.return_value = 0

        # Create test plan
        plan = self.create_test_plan()

        # Execute plan with atomic operations
        failures = perform_plan(
            plan=plan,
            execute=False,  # Dry run to avoid actual file operations
            rsync_extra=[],
            allow_merge=False,
            rsync_mode="balanced"
        )

        # Verify execution
        self.assertEqual(failures, 0)

        # Verify rsync was called with atomic flags
        self.assertTrue(mock_run.called)
        call_args = mock_run.call_args[0][0]  # First positional argument (cmd)

        # Check atomic operation flag
        self.assertIn("--remove-source-files", call_args)

        # Check performance mode flags
        balanced_flags = get_rsync_flags("balanced")
        for flag in balanced_flags:
            self.assertIn(flag, call_args)

    @patch('unraid_rebalancer.run')
    def test_atomic_transfer_error_handling(self, mock_run):
        """Test error handling in atomic transfer workflow."""
        # Mock rsync failure
        mock_run.return_value = 23  # Partial transfer error

        plan = self.create_test_plan()

        # Execute plan and expect failure handling
        failures = perform_plan(
            plan=plan,
            execute=False,
            rsync_extra=[],
            allow_merge=False,
            rsync_mode="fast"
        )

        # Should report failure
        self.assertEqual(failures, 1)

    @patch('unraid_rebalancer.run')
    def test_validation_integration(self, mock_run):
        """Test that validation is properly integrated into transfer workflow."""
        mock_run.return_value = 0

        # Create plan with non-existent source
        disk1 = Disk(name="disk1", path=self.disk1_path, used_gb=1.0, free_gb=99.0)
        disk2 = Disk(name="disk2", path=self.disk2_path, used_gb=0.5, free_gb=99.5)

        # Create unit with non-existent path
        non_existent_path = self.disk1_path / "Movies" / "NonExistent"
        unit = Unit(
            share="Movies",
            rel_path="NonExistent",
            src_disk="disk1",
            size_bytes=1000
        )
        unit._src_abs_cache = non_existent_path

        move = Move(unit=unit, dest_disk="disk2")
        plan = Plan(moves=[move])

        # Execute plan - should fail validation
        failures = perform_plan(
            plan=plan,
            execute=False,
            rsync_extra=[],
            allow_merge=False,
            rsync_mode="fast"
        )

        # Should report validation failure
        self.assertEqual(failures, 1)

        # rsync should not be called due to validation failure
        self.assertFalse(mock_run.called)


class TestPerformanceModeIntegration(unittest.TestCase):
    """Integration tests for performance mode functionality."""

    def test_all_performance_modes_have_required_flags(self):
        """Test that all performance modes have required flags for atomic operations."""
        for mode_name, mode_config in RSYNC_MODES.items():
            flags = get_rsync_flags(mode_name)

            # All modes should have these flags for atomic operations
            required_flags = ["--partial", "--inplace", "--numeric-ids"]
            for flag in required_flags:
                self.assertIn(flag, flags, f"Mode '{mode_name}' missing required flag '{flag}'")

            # All modes should have progress reporting
            self.assertIn("--info=progress2", flags, f"Mode '{mode_name}' missing progress reporting")

    def test_mode_specific_flags_integration(self):
        """Test that mode-specific flags are properly integrated."""
        # Fast mode should have no compression
        fast_flags = get_rsync_flags("fast")
        self.assertIn("--no-compress", fast_flags)

        # Balanced mode should have extended attributes
        balanced_flags = get_rsync_flags("balanced")
        self.assertIn("-X", balanced_flags)

        # Integrity mode should have checksums and hard links
        integrity_flags = get_rsync_flags("integrity")
        self.assertIn("--checksum", integrity_flags)
        # Note: -H is part of -aHAX combination

    @patch('unraid_rebalancer.run')
    def test_mode_selection_in_workflow(self, mock_run):
        """Test that different performance modes work in the complete workflow."""
        mock_run.return_value = 0

        # Create a simple test environment
        test_root = Path(tempfile.mkdtemp())
        try:
            # Create test structure
            source_dir = test_root / "mnt" / "disk1" / "Movies" / "TestMovie"
            dest_parent = test_root / "mnt" / "disk2" / "Movies"

            source_dir.mkdir(parents=True)
            dest_parent.mkdir(parents=True)
            (source_dir / "test.txt").write_text("test content")

            # Create plan
            disk1 = Disk(name="disk1", path=test_root / "mnt" / "disk1", used_gb=1.0, free_gb=99.0)
            disk2 = Disk(name="disk2", path=test_root / "mnt" / "disk2", used_gb=0.5, free_gb=99.5)

            unit = Unit(share="Movies", rel_path="TestMovie", src_disk="disk1", size_bytes=1000)
            unit._src_abs_cache = source_dir

            move = Move(unit=unit, dest_disk="disk2")
            plan = Plan(moves=[move])

            # Test each performance mode
            for mode in ["fast", "balanced", "integrity"]:
                mock_run.reset_mock()

                failures = perform_plan(
                    plan=plan,
                    execute=False,
                    rsync_extra=[],
                    allow_merge=False,
                    rsync_mode=mode
                )

                self.assertEqual(failures, 0, f"Mode '{mode}' failed")
                self.assertTrue(mock_run.called, f"Mode '{mode}' didn't call rsync")

                # Verify mode-specific flags were used
                call_args = mock_run.call_args[0][0]
                mode_flags = get_rsync_flags(mode)

                for flag in mode_flags:
                    self.assertIn(flag, call_args, f"Mode '{mode}' missing flag '{flag}'")

                # Verify atomic flag was added
                self.assertIn("--remove-source-files", call_args)

        finally:
            shutil.rmtree(test_root)


class TestErrorRecoveryIntegration(unittest.TestCase):
    """Integration tests for error recovery and rollback functionality."""

    def setUp(self):
        """Set up test environment."""
        self.test_root = Path(tempfile.mkdtemp())
        self.source_dir = self.test_root / "mnt" / "disk1" / "TestShare" / "TestUnit"
        self.dest_dir = self.test_root / "mnt" / "disk2" / "TestShare" / "TestUnit"

        # Create source with content
        self.source_dir.mkdir(parents=True)
        (self.source_dir / "file1.txt").write_text("test content 1")
        (self.source_dir / "file2.txt").write_text("test content 2")

        # Create destination parent
        self.dest_dir.parent.mkdir(parents=True)

    def tearDown(self):
        """Clean up test environment."""
        if self.test_root.exists():
            shutil.rmtree(self.test_root)

    @patch('unraid_rebalancer.run')
    @patch('unraid_rebalancer.logging')
    def test_partial_transfer_error_handling(self, mock_logging, mock_run):
        """Test handling of partial transfer errors."""
        # Mock partial transfer error
        mock_run.return_value = 23

        # Create test plan
        disk1 = Disk(name="disk1", path=self.test_root / "mnt" / "disk1", used_gb=1.0, free_gb=99.0)
        disk2 = Disk(name="disk2", path=self.test_root / "mnt" / "disk2", used_gb=0.5, free_gb=99.5)

        unit = Unit(share="TestShare", rel_path="TestUnit", src_disk="disk1", size_bytes=1000)
        unit._src_abs_cache = self.source_dir

        move = Move(unit=unit, dest_disk="disk2")
        plan = Plan(moves=[move])

        # Execute plan
        failures = perform_plan(
            plan=plan,
            execute=False,
            rsync_extra=[],
            allow_merge=False,
            rsync_mode="fast"
        )

        # Should report failure
        self.assertEqual(failures, 1)

        # Should log appropriate error information
        mock_logging.warning.assert_called()

    @patch('unraid_rebalancer.run')
    def test_critical_error_handling(self, mock_run):
        """Test handling of critical rsync errors."""
        # Mock critical configuration error
        mock_run.return_value = 1  # Syntax or usage error

        unit = Unit(share="TestShare", rel_path="TestUnit", src_disk="disk1", size_bytes=1000)
        unit._src_abs_cache = self.source_dir

        move = Move(unit=unit, dest_disk="disk2")
        plan = Plan(moves=[move])

        # Execute plan
        failures = perform_plan(
            plan=plan,
            execute=False,
            rsync_extra=[],
            allow_merge=False,
            rsync_mode="integrity"
        )

        # Should report failure
        self.assertEqual(failures, 1)


class TestBackwardCompatibilityIntegration(unittest.TestCase):
    """Integration tests for backward compatibility with existing configurations."""

    def test_existing_rsync_extra_flags_compatibility(self):
        """Test that existing --rsync-extra configurations still work."""
        # Test common rsync extra flags that users might have
        test_extra_flags = [
            ["--bwlimit=50M"],
            ["--verbose"],
            ["--stats"],
            ["--bwlimit=100M", "--verbose"],
        ]

        for extra_flags in test_extra_flags:
            with patch('unraid_rebalancer.run') as mock_run:
                mock_run.return_value = 0

                # Create minimal test environment
                test_root = Path(tempfile.mkdtemp())
                try:
                    source_dir = test_root / "mnt" / "disk1" / "Share" / "Unit"
                    source_dir.mkdir(parents=True)
                    (source_dir / "test.txt").write_text("test")

                    dest_parent = test_root / "mnt" / "disk2" / "Share"
                    dest_parent.mkdir(parents=True)

                    # Create plan
                    unit = Unit(share="Share", rel_path="Unit", src_disk="disk1", size_bytes=100)
                    unit._src_abs_cache = source_dir

                    move = Move(unit=unit, dest_disk="disk2")
                    plan = Plan(moves=[move])

                    # Execute with extra flags
                    failures = perform_plan(
                        plan=plan,
                        execute=False,
                        rsync_extra=extra_flags,
                        allow_merge=False,
                        rsync_mode="balanced"
                    )

                    self.assertEqual(failures, 0)

                    # Verify extra flags were included
                    call_args = mock_run.call_args[0][0]
                    for flag in extra_flags:
                        self.assertIn(flag, call_args)

                finally:
                    shutil.rmtree(test_root)

    def test_existing_performance_mode_behavior(self):
        """Test that existing performance mode selections still work as expected."""
        # Users who were using these modes should get enhanced versions
        for mode in ["fast", "balanced", "integrity"]:
            flags = get_rsync_flags(mode)

            # Should still have the core functionality they expect
            self.assertIn("-a", flags[0])  # Archive mode (or -av)
            self.assertIn("--partial", flags)
            self.assertIn("--inplace", flags)
            self.assertIn("--numeric-ids", flags)

            # But now with enhanced progress reporting
            self.assertIn("--info=progress2", flags)

    def test_dry_run_behavior_consistency(self):
        """Test that dry-run behavior is consistent with previous versions."""
        with patch('unraid_rebalancer.run') as mock_run:
            mock_run.return_value = 0

            test_root = Path(tempfile.mkdtemp())
            try:
                # Create test structure
                source_dir = test_root / "mnt" / "disk1" / "Share" / "Unit"
                source_dir.mkdir(parents=True)
                (source_dir / "test.txt").write_text("test content")

                dest_parent = test_root / "mnt" / "disk2" / "Share"
                dest_parent.mkdir(parents=True)

                # Create plan
                unit = Unit(share="Share", rel_path="Unit", src_disk="disk1", size_bytes=100)
                unit._src_abs_cache = source_dir

                move = Move(unit=unit, dest_disk="disk2")
                plan = Plan(moves=[move])

                # Test dry-run (execute=False)
                failures = perform_plan(
                    plan=plan,
                    execute=False,  # This is the default dry-run behavior
                    rsync_extra=[],
                    allow_merge=False,
                    rsync_mode="fast"
                )

                self.assertEqual(failures, 0)

                # Verify run was called with dry_run=True
                mock_run.assert_called()
                self.assertTrue(mock_run.call_args[1]['dry_run'])

            finally:
                shutil.rmtree(test_root)


class TestEndToEndWorkflow(unittest.TestCase):
    """End-to-end integration tests simulating real usage scenarios."""

    def setUp(self):
        """Set up realistic test environment."""
        self.test_root = Path(tempfile.mkdtemp())

        # Create realistic Unraid structure
        self.setup_realistic_unraid_structure()

    def tearDown(self):
        """Clean up test environment."""
        if self.test_root.exists():
            shutil.rmtree(self.test_root)

    def setup_realistic_unraid_structure(self):
        """Create a realistic Unraid disk structure for testing."""
        # Create multiple disks
        for disk_num in range(1, 4):  # disk1, disk2, disk3
            disk_path = self.test_root / "mnt" / f"disk{disk_num}"
            disk_path.mkdir(parents=True)

            # Create common shares
            for share in ["Movies", "TV", "Music", "Documents"]:
                share_path = disk_path / share
                share_path.mkdir()

                # Add some content to each share
                if share == "Movies":
                    for i in range(2):
                        movie_dir = share_path / f"Movie_{disk_num}_{i}"
                        movie_dir.mkdir()
                        (movie_dir / "movie.mkv").write_text(f"fake movie content {disk_num}_{i} " * 500)

                elif share == "TV":
                    show_dir = share_path / f"Show_{disk_num}"
                    show_dir.mkdir()
                    season_dir = show_dir / "Season 1"
                    season_dir.mkdir()
                    (season_dir / "episode1.mkv").write_text("fake episode content " * 300)

    @patch('unraid_rebalancer.run')
    def test_complete_rebalancing_scenario(self, mock_run):
        """Test a complete rebalancing scenario with multiple transfers."""
        mock_run.return_value = 0

        # Create a plan that moves content between disks
        moves = []

        # Move a movie from disk1 to disk2
        movie_source = self.test_root / "mnt" / "disk1" / "Movies" / "Movie_1_0"
        if movie_source.exists():
            unit1 = Unit(share="Movies", rel_path="Movie_1_0", src_disk="disk1", size_bytes=5000)
            unit1._src_abs_cache = movie_source
            moves.append(Move(unit=unit1, dest_disk="disk2"))

        # Move a TV show from disk2 to disk3
        tv_source = self.test_root / "mnt" / "disk2" / "TV" / "Show_2"
        if tv_source.exists():
            unit2 = Unit(share="TV", rel_path="Show_2", src_disk="disk2", size_bytes=3000)
            unit2._src_abs_cache = tv_source
            moves.append(Move(unit=unit2, dest_disk="disk3"))

        if moves:
            plan = Plan(moves=moves)

            # Execute the complete plan
            failures = perform_plan(
                plan=plan,
                execute=False,  # Dry run
                rsync_extra=["--verbose"],
                allow_merge=False,
                rsync_mode="balanced"
            )

            # All transfers should succeed
            self.assertEqual(failures, 0)

            # Verify correct number of rsync calls
            self.assertEqual(mock_run.call_count, len(moves))

            # Verify each call had correct atomic flags
            for call in mock_run.call_args_list:
                call_args = call[0][0]
                self.assertIn("--remove-source-files", call_args)
                self.assertIn("--verbose", call_args)  # User's extra flag

                # Verify balanced mode flags
                balanced_flags = get_rsync_flags("balanced")
                for flag in balanced_flags:
                    self.assertIn(flag, call_args)

    @patch('unraid_rebalancer.run')
    def test_mixed_success_failure_scenario(self, mock_run):
        """Test scenario with mixed success and failure results."""
        # Mock alternating success/failure
        mock_run.side_effect = [0, 23, 0]  # Success, partial transfer error, success

        # Create multiple moves
        moves = []
        for i in range(3):
            source_dir = self.test_root / "mnt" / "disk1" / "Movies" / f"Movie_1_{i}"
            if source_dir.exists():
                unit = Unit(share="Movies", rel_path=f"Movie_1_{i}", src_disk="disk1", size_bytes=1000)
                unit._src_abs_cache = source_dir
                moves.append(Move(unit=unit, dest_disk="disk2"))

        if len(moves) >= 3:
            plan = Plan(moves=moves[:3])  # Take first 3 moves

            failures = perform_plan(
                plan=plan,
                execute=False,
                rsync_extra=[],
                allow_merge=False,
                rsync_mode="integrity"
            )

            # Should report 1 failure (the middle one)
            self.assertEqual(failures, 1)

            # Should have called rsync 3 times
            self.assertEqual(mock_run.call_count, 3)


if __name__ == '__main__':
    # Set up test environment
    os.environ['PYTHONPATH'] = str(Path(__file__).parent.parent.parent)

    # Create test suite
    test_suite = unittest.TestSuite()

    # Add test classes
    test_classes = [
        TestAtomicTransferWorkflow,
        TestPerformanceModeIntegration,
        TestErrorRecoveryIntegration,
        TestBackwardCompatibilityIntegration,
        TestEndToEndWorkflow
    ]

    for test_class in test_classes:
        tests = unittest.TestLoader().loadTestsFromTestCase(test_class)
        test_suite.addTests(tests)

    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(test_suite)

    # Print summary
    print(f"\nTests run: {result.testsRun}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")
    print(f"Success rate: {((result.testsRun - len(result.failures) - len(result.errors)) / result.testsRun * 100):.1f}%")

    # Exit with appropriate code
    sys.exit(0 if result.wasSuccessful() else 1)