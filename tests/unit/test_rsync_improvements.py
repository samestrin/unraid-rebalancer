#!/usr/bin/env python3
"""
Unit Tests for Rsync Improvements

This module contains comprehensive unit tests for all new rsync improvement
functionality including atomic operations, standardized performance modes,
error handling, validation, and progress reporting.
"""

import os
import sys
import time
import tempfile
import unittest
from unittest.mock import Mock, patch, MagicMock, call
from pathlib import Path

# Add project root to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from artifacts.atomic_rsync_implementation import (
    perform_atomic_move,
    validate_atomic_transfer_prerequisites,
    verify_atomic_transfer_completion,
    AtomicTransferResult
)

from artifacts.performance_mode_standards import (
    get_standardized_rsync_flags,
    get_mode_description,
    get_mode_features,
    recommend_mode_for_hardware,
    validate_mode_compatibility,
    STANDARDIZED_RSYNC_MODES
)

from artifacts.error_handling_implementation import (
    TransferErrorHandler,
    TransferError,
    ErrorCategory,
    ErrorSeverity,
    handle_rsync_error
)

from artifacts.validation_implementation import (
    TransferValidator,
    ValidationResult,
    validate_transfer,
    verify_transfer
)

from artifacts.progress_reporting_implementation import (
    EnhancedProgressReporter,
    RsyncProgressParser,
    TransferProgress,
    ProgressPhase
)


class TestAtomicRsyncOperations(unittest.TestCase):
    """Test cases for atomic rsync operations."""

    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = Path(tempfile.mkdtemp())
        self.source_path = self.temp_dir / "source"
        self.dest_path = self.temp_dir / "dest"

        # Create test source file
        self.source_path.mkdir(parents=True)
        (self.source_path / "test_file.txt").write_text("test content")

    def tearDown(self):
        """Clean up test fixtures."""
        import shutil
        if self.temp_dir.exists():
            shutil.rmtree(self.temp_dir)

    @patch('artifacts.atomic_rsync_implementation.run')
    @patch('artifacts.atomic_rsync_implementation.get_rsync_flags')
    def test_perform_atomic_move_success(self, mock_get_flags, mock_run):
        """Test successful atomic move operation."""
        # Setup mocks
        mock_get_flags.return_value = ["-av", "--partial"]
        mock_run.return_value = 0  # Success

        # Test atomic move
        result = perform_atomic_move(
            self.source_path,
            self.dest_path,
            "fast",
            [],
            dry_run=True
        )

        # Verify result
        self.assertIsInstance(result, AtomicTransferResult)
        self.assertTrue(result.success)
        self.assertEqual(result.return_code, 0)
        self.assertIsNone(result.error_message)

        # Verify rsync command construction
        mock_get_flags.assert_called_once_with("fast")
        mock_run.assert_called_once()

        # Check that --remove-source-files was added to command
        call_args = mock_run.call_args[0][0]  # First positional argument (cmd)
        self.assertIn("--remove-source-files", call_args)

    @patch('artifacts.atomic_rsync_implementation.run')
    @patch('artifacts.atomic_rsync_implementation.get_rsync_flags')
    def test_perform_atomic_move_failure(self, mock_get_flags, mock_run):
        """Test atomic move operation failure."""
        # Setup mocks
        mock_get_flags.return_value = ["-av", "--partial"]
        mock_run.return_value = 23  # Partial transfer error

        # Test atomic move
        result = perform_atomic_move(
            self.source_path,
            self.dest_path,
            "fast",
            [],
            dry_run=True
        )

        # Verify result
        self.assertIsInstance(result, AtomicTransferResult)
        self.assertFalse(result.success)
        self.assertEqual(result.return_code, 23)
        self.assertIsNotNone(result.error_message)
        self.assertIn("Atomic rsync failed", result.error_message)

    def test_validate_atomic_transfer_prerequisites(self):
        """Test prerequisite validation for atomic transfers."""
        # Test with valid paths
        result = validate_atomic_transfer_prerequisites(self.source_path, self.dest_path)
        self.assertTrue(result)

        # Test with non-existent source
        non_existent = self.temp_dir / "non_existent"
        result = validate_atomic_transfer_prerequisites(non_existent, self.dest_path)
        self.assertFalse(result)

    def test_verify_atomic_transfer_completion(self):
        """Test verification of atomic transfer completion."""
        # Create destination to simulate successful transfer
        self.dest_path.mkdir(parents=True)
        (self.dest_path / "test_file.txt").write_text("test content")

        # Remove source to simulate atomic move
        import shutil
        shutil.rmtree(self.source_path)

        # Test verification
        result = verify_atomic_transfer_completion(self.source_path, self.dest_path)
        self.assertTrue(result)

        # Test with source still existing (failed atomic move)
        self.source_path.mkdir()
        result = verify_atomic_transfer_completion(self.source_path, self.dest_path)
        self.assertFalse(result)


class TestPerformanceModeStandards(unittest.TestCase):
    """Test cases for standardized performance modes."""

    def test_get_standardized_rsync_flags(self):
        """Test retrieval of standardized rsync flags."""
        # Test valid modes
        for mode in ["fast", "balanced", "integrity"]:
            flags = get_standardized_rsync_flags(mode)
            self.assertIsInstance(flags, list)
            self.assertGreater(len(flags), 0)

            # All modes should include these common flags
            self.assertIn("--partial", flags)
            self.assertIn("--inplace", flags)
            self.assertIn("--numeric-ids", flags)

        # Test invalid mode
        with self.assertRaises(ValueError):
            get_standardized_rsync_flags("invalid_mode")

    def test_mode_specific_flags(self):
        """Test mode-specific flag configurations."""
        # Fast mode should have --no-compress
        fast_flags = get_standardized_rsync_flags("fast")
        self.assertIn("--no-compress", fast_flags)

        # Balanced mode should have -X (extended attributes)
        balanced_flags = get_standardized_rsync_flags("balanced")
        self.assertIn("-X", balanced_flags)

        # Integrity mode should have --checksum
        integrity_flags = get_standardized_rsync_flags("integrity")
        self.assertIn("--checksum", integrity_flags)

    def test_get_mode_description(self):
        """Test mode description retrieval."""
        for mode in ["fast", "balanced", "integrity"]:
            description = get_mode_description(mode)
            self.assertIsInstance(description, str)
            self.assertGreater(len(description), 0)

        # Test invalid mode
        description = get_mode_description("invalid")
        self.assertIn("Unknown mode", description)

    def test_get_mode_features(self):
        """Test mode features retrieval."""
        # Test fast mode features
        fast_features = get_mode_features("fast")
        self.assertIn("no_compression", fast_features)
        self.assertIn("progress_reporting", fast_features)

        # Test integrity mode features
        integrity_features = get_mode_features("integrity")
        self.assertIn("checksum_verification", integrity_features)
        self.assertIn("maximum_integrity", integrity_features)

    def test_recommend_mode_for_hardware(self):
        """Test hardware-based mode recommendations."""
        # High-end hardware should recommend integrity
        mode = recommend_mode_for_hardware(cpu_cores=8, available_memory_gb=16)
        self.assertEqual(mode, "integrity")

        # Mid-range hardware should recommend balanced
        mode = recommend_mode_for_hardware(cpu_cores=4, available_memory_gb=8)
        self.assertEqual(mode, "balanced")

        # Low-end hardware should recommend fast
        mode = recommend_mode_for_hardware(cpu_cores=2, available_memory_gb=4)
        self.assertEqual(mode, "fast")

    def test_validate_mode_compatibility(self):
        """Test mode compatibility validation."""
        # Test fast mode with hard links (should warn)
        compatible, warnings = validate_mode_compatibility("fast", {"has_hard_links": True})
        self.assertFalse(compatible)
        self.assertGreater(len(warnings), 0)

        # Test integrity mode with hard links (should be compatible)
        compatible, warnings = validate_mode_compatibility("integrity", {"has_hard_links": True})
        self.assertTrue(compatible)


class TestErrorHandling(unittest.TestCase):
    """Test cases for error handling implementation."""

    def setUp(self):
        """Set up test fixtures."""
        self.error_handler = TransferErrorHandler()

    def test_categorize_rsync_error(self):
        """Test rsync error categorization."""
        # Test various error codes
        test_cases = [
            (0, ErrorCategory.UNKNOWN_ERROR, ErrorSeverity.LOW),
            (1, ErrorCategory.RSYNC_ERROR, ErrorSeverity.HIGH),
            (11, ErrorCategory.FILESYSTEM_ERROR, ErrorSeverity.HIGH),
            (23, ErrorCategory.RSYNC_ERROR, ErrorSeverity.MEDIUM),
            (30, ErrorCategory.NETWORK_ERROR, ErrorSeverity.MEDIUM),
        ]

        for return_code, expected_category, expected_severity in test_cases:
            error = self.error_handler.categorize_rsync_error(return_code)
            self.assertEqual(error.category, expected_category)
            self.assertEqual(error.severity, expected_severity)
            self.assertEqual(error.rsync_return_code, return_code)

    def test_categorize_rsync_error_with_stderr(self):
        """Test rsync error categorization with stderr analysis."""
        # Test disk space error
        error = self.error_handler.categorize_rsync_error(11, "No space left on device")
        self.assertEqual(error.category, ErrorCategory.DISK_SPACE_ERROR)

        # Test permission error
        error = self.error_handler.categorize_rsync_error(11, "Permission denied")
        self.assertEqual(error.category, ErrorCategory.PERMISSION_ERROR)

        # Test network error
        error = self.error_handler.categorize_rsync_error(10, "Connection refused")
        self.assertEqual(error.category, ErrorCategory.NETWORK_ERROR)

    @patch('time.sleep')  # Mock sleep to speed up tests
    def test_handle_transfer_error_recovery(self, mock_sleep):
        """Test error handling with recovery attempts."""
        temp_dir = Path(tempfile.mkdtemp())
        source = temp_dir / "source.txt"
        dest = temp_dir / "dest.txt"

        try:
            source.write_text("test")

            # Create recoverable network error
            error = TransferError(
                category=ErrorCategory.NETWORK_ERROR,
                severity=ErrorSeverity.MEDIUM,
                message="Network timeout",
                recoverable=True,
                retry_count=0
            )

            # Test recovery attempt
            result = self.error_handler.handle_transfer_error(error, source, dest)
            self.assertTrue(result)  # Should succeed with recovery

        finally:
            import shutil
            shutil.rmtree(temp_dir)

    def test_handle_rsync_error_utility(self):
        """Test the handle_rsync_error utility function."""
        temp_dir = Path(tempfile.mkdtemp())
        source = temp_dir / "source.txt"
        dest = temp_dir / "dest.txt"

        try:
            source.write_text("test")

            # Test with error handler
            result = handle_rsync_error(23, "Partial transfer", source, dest, self.error_handler)
            self.assertIsInstance(result, bool)

        finally:
            import shutil
            shutil.rmtree(temp_dir)


class TestValidationImplementation(unittest.TestCase):
    """Test cases for validation implementation."""

    def setUp(self):
        """Set up test fixtures."""
        self.validator = TransferValidator()
        self.temp_dir = Path(tempfile.mkdtemp())
        self.source_path = self.temp_dir / "source"
        self.dest_path = self.temp_dir / "dest"

        # Create test source
        self.source_path.mkdir(parents=True)
        (self.source_path / "test_file.txt").write_text("test content")

    def tearDown(self):
        """Clean up test fixtures."""
        import shutil
        if self.temp_dir.exists():
            shutil.rmtree(self.temp_dir)

    def test_validate_transfer_prerequisites(self):
        """Test transfer prerequisite validation."""
        validation = self.validator.validate_transfer_prerequisites(
            self.source_path,
            self.dest_path,
            "fast"
        )

        self.assertIsNotNone(validation)
        self.assertGreater(len(validation.checks), 0)

        # Should have various validation checks
        check_names = [check.check_name for check in validation.checks]
        self.assertIn("source_validation", check_names)
        self.assertIn("destination_parent", check_names)
        self.assertIn("disk_space", check_names)

    def test_validation_with_non_existent_source(self):
        """Test validation with non-existent source."""
        non_existent = self.temp_dir / "non_existent"

        validation = self.validator.validate_transfer_prerequisites(
            non_existent,
            self.dest_path,
            "fast"
        )

        self.assertEqual(validation.overall_result, ValidationResult.FAILED)
        self.assertFalse(validation.can_proceed)

    def test_verify_transfer_completion(self):
        """Test transfer completion verification."""
        # Create destination and remove source to simulate successful transfer
        self.dest_path.mkdir(parents=True)
        (self.dest_path / "test_file.txt").write_text("test content")

        import shutil
        shutil.rmtree(self.source_path)

        verification = self.validator.verify_transfer_completion(
            self.source_path,
            self.dest_path
        )

        self.assertEqual(verification.overall_result, ValidationResult.PASSED)
        self.assertTrue(verification.can_proceed)

    def test_utility_functions(self):
        """Test validation utility functions."""
        # Test validate_transfer utility
        validation = validate_transfer(self.source_path, self.dest_path, "balanced")
        self.assertIsNotNone(validation)

        # Test verify_transfer utility (create destination first)
        self.dest_path.mkdir(parents=True)
        (self.dest_path / "test_file.txt").write_text("test content")

        verification = verify_transfer(self.source_path, self.dest_path, 100)
        self.assertIsNotNone(verification)


class TestProgressReporting(unittest.TestCase):
    """Test cases for progress reporting implementation."""

    def setUp(self):
        """Set up test fixtures."""
        self.reporter = EnhancedProgressReporter()
        self.parser = RsyncProgressParser()

    def test_transfer_progress_creation(self):
        """Test TransferProgress object creation and methods."""
        progress = TransferProgress(
            transfer_id="test_1",
            source_path=Path("/source"),
            destination_path=Path("/dest"),
            total_bytes=1000000,
            transferred_bytes=500000,
            transfer_rate_bps=100000
        )

        # Test calculated properties
        self.assertEqual(progress.progress_percent, 50.0)
        self.assertIsInstance(progress.elapsed_time, float)
        self.assertIsNotNone(progress.eta_seconds)
        self.assertAlmostEqual(progress.transfer_rate_mbps, 100000 / (1024 * 1024), places=2)

        # Test status line formatting
        status_line = progress.format_status_line()
        self.assertIsInstance(status_line, str)
        self.assertIn("50.0%", status_line)

    def test_enhanced_progress_reporter(self):
        """Test EnhancedProgressReporter functionality."""
        source = Path("/mnt/disk1/test")
        dest = Path("/mnt/disk2/test")

        # Start monitoring
        progress = self.reporter.start_transfer_monitoring("test_1", source, dest, 1000000)
        self.assertIsNotNone(progress)
        self.assertEqual(progress.transfer_id, "test_1")

        # Update progress
        updated = self.reporter.update_transfer_progress(
            "test_1",
            transferred_bytes=500000,
            transfer_rate_bps=100000
        )
        self.assertIsNotNone(updated)
        self.assertEqual(updated.transferred_bytes, 500000)

        # Complete transfer
        self.reporter.complete_transfer("test_1", success=True)
        progress = self.reporter.get_transfer_progress("test_1")
        self.assertEqual(progress.phase, ProgressPhase.COMPLETED)

        # Test overall progress
        overall = self.reporter.get_overall_progress()
        self.assertIsInstance(overall, dict)
        self.assertIn("active_transfers", overall)

    def test_rsync_progress_parser(self):
        """Test RsyncProgressParser functionality."""
        # Test progress2 format parsing
        line = "1,234,567  45%   10.50MB/s    0:01:23"
        parsed = self.parser.parse_progress_line(line)

        self.assertEqual(parsed.get("type"), "progress")
        self.assertEqual(parsed.get("transferred_bytes"), 1234567)
        self.assertEqual(parsed.get("progress_percent"), 45)
        self.assertAlmostEqual(parsed.get("transfer_rate_bps"), 10.50 * 1024 * 1024, places=0)

        # Test file transfer line
        file_line = "/mnt/disk1/Movies/test_movie.mkv"
        parsed = self.parser.parse_progress_line(file_line)
        self.assertEqual(parsed.get("type"), "file")
        self.assertEqual(parsed.get("current_file"), file_line)

        # Test total size line
        size_line = "Total transferred file size: 1,234,567 bytes"
        parsed = self.parser.parse_progress_line(size_line)
        self.assertEqual(parsed.get("type"), "total_size")
        self.assertEqual(parsed.get("total_bytes"), 1234567)

    def test_progress_callback_system(self):
        """Test progress callback system."""
        callback_called = False
        received_progress = None

        def test_callback(progress):
            nonlocal callback_called, received_progress
            callback_called = True
            received_progress = progress

        # Register callback
        self.reporter.register_progress_callback(test_callback)

        # Start and update transfer
        progress = self.reporter.start_transfer_monitoring(
            "test_callback",
            Path("/source"),
            Path("/dest"),
            1000
        )

        self.reporter.update_transfer_progress("test_callback", transferred_bytes=500)

        # Verify callback was called
        self.assertTrue(callback_called)
        self.assertIsNotNone(received_progress)
        self.assertEqual(received_progress.transfer_id, "test_callback")


class TestMainIntegration(unittest.TestCase):
    """Test cases for integration with main unraid_rebalancer.py functionality."""

    def test_rsync_modes_consistency(self):
        """Test that all RSYNC_MODES have consistent structure."""
        # Import the main module
        import unraid_rebalancer

        for mode_name, mode_config in unraid_rebalancer.RSYNC_MODES.items():
            # Check required keys
            self.assertIn("flags", mode_config)
            self.assertIn("description", mode_config)
            self.assertIn("features", mode_config)
            self.assertIn("target_hardware", mode_config)

            # Check flags format
            self.assertIsInstance(mode_config["flags"], list)
            self.assertGreater(len(mode_config["flags"]), 0)

            # Check that all modes have progress reporting
            self.assertIn("--info=progress2", mode_config["flags"])

            # Check that all modes have atomic operation support
            # (This will be added by the perform_plan function)
            self.assertIn("--partial", mode_config["flags"])
            self.assertIn("--inplace", mode_config["flags"])

    @patch('unraid_rebalancer.run')
    def test_atomic_operation_integration(self, mock_run):
        """Test that atomic operations are properly integrated."""
        import unraid_rebalancer

        # Mock successful rsync
        mock_run.return_value = 0

        # Test that get_rsync_flags works with new modes
        for mode in ["fast", "balanced", "integrity"]:
            flags = unraid_rebalancer.get_rsync_flags(mode)
            self.assertIsInstance(flags, list)
            self.assertGreater(len(flags), 0)


if __name__ == '__main__':
    # Set up test environment
    os.environ['PYTHONPATH'] = str(Path(__file__).parent.parent.parent)

    # Create test suite
    test_suite = unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])

    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(test_suite)

    # Exit with appropriate code
    sys.exit(0 if result.wasSuccessful() else 1)