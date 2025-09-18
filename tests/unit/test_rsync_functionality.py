#!/usr/bin/env python3
"""
Unit Tests for Core Rsync Functionality

This module tests the core rsync improvements that have been integrated
into the main unraid_rebalancer.py codebase, focusing on the actual
functionality rather than complex integration scenarios.
"""

import os
import sys
import unittest
from pathlib import Path

# Add project root to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import unraid_rebalancer
from unraid_rebalancer import RSYNC_MODES, get_rsync_flags


class TestRsyncModes(unittest.TestCase):
    """Test cases for rsync mode improvements."""

    def test_all_modes_exist(self):
        """Test that all expected modes exist."""
        expected_modes = ['fast', 'balanced', 'integrity']
        for mode in expected_modes:
            self.assertIn(mode, RSYNC_MODES, f"Mode '{mode}' missing from RSYNC_MODES")

    def test_rsync_modes_structure(self):
        """Test that RSYNC_MODES has the correct enhanced structure."""
        required_keys = ['flags', 'description', 'features', 'target_hardware']

        for mode_name, mode_config in RSYNC_MODES.items():
            with self.subTest(mode=mode_name):
                # Check all required keys exist
                for key in required_keys:
                    self.assertIn(key, mode_config, f"Mode '{mode_name}' missing '{key}'")

                # Check data types
                self.assertIsInstance(mode_config['flags'], list)
                self.assertIsInstance(mode_config['description'], str)
                self.assertIsInstance(mode_config['features'], list)
                self.assertIsInstance(mode_config['target_hardware'], str)

                # Check content is not empty
                self.assertGreater(len(mode_config['flags']), 0)
                self.assertGreater(len(mode_config['description']), 0)
                self.assertGreater(len(mode_config['features']), 0)
                self.assertGreater(len(mode_config['target_hardware']), 0)

    def test_all_modes_have_progress_reporting(self):
        """Test that all performance modes include progress reporting."""
        for mode_name in RSYNC_MODES.keys():
            with self.subTest(mode=mode_name):
                flags = get_rsync_flags(mode_name)
                self.assertIn('--info=progress2', flags,
                            f"Mode '{mode_name}' missing progress reporting")

    def test_atomic_operation_prerequisites(self):
        """Test that all modes have flags required for atomic operations."""
        required_flags = ['--partial', '--inplace', '--numeric-ids']

        for mode_name in RSYNC_MODES.keys():
            with self.subTest(mode=mode_name):
                flags = get_rsync_flags(mode_name)
                for flag in required_flags:
                    self.assertIn(flag, flags,
                                f"Mode '{mode_name}' missing required flag '{flag}'")

    def test_mode_specific_optimizations(self):
        """Test that each mode has its expected optimization flags."""
        # Fast mode should have --no-compress for speed
        fast_flags = get_rsync_flags('fast')
        self.assertIn('--no-compress', fast_flags, "Fast mode missing --no-compress optimization")

        # Balanced mode should have -X for extended attributes
        balanced_flags = get_rsync_flags('balanced')
        self.assertIn('-X', balanced_flags, "Balanced mode missing extended attributes support")

        # Integrity mode should have --checksum for maximum safety
        integrity_flags = get_rsync_flags('integrity')
        self.assertIn('--checksum', integrity_flags, "Integrity mode missing checksum verification")

    def test_get_rsync_flags_function(self):
        """Test the get_rsync_flags function works correctly."""
        # Test valid modes
        for mode_name in RSYNC_MODES.keys():
            with self.subTest(mode=mode_name):
                flags = get_rsync_flags(mode_name)
                self.assertIsInstance(flags, list)
                self.assertGreater(len(flags), 0)

        # Test invalid mode raises ValueError
        with self.assertRaises(ValueError):
            get_rsync_flags('invalid_mode')

    def test_flags_are_properly_formatted(self):
        """Test that all flags are properly formatted."""
        for mode_name in RSYNC_MODES.keys():
            with self.subTest(mode=mode_name):
                flags = get_rsync_flags(mode_name)
                for flag in flags:
                    # Should be strings
                    self.assertIsInstance(flag, str)
                    # Should not be empty
                    self.assertGreater(len(flag), 0)
                    # Should not have extra whitespace
                    self.assertEqual(flag, flag.strip())


class TestBackwardCompatibility(unittest.TestCase):
    """Test that improvements maintain backward compatibility."""

    def test_original_mode_names_preserved(self):
        """Test that original mode names still exist."""
        original_modes = ['fast', 'balanced', 'integrity']
        for mode in original_modes:
            self.assertIn(mode, RSYNC_MODES)

    def test_archive_mode_preserved(self):
        """Test that all modes still include archive functionality."""
        for mode_name in RSYNC_MODES.keys():
            with self.subTest(mode=mode_name):
                flags = get_rsync_flags(mode_name)
                # Archive mode can be specified as -a or as part of combined flags like -av or -aHAX
                archive_present = any('-a' in flag for flag in flags)
                self.assertTrue(archive_present, f"Mode '{mode_name}' missing archive functionality")

    def test_essential_rsync_options_preserved(self):
        """Test that essential rsync options are still present."""
        essential_flags = ['--partial', '--inplace', '--numeric-ids']

        for mode_name in RSYNC_MODES.keys():
            with self.subTest(mode=mode_name):
                flags = get_rsync_flags(mode_name)
                for essential_flag in essential_flags:
                    self.assertIn(essential_flag, flags,
                                f"Mode '{mode_name}' missing essential flag '{essential_flag}'")


class TestEnhancedFeatures(unittest.TestCase):
    """Test the enhanced features added to rsync modes."""

    def test_enhanced_descriptions(self):
        """Test that mode descriptions are enhanced and informative."""
        for mode_name, mode_config in RSYNC_MODES.items():
            with self.subTest(mode=mode_name):
                description = mode_config['description']
                # Should be a meaningful description, not just a few words
                self.assertGreater(len(description), 20)
                # Should contain key information about the mode
                if mode_name == 'fast':
                    self.assertIn('fast', description.lower())
                elif mode_name == 'balanced':
                    self.assertIn('balanced', description.lower())
                elif mode_name == 'integrity':
                    self.assertIn('integrity', description.lower())

    def test_feature_lists(self):
        """Test that modes have comprehensive feature lists."""
        for mode_name, mode_config in RSYNC_MODES.items():
            with self.subTest(mode=mode_name):
                features = mode_config['features']
                self.assertIsInstance(features, list)
                self.assertGreater(len(features), 0)

                # Check that features are meaningful strings
                for feature in features:
                    self.assertIsInstance(feature, str)
                    self.assertGreater(len(feature), 2)

    def test_target_hardware_information(self):
        """Test that modes have target hardware information."""
        for mode_name, mode_config in RSYNC_MODES.items():
            with self.subTest(mode=mode_name):
                target_hardware = mode_config['target_hardware']
                self.assertIsInstance(target_hardware, str)
                self.assertGreater(len(target_hardware), 10)

    def test_progress_reporting_consistency(self):
        """Test that progress reporting is now consistent across all modes."""
        for mode_name in RSYNC_MODES.keys():
            with self.subTest(mode=mode_name):
                flags = get_rsync_flags(mode_name)
                self.assertIn('--info=progress2', flags,
                            f"Mode '{mode_name}' missing consistent progress reporting")


class TestRsyncModesList(unittest.TestCase):
    """Test the enhanced --list-rsync-modes functionality."""

    def test_enhanced_output_data_available(self):
        """Test that enhanced output data is available for --list-rsync-modes."""
        for mode_name, mode_config in RSYNC_MODES.items():
            with self.subTest(mode=mode_name):
                # Test that we can format the enhanced output without errors
                try:
                    flags_str = " ".join(mode_config["flags"])
                    features_str = ", ".join(mode_config.get("features", []))
                    target_hardware = mode_config.get("target_hardware", "General purpose")

                    # All should be valid non-empty strings
                    self.assertIsInstance(flags_str, str)
                    self.assertIsInstance(features_str, str)
                    self.assertIsInstance(target_hardware, str)
                    self.assertGreater(len(flags_str), 0)
                    self.assertGreater(len(features_str), 0)
                    self.assertGreater(len(target_hardware), 0)

                except Exception as e:
                    self.fail(f"Enhanced output formatting failed for mode '{mode_name}': {e}")


if __name__ == '__main__':
    # Set up test environment
    os.environ['PYTHONPATH'] = str(Path(__file__).parent.parent.parent)

    # Run tests
    unittest.main(verbosity=2)