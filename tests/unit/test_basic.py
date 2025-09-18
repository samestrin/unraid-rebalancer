#!/usr/bin/env python3
"""
Basic tests for Unraid Rebalancer

These tests validate core functionality without requiring actual Unraid disks.
They use mocking and temporary directories to simulate disk operations.
"""

import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock

# Add parent directory to path to import the main module
sys.path.insert(0, str(Path(__file__).parent.parent))

import unraid_rebalancer as ur


class TestUtilityFunctions(unittest.TestCase):
    """Test utility functions."""
    
    def test_parse_size(self):
        """Test size parsing function."""
        # Test basic units
        self.assertEqual(ur.parse_size("1B"), 1)
        self.assertEqual(ur.parse_size("1KB"), 1000)
        self.assertEqual(ur.parse_size("1MB"), 1000**2)
        self.assertEqual(ur.parse_size("1GB"), 1000**3)
        
        # Test binary units
        self.assertEqual(ur.parse_size("1KiB"), 1024)
        self.assertEqual(ur.parse_size("1MiB"), 1024**2)
        self.assertEqual(ur.parse_size("1GiB"), 1024**3)
        
        # Test decimal values
        self.assertEqual(ur.parse_size("1.5GiB"), int(1.5 * 1024**3))
        
        # Test case insensitivity
        self.assertEqual(ur.parse_size("1gib"), 1024**3)
        self.assertEqual(ur.parse_size("1GIB"), 1024**3)
        
        # Test whitespace handling
        self.assertEqual(ur.parse_size(" 1 GiB "), 1024**3)
        
        # Test invalid inputs
        with self.assertRaises(Exception):
            ur.parse_size("invalid")
        with self.assertRaises(Exception):
            ur.parse_size("1XB")
    
    def test_human_bytes(self):
        """Test human-readable byte formatting."""
        self.assertEqual(ur.human_bytes(0), "0 B")
        self.assertEqual(ur.human_bytes(1), "1 B")
        self.assertEqual(ur.human_bytes(1023), "1023 B")
        self.assertEqual(ur.human_bytes(1024), "1.00 KiB")
        self.assertEqual(ur.human_bytes(1536), "1.50 KiB")
        self.assertEqual(ur.human_bytes(1024**2), "1.00 MiB")
        self.assertEqual(ur.human_bytes(1024**3), "1.00 GiB")
        self.assertEqual(ur.human_bytes(1024**4), "1.00 TiB")


class TestDataStructures(unittest.TestCase):
    """Test data structure classes."""
    
    def test_disk_creation(self):
        """Test Disk dataclass."""
        disk = ur.Disk(
            name="disk1",
            path=Path("/mnt/disk1"),
            size_bytes=1000000000,
            used_bytes=800000000,
            free_bytes=200000000
        )
        
        self.assertEqual(disk.name, "disk1")
        self.assertEqual(disk.used_pct, 80.0)
    
    def test_unit_creation(self):
        """Test Unit dataclass."""
        unit = ur.Unit(
            share="Movies",
            rel_path="Action/Movie1",
            size_bytes=5000000000,
            src_disk="disk1"
        )
        
        self.assertEqual(unit.share, "Movies")
        self.assertEqual(unit.src_abs(), Path("/mnt/disk1/Movies/Action/Movie1"))
        self.assertEqual(unit.dest_abs("disk2"), Path("/mnt/disk2/Movies/Action/Movie1"))
    
    def test_plan_serialization(self):
        """Test Plan JSON serialization."""
        unit = ur.Unit(
            share="Movies",
            rel_path="Action/Movie1",
            size_bytes=5000000000,
            src_disk="disk1"
        )
        move = ur.Move(unit=unit, dest_disk="disk2")
        plan = ur.Plan(moves=[move], summary={"total_moves": 1, "total_bytes": 5000000000.0})
        
        # Test serialization
        json_str = plan.to_json()
        self.assertIsInstance(json_str, str)
        
        # Test deserialization
        restored_plan = ur.Plan.from_json(json_str)
        self.assertEqual(len(restored_plan.moves), 1)
        self.assertEqual(restored_plan.moves[0].unit.share, "Movies")
        self.assertEqual(restored_plan.moves[0].dest_disk, "disk2")
        self.assertEqual(restored_plan.summary["total_moves"], 1)


class TestPlanningLogic(unittest.TestCase):
    """Test planning algorithms."""
    
    def setUp(self):
        """Set up test data."""
        # Create mock disks with different usage levels
        self.disk1 = ur.Disk(
            name="disk1",
            path=Path("/mnt/disk1"),
            size_bytes=1000000000000,  # 1TB
            used_bytes=900000000000,   # 900GB (90% full)
            free_bytes=100000000000    # 100GB free
        )
        
        self.disk2 = ur.Disk(
            name="disk2",
            path=Path("/mnt/disk2"),
            size_bytes=1000000000000,  # 1TB
            used_bytes=300000000000,   # 300GB (30% full)
            free_bytes=700000000000    # 700GB free
        )
        
        self.disks = [self.disk1, self.disk2]
        
        # Create mock units on the full disk
        self.units = [
            ur.Unit("Movies", "Movie1", 50000000000, "disk1"),  # 50GB
            ur.Unit("Movies", "Movie2", 30000000000, "disk1"),  # 30GB
            ur.Unit("TV", "Show1", 20000000000, "disk1"),       # 20GB
        ]
    
    def test_build_plan_basic(self):
        """Test basic plan building."""
        plan = ur.build_plan(self.disks, self.units, target_percent=80.0, headroom_percent=5.0)
        
        # Should generate moves to balance the disks
        self.assertGreater(len(plan.moves), 0)
        self.assertIn("total_moves", plan.summary)
        self.assertIn("total_bytes", plan.summary)
        
        # All moves should be from disk1 to disk2
        for move in plan.moves:
            self.assertEqual(move.unit.src_disk, "disk1")
            self.assertEqual(move.dest_disk, "disk2")
    
    def test_build_plan_no_moves_needed(self):
        """Test plan building when no moves are needed."""
        # Create balanced disks
        balanced_disk1 = ur.Disk("disk1", Path("/mnt/disk1"), 1000000000000, 400000000000, 600000000000)
        balanced_disk2 = ur.Disk("disk2", Path("/mnt/disk2"), 1000000000000, 400000000000, 600000000000)
        balanced_disks = [balanced_disk1, balanced_disk2]
        
        plan = ur.build_plan(balanced_disks, self.units, target_percent=80.0, headroom_percent=5.0)
        
        # Should generate no moves since disks are already balanced
        self.assertEqual(len(plan.moves), 0)


class TestFileOperations(unittest.TestCase):
    """Test file operation functions."""
    
    def test_du_path_file(self):
        """Test du_path with a single file."""
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            tmp.write(b"Hello, World!" * 1000)  # Write some data
            tmp.flush()
            
            size = ur.du_path(Path(tmp.name))
            self.assertEqual(size, 13000)  # 13 * 1000 bytes
            
            os.unlink(tmp.name)
    
    def test_du_path_directory(self):
        """Test du_path with a directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            
            # Create some files
            (tmpdir_path / "file1.txt").write_text("Hello" * 100)
            (tmpdir_path / "file2.txt").write_text("World" * 200)
            
            # Create subdirectory with file
            subdir = tmpdir_path / "subdir"
            subdir.mkdir()
            (subdir / "file3.txt").write_text("Test" * 50)
            
            total_size = ur.du_path(tmpdir_path)
            expected_size = 500 + 1000 + 200  # file1 + file2 + file3
            self.assertEqual(total_size, expected_size)
    
    @patch('os.statvfs')
    def test_is_mounted(self, mock_statvfs):
        """Test is_mounted function."""
        # Test successful mount check
        mock_statvfs.return_value = Mock()
        self.assertTrue(ur.is_mounted(Path("/mnt/disk1")))
        
        # Test failed mount check
        mock_statvfs.side_effect = OSError("Not mounted")
        self.assertFalse(ur.is_mounted(Path("/mnt/nonexistent")))


class TestCommandExecution(unittest.TestCase):
    """Test command execution functions."""
    
    @patch('subprocess.call')
    def test_run_dry_run(self, mock_call):
        """Test run function in dry-run mode."""
        result = ur.run(["echo", "test"], dry_run=True)
        self.assertEqual(result, 0)
        mock_call.assert_not_called()
    
    @patch('subprocess.call')
    def test_run_execute(self, mock_call):
        """Test run function in execute mode."""
        mock_call.return_value = 0
        result = ur.run(["echo", "test"], dry_run=False)
        self.assertEqual(result, 0)
        mock_call.assert_called_once_with(["echo", "test"])
    
    @patch('subprocess.call')
    def test_run_command_not_found(self, mock_call):
        """Test run function with command not found."""
        mock_call.side_effect = FileNotFoundError("Command not found")
        result = ur.run(["nonexistent_command"], dry_run=False)
        self.assertEqual(result, 127)


if __name__ == '__main__':
    # Configure logging for tests
    import logging
    logging.basicConfig(level=logging.CRITICAL)  # Suppress logs during tests
    
    # Run tests
    unittest.main(verbosity=2)