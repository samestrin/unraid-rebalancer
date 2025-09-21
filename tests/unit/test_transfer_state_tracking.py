"""Unit tests for transfer state tracking functionality."""

import unittest
import time
import tempfile
import os
from unittest.mock import Mock, patch, MagicMock
from pathlib import Path

import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from unraid_rebalancer import TransferState, TransferStateManager
from metrics_storage import MetricsDatabase


class TestTransferState(unittest.TestCase):
    """Test TransferState class functionality."""

    def setUp(self):
        """Set up test fixtures."""
        self.transfer_state = TransferState(
            unit_path="Movies/Test Movie (2023)",
            src_disk="disk1",
            dest_disk="disk2",
            size_bytes=1024*1024*1024,  # 1GB
            start_time=time.time()
        )

    def test_transfer_state_creation(self):
        """Test TransferState creation with valid data."""
        self.assertEqual(self.transfer_state.unit_path, "Movies/Test Movie (2023)")
        self.assertEqual(self.transfer_state.src_disk, "disk1")
        self.assertEqual(self.transfer_state.dest_disk, "disk2")
        self.assertEqual(self.transfer_state.size_bytes, 1024*1024*1024)
        self.assertFalse(self.transfer_state.completed)
        self.assertIsNone(self.transfer_state.error_message)

    def test_is_in_progress(self):
        """Test is_in_progress method."""
        # Fresh transfer should be in progress
        self.assertTrue(self.transfer_state.is_in_progress())

        # Completed transfer should not be in progress
        self.transfer_state.completed = True
        self.assertFalse(self.transfer_state.is_in_progress())

        # Failed transfer should not be in progress
        self.transfer_state.completed = False
        self.transfer_state.error_message = "Transfer failed"
        self.assertFalse(self.transfer_state.is_in_progress())

    def test_to_dict(self):
        """Test to_dict serialization."""
        data = self.transfer_state.to_dict()
        self.assertIsInstance(data, dict)
        self.assertEqual(data['unit_path'], "Movies/Test Movie (2023)")
        self.assertEqual(data['src_disk'], "disk1")
        self.assertEqual(data['dest_disk'], "disk2")
        self.assertEqual(data['size_bytes'], 1024*1024*1024)
        self.assertFalse(data['completed'])
        self.assertIsNone(data['error_message'])

    def test_from_dict(self):
        """Test from_dict deserialization."""
        data = {
            'unit_path': "Shows/Test Show/Season 1",
            'src_disk': "disk3",
            'dest_disk': "disk4",
            'size_bytes': 512*1024*1024,
            'start_time': time.time(),
            'completed': True,
            'error_message': None
        }

        transfer = TransferState.from_dict(data)
        self.assertEqual(transfer.unit_path, "Shows/Test Show/Season 1")
        self.assertEqual(transfer.src_disk, "disk3")
        self.assertEqual(transfer.dest_disk, "disk4")
        self.assertEqual(transfer.size_bytes, 512*1024*1024)
        self.assertTrue(transfer.completed)
        self.assertIsNone(transfer.error_message)


class TestTransferStateManager(unittest.TestCase):
    """Test TransferStateManager class functionality."""

    def setUp(self):
        """Set up test fixtures."""
        self.mock_database = Mock(spec=MetricsDatabase)
        self.operation_id = "test_operation_001"
        self.manager = TransferStateManager(self.mock_database, self.operation_id)

    def test_transfer_state_manager_creation(self):
        """Test TransferStateManager creation."""
        self.assertEqual(self.manager.database, self.mock_database)
        self.assertEqual(self.manager.operation_id, self.operation_id)
        self.assertEqual(len(self.manager._active_transfers), 0)

    def test_start_transfer(self):
        """Test starting a new transfer."""
        unit_path = "Movies/Test Movie (2023)"
        src_disk = "disk1"
        dest_disk = "disk2"
        size_bytes = 1024*1024*1024

        transfer = self.manager.start_transfer(unit_path, src_disk, dest_disk, size_bytes)

        self.assertIsInstance(transfer, TransferState)
        self.assertEqual(transfer.unit_path, unit_path)
        self.assertEqual(transfer.src_disk, src_disk)
        self.assertEqual(transfer.dest_disk, dest_disk)
        self.assertEqual(transfer.size_bytes, size_bytes)
        self.assertFalse(transfer.completed)
        self.assertTrue(transfer.is_in_progress())

        # Check that transfer is stored in active transfers
        transfer_key = f"{src_disk}:{unit_path}"
        self.assertIn(transfer_key, self.manager._active_transfers)

        # Check that database method was called
        self.mock_database.store_transfer.assert_called_once()

    def test_complete_transfer_success(self):
        """Test completing a transfer successfully."""
        # Start a transfer first
        unit_path = "Movies/Test Movie (2023)"
        transfer = self.manager.start_transfer(unit_path, "disk1", "disk2", 1024*1024*1024)

        # Complete the transfer successfully
        self.manager.complete_transfer(transfer, success=True)

        self.assertTrue(transfer.completed)
        self.assertIsNone(transfer.error_message)

        # Check that transfer is removed from active transfers
        transfer_key = f"disk1:{unit_path}"
        self.assertNotIn(transfer_key, self.manager._active_transfers)

    def test_complete_transfer_failure(self):
        """Test completing a transfer with failure."""
        # Start a transfer first
        unit_path = "Movies/Test Movie (2023)"
        transfer = self.manager.start_transfer(unit_path, "disk1", "disk2", 1024*1024*1024)

        # Complete the transfer with failure
        error_message = "Disk full"
        self.manager.complete_transfer(transfer, success=False, error_message=error_message)

        self.assertTrue(transfer.completed)
        self.assertEqual(transfer.error_message, error_message)

        # Check that transfer is removed from active transfers
        transfer_key = f"disk1:{unit_path}"
        self.assertNotIn(transfer_key, self.manager._active_transfers)

    def test_get_active_transfers(self):
        """Test getting active transfers."""
        # Start multiple transfers
        transfer1 = self.manager.start_transfer("Movies/Movie1", "disk1", "disk2", 1024*1024)
        transfer2 = self.manager.start_transfer("Movies/Movie2", "disk3", "disk4", 2048*1024)

        active = self.manager.get_active_transfers()
        self.assertEqual(len(active), 2)
        self.assertIn(transfer1, active)
        self.assertIn(transfer2, active)

        # Complete one transfer
        self.manager.complete_transfer(transfer1, success=True)

        active = self.manager.get_active_transfers()
        self.assertEqual(len(active), 1)
        self.assertIn(transfer2, active)
        self.assertNotIn(transfer1, active)

    def test_get_orphaned_transfers(self):
        """Test detecting orphaned transfers."""
        # Mock database to return some incomplete transfers
        mock_incomplete_transfers = [
            {
                'unit_path': "Movies/Orphaned Movie",
                'src_disk': "disk1",
                'dest_disk': "disk2",
                'size_bytes': 1024*1024,
                'start_time': time.time() - 3600,  # 1 hour ago
                'error_message': None
            },
            {
                'unit_path': "Movies/Valid Movie",
                'src_disk': "disk3",
                'dest_disk': "disk4",
                'size_bytes': 2048*1024,
                'start_time': time.time() - 1800,  # 30 minutes ago
                'error_message': None
            }
        ]

        self.manager._get_incomplete_transfers_from_db = Mock(return_value=[
            TransferState(
                unit_path=data['unit_path'],
                src_disk=data['src_disk'],
                dest_disk=data['dest_disk'],
                size_bytes=data['size_bytes'],
                start_time=data['start_time'],
                completed=False,
                error_message=data['error_message']
            )
            for data in mock_incomplete_transfers
        ])

        current_plan_units = {"Movies/Valid Movie", "Movies/New Movie"}

        orphaned = self.manager.get_orphaned_transfers(current_plan_units)

        self.assertEqual(len(orphaned), 1)
        self.assertEqual(orphaned[0].unit_path, "Movies/Orphaned Movie")

    def test_cleanup_orphaned_transfers(self):
        """Test cleaning up orphaned transfers."""
        # Create some orphaned transfers
        orphaned_transfer = TransferState(
            unit_path="Movies/Orphaned Movie",
            src_disk="disk1",
            dest_disk="disk2",
            size_bytes=1024*1024,
            start_time=time.time() - 3600,
            completed=False
        )

        orphaned_transfers = [orphaned_transfer]

        # Mock the complete_transfer method to verify it's called
        with patch.object(self.manager, 'complete_transfer') as mock_complete:
            self.manager.cleanup_orphaned_transfers(orphaned_transfers)

            mock_complete.assert_called_once_with(
                orphaned_transfer,
                success=False,
                error_message="Orphaned transfer cleaned up"
            )

    def test_load_existing_transfers(self):
        """Test loading existing transfers from database."""
        # Mock database to return incomplete transfers
        mock_incomplete_transfers = [
            TransferState(
                unit_path="Movies/Existing Movie",
                src_disk="disk1",
                dest_disk="disk2",
                size_bytes=1024*1024,
                start_time=time.time() - 1800,
                completed=False
            )
        ]

        self.manager._get_incomplete_transfers_from_db = Mock(return_value=mock_incomplete_transfers)

        self.manager.load_existing_transfers()

        # Check that transfer was loaded into active transfers
        transfer_key = "disk1:Movies/Existing Movie"
        self.assertIn(transfer_key, self.manager._active_transfers)
        self.assertEqual(self.manager._active_transfers[transfer_key], mock_incomplete_transfers[0])

    def test_transfer_state_manager_without_database(self):
        """Test TransferStateManager functionality without database."""
        manager_no_db = TransferStateManager(None, "test_op")

        # Should work without errors
        transfer = manager_no_db.start_transfer("Movies/Test", "disk1", "disk2", 1024)
        self.assertIsInstance(transfer, TransferState)

        manager_no_db.complete_transfer(transfer, success=True)
        self.assertTrue(transfer.completed)

        # Should return empty lists when no database
        orphaned = manager_no_db.get_orphaned_transfers({"Movies/Test"})
        self.assertEqual(len(orphaned), 0)

        # Should not crash when loading existing transfers
        manager_no_db.load_existing_transfers()


class TestTransferStateIntegration(unittest.TestCase):
    """Integration tests for transfer state functionality."""

    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.db_path = Path(self.temp_dir) / "test_metrics.db"

    def tearDown(self):
        """Clean up test fixtures."""
        import shutil
        shutil.rmtree(self.temp_dir)

    def test_transfer_state_persistence(self):
        """Test that transfer states persist across manager instances."""
        operation_id = "test_persist_001"

        # Create first manager and start a transfer
        with patch('metrics_storage.MetricsDatabase') as mock_db_class:
            mock_db = Mock()
            mock_db_class.return_value = mock_db

            manager1 = TransferStateManager(mock_db, operation_id)
            transfer = manager1.start_transfer("Movies/Test Movie", "disk1", "disk2", 1024*1024)

            # Verify database store was called
            mock_db.store_transfer.assert_called()

            # Create second manager and check if it loads the transfer
            mock_db.get_incomplete_transfers.return_value = [
                {
                    'unit_path': "Movies/Test Movie",
                    'src_disk': "disk1",
                    'dest_disk': "disk2",
                    'size_bytes': 1024*1024,
                    'start_time': transfer.start_time,
                    'error_message': None
                }
            ]

            manager2 = TransferStateManager(mock_db, operation_id)
            manager2.load_existing_transfers()

            # Check that the transfer was loaded
            active_transfers = manager2.get_active_transfers()
            self.assertEqual(len(active_transfers), 1)
            self.assertEqual(active_transfers[0].unit_path, "Movies/Test Movie")


if __name__ == '__main__':
    unittest.main()