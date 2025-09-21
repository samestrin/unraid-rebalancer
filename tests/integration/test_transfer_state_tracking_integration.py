"""Integration tests for transfer state tracking functionality."""

import unittest
import time
import tempfile
import os
import json
from unittest.mock import Mock, patch, MagicMock
from pathlib import Path

import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from unraid_rebalancer import (
    TransferState, TransferStateManager, PerformanceMonitor,
    Disk, Unit, Move, Plan, build_plan
)
from metrics_storage import MetricsDatabase


class TestTransferStateIntegration(unittest.TestCase):
    """Integration tests for transfer state tracking with existing systems."""

    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.db_path = Path(self.temp_dir) / "test_metrics.db"

        # Create test disks
        self.disks = [
            Disk(name="disk1", path=Path("/mnt/disk1"), size_bytes=1000*1024*1024*1024,
                 used_bytes=800*1024*1024*1024, free_bytes=200*1024*1024*1024),
            Disk(name="disk2", path=Path("/mnt/disk2"), size_bytes=1000*1024*1024*1024,
                 used_bytes=300*1024*1024*1024, free_bytes=700*1024*1024*1024),
            Disk(name="disk3", path=Path("/mnt/disk3"), size_bytes=1000*1024*1024*1024,
                 used_bytes=600*1024*1024*1024, free_bytes=400*1024*1024*1024)
        ]

        # Create test units
        self.units = [
            Unit(share="Movies", rel_path="Movie1 (2023)", size_bytes=50*1024*1024*1024, src_disk="disk1"),
            Unit(share="Movies", rel_path="Movie2 (2022)", size_bytes=40*1024*1024*1024, src_disk="disk1"),
            Unit(share="Shows", rel_path="Show1/Season1", size_bytes=30*1024*1024*1024, src_disk="disk1")
        ]

    def tearDown(self):
        """Clean up test fixtures."""
        import shutil
        shutil.rmtree(self.temp_dir)

    def test_performance_monitor_transfer_state_integration(self):
        """Test PerformanceMonitor integration with transfer state tracking."""
        operation_id = "test_integration_001"

        with patch('unraid_rebalancer.MetricsDatabase') as mock_db_class:
            mock_db = Mock()
            mock_db_class.return_value = mock_db

            # Create PerformanceMonitor with transfer state tracking
            monitor = PerformanceMonitor(
                operation_id=operation_id,
                metrics_enabled=True,
                database_path=self.db_path
            )

            # Verify transfer manager was initialized
            self.assertIsNotNone(monitor.transfer_manager)
            self.assertEqual(monitor.transfer_manager.operation_id, operation_id)
            self.assertEqual(monitor.transfer_manager.database, mock_db)

            # Test starting transfer state tracking
            unit = self.units[0]
            dest_disk = "disk2"

            transfer_state = monitor.start_transfer_state_tracking(unit, dest_disk)

            self.assertIsInstance(transfer_state, TransferState)
            self.assertEqual(transfer_state.unit_path, f"{unit.share}/{unit.rel_path}")
            self.assertEqual(transfer_state.src_disk, unit.src_disk)
            self.assertEqual(transfer_state.dest_disk, dest_disk)
            self.assertEqual(transfer_state.size_bytes, unit.size_bytes)
            self.assertTrue(transfer_state.is_in_progress())

            # Test completing transfer state tracking
            monitor.complete_transfer_state_tracking(transfer_state, success=True)

            self.assertTrue(transfer_state.completed)
            self.assertIsNone(transfer_state.error_message)

            # Verify database interactions
            mock_db.store_transfer.assert_called()

    def test_build_plan_with_transfer_awareness(self):
        """Test build_plan function with transfer state awareness."""
        operation_id = "test_plan_001"

        with patch('metrics_storage.MetricsDatabase') as mock_db_class:
            mock_db = Mock()
            mock_db_class.return_value = mock_db

            # Create transfer manager with some orphaned transfers
            transfer_manager = TransferStateManager(mock_db, operation_id)

            # Mock orphaned transfers that are not in current plan
            orphaned_transfer = TransferState(
                unit_path="Movies/Orphaned Movie (2020)",
                src_disk="disk1",
                dest_disk="disk2",
                size_bytes=25*1024*1024*1024,
                start_time=time.time() - 3600,
                completed=False
            )

            with patch.object(transfer_manager, 'get_orphaned_transfers') as mock_get_orphaned:
                with patch.object(transfer_manager, 'cleanup_orphaned_transfers') as mock_cleanup:
                    mock_get_orphaned.return_value = [orphaned_transfer]

                    # Build plan with transfer awareness
                    plan = build_plan(
                        disks=self.disks,
                        units=self.units,
                        target_percent=70.0,
                        headroom_percent=10.0,
                        strategy='size',
                        transfer_manager=transfer_manager
                    )

                    # Verify orphaned transfer detection was called
                    current_plan_units = {f"{unit.share}/{unit.rel_path}" for unit in self.units}
                    mock_get_orphaned.assert_called_once_with(current_plan_units)

                    # Verify cleanup was called
                    mock_cleanup.assert_called_once_with([orphaned_transfer])

                    # Verify plan was still generated
                    self.assertIsInstance(plan, Plan)

    def test_build_plan_without_transfer_manager(self):
        """Test build_plan function works without transfer manager (backwards compatibility)."""
        # Build plan without transfer manager (existing behavior)
        plan = build_plan(
            disks=self.disks,
            units=self.units,
            target_percent=70.0,
            headroom_percent=10.0,
            strategy='size'
        )

        # Verify plan was generated normally
        self.assertIsInstance(plan, Plan)
        self.assertIsInstance(plan.moves, list)

    def test_transfer_state_workflow_with_interruption(self):
        """Test complete transfer state workflow with simulated interruption."""
        operation_id = "test_workflow_001"

        with patch('unraid_rebalancer.MetricsDatabase') as mock_db_class:
            mock_db = Mock()
            mock_db_class.return_value = mock_db

            # Phase 1: Start operation and track transfers
            monitor1 = PerformanceMonitor(
                operation_id=operation_id,
                metrics_enabled=True,
                database_path=self.db_path
            )

            # Start tracking multiple transfers
            transfer1 = monitor1.start_transfer_state_tracking(self.units[0], "disk2")
            transfer2 = monitor1.start_transfer_state_tracking(self.units[1], "disk3")

            # Complete one transfer
            monitor1.complete_transfer_state_tracking(transfer1, success=True)

            # Simulate interruption - second transfer remains incomplete

            # Phase 2: Resume operation (new monitor instance)
            mock_db.get_incomplete_transfers.return_value = [
                {
                    'unit_path': f"{self.units[1].share}/{self.units[1].rel_path}",
                    'src_disk': self.units[1].src_disk,
                    'dest_disk': "disk3",
                    'size_bytes': self.units[1].size_bytes,
                    'start_time': transfer2.start_time,
                    'error_message': None
                }
            ]

            monitor2 = PerformanceMonitor(
                operation_id=operation_id,
                metrics_enabled=True,
                database_path=self.db_path
            )

            # Verify existing transfer was loaded
            active_transfers = monitor2.get_active_transfer_states()
            self.assertEqual(len(active_transfers), 1)
            self.assertEqual(active_transfers[0].unit_path, f"{self.units[1].share}/{self.units[1].rel_path}")

            # Complete the resumed transfer
            resumed_transfer = active_transfers[0]
            monitor2.complete_transfer_state_tracking(resumed_transfer, success=True)

            # Verify no more active transfers
            active_transfers = monitor2.get_active_transfer_states()
            self.assertEqual(len(active_transfers), 0)

    def test_orphaned_transfer_cleanup_during_planning(self):
        """Test orphaned transfer cleanup during plan generation."""
        operation_id = "test_cleanup_001"

        with patch('metrics_storage.MetricsDatabase') as mock_db_class:
            mock_db = Mock()
            mock_db_class.return_value = mock_db

            transfer_manager = TransferStateManager(mock_db, operation_id)

            # Create orphaned transfers (not in current plan)
            orphaned_transfers = [
                TransferState(
                    unit_path="Movies/Old Movie (2015)",
                    src_disk="disk1",
                    dest_disk="disk2",
                    size_bytes=20*1024*1024*1024,
                    start_time=time.time() - 7200,  # 2 hours ago
                    completed=False
                ),
                TransferState(
                    unit_path="Shows/Cancelled Show/Season1",
                    src_disk="disk3",
                    dest_disk="disk1",
                    size_bytes=15*1024*1024*1024,
                    start_time=time.time() - 3600,  # 1 hour ago
                    completed=False
                )
            ]

            with patch.object(transfer_manager, '_get_incomplete_transfers_from_db') as mock_get_incomplete:
                mock_get_incomplete.return_value = orphaned_transfers

                # Current plan only includes units from self.units
                current_plan_units = {f"{unit.share}/{unit.rel_path}" for unit in self.units}

                orphaned = transfer_manager.get_orphaned_transfers(current_plan_units)

                # Both transfers should be detected as orphaned
                self.assertEqual(len(orphaned), 2)
                self.assertIn(orphaned_transfers[0], orphaned)
                self.assertIn(orphaned_transfers[1], orphaned)

                # Test cleanup
                with patch.object(transfer_manager, 'complete_transfer') as mock_complete:
                    transfer_manager.cleanup_orphaned_transfers(orphaned)

                    # Verify complete_transfer was called for each orphaned transfer
                    self.assertEqual(mock_complete.call_count, 2)

                    # Verify calls were made with correct parameters
                    for call_args in mock_complete.call_args_list:
                        args, kwargs = call_args
                        transfer = args[0]
                        success = kwargs.get('success', args[1] if len(args) > 1 else False)
                        error_message = kwargs.get('error_message', args[2] if len(args) > 2 else None)
                        self.assertIn(transfer, orphaned_transfers)
                        self.assertFalse(success)
                        self.assertEqual(error_message, "Orphaned transfer cleaned up")

    def test_transfer_state_error_handling(self):
        """Test error handling in transfer state operations."""
        operation_id = "test_error_001"

        with patch('metrics_storage.MetricsDatabase') as mock_db_class:
            mock_db = Mock()
            mock_db.store_transfer.side_effect = Exception("Database error")
            mock_db_class.return_value = mock_db

            # Should not crash when database operations fail
            monitor = PerformanceMonitor(
                operation_id=operation_id,
                metrics_enabled=True,
                database_path=self.db_path
            )

            # Start transfer should still work despite database error
            unit = self.units[0]
            transfer_state = monitor.start_transfer_state_tracking(unit, "disk2")

            self.assertIsInstance(transfer_state, TransferState)
            self.assertTrue(transfer_state.is_in_progress())

            # Complete transfer should also work
            monitor.complete_transfer_state_tracking(transfer_state, success=False, error_message="Test failure")

            self.assertTrue(transfer_state.completed)
            self.assertEqual(transfer_state.error_message, "Test failure")

    def test_concurrent_transfer_state_operations(self):
        """Test concurrent transfer state operations."""
        operation_id = "test_concurrent_001"

        with patch('metrics_storage.MetricsDatabase') as mock_db_class:
            mock_db = Mock()
            mock_db_class.return_value = mock_db

            transfer_manager = TransferStateManager(mock_db, operation_id)

            # Simulate concurrent operations on the same transfer manager
            import threading

            results = []
            errors = []

            def start_transfer_worker(index):
                try:
                    unit = Unit(
                        share="Movies",
                        rel_path=f"Concurrent Movie {index}",
                        size_bytes=10*1024*1024*1024,
                        src_disk="disk1"
                    )
                    transfer = transfer_manager.start_transfer(
                        f"{unit.share}/{unit.rel_path}",
                        unit.src_disk,
                        "disk2",
                        unit.size_bytes
                    )
                    results.append(transfer)
                except Exception as e:
                    errors.append(e)

            # Start multiple threads
            threads = []
            for i in range(5):
                thread = threading.Thread(target=start_transfer_worker, args=(i,))
                threads.append(thread)
                thread.start()

            # Wait for all threads to complete
            for thread in threads:
                thread.join()

            # Verify no errors occurred
            self.assertEqual(len(errors), 0)

            # Verify all transfers were created
            self.assertEqual(len(results), 5)

            # Verify all transfers are unique
            unit_paths = [t.unit_path for t in results]
            self.assertEqual(len(set(unit_paths)), 5)

            # Verify all transfers are active
            active_transfers = transfer_manager.get_active_transfers()
            self.assertEqual(len(active_transfers), 5)


if __name__ == '__main__':
    unittest.main()