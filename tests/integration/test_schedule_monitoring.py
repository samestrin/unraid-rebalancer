#!/usr/bin/env python3
"""
Test suite for schedule monitoring and control features.

Tests the ScheduleMonitor class and related monitoring functionality
including execution tracking, statistics, and emergency controls.
"""

import json
import os
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

# Add the parent directory to the path so we can import the modules
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from scheduler import (
    ScheduleMonitor, ScheduleExecution, ScheduleStatistics, ExecutionStatus,
    ScheduleConfig, ScheduleType, TriggerType
)


class TestScheduleMonitor(unittest.TestCase):
    """Test the ScheduleMonitor class."""
    
    def setUp(self):
        """Set up test environment."""
        self.temp_dir = tempfile.mkdtemp()
        self.monitor = ScheduleMonitor(self.temp_dir)
        
        # Create test schedule
        self.test_schedule_id = "test_schedule"
        
    def tearDown(self):
        """Clean up test environment."""
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_start_execution(self):
        """Test starting a new execution."""
        execution = self.monitor.start_execution(self.test_schedule_id, pid=12345)
        
        self.assertIsNotNone(execution.execution_id)
        self.assertEqual(execution.schedule_id, self.test_schedule_id)
        self.assertEqual(execution.status, ExecutionStatus.RUNNING)
        self.assertEqual(execution.pid, 12345)
        self.assertIsNotNone(execution.start_time)
        self.assertIsNone(execution.end_time)
        
        # Check it's in running executions
        running = self.monitor.get_running_executions()
        self.assertEqual(len(running), 1)
        self.assertEqual(running[0].execution_id, execution.execution_id)
    
    def test_complete_execution_success(self):
        """Test completing an execution successfully."""
        execution = self.monitor.start_execution(self.test_schedule_id)
        
        # Complete successfully
        success = self.monitor.complete_execution(
            execution.execution_id,
            exit_code=0,
            files_moved=10,
            bytes_moved=1024000
        )
        
        self.assertTrue(success)
        
        # Check it's no longer running
        running = self.monitor.get_running_executions()
        self.assertEqual(len(running), 0)
        
        # Check execution history
        history = self.monitor.get_execution_history(self.test_schedule_id)
        self.assertEqual(len(history), 1)
        completed = history[0]
        self.assertEqual(completed.status, ExecutionStatus.COMPLETED)
        self.assertEqual(completed.files_moved, 10)
        self.assertEqual(completed.bytes_moved, 1024000)
        self.assertIsNotNone(completed.end_time)
    
    def test_complete_execution_failure(self):
        """Test completing an execution with failure."""
        execution = self.monitor.start_execution(self.test_schedule_id)
        
        # Complete with failure
        success = self.monitor.complete_execution(
            execution.execution_id,
            exit_code=1,
            error_message="Test error"
        )
        
        self.assertTrue(success)
        
        # Check execution history
        history = self.monitor.get_execution_history(self.test_schedule_id)
        self.assertEqual(len(history), 1)
        failed = history[0]
        self.assertEqual(failed.status, ExecutionStatus.FAILED)
        self.assertEqual(failed.error_message, "Test error")
    
    def test_cancel_execution(self):
        """Test cancelling a running execution."""
        execution = self.monitor.start_execution(self.test_schedule_id, pid=12345)
        
        # Mock killing the process
        with patch('os.kill') as mock_kill:
            success = self.monitor.cancel_execution(execution.execution_id, "Test cancellation")
            
            self.assertTrue(success)
            # Check that os.kill was called with the correct PID (may be called multiple times)
            self.assertGreater(mock_kill.call_count, 0)
            # Check that the first call was with the correct PID
            first_call = mock_kill.call_args_list[0]
            self.assertEqual(first_call[0][0], 12345)  # First argument should be PID
        
        # Check it's no longer running
        running = self.monitor.get_running_executions()
        self.assertEqual(len(running), 0)
        
        # Check execution history
        history = self.monitor.get_execution_history(self.test_schedule_id)
        self.assertEqual(len(history), 1)
        cancelled = history[0]
        self.assertEqual(cancelled.status, ExecutionStatus.CANCELLED)
        self.assertEqual(cancelled.error_message, "Test cancellation")
    
    def test_suspend_and_resume_schedule(self):
        """Test suspending and resuming a schedule."""
        # Start an execution
        execution = self.monitor.start_execution(self.test_schedule_id, pid=12345)
        
        # Suspend the schedule
        with patch('os.kill') as mock_kill:
            success = self.monitor.suspend_schedule(self.test_schedule_id, "Test suspension")
            self.assertTrue(success)
            # Check that os.kill was called with the correct PID (may be called multiple times)
            self.assertGreater(mock_kill.call_count, 0)
            # Check that the first call was with the correct PID
            first_call = mock_kill.call_args_list[0]
            self.assertEqual(first_call[0][0], 12345)  # First argument should be PID
        
        # Check execution was cancelled
        running = self.monitor.get_running_executions()
        self.assertEqual(len(running), 0)
        
        # Resume the schedule
        success = self.monitor.resume_schedule(self.test_schedule_id)
        self.assertTrue(success)
    
    def test_get_schedule_statistics(self):
        """Test getting schedule statistics."""
        # Create multiple executions
        exec1 = self.monitor.start_execution(self.test_schedule_id)
        time.sleep(0.1)  # Small delay to ensure different timestamps
        self.monitor.complete_execution(exec1.execution_id, exit_code=0, files_moved=5, bytes_moved=500000)
        
        exec2 = self.monitor.start_execution(self.test_schedule_id)
        time.sleep(0.1)
        self.monitor.complete_execution(exec2.execution_id, exit_code=1, error_message="Test failure")
        
        exec3 = self.monitor.start_execution(self.test_schedule_id)
        time.sleep(0.1)
        self.monitor.complete_execution(exec3.execution_id, exit_code=0, files_moved=3, bytes_moved=300000)
        
        # Get statistics
        stats = self.monitor.get_schedule_statistics(self.test_schedule_id)
        
        self.assertIsNotNone(stats)
        self.assertEqual(stats.schedule_id, self.test_schedule_id)
        self.assertEqual(stats.total_executions, 3)
        self.assertEqual(stats.successful_executions, 2)
        self.assertEqual(stats.failed_executions, 1)
        self.assertAlmostEqual(stats.success_rate, 66.67, places=1)  # 2/3 * 100
        self.assertEqual(stats.total_files_moved, 8)  # 5 + 3
        self.assertEqual(stats.total_bytes_moved, 800000)  # 500000 + 300000
        self.assertIsNotNone(stats.last_execution_time)
        self.assertIsNotNone(stats.last_success_time)
        self.assertIsNotNone(stats.last_failure_time)
    
    def test_cleanup_old_executions(self):
        """Test cleaning up old execution records."""
        # Create executions with different timestamps
        old_time = time.time() - (40 * 24 * 60 * 60)  # 40 days ago
        recent_time = time.time() - (10 * 24 * 60 * 60)  # 10 days ago
        
        # Create old execution
        old_execution = ScheduleExecution(
            execution_id="old_exec",
            schedule_id=self.test_schedule_id,
            start_time=old_time,
            end_time=old_time + 3600,
            status=ExecutionStatus.COMPLETED
        )
        self.monitor._save_execution(old_execution)
        
        # Create recent execution
        recent_execution = ScheduleExecution(
            execution_id="recent_exec",
            schedule_id=self.test_schedule_id,
            start_time=recent_time,
            end_time=recent_time + 3600,
            status=ExecutionStatus.COMPLETED
        )
        self.monitor._save_execution(recent_execution)
        
        # Cleanup executions older than 30 days
        cleaned = self.monitor.cleanup_old_executions(30)
        self.assertEqual(cleaned, 1)
        
        # Check only recent execution remains
        history = self.monitor.get_execution_history(self.test_schedule_id)
        self.assertEqual(len(history), 1)
        self.assertEqual(history[0].execution_id, "recent_exec")
    
    def test_execution_serialization(self):
        """Test execution serialization and deserialization."""
        execution = ScheduleExecution(
            execution_id="test_exec",
            schedule_id=self.test_schedule_id,
            start_time=time.time(),
            status=ExecutionStatus.RUNNING,
            files_moved=5,
            bytes_moved=1024000,
            pid=12345
        )
        
        # Test to_dict
        data = execution.to_dict()
        self.assertIsInstance(data, dict)
        self.assertEqual(data['execution_id'], "test_exec")
        self.assertEqual(data['schedule_id'], self.test_schedule_id)
        self.assertEqual(data['status'], "running")
        self.assertEqual(data['files_moved'], 5)
        self.assertEqual(data['bytes_moved'], 1024000)
        self.assertEqual(data['pid'], 12345)
        
        # Test from_dict
        restored = ScheduleExecution.from_dict(data)
        self.assertEqual(restored.execution_id, execution.execution_id)
        self.assertEqual(restored.schedule_id, execution.schedule_id)
        self.assertEqual(restored.status, execution.status)
        self.assertEqual(restored.files_moved, execution.files_moved)
        self.assertEqual(restored.bytes_moved, execution.bytes_moved)
        self.assertEqual(restored.pid, execution.pid)
    
    def test_statistics_serialization(self):
        """Test statistics serialization and deserialization."""
        stats = ScheduleStatistics(
            schedule_id=self.test_schedule_id,
            total_executions=10,
            successful_executions=8,
            failed_executions=2,
            total_files_moved=100,
            total_bytes_moved=10240000
        )
        
        # Test to_dict
        data = stats.to_dict()
        self.assertIsInstance(data, dict)
        self.assertEqual(data['schedule_id'], self.test_schedule_id)
        self.assertEqual(data['total_executions'], 10)
        self.assertEqual(data['successful_executions'], 8)
        self.assertEqual(data['failed_executions'], 2)
        # success_rate is a property, not stored in serialized data
        self.assertNotIn('success_rate', data)
        # But the property should work on the object
        self.assertAlmostEqual(stats.success_rate, 80.0, places=1)
        
        # Test from_dict
        restored = ScheduleStatistics.from_dict(data)
        self.assertEqual(restored.schedule_id, stats.schedule_id)
        self.assertEqual(restored.total_executions, stats.total_executions)
        self.assertEqual(restored.successful_executions, stats.successful_executions)
        self.assertEqual(restored.failed_executions, stats.failed_executions)
        self.assertAlmostEqual(restored.success_rate, 80.0, places=1)


class TestScheduleMonitorIntegration(unittest.TestCase):
    """Integration tests for schedule monitoring."""
    
    def setUp(self):
        """Set up test environment."""
        self.temp_dir = tempfile.mkdtemp()
        self.monitor = ScheduleMonitor(self.temp_dir)
    
    def tearDown(self):
        """Clean up test environment."""
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_multiple_schedules_monitoring(self):
        """Test monitoring multiple schedules simultaneously."""
        schedule1_id = "schedule1"
        schedule2_id = "schedule2"
        
        # Start executions for both schedules
        exec1 = self.monitor.start_execution(schedule1_id, pid=11111)
        exec2 = self.monitor.start_execution(schedule2_id, pid=22222)
        
        # Check both are running
        running = self.monitor.get_running_executions()
        self.assertEqual(len(running), 2)
        
        # Complete one, cancel the other
        self.monitor.complete_execution(exec1.execution_id, exit_code=0)
        
        with patch('os.kill'):
            self.monitor.cancel_execution(exec2.execution_id, "Test cancellation")
        
        # Check no executions running
        running = self.monitor.get_running_executions()
        self.assertEqual(len(running), 0)
        
        # Check individual histories
        history1 = self.monitor.get_execution_history(schedule1_id)
        history2 = self.monitor.get_execution_history(schedule2_id)
        
        self.assertEqual(len(history1), 1)
        self.assertEqual(len(history2), 1)
        self.assertEqual(history1[0].status, ExecutionStatus.COMPLETED)
        self.assertEqual(history2[0].status, ExecutionStatus.CANCELLED)
    
    def test_emergency_stop_scenario(self):
        """Test emergency stop functionality."""
        # Start multiple executions
        exec1 = self.monitor.start_execution("schedule1", pid=11111)
        exec2 = self.monitor.start_execution("schedule2", pid=22222)
        exec3 = self.monitor.start_execution("schedule3", pid=33333)
        
        # Verify all are running
        running = self.monitor.get_running_executions()
        self.assertEqual(len(running), 3)
        
        # Simulate emergency stop by cancelling all
        with patch('os.kill') as mock_kill:
            for execution in running:
                self.monitor.cancel_execution(execution.execution_id, "Emergency stop")
            
            # Verify all processes were killed (may be called multiple times due to signal handling)
            self.assertGreaterEqual(mock_kill.call_count, 3)
        
        # Verify no executions are running
        running = self.monitor.get_running_executions()
        self.assertEqual(len(running), 0)
        
        # Verify all executions are marked as cancelled
        all_history = self.monitor.get_execution_history()
        self.assertEqual(len(all_history), 3)
        for execution in all_history:
            self.assertEqual(execution.status, ExecutionStatus.CANCELLED)
            self.assertEqual(execution.error_message, "Emergency stop")


if __name__ == '__main__':
    unittest.main()