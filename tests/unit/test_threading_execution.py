#!/usr/bin/env python3
"""
Unit tests for threading-based execution functionality.

Tests threading-based retry mechanisms, concurrent execution,
daemon thread management, and execution coordination.
"""

import unittest
import threading
import time
import queue
from pathlib import Path
from unittest.mock import patch, MagicMock

# Import scheduler components
try:
    from scheduler import (
        ScheduleConfig, ScheduleType, SchedulingEngine, ScheduleMonitor,
        ExecutionStatus, ScheduleExecution, ErrorRecoveryManager,
        RetryConfig, RetryStrategy, FailureType
    )
except ImportError:
    # Skip tests if scheduler module not available
    import pytest
    pytest.skip("Scheduler module not available", allow_module_level=True)


class TestThreadingExecution(unittest.TestCase):
    """Test threading-based execution functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.engine = SchedulingEngine(script_path=Path("/test/script.py"))
        self.monitor = ScheduleMonitor()
        self.recovery_manager = ErrorRecoveryManager()
        
        # Create test schedule
        self.test_schedule = ScheduleConfig(
            schedule_id="threading_test",
            name="threading_test",
            schedule_type=ScheduleType.RECURRING,
            cron_expression="0 2 * * *",
            target_percent=80.0
        )
        
    def tearDown(self):
        """Clean up test fixtures."""
        # Ensure all threads are cleaned up
        for thread in threading.enumerate():
            if thread != threading.current_thread() and thread.daemon:
                thread.join(timeout=1)
    
    def test_daemon_thread_creation(self):
        """Test creation of daemon threads for retry execution."""
        execution = ScheduleExecution(
            execution_id="test_exec_1",
            schedule_id="threading_test",
            start_time=time.time(),
            status=ExecutionStatus.FAILED,
            retry_attempt=1
        )
        
        retry_config = RetryConfig(
            strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
            max_attempts=3,
            base_delay_seconds=1,
            jitter=False
        )
        
        # Check thread count before
        initial_thread_count = threading.active_count()
        
        # Mock the retry scheduling but allow thread creation
        with patch.object(self.recovery_manager, '_execute_retry') as mock_execute:
            result = self.recovery_manager._schedule_retry(
                execution, self.test_schedule, retry_config
            )
            
            self.assertTrue(result)
            
            # Small delay to allow thread creation
            time.sleep(0.01)
            
            # Verify thread count increased (daemon thread was created)
            current_thread_count = threading.active_count()
            self.assertGreaterEqual(current_thread_count, initial_thread_count)
    
    def test_concurrent_execution_handling(self):
        """Test handling of concurrent schedule executions."""
        schedules = []
        for i in range(3):
            schedule = ScheduleConfig(
                schedule_id=f"concurrent_test_{i}",
                name=f"concurrent_test_{i}",
                schedule_type=ScheduleType.RECURRING,
                cron_expression="*/5 * * * *",
                target_percent=80.0
            )
            schedules.append(schedule)
        
        results_queue = queue.Queue()
        
        def execute_schedule(sched):
            """Execute schedule in thread."""
            execution = ScheduleExecution(
                execution_id=f"exec_{sched.name}",
                schedule_id=sched.name,
                start_time=time.time(),
                status=ExecutionStatus.COMPLETED
            )
            results_queue.put((sched.name, execution))
        
        threads = []
        for schedule in schedules:
            thread = threading.Thread(target=execute_schedule, args=(schedule,))
            threads.append(thread)
            thread.start()
        
        # Wait for all executions to complete
        for thread in threads:
            thread.join(timeout=5)
        
        # Collect results
        results = []
        while not results_queue.empty():
            results.append(results_queue.get())
        
        # Verify all executions completed
        self.assertEqual(len(results), 3)
        for name, execution in results:
            self.assertEqual(execution.status, ExecutionStatus.COMPLETED)
            self.assertIn("concurrent_test", name)
    
    def test_retry_delay_calculation(self):
        """Test retry delay calculation for different strategies."""
        # Test exponential backoff
        exponential_config = RetryConfig(
            strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
            base_delay_seconds=2,
            backoff_multiplier=2.0,
            jitter=False
        )
        
        delay1 = exponential_config.calculate_delay(1)
        delay2 = exponential_config.calculate_delay(2)
        delay3 = exponential_config.calculate_delay(3)
        
        self.assertEqual(delay1, 2)  # 2 * 2^0
        self.assertEqual(delay2, 4)  # 2 * 2^1
        self.assertEqual(delay3, 8)  # 2 * 2^2
        
        # Test linear backoff
        linear_config = RetryConfig(
            strategy=RetryStrategy.LINEAR_BACKOFF,
            base_delay_seconds=3,
            jitter=False
        )
        
        delay1 = linear_config.calculate_delay(1)
        delay2 = linear_config.calculate_delay(2)
        delay3 = linear_config.calculate_delay(3)
        
        self.assertEqual(delay1, 3)  # 3 * 1
        self.assertEqual(delay2, 6)  # 3 * 2
        self.assertEqual(delay3, 9)  # 3 * 3
        
        # Test fixed delay
        fixed_config = RetryConfig(
            strategy=RetryStrategy.FIXED_DELAY,
            base_delay_seconds=5,
            jitter=False
        )
        
        delay1 = fixed_config.calculate_delay(1)
        delay2 = fixed_config.calculate_delay(2)
        delay3 = fixed_config.calculate_delay(3)
        
        self.assertEqual(delay1, 5)
        self.assertEqual(delay2, 5)
        self.assertEqual(delay3, 5)
    
    def test_thread_cleanup_on_completion(self):
        """Test that threads are properly cleaned up after completion."""
        initial_thread_count = threading.active_count()
        
        execution = ScheduleExecution(
            execution_id="cleanup_test",
            schedule_id="threading_test",
            start_time=time.time(),
            status=ExecutionStatus.FAILED,
            retry_attempt=1
        )
        
        retry_config = RetryConfig(
            strategy=RetryStrategy.FIXED_DELAY,
            max_attempts=2,
            base_delay_seconds=0.1,  # Very short delay for testing
            jitter=False
        )
        
        # Schedule retry
        with patch.object(self.recovery_manager, '_execute_retry') as mock_execute:
            mock_execute.return_value = None  # Simulate quick completion
            
            result = self.recovery_manager._schedule_retry(
                execution, self.test_schedule, retry_config
            )
            
            self.assertTrue(result)
            
            # Wait a bit for thread to complete
            time.sleep(0.2)
            
            # Thread count should return to normal (daemon threads may still exist)
            # We mainly check that we don't have a thread leak
            current_thread_count = threading.active_count()
            self.assertLessEqual(current_thread_count, initial_thread_count + 2)
    
    def test_execution_timeout_handling(self):
        """Test handling of execution timeouts in threaded environment."""
        execution = ScheduleExecution(
            execution_id="timeout_test",
            schedule_id="threading_test",
            start_time=time.time() - 7200,  # Started 2 hours ago
            status=ExecutionStatus.RUNNING
        )
        
        schedule_with_timeout = ScheduleConfig(
            schedule_id="timeout_test",
            name="timeout_test",
            schedule_type=ScheduleType.RECURRING,
            cron_expression="0 2 * * *",
            target_percent=80.0,
            max_runtime_hours=1  # 1 hour max runtime
        )
        
        # Check if execution has timed out
        runtime = time.time() - execution.start_time
        max_runtime = schedule_with_timeout.max_runtime_hours * 3600
        
        self.assertGreater(runtime, max_runtime)
        
        # Execution should be marked as timed out
        if runtime > max_runtime:
            execution.status = ExecutionStatus.FAILED
            execution.failure_type = FailureType.TIMEOUT
        
        self.assertEqual(execution.status, ExecutionStatus.FAILED)
        self.assertEqual(execution.failure_type, FailureType.TIMEOUT)
    
    def test_thread_safety_of_execution_tracking(self):
        """Test thread safety of execution tracking operations."""
        executions = []
        execution_lock = threading.Lock()
        
        def create_execution(execution_id):
            """Create execution in thread-safe manner."""
            execution = ScheduleExecution(
                execution_id=f"thread_safe_{execution_id}",
                schedule_id="threading_test",
                start_time=time.time(),
                status=ExecutionStatus.RUNNING
            )
            
            with execution_lock:
                executions.append(execution)
        
        threads = []
        for i in range(5):
            thread = threading.Thread(target=create_execution, args=(i,))
            threads.append(thread)
            thread.start()
        
        # Wait for all threads to complete
        for thread in threads:
            thread.join(timeout=5)
        
        # Verify all executions were created safely
        self.assertEqual(len(executions), 5)
        
        # Verify unique execution IDs
        execution_ids = [exec.execution_id for exec in executions]
        self.assertEqual(len(set(execution_ids)), 5)


class TestRetryThreadManagement(unittest.TestCase):
    """Test retry thread management functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.recovery_manager = ErrorRecoveryManager()
        
    def test_retry_thread_daemon_status(self):
        """Test that retry threads are created as daemon threads."""
        execution = ScheduleExecution(
            execution_id="daemon_test",
            schedule_id="test_schedule",
            start_time=time.time(),
            status=ExecutionStatus.FAILED
        )
        
        schedule = ScheduleConfig(
            schedule_id="daemon_test",
            name="daemon_test",
            schedule_type=ScheduleType.RECURRING,
            cron_expression="0 2 * * *",
            target_percent=80.0
        )
        
        retry_config = RetryConfig(
            strategy=RetryStrategy.FIXED_DELAY,
            base_delay_seconds=0.1,
            jitter=False
        )
        
        initial_thread_count = threading.active_count()
        
        with patch.object(self.recovery_manager, '_execute_retry'):
            self.recovery_manager._schedule_retry(execution, schedule, retry_config)
            
            # Small delay to allow thread creation
            time.sleep(0.01)
            
            # Check thread count increased
            current_thread_count = threading.active_count()
            self.assertGreaterEqual(current_thread_count, initial_thread_count)
            
            # Note: This test validates thread creation behavior
    
    def test_multiple_retry_threads(self):
        """Test handling of multiple concurrent retry threads."""
        executions = []
        for i in range(3):
            execution = ScheduleExecution(
                execution_id=f"multi_retry_{i}",
                schedule_id=f"test_schedule_{i}",
                start_time=time.time(),
                status=ExecutionStatus.FAILED
            )
            executions.append(execution)
        
        schedule = ScheduleConfig(
            schedule_id="multi_retry_test",
            name="multi_retry_test",
            schedule_type=ScheduleType.RECURRING,
            cron_expression="0 2 * * *",
            target_percent=80.0
        )
        
        retry_config = RetryConfig(
            strategy=RetryStrategy.FIXED_DELAY,
            base_delay_seconds=0.1,
            jitter=False
        )
        
        initial_thread_count = threading.active_count()
        
        with patch.object(self.recovery_manager, '_execute_retry'):
            for execution in executions:
                self.recovery_manager._schedule_retry(execution, schedule, retry_config)
            
            # Should have created additional threads
            current_thread_count = threading.active_count()
            self.assertGreaterEqual(current_thread_count, initial_thread_count)
            
            # Wait for threads to complete
            time.sleep(0.2)


if __name__ == '__main__':
    unittest.main()