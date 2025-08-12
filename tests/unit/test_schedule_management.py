#!/usr/bin/env python3
"""
Unit tests for schedule management functionality.

Tests schedule creation, modification, deletion, status tracking,
and schedule lifecycle management.
"""

import unittest
import tempfile
import json
import time
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch, MagicMock

# Import scheduler components
try:
    from scheduler import (
        ScheduleConfig, ScheduleType, TriggerType, SchedulingEngine,
        ScheduleMonitor, ExecutionStatus, ScheduleExecution,
        ScheduleStatistics, ScheduleHealthMonitor, ErrorRecoveryManager,
        NotificationManager, NotificationConfig, FailureType, RetryConfig
    )
except ImportError:
    # Skip tests if scheduler module not available
    import sys
    print("Scheduler module not available - skipping schedule management tests")
    sys.exit(0)


class TestScheduleManagement(unittest.TestCase):
    """Test schedule management functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.engine = SchedulingEngine()
        self.monitor = ScheduleMonitor()
        
        # Create test schedules
        self.daily_schedule = ScheduleConfig(
            name="daily_rebalance",
            schedule_type=ScheduleType.CRON,
            cron_expression="0 2 * * *",
            command=["python", "unraid_rebalancer.py", "--target", "80"]
        )
        
        self.weekly_schedule = ScheduleConfig(
            name="weekly_cleanup",
            schedule_type=ScheduleType.CRON,
            cron_expression="0 3 * * 0",
            command=["python", "unraid_rebalancer.py", "--cleanup"]
        )
        
    def tearDown(self):
        """Clean up test fixtures."""
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_schedule_creation(self):
        """Test creating new schedules."""
        result = self.engine.create_schedule(self.daily_schedule)
        self.assertTrue(result)
        
        # Verify schedule was added
        schedules = self.engine.list_schedules()
        schedule_names = [s.name for s in schedules]
        self.assertIn("daily_rebalance", schedule_names)
    
    def test_schedule_modification(self):
        """Test modifying existing schedules."""
        # Create initial schedule
        self.engine.create_schedule(self.daily_schedule)
        
        # Modify schedule
        modified_schedule = ScheduleConfig(
            name="daily_rebalance",
            schedule_type=ScheduleType.CRON,
            cron_expression="0 3 * * *",  # Changed from 2 AM to 3 AM
            command=["python", "unraid_rebalancer.py", "--target", "85"]  # Changed target
        )
        
        result = self.engine.update_schedule(modified_schedule)
        self.assertTrue(result)
        
        # Verify changes
        updated_schedule = self.engine.get_schedule("daily_rebalance")
        self.assertEqual(updated_schedule.cron_expression, "0 3 * * *")
        self.assertIn("--target", updated_schedule.command)
        self.assertIn("85", updated_schedule.command)
    
    def test_schedule_deletion(self):
        """Test deleting schedules."""
        # Create schedule
        self.engine.create_schedule(self.daily_schedule)
        
        # Verify it exists
        schedules = self.engine.list_schedules()
        self.assertEqual(len(schedules), 1)
        
        # Delete schedule
        result = self.engine.delete_schedule("daily_rebalance")
        self.assertTrue(result)
        
        # Verify it's gone
        schedules = self.engine.list_schedules()
        self.assertEqual(len(schedules), 0)
    
    def test_schedule_enabling_disabling(self):
        """Test enabling and disabling schedules."""
        # Create enabled schedule
        self.engine.create_schedule(self.daily_schedule)
        
        # Disable schedule
        result = self.engine.disable_schedule("daily_rebalance")
        self.assertTrue(result)
        
        schedule = self.engine.get_schedule("daily_rebalance")
        self.assertFalse(schedule.enabled)
        
        # Re-enable schedule
        result = self.engine.enable_schedule("daily_rebalance")
        self.assertTrue(result)
        
        schedule = self.engine.get_schedule("daily_rebalance")
        self.assertTrue(schedule.enabled)
    
    def test_schedule_listing_and_filtering(self):
        """Test listing and filtering schedules."""
        # Create multiple schedules
        self.engine.create_schedule(self.daily_schedule)
        self.engine.create_schedule(self.weekly_schedule)
        
        # List all schedules
        all_schedules = self.engine.list_schedules()
        self.assertEqual(len(all_schedules), 2)
        
        # Filter by enabled status
        self.engine.disable_schedule("weekly_cleanup")
        enabled_schedules = self.engine.list_schedules(enabled_only=True)
        self.assertEqual(len(enabled_schedules), 1)
        self.assertEqual(enabled_schedules[0].name, "daily_rebalance")
        
        # Filter by schedule type
        cron_schedules = self.engine.list_schedules(schedule_type=ScheduleType.CRON)
        self.assertEqual(len(cron_schedules), 2)
    
    def test_schedule_validation(self):
        """Test schedule validation during management operations."""
        # Try to create invalid schedule
        invalid_schedule = ScheduleConfig(
            name="invalid",
            schedule_type=ScheduleType.CRON,
            cron_expression="invalid expression",
            command=[]
        )
        
        result = self.engine.create_schedule(invalid_schedule)
        self.assertFalse(result)
        
        # Verify it wasn't added
        schedules = self.engine.list_schedules()
        schedule_names = [s.name for s in schedules]
        self.assertNotIn("invalid", schedule_names)
    
    def test_schedule_conflict_detection(self):
        """Test detection of schedule conflicts."""
        # Create first schedule
        self.engine.create_schedule(self.daily_schedule)
        
        # Try to create conflicting schedule (same time)
        conflicting_schedule = ScheduleConfig(
            name="conflicting_schedule",
            schedule_type=ScheduleType.CRON,
            cron_expression="0 2 * * *",  # Same time as daily_schedule
            command=["echo", "conflict"]
        )
        
        # Should detect conflict
        has_conflict = self.engine.check_schedule_conflicts(conflicting_schedule)
        self.assertTrue(has_conflict)
        
        # Different time should not conflict
        non_conflicting_schedule = ScheduleConfig(
            name="non_conflicting",
            schedule_type=ScheduleType.CRON,
            cron_expression="0 4 * * *",  # Different time
            command=["echo", "no conflict"]
        )
        
        has_conflict = self.engine.check_schedule_conflicts(non_conflicting_schedule)
        self.assertFalse(has_conflict)


class TestScheduleExecution(unittest.TestCase):
    """Test schedule execution functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.engine = SchedulingEngine()
        self.monitor = ScheduleMonitor()
        
        self.test_schedule = ScheduleConfig(
            name="test_execution",
            schedule_type=ScheduleType.CRON,
            cron_expression="0 2 * * *",
            command=["echo", "test execution"]
        )
    
    @patch('subprocess.run')
    def test_schedule_execution_success(self, mock_run):
        """Test successful schedule execution."""
        # Mock successful command execution
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout="test execution\n",
            stderr=""
        )
        
        execution = self.engine.execute_schedule(self.test_schedule)
        
        self.assertEqual(execution.status, ExecutionStatus.SUCCESS)
        self.assertIsNotNone(execution.start_time)
        self.assertIsNotNone(execution.end_time)
        self.assertEqual(execution.exit_code, 0)
    
    @patch('subprocess.run')
    def test_schedule_execution_failure(self, mock_run):
        """Test failed schedule execution."""
        # Mock failed command execution
        mock_run.return_value = MagicMock(
            returncode=1,
            stdout="",
            stderr="Command failed"
        )
        
        execution = self.engine.execute_schedule(self.test_schedule)
        
        self.assertEqual(execution.status, ExecutionStatus.FAILED)
        self.assertEqual(execution.exit_code, 1)
        self.assertIn("Command failed", execution.error_message)
    
    def test_execution_timeout(self):
        """Test execution timeout handling."""
        # Create schedule with timeout
        timeout_schedule = ScheduleConfig(
            name="timeout_test",
            schedule_type=ScheduleType.CRON,
            cron_expression="0 2 * * *",
            command=["sleep", "10"],
            timeout_seconds=1  # Very short timeout
        )
        
        execution = self.engine.execute_schedule(timeout_schedule)
        
        self.assertEqual(execution.status, ExecutionStatus.TIMEOUT)
        self.assertIsNotNone(execution.error_message)
    
    def test_execution_history_tracking(self):
        """Test tracking of execution history."""
        # Execute schedule multiple times
        for i in range(3):
            execution = ScheduleExecution(
                schedule_name="test_execution",
                start_time=time.time() - (i * 3600),  # 1 hour apart
                end_time=time.time() - (i * 3600) + 60,  # 1 minute duration
                status=ExecutionStatus.SUCCESS,
                exit_code=0
            )
            self.monitor.record_execution(execution)
        
        # Get execution history
        history = self.monitor.get_execution_history("test_execution")
        self.assertEqual(len(history), 3)
        
        # Test filtering by status
        successful_executions = self.monitor.get_execution_history(
            "test_execution", 
            status=ExecutionStatus.SUCCESS
        )
        self.assertEqual(len(successful_executions), 3)
    
    def test_execution_statistics(self):
        """Test calculation of execution statistics."""
        # Record multiple executions with different outcomes
        executions = [
            ScheduleExecution(
                schedule_name="stats_test",
                start_time=time.time() - 3600,
                end_time=time.time() - 3540,  # 1 minute duration
                status=ExecutionStatus.SUCCESS,
                exit_code=0
            ),
            ScheduleExecution(
                schedule_name="stats_test",
                start_time=time.time() - 1800,
                end_time=time.time() - 1680,  # 2 minute duration
                status=ExecutionStatus.SUCCESS,
                exit_code=0
            ),
            ScheduleExecution(
                schedule_name="stats_test",
                start_time=time.time() - 900,
                end_time=time.time() - 900,  # Failed immediately
                status=ExecutionStatus.FAILED,
                exit_code=1
            )
        ]
        
        for execution in executions:
            self.monitor.record_execution(execution)
        
        stats = self.monitor.get_schedule_statistics("stats_test")
        
        self.assertEqual(stats.total_executions, 3)
        self.assertEqual(stats.successful_executions, 2)
        self.assertEqual(stats.failed_executions, 1)
        self.assertAlmostEqual(stats.success_rate, 66.67, places=1)
        self.assertGreater(stats.average_duration, 0)


class TestScheduleHealthMonitoring(unittest.TestCase):
    """Test schedule health monitoring functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.health_monitor = ScheduleHealthMonitor()
        self.monitor = ScheduleMonitor()
        
    def test_schedule_health_assessment(self):
        """Test assessment of schedule health."""
        # Create schedule with good health (recent successful executions)
        healthy_executions = [
            ScheduleExecution(
                schedule_name="healthy_schedule",
                start_time=time.time() - 3600,
                end_time=time.time() - 3540,
                status=ExecutionStatus.SUCCESS,
                exit_code=0
            ),
            ScheduleExecution(
                schedule_name="healthy_schedule",
                start_time=time.time() - 1800,
                end_time=time.time() - 1740,
                status=ExecutionStatus.SUCCESS,
                exit_code=0
            )
        ]
        
        for execution in healthy_executions:
            self.monitor.record_execution(execution)
        
        health = self.health_monitor.assess_schedule_health("healthy_schedule")
        self.assertEqual(health.status, "healthy")
        self.assertGreater(health.score, 80)
    
    def test_unhealthy_schedule_detection(self):
        """Test detection of unhealthy schedules."""
        # Create schedule with poor health (recent failures)
        unhealthy_executions = [
            ScheduleExecution(
                schedule_name="unhealthy_schedule",
                start_time=time.time() - 3600,
                end_time=time.time() - 3600,
                status=ExecutionStatus.FAILED,
                exit_code=1
            ),
            ScheduleExecution(
                schedule_name="unhealthy_schedule",
                start_time=time.time() - 1800,
                end_time=time.time() - 1800,
                status=ExecutionStatus.FAILED,
                exit_code=1
            )
        ]
        
        for execution in unhealthy_executions:
            self.monitor.record_execution(execution)
        
        health = self.health_monitor.assess_schedule_health("unhealthy_schedule")
        self.assertEqual(health.status, "unhealthy")
        self.assertLess(health.score, 50)
    
    def test_schedule_suspension_on_failures(self):
        """Test automatic schedule suspension on repeated failures."""
        schedule = ScheduleConfig(
            name="failure_prone",
            schedule_type=ScheduleType.CRON,
            cron_expression="0 2 * * *",
            command=["false"],  # Command that always fails
            max_consecutive_failures=3
        )
        
        engine = SchedulingEngine()
        engine.create_schedule(schedule)
        
        # Simulate multiple failures
        for i in range(4):  # One more than the limit
            execution = ScheduleExecution(
                schedule_name="failure_prone",
                start_time=time.time() - (i * 3600),
                end_time=time.time() - (i * 3600),
                status=ExecutionStatus.FAILED,
                exit_code=1
            )
            self.monitor.record_execution(execution)
        
        # Check if schedule was automatically suspended
        updated_schedule = engine.get_schedule("failure_prone")
        self.assertFalse(updated_schedule.enabled)


class TestErrorRecoveryManager(unittest.TestCase):
    """Test error recovery and retry functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.recovery_manager = ErrorRecoveryManager()
        
    def test_retry_configuration(self):
        """Test retry configuration setup."""
        retry_config = RetryConfig(
            max_attempts=3,
            initial_delay=60,
            backoff_multiplier=2.0,
            max_delay=300
        )
        
        self.assertEqual(retry_config.max_attempts, 3)
        self.assertEqual(retry_config.initial_delay, 60)
        self.assertEqual(retry_config.backoff_multiplier, 2.0)
        self.assertEqual(retry_config.max_delay, 300)
    
    def test_exponential_backoff_calculation(self):
        """Test exponential backoff delay calculation."""
        retry_config = RetryConfig(
            max_attempts=4,
            initial_delay=60,
            backoff_multiplier=2.0,
            max_delay=300
        )
        
        # Test delay calculation for each attempt
        delays = []
        for attempt in range(1, 5):
            delay = self.recovery_manager.calculate_retry_delay(retry_config, attempt)
            delays.append(delay)
        
        # Should be: 60, 120, 240, 300 (capped at max_delay)
        expected_delays = [60, 120, 240, 300]
        self.assertEqual(delays, expected_delays)
    
    def test_failure_type_classification(self):
        """Test classification of different failure types."""
        # Test transient failure
        transient_error = "Connection timeout"
        failure_type = self.recovery_manager.classify_failure(transient_error)
        self.assertEqual(failure_type, FailureType.TRANSIENT)
        
        # Test permanent failure
        permanent_error = "Command not found"
        failure_type = self.recovery_manager.classify_failure(permanent_error)
        self.assertEqual(failure_type, FailureType.PERMANENT)
        
        # Test resource failure
        resource_error = "Disk full"
        failure_type = self.recovery_manager.classify_failure(resource_error)
        self.assertEqual(failure_type, FailureType.RESOURCE)
    
    def test_retry_decision_making(self):
        """Test decision making for retry attempts."""
        retry_config = RetryConfig(max_attempts=3)
        
        # Should retry transient failures
        should_retry = self.recovery_manager.should_retry(
            FailureType.TRANSIENT, 
            attempt=1, 
            retry_config=retry_config
        )
        self.assertTrue(should_retry)
        
        # Should not retry permanent failures
        should_retry = self.recovery_manager.should_retry(
            FailureType.PERMANENT, 
            attempt=1, 
            retry_config=retry_config
        )
        self.assertFalse(should_retry)
        
        # Should not retry after max attempts
        should_retry = self.recovery_manager.should_retry(
            FailureType.TRANSIENT, 
            attempt=4, 
            retry_config=retry_config
        )
        self.assertFalse(should_retry)


if __name__ == '__main__':
    unittest.main()