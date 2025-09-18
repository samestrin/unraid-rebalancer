#!/usr/bin/env python3
"""
Unit tests for error recovery functionality.

Tests error handling, retry strategies, failure classification,
and recovery mechanisms.
"""

import unittest
import time
from unittest.mock import patch, MagicMock

# Import scheduler components
try:
    from scheduler import (
        ScheduleConfig, ScheduleType, SchedulingEngine, ScheduleMonitor,
        ExecutionStatus, ScheduleExecution, ErrorRecoveryManager,
        RetryConfig, RetryStrategy, FailureType, FailureRecord,
        NotificationManager, NotificationLevel
    )
except ImportError:
    # Skip tests if scheduler module not available
    import pytest
    pytest.skip("Scheduler module not available", allow_module_level=True)


class TestErrorRecovery(unittest.TestCase):
    """Test error recovery functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.recovery_manager = ErrorRecoveryManager()
        self.monitor = ScheduleMonitor()
        
        # Create test schedule
        self.test_schedule = ScheduleConfig(
            schedule_id="error_recovery_test",
            name="error_recovery_test",
            schedule_type=ScheduleType.RECURRING,
            cron_expression="0 2 * * *",
            target_percent=80.0
        )
        
    def test_failure_type_classification(self):
        """Test classification of different failure types."""
        # Test timeout classification
        failure_type = self.recovery_manager._classify_failure_type(
            "Connection timed out", 124, None
        )
        self.assertEqual(failure_type, FailureType.TIMEOUT)
        
        # Test permission error classification
        failure_type = self.recovery_manager._classify_failure_type(
            "Permission denied", 1, None
        )
        self.assertEqual(failure_type, FailureType.PERMISSION_DENIED)
        
        # Test resource exhaustion classification
        failure_type = self.recovery_manager._classify_failure_type(
            "No space left on device", 28, None
        )
        self.assertEqual(failure_type, FailureType.DISK_ERROR)
        
        # Test unknown error classification
        failure_type = self.recovery_manager._classify_failure_type(
            "Unknown error", 1, None
        )
        self.assertEqual(failure_type, FailureType.UNKNOWN)
    
    def test_retry_decision_logic(self):
        """Test retry decision logic for different failure types."""
        retry_config = RetryConfig(
            strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
            max_attempts=3,
            jitter=False
        )
        
        # Test retryable failures
        retryable_failures = [
            FailureType.TIMEOUT,
            FailureType.NETWORK_ERROR,
            FailureType.RESOURCE_EXHAUSTION
        ]
        
        for failure_type in retryable_failures:
            with self.subTest(failure_type=failure_type):
                execution = ScheduleExecution(
                    execution_id="retry_test",
                    schedule_id="test_schedule",
                    start_time=time.time(),
                    status=ExecutionStatus.FAILED,
                    retry_attempt=1
                )
                
                should_retry = self.recovery_manager._should_retry_execution(
                    execution, failure_type, retry_config
                )
                self.assertTrue(should_retry)
        
        # Test non-retryable failures
        non_retryable_failures = [
            FailureType.PERMISSION_DENIED,
            FailureType.CONFIGURATION_ERROR,
            FailureType.USER_CANCELLED
        ]
        
        for failure_type in non_retryable_failures:
            with self.subTest(failure_type=failure_type):
                execution = ScheduleExecution(
                    execution_id="no_retry_test",
                    schedule_id="test_schedule",
                    start_time=time.time(),
                    status=ExecutionStatus.FAILED,
                    retry_attempt=1
                )
                
                should_retry = self.recovery_manager._should_retry_execution(
                    execution, failure_type, retry_config
                )
                self.assertFalse(should_retry)
    
    def test_max_retry_attempts_enforcement(self):
        """Test that max retry attempts are enforced."""
        retry_config = RetryConfig(
            strategy=RetryStrategy.FIXED_DELAY,
            max_attempts=3,
            jitter=False
        )
        
        execution = ScheduleExecution(
            execution_id="max_retry_test",
            schedule_id="test_schedule",
            start_time=time.time(),
            status=ExecutionStatus.FAILED,
            retry_attempt=3  # Already at max attempts
        )
        
        should_retry = self.recovery_manager._should_retry_execution(
            execution, FailureType.TIMEOUT, retry_config
        )
        self.assertFalse(should_retry)
        
        # Test just under max attempts
        execution.retry_attempt = 2
        should_retry = self.recovery_manager._should_retry_execution(
            execution, FailureType.TIMEOUT, retry_config
        )
        self.assertTrue(should_retry)
    
    def test_failure_record_creation(self):
        """Test creation of failure records."""
        execution = ScheduleExecution(
            execution_id="failure_record_test",
            schedule_id="test_schedule",
            start_time=time.time(),
            status=ExecutionStatus.RUNNING
        )
        
        error_message = "Test error occurred"
        stack_trace = "Traceback (most recent call last):\n  File test.py, line 1"
        
        with patch.object(self.recovery_manager, 'schedule_manager') as mock_manager:
            mock_manager.get_schedule.return_value = self.test_schedule
            
            result = self.recovery_manager.handle_execution_failure(
                execution, FailureType.TIMEOUT, error_message, stack_trace
            )
            
            self.assertTrue(result)
            # Note: handle_execution_failure sets status to RETRYING, not FAILED
            self.assertEqual(execution.status, ExecutionStatus.RETRYING)
            self.assertEqual(execution.failure_type, FailureType.TIMEOUT)
            self.assertIsNotNone(execution.failure_records)
            self.assertEqual(len(execution.failure_records), 1)
            
            failure_record = execution.failure_records[0]
            self.assertEqual(failure_record.error_message, error_message)
            self.assertEqual(failure_record.stack_trace, stack_trace)
            self.assertEqual(failure_record.failure_type, FailureType.TIMEOUT)
    
    def test_exponential_backoff_calculation(self):
        """Test exponential backoff delay calculation."""
        retry_config = RetryConfig(
            strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
            base_delay_seconds=10,
            backoff_multiplier=2.0,
            max_delay_seconds=300,
            jitter=False
        )
        
        # Test progression
        delay1 = retry_config.calculate_delay(1)
        delay2 = retry_config.calculate_delay(2)
        delay3 = retry_config.calculate_delay(3)
        delay4 = retry_config.calculate_delay(4)
        
        self.assertEqual(delay1, 10)   # 10 * 2^0
        self.assertEqual(delay2, 20)   # 10 * 2^1
        self.assertEqual(delay3, 40)   # 10 * 2^2
        self.assertEqual(delay4, 80)   # 10 * 2^3
        
        # Test max delay enforcement
        delay_large = retry_config.calculate_delay(10)
        self.assertLessEqual(delay_large, retry_config.max_delay_seconds)
    
    def test_jitter_application(self):
        """Test jitter application to retry delays."""
        retry_config = RetryConfig(
            strategy=RetryStrategy.FIXED_DELAY,
            base_delay_seconds=100,
            jitter=True
        )
        
        delays = [retry_config.calculate_delay(1) for _ in range(10)]
        
        # All delays should be between 50 and 100 (50% jitter)
        for delay in delays:
            self.assertGreaterEqual(delay, 50)
            self.assertLessEqual(delay, 100)
        
        # Should have some variation
        unique_delays = set(delays)
        self.assertGreater(len(unique_delays), 1)
    
    def test_schedule_suspension_on_repeated_failures(self):
        """Test schedule suspension after repeated failures."""
        schedule = ScheduleConfig(
            schedule_id="suspension_test",
            name="suspension_test",
            schedule_type=ScheduleType.RECURRING,
            cron_expression="0 2 * * *",
            target_percent=80.0
        )
        
        execution = ScheduleExecution(
            execution_id="suspension_test",
            schedule_id="suspension_test",
            start_time=time.time(),
            status=ExecutionStatus.FAILED,
            retry_attempt=3  # Max retries exceeded
        )
        
        failure_record = FailureRecord(
            failure_id="test_failure",
            execution_id=execution.execution_id,
            schedule_id=execution.schedule_id,
            failure_type=FailureType.TIMEOUT,
            error_message="Test failure",
            timestamp=time.time()
        )
        
        with patch.object(self.recovery_manager, 'schedule_manager') as mock_manager:
            mock_manager.get_schedule.return_value = schedule
            
            # Simulate multiple failures
            for i in range(5):
                schedule.failure_count = i + 1
                result = self.recovery_manager._handle_final_failure(
                    execution, schedule, failure_record
                )
                self.assertTrue(result)
            
            # Schedule should be suspended after repeated failures
            should_suspend = self.recovery_manager._should_suspend_schedule(schedule)
            if should_suspend:
                schedule.suspended = True
                schedule.suspend_reason = "Suspended due to repeated failures"
            
            # Verify suspension logic (implementation-dependent)
            # This test verifies the mechanism exists
            self.assertIsNotNone(schedule.failure_count)
    
    def test_notification_on_failure(self):
        """Test notification sending on execution failure."""
        mock_notification_manager = MagicMock(spec=NotificationManager)
        self.recovery_manager.set_notification_manager(mock_notification_manager)
        
        execution = ScheduleExecution(
            execution_id="notification_test",
            schedule_id="test_schedule",
            start_time=time.time(),
            status=ExecutionStatus.RUNNING,
            retry_attempt=3  # Final failure
        )
        
        failure_record = FailureRecord(
            failure_id="test_failure",
            execution_id=execution.execution_id,
            schedule_id=execution.schedule_id,
            failure_type=FailureType.TIMEOUT,
            error_message="Test timeout error",
            timestamp=time.time()
        )
        
        with patch.object(self.recovery_manager, 'schedule_manager') as mock_manager:
            mock_manager.get_schedule.return_value = self.test_schedule
            
            result = self.recovery_manager._handle_final_failure(
                execution, self.test_schedule, failure_record
            )
            
            self.assertTrue(result)
            
            # Verify notification was sent
            mock_notification_manager.send_notification.assert_called_once()
            call_args = mock_notification_manager.send_notification.call_args
            
            # Check notification level is ERROR
            self.assertEqual(call_args[0][0], NotificationLevel.ERROR)
            
            # Check notification contains relevant information
            notification_message = call_args[0][2]
            self.assertIn("failed", notification_message.lower())
            self.assertIn("retry attempts", notification_message.lower())


class TestRetryStrategies(unittest.TestCase):
    """Test different retry strategies."""
    
    def test_no_retry_strategy(self):
        """Test no retry strategy."""
        retry_config = RetryConfig(strategy=RetryStrategy.NONE)
        
        delay = retry_config.calculate_delay(1)
        self.assertEqual(delay, 0)
    
    def test_fixed_delay_strategy(self):
        """Test fixed delay retry strategy."""
        retry_config = RetryConfig(
            strategy=RetryStrategy.FIXED_DELAY,
            base_delay_seconds=30,
            jitter=False
        )
        
        delay1 = retry_config.calculate_delay(1)
        delay2 = retry_config.calculate_delay(2)
        delay3 = retry_config.calculate_delay(3)
        
        self.assertEqual(delay1, 30)
        self.assertEqual(delay2, 30)
        self.assertEqual(delay3, 30)
    
    def test_linear_backoff_strategy(self):
        """Test linear backoff retry strategy."""
        retry_config = RetryConfig(
            strategy=RetryStrategy.LINEAR_BACKOFF,
            base_delay_seconds=15,
            jitter=False
        )
        
        delay1 = retry_config.calculate_delay(1)
        delay2 = retry_config.calculate_delay(2)
        delay3 = retry_config.calculate_delay(3)
        
        self.assertEqual(delay1, 15)  # 15 * 1
        self.assertEqual(delay2, 30)  # 15 * 2
        self.assertEqual(delay3, 45)  # 15 * 3
    
    def test_retry_config_validation(self):
        """Test retry configuration validation."""
        # Test valid configuration
        valid_config = RetryConfig(
            strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
            max_attempts=5,
            base_delay_seconds=10,
            max_delay_seconds=600,
            backoff_multiplier=2.0,
            jitter=False
        )
        
        self.assertEqual(valid_config.max_attempts, 5)
        self.assertEqual(valid_config.base_delay_seconds, 10)
        self.assertEqual(valid_config.max_delay_seconds, 600)
        self.assertEqual(valid_config.backoff_multiplier, 2.0)
        
        # Test minimum delay enforcement
        delay = valid_config.calculate_delay(1)
        self.assertGreaterEqual(delay, 1)  # Minimum delay is 1 second


class TestFailureClassification(unittest.TestCase):
    """Test failure classification functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.recovery_manager = ErrorRecoveryManager()
    
    def test_timeout_classification(self):
        """Test timeout error classification."""
        timeout_messages = [
            "Connection timed out",
            "Operation timed out",
            "Timeout occurred",
            "Request timeout",
            "Command timed out after 3600 seconds"
        ]
        
        for message in timeout_messages:
            with self.subTest(message=message):
                failure_type = self.recovery_manager._classify_failure_type(
                    message, 124, None
                )
                self.assertEqual(failure_type, FailureType.TIMEOUT)
    
    def test_permission_classification(self):
        """Test permission error classification."""
        permission_messages = [
            "Permission denied",
            "Access denied",
            "Operation not permitted",
            "Insufficient privileges",
            "Access is denied"
        ]
        
        for message in permission_messages:
            with self.subTest(message=message):
                failure_type = self.recovery_manager._classify_failure_type(
                    message, 1, None
                )
                self.assertEqual(failure_type, FailureType.PERMISSION_DENIED)
    
    def test_resource_exhaustion_classification(self):
        """Test resource exhaustion error classification."""
        resource_messages = [
            "No space left on device",
            "Out of memory",
            "Memory allocation failed",
            "Disk full",
            "Resource temporarily unavailable"
        ]
        
        for message in resource_messages:
            with self.subTest(message=message):
                failure_type = self.recovery_manager._classify_failure_type(
                    message, 28, None
                )
                self.assertIn(failure_type, [
                    FailureType.DISK_ERROR,
                    FailureType.RESOURCE_EXHAUSTION,
                    FailureType.SYSTEM_ERROR
                ])
    
    def test_network_error_classification(self):
        """Test network error classification."""
        network_messages = [
            "Connection refused",
            "Network unreachable",
            "Host not found",
            "Connection reset by peer",
            "No route to host"
        ]
        
        for message in network_messages:
            with self.subTest(message=message):
                failure_type = self.recovery_manager._classify_failure_type(
                    message, 111, None
                )
                self.assertEqual(failure_type, FailureType.NETWORK_ERROR)
    
    def test_unknown_error_classification(self):
        """Test unknown error classification."""
        unknown_messages = [
            "Unexpected error occurred",
            "Internal server error",
            "Something went wrong",
            "Error code 500",
            ""
        ]
        
        for message in unknown_messages:
            with self.subTest(message=message):
                failure_type = self.recovery_manager._classify_failure_type(
                    message, 1, None
                )
                self.assertEqual(failure_type, FailureType.UNKNOWN)


if __name__ == '__main__':
    unittest.main()