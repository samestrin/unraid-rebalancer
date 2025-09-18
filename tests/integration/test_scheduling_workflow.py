#!/usr/bin/env python3
"""
Integration tests for complete scheduling workflow.

Tests end-to-end scheduling functionality including schedule creation,
execution, monitoring, error handling, and system integration.
"""

import unittest
import tempfile
import json
import time
import os
import subprocess
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch, MagicMock
import pytest

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
    pytest.skip("Scheduler module not available", allow_module_level=True)


class TestSchedulingWorkflow(unittest.TestCase):
    """Test complete scheduling workflow integration."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.config_file = os.path.join(self.temp_dir, "scheduler_config.json")
        self.log_file = os.path.join(self.temp_dir, "scheduler.log")
        
        # Initialize components
        self.engine = SchedulingEngine(config_file=self.config_file)
        self.monitor = ScheduleMonitor(log_file=self.log_file)
        self.health_monitor = ScheduleHealthMonitor()
        self.recovery_manager = ErrorRecoveryManager()
        
        # Create test script
        self.test_script = os.path.join(self.temp_dir, "test_rebalancer.py")
        with open(self.test_script, 'w') as f:
            f.write('''
#!/usr/bin/env python3
import sys
import time
import json

def main():
    if len(sys.argv) > 1 and sys.argv[1] == "--fail":
        print("Simulated failure", file=sys.stderr)
        sys.exit(1)
    elif len(sys.argv) > 1 and sys.argv[1] == "--slow":
        time.sleep(2)
    
    result = {
        "status": "success",
        "timestamp": time.time(),
        "message": "Rebalancing completed successfully"
    }
    print(json.dumps(result))
    sys.exit(0)

if __name__ == "__main__":
    main()
''')
        os.chmod(self.test_script, 0o755)
        
    def tearDown(self):
        """Clean up test fixtures."""
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_complete_schedule_lifecycle(self):
        """Test complete schedule lifecycle from creation to execution."""
        # 1. Create schedule
        schedule = ScheduleConfig(
            name="integration_test",
            schedule_type=ScheduleType.CRON,
            cron_expression="*/5 * * * *",  # Every 5 minutes
            command=["python3", self.test_script],
            enabled=True,
            timeout_seconds=30
        )
        
        result = self.engine.create_schedule(schedule)
        self.assertTrue(result)
        
        # 2. Verify schedule was created
        created_schedule = self.engine.get_schedule("integration_test")
        self.assertIsNotNone(created_schedule)
        self.assertEqual(created_schedule.name, "integration_test")
        self.assertTrue(created_schedule.enabled)
        
        # 3. Execute schedule manually
        execution = self.engine.execute_schedule(created_schedule)
        self.assertEqual(execution.status, ExecutionStatus.SUCCESS)
        self.assertEqual(execution.exit_code, 0)
        
        # 4. Record execution in monitor
        self.monitor.record_execution(execution)
        
        # 5. Verify execution history
        history = self.monitor.get_execution_history("integration_test")
        self.assertEqual(len(history), 1)
        self.assertEqual(history[0].status, ExecutionStatus.SUCCESS)
        
        # 6. Check schedule health
        health = self.health_monitor.assess_schedule_health("integration_test")
        self.assertEqual(health.status, "healthy")
        
        # 7. Update schedule
        updated_schedule = ScheduleConfig(
            name="integration_test",
            schedule_type=ScheduleType.CRON,
            cron_expression="*/10 * * * *",  # Changed to every 10 minutes
            command=["python3", self.test_script],
            enabled=True,
            timeout_seconds=30
        )
        
        result = self.engine.update_schedule(updated_schedule)
        self.assertTrue(result)
        
        # 8. Verify update
        modified_schedule = self.engine.get_schedule("integration_test")
        self.assertEqual(modified_schedule.cron_expression, "*/10 * * * *")
        
        # 9. Delete schedule
        result = self.engine.delete_schedule("integration_test")
        self.assertTrue(result)
        
        # 10. Verify deletion
        deleted_schedule = self.engine.get_schedule("integration_test")
        self.assertIsNone(deleted_schedule)
    
    def test_error_handling_and_recovery(self):
        """Test error handling and recovery mechanisms."""
        # Create schedule that will fail
        failing_schedule = ScheduleConfig(
            name="failing_test",
            schedule_type=ScheduleType.CRON,
            cron_expression="*/5 * * * *",
            command=["python3", self.test_script, "--fail"],
            enabled=True,
            max_consecutive_failures=2,
            retry_config=RetryConfig(
                max_attempts=3,
                initial_delay=1,
                backoff_multiplier=2.0
            )
        )
        
        self.engine.create_schedule(failing_schedule)
        
        # Execute and expect failure
        execution = self.engine.execute_schedule(failing_schedule)
        self.assertEqual(execution.status, ExecutionStatus.FAILED)
        self.assertEqual(execution.exit_code, 1)
        
        # Record failure
        self.monitor.record_execution(execution)
        
        # Test retry logic
        retry_config = failing_schedule.retry_config
        failure_type = self.recovery_manager.classify_failure(execution.error_message)
        
        should_retry = self.recovery_manager.should_retry(
            failure_type, 
            attempt=1, 
            retry_config=retry_config
        )
        self.assertTrue(should_retry)
        
        # Calculate retry delay
        delay = self.recovery_manager.calculate_retry_delay(retry_config, 1)
        self.assertEqual(delay, 1)  # Initial delay
        
        # Simulate multiple failures leading to suspension
        for i in range(3):
            execution = ScheduleExecution(
                schedule_name="failing_test",
                start_time=time.time() - (i * 300),
                end_time=time.time() - (i * 300),
                status=ExecutionStatus.FAILED,
                exit_code=1,
                error_message="Simulated failure"
            )
            self.monitor.record_execution(execution)
        
        # Check if schedule should be suspended
        consecutive_failures = self.monitor.get_consecutive_failures("failing_test")
        self.assertGreaterEqual(consecutive_failures, 2)
        
        # Health should be poor
        health = self.health_monitor.assess_schedule_health("failing_test")
        self.assertEqual(health.status, "unhealthy")
    
    def test_concurrent_schedule_execution(self):
        """Test handling of concurrent schedule executions."""
        # Create multiple schedules
        schedules = []
        for i in range(3):
            schedule = ScheduleConfig(
                name=f"concurrent_test_{i}",
                schedule_type=ScheduleType.CRON,
                cron_expression="*/5 * * * *",
                command=["python3", self.test_script],
                enabled=True
            )
            schedules.append(schedule)
            self.engine.create_schedule(schedule)
        
        # Execute all schedules concurrently
        import threading
        import queue
        
        results_queue = queue.Queue()
        
        def execute_schedule(sched):
            execution = self.engine.execute_schedule(sched)
            results_queue.put((sched.name, execution))
        
        threads = []
        for schedule in schedules:
            thread = threading.Thread(target=execute_schedule, args=(schedule,))
            threads.append(thread)
            thread.start()
        
        # Wait for all executions to complete
        for thread in threads:
            thread.join(timeout=10)
        
        # Collect results
        results = []
        while not results_queue.empty():
            results.append(results_queue.get())
        
        # Verify all executions completed successfully
        self.assertEqual(len(results), 3)
        for name, execution in results:
            self.assertEqual(execution.status, ExecutionStatus.SUCCESS)
            self.assertIn("concurrent_test", name)
    
    def test_schedule_persistence(self):
        """Test schedule persistence across engine restarts."""
        # Create schedule
        schedule = ScheduleConfig(
            name="persistence_test",
            schedule_type=ScheduleType.CRON,
            cron_expression="0 2 * * *",
            command=["python3", self.test_script],
            enabled=True
        )
        
        self.engine.create_schedule(schedule)
        
        # Save configuration
        self.engine.save_configuration()
        
        # Create new engine instance (simulating restart)
        new_engine = SchedulingEngine(config_file=self.config_file)
        new_engine.load_configuration()
        
        # Verify schedule was persisted
        loaded_schedule = new_engine.get_schedule("persistence_test")
        self.assertIsNotNone(loaded_schedule)
        self.assertEqual(loaded_schedule.name, "persistence_test")
        self.assertEqual(loaded_schedule.cron_expression, "0 2 * * *")
        self.assertTrue(loaded_schedule.enabled)
    
    def test_execution_timeout_handling(self):
        """Test handling of execution timeouts."""
        # Create schedule with short timeout
        timeout_schedule = ScheduleConfig(
            name="timeout_test",
            schedule_type=ScheduleType.CRON,
            cron_expression="*/5 * * * *",
            command=["python3", self.test_script, "--slow"],
            enabled=True,
            timeout_seconds=1  # Very short timeout
        )
        
        self.engine.create_schedule(timeout_schedule)
        
        # Execute and expect timeout
        execution = self.engine.execute_schedule(timeout_schedule)
        self.assertEqual(execution.status, ExecutionStatus.TIMEOUT)
        self.assertIsNotNone(execution.error_message)
        self.assertIn("timeout", execution.error_message.lower())
    
    def test_schedule_statistics_calculation(self):
        """Test calculation of comprehensive schedule statistics."""
        schedule_name = "stats_test"
        
        # Create multiple executions with different outcomes
        executions = [
            # Successful executions
            ScheduleExecution(
                schedule_name=schedule_name,
                start_time=time.time() - 3600,
                end_time=time.time() - 3540,  # 1 minute
                status=ExecutionStatus.SUCCESS,
                exit_code=0
            ),
            ScheduleExecution(
                schedule_name=schedule_name,
                start_time=time.time() - 1800,
                end_time=time.time() - 1680,  # 2 minutes
                status=ExecutionStatus.SUCCESS,
                exit_code=0
            ),
            # Failed execution
            ScheduleExecution(
                schedule_name=schedule_name,
                start_time=time.time() - 900,
                end_time=time.time() - 900,
                status=ExecutionStatus.FAILED,
                exit_code=1,
                error_message="Test failure"
            ),
            # Timeout execution
            ScheduleExecution(
                schedule_name=schedule_name,
                start_time=time.time() - 450,
                end_time=time.time() - 450,
                status=ExecutionStatus.TIMEOUT,
                exit_code=-1,
                error_message="Execution timeout"
            )
        ]
        
        # Record all executions
        for execution in executions:
            self.monitor.record_execution(execution)
        
        # Calculate statistics
        stats = self.monitor.get_schedule_statistics(schedule_name)
        
        # Verify statistics
        self.assertEqual(stats.total_executions, 4)
        self.assertEqual(stats.successful_executions, 2)
        self.assertEqual(stats.failed_executions, 1)
        self.assertEqual(stats.timeout_executions, 1)
        self.assertEqual(stats.success_rate, 50.0)
        self.assertGreater(stats.average_duration, 0)
        self.assertIsNotNone(stats.last_execution_time)
        self.assertIsNotNone(stats.last_success_time)
    
    def test_notification_integration(self):
        """Test integration with notification system."""
        # Create notification configuration
        notification_config = NotificationConfig(
            enabled=True,
            notify_on_success=False,
            notify_on_failure=True,
            notify_on_timeout=True,
            email_recipients=["admin@example.com"],
            webhook_url="http://localhost:8080/webhook"
        )
        
        notification_manager = NotificationManager(notification_config)
        
        # Create schedule with notifications
        schedule = ScheduleConfig(
            name="notification_test",
            schedule_type=ScheduleType.CRON,
            cron_expression="*/5 * * * *",
            command=["python3", self.test_script, "--fail"],
            enabled=True,
            notification_config=notification_config
        )
        
        self.engine.create_schedule(schedule)
        
        # Execute and expect failure
        execution = self.engine.execute_schedule(schedule)
        self.assertEqual(execution.status, ExecutionStatus.FAILED)
        
        # Test notification sending (mocked)
        with patch.object(notification_manager, 'send_notification') as mock_send:
            notification_manager.handle_execution_result(execution)
            mock_send.assert_called_once()
            
            # Verify notification content
            call_args = mock_send.call_args[0]
            self.assertIn("failed", call_args[0].lower())  # Subject should mention failure
            self.assertIn("notification_test", call_args[1])  # Body should mention schedule name


class TestUnraidIntegration(unittest.TestCase):
    """Test integration with Unraid-specific functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.engine = SchedulingEngine()
        
        # Mock Unraid paths
        self.mock_user_scripts_path = os.path.join(self.temp_dir, "user.scripts")
        self.mock_maintenance_config = os.path.join(self.temp_dir, "maintenance.conf")
        
        os.makedirs(self.mock_user_scripts_path, exist_ok=True)
        
        # Create mock maintenance configuration
        with open(self.mock_maintenance_config, 'w') as f:
            f.write('''
# Maintenance windows configuration
# Format: day_of_week:start_hour:end_hour
# 0=Sunday, 1=Monday, etc.
0:02:06  # Sunday 2 AM to 6 AM
3:01:05  # Wednesday 1 AM to 5 AM
''')
    
    def tearDown(self):
        """Clean up test fixtures."""
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    @patch('unraid_integration.UnraidIntegrationManager')
    def test_unraid_user_script_integration(self, mock_integration):
        """Test integration with Unraid user scripts."""
        # Mock Unraid integration
        mock_instance = mock_integration.return_value
        mock_instance.user_scripts_path = self.mock_user_scripts_path
        mock_instance.get_user_scripts.return_value = []
        
        # Create schedule that should generate user script
        schedule = ScheduleConfig(
            name="unraid_rebalance",
            schedule_type=ScheduleType.CRON,
            cron_expression="0 2 * * *",
            command=["python3", "/boot/config/plugins/unraid-rebalancer/unraid_rebalancer.py"],
            enabled=True,
            create_user_script=True
        )
        
        self.engine.create_schedule(schedule)
        
        # Verify user script creation was attempted
        mock_instance.create_rebalancer_user_script.assert_called_once()
    
    @patch('unraid_integration.UnraidIntegrationManager')
    def test_maintenance_window_respect(self, mock_integration):
        """Test respecting Unraid maintenance windows."""
        # Mock Unraid integration
        mock_instance = mock_integration.return_value
        mock_instance.maintenance_config_path = self.mock_maintenance_config
        
        # Mock current time to be during maintenance window (Sunday 3 AM)
        with patch('time.localtime') as mock_time:
            # Sunday (0), 3 AM
            mock_time.return_value = time.struct_time((
                2024, 1, 7, 3, 0, 0, 6, 7, 0  # Sunday 3 AM
            ))
            
            mock_instance.is_maintenance_window.return_value = True
            
            # Create schedule
            schedule = ScheduleConfig(
                name="maintenance_aware",
                schedule_type=ScheduleType.CRON,
                cron_expression="0 * * * *",  # Every hour
                command=["echo", "test"],
                enabled=True,
                respect_maintenance_windows=True
            )
            
            self.engine.create_schedule(schedule)
            
            # Execution should be skipped during maintenance window
            execution = self.engine.execute_schedule(schedule)
            self.assertEqual(execution.status, ExecutionStatus.SKIPPED)
            self.assertIn("maintenance", execution.error_message.lower())
    
    @patch('unraid_integration.UnraidIntegrationManager')
    def test_unraid_notification_integration(self, mock_integration):
        """Test integration with Unraid notification system."""
        # Mock Unraid integration
        mock_instance = mock_integration.return_value
        mock_instance.send_notification.return_value = True
        
        # Create schedule with Unraid notifications
        schedule = ScheduleConfig(
            name="unraid_notify_test",
            schedule_type=ScheduleType.CRON,
            cron_expression="0 2 * * *",
            command=["echo", "test"],
            enabled=True,
            use_unraid_notifications=True
        )
        
        self.engine.create_schedule(schedule)
        
        # Execute schedule
        execution = self.engine.execute_schedule(schedule)
        self.assertEqual(execution.status, ExecutionStatus.SUCCESS)
        
        # Verify Unraid notification was sent
        mock_instance.send_notification.assert_called_once()
        
        # Check notification content
        call_args = mock_instance.send_notification.call_args[1]
        self.assertIn("subject", call_args)
        self.assertIn("message", call_args)
        self.assertIn("unraid_notify_test", call_args["message"])


class TestSchedulingTemplates(unittest.TestCase):
    """Test scheduling template functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.engine = SchedulingEngine()
        
    def tearDown(self):
        """Clean up test fixtures."""
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_template_creation_and_usage(self):
        """Test creating and using scheduling templates."""
        # Create template
        template = {
            "name": "daily_rebalance_template",
            "description": "Daily rebalancing at 2 AM",
            "schedule_type": "cron",
            "cron_expression": "0 2 * * *",
            "command_template": ["python3", "/path/to/rebalancer.py", "--target", "{target_percent}"],
            "default_params": {
                "target_percent": "80"
            },
            "timeout_seconds": 3600,
            "enabled": True
        }
        
        # Save template
        template_file = os.path.join(self.temp_dir, "daily_rebalance.json")
        with open(template_file, 'w') as f:
            json.dump(template, f, indent=2)
        
        # Create schedule from template
        schedule = self.engine.create_schedule_from_template(
            template_file,
            name="my_daily_rebalance",
            params={"target_percent": "85"}
        )
        
        self.assertIsNotNone(schedule)
        self.assertEqual(schedule.name, "my_daily_rebalance")
        self.assertEqual(schedule.cron_expression, "0 2 * * *")
        self.assertIn("85", schedule.command)
    
    def test_template_validation(self):
        """Test validation of scheduling templates."""
        # Invalid template (missing required fields)
        invalid_template = {
            "name": "invalid_template",
            "description": "Missing required fields"
            # Missing schedule_type, cron_expression, command_template
        }
        
        template_file = os.path.join(self.temp_dir, "invalid.json")
        with open(template_file, 'w') as f:
            json.dump(invalid_template, f)
        
        # Should fail validation
        with self.assertRaises(ValueError):
            self.engine.create_schedule_from_template(
                template_file,
                name="test_invalid"
            )
    
    def test_template_parameter_substitution(self):
        """Test parameter substitution in templates."""
        template = {
            "name": "parameterized_template",
            "description": "Template with parameters",
            "schedule_type": "cron",
            "cron_expression": "0 {hour} * * *",
            "command_template": [
                "python3", 
                "/path/to/script.py", 
                "--target", "{target}",
                "--mode", "{mode}",
                "--verbose" if "{verbose}" == "true" else ""
            ],
            "default_params": {
                "hour": "2",
                "target": "80",
                "mode": "balanced",
                "verbose": "false"
            }
        }
        
        template_file = os.path.join(self.temp_dir, "parameterized.json")
        with open(template_file, 'w') as f:
            json.dump(template, f, indent=2)
        
        # Create schedule with custom parameters
        schedule = self.engine.create_schedule_from_template(
            template_file,
            name="custom_schedule",
            params={
                "hour": "3",
                "target": "90",
                "verbose": "true"
            }
        )
        
        self.assertEqual(schedule.cron_expression, "0 3 * * *")
        self.assertIn("90", schedule.command)
        self.assertIn("--verbose", schedule.command)


if __name__ == '__main__':
    unittest.main()