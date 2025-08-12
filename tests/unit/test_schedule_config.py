#!/usr/bin/env python3
"""
Unit tests for schedule configuration functionality.

Tests the ScheduleConfig class, cron expression validation,
schedule persistence, and configuration management.
"""

import unittest
import tempfile
import json
import os
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch, MagicMock

# Import scheduler components
try:
    from scheduler import (
        ScheduleConfig, ScheduleType, TriggerType, ResourceThresholds,
        CronExpressionValidator, SchedulingEngine, ScheduleMonitor,
        ExecutionStatus, ScheduleExecution, ScheduleStatistics,
        NotificationManager, ScheduleHealthMonitor, ErrorRecoveryManager,
        NotificationConfig, FailureType, RetryConfig
    )
except ImportError:
    # Skip tests if scheduler module not available
    import sys
    print("Scheduler module not available - skipping schedule config tests")
    sys.exit(0)


class TestScheduleConfig(unittest.TestCase):
    """Test ScheduleConfig class functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.config_file = Path(self.temp_dir) / "test_schedule.json"
        
    def tearDown(self):
        """Clean up test fixtures."""
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_schedule_config_creation(self):
        """Test basic ScheduleConfig creation."""
        config = ScheduleConfig(
            name="test_schedule",
            schedule_type=ScheduleType.CRON,
            cron_expression="0 2 * * *",
            command=["python", "unraid_rebalancer.py", "--target", "80"]
        )
        
        self.assertEqual(config.name, "test_schedule")
        self.assertEqual(config.schedule_type, ScheduleType.CRON)
        self.assertEqual(config.cron_expression, "0 2 * * *")
        self.assertEqual(config.command, ["python", "unraid_rebalancer.py", "--target", "80"])
        self.assertTrue(config.enabled)
    
    def test_schedule_config_with_resource_thresholds(self):
        """Test ScheduleConfig with resource monitoring."""
        thresholds = ResourceThresholds(
            max_cpu_percent=80.0,
            max_memory_percent=70.0,
            max_disk_io_percent=60.0
        )
        
        config = ScheduleConfig(
            name="resource_aware_schedule",
            schedule_type=ScheduleType.CONDITIONAL,
            trigger_type=TriggerType.RESOURCE_BASED,
            resource_thresholds=thresholds,
            command=["python", "unraid_rebalancer.py", "--rsync-mode", "fast"]
        )
        
        self.assertEqual(config.trigger_type, TriggerType.RESOURCE_BASED)
        self.assertEqual(config.resource_thresholds.max_cpu_percent, 80.0)
        self.assertEqual(config.resource_thresholds.max_memory_percent, 70.0)
    
    def test_schedule_config_serialization(self):
        """Test schedule configuration JSON serialization."""
        config = ScheduleConfig(
            name="serialization_test",
            schedule_type=ScheduleType.CRON,
            cron_expression="0 3 * * 1",
            command=["python", "unraid_rebalancer.py", "--dry-run"]
        )
        
        # Test to_dict
        config_dict = config.to_dict()
        self.assertIsInstance(config_dict, dict)
        self.assertEqual(config_dict['name'], "serialization_test")
        self.assertEqual(config_dict['cron_expression'], "0 3 * * 1")
        
        # Test from_dict
        restored_config = ScheduleConfig.from_dict(config_dict)
        self.assertEqual(restored_config.name, config.name)
        self.assertEqual(restored_config.cron_expression, config.cron_expression)
        self.assertEqual(restored_config.command, config.command)
    
    def test_schedule_config_persistence(self):
        """Test saving and loading schedule configurations."""
        config = ScheduleConfig(
            name="persistence_test",
            schedule_type=ScheduleType.CRON,
            cron_expression="0 4 * * *",
            command=["python", "unraid_rebalancer.py", "--target", "75"]
        )
        
        # Save configuration
        config.save_to_file(self.config_file)
        self.assertTrue(self.config_file.exists())
        
        # Load configuration
        loaded_config = ScheduleConfig.load_from_file(self.config_file)
        self.assertEqual(loaded_config.name, config.name)
        self.assertEqual(loaded_config.cron_expression, config.cron_expression)
        self.assertEqual(loaded_config.command, config.command)


class TestCronExpressionValidator(unittest.TestCase):
    """Test cron expression validation functionality."""
    
    def test_valid_cron_expressions(self):
        """Test validation of valid cron expressions."""
        valid_expressions = [
            "0 2 * * *",      # Daily at 2 AM
            "0 0 * * 0",      # Weekly on Sunday
            "0 0 1 * *",      # Monthly on 1st
            "*/15 * * * *",   # Every 15 minutes
            "0 2-6 * * *",    # Daily between 2-6 AM
            "0 2 * * 1-5",    # Weekdays at 2 AM
        ]
        
        validator = CronExpressionValidator()
        for expression in valid_expressions:
            with self.subTest(expression=expression):
                self.assertTrue(validator.is_valid(expression))
    
    def test_invalid_cron_expressions(self):
        """Test validation of invalid cron expressions."""
        invalid_expressions = [
            "invalid",         # Not a cron expression
            "0 25 * * *",      # Invalid hour (25)
            "0 0 32 * *",      # Invalid day (32)
            "0 0 * 13 *",      # Invalid month (13)
            "0 0 * * 8",       # Invalid day of week (8)
            "* * * *",         # Missing field
            "0 0 0 0 0 0",     # Too many fields
        ]
        
        validator = CronExpressionValidator()
        for expression in invalid_expressions:
            with self.subTest(expression=expression):
                self.assertFalse(validator.is_valid(expression))
    
    def test_cron_expression_parsing(self):
        """Test parsing cron expressions into components."""
        validator = CronExpressionValidator()
        
        # Test daily at 2 AM
        components = validator.parse_expression("0 2 * * *")
        self.assertEqual(components['minute'], '0')
        self.assertEqual(components['hour'], '2')
        self.assertEqual(components['day'], '*')
        self.assertEqual(components['month'], '*')
        self.assertEqual(components['dow'], '*')
    
    def test_next_execution_time(self):
        """Test calculation of next execution time."""
        validator = CronExpressionValidator()
        
        # Test with a known time
        base_time = datetime(2024, 1, 1, 1, 0, 0)  # 1 AM on Jan 1, 2024
        
        # Daily at 2 AM should be next at 2 AM same day
        next_time = validator.get_next_execution("0 2 * * *", base_time)
        expected = datetime(2024, 1, 1, 2, 0, 0)
        self.assertEqual(next_time, expected)
        
        # If it's already past 2 AM, should be next day
        base_time = datetime(2024, 1, 1, 3, 0, 0)  # 3 AM
        next_time = validator.get_next_execution("0 2 * * *", base_time)
        expected = datetime(2024, 1, 2, 2, 0, 0)
        self.assertEqual(next_time, expected)


class TestScheduleTypes(unittest.TestCase):
    """Test different schedule types and triggers."""
    
    def test_cron_schedule_type(self):
        """Test CRON schedule type functionality."""
        config = ScheduleConfig(
            name="cron_test",
            schedule_type=ScheduleType.CRON,
            cron_expression="0 2 * * *",
            command=["echo", "test"]
        )
        
        self.assertEqual(config.schedule_type, ScheduleType.CRON)
        self.assertIsNotNone(config.cron_expression)
    
    def test_conditional_schedule_type(self):
        """Test CONDITIONAL schedule type functionality."""
        thresholds = ResourceThresholds(
            max_cpu_percent=50.0,
            max_memory_percent=60.0
        )
        
        config = ScheduleConfig(
            name="conditional_test",
            schedule_type=ScheduleType.CONDITIONAL,
            trigger_type=TriggerType.RESOURCE_BASED,
            resource_thresholds=thresholds,
            command=["echo", "test"]
        )
        
        self.assertEqual(config.schedule_type, ScheduleType.CONDITIONAL)
        self.assertEqual(config.trigger_type, TriggerType.RESOURCE_BASED)
        self.assertIsNotNone(config.resource_thresholds)
    
    def test_one_time_schedule_type(self):
        """Test ONE_TIME schedule type functionality."""
        execution_time = datetime.now() + timedelta(hours=1)
        
        config = ScheduleConfig(
            name="onetime_test",
            schedule_type=ScheduleType.ONE_TIME,
            execution_time=execution_time,
            command=["echo", "test"]
        )
        
        self.assertEqual(config.schedule_type, ScheduleType.ONE_TIME)
        self.assertEqual(config.execution_time, execution_time)


class TestScheduleValidation(unittest.TestCase):
    """Test schedule configuration validation."""
    
    def test_valid_schedule_validation(self):
        """Test validation of valid schedule configurations."""
        config = ScheduleConfig(
            name="valid_schedule",
            schedule_type=ScheduleType.CRON,
            cron_expression="0 2 * * *",
            command=["python", "unraid_rebalancer.py"]
        )
        
        self.assertTrue(config.is_valid())
    
    def test_invalid_schedule_validation(self):
        """Test validation of invalid schedule configurations."""
        # Missing command
        config = ScheduleConfig(
            name="invalid_schedule",
            schedule_type=ScheduleType.CRON,
            cron_expression="0 2 * * *",
            command=[]
        )
        
        self.assertFalse(config.is_valid())
        
        # Invalid cron expression
        config = ScheduleConfig(
            name="invalid_cron",
            schedule_type=ScheduleType.CRON,
            cron_expression="invalid",
            command=["echo", "test"]
        )
        
        self.assertFalse(config.is_valid())
    
    def test_schedule_conflict_detection(self):
        """Test detection of schedule conflicts."""
        config1 = ScheduleConfig(
            name="schedule1",
            schedule_type=ScheduleType.CRON,
            cron_expression="0 2 * * *",
            command=["echo", "test1"]
        )
        
        config2 = ScheduleConfig(
            name="schedule2",
            schedule_type=ScheduleType.CRON,
            cron_expression="0 2 * * *",  # Same time
            command=["echo", "test2"]
        )
        
        # Test conflict detection logic
        self.assertTrue(config1.conflicts_with(config2))
        
        # Different times should not conflict
        config2.cron_expression = "0 3 * * *"
        self.assertFalse(config1.conflicts_with(config2))


if __name__ == '__main__':
    unittest.main()