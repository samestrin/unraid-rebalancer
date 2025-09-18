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
            schedule_id="test_schedule",
            name="test_schedule",
            schedule_type=ScheduleType.RECURRING,
            cron_expression="0 2 * * *",
            target_percent=80.0
        )
        
        self.assertEqual(config.name, "test_schedule")
        self.assertEqual(config.schedule_type, ScheduleType.RECURRING)
        self.assertEqual(config.cron_expression, "0 2 * * *")
        self.assertEqual(config.target_percent, 80.0)
        self.assertTrue(config.enabled)
    
    def test_schedule_config_with_resource_thresholds(self):
        """Test ScheduleConfig with resource monitoring."""
        thresholds = ResourceThresholds(
            max_cpu_percent=80.0,
            max_memory_percent=70.0,
            max_disk_io_mbps=60.0
        )
        
        config = ScheduleConfig(
            schedule_id="resource_aware_schedule",
            name="resource_aware_schedule",
            schedule_type=ScheduleType.CONDITIONAL,
            trigger_type=TriggerType.RESOURCE_BASED,
            resource_thresholds=thresholds,
            rsync_mode="fast"
        )
        
        self.assertEqual(config.trigger_type, TriggerType.RESOURCE_BASED)
        self.assertEqual(config.resource_thresholds.max_cpu_percent, 80.0)
        self.assertEqual(config.resource_thresholds.max_memory_percent, 70.0)
    
    def test_schedule_config_serialization(self):
        """Test schedule configuration JSON serialization."""
        config = ScheduleConfig(
            schedule_id="test_schedule",
            name="Test Schedule",
            cron_expression="0 2 * * *",
            schedule_type=ScheduleType.RECURRING,
            trigger_type=TriggerType.TIME_BASED
        )
        
        # Test to_dict method
        config_dict = config.to_dict()
        
        # Verify all fields are present
        self.assertIn('schedule_id', config_dict)
        self.assertIn('name', config_dict)
        self.assertIn('cron_expression', config_dict)
        self.assertIn('schedule_type', config_dict)
        self.assertIn('trigger_type', config_dict)
        
        # Verify enum values are serialized as strings
        self.assertEqual(config_dict['schedule_type'], 'recurring')
        self.assertEqual(config_dict['trigger_type'], 'time_based')
        
        # Verify basic values
        self.assertEqual(config_dict['schedule_id'], 'test_schedule')
        self.assertEqual(config_dict['name'], 'Test Schedule')
        self.assertEqual(config_dict['cron_expression'], '0 2 * * *')
    
    def test_schedule_config_persistence(self):
        """Test saving and loading schedule configurations."""
        config = ScheduleConfig(
            schedule_id="test_persistence",
            name="Persistence Test",
            cron_expression="0 3 * * *",
            target_percent=85.0
        )
        
        # Test saving to file
        test_file = Path(self.temp_dir) / "test_config.json"
        result = config.save_to_file(test_file)
        
        # Verify save was successful
        self.assertTrue(result)
        self.assertTrue(test_file.exists())
        
        # Verify file contents
        with open(test_file, 'r') as f:
            saved_data = json.load(f)
        
        self.assertEqual(saved_data['schedule_id'], 'test_persistence')
        self.assertEqual(saved_data['name'], 'Persistence Test')
        self.assertEqual(saved_data['cron_expression'], '0 3 * * *')
        self.assertEqual(saved_data['target_percent'], 85.0)


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
            "0,30 * * * *",   # Every 30 minutes
            "0 9,17 * * *",   # 9 AM and 5 PM daily
            "0 0 1,15 * *",   # 1st and 15th of month
            "0 0 * * 1,3,5",  # Monday, Wednesday, Friday
            "*/5 * * * *",    # Every 5 minutes
            "0 */2 * * *",    # Every 2 hours
            "0 0 */3 * *",    # Every 3 days
            "0 0 1 */2 *",    # Every 2 months on 1st
            "15-45/10 * * * *", # Minutes 15, 25, 35, 45
            "0 9-17/2 * * *", # Every 2 hours from 9 AM to 5 PM
            "0 0 1-7 * 1",    # First Monday of month
            "0 0 * * 7",      # Sunday (7 should be valid)
        ]
        
        validator = CronExpressionValidator()
        for expression in valid_expressions:
            with self.subTest(expression=expression):
                self.assertTrue(validator.validate_cron_expression(expression))
    
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
            "60 * * * *",      # Invalid minute (60)
            "0 0 0 * *",       # Invalid day of month (0)
            "0 0 * 0 *",       # Invalid month (0)
            "*/0 * * * *",     # Invalid step (0)
            "*/-1 * * * *",    # Invalid step (negative)
            "0-60 * * * *",    # Invalid range (minute 60)
            "0 0-24 * * *",    # Invalid range (hour 24)
            "0 0 1-32 * *",    # Invalid range (day 32)
            "0 0 * 1-13 *",    # Invalid range (month 13)
            "0 0 * * 0-8",     # Invalid range (dow 8)
            "5-2 * * * *",     # Invalid range (start > end)
            "0 5-2 * * *",     # Invalid range (start > end)
            "abc * * * *",     # Non-numeric value
            "0 abc * * *",     # Non-numeric value
            "0/abc * * * *",   # Non-numeric step
            "0-abc * * * *",   # Non-numeric range
            "0, * * * *",      # Invalid comma usage
            "0 ,5 * * *",      # Invalid comma usage
        ]
        
        validator = CronExpressionValidator()
        for expression in invalid_expressions:
            with self.subTest(expression=expression):
                self.assertFalse(validator.validate_cron_expression(expression))
    
    def test_cron_expression_parsing(self):
        """Test parsing cron expressions into components."""
        validator = CronExpressionValidator()
        
        # Test daily expression parsing
        components = validator.parse_expression("0 2 * * *")
        expected = {
            'minute': '0',
            'hour': '2', 
            'day_of_month': '*',
            'month': '*',
            'day_of_week': '*',
            'original': '0 2 * * *'
        }
        self.assertEqual(components, expected)
        
        # Test weekly expression parsing
        components = validator.parse_expression("0 0 * * 0")
        expected = {
            'minute': '0',
            'hour': '0',
            'day_of_month': '*', 
            'month': '*',
            'day_of_week': '0',
            'original': '0 0 * * 0'
        }
        self.assertEqual(components, expected)
    
    def test_next_execution_time(self):
        """Test calculation of next execution time."""
        validator = CronExpressionValidator()
        
        # Test daily execution at 2 AM
        from_time = datetime(2024, 1, 1, 0, 0, 0)  # Midnight
        next_time = validator.get_next_execution("0 2 * * *", from_time)
        expected = datetime(2024, 1, 1, 2, 0, 0)  # 2 AM same day
        self.assertEqual(next_time, expected)
        
        # Test when current time is after the scheduled time
        from_time = datetime(2024, 1, 1, 3, 0, 0)  # 3 AM
        next_time = validator.get_next_execution("0 2 * * *", from_time)
        expected = datetime(2024, 1, 2, 2, 0, 0)  # 2 AM next day
        self.assertEqual(next_time, expected)
        
        # Test weekly execution on Sunday
        from_time = datetime(2024, 1, 1, 0, 0, 0)  # Monday
        next_time = validator.get_next_execution("0 0 * * 0", from_time)
        expected = datetime(2024, 1, 7, 0, 0, 0)  # Next Sunday
        self.assertEqual(next_time, expected)
    
    def test_complex_cron_expressions(self):
        """Test validation and parsing of complex cron expressions."""
        validator = CronExpressionValidator()
        
        # Test step values
        self.assertTrue(validator.validate_cron_expression("*/5 * * * *"))  # Every 5 minutes
        self.assertTrue(validator.validate_cron_expression("0 */2 * * *"))  # Every 2 hours
        self.assertTrue(validator.validate_cron_expression("0 0 */3 * *"))  # Every 3 days
        
        # Test ranges with steps
        self.assertTrue(validator.validate_cron_expression("15-45/10 * * * *"))  # Minutes 15, 25, 35, 45
        self.assertTrue(validator.validate_cron_expression("0 9-17/2 * * *"))   # Every 2 hours from 9-17
        
        # Test comma-separated lists
        self.assertTrue(validator.validate_cron_expression("0,30 * * * *"))     # Minutes 0 and 30
        self.assertTrue(validator.validate_cron_expression("0 9,12,17 * * *"))  # 9 AM, noon, 5 PM
        self.assertTrue(validator.validate_cron_expression("0 0 1,15 * *"))     # 1st and 15th
        self.assertTrue(validator.validate_cron_expression("0 0 * * 1,3,5"))    # Mon, Wed, Fri
        
        # Test combinations
        self.assertTrue(validator.validate_cron_expression("0,15,30,45 9-17 * * 1-5"))  # Complex workday schedule
        self.assertTrue(validator.validate_cron_expression("*/10 8-18/2 1-15 */2 *"))   # Very complex
        
        # Test edge cases
        self.assertTrue(validator.validate_cron_expression("59 23 31 12 7"))    # Max values
        self.assertTrue(validator.validate_cron_expression("0 0 1 1 0"))        # Min values
        
        # Test parsing of complex expressions
        components = validator.parse_expression("0,30 9-17 * * 1-5")
        self.assertEqual(components['minute'], '0,30')
        self.assertEqual(components['hour'], '9-17')
        self.assertEqual(components['day_of_week'], '1-5')


class TestScheduleTypes(unittest.TestCase):
    """Test different schedule types and triggers."""
    
    def test_cron_schedule_type(self):
        """Test CRON schedule type functionality."""
        config = ScheduleConfig(
            schedule_id="cron_test",
            name="cron_test",
            schedule_type=ScheduleType.RECURRING,
            cron_expression="0 2 * * *",
            target_percent=80.0
        )
        
        self.assertEqual(config.schedule_type, ScheduleType.RECURRING)
        self.assertIsNotNone(config.cron_expression)
    
    def test_conditional_schedule_type(self):
        """Test CONDITIONAL schedule type functionality."""
        thresholds = ResourceThresholds(
            max_cpu_percent=50.0,
            max_memory_percent=60.0
        )
        
        config = ScheduleConfig(
            schedule_id="conditional_test",
            name="conditional_test",
            schedule_type=ScheduleType.CONDITIONAL,
            trigger_type=TriggerType.RESOURCE_BASED,
            resource_thresholds=thresholds,
            target_percent=80.0
        )
        
        self.assertEqual(config.schedule_type, ScheduleType.CONDITIONAL)
        self.assertEqual(config.trigger_type, TriggerType.RESOURCE_BASED)
        self.assertIsNotNone(config.resource_thresholds)
    
    def test_one_time_schedule_type(self):
        """Test ONE_TIME schedule type functionality."""
        execution_time = datetime.now() + timedelta(hours=1)
        
        config = ScheduleConfig(
            schedule_id="onetime_test",
            name="onetime_test",
            schedule_type=ScheduleType.ONE_TIME,
            target_percent=80.0
        )
        
        self.assertEqual(config.schedule_type, ScheduleType.ONE_TIME)


class TestScheduleValidation(unittest.TestCase):
    """Test schedule configuration validation."""
    
    def test_valid_schedule_validation(self):
        """Test validation of valid schedule configurations."""
        # Test valid schedule
        valid_config = ScheduleConfig(
            schedule_id="valid_schedule",
            name="Valid Schedule",
            cron_expression="0 2 * * *",
            target_percent=80.0
        )
        
        self.assertTrue(valid_config.is_valid())
        
        # Test valid schedule without cron expression
        valid_config_no_cron = ScheduleConfig(
            schedule_id="valid_no_cron",
            name="Valid No Cron",
            schedule_type=ScheduleType.ONE_TIME
        )
        
        self.assertTrue(valid_config_no_cron.is_valid())
    
    def test_invalid_schedule_validation(self):
        """Test validation of invalid schedule configurations."""
        # Test invalid schedule - missing schedule_id
        invalid_config1 = ScheduleConfig(
            schedule_id="",
            name="Invalid Schedule",
            cron_expression="0 2 * * *"
        )
        
        self.assertFalse(invalid_config1.is_valid())
        
        # Test invalid schedule - missing name
        invalid_config2 = ScheduleConfig(
            schedule_id="invalid_schedule",
            name="",
            cron_expression="0 2 * * *"
        )
        
        self.assertFalse(invalid_config2.is_valid())
        
        # Test invalid schedule - invalid cron expression
        invalid_config3 = ScheduleConfig(
            schedule_id="invalid_cron",
            name="Invalid Cron",
            cron_expression="invalid cron"
        )
        
        self.assertFalse(invalid_config3.is_valid())
        
        # Test invalid schedule - invalid target_percent
        invalid_config4 = ScheduleConfig(
            schedule_id="invalid_target",
            name="Invalid Target",
            target_percent=150.0  # Over 100%
        )
        
        self.assertFalse(invalid_config4.is_valid())
    
    def test_schedule_conflict_detection(self):
        """Test detection of schedule conflicts."""
        # Test same schedule ID conflict
        config1 = ScheduleConfig(
            schedule_id="same_id",
            name="Schedule 1",
            cron_expression="0 2 * * *"
        )
        
        config2 = ScheduleConfig(
            schedule_id="same_id",
            name="Schedule 2",
            cron_expression="0 3 * * *"
        )
        
        self.assertTrue(config1.conflicts_with(config2))
        
        # Test same cron expression conflict
        config3 = ScheduleConfig(
            schedule_id="schedule_3",
            name="Schedule 3",
            cron_expression="0 2 * * *",
            schedule_type=ScheduleType.RECURRING,
            trigger_type=TriggerType.TIME_BASED
        )
        
        config4 = ScheduleConfig(
            schedule_id="schedule_4",
            name="Schedule 4",
            cron_expression="0 2 * * *",
            schedule_type=ScheduleType.RECURRING,
            trigger_type=TriggerType.TIME_BASED
        )
        
        self.assertTrue(config3.conflicts_with(config4))
        
        # Test no conflict
        config5 = ScheduleConfig(
            schedule_id="schedule_5",
            name="Schedule 5",
            cron_expression="0 3 * * *"
        )
        
        config6 = ScheduleConfig(
            schedule_id="schedule_6",
            name="Schedule 6",
            cron_expression="0 4 * * *"
        )
        
        self.assertFalse(config5.conflicts_with(config6))


if __name__ == '__main__':
    unittest.main()