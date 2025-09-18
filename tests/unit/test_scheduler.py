#!/usr/bin/env python3
"""
Unit tests for the scheduling system.
"""

import json
import os
import tempfile
import time
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock

# Set up environment for testing
os.environ['PYTHONPATH'] = str(Path(__file__).parent.parent)

from scheduler import (
    ScheduleConfig, ScheduleType, TriggerType, ResourceThresholds,
    CronExpressionValidator, ScheduleManager, CronManager, SchedulingEngine,
    SystemResourceMonitor, ConditionalScheduler, ScheduleTemplateManager
)


class TestScheduleConfig(unittest.TestCase):
    """Test ScheduleConfig dataclass."""
    
    def test_schedule_config_creation(self):
        """Test basic schedule config creation."""
        config = ScheduleConfig(
            schedule_id="test_schedule",
            name="Test Schedule",
            cron_expression="0 2 * * *"
        )
        
        self.assertEqual(config.schedule_id, "test_schedule")
        self.assertEqual(config.name, "Test Schedule")
        self.assertEqual(config.cron_expression, "0 2 * * *")
        self.assertEqual(config.target_percent, 80.0)
        self.assertTrue(config.enabled)
        self.assertIsNotNone(config.resource_thresholds)
    
    def test_schedule_config_defaults(self):
        """Test default values are set correctly."""
        config = ScheduleConfig(
            schedule_id="test",
            name="Test"
        )
        
        self.assertEqual(config.schedule_type, ScheduleType.RECURRING)
        self.assertEqual(config.trigger_type, TriggerType.TIME_BASED)
        self.assertEqual(config.rsync_mode, "balanced")
        self.assertEqual(config.max_runtime_hours, 6)
        self.assertEqual(config.retry_count, 3)
        self.assertIsInstance(config.resource_thresholds, ResourceThresholds)


class TestCronExpressionValidator(unittest.TestCase):
    """Test cron expression validation."""
    
    def test_valid_expressions(self):
        """Test validation of valid cron expressions."""
        valid_expressions = [
            "0 2 * * *",           # Daily at 2 AM
            "30 14 * * 0",         # Weekly on Sunday at 2:30 PM
            "0 0 1 * *",           # Monthly on 1st at midnight
            "*/15 * * * *",        # Every 15 minutes
            "0 9-17 * * 1-5",      # Weekdays 9 AM to 5 PM
        ]
        
        for expr in valid_expressions:
            with self.subTest(expression=expr):
                self.assertTrue(CronExpressionValidator.validate_cron_expression(expr))
    
    def test_invalid_expressions(self):
        """Test validation of invalid cron expressions."""
        invalid_expressions = [
            "",                    # Empty
            "0 2 * *",            # Missing field
            "0 2 * * * *",        # Extra field
            "60 2 * * *",         # Invalid minute
            "0 25 * * *",         # Invalid hour
            "0 2 32 * *",         # Invalid day
            "0 2 * 13 *",         # Invalid month
            "0 2 * * 8",          # Invalid day of week
        ]
        
        for expr in invalid_expressions:
            with self.subTest(expression=expr):
                self.assertFalse(CronExpressionValidator.validate_cron_expression(expr))
    
    def test_helper_methods(self):
        """Test cron expression helper methods."""
        # Test daily expression
        daily_expr = CronExpressionValidator.create_daily_expression(14, 30)
        self.assertEqual(daily_expr, "30 14 * * *")
        
        # Test weekly expression
        weekly_expr = CronExpressionValidator.create_weekly_expression(0, 9)  # Sunday 9 AM
        self.assertEqual(weekly_expr, "0 9 * * 0")
        
        # Test monthly expression
        monthly_expr = CronExpressionValidator.create_monthly_expression(15, 12)  # 15th at noon
        self.assertEqual(monthly_expr, "0 12 15 * *")


class TestScheduleManager(unittest.TestCase):
    """Test schedule management."""
    
    def setUp(self):
        """Set up test environment."""
        self.temp_dir = Path(tempfile.mkdtemp())
        self.manager = ScheduleManager(self.temp_dir)
    
    def tearDown(self):
        """Clean up test environment."""
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_create_schedule(self):
        """Test schedule creation."""
        schedule = ScheduleConfig(
            schedule_id="test_schedule",
            name="Test Schedule",
            cron_expression="0 2 * * *"
        )
        
        result = self.manager.create_schedule(schedule)
        self.assertTrue(result)
        self.assertIn("test_schedule", self.manager.schedules)
        
        # Verify file was created
        config_file = self.temp_dir / "test_schedule.json"
        self.assertTrue(config_file.exists())
    
    def test_create_duplicate_schedule(self):
        """Test that duplicate schedule IDs are rejected."""
        schedule1 = ScheduleConfig(
            schedule_id="test_schedule",
            name="Test Schedule 1"
        )
        
        schedule2 = ScheduleConfig(
            schedule_id="test_schedule",
            name="Test Schedule 2"
        )
        
        self.assertTrue(self.manager.create_schedule(schedule1))
        self.assertFalse(self.manager.create_schedule(schedule2))
    
    def test_invalid_cron_expression(self):
        """Test that invalid cron expressions are rejected."""
        schedule = ScheduleConfig(
            schedule_id="test_schedule",
            name="Test Schedule",
            cron_expression="invalid cron"
        )
        
        result = self.manager.create_schedule(schedule)
        self.assertFalse(result)
    
    def test_update_schedule(self):
        """Test schedule updates."""
        original_schedule = ScheduleConfig(
            schedule_id="test_schedule",
            name="Original Name",
            target_percent=80.0
        )
        
        self.manager.create_schedule(original_schedule)
        
        updated_schedule = ScheduleConfig(
            schedule_id="test_schedule",
            name="Updated Name",
            target_percent=75.0
        )
        
        result = self.manager.update_schedule("test_schedule", updated_schedule)
        self.assertTrue(result)
        
        retrieved = self.manager.get_schedule("test_schedule")
        self.assertEqual(retrieved.name, "Updated Name")
        self.assertEqual(retrieved.target_percent, 75.0)
    
    def test_delete_schedule(self):
        """Test schedule deletion."""
        schedule = ScheduleConfig(
            schedule_id="test_schedule",
            name="Test Schedule"
        )
        
        self.manager.create_schedule(schedule)
        self.assertIsNotNone(self.manager.get_schedule("test_schedule"))
        
        result = self.manager.delete_schedule("test_schedule")
        self.assertTrue(result)
        self.assertIsNone(self.manager.get_schedule("test_schedule"))
        
        # Verify file was deleted
        config_file = self.temp_dir / "test_schedule.json"
        self.assertFalse(config_file.exists())
    
    def test_list_schedules(self):
        """Test listing schedules."""
        schedules = [
            ScheduleConfig(schedule_id="schedule1", name="Schedule 1", enabled=True),
            ScheduleConfig(schedule_id="schedule2", name="Schedule 2", enabled=False),
            ScheduleConfig(schedule_id="schedule3", name="Schedule 3", enabled=True),
        ]
        
        for schedule in schedules:
            self.manager.create_schedule(schedule)
        
        all_schedules = self.manager.list_schedules()
        self.assertEqual(len(all_schedules), 3)
        
        enabled_schedules = self.manager.list_enabled_schedules()
        self.assertEqual(len(enabled_schedules), 2)


class TestCronManager(unittest.TestCase):
    """Test cron job management."""
    
    def setUp(self):
        """Set up test environment."""
        self.script_path = Path("/test/script.py")
        self.cron_manager = CronManager(self.script_path)
    
    @patch('subprocess.run')
    def test_get_current_crontab_empty(self, mock_run):
        """Test getting empty crontab."""
        mock_run.return_value = Mock(returncode=1, stdout="", stderr="no crontab for user")
        
        crontab = self.cron_manager._get_current_crontab()
        self.assertEqual(crontab, [])
    
    @patch('subprocess.run')
    def test_get_current_crontab_with_content(self, mock_run):
        """Test getting crontab with existing content."""
        mock_run.return_value = Mock(
            returncode=0,
            stdout="# Comment\n0 2 * * * /some/command\n30 14 * * 0 /other/command\n",
            stderr=""
        )
        
        crontab = self.cron_manager._get_current_crontab()
        expected = [
            "# Comment",
            "0 2 * * * /some/command",
            "30 14 * * 0 /other/command"
        ]
        self.assertEqual(crontab, expected)
    
    def test_generate_cron_command(self):
        """Test cron command generation."""
        schedule = ScheduleConfig(
            schedule_id="test_schedule",
            name="Test Schedule",
            target_percent=75.0,
            rsync_mode="balanced",
            include_disks=["disk1", "disk2"],
            exclude_shares=["appdata"],
            max_runtime_hours=4
        )
        
        command = self.cron_manager._generate_cron_command(schedule)
        
        # Verify key parameters are included
        self.assertIn("--target-percent 75.0", command)
        self.assertIn("--rsync-mode balanced", command)
        self.assertIn("--include-disks disk1,disk2", command)
        self.assertIn("--exclude-shares appdata", command)
        self.assertIn("--schedule-id test_schedule", command)
        self.assertIn("--max-runtime 4", command)
        self.assertIn("--execute", command)
        self.assertIn("--metrics", command)
    
    def test_remove_schedule_from_crontab(self):
        """Test removing schedule entries from crontab list."""
        crontab_lines = [
            "# Other comment",
            "0 1 * * * /other/command",
            "# Unraid Rebalancer Schedule: test_schedule",
            "0 2 * * * /path/to/script.py --schedule-id test_schedule",
            "# Another comment",
            "0 3 * * * /another/command"
        ]
        
        original_length = len(crontab_lines)
        self.cron_manager._remove_schedule_from_crontab("test_schedule", crontab_lines)
        
        # Should remove 2 lines (comment + command)
        self.assertEqual(len(crontab_lines), original_length - 2)
        
        # Verify the right lines were removed
        self.assertNotIn("# Unraid Rebalancer Schedule: test_schedule", crontab_lines)
        self.assertIn("# Other comment", crontab_lines)
        self.assertIn("0 1 * * * /other/command", crontab_lines)


class TestSystemResourceMonitor(unittest.TestCase):
    """Test system resource monitoring."""
    
    @patch('scheduler.psutil')
    def test_get_current_usage_with_psutil(self, mock_psutil):
        """Test resource usage collection with psutil available."""
        # Mock psutil components
        mock_psutil.cpu_percent.return_value = 25.5
        mock_psutil.virtual_memory.return_value = Mock(percent=60.2)
        
        # Mock disk I/O
        mock_disk_io = Mock(read_bytes=1000000, write_bytes=2000000)
        mock_psutil.disk_io_counters.return_value = mock_disk_io
        
        # Mock network I/O
        mock_net_io = Mock(bytes_sent=500000, bytes_recv=800000)
        mock_psutil.net_io_counters.return_value = mock_net_io
        
        monitor = SystemResourceMonitor()
        usage = monitor.get_current_usage()
        
        self.assertEqual(usage['cpu_percent'], 25.5)
        self.assertEqual(usage['memory_percent'], 60.2)
        self.assertIn('disk_io_bps', usage)
        self.assertIn('timestamp', usage)
    
    def test_get_current_usage_without_psutil(self):
        """Test resource usage collection when psutil is not available."""
        with patch('scheduler.psutil', side_effect=ImportError):
            monitor = SystemResourceMonitor()
            usage = monitor.get_current_usage()
            
            # Should return zero values when psutil is not available
            self.assertEqual(usage['cpu_percent'], 0)
            self.assertEqual(usage['memory_percent'], 0)
            self.assertEqual(usage['disk_io_bps'], 0)
            self.assertIn('timestamp', usage)
    
    def test_check_resource_thresholds(self):
        """Test resource threshold checking."""
        monitor = SystemResourceMonitor()
        thresholds = ResourceThresholds(
            max_cpu_percent=50.0,
            max_memory_percent=80.0,
            max_disk_io_mbps=100.0
        )
        
        # Mock low resource usage
        with patch.object(monitor, 'get_current_usage') as mock_usage:
            mock_usage.return_value = {
                'cpu_percent': 30.0,
                'memory_percent': 60.0,
                'disk_io_mbps': 50.0
            }
            
            self.assertTrue(monitor.check_resource_thresholds(thresholds))
        
        # Mock high resource usage
        with patch.object(monitor, 'get_current_usage') as mock_usage:
            mock_usage.return_value = {
                'cpu_percent': 70.0,  # Exceeds 50% threshold
                'memory_percent': 60.0,
                'disk_io_mbps': 50.0
            }
            
            self.assertFalse(monitor.check_resource_thresholds(thresholds))


class TestConditionalScheduler(unittest.TestCase):
    """Test conditional scheduling logic."""
    
    def setUp(self):
        """Set up test environment."""
        self.scheduler = ConditionalScheduler()
    
    def test_time_based_schedule(self):
        """Test time-based schedule execution."""
        schedule = ScheduleConfig(
            schedule_id="test",
            name="Test",
            trigger_type=TriggerType.TIME_BASED
        )
        
        should_execute, reason = self.scheduler.should_execute_schedule(schedule)
        self.assertTrue(should_execute)
        self.assertEqual(reason, "Time-based schedule")
    
    def test_resource_based_schedule_ok(self):
        """Test resource-based schedule when resources are available."""
        schedule = ScheduleConfig(
            schedule_id="test",
            name="Test",
            trigger_type=TriggerType.RESOURCE_BASED,
            resource_thresholds=ResourceThresholds(max_cpu_percent=50.0)
        )
        
        with patch.object(self.scheduler.resource_monitor, 'check_resource_thresholds') as mock_check:
            mock_check.return_value = True
            
            should_execute, reason = self.scheduler.should_execute_schedule(schedule)
            self.assertTrue(should_execute)
            self.assertEqual(reason, "Resource conditions met")
    
    def test_resource_based_schedule_busy(self):
        """Test resource-based schedule when system is busy."""
        schedule = ScheduleConfig(
            schedule_id="test",
            name="Test",
            trigger_type=TriggerType.RESOURCE_BASED,
            resource_thresholds=ResourceThresholds(max_cpu_percent=50.0)
        )
        
        with patch.object(self.scheduler.resource_monitor, 'check_resource_thresholds') as mock_check:
            mock_check.return_value = False
            
            should_execute, reason = self.scheduler.should_execute_schedule(schedule)
            self.assertFalse(should_execute)
            self.assertEqual(reason, "System resources exceed thresholds")


class TestScheduleTemplateManager(unittest.TestCase):
    """Test schedule template management."""
    
    def test_nightly_template(self):
        """Test nightly schedule template."""
        template = ScheduleTemplateManager.get_nightly_template(hour=3)
        
        self.assertEqual(template.name, "Nightly Rebalance")
        self.assertEqual(template.cron_expression, "0 3 * * *")
        self.assertEqual(template.rsync_mode, "balanced")
        self.assertEqual(template.max_runtime_hours, 4)
        self.assertIsNotNone(template.resource_thresholds)
    
    def test_weekly_template(self):
        """Test weekly schedule template."""
        template = ScheduleTemplateManager.get_weekly_template(day=0, hour=2)  # Sunday 2 AM
        
        self.assertEqual(template.name, "Weekly Rebalance")
        self.assertEqual(template.cron_expression, "0 2 * * 0")
        self.assertEqual(template.rsync_mode, "integrity")
        self.assertEqual(template.max_runtime_hours, 8)
    
    def test_idle_template(self):
        """Test idle-based schedule template."""
        template = ScheduleTemplateManager.get_idle_template()
        
        self.assertEqual(template.name, "Idle System Rebalance")
        self.assertEqual(template.trigger_type, TriggerType.SYSTEM_IDLE)
        self.assertEqual(template.rsync_mode, "fast")
        self.assertEqual(template.resource_thresholds.min_idle_minutes, 30)
    
    def test_disk_usage_template(self):
        """Test disk usage threshold template."""
        template = ScheduleTemplateManager.get_disk_usage_template(threshold=85.0)
        
        self.assertEqual(template.name, "High Disk Usage Rebalance")
        self.assertEqual(template.trigger_type, TriggerType.DISK_USAGE)
        self.assertEqual(template.disk_usage_threshold, 85.0)
        self.assertEqual(template.target_percent, 75.0)


if __name__ == '__main__':
    unittest.main()