#!/usr/bin/env python3
"""
Unit tests for cron integration functionality.

Tests cron job creation, management, crontab manipulation,
and schedule installation/removal.
"""

import unittest
import tempfile
import subprocess
import os
from pathlib import Path
from unittest.mock import patch, MagicMock, call

# Import scheduler components
try:
    from scheduler import (
        ScheduleConfig, ScheduleType, SchedulingEngine, ScheduleMonitor,
        CronExpressionValidator
    )
except ImportError:
    # Skip tests if scheduler module not available
    import sys
    print("Scheduler module not available - skipping cron integration tests")
    sys.exit(0)


class TestCronIntegration(unittest.TestCase):
    """Test cron integration functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.test_crontab = Path(self.temp_dir) / "test_crontab"
        
        # Create test schedule
        self.test_schedule = ScheduleConfig(
            schedule_id="test_rebalance",
            name="test_rebalance",
            schedule_type=ScheduleType.RECURRING,
            cron_expression="0 2 * * *",
            target_percent=80.0
        )
        
    def tearDown(self):
        """Clean up test fixtures."""
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    @patch('subprocess.run')
    def test_cron_job_creation(self, mock_run):
        """Test creation of cron jobs."""
        # Mock successful crontab command
        mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
        
        engine = SchedulingEngine(script_path=Path("/test/script.py"))
        result = engine.create_and_install_schedule(self.test_schedule)
        
        self.assertTrue(result)
        
        # Verify crontab command was called
        mock_run.assert_called()
        
        # Check that the command includes crontab
        call_args = mock_run.call_args[0][0]
        self.assertIn('crontab', ' '.join(call_args))
    
    @patch('subprocess.run')
    def test_cron_job_removal(self, mock_run):
        """Test removal of cron jobs."""
        # Mock existing crontab content
        existing_crontab = (
            "# Unraid Rebalancer Schedule: test_rebalance\n"
            "0 2 * * * python /path/to/unraid_rebalancer.py --target 80\n"
            "# Other cron job\n"
            "0 1 * * * /some/other/command\n"
        )
        
        mock_run.side_effect = [
            MagicMock(returncode=0, stdout=existing_crontab, stderr=""),  # crontab -l
            MagicMock(returncode=0, stdout="", stderr="")  # crontab -
        ]
        
        engine = SchedulingEngine(script_path=Path("/test/script.py"))
        result = engine.delete_schedule("test_rebalance")
        
        self.assertTrue(result)
        
        # Verify crontab commands were called
        self.assertEqual(mock_run.call_count, 2)
    
    @patch('subprocess.run')
    def test_crontab_parsing(self, mock_run):
        """Test parsing of existing crontab entries."""
        existing_crontab = (
            "# Unraid Rebalancer Schedule: daily_rebalance\n"
            "0 2 * * * python /path/to/unraid_rebalancer.py --target 80\n"
            "# Unraid Rebalancer Schedule: weekly_cleanup\n"
            "0 3 * * 0 python /path/to/unraid_rebalancer.py --cleanup\n"
            "# Other system job\n"
            "0 1 * * * /usr/bin/updatedb\n"
        )
        
        mock_run.return_value = MagicMock(returncode=0, stdout=existing_crontab, stderr="")
        
        engine = SchedulingEngine(script_path=Path("/test/script.py"))
        schedules = engine.list_installed_schedules()
        
        # Should find 2 rebalancer schedules
        self.assertEqual(len(schedules), 2)
        
        schedule_names = [s.name for s in schedules]
        self.assertIn("daily_rebalance", schedule_names)
        self.assertIn("weekly_cleanup", schedule_names)
    
    @patch('subprocess.run')
    def test_cron_job_update(self, mock_run):
        """Test updating existing cron jobs."""
        existing_crontab = (
            "# Unraid Rebalancer Schedule: test_rebalance\n"
            "0 2 * * * python /path/to/unraid_rebalancer.py --target 80\n"
        )
        
        mock_run.side_effect = [
            MagicMock(returncode=0, stdout=existing_crontab, stderr=""),  # crontab -l
            MagicMock(returncode=0, stdout="", stderr="")  # crontab -
        ]
        
        # Update schedule with new time
        updated_schedule = ScheduleConfig(
            schedule_id="test_rebalance_update",
            name="test_rebalance",
            schedule_type=ScheduleType.RECURRING,
            cron_expression="0 3 * * *",  # Changed from 2 AM to 3 AM
            target_percent=85.0
        )
        
        engine = SchedulingEngine(script_path=Path("/test/script.py"))
        result = engine.update_schedule(updated_schedule)
        
        self.assertTrue(result)
        
        # Verify crontab was read and written
        self.assertEqual(mock_run.call_count, 2)
    
    def test_cron_expression_generation(self):
        """Test generation of cron expressions from schedule configs."""
        engine = SchedulingEngine(script_path=Path("/test/script.py"))
        
        # Test daily schedule
        daily_config = ScheduleConfig(
            schedule_id="daily_test",
            name="daily",
            schedule_type=ScheduleType.RECURRING,
            cron_expression="0 2 * * *",
            target_percent=80.0
        )
        
        cron_line = engine.generate_cron_line(daily_config)
        self.assertIn("0 2 * * *", cron_line)
        self.assertIn("/test/script.py", cron_line)  # Should contain the script path
        self.assertIn("# Unraid Rebalancer Schedule: daily", cron_line)
    
    @patch('subprocess.run')
    def test_cron_error_handling(self, mock_run):
        """Test error handling for cron operations."""
        # Mock crontab command failure
        mock_run.return_value = MagicMock(
            returncode=1, 
            stdout="", 
            stderr="crontab: command not found"
        )
        
        engine = SchedulingEngine(script_path=Path("/test/script.py"))
        result = engine.create_and_install_schedule(self.test_schedule)
        
        self.assertFalse(result)
    
    @patch('subprocess.run')
    def test_cron_permission_handling(self, mock_run):
        """Test handling of cron permission issues."""
        # Mock permission denied error
        mock_run.return_value = MagicMock(
            returncode=1,
            stdout="",
            stderr="crontab: you are not allowed to use this program"
        )
        
        engine = SchedulingEngine(script_path=Path("/test/script.py"))
        result = engine.create_and_install_schedule(self.test_schedule)
        
        self.assertFalse(result)
    
    def test_cron_line_parsing(self):
        """Test parsing individual cron lines."""
        engine = SchedulingEngine(script_path=Path("/test/script.py"))
        
        # Test valid cron line
        cron_line = "0 2 * * * python /path/to/script.py --arg value"
        parsed = engine.parse_cron_line(cron_line)
        
        self.assertEqual(parsed['cron_expression'], "0 2 * * *")
        self.assertEqual(parsed['command'], "python /path/to/script.py --arg value")
        
        # Test invalid cron line
        invalid_line = "invalid cron line"
        parsed = engine.parse_cron_line(invalid_line)
        self.assertIsNone(parsed)
    
    @patch('subprocess.run')
    def test_schedule_validation_before_install(self, mock_run):
        """Test that schedules are validated before installation."""
        # Create invalid schedule
        invalid_schedule = ScheduleConfig(
            schedule_id="invalid_test",
            name="invalid",
            schedule_type=ScheduleType.RECURRING,
            cron_expression="invalid expression",
            target_percent=80.0
        )
        
        engine = SchedulingEngine(script_path=Path("/test/script.py"))
        result = engine.install_schedule(invalid_schedule)
        
        # Should fail validation before attempting cron installation
        self.assertFalse(result)
        mock_run.assert_not_called()
    
    @patch('subprocess.run')
    def test_backup_and_restore_crontab(self, mock_run):
        """Test backup and restore functionality for crontab."""
        original_crontab = (
            "# Original cron job\n"
            "0 1 * * * /usr/bin/updatedb\n"
        )
        
        mock_run.return_value = MagicMock(
            returncode=0, 
            stdout=original_crontab, 
            stderr=""
        )
        
        engine = SchedulingEngine(script_path=Path("/test/script.py"))
        
        # Test backup
        backup_file = Path(self.temp_dir) / "crontab_backup"
        result = engine.backup_crontab(backup_file)
        self.assertTrue(result)
        self.assertTrue(backup_file.exists())
        
        # Test restore
        mock_run.reset_mock()
        mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
        
        result = engine.restore_crontab(backup_file)
        self.assertTrue(result)
        mock_run.assert_called()


class TestCronExpressionHelpers(unittest.TestCase):
    """Test cron expression helper functions."""
    
    def test_daily_cron_helper(self):
        """Test daily cron expression helper."""
        engine = SchedulingEngine(script_path=Path("/test/script.py"))
        
        # Test daily at specific time
        expression = engine.create_daily_cron(hour=2, minute=30)
        self.assertEqual(expression, "30 2 * * *")
        
        # Test daily at midnight
        expression = engine.create_daily_cron(hour=0, minute=0)
        self.assertEqual(expression, "0 0 * * *")
    
    def test_weekly_cron_helper(self):
        """Test weekly cron expression helper."""
        engine = SchedulingEngine(script_path=Path("/test/script.py"))
        
        # Test weekly on Sunday at 2 AM
        expression = engine.create_weekly_cron(day_of_week=0, hour=2, minute=0)
        self.assertEqual(expression, "0 2 * * 0")
        
        # Test weekly on Friday at 11:30 PM
        expression = engine.create_weekly_cron(day_of_week=5, hour=23, minute=30)
        self.assertEqual(expression, "30 23 * * 5")
    
    def test_monthly_cron_helper(self):
        """Test monthly cron expression helper."""
        engine = SchedulingEngine(script_path=Path("/test/script.py"))
        
        # Test monthly on 1st at 3 AM
        expression = engine.create_monthly_cron(day=1, hour=3, minute=0)
        self.assertEqual(expression, "0 3 1 * *")
        
        # Test monthly on 15th at 6:30 PM
        expression = engine.create_monthly_cron(day=15, hour=18, minute=30)
        self.assertEqual(expression, "30 18 15 * *")
    
    def test_interval_cron_helper(self):
        """Test interval-based cron expression helper."""
        engine = SchedulingEngine(script_path=Path("/test/script.py"))
        
        # Test every 15 minutes
        expression = engine.create_interval_cron(minutes=15)
        self.assertEqual(expression, "*/15 * * * *")
        
        # Test every 2 hours
        expression = engine.create_interval_cron(hours=2)
        self.assertEqual(expression, "0 */2 * * *")
        
        # Test every 3 days
        expression = engine.create_interval_cron(days=3)
        self.assertEqual(expression, "0 0 */3 * *")


if __name__ == '__main__':
    unittest.main()