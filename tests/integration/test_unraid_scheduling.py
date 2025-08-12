#!/usr/bin/env python3
"""
Integration tests for Unraid-specific scheduling functionality.

Tests integration with Unraid user scripts, maintenance windows,
notification system, and scheduling templates.
"""

import unittest
import tempfile
import json
import time
import os
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch, MagicMock, mock_open

# Import components
try:
    from scheduler import (
        ScheduleConfig, ScheduleType, SchedulingEngine,
        ScheduleMonitor, ExecutionStatus
    )
    from unraid_integration import (
        UnraidIntegrationManager, UnraidSystemMonitor
    )
except ImportError:
    # Skip tests if modules not available
    import sys
    print("Required modules not available - skipping Unraid scheduling tests")
    sys.exit(0)


class TestUnraidUserScriptIntegration(unittest.TestCase):
    """Test integration with Unraid user scripts."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.user_scripts_path = os.path.join(self.temp_dir, "user.scripts")
        self.scripts_dir = os.path.join(self.user_scripts_path, "scripts")
        
        # Create user scripts directory structure
        os.makedirs(self.scripts_dir, exist_ok=True)
        
        # Create mock user scripts
        self.create_mock_user_script("existing_script", "#!/bin/bash\necho 'existing script'")
        
        self.integration_manager = UnraidIntegrationManager(
            user_scripts_path=self.user_scripts_path
        )
        
    def tearDown(self):
        """Clean up test fixtures."""
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def create_mock_user_script(self, name, content):
        """Create a mock user script."""
        script_dir = os.path.join(self.scripts_dir, name)
        os.makedirs(script_dir, exist_ok=True)
        
        script_file = os.path.join(script_dir, "script")
        with open(script_file, 'w') as f:
            f.write(content)
        os.chmod(script_file, 0o755)
        
        # Create description file
        desc_file = os.path.join(script_dir, "description")
        with open(desc_file, 'w') as f:
            f.write(f"Description for {name}")
    
    def test_get_user_scripts(self):
        """Test retrieving existing user scripts."""
        scripts = self.integration_manager.get_user_scripts()
        
        self.assertEqual(len(scripts), 1)
        self.assertEqual(scripts[0]['name'], 'existing_script')
        self.assertEqual(scripts[0]['description'], 'Description for existing_script')
        self.assertTrue(scripts[0]['executable'])
    
    def test_create_rebalancer_user_script(self):
        """Test creating rebalancer user script."""
        schedule_config = {
            'name': 'daily_rebalance',
            'cron_expression': '0 2 * * *',
            'command': ['python3', '/boot/config/plugins/unraid-rebalancer/unraid_rebalancer.py', '--target', '80'],
            'description': 'Daily rebalancing at 2 AM'
        }
        
        result = self.integration_manager.create_rebalancer_user_script(schedule_config)
        self.assertTrue(result)
        
        # Verify script was created
        script_dir = os.path.join(self.scripts_dir, 'daily_rebalance')
        self.assertTrue(os.path.exists(script_dir))
        
        script_file = os.path.join(script_dir, 'script')
        self.assertTrue(os.path.exists(script_file))
        
        # Check script content
        with open(script_file, 'r') as f:
            content = f.read()
        
        self.assertIn('#!/bin/bash', content)
        self.assertIn('unraid_rebalancer.py', content)
        self.assertIn('--target 80', content)
        
        # Check description file
        desc_file = os.path.join(script_dir, 'description')
        with open(desc_file, 'r') as f:
            description = f.read()
        
        self.assertEqual(description.strip(), 'Daily rebalancing at 2 AM')
    
    def test_create_user_script_with_schedule(self):
        """Test creating user script with cron schedule."""
        schedule_config = {
            'name': 'weekly_cleanup',
            'cron_expression': '0 3 * * 0',
            'command': ['python3', '/boot/config/plugins/unraid-rebalancer/unraid_rebalancer.py', '--cleanup'],
            'description': 'Weekly cleanup on Sunday'
        }
        
        result = self.integration_manager.create_rebalancer_user_script(
            schedule_config, 
            include_cron=True
        )
        self.assertTrue(result)
        
        # Check script includes cron setup
        script_file = os.path.join(self.scripts_dir, 'weekly_cleanup', 'script')
        with open(script_file, 'r') as f:
            content = f.read()
        
        self.assertIn('crontab', content)
        self.assertIn('0 3 * * 0', content)
    
    def test_update_existing_user_script(self):
        """Test updating an existing user script."""
        # Create initial script
        initial_config = {
            'name': 'test_script',
            'cron_expression': '0 2 * * *',
            'command': ['echo', 'initial'],
            'description': 'Initial description'
        }
        
        self.integration_manager.create_rebalancer_user_script(initial_config)
        
        # Update script
        updated_config = {
            'name': 'test_script',
            'cron_expression': '0 3 * * *',
            'command': ['echo', 'updated'],
            'description': 'Updated description'
        }
        
        result = self.integration_manager.create_rebalancer_user_script(updated_config)
        self.assertTrue(result)
        
        # Verify update
        script_file = os.path.join(self.scripts_dir, 'test_script', 'script')
        with open(script_file, 'r') as f:
            content = f.read()
        
        self.assertIn('echo updated', content)
        self.assertNotIn('echo initial', content)
        
        desc_file = os.path.join(self.scripts_dir, 'test_script', 'description')
        with open(desc_file, 'r') as f:
            description = f.read()
        
        self.assertEqual(description.strip(), 'Updated description')
    
    def test_user_script_permissions(self):
        """Test that user scripts have correct permissions."""
        schedule_config = {
            'name': 'permission_test',
            'cron_expression': '0 2 * * *',
            'command': ['echo', 'test'],
            'description': 'Permission test script'
        }
        
        self.integration_manager.create_rebalancer_user_script(schedule_config)
        
        script_file = os.path.join(self.scripts_dir, 'permission_test', 'script')
        
        # Check file is executable
        self.assertTrue(os.access(script_file, os.X_OK))
        
        # Check specific permissions (should be 755)
        import stat
        file_stat = os.stat(script_file)
        permissions = stat.filemode(file_stat.st_mode)
        self.assertEqual(permissions, '-rwxr-xr-x')


class TestUnraidMaintenanceWindows(unittest.TestCase):
    """Test Unraid maintenance window functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.maintenance_config_path = os.path.join(self.temp_dir, "maintenance.conf")
        
        # Create maintenance configuration
        maintenance_config = '''
# Maintenance windows configuration
# Format: day_of_week:start_hour:end_hour
# 0=Sunday, 1=Monday, etc.
0:02:06  # Sunday 2 AM to 6 AM
3:01:05  # Wednesday 1 AM to 5 AM
6:23:02  # Saturday 11 PM to 2 AM next day
'''
        
        with open(self.maintenance_config_path, 'w') as f:
            f.write(maintenance_config)
        
        self.integration_manager = UnraidIntegrationManager(
            maintenance_config_path=self.maintenance_config_path
        )
    
    def tearDown(self):
        """Clean up test fixtures."""
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_parse_maintenance_config(self):
        """Test parsing maintenance configuration."""
        windows = self.integration_manager._parse_maintenance_config()
        
        self.assertEqual(len(windows), 3)
        
        # Check Sunday window
        sunday_window = next(w for w in windows if w['day'] == 0)
        self.assertEqual(sunday_window['start_hour'], 2)
        self.assertEqual(sunday_window['end_hour'], 6)
        
        # Check Wednesday window
        wednesday_window = next(w for w in windows if w['day'] == 3)
        self.assertEqual(wednesday_window['start_hour'], 1)
        self.assertEqual(wednesday_window['end_hour'], 5)
        
        # Check Saturday window (crosses midnight)
        saturday_window = next(w for w in windows if w['day'] == 6)
        self.assertEqual(saturday_window['start_hour'], 23)
        self.assertEqual(saturday_window['end_hour'], 2)
    
    def test_is_maintenance_window_during_window(self):
        """Test maintenance window detection during active window."""
        # Mock time to Sunday 3 AM (during maintenance window)
        with patch('time.localtime') as mock_time:
            mock_time.return_value = time.struct_time((
                2024, 1, 7, 3, 0, 0, 6, 7, 0  # Sunday 3 AM
            ))
            
            result = self.integration_manager.is_maintenance_window()
            self.assertTrue(result)
    
    def test_is_maintenance_window_outside_window(self):
        """Test maintenance window detection outside active window."""
        # Mock time to Sunday 8 AM (outside maintenance window)
        with patch('time.localtime') as mock_time:
            mock_time.return_value = time.struct_time((
                2024, 1, 7, 8, 0, 0, 6, 7, 0  # Sunday 8 AM
            ))
            
            result = self.integration_manager.is_maintenance_window()
            self.assertFalse(result)
    
    def test_is_maintenance_window_cross_midnight(self):
        """Test maintenance window detection for windows crossing midnight."""
        # Test Saturday 11:30 PM (during cross-midnight window)
        with patch('time.localtime') as mock_time:
            mock_time.return_value = time.struct_time((
                2024, 1, 6, 23, 30, 0, 5, 6, 0  # Saturday 11:30 PM
            ))
            
            result = self.integration_manager.is_maintenance_window()
            self.assertTrue(result)
        
        # Test Sunday 1 AM (continuation of Saturday's cross-midnight window)
        with patch('time.localtime') as mock_time:
            mock_time.return_value = time.struct_time((
                2024, 1, 7, 1, 0, 0, 6, 7, 0  # Sunday 1 AM
            ))
            
            result = self.integration_manager.is_maintenance_window()
            self.assertTrue(result)
    
    def test_maintenance_window_with_no_config(self):
        """Test maintenance window behavior with no configuration file."""
        # Create manager with non-existent config file
        manager = UnraidIntegrationManager(
            maintenance_config_path="/nonexistent/path"
        )
        
        # Should return False when no config exists
        result = manager.is_maintenance_window()
        self.assertFalse(result)
    
    def test_maintenance_window_with_invalid_config(self):
        """Test maintenance window behavior with invalid configuration."""
        # Create invalid config
        invalid_config_path = os.path.join(self.temp_dir, "invalid.conf")
        with open(invalid_config_path, 'w') as f:
            f.write("invalid config content\n")
        
        manager = UnraidIntegrationManager(
            maintenance_config_path=invalid_config_path
        )
        
        # Should handle invalid config gracefully
        result = manager.is_maintenance_window()
        self.assertFalse(result)


class TestUnraidSchedulingTemplates(unittest.TestCase):
    """Test Unraid-specific scheduling templates."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.integration_manager = UnraidIntegrationManager()
        
    def tearDown(self):
        """Clean up test fixtures."""
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_get_scheduling_templates(self):
        """Test retrieving built-in scheduling templates."""
        templates = self.integration_manager.get_scheduling_templates()
        
        self.assertIsInstance(templates, list)
        self.assertGreater(len(templates), 0)
        
        # Check template structure
        template = templates[0]
        required_fields = ['name', 'description', 'cron_expression', 'command_template']
        for field in required_fields:
            self.assertIn(field, template)
    
    def test_daily_rebalance_template(self):
        """Test daily rebalance template."""
        templates = self.integration_manager.get_scheduling_templates()
        daily_template = next(
            (t for t in templates if 'daily' in t['name'].lower()), 
            None
        )
        
        self.assertIsNotNone(daily_template)
        self.assertIn('rebalance', daily_template['name'].lower())
        self.assertEqual(daily_template['cron_expression'], '0 2 * * *')
        self.assertIn('unraid_rebalancer.py', ' '.join(daily_template['command_template']))
    
    def test_weekly_cleanup_template(self):
        """Test weekly cleanup template."""
        templates = self.integration_manager.get_scheduling_templates()
        weekly_template = next(
            (t for t in templates if 'weekly' in t['name'].lower()), 
            None
        )
        
        self.assertIsNotNone(weekly_template)
        self.assertIn('cleanup', weekly_template['name'].lower())
        self.assertEqual(weekly_template['cron_expression'], '0 3 * * 0')
    
    def test_create_template_schedule(self):
        """Test creating schedule from template."""
        template = {
            'name': 'test_template',
            'description': 'Test template',
            'cron_expression': '0 {hour} * * *',
            'command_template': [
                'python3', 
                '/boot/config/plugins/unraid-rebalancer/unraid_rebalancer.py',
                '--target', '{target_percent}',
                '--mode', '{mode}'
            ],
            'default_params': {
                'hour': '2',
                'target_percent': '80',
                'mode': 'balanced'
            }
        }
        
        schedule = self.integration_manager.create_template_schedule(
            template,
            name='custom_rebalance',
            params={'hour': '3', 'target_percent': '85'}
        )
        
        self.assertEqual(schedule['name'], 'custom_rebalance')
        self.assertEqual(schedule['cron_expression'], '0 3 * * *')
        self.assertIn('--target', schedule['command'])
        self.assertIn('85', schedule['command'])
        self.assertIn('--mode', schedule['command'])
        self.assertIn('balanced', schedule['command'])  # Default value used
    
    def test_template_parameter_validation(self):
        """Test validation of template parameters."""
        template = {
            'name': 'validation_template',
            'description': 'Template for validation testing',
            'cron_expression': '0 {hour} * * *',
            'command_template': ['echo', '{required_param}'],
            'required_params': ['required_param'],
            'default_params': {}
        }
        
        # Should fail without required parameter
        with self.assertRaises(ValueError):
            self.integration_manager.create_template_schedule(
                template,
                name='invalid_schedule',
                params={}  # Missing required_param
            )
        
        # Should succeed with required parameter
        schedule = self.integration_manager.create_template_schedule(
            template,
            name='valid_schedule',
            params={'required_param': 'test_value'}
        )
        
        self.assertEqual(schedule['name'], 'valid_schedule')
        self.assertIn('test_value', schedule['command'])


class TestUnraidNotificationIntegration(unittest.TestCase):
    """Test integration with Unraid notification system."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.integration_manager = UnraidIntegrationManager()
    
    @patch('subprocess.run')
    def test_send_unraid_notification_success(self, mock_run):
        """Test sending successful Unraid notification."""
        mock_run.return_value = MagicMock(returncode=0)
        
        result = self.integration_manager.send_notification(
            subject="Test Notification",
            message="This is a test message",
            importance="normal"
        )
        
        self.assertTrue(result)
        mock_run.assert_called_once()
        
        # Check command structure
        call_args = mock_run.call_args[0][0]
        self.assertIn('/usr/local/emhttp/webGui/scripts/notify', call_args)
        self.assertIn('Test Notification', call_args)
        self.assertIn('This is a test message', call_args)
    
    @patch('subprocess.run')
    def test_send_unraid_notification_failure(self, mock_run):
        """Test handling of Unraid notification failure."""
        mock_run.return_value = MagicMock(returncode=1)
        
        result = self.integration_manager.send_notification(
            subject="Test Notification",
            message="This is a test message"
        )
        
        self.assertFalse(result)
    
    def test_format_schedule_notification(self):
        """Test formatting of schedule-specific notifications."""
        execution_result = {
            'schedule_name': 'daily_rebalance',
            'status': 'success',
            'start_time': time.time() - 3600,
            'end_time': time.time(),
            'output': 'Rebalancing completed successfully'
        }
        
        subject, message = self.integration_manager.format_schedule_notification(
            execution_result
        )
        
        self.assertIn('daily_rebalance', subject)
        self.assertIn('success', subject.lower())
        self.assertIn('daily_rebalance', message)
        self.assertIn('Rebalancing completed successfully', message)
        self.assertIn('Duration:', message)
    
    def test_format_error_notification(self):
        """Test formatting of error notifications."""
        execution_result = {
            'schedule_name': 'daily_rebalance',
            'status': 'failed',
            'start_time': time.time() - 1800,
            'end_time': time.time(),
            'error_message': 'Disk not found',
            'exit_code': 1
        }
        
        subject, message = self.integration_manager.format_schedule_notification(
            execution_result
        )
        
        self.assertIn('failed', subject.lower())
        self.assertIn('ERROR', subject)
        self.assertIn('Disk not found', message)
        self.assertIn('Exit code: 1', message)


class TestUnraidSystemIntegration(unittest.TestCase):
    """Test integration with Unraid system monitoring."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.system_monitor = UnraidSystemMonitor()
    
    @patch('subprocess.run')
    def test_get_array_status(self, mock_run):
        """Test getting Unraid array status."""
        # Mock mdcmd output
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout="mdState=STARTED\nmdNumDisks=8\nmdNumInvalid=0\n"
        )
        
        status = self.system_monitor.get_array_status()
        
        self.assertEqual(status['state'], 'STARTED')
        self.assertEqual(status['num_disks'], 8)
        self.assertEqual(status['num_invalid'], 0)
    
    @patch('subprocess.run')
    def test_get_disk_details(self, mock_run):
        """Test getting disk details."""
        # Mock mdcmd output for disk details
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout="rdevName.0=sda\nrdevSize.0=2000398934016\nrdevStatus.0=DISK_OK\n"
        )
        
        details = self.system_monitor.get_disk_details()
        
        self.assertIn('disk0', details)
        self.assertEqual(details['disk0']['device'], 'sda')
        self.assertEqual(details['disk0']['size'], 2000398934016)
        self.assertEqual(details['disk0']['status'], 'DISK_OK')
    
    @patch('os.path.exists')
    @patch('os.listdir')
    def test_get_user_shares(self, mock_listdir, mock_exists):
        """Test getting user shares information."""
        mock_exists.return_value = True
        mock_listdir.return_value = ['Movies', 'Music', 'Documents']
        
        with patch('os.path.getsize') as mock_getsize:
            mock_getsize.return_value = 1024 * 1024 * 1024  # 1GB
            
            shares = self.system_monitor.get_user_shares()
            
            self.assertEqual(len(shares), 3)
            self.assertIn('Movies', [s['name'] for s in shares])
            self.assertIn('Music', [s['name'] for s in shares])
            self.assertIn('Documents', [s['name'] for s in shares])
    
    @patch('subprocess.run')
    def test_check_parity_status(self, mock_run):
        """Test checking parity status."""
        # Mock mdcmd output for parity check
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout="mdResync=0\nmdResyncPos=0\nmdResyncSize=0\n"
        )
        
        parity_status = self.system_monitor.check_parity_status()
        
        self.assertFalse(parity_status['in_progress'])
        self.assertEqual(parity_status['position'], 0)
        self.assertEqual(parity_status['size'], 0)
    
    def test_is_safe_for_rebalancing(self):
        """Test safety check for rebalancing operations."""
        # Mock safe conditions
        with patch.object(self.system_monitor, 'get_array_status') as mock_array:
            with patch.object(self.system_monitor, 'check_parity_status') as mock_parity:
                mock_array.return_value = {'state': 'STARTED', 'num_invalid': 0}
                mock_parity.return_value = {'in_progress': False}
                
                result = self.system_monitor.is_safe_for_rebalancing()
                self.assertTrue(result)
        
        # Mock unsafe conditions (parity check in progress)
        with patch.object(self.system_monitor, 'get_array_status') as mock_array:
            with patch.object(self.system_monitor, 'check_parity_status') as mock_parity:
                mock_array.return_value = {'state': 'STARTED', 'num_invalid': 0}
                mock_parity.return_value = {'in_progress': True}
                
                result = self.system_monitor.is_safe_for_rebalancing()
                self.assertFalse(result)


if __name__ == '__main__':
    unittest.main()