#!/usr/bin/env python3
"""
Unit tests for resource monitoring functionality.

Tests CPU, memory, disk I/O monitoring, resource thresholds,
and adaptive scheduling based on system load.
"""

import unittest
import time
from unittest.mock import patch, MagicMock

# Import scheduler components
try:
    from scheduler import (
        ResourceThresholds, SchedulingEngine, ScheduleMonitor,
        ScheduleConfig, ScheduleType, TriggerType
    )
except ImportError:
    # Skip tests if scheduler module not available
    import sys
    print("Scheduler module not available - skipping resource monitoring tests")
    sys.exit(0)


class TestResourceThresholds(unittest.TestCase):
    """Test ResourceThresholds class functionality."""
    
    def test_resource_thresholds_creation(self):
        """Test basic ResourceThresholds creation."""
        thresholds = ResourceThresholds(
            max_cpu_percent=80.0,
            max_memory_percent=70.0,
            max_disk_io_percent=60.0,
            max_network_mbps=100.0
        )
        
        self.assertEqual(thresholds.max_cpu_percent, 80.0)
        self.assertEqual(thresholds.max_memory_percent, 70.0)
        self.assertEqual(thresholds.max_disk_io_percent, 60.0)
        self.assertEqual(thresholds.max_network_mbps, 100.0)
    
    def test_resource_thresholds_defaults(self):
        """Test default values for ResourceThresholds."""
        thresholds = ResourceThresholds()
        
        # Should have reasonable defaults
        self.assertIsNotNone(thresholds.max_cpu_percent)
        self.assertIsNotNone(thresholds.max_memory_percent)
        self.assertIsNotNone(thresholds.max_disk_io_percent)
        
        # Defaults should be reasonable values
        self.assertGreater(thresholds.max_cpu_percent, 0)
        self.assertLess(thresholds.max_cpu_percent, 100)
    
    def test_resource_thresholds_validation(self):
        """Test validation of ResourceThresholds values."""
        # Valid thresholds
        valid_thresholds = ResourceThresholds(
            max_cpu_percent=75.0,
            max_memory_percent=80.0
        )
        self.assertTrue(valid_thresholds.is_valid())
        
        # Invalid thresholds (over 100%)
        invalid_thresholds = ResourceThresholds(
            max_cpu_percent=150.0,
            max_memory_percent=80.0
        )
        self.assertFalse(invalid_thresholds.is_valid())
        
        # Invalid thresholds (negative)
        invalid_thresholds = ResourceThresholds(
            max_cpu_percent=-10.0,
            max_memory_percent=80.0
        )
        self.assertFalse(invalid_thresholds.is_valid())


class TestResourceMonitoring(unittest.TestCase):
    """Test resource monitoring functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.monitor = ScheduleMonitor()
        
    @patch('psutil.cpu_percent')
    def test_cpu_monitoring(self, mock_cpu_percent):
        """Test CPU usage monitoring."""
        # Mock CPU usage at 75%
        mock_cpu_percent.return_value = 75.0
        
        cpu_usage = self.monitor.get_cpu_usage()
        self.assertEqual(cpu_usage, 75.0)
        
        # Verify psutil was called correctly
        mock_cpu_percent.assert_called_once()
    
    @patch('psutil.virtual_memory')
    def test_memory_monitoring(self, mock_virtual_memory):
        """Test memory usage monitoring."""
        # Mock memory usage at 60%
        mock_memory = MagicMock()
        mock_memory.percent = 60.0
        mock_virtual_memory.return_value = mock_memory
        
        memory_usage = self.monitor.get_memory_usage()
        self.assertEqual(memory_usage, 60.0)
        
        mock_virtual_memory.assert_called_once()
    
    @patch('psutil.disk_io_counters')
    def test_disk_io_monitoring(self, mock_disk_io):
        """Test disk I/O monitoring."""
        # Mock disk I/O counters
        mock_io = MagicMock()
        mock_io.read_bytes = 1000000  # 1MB
        mock_io.write_bytes = 2000000  # 2MB
        mock_disk_io.return_value = mock_io
        
        # First call to establish baseline
        self.monitor.get_disk_io_usage()
        
        # Second call with increased values
        mock_io.read_bytes = 2000000  # 2MB (1MB increase)
        mock_io.write_bytes = 4000000  # 4MB (2MB increase)
        
        # Wait a bit to simulate time passage
        time.sleep(0.1)
        
        io_usage = self.monitor.get_disk_io_usage()
        
        # Should return some positive I/O rate
        self.assertIsInstance(io_usage, dict)
        self.assertIn('read_bps', io_usage)
        self.assertIn('write_bps', io_usage)
    
    @patch('psutil.net_io_counters')
    def test_network_monitoring(self, mock_net_io):
        """Test network usage monitoring."""
        # Mock network I/O counters
        mock_net = MagicMock()
        mock_net.bytes_sent = 500000  # 500KB
        mock_net.bytes_recv = 1000000  # 1MB
        mock_net_io.return_value = mock_net
        
        # First call to establish baseline
        self.monitor.get_network_usage()
        
        # Second call with increased values
        mock_net.bytes_sent = 1000000  # 1MB (500KB increase)
        mock_net.bytes_recv = 2000000  # 2MB (1MB increase)
        
        time.sleep(0.1)
        
        network_usage = self.monitor.get_network_usage()
        
        self.assertIsInstance(network_usage, dict)
        self.assertIn('sent_bps', network_usage)
        self.assertIn('recv_bps', network_usage)
    
    def test_resource_threshold_checking(self):
        """Test checking resources against thresholds."""
        thresholds = ResourceThresholds(
            max_cpu_percent=80.0,
            max_memory_percent=70.0,
            max_disk_io_percent=60.0
        )
        
        # Test within thresholds
        current_resources = {
            'cpu_percent': 75.0,
            'memory_percent': 65.0,
            'disk_io_percent': 55.0
        }
        
        result = self.monitor.check_resource_thresholds(current_resources, thresholds)
        self.assertTrue(result['within_limits'])
        
        # Test exceeding thresholds
        current_resources = {
            'cpu_percent': 85.0,  # Exceeds 80% limit
            'memory_percent': 65.0,
            'disk_io_percent': 55.0
        }
        
        result = self.monitor.check_resource_thresholds(current_resources, thresholds)
        self.assertFalse(result['within_limits'])
        self.assertIn('cpu_percent', result['exceeded_limits'])


class TestAdaptiveScheduling(unittest.TestCase):
    """Test adaptive scheduling based on resource usage."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.engine = SchedulingEngine()
        
        # Create test schedule with resource thresholds
        self.resource_schedule = ScheduleConfig(
            name="adaptive_test",
            schedule_type=ScheduleType.CONDITIONAL,
            trigger_type=TriggerType.RESOURCE_BASED,
            resource_thresholds=ResourceThresholds(
                max_cpu_percent=80.0,
                max_memory_percent=70.0
            ),
            command=["python", "unraid_rebalancer.py", "--rsync-mode", "fast"]
        )
    
    @patch('scheduler.ScheduleMonitor.get_current_resources')
    def test_schedule_execution_with_low_resources(self, mock_get_resources):
        """Test schedule execution when resources are within limits."""
        # Mock low resource usage
        mock_get_resources.return_value = {
            'cpu_percent': 30.0,
            'memory_percent': 40.0,
            'disk_io_percent': 20.0
        }
        
        can_execute = self.engine.can_execute_schedule(self.resource_schedule)
        self.assertTrue(can_execute)
    
    @patch('scheduler.ScheduleMonitor.get_current_resources')
    def test_schedule_deferral_with_high_resources(self, mock_get_resources):
        """Test schedule deferral when resources exceed limits."""
        # Mock high resource usage
        mock_get_resources.return_value = {
            'cpu_percent': 90.0,  # Exceeds 80% limit
            'memory_percent': 40.0,
            'disk_io_percent': 20.0
        }
        
        can_execute = self.engine.can_execute_schedule(self.resource_schedule)
        self.assertFalse(can_execute)
    
    def test_rsync_mode_adaptation(self):
        """Test automatic rsync mode adaptation based on load."""
        # High CPU load should suggest fast mode
        high_load_resources = {
            'cpu_percent': 85.0,
            'memory_percent': 60.0
        }
        
        suggested_mode = self.engine.suggest_rsync_mode(high_load_resources)
        self.assertEqual(suggested_mode, "fast")
        
        # Low CPU load should allow integrity mode
        low_load_resources = {
            'cpu_percent': 20.0,
            'memory_percent': 30.0
        }
        
        suggested_mode = self.engine.suggest_rsync_mode(low_load_resources)
        self.assertEqual(suggested_mode, "integrity")
        
        # Medium load should suggest balanced mode
        medium_load_resources = {
            'cpu_percent': 50.0,
            'memory_percent': 45.0
        }
        
        suggested_mode = self.engine.suggest_rsync_mode(medium_load_resources)
        self.assertEqual(suggested_mode, "balanced")
    
    @patch('scheduler.ScheduleMonitor.get_current_resources')
    def test_operation_queuing_during_high_load(self, mock_get_resources):
        """Test operation queuing when system is under high load."""
        # Mock high resource usage
        mock_get_resources.return_value = {
            'cpu_percent': 95.0,
            'memory_percent': 85.0,
            'disk_io_percent': 80.0
        }
        
        # Try to execute schedule
        result = self.engine.execute_schedule(self.resource_schedule)
        
        # Should be queued, not executed immediately
        self.assertEqual(result['status'], 'queued')
        self.assertIn('reason', result)
    
    def test_priority_based_scheduling(self):
        """Test priority-based operation scheduling."""
        # Create schedules with different priorities
        high_priority = ScheduleConfig(
            name="high_priority",
            schedule_type=ScheduleType.CONDITIONAL,
            priority=1,
            command=["echo", "high"]
        )
        
        low_priority = ScheduleConfig(
            name="low_priority",
            schedule_type=ScheduleType.CONDITIONAL,
            priority=5,
            command=["echo", "low"]
        )
        
        # Add to queue
        self.engine.queue_schedule(low_priority)
        self.engine.queue_schedule(high_priority)
        
        # High priority should be first in queue
        next_schedule = self.engine.get_next_queued_schedule()
        self.assertEqual(next_schedule.name, "high_priority")


class TestResourceMonitoringIntegration(unittest.TestCase):
    """Test integration of resource monitoring with scheduling."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.monitor = ScheduleMonitor()
        self.engine = SchedulingEngine()
    
    @patch('psutil.cpu_percent')
    @patch('psutil.virtual_memory')
    def test_comprehensive_resource_check(self, mock_memory, mock_cpu):
        """Test comprehensive resource checking."""
        # Mock system resources
        mock_cpu.return_value = 45.0
        mock_memory.return_value = MagicMock(percent=55.0)
        
        resources = self.monitor.get_all_resources()
        
        self.assertIn('cpu_percent', resources)
        self.assertIn('memory_percent', resources)
        self.assertEqual(resources['cpu_percent'], 45.0)
        self.assertEqual(resources['memory_percent'], 55.0)
    
    def test_resource_history_tracking(self):
        """Test tracking of resource usage history."""
        # Add some resource samples
        self.monitor.add_resource_sample({
            'timestamp': time.time(),
            'cpu_percent': 50.0,
            'memory_percent': 60.0
        })
        
        self.monitor.add_resource_sample({
            'timestamp': time.time(),
            'cpu_percent': 55.0,
            'memory_percent': 65.0
        })
        
        history = self.monitor.get_resource_history()
        self.assertEqual(len(history), 2)
        
        # Test average calculation
        avg_cpu = self.monitor.get_average_cpu_usage()
        self.assertEqual(avg_cpu, 52.5)  # (50 + 55) / 2
    
    def test_resource_trend_analysis(self):
        """Test analysis of resource usage trends."""
        # Add increasing CPU usage samples
        timestamps = [time.time() - i for i in range(5, 0, -1)]
        cpu_values = [30.0, 40.0, 50.0, 60.0, 70.0]
        
        for timestamp, cpu in zip(timestamps, cpu_values):
            self.monitor.add_resource_sample({
                'timestamp': timestamp,
                'cpu_percent': cpu,
                'memory_percent': 50.0
            })
        
        trend = self.monitor.analyze_cpu_trend()
        self.assertEqual(trend, 'increasing')
        
        # Test prediction
        predicted_cpu = self.monitor.predict_cpu_usage(minutes=5)
        self.assertGreater(predicted_cpu, 70.0)  # Should predict higher usage


if __name__ == '__main__':
    unittest.main()