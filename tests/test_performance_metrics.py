#!/usr/bin/env python3
"""
Performance metrics tests for Unraid Rebalancer

Tests the performance monitoring, metrics collection, reporting, and visualization features.
"""

import json
import os
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock

# Add parent directory to path to import the main module
sys.path.insert(0, str(Path(__file__).parent.parent))

import unraid_rebalancer as ur


class TestTransferMetrics(unittest.TestCase):
    """Test TransferMetrics data structure."""
    
    def test_transfer_metrics_creation(self):
        """Test basic TransferMetrics creation."""
        transfer = ur.TransferMetrics(
            unit_path="Movies/Action/Movie1",
            src_disk="disk1",
            dest_disk="disk2",
            size_bytes=5000000000,
            start_time=1234567890.0
        )
        
        self.assertEqual(transfer.unit_path, "Movies/Action/Movie1")
        self.assertEqual(transfer.src_disk, "disk1")
        self.assertEqual(transfer.dest_disk, "disk2")
        self.assertEqual(transfer.size_bytes, 5000000000)
        self.assertEqual(transfer.start_time, 1234567890.0)
        self.assertIsNone(transfer.end_time)
        self.assertFalse(transfer.success)
    
    def test_transfer_metrics_duration(self):
        """Test duration calculation."""
        transfer = ur.TransferMetrics(
            unit_path="Movies/Movie1",
            src_disk="disk1",
            dest_disk="disk2",
            size_bytes=1000000000,
            start_time=1234567890.0,
            end_time=1234567950.0
        )
        
        self.assertEqual(transfer.duration_seconds, 60.0)
    
    def test_transfer_metrics_rate(self):
        """Test transfer rate calculation."""
        transfer = ur.TransferMetrics(
            unit_path="Movies/Movie1",
            src_disk="disk1",
            dest_disk="disk2",
            size_bytes=1048576000,  # 1000 MiB in bytes
            start_time=1234567890.0,
            transfer_rate_bps=104857600  # 100 MiB/s in bytes
        )
        
        self.assertEqual(transfer.transfer_rate_mbps, 100.0)


class TestSystemMetrics(unittest.TestCase):
    """Test SystemMetrics data structure."""
    
    def test_system_metrics_creation(self):
        """Test SystemMetrics creation."""
        metrics = ur.SystemMetrics(
            timestamp=1234567890.0,
            cpu_percent=25.5,
            memory_percent=60.2,
            disk_io_read_bps=50000000,
            disk_io_write_bps=75000000,
            network_sent_bps=1000000,
            network_recv_bps=2000000
        )
        
        self.assertEqual(metrics.timestamp, 1234567890.0)
        self.assertEqual(metrics.cpu_percent, 25.5)
        self.assertEqual(metrics.memory_percent, 60.2)
        self.assertEqual(metrics.disk_io_read_bps, 50000000)
        self.assertEqual(metrics.disk_io_write_bps, 75000000)
        self.assertEqual(metrics.network_sent_bps, 1000000)
        self.assertEqual(metrics.network_recv_bps, 2000000)


class TestOperationMetrics(unittest.TestCase):
    """Test OperationMetrics data structure and methods."""
    
    def setUp(self):
        """Set up test data."""
        self.operation = ur.OperationMetrics(
            operation_id="test_op_123",
            start_time=1234567890.0,
            end_time=1234567950.0,
            total_files=10,
            completed_files=8,
            failed_files=2,
            total_bytes=1000000000,
            transferred_bytes=800000000,
            rsync_mode="fast"
        )
    
    def test_operation_metrics_properties(self):
        """Test calculated properties."""
        self.assertEqual(self.operation.duration_seconds, 60.0)
        self.assertEqual(self.operation.success_rate, 0.8)
        self.assertAlmostEqual(self.operation.overall_transfer_rate_mbps, 12.716, places=2)
    
    def test_operation_metrics_to_dict(self):
        """Test dictionary conversion."""
        data = self.operation.to_dict()
        
        self.assertIsInstance(data, dict)
        self.assertEqual(data['operation_id'], "test_op_123")
        self.assertEqual(data['total_files'], 10)
        self.assertEqual(data['success_rate'], 0.8)
        self.assertIn('transfers', data)
        self.assertIn('system_samples', data)
    
    def test_operation_metrics_with_transfers(self):
        """Test operation with transfer data."""
        transfer1 = ur.TransferMetrics(
            unit_path="Movies/Movie1",
            src_disk="disk1",
            dest_disk="disk2",
            size_bytes=500000000,
            start_time=1234567890.0,
            end_time=1234567920.0,
            success=True,
            transfer_rate_bps=16666667  # ~16.7 MB/s
        )
        
        transfer2 = ur.TransferMetrics(
            unit_path="Movies/Movie2",
            src_disk="disk1",
            dest_disk="disk2",
            size_bytes=300000000,
            start_time=1234567920.0,
            end_time=1234567950.0,
            success=True,
            transfer_rate_bps=10000000  # 10 MB/s
        )
        
        self.operation.transfers = [transfer1, transfer2]
        data = self.operation.to_dict()
        
        self.assertEqual(len(data['transfers']), 2)
        self.assertEqual(data['transfers'][0]['unit_path'], "Movies/Movie1")
        self.assertTrue(data['transfers'][0]['success'])


@patch('psutil.cpu_percent')
@patch('psutil.virtual_memory')
@patch('psutil.disk_io_counters')
@patch('psutil.net_io_counters')
class TestPerformanceMonitor(unittest.TestCase):
    """Test PerformanceMonitor class."""
    
    def test_performance_monitor_creation(self, mock_net, mock_disk, mock_memory, mock_cpu):
        """Test PerformanceMonitor initialization."""
        mock_cpu.return_value = 25.0
        mock_memory.return_value = Mock(percent=60.0)
        mock_disk.return_value = Mock(read_bytes=1000, write_bytes=2000)
        mock_net.return_value = Mock(bytes_sent=500, bytes_recv=750)
        
        monitor = ur.PerformanceMonitor("test_op", rsync_mode="fast", metrics_enabled=True)
        
        self.assertEqual(monitor.operation.operation_id, "test_op")
        self.assertEqual(monitor.operation.rsync_mode, "fast")
        self.assertTrue(monitor.metrics_enabled)
        self.assertIsNone(monitor._monitoring_thread)
    
    def test_start_stop_monitoring(self, mock_net, mock_disk, mock_memory, mock_cpu):
        """Test starting and stopping monitoring."""
        mock_cpu.return_value = 25.0
        mock_memory.return_value = Mock(percent=60.0)
        mock_disk.return_value = Mock(read_bytes=1000, write_bytes=2000)
        mock_net.return_value = Mock(bytes_sent=500, bytes_recv=750)
        
        monitor = ur.PerformanceMonitor("test_op", sample_interval=0.1, metrics_enabled=True)
        
        # Start monitoring
        monitor.start_monitoring()
        self.assertIsNotNone(monitor._monitoring_thread)
        self.assertTrue(monitor._monitoring_thread.is_alive())
        
        # Let it collect a few samples
        time.sleep(0.3)
        
        # Stop monitoring
        monitor.stop_monitoring()
        self.assertIsNotNone(monitor.operation.end_time)
        
        # Check that samples were collected
        self.assertGreater(len(monitor.operation.system_samples), 0)
    
    def test_transfer_tracking(self, mock_net, mock_disk, mock_memory, mock_cpu):
        """Test transfer operation tracking."""
        mock_cpu.return_value = 25.0
        mock_memory.return_value = Mock(percent=60.0)
        mock_disk.return_value = Mock(read_bytes=1000, write_bytes=2000)
        mock_net.return_value = Mock(bytes_sent=500, bytes_recv=750)
        
        monitor = ur.PerformanceMonitor("test_op", metrics_enabled=True)
        
        # Create a test unit
        unit = ur.Unit("Movies", "Movie1", 1000000000, "disk1")
        
        # Start transfer
        transfer = monitor.start_transfer(unit, "disk2")
        
        self.assertEqual(len(monitor.operation.transfers), 1)
        self.assertEqual(monitor.operation.total_files, 1)
        self.assertEqual(monitor.operation.total_bytes, 1000000000)
        self.assertEqual(transfer.unit_path, "Movies/Movie1")
        self.assertEqual(transfer.src_disk, "disk1")
        self.assertEqual(transfer.dest_disk, "disk2")
        
        # Complete transfer successfully
        time.sleep(0.01)  # Small delay to ensure duration > 0
        monitor.complete_transfer(transfer, True)
        
        self.assertTrue(transfer.success)
        self.assertIsNotNone(transfer.end_time)
        self.assertIsNotNone(transfer.transfer_rate_bps)
        self.assertEqual(monitor.operation.completed_files, 1)
        self.assertEqual(monitor.operation.transferred_bytes, 1000000000)
    
    def test_transfer_failure(self, mock_net, mock_disk, mock_memory, mock_cpu):
        """Test transfer failure tracking."""
        mock_cpu.return_value = 25.0
        mock_memory.return_value = Mock(percent=60.0)
        mock_disk.return_value = Mock(read_bytes=1000, write_bytes=2000)
        mock_net.return_value = Mock(bytes_sent=500, bytes_recv=750)
        
        monitor = ur.PerformanceMonitor("test_op", metrics_enabled=True)
        
        unit = ur.Unit("Movies", "Movie1", 1000000000, "disk1")
        transfer = monitor.start_transfer(unit, "disk2")
        
        # Complete transfer with failure
        monitor.complete_transfer(transfer, False, "Disk full")
        
        self.assertFalse(transfer.success)
        self.assertEqual(transfer.error_message, "Disk full")
        self.assertEqual(monitor.operation.failed_files, 1)
        self.assertEqual(len(monitor.operation.errors), 1)
        self.assertIn("Disk full", monitor.operation.errors[0])
    
    def test_progress_info(self, mock_net, mock_disk, mock_memory, mock_cpu):
        """Test progress information calculation."""
        mock_cpu.return_value = 25.0
        mock_memory.return_value = Mock(percent=60.0)
        mock_disk.return_value = Mock(read_bytes=1000, write_bytes=2000)
        mock_net.return_value = Mock(bytes_sent=500, bytes_recv=750)
        
        monitor = ur.PerformanceMonitor("test_op", metrics_enabled=True)
        
        # Add some transfer data
        unit1 = ur.Unit("Movies", "Movie1", 1000000000, "disk1")
        unit2 = ur.Unit("Movies", "Movie2", 500000000, "disk1")
        
        transfer1 = monitor.start_transfer(unit1, "disk2")
        transfer2 = monitor.start_transfer(unit2, "disk2")
        
        # Complete first transfer
        time.sleep(0.01)
        monitor.complete_transfer(transfer1, True)
        
        progress = monitor.get_progress_info()
        
        self.assertEqual(progress['total_files'], 2)
        self.assertEqual(progress['completed_files'], 1)
        self.assertEqual(progress['failed_files'], 0)
        self.assertEqual(progress['progress_percent'], 50.0)
        self.assertEqual(progress['transferred_bytes'], 1000000000)
        self.assertEqual(progress['total_bytes'], 1500000000)
        self.assertIsNotNone(progress['current_transfer_rate_mbps'])
    
    def test_save_and_export_metrics(self, mock_net, mock_disk, mock_memory, mock_cpu):
        """Test saving metrics to file and CSV export."""
        mock_cpu.return_value = 25.0
        mock_memory.return_value = Mock(percent=60.0)
        mock_disk.return_value = Mock(read_bytes=1000, write_bytes=2000)
        mock_net.return_value = Mock(bytes_sent=500, bytes_recv=750)
        
        monitor = ur.PerformanceMonitor("test_op", metrics_enabled=True)
        
        # Add some test data
        unit = ur.Unit("Movies", "Movie1", 1000000000, "disk1")
        transfer = monitor.start_transfer(unit, "disk2")
        time.sleep(0.01)
        monitor.complete_transfer(transfer, True)
        
        with tempfile.TemporaryDirectory() as tmpdir:
            # Test JSON save
            json_path = Path(tmpdir) / "metrics.json"
            monitor.save_metrics(json_path)
            self.assertTrue(json_path.exists())
            
            # Verify JSON content
            with open(json_path) as f:
                data = json.load(f)
            self.assertEqual(data['operation_id'], "test_op")
            self.assertEqual(len(data['transfers']), 1)
            
            # Test CSV export
            csv_path = Path(tmpdir) / "metrics.csv"
            monitor.export_csv(csv_path)
            self.assertTrue(csv_path.exists())
            
            # Verify CSV content
            with open(csv_path) as f:
                content = f.read()
            self.assertIn("Unit Path,Source Disk,Dest Disk", content)
            self.assertIn("Movies/Movie1,disk1,disk2", content)


class TestMetricsReporter(unittest.TestCase):
    """Test MetricsReporter class."""
    
    def setUp(self):
        """Set up test data."""
        self.operation = ur.OperationMetrics(
            operation_id="test_report",
            start_time=1234567890.0,
            end_time=1234567950.0,
            total_files=5,
            completed_files=4,
            failed_files=1,
            total_bytes=2000000000,
            transferred_bytes=1600000000,
            average_transfer_rate_bps=26666667,
            peak_transfer_rate_bps=33333333,
            rsync_mode="balanced"
        )
        
        # Add some transfers
        self.operation.transfers = [
            ur.TransferMetrics(
                unit_path="Movies/Movie1",
                src_disk="disk1",
                dest_disk="disk2",
                size_bytes=800000000,
                start_time=1234567890.0,
                end_time=1234567920.0,
                success=True,
                transfer_rate_bps=26666667
            ),
            ur.TransferMetrics(
                unit_path="Movies/Movie2",
                src_disk="disk1",
                dest_disk="disk2",
                size_bytes=800000000,
                start_time=1234567920.0,
                end_time=1234567940.0,
                success=True,
                transfer_rate_bps=40000000
            ),
            ur.TransferMetrics(
                unit_path="Movies/Movie3",
                src_disk="disk1",
                dest_disk="disk2",
                size_bytes=400000000,
                start_time=1234567940.0,
                end_time=1234567950.0,
                success=False,
                error_message="Disk full"
            )
        ]
        
        # Add system samples
        self.operation.system_samples = [
            ur.SystemMetrics(1234567890.0, 30.0, 65.0, 50000000, 75000000),
            ur.SystemMetrics(1234567900.0, 45.0, 70.0, 60000000, 85000000),
            ur.SystemMetrics(1234567910.0, 35.0, 68.0, 55000000, 80000000)
        ]
        
        self.operation.errors = ["Movies/Movie3: Disk full"]
    
    def test_generate_summary_report(self):
        """Test summary report generation."""
        report = ur.MetricsReporter.generate_summary_report(self.operation)
        
        self.assertIsInstance(report, str)
        self.assertIn("UNRAID REBALANCER - OPERATION SUMMARY", report)
        self.assertIn("test_report", report)
        self.assertIn("Total Files: 5", report)
        self.assertIn("Completed: 4", report)
        self.assertIn("Failed: 1", report)
        self.assertIn("Success Rate: 80.0%", report)
        self.assertIn("Rsync Mode: balanced", report)
        self.assertIn("CPU Usage", report)
        self.assertIn("Memory Usage", report)
        self.assertIn("Movies/Movie1", report)
        self.assertIn("ERRORS:", report)
        self.assertIn("Disk full", report)
    
    def test_create_ascii_chart(self):
        """Test ASCII chart creation."""
        values = [10.0, 20.0, 15.0, 25.0, 30.0, 18.0]
        chart = ur.MetricsReporter.create_ascii_chart(values, "Test Chart")
        
        self.assertIsInstance(chart, str)
        self.assertIn("Test Chart:", chart)
        self.assertIn("Max: 30.0", chart)
        self.assertIn("Min: 10.0", chart)
        self.assertIn("Avg: 19.7", chart)
        self.assertIn("|", chart)  # Chart bars
        self.assertIn("+", chart)  # Chart axis
    
    def test_create_ascii_chart_empty(self):
        """Test ASCII chart with empty data."""
        chart = ur.MetricsReporter.create_ascii_chart([], "Empty Chart")
        self.assertIn("No data available", chart)
    
    def test_generate_performance_charts(self):
        """Test performance charts generation."""
        charts = ur.MetricsReporter.generate_performance_charts(self.operation)
        
        self.assertIsInstance(charts, str)
        self.assertIn("CPU Usage (%)", charts)
        self.assertIn("Memory Usage (%)", charts)
        self.assertIn("Disk Read (MB/s)", charts)
        self.assertIn("Disk Write (MB/s)", charts)
        self.assertIn("Transfer Rate (MB/s)", charts)
    
    def test_compare_operations(self):
        """Test operation comparison."""
        # Create second operation for comparison
        operation2 = ur.OperationMetrics(
            operation_id="test_report_2",
            start_time=1234568000.0,
            end_time=1234568080.0,
            total_files=3,
            completed_files=3,
            failed_files=0,
            total_bytes=1500000000,
            transferred_bytes=1500000000,
            average_transfer_rate_bps=18750000,
            peak_transfer_rate_bps=25000000,
            rsync_mode="fast"
        )
        
        operations = [self.operation, operation2]
        comparison = ur.MetricsReporter.compare_operations(operations)
        
        self.assertIsInstance(comparison, str)
        self.assertIn("OPERATION COMPARISON REPORT", comparison)
        self.assertIn("test_report", comparison)
        self.assertIn("test_report_2", comparison)
        self.assertIn("Best Performance:", comparison)
        self.assertIn("Best Success Rate:", comparison)
    
    def test_compare_operations_insufficient_data(self):
        """Test operation comparison with insufficient data."""
        comparison = ur.MetricsReporter.compare_operations([self.operation])
        self.assertIn("Need at least 2 operations", comparison)
    
    def test_load_metrics_from_file(self):
        """Test loading metrics from JSON file."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as tmp:
            json.dump(self.operation.to_dict(), tmp, default=str)
            tmp.flush()
            
            try:
                loaded_operation = ur.MetricsReporter.load_metrics_from_file(Path(tmp.name))
                
                self.assertEqual(loaded_operation.operation_id, "test_report")
                self.assertEqual(loaded_operation.total_files, 5)
                self.assertEqual(loaded_operation.rsync_mode, "balanced")
                self.assertEqual(len(loaded_operation.transfers), 3)
                self.assertEqual(len(loaded_operation.system_samples), 3)
                self.assertEqual(len(loaded_operation.errors), 1)
            finally:
                os.unlink(tmp.name)


class TestHistoricalAnalyzer(unittest.TestCase):
    """Test HistoricalAnalyzer class."""
    
    def setUp(self):
        """Set up test data."""
        self.temp_dir = tempfile.mkdtemp()
        self.metrics_dir = Path(self.temp_dir)
        self.analyzer = ur.HistoricalAnalyzer(self.metrics_dir)
        
        # Create test operations
        self.operations = [
            ur.OperationMetrics(
                operation_id="op1",
                start_time=1234567890.0,
                end_time=1234567950.0,
                total_files=5,
                completed_files=5,
                failed_files=0,
                total_bytes=2000000000,
                transferred_bytes=2000000000,
                rsync_mode="fast"
            ),
            ur.OperationMetrics(
                operation_id="op2",
                start_time=1234568000.0,
                end_time=1234568120.0,
                total_files=8,
                completed_files=7,
                failed_files=1,
                total_bytes=3000000000,
                transferred_bytes=2600000000,
                rsync_mode="balanced"
            )
        ]
        
        # Save operations to files
        for i, op in enumerate(self.operations):
            filepath = self.metrics_dir / f"metrics_op{i+1}.json"
            with open(filepath, 'w') as f:
                json.dump(op.to_dict(), f, default=str)
    
    def tearDown(self):
        """Clean up test data."""
        import shutil
        shutil.rmtree(self.temp_dir)
    
    def test_find_metrics_files(self):
        """Test finding metrics files."""
        files = self.analyzer.find_metrics_files()
        self.assertEqual(len(files), 2)
        self.assertTrue(all(f.suffix == '.json' for f in files))
    
    def test_load_all_operations(self):
        """Test loading all operations."""
        operations = self.analyzer.load_all_operations()
        self.assertEqual(len(operations), 2)
        self.assertEqual(operations[0].operation_id, "op1")
        self.assertEqual(operations[1].operation_id, "op2")
    
    def test_analyze_trends(self):
        """Test trend analysis."""
        trends = self.analyzer.analyze_trends()
        
        self.assertIsInstance(trends, dict)
        self.assertEqual(trends["total_operations"], 2)
        self.assertIn("date_range", trends)
        self.assertIn("success_rate", trends)
        self.assertIn("duration", trends)
    
    def test_analyze_trends_insufficient_data(self):
        """Test trend analysis with insufficient data."""
        # Remove one file
        files = list(self.metrics_dir.glob("*.json"))
        files[1].unlink()
        
        trends = self.analyzer.analyze_trends()
        self.assertIn("error", trends)
    
    def test_generate_recommendations(self):
        """Test recommendation generation."""
        recommendations = self.analyzer.generate_recommendations()
        
        self.assertIsInstance(recommendations, list)
        self.assertGreater(len(recommendations), 0)
        self.assertTrue(all(isinstance(rec, str) for rec in recommendations))
    
    def test_generate_recommendations_no_data(self):
        """Test recommendations with no historical data."""
        # Remove all files
        for file in self.metrics_dir.glob("*.json"):
            file.unlink()
        
        recommendations = self.analyzer.generate_recommendations()
        self.assertEqual(recommendations, ["No historical data available for recommendations"])


if __name__ == '__main__':
    # Configure logging for tests
    import logging
    logging.basicConfig(level=logging.CRITICAL)  # Suppress logs during tests
    
    # Run tests
    unittest.main(verbosity=2)