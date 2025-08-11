#!/usr/bin/env python3
"""
Integration tests for Unraid Rebalancer performance metrics

Tests the end-to-end integration of performance metrics with the main rebalancing workflow.
"""

import json
import os
import shutil
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock

# Add parent directory to path to import the main module
sys.path.insert(0, str(Path(__file__).parent.parent))

import unraid_rebalancer as ur


class TestMetricsWorkflowIntegration(unittest.TestCase):
    """Test integration of metrics with the main workflow."""
    
    def setUp(self):
        """Set up test environment."""
        self.temp_dir = tempfile.mkdtemp()
        self.metrics_dir = Path(self.temp_dir) / "metrics"
        self.metrics_dir.mkdir()
        
        # Create test data directories
        self.source_dir = Path(self.temp_dir) / "source"
        self.dest_dir = Path(self.temp_dir) / "dest"
        self.source_dir.mkdir()
        self.dest_dir.mkdir()
        
        # Create test files
        self.test_files = []
        for i in range(3):
            test_file = self.source_dir / f"testfile{i}.txt"
            test_file.write_text("x" * (1000 * (i + 1)))  # Files of different sizes
            self.test_files.append(test_file)
    
    def tearDown(self):
        """Clean up test environment."""
        shutil.rmtree(self.temp_dir)
    
    @patch('psutil.cpu_percent')
    @patch('psutil.virtual_memory')
    @patch('psutil.disk_io_counters')
    @patch('psutil.net_io_counters')
    def test_end_to_end_metrics_collection(self, mock_net, mock_disk, mock_memory, mock_cpu):
        """Test complete metrics collection during a rebalancing operation."""
        # Mock system metrics
        mock_cpu.return_value = 25.0
        mock_memory.return_value = Mock(percent=60.0)
        mock_disk.return_value = Mock(read_bytes=1000, write_bytes=2000)
        mock_net.return_value = Mock(bytes_sent=500, bytes_recv=750)
        
        # Create test units and disks
        disk1 = ur.Disk("disk1", Path("/mnt/disk1"), 1000000000, 900000000, 100000000)
        disk2 = ur.Disk("disk2", Path("/mnt/disk2"), 1000000000, 300000000, 700000000)
        
        units = [
            ur.Unit("Movies", "Movie1", 50000000, "disk1"),
            ur.Unit("Movies", "Movie2", 30000000, "disk1"),
        ]
        
        # Create plan
        plan = ur.Plan(
            moves=[
                ur.Move(units[0], "disk2"),
                ur.Move(units[1], "disk2")
            ],
            summary={"total_moves": 2, "total_bytes": 80000000.0}
        )
        
        # Initialize performance monitor
        monitor = ur.PerformanceMonitor(
            operation_id="integration_test",
            rsync_mode="fast",
            sample_interval=0.1,
            metrics_enabled=True
        )
        
        # Start monitoring
        monitor.start_monitoring()
        
        # Simulate transfers
        for move in plan.moves:
            transfer = monitor.start_transfer(move.unit, move.dest_disk)
            time.sleep(0.05)  # Simulate transfer time
            monitor.complete_transfer(transfer, True)
        
        # Stop monitoring
        monitor.stop_monitoring()
        
        # Verify metrics were collected
        self.assertEqual(len(monitor.operation.transfers), 2)
        self.assertEqual(monitor.operation.completed_files, 2)
        self.assertEqual(monitor.operation.transferred_bytes, 80000000)
        self.assertGreater(len(monitor.operation.system_samples), 0)
        
        # Test saving metrics
        metrics_file = self.metrics_dir / "test_metrics.json"
        monitor.save_metrics(metrics_file)
        self.assertTrue(metrics_file.exists())
        
        # Test loading metrics
        loaded_operation = ur.MetricsReporter.load_metrics_from_file(metrics_file)
        self.assertEqual(loaded_operation.operation_id, "integration_test")
        self.assertEqual(len(loaded_operation.transfers), 2)
    
    @patch('psutil.cpu_percent')
    @patch('psutil.virtual_memory')
    @patch('psutil.disk_io_counters')
    @patch('psutil.net_io_counters')
    def test_perform_plan_with_metrics(self, mock_net, mock_disk, mock_memory, mock_cpu):
        """Test perform_plan function with metrics integration."""
        # Mock system metrics
        mock_cpu.return_value = 30.0
        mock_memory.return_value = Mock(percent=65.0)
        mock_disk.return_value = Mock(read_bytes=2000, write_bytes=3000)
        mock_net.return_value = Mock(bytes_sent=1000, bytes_recv=1500)
        
        # Create mock units that point to real test files
        units = []
        for i, test_file in enumerate(self.test_files):
            unit = ur.Unit("TestShare", test_file.name, test_file.stat().st_size, "disk1")
            
            # Mock the paths to point to our test files
            with patch.object(unit, 'src_abs', return_value=test_file):
                dest_path = self.dest_dir / test_file.name
                with patch.object(unit, 'dest_abs', return_value=dest_path):
                    units.append(unit)
        
        # Create moves
        moves = [ur.Move(unit, "disk2") for unit in units]
        plan = ur.Plan(moves=moves, summary={"total_moves": len(moves), "total_bytes": sum(u.size_bytes for u in units)})
        
        # Initialize monitor
        monitor = ur.PerformanceMonitor("integration_test_plan", metrics_enabled=True, sample_interval=0.1)
        monitor.start_monitoring()
        
        # Mock rsync execution to simulate file operations instead of actual rsync
        def mock_run(cmd, dry_run=False):
            if not dry_run and cmd[0] == "rsync":
                # Simulate file copy by copying the source to destination
                src = Path(cmd[-2])  # Second to last argument (source)
                dst = Path(cmd[-1])  # Last argument (destination)
                if src.is_file():
                    shutil.copy2(src, dst)
                return 0
            return 0
        
        with patch('unraid_rebalancer.run', side_effect=mock_run):
            failures = ur.perform_plan(
                plan=plan,
                execute=True,
                rsync_extra=[],
                allow_merge=True,
                rsync_mode="fast",
                monitor=monitor,
                show_progress=True
            )
        
        monitor.stop_monitoring()
        
        # Verify no failures
        self.assertEqual(failures, 0)
        
        # Verify metrics collection
        self.assertEqual(len(monitor.operation.transfers), len(units))
        self.assertEqual(monitor.operation.completed_files, len(units))
        self.assertGreater(monitor.operation.transferred_bytes, 0)
        
        # Verify all transfers were successful
        for transfer in monitor.operation.transfers:
            self.assertTrue(transfer.success)
            self.assertIsNotNone(transfer.end_time)
            self.assertIsNotNone(transfer.transfer_rate_bps)
    
    def test_metrics_report_generation_integration(self):
        """Test integration of report generation with metrics data."""
        # Create sample operation with comprehensive data
        operation = ur.OperationMetrics(
            operation_id="integration_report_test",
            start_time=time.time() - 300,  # 5 minutes ago
            end_time=time.time(),
            total_files=10,
            completed_files=8,
            failed_files=2,
            total_bytes=5000000000,  # 5GB
            transferred_bytes=4000000000,  # 4GB
            average_transfer_rate_bps=13333333,  # ~13.3 MB/s
            peak_transfer_rate_bps=20000000,    # 20 MB/s
            rsync_mode="balanced"
        )
        
        # Add transfer data
        for i in range(8):  # 8 successful transfers
            transfer = ur.TransferMetrics(
                unit_path=f"Movies/Movie{i+1}",
                src_disk="disk1",
                dest_disk="disk2",
                size_bytes=500000000,  # 500MB each
                start_time=time.time() - 300 + i * 30,
                end_time=time.time() - 300 + (i + 1) * 30,
                success=True,
                transfer_rate_bps=16666667  # ~16.7 MB/s
            )
            operation.transfers.append(transfer)
        
        # Add failed transfers
        for i in range(2):  # 2 failed transfers
            transfer = ur.TransferMetrics(
                unit_path=f"Movies/FailedMovie{i+1}",
                src_disk="disk1",
                dest_disk="disk2",
                size_bytes=500000000,
                start_time=time.time() - 60 + i * 30,
                end_time=None,
                success=False,
                error_message="Insufficient space"
            )
            operation.transfers.append(transfer)
        
        # Add system metrics
        for i in range(60):  # 60 samples over 5 minutes
            sample = ur.SystemMetrics(
                timestamp=time.time() - 300 + i * 5,
                cpu_percent=20 + (i % 20),  # Varying CPU usage
                memory_percent=60 + (i % 10),  # Varying memory usage
                disk_io_read_bps=50000000 + (i % 10) * 1000000,
                disk_io_write_bps=75000000 + (i % 15) * 1000000
            )
            operation.system_samples.append(sample)
        
        operation.errors = ["Movies/FailedMovie1: Insufficient space", "Movies/FailedMovie2: Insufficient space"]
        
        # Test summary report generation
        summary = ur.MetricsReporter.generate_summary_report(operation)
        self.assertIn("integration_report_test", summary)
        self.assertIn("Total Files: 10", summary)
        self.assertIn("Success Rate: 80.0%", summary)
        self.assertIn("balanced", summary)
        
        # Test performance charts generation
        charts = ur.MetricsReporter.generate_performance_charts(operation)
        self.assertIn("CPU Usage (%)", charts)
        self.assertIn("Transfer Rate (MB/s)", charts)
        
        # Save and verify file operations
        metrics_file = self.metrics_dir / "integration_report.json"
        
        # Create a monitor to use its save functionality
        monitor = ur.PerformanceMonitor("test", metrics_enabled=False)
        monitor.operation = operation
        monitor.save_metrics(metrics_file)
        
        self.assertTrue(metrics_file.exists())
        
        # Test CSV export
        csv_file = self.metrics_dir / "integration_report.csv"
        monitor.export_csv(csv_file)
        self.assertTrue(csv_file.exists())
        
        # Verify CSV content
        with open(csv_file) as f:
            csv_content = f.read()
        self.assertIn("Movies/Movie1", csv_content)
        self.assertIn("Movies/FailedMovie1", csv_content)
    
    def test_historical_analysis_integration(self):
        """Test integration of historical analysis with multiple operations."""
        analyzer = ur.HistoricalAnalyzer(self.metrics_dir)
        
        # Create and save multiple operations
        operations = []
        for i in range(5):
            operation = ur.OperationMetrics(
                operation_id=f"historical_op_{i}",
                start_time=time.time() - 3600 * (5 - i),  # Spread over 5 hours
                end_time=time.time() - 3600 * (5 - i) + 1800,  # 30 minutes each
                total_files=10 + i * 2,
                completed_files=10 + i * 2 - (i % 2),  # Some failures
                failed_files=i % 2,
                total_bytes=(2 + i) * 1000000000,  # Increasing data size
                transferred_bytes=(2 + i) * 1000000000 - (i % 2) * 500000000,
                rsync_mode=["fast", "balanced", "integrity"][i % 3]
            )
            operations.append(operation)
            
            # Save to file
            metrics_file = self.metrics_dir / f"metrics_historical_{i}.json"
            with open(metrics_file, 'w') as f:
                json.dump(operation.to_dict(), f, default=str)
        
        # Test loading all operations
        loaded_operations = analyzer.load_all_operations()
        self.assertEqual(len(loaded_operations), 5)
        
        # Test trend analysis
        trends = analyzer.analyze_trends()
        self.assertEqual(trends["total_operations"], 5)
        self.assertIn("date_range", trends)
        
        # Test recommendations
        recommendations = analyzer.generate_recommendations()
        self.assertIsInstance(recommendations, list)
        self.assertGreater(len(recommendations), 0)
        
        # Test comparison report
        comparison = ur.MetricsReporter.compare_operations(loaded_operations)
        self.assertIn("OPERATION COMPARISON REPORT", comparison)
        for i in range(5):
            self.assertIn(f"historical_op_{i}", comparison)


class TestCLIIntegrationWithMetrics(unittest.TestCase):
    """Test CLI integration with metrics features."""
    
    def setUp(self):
        """Set up test environment."""
        self.temp_dir = tempfile.mkdtemp()
        self.metrics_dir = Path(self.temp_dir) / "metrics"
        self.metrics_dir.mkdir()
        
        # Create sample metrics file
        self.sample_operation = ur.OperationMetrics(
            operation_id="cli_test_op",
            start_time=time.time() - 1800,
            end_time=time.time(),
            total_files=5,
            completed_files=4,
            failed_files=1,
            total_bytes=2000000000,
            transferred_bytes=1600000000,
            rsync_mode="fast"
        )
        
        self.metrics_file = self.metrics_dir / "cli_test.json"
        with open(self.metrics_file, 'w') as f:
            json.dump(self.sample_operation.to_dict(), f, default=str)
    
    def tearDown(self):
        """Clean up test environment."""
        shutil.rmtree(self.temp_dir)
    
    @patch('sys.argv')
    def test_cli_export_metrics(self, mock_argv):
        """Test CLI export metrics functionality."""
        # Mock command line arguments for export
        mock_argv[:] = ['unraid_rebalancer.py', '--export-metrics', str(self.metrics_file)]
        
        # Capture the main function execution
        with patch('unraid_rebalancer.MetricsReporter.load_metrics_from_file') as mock_load:
            mock_load.return_value = self.sample_operation
            
            with patch('unraid_rebalancer.PerformanceMonitor') as mock_monitor_class:
                mock_monitor = Mock()
                mock_monitor_class.return_value = mock_monitor
                
                with patch('builtins.print') as mock_print:
                    with self.assertRaises(SystemExit) as cm:
                        ur.main()
                    
                    # Should exit with code 0 (success)
                    self.assertEqual(cm.exception.code, 0)
                    
                    # Should have called export_csv
                    mock_monitor.export_csv.assert_called_once()
    
    @patch('sys.argv')
    def test_cli_show_history(self, mock_argv):
        """Test CLI show history functionality."""
        mock_argv[:] = ['unraid_rebalancer.py', '--show-history', '--metrics-dir', str(self.metrics_dir)]
        
        with patch('unraid_rebalancer.HistoricalAnalyzer') as mock_analyzer_class:
            mock_analyzer = Mock()
            mock_analyzer_class.return_value = mock_analyzer
            mock_analyzer.load_all_operations.return_value = [self.sample_operation]
            
            with patch('unraid_rebalancer.MetricsReporter.compare_operations') as mock_compare:
                mock_compare.return_value = "Test comparison report"
                
                with patch('builtins.print') as mock_print:
                    with self.assertRaises(SystemExit) as cm:
                        ur.main()
                    
                    self.assertEqual(cm.exception.code, 0)
                    mock_print.assert_called_with("Test comparison report")
    
    @patch('sys.argv')
    def test_cli_metrics_summary(self, mock_argv):
        """Test CLI metrics summary functionality."""
        mock_argv[:] = ['unraid_rebalancer.py', '--metrics-summary', '--metrics-dir', str(self.metrics_dir)]
        
        with patch('unraid_rebalancer.HistoricalAnalyzer') as mock_analyzer_class:
            mock_analyzer = Mock()
            mock_analyzer_class.return_value = mock_analyzer
            mock_analyzer.load_all_operations.return_value = [self.sample_operation]
            
            with patch('unraid_rebalancer.MetricsReporter.generate_summary_report') as mock_summary:
                mock_summary.return_value = "Test summary report"
                
                with patch('unraid_rebalancer.MetricsReporter.generate_performance_charts') as mock_charts:
                    mock_charts.return_value = "Test charts"
                    
                    with patch('builtins.print') as mock_print:
                        with self.assertRaises(SystemExit) as cm:
                            ur.main()
                        
                        self.assertEqual(cm.exception.code, 0)
                        # Should print both summary and charts
                        self.assertGreater(mock_print.call_count, 1)
    
    def test_metrics_directory_creation(self):
        """Test that metrics directory is created when needed."""
        new_metrics_dir = Path(self.temp_dir) / "new_metrics"
        self.assertFalse(new_metrics_dir.exists())
        
        # This should create the directory
        with patch('sys.argv', ['unraid_rebalancer.py', '--metrics', '--metrics-dir', str(new_metrics_dir)]):
            with patch('unraid_rebalancer.discover_disks', return_value=[]):
                with patch('builtins.print'):
                    with self.assertRaises(SystemExit):
                        ur.main()
        
        self.assertTrue(new_metrics_dir.exists())


if __name__ == '__main__':
    # Configure logging for tests
    import logging
    logging.basicConfig(level=logging.CRITICAL)  # Suppress logs during tests
    
    # Run tests
    unittest.main(verbosity=2)