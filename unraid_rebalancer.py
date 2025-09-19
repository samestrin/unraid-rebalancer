#!/usr/bin/env python3
"""
Unraid Rebalancer

Scans /mnt/disk* mounts, builds an intelligent redistribution plan to balance
fill levels across data drives, then executes the plan using rsync.

Default behavior is a dry run (no data is modified). Use --execute to move.

Key ideas
- Works at a configurable "allocation unit" granularity (default: one
  directory level below each share, e.g., /mnt/disk1/Movies/<MovieName>).
- Avoids the user share copy bug by doing disk→disk paths only.
- Preserves permissions/attrs/hardlinks with rsync -aHAX and can resume.
- Lets you target a fill percentage (default 80%) or auto-evening.
- Prints a clear plan before acting and can save/load plans as JSON.

Example
  # Plan only, dry-run copy commands (no changes)
  sudo ./unraid_rebalancer.py --target-percent 80

  # Actually execute the moves from the computed plan
  sudo ./unraid_rebalancer.py --target-percent 80 --execute

  # Exclude certain shares and only consider large units (>= 5 GiB)
  sudo ./unraid_rebalancer.py --exclude-shares appdata,System \
       --min-unit-size 5GiB --execute

Safety notes
- Run at the console or via SSH screen/tmux. Avoid running from the Unraid GUI
  terminal if you might close the browser.
- Never mix /mnt/user with /mnt/diskX in the same command. This script uses
  /mnt/disk* only by design.
- Stop heavy writers (e.g., big downloads) during redistribution.
- Always keep good backups. Use --execute only after reviewing the plan.
"""

from __future__ import annotations

import argparse
import csv
import dataclasses
import fnmatch
import json
import logging
import math
import os
import psutil
import re
import shlex
import shutil
import sqlite3
import subprocess
import sys
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

# Import SQLite storage system
try:
    from metrics_storage import MetricsDatabase, JSONToSQLiteMigrator
except ImportError:
    MetricsDatabase = None
    JSONToSQLiteMigrator = None

# Import scheduling system
try:
    from scheduler import (
        ScheduleConfig, ScheduleType, TriggerType, ResourceThresholds,
        CronExpressionValidator, SchedulingEngine, ScheduleMonitor,
        ExecutionStatus, ScheduleExecution, ScheduleStatistics,
        NotificationManager, ScheduleHealthMonitor, ErrorRecoveryManager,
        NotificationConfig, FailureType, RetryConfig
    )
except ImportError:
    ScheduleConfig = None
    SchedulingEngine = None
    ScheduleMonitor = None
    NotificationManager = None
    ScheduleHealthMonitor = None
    ErrorRecoveryManager = None
    logging.warning("Scheduling system not available - scheduling features disabled")
    logging.warning("SQLite metrics storage not available - falling back to JSON")

# Import Unraid integration system
try:
    from unraid_integration import (
        UnraidSystemMonitor, UnraidIntegrationManager, ArrayStatus, DiskStatus,
        NotificationLevel as UnraidNotificationLevel, UnraidDisk, ArrayInfo, UserShare
    )
except ImportError:
    UnraidSystemMonitor = None
    UnraidIntegrationManager = None
    ArrayStatus = None
    DiskStatus = None
    UnraidNotificationLevel = None
    logging.warning("Unraid integration not available - system integration features disabled")

# ---------- Utilities ----------

SIZE_UNITS = {
    "B": 1,
    "KB": 1000,
    "MB": 1000**2,
    "GB": 1000**3,
    "TB": 1000**4,
    "KiB": 1024,
    "MiB": 1024**2,
    "GiB": 1024**3,
    "TiB": 1024**4,
}

# Rsync performance modes for different CPU capabilities
RSYNC_MODES = {
    "fast": {
        "flags": ["-av", "--partial", "--inplace", "--numeric-ids", "--no-compress", "--info=progress2"],
        "description": "Fastest transfers, minimal CPU overhead with progress reporting",
        "features": ["basic_archive", "no_compression", "progress_reporting", "minimal_cpu"],
        "target_hardware": "Lower-end CPUs, slower storage"
    },
    "balanced": {
        "flags": ["-av", "-X", "--partial", "--inplace", "--numeric-ids", "--info=progress2"],
        "description": "Balanced speed and features with extended attributes",
        "features": ["extended_attrs", "progress_reporting", "moderate_features", "mid_range_cpu"],
        "target_hardware": "Mid-range CPUs, mixed storage types"
    },
    "integrity": {
        "flags": ["-aHAX", "--partial", "--inplace", "--numeric-ids", "--info=progress2", "--checksum"],
        "description": "Maximum integrity checking with hard links, ACLs, and checksums",
        "features": ["hard_links", "acls", "extended_attrs", "checksum_verification", "detailed_progress", "maximum_integrity"],
        "target_hardware": "High-end CPUs, fast storage, integrity-critical operations"
    }
}

def parse_size(s: str) -> int:
    m = re.fullmatch(r"\s*(\d+(?:\.\d+)?)\s*([KMGT]?i?B)\s*", s, re.I)
    if not m:
        raise argparse.ArgumentTypeError(f"Invalid size: {s}")
    val = float(m.group(1))
    unit = m.group(2)
    # Normalize case to match dict keys (e.g., GiB)
    for k in SIZE_UNITS:
        if k.lower() == unit.lower():
            return int(val * SIZE_UNITS[k])
    raise argparse.ArgumentTypeError(f"Unknown unit in size: {s}")


def human_bytes(n: int) -> str:
    """Convert bytes to human-readable format using binary units."""
    if n == 0:
        return "0 B"
    
    units = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"]
    for i, unit in enumerate(units):
        if n < 1024 or i == len(units) - 1:
            if unit == "B":
                return f"{n} {unit}"
            else:
                return f"{n:.2f} {unit}"
        n /= 1024
    return f"{n:.2f} PiB"


def run(cmd: List[str], dry_run: bool = False) -> int:
    """Execute a command, optionally in dry-run mode."""
    cmd_str = " ".join(shlex.quote(c) for c in cmd)
    print("$", cmd_str)
    
    if dry_run:
        return 0
    
    try:
        return subprocess.call(cmd)
    except FileNotFoundError as e:
        logging.error(f"Command not found: {cmd[0]}")
        return 127
    except Exception as e:
        logging.error(f"Error executing command: {e}")
        return 1


def is_mounted(path: Path) -> bool:
    """Check if a path is mounted by attempting to get filesystem stats."""
    try:
        os.statvfs(path)
        return True
    except (FileNotFoundError, OSError):
        return False

# ---------- Data Structures ----------

@dataclasses.dataclass
class Disk:
    name: str  # e.g., disk1
    path: Path  # /mnt/disk1
    size_bytes: int
    used_bytes: int
    free_bytes: int

    @property
    def used_pct(self) -> float:
        return (self.used_bytes / self.size_bytes) * 100 if self.size_bytes else 0.0

    @property
    def fill_percentage(self) -> float:
        """Calculate fill percentage for drive prioritization."""
        return self.used_pct


@dataclasses.dataclass
class Unit:
    """An allocation unit to move as a whole (a directory or a single file)."""
    share: str           # top-level share name (e.g., Movies)
    rel_path: str        # path relative to share root (e.g., "Alien (1979)")
    size_bytes: int
    src_disk: str        # e.g., disk1

    def src_abs(self) -> Path:
        return Path(f"/mnt/{self.src_disk}") / self.share / self.rel_path

    def dest_abs(self, dest_disk: str) -> Path:
        return Path(f"/mnt/{dest_disk}") / self.share / self.rel_path


@dataclasses.dataclass
class Move:
    unit: Unit
    dest_disk: str


@dataclasses.dataclass
class Plan:
    moves: List[Move]
    summary: Dict[str, float]

    def to_json(self) -> str:
        obj = {
            "moves": [
                {
                    "share": m.unit.share,
                    "rel_path": m.unit.rel_path,
                    "size_bytes": m.unit.size_bytes,
                    "src_disk": m.unit.src_disk,
                    "dest_disk": m.dest_disk,
                }
                for m in self.moves
            ],
            "summary": self.summary,
        }
        return json.dumps(obj, indent=2)

    @staticmethod
    def from_json(s: str) -> "Plan":
        obj = json.loads(s)
        moves = [
            Move(
                Unit(
                    share=mo["share"],
                    rel_path=mo["rel_path"],
                    size_bytes=int(mo["size_bytes"]),
                    src_disk=mo["src_disk"],
                ),
                dest_disk=mo["dest_disk"],
            )
            for mo in obj["moves"]
        ]
        return Plan(moves=moves, summary=obj.get("summary", {}))

# ---------- Performance Metrics ----------

@dataclasses.dataclass
class TransferMetrics:
    """Metrics for a single file/directory transfer operation."""
    unit_path: str
    src_disk: str
    dest_disk: str
    size_bytes: int
    start_time: float
    end_time: Optional[float] = None
    success: bool = False
    error_message: Optional[str] = None
    transfer_rate_bps: Optional[float] = None

    @property
    def duration_seconds(self) -> Optional[float]:
        if self.end_time and self.start_time:
            return self.end_time - self.start_time
        return None

    @property
    def transfer_rate_mbps(self) -> Optional[float]:
        if self.transfer_rate_bps:
            return self.transfer_rate_bps / (1024 * 1024)
        return None


@dataclasses.dataclass
class SystemMetrics:
    """System resource metrics at a point in time."""
    timestamp: float
    cpu_percent: float
    memory_percent: float
    disk_io_read_bps: float
    disk_io_write_bps: float
    network_sent_bps: float = 0.0
    network_recv_bps: float = 0.0


@dataclasses.dataclass
class OperationMetrics:
    """Complete metrics for an entire rebalancing operation."""
    operation_id: str
    start_time: float
    end_time: Optional[float] = None
    total_files: int = 0
    completed_files: int = 0
    failed_files: int = 0
    total_bytes: int = 0
    transferred_bytes: int = 0
    average_transfer_rate_bps: float = 0.0
    peak_transfer_rate_bps: float = 0.0
    rsync_mode: str = "fast"
    transfers: List[TransferMetrics] = dataclasses.field(default_factory=list)
    system_samples: List[SystemMetrics] = dataclasses.field(default_factory=list)
    errors: List[str] = dataclasses.field(default_factory=list)

    @property
    def duration_seconds(self) -> Optional[float]:
        if self.end_time and self.start_time:
            return self.end_time - self.start_time
        return None

    @property
    def success_rate(self) -> float:
        if self.total_files == 0:
            return 1.0
        return self.completed_files / self.total_files

    @property
    def overall_transfer_rate_mbps(self) -> Optional[float]:
        if self.duration_seconds and self.duration_seconds > 0:
            return (self.transferred_bytes / self.duration_seconds) / (1024 * 1024)
        return None

    def to_dict(self) -> Dict:
        """Convert to dictionary for JSON serialization."""
        return {
            "operation_id": self.operation_id,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "duration_seconds": self.duration_seconds,
            "total_files": self.total_files,
            "completed_files": self.completed_files,
            "failed_files": self.failed_files,
            "total_bytes": self.total_bytes,
            "transferred_bytes": self.transferred_bytes,
            "success_rate": self.success_rate,
            "average_transfer_rate_bps": self.average_transfer_rate_bps,
            "peak_transfer_rate_bps": self.peak_transfer_rate_bps,
            "overall_transfer_rate_mbps": self.overall_transfer_rate_mbps,
            "rsync_mode": self.rsync_mode,
            "transfers": [
                {
                    "unit_path": t.unit_path,
                    "src_disk": t.src_disk,
                    "dest_disk": t.dest_disk,
                    "size_bytes": t.size_bytes,
                    "start_time": t.start_time,
                    "end_time": t.end_time,
                    "duration_seconds": t.duration_seconds,
                    "success": t.success,
                    "error_message": t.error_message,
                    "transfer_rate_bps": t.transfer_rate_bps,
                    "transfer_rate_mbps": t.transfer_rate_mbps,
                }
                for t in self.transfers
            ],
            "system_samples": [
                {
                    "timestamp": s.timestamp,
                    "cpu_percent": s.cpu_percent,
                    "memory_percent": s.memory_percent,
                    "disk_io_read_bps": s.disk_io_read_bps,
                    "disk_io_write_bps": s.disk_io_write_bps,
                    "network_sent_bps": s.network_sent_bps,
                    "network_recv_bps": s.network_recv_bps,
                }
                for s in self.system_samples
            ],
            "errors": self.errors,
        }


class PerformanceMonitor:
    """Real-time performance monitoring and metrics collection with SQLite storage."""

    def __init__(self, operation_id: str, rsync_mode: str = "fast", 
                 sample_interval: float = 5.0, metrics_enabled: bool = True,
                 database_path: Optional[Path] = None):
        self.operation = OperationMetrics(
            operation_id=operation_id,
            start_time=time.time(),
            rsync_mode=rsync_mode
        )
        self.sample_interval = sample_interval
        self.metrics_enabled = metrics_enabled
        
        # Database initialization
        self.database = None
        if self.metrics_enabled and MetricsDatabase:
            db_path = database_path or Path("./metrics/rebalancer_metrics.db")
            try:
                self.database = MetricsDatabase(db_path)
                logging.info(f"Using SQLite metrics storage: {db_path}")
            except Exception as e:
                logging.error(f"Failed to initialize SQLite database: {e}")
                raise RuntimeError(f"Metrics database initialization failed: {e}")
        
        # Threading and monitoring
        self._monitoring_thread: Optional[threading.Thread] = None
        self._stop_monitoring = threading.Event()
        self._lock = threading.Lock()
        
        # Performance thresholds for alerts
        self.slow_transfer_threshold_mbps = 10.0  # Alert if < 10 MB/s
        self.high_cpu_threshold = 90.0  # Alert if > 90%
        self.high_memory_threshold = 90.0  # Alert if > 90%
        
        # Initialize baseline system metrics
        self._last_disk_io = psutil.disk_io_counters() if psutil.disk_io_counters() else None
        self._last_network_io = psutil.net_io_counters() if psutil.net_io_counters() else None
        self._last_sample_time = time.time()
        
        # Store initial operation data if using SQLite
        if self.database and self.metrics_enabled:
            self._store_operation_to_db()

    def _store_operation_to_db(self):
        """Store operation data to SQLite database."""
        if not self.database:
            return
        
        operation_data = {
            'operation_id': self.operation.operation_id,
            'start_time': self.operation.start_time,
            'end_time': self.operation.end_time,
            'total_files': self.operation.total_files,
            'completed_files': self.operation.completed_files,
            'failed_files': self.operation.failed_files,
            'total_bytes': self.operation.total_bytes,
            'transferred_bytes': self.operation.transferred_bytes,
            'average_transfer_rate_bps': self.operation.average_transfer_rate_bps,
            'peak_transfer_rate_bps': self.operation.peak_transfer_rate_bps,
            'rsync_mode': self.operation.rsync_mode,
            'success_rate': self.operation.success_rate,
            'duration_seconds': self.operation.duration_seconds,
            'overall_transfer_rate_mbps': self.operation.overall_transfer_rate_mbps
        }
        
        try:
            self.database.store_operation(operation_data)
        except Exception as e:
            logging.error(f"Failed to store operation to database: {e}")

    def _update_operation_in_db(self):
        """Update operation data in SQLite database."""
        if not self.database:
            return
        
        operation_data = {
            'end_time': self.operation.end_time,
            'completed_files': self.operation.completed_files,
            'failed_files': self.operation.failed_files,
            'transferred_bytes': self.operation.transferred_bytes,
            'average_transfer_rate_bps': self.operation.average_transfer_rate_bps,
            'peak_transfer_rate_bps': self.operation.peak_transfer_rate_bps,
            'success_rate': self.operation.success_rate,
            'duration_seconds': self.operation.duration_seconds,
            'overall_transfer_rate_mbps': self.operation.overall_transfer_rate_mbps
        }
        
        try:
            self.database.update_operation(self.operation.operation_id, operation_data)
        except Exception as e:
            logging.error(f"Failed to update operation in database: {e}")

    def _store_transfer_to_db(self, transfer: TransferMetrics):
        """Store transfer data to SQLite database."""
        if not self.database:
            return
        
        transfer_data = {
            'operation_id': self.operation.operation_id,
            'unit_path': transfer.unit_path,
            'src_disk': transfer.src_disk,
            'dest_disk': transfer.dest_disk,
            'size_bytes': transfer.size_bytes,
            'start_time': transfer.start_time,
            'end_time': transfer.end_time,
            'success': transfer.success,
            'error_message': transfer.error_message,
            'transfer_rate_bps': transfer.transfer_rate_bps,
            'transfer_rate_mbps': transfer.transfer_rate_mbps,
            'duration_seconds': transfer.duration_seconds
        }
        
        try:
            self.database.store_transfer(transfer_data)
        except Exception as e:
            logging.error(f"Failed to store transfer to database: {e}")

    def _store_system_metric_to_db(self, metric: SystemMetrics):
        """Store system metric to SQLite database."""
        if not self.database:
            return
        
        metric_data = {
            'operation_id': self.operation.operation_id,
            'timestamp': metric.timestamp,
            'cpu_percent': metric.cpu_percent,
            'memory_percent': metric.memory_percent,
            'disk_io_read_bps': metric.disk_io_read_bps,
            'disk_io_write_bps': metric.disk_io_write_bps,
            'network_sent_bps': metric.network_sent_bps,
            'network_recv_bps': metric.network_recv_bps
        }
        
        try:
            self.database.store_system_metric(metric_data)
        except Exception as e:
            logging.error(f"Failed to store system metric to database: {e}")

    def _store_error_to_db(self, error_message: str):
        """Store error to SQLite database."""
        if not self.database:
            return
        
        try:
            self.database.store_error(self.operation.operation_id, error_message)
        except Exception as e:
            logging.error(f"Failed to store error to database: {e}")

    def start_monitoring(self):
        """Start background system monitoring."""
        if not self.metrics_enabled or self._monitoring_thread:
            return
        
        self._stop_monitoring.clear()
        self._monitoring_thread = threading.Thread(target=self._monitor_system, daemon=True)
        self._monitoring_thread.start()

    def stop_monitoring(self):
        """Stop background system monitoring."""
        if self._monitoring_thread:
            self._stop_monitoring.set()
            self._monitoring_thread.join(timeout=2.0)
            self._monitoring_thread = None
        
        with self._lock:
            self.operation.end_time = time.time()
        
        # Final update to database
        if self.database:
            self._update_operation_in_db()

    def _monitor_system(self):
        """Background thread for collecting system metrics."""
        while not self._stop_monitoring.wait(self.sample_interval):
            try:
                current_time = time.time()
                cpu_percent = psutil.cpu_percent()
                memory = psutil.virtual_memory()
                
                # Calculate disk I/O rates
                disk_io_read_bps = 0.0
                disk_io_write_bps = 0.0
                if self._last_disk_io:
                    current_disk_io = psutil.disk_io_counters()
                    if current_disk_io:
                        time_delta = current_time - self._last_sample_time
                        if time_delta > 0:
                            disk_io_read_bps = (current_disk_io.read_bytes - self._last_disk_io.read_bytes) / time_delta
                            disk_io_write_bps = (current_disk_io.write_bytes - self._last_disk_io.write_bytes) / time_delta
                        self._last_disk_io = current_disk_io
                
                # Calculate network I/O rates
                network_sent_bps = 0.0
                network_recv_bps = 0.0
                if self._last_network_io:
                    current_network_io = psutil.net_io_counters()
                    if current_network_io:
                        time_delta = current_time - self._last_sample_time
                        if time_delta > 0:
                            network_sent_bps = (current_network_io.bytes_sent - self._last_network_io.bytes_sent) / time_delta
                            network_recv_bps = (current_network_io.bytes_recv - self._last_network_io.bytes_recv) / time_delta
                        self._last_network_io = current_network_io
                
                metrics = SystemMetrics(
                    timestamp=current_time,
                    cpu_percent=cpu_percent,
                    memory_percent=memory.percent,
                    disk_io_read_bps=disk_io_read_bps,
                    disk_io_write_bps=disk_io_write_bps,
                    network_sent_bps=network_sent_bps,
                    network_recv_bps=network_recv_bps,
                )
                
                with self._lock:
                    self.operation.system_samples.append(metrics)
                
                # Store to database if enabled
                if self.database:
                    self._store_system_metric_to_db(metrics)
                
                # Check for performance alerts
                self._check_performance_alerts(metrics)
                
                self._last_sample_time = current_time
                
            except Exception as e:
                logging.warning(f"Error collecting system metrics: {e}")

    def _check_performance_alerts(self, metrics: SystemMetrics):
        """Check system metrics against thresholds and log warnings."""
        if metrics.cpu_percent > self.high_cpu_threshold:
            logging.warning(f"High CPU usage detected: {metrics.cpu_percent:.1f}%")
        
        if metrics.memory_percent > self.high_memory_threshold:
            logging.warning(f"High memory usage detected: {metrics.memory_percent:.1f}%")

    def start_transfer(self, unit: Unit, dest_disk: str) -> TransferMetrics:
        """Start tracking a new transfer operation."""
        transfer = TransferMetrics(
            unit_path=f"{unit.share}/{unit.rel_path}",
            src_disk=unit.src_disk,
            dest_disk=dest_disk,
            size_bytes=unit.size_bytes,
            start_time=time.time(),
        )
        
        with self._lock:
            self.operation.transfers.append(transfer)
            self.operation.total_files += 1
            self.operation.total_bytes += unit.size_bytes
        
        # Update operation in database
        if self.database:
            self._update_operation_in_db()
        
        return transfer

    def complete_transfer(self, transfer: TransferMetrics, success: bool, error_message: Optional[str] = None):
        """Mark a transfer as completed and calculate performance metrics."""
        transfer.end_time = time.time()
        transfer.success = success
        transfer.error_message = error_message
        
        if success and transfer.duration_seconds and transfer.duration_seconds > 0:
            transfer.transfer_rate_bps = transfer.size_bytes / transfer.duration_seconds
            
            with self._lock:
                self.operation.completed_files += 1
                self.operation.transferred_bytes += transfer.size_bytes
                
                # Update peak transfer rate
                if transfer.transfer_rate_bps > self.operation.peak_transfer_rate_bps:
                    self.operation.peak_transfer_rate_bps = transfer.transfer_rate_bps
                
                # Update average transfer rate
                completed_transfers = [t for t in self.operation.transfers if t.success and t.transfer_rate_bps]
                if completed_transfers:
                    self.operation.average_transfer_rate_bps = sum(t.transfer_rate_bps for t in completed_transfers) / len(completed_transfers)
            
            # Check for slow transfer alert
            if transfer.transfer_rate_mbps and transfer.transfer_rate_mbps < self.slow_transfer_threshold_mbps:
                logging.warning(f"Slow transfer detected: {transfer.unit_path} at {transfer.transfer_rate_mbps:.1f} MB/s")
        else:
            with self._lock:
                self.operation.failed_files += 1
                if error_message:
                    self.operation.errors.append(f"{transfer.unit_path}: {error_message}")
        
        # Store transfer data to database
        if self.database:
            self._store_transfer_to_db(transfer)
            self._update_operation_in_db()
            if error_message:
                self._store_error_to_db(f"{transfer.unit_path}: {error_message}")

    def get_progress_info(self) -> Dict[str, any]:
        """Get current progress information for display."""
        with self._lock:
            total_files = self.operation.total_files
            completed_files = self.operation.completed_files
            failed_files = self.operation.failed_files
            total_bytes = self.operation.total_bytes
            transferred_bytes = self.operation.transferred_bytes
            
            progress_percent = (completed_files / total_files * 100) if total_files > 0 else 0
            
            # Calculate ETA based on current transfer rate
            eta_seconds = None
            if self.operation.average_transfer_rate_bps > 0:
                remaining_bytes = total_bytes - transferred_bytes
                eta_seconds = remaining_bytes / self.operation.average_transfer_rate_bps
            
            # Get current system metrics
            current_system = self.operation.system_samples[-1] if self.operation.system_samples else None
            
            return {
                "progress_percent": progress_percent,
                "completed_files": completed_files,
                "total_files": total_files,
                "failed_files": failed_files,
                "transferred_bytes": transferred_bytes,
                "total_bytes": total_bytes,
                "current_transfer_rate_mbps": self.operation.average_transfer_rate_bps / (1024 * 1024) if self.operation.average_transfer_rate_bps else 0,
                "peak_transfer_rate_mbps": self.operation.peak_transfer_rate_bps / (1024 * 1024) if self.operation.peak_transfer_rate_bps else 0,
                "eta_seconds": eta_seconds,
                "current_cpu_percent": current_system.cpu_percent if current_system else 0,
                "current_memory_percent": current_system.memory_percent if current_system else 0,
            }

    def save_metrics(self, filepath: Path):
        """Save metrics to SQLite database. JSON file path parameter kept for compatibility."""
        if not self.database:
            logging.warning("No database available for metrics storage")
            return
            
        # Metrics are automatically stored to SQLite during operation
        logging.info(f"Metrics stored in SQLite database (filepath parameter ignored: {filepath})")

    def cleanup(self):
        """Clean up resources and close database connections."""
        if self.database:
            try:
                self.database.close()
            except Exception as e:
                logging.error(f"Error closing database: {e}")
    
    def __del__(self):
        """Ensure cleanup on object destruction."""
        try:
            self.cleanup()
        except:
            pass  # Ignore errors during cleanup

    def export_csv(self, filepath: Path):
        """Export transfer metrics to CSV file."""
        try:
            with open(filepath, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow([
                    'Unit Path', 'Source Disk', 'Dest Disk', 'Size (Bytes)', 
                    'Start Time', 'End Time', 'Duration (s)', 'Success', 
                    'Transfer Rate (MB/s)', 'Error Message'
                ])
                
                with self._lock:
                    for transfer in self.operation.transfers:
                        writer.writerow([
                            transfer.unit_path,
                            transfer.src_disk,
                            transfer.dest_disk,
                            transfer.size_bytes,
                            datetime.fromtimestamp(transfer.start_time).isoformat() if transfer.start_time else '',
                            datetime.fromtimestamp(transfer.end_time).isoformat() if transfer.end_time else '',
                            transfer.duration_seconds or '',
                            transfer.success,
                            transfer.transfer_rate_mbps or '',
                            transfer.error_message or '',
                        ])
            logging.info(f"Transfer metrics exported to CSV: {filepath}")
        except Exception as e:
            logging.error(f"Failed to export CSV to {filepath}: {e}")


# ---------- Report Generation & Visualization ----------

class MetricsReporter:
    """Generate comprehensive reports and visualizations from performance metrics."""
    
    def __init__(self, database: Optional[MetricsDatabase] = None):
        self.database = database
    
    def load_operation_from_database(self, operation_id: str) -> Optional[OperationMetrics]:
        """Load operation metrics from SQLite database."""
        if not self.database:
            return None
        
        # Get operation data
        operation_data = self.database.get_operation(operation_id)
        if not operation_data:
            return None
        
        # Create OperationMetrics object
        operation = OperationMetrics(
            operation_id=operation_data['operation_id'],
            start_time=operation_data['start_time'],
            end_time=operation_data['end_time'],
            total_files=operation_data['total_files'],
            completed_files=operation_data['completed_files'],
            failed_files=operation_data['failed_files'],
            total_bytes=operation_data['total_bytes'],
            transferred_bytes=operation_data['transferred_bytes'],
            average_transfer_rate_bps=operation_data['average_transfer_rate_bps'],
            peak_transfer_rate_bps=operation_data['peak_transfer_rate_bps'],
            rsync_mode=operation_data['rsync_mode']
        )
        
        # Load transfers
        transfers_data = self.database.get_transfers(operation_id)
        for transfer_data in transfers_data:
            transfer = TransferMetrics(
                unit_path=transfer_data['unit_path'],
                src_disk=transfer_data['src_disk'],
                dest_disk=transfer_data['dest_disk'],
                size_bytes=transfer_data['size_bytes'],
                start_time=transfer_data['start_time'],
                end_time=transfer_data['end_time'],
                success=bool(transfer_data['success']),
                error_message=transfer_data['error_message'],
                transfer_rate_bps=transfer_data['transfer_rate_bps']
            )
            operation.transfers.append(transfer)
        
        # Load system metrics
        system_data = self.database.get_system_metrics(operation_id)
        for metric_data in system_data:
            metric = SystemMetrics(
                timestamp=metric_data['timestamp'],
                cpu_percent=metric_data['cpu_percent'],
                memory_percent=metric_data['memory_percent'],
                disk_io_read_bps=metric_data['disk_io_read_bps'],
                disk_io_write_bps=metric_data['disk_io_write_bps'],
                network_sent_bps=metric_data['network_sent_bps'],
                network_recv_bps=metric_data['network_recv_bps']
            )
            operation.system_samples.append(metric)
        
        # Load errors
        errors_data = self.database.get_operation_errors(operation_id)
        operation.errors = [error_data['error_message'] for error_data in errors_data]
        
        return operation
    
    def get_operations_summary(self, limit: int = 10, days: int = 30) -> List[Dict[str, Any]]:
        """Get summary of recent operations from database."""
        if not self.database:
            return []
        
        start_time = time.time() - (days * 24 * 60 * 60)
        operations = self.database.get_operations(limit=limit, start_time=start_time)
        
        return operations
    
    def get_performance_trends(self, days: int = 30) -> Dict[str, Any]:
        """Analyze performance trends from database."""
        if not self.database:
            return {}
        
        operations = self.get_operations_summary(limit=100, days=days)
        if not operations:
            return {"error": "No operations found"}
        
        # Calculate trends
        transfer_rates = [op['overall_transfer_rate_mbps'] for op in operations 
                         if op['overall_transfer_rate_mbps'] is not None]
        success_rates = [op['success_rate'] for op in operations]
        durations = [op['duration_seconds'] for op in operations 
                    if op['duration_seconds'] is not None]
        
        trends = {
            "total_operations": len(operations),
            "date_range": {
                "start": datetime.fromtimestamp(operations[-1]['start_time']).isoformat(),
                "end": datetime.fromtimestamp(operations[0]['start_time']).isoformat()
            }
        }
        
        if transfer_rates:
            trends["transfer_rate"] = {
                "average": sum(transfer_rates) / len(transfer_rates),
                "best": max(transfer_rates),
                "worst": min(transfer_rates),
                "trend": "improving" if len(transfer_rates) > 1 and transfer_rates[0] > transfer_rates[-1] else "declining"
            }
        
        if success_rates:
            trends["success_rate"] = {
                "average": sum(success_rates) / len(success_rates),
                "best": max(success_rates),
                "worst": min(success_rates)
            }
        
        if durations:
            trends["duration"] = {
                "average_minutes": sum(durations) / len(durations) / 60,
                "shortest_minutes": min(durations) / 60,
                "longest_minutes": max(durations) / 60
            }
        
        return trends
    
    @staticmethod
    def load_metrics_from_file(filepath: Path) -> OperationMetrics:
        """Load metrics from JSON file."""
        try:
            with open(filepath, 'r') as f:
                data = json.load(f)
            
            # Reconstruct OperationMetrics from dictionary
            operation = OperationMetrics(
                operation_id=data['operation_id'],
                start_time=data['start_time'],
                end_time=data.get('end_time'),
                total_files=data['total_files'],
                completed_files=data['completed_files'],
                failed_files=data['failed_files'],
                total_bytes=data['total_bytes'],
                transferred_bytes=data['transferred_bytes'],
                average_transfer_rate_bps=data['average_transfer_rate_bps'],
                peak_transfer_rate_bps=data['peak_transfer_rate_bps'],
                rsync_mode=data.get('rsync_mode', 'unknown'),
            )
            
            # Reconstruct transfers
            for t_data in data.get('transfers', []):
                transfer = TransferMetrics(
                    unit_path=t_data['unit_path'],
                    src_disk=t_data['src_disk'],
                    dest_disk=t_data['dest_disk'],
                    size_bytes=t_data['size_bytes'],
                    start_time=t_data['start_time'],
                    end_time=t_data.get('end_time'),
                    success=t_data['success'],
                    error_message=t_data.get('error_message'),
                    transfer_rate_bps=t_data.get('transfer_rate_bps'),
                )
                operation.transfers.append(transfer)
            
            # Reconstruct system samples
            for s_data in data.get('system_samples', []):
                sample = SystemMetrics(
                    timestamp=s_data['timestamp'],
                    cpu_percent=s_data['cpu_percent'],
                    memory_percent=s_data['memory_percent'],
                    disk_io_read_bps=s_data['disk_io_read_bps'],
                    disk_io_write_bps=s_data['disk_io_write_bps'],
                    network_sent_bps=s_data.get('network_sent_bps', 0.0),
                    network_recv_bps=s_data.get('network_recv_bps', 0.0),
                )
                operation.system_samples.append(sample)
            
            operation.errors = data.get('errors', [])
            return operation
            
        except Exception as e:
            raise ValueError(f"Failed to load metrics from {filepath}: {e}")
    
    @staticmethod
    def generate_summary_report(operation: OperationMetrics) -> str:
        """Generate a human-readable summary report."""
        lines = []
        lines.append("=" * 80)
        lines.append(f"UNRAID REBALANCER - OPERATION SUMMARY")
        lines.append("=" * 80)
        lines.append(f"Operation ID: {operation.operation_id}")
        lines.append(f"Start Time: {datetime.fromtimestamp(operation.start_time).strftime('%Y-%m-%d %H:%M:%S')}")
        
        if operation.end_time:
            lines.append(f"End Time: {datetime.fromtimestamp(operation.end_time).strftime('%Y-%m-%d %H:%M:%S')}")
            lines.append(f"Duration: {operation.duration_seconds:.1f} seconds ({operation.duration_seconds/60:.1f} minutes)")
        
        lines.append(f"Rsync Mode: {operation.rsync_mode}")
        lines.append("")
        
        # File Statistics
        lines.append("FILE TRANSFER STATISTICS:")
        lines.append("-" * 40)
        lines.append(f"Total Files: {operation.total_files:,}")
        lines.append(f"Completed: {operation.completed_files:,}")
        lines.append(f"Failed: {operation.failed_files:,}")
        lines.append(f"Success Rate: {operation.success_rate:.1%}")
        lines.append("")
        
        # Data Transfer Statistics
        lines.append("DATA TRANSFER STATISTICS:")
        lines.append("-" * 40)
        lines.append(f"Total Data: {human_bytes(operation.total_bytes)}")
        lines.append(f"Transferred: {human_bytes(operation.transferred_bytes)}")
        
        if operation.overall_transfer_rate_mbps:
            lines.append(f"Overall Transfer Rate: {operation.overall_transfer_rate_mbps:.1f} MB/s")
        if operation.average_transfer_rate_bps:
            lines.append(f"Average Transfer Rate: {operation.average_transfer_rate_bps / (1024*1024):.1f} MB/s")
        if operation.peak_transfer_rate_bps:
            lines.append(f"Peak Transfer Rate: {operation.peak_transfer_rate_bps / (1024*1024):.1f} MB/s")
        lines.append("")
        
        # System Resource Summary
        if operation.system_samples:
            cpu_values = [s.cpu_percent for s in operation.system_samples]
            memory_values = [s.memory_percent for s in operation.system_samples]
            disk_read_values = [s.disk_io_read_bps for s in operation.system_samples]
            disk_write_values = [s.disk_io_write_bps for s in operation.system_samples]
            
            lines.append("SYSTEM RESOURCE SUMMARY:")
            lines.append("-" * 40)
            lines.append(f"CPU Usage - Avg: {sum(cpu_values)/len(cpu_values):.1f}%, Peak: {max(cpu_values):.1f}%")
            lines.append(f"Memory Usage - Avg: {sum(memory_values)/len(memory_values):.1f}%, Peak: {max(memory_values):.1f}%")
            lines.append(f"Disk Read - Avg: {human_bytes(int(sum(disk_read_values)/len(disk_read_values)))}/s, Peak: {human_bytes(int(max(disk_read_values)))}/s")
            lines.append(f"Disk Write - Avg: {human_bytes(int(sum(disk_write_values)/len(disk_write_values)))}/s, Peak: {human_bytes(int(max(disk_write_values)))}/s")
            lines.append("")
        
        # Top Transfers by Size
        if operation.transfers:
            successful_transfers = [t for t in operation.transfers if t.success]
            if successful_transfers:
                lines.append("LARGEST SUCCESSFUL TRANSFERS:")
                lines.append("-" * 40)
                top_transfers = sorted(successful_transfers, key=lambda t: t.size_bytes, reverse=True)[:10]
                for i, transfer in enumerate(top_transfers, 1):
                    rate_info = f" ({transfer.transfer_rate_mbps:.1f} MB/s)" if transfer.transfer_rate_mbps else ""
                    lines.append(f"{i:2d}. {transfer.unit_path} - {human_bytes(transfer.size_bytes)}{rate_info}")
                lines.append("")
        
        # Errors
        if operation.errors:
            lines.append("ERRORS:")
            lines.append("-" * 40)
            for error in operation.errors[:10]:  # Show first 10 errors
                lines.append(f"• {error}")
            if len(operation.errors) > 10:
                lines.append(f"... and {len(operation.errors) - 10} more errors")
            lines.append("")
        
        lines.append("=" * 80)
        return "\n".join(lines)
    
    @staticmethod
    def create_ascii_chart(values: List[float], title: str, width: int = 60, height: int = 10) -> str:
        """Create a simple ASCII chart from a list of values."""
        if not values:
            return f"{title}: No data available"
        
        min_val = min(values)
        max_val = max(values)
        val_range = max_val - min_val if max_val > min_val else 1
        
        lines = [f"{title}:"]
        lines.append(f"Max: {max_val:.1f}  Min: {min_val:.1f}  Avg: {sum(values)/len(values):.1f}")
        lines.append("")
        
        # Create chart
        for row in range(height, 0, -1):
            line = "|"
            threshold = min_val + (val_range * row / height)
            for i in range(0, len(values), max(1, len(values) // width)):
                if values[i] >= threshold:
                    line += "█"
                else:
                    line += " "
            lines.append(line)
        
        # Add bottom axis
        lines.append("+" + "-" * width)
        return "\n".join(lines)
    
    @staticmethod
    def generate_performance_charts(operation: OperationMetrics) -> str:
        """Generate ASCII performance charts."""
        charts = []
        
        if operation.system_samples:
            # CPU Usage Chart
            cpu_values = [s.cpu_percent for s in operation.system_samples]
            charts.append(MetricsReporter.create_ascii_chart(cpu_values, "CPU Usage (%)"))
            charts.append("")
            
            # Memory Usage Chart  
            memory_values = [s.memory_percent for s in operation.system_samples]
            charts.append(MetricsReporter.create_ascii_chart(memory_values, "Memory Usage (%)"))
            charts.append("")
            
            # Disk I/O Charts
            disk_read_mb = [s.disk_io_read_bps / (1024*1024) for s in operation.system_samples]
            charts.append(MetricsReporter.create_ascii_chart(disk_read_mb, "Disk Read (MB/s)"))
            charts.append("")
            
            disk_write_mb = [s.disk_io_write_bps / (1024*1024) for s in operation.system_samples]
            charts.append(MetricsReporter.create_ascii_chart(disk_write_mb, "Disk Write (MB/s)"))
            charts.append("")
        
        # Transfer Rate Over Time
        if operation.transfers:
            successful_transfers = [t for t in operation.transfers if t.success and t.transfer_rate_mbps]
            if successful_transfers:
                transfer_rates = [t.transfer_rate_mbps for t in successful_transfers]
                charts.append(MetricsReporter.create_ascii_chart(transfer_rates, "Transfer Rate (MB/s)"))
                charts.append("")
        
        return "\n".join(charts)
    
    @staticmethod
    def compare_operations(operations: List[OperationMetrics]) -> str:
        """Compare multiple operations and generate a comparison report."""
        if len(operations) < 2:
            return "Need at least 2 operations to compare"
        
        lines = []
        lines.append("=" * 80)
        lines.append("OPERATION COMPARISON REPORT")
        lines.append("=" * 80)
        lines.append("")
        
        # Header
        lines.append(f"{'Operation ID':<20} {'Files':<8} {'Data':<12} {'Rate MB/s':<10} {'Success%':<8} {'Duration':<10}")
        lines.append("-" * 80)
        
        for op in operations:
            duration_str = f"{op.duration_seconds/60:.1f}m" if op.duration_seconds else "N/A"
            rate_str = f"{op.overall_transfer_rate_mbps:.1f}" if op.overall_transfer_rate_mbps else "N/A"
            
            lines.append(f"{op.operation_id:<20} {op.completed_files:<8} "
                        f"{human_bytes(op.transferred_bytes):<12} {rate_str:<10} "
                        f"{op.success_rate:.1%} {duration_str:<10}")
        
        lines.append("")
        
        # Performance comparison
        rates = [op.overall_transfer_rate_mbps for op in operations if op.overall_transfer_rate_mbps]
        if rates:
            best_rate = max(rates)
            best_idx = next(i for i, op in enumerate(operations) if op.overall_transfer_rate_mbps == best_rate)
            lines.append(f"Best Performance: {operations[best_idx].operation_id} ({best_rate:.1f} MB/s)")
            
            avg_rate = sum(rates) / len(rates)
            lines.append(f"Average Rate: {avg_rate:.1f} MB/s")
        
        # Success rate comparison
        success_rates = [op.success_rate for op in operations]
        if success_rates:
            best_success = max(success_rates)
            lines.append(f"Best Success Rate: {best_success:.1%}")
        
        lines.append("")
        lines.append("=" * 80)
        return "\n".join(lines)


# ---------- Historical Analysis ----------

class HistoricalAnalyzer:
    """Analyze historical performance data and trends."""
    
    def __init__(self, metrics_dir: Path, database: Optional[MetricsDatabase] = None):
        self.metrics_dir = metrics_dir
        self.database = database
        
        # Initialize database if not provided but SQLite is available
        if not self.database and MetricsDatabase is not None:
            db_path = metrics_dir / "rebalancer_metrics.db"
            if db_path.exists():
                try:
                    self.database = MetricsDatabase(db_path)
                except Exception as e:
                    logging.warning(f"Failed to open database {db_path}: {e}")
    
    def find_metrics_files(self) -> List[Path]:
        """Find all metrics JSON files in the metrics directory."""
        if not self.metrics_dir.exists():
            return []
        
        return list(self.metrics_dir.glob("*.json"))
    
    def load_all_operations(self) -> List[OperationMetrics]:
        """Load all available operation metrics from SQLite database."""
        operations = []
        
        if not self.database:
            logging.warning("No database available for loading operations")
            return operations
        
        try:
            operation_summaries = self.database.get_operations(limit=1000)  # Get large number
            reporter = MetricsReporter(self.database)
            
            for summary in operation_summaries:
                operation = reporter.load_operation_from_database(summary['operation_id'])
                if operation:
                    operations.append(operation)
            
            logging.info(f"Loaded {len(operations)} operations from SQLite database")
            return sorted(operations, key=lambda op: op.start_time)
            
        except Exception as e:
            logging.error(f"Failed to load operations from database: {e}")
            return operations
    
    def analyze_trends(self) -> Dict[str, any]:
        """Analyze performance trends over time."""
        operations = self.load_all_operations()
        if len(operations) < 2:
            return {"error": "Need at least 2 operations for trend analysis"}
        
        # Calculate trends
        transfer_rates = [op.overall_transfer_rate_mbps for op in operations if op.overall_transfer_rate_mbps]
        success_rates = [op.success_rate for op in operations]
        durations = [op.duration_seconds for op in operations if op.duration_seconds]
        
        trends = {
            "total_operations": len(operations),
            "date_range": {
                "start": datetime.fromtimestamp(operations[0].start_time).isoformat(),
                "end": datetime.fromtimestamp(operations[-1].start_time).isoformat()
            }
        }
        
        if transfer_rates:
            trends["transfer_rate"] = {
                "average": sum(transfer_rates) / len(transfer_rates),
                "best": max(transfer_rates),
                "worst": min(transfer_rates),
                "trend": "improving" if transfer_rates[-1] > transfer_rates[0] else "declining"
            }
        
        if success_rates:
            trends["success_rate"] = {
                "average": sum(success_rates) / len(success_rates),
                "best": max(success_rates),
                "worst": min(success_rates)
            }
        
        if durations:
            trends["duration"] = {
                "average_minutes": sum(durations) / len(durations) / 60,
                "shortest_minutes": min(durations) / 60,
                "longest_minutes": max(durations) / 60
            }
        
        return trends
    
    def generate_recommendations(self) -> List[str]:
        """Generate performance recommendations based on historical data."""
        operations = self.load_all_operations()
        if not operations:
            return ["No historical data available for recommendations"]
        
        recommendations = []
        
        # Analyze rsync mode performance
        mode_performance = {}
        for op in operations:
            if op.overall_transfer_rate_mbps:
                if op.rsync_mode not in mode_performance:
                    mode_performance[op.rsync_mode] = []
                mode_performance[op.rsync_mode].append(op.overall_transfer_rate_mbps)
        
        if len(mode_performance) > 1:
            best_mode = max(mode_performance.items(), key=lambda x: sum(x[1])/len(x[1]))
            recommendations.append(f"Best performing rsync mode: '{best_mode[0]}' (avg {sum(best_mode[1])/len(best_mode[1]):.1f} MB/s)")
        
        # Analyze failure patterns
        failed_ops = [op for op in operations if op.failed_files > 0]
        if failed_ops:
            total_failures = sum(op.failed_files for op in failed_ops)
            recommendations.append(f"Consider investigating {total_failures} failed transfers across {len(failed_ops)} operations")
        
        # Performance recommendations
        recent_ops = operations[-5:]  # Last 5 operations
        if recent_ops:
            avg_recent_rate = sum(op.overall_transfer_rate_mbps for op in recent_ops if op.overall_transfer_rate_mbps) / len([op for op in recent_ops if op.overall_transfer_rate_mbps])
            if avg_recent_rate < 50:  # Less than 50 MB/s average
                recommendations.append("Consider using 'fast' rsync mode for better performance on slower systems")
            elif avg_recent_rate > 100:  # Greater than 100 MB/s
                recommendations.append("System performs well - consider 'integrity' mode for better data validation")
        
        return recommendations if recommendations else ["System performance appears optimal"]


# ---------- Discovery & Scanning ----------

def discover_disks(include: Optional[List[str]] = None,
                   exclude: Optional[List[str]] = None) -> List[Disk]:
    roots = sorted(p for p in Path("/mnt").glob("disk*") if p.is_dir())
    disks: List[Disk] = []
    for p in roots:
        name = p.name  # disk1, disk2, ...
        if include and name not in include:
            continue
        if exclude and name in exclude:
            continue
        if not is_mounted(p):
            continue
        st = os.statvfs(p)
        size = st.f_frsize * st.f_blocks
        free = st.f_frsize * st.f_bavail
        used = size - free
        disks.append(Disk(name=name, path=p, size_bytes=size, used_bytes=used, free_bytes=free))
    return disks


def iter_units_on_disk(disk: Disk, unit_depth: int, 
                       include_shares: Optional[List[str]],
                       exclude_shares: Optional[List[str]],
                       min_unit_size: int,
                       exclude_globs: List[str]) -> Iterable[Unit]:
    # Scan top-level shares under this disk
    if not disk.path.exists():
        return
    for share_root in sorted(p for p in disk.path.iterdir() if p.is_dir()):
        share = share_root.name
        if include_shares and share not in include_shares:
            continue
        if exclude_shares and share in exclude_shares:
            continue
        # Build allocation units at requested depth
        # depth=1: each direct child of share root is a unit; files at root are individual units
        # depth=0: the entire share on this disk is one unit
        # depth>=2: go deeper
        if unit_depth == 0:
            size = du_path(share_root)
            if size >= min_unit_size:
                rel = ""  # entire share content
                yield Unit(share=share, rel_path=rel, size_bytes=size, src_disk=disk.name)
            continue

        # Descend to unit_depth below share_root
        def gen_candidates(root: Path, depth: int) -> Iterable[Path]:
            if depth == 0:
                yield root
            else:
                try:
                    for child in root.iterdir():
                        if child.is_dir():
                            yield from gen_candidates(child, depth - 1)
                        elif depth == 1 and child.is_file():
                            # files at target depth count as units too
                            yield child
                except PermissionError:
                    return
        for cand in gen_candidates(share_root, unit_depth):
            rel = str(cand.relative_to(share_root)) if cand != share_root else ""
            # apply globs relative to share
            rel_for_match = f"{share}/{rel}" if rel else f"{share}"
            if any(fnmatch.fnmatch(rel_for_match, g) for g in exclude_globs):
                continue
            size = du_path(cand)
            if size >= min_unit_size:
                yield Unit(share=share, rel_path=rel, size_bytes=size, src_disk=disk.name)


def du_path(path: Path) -> int:
    """Calculate total size of a path (file or directory) in bytes."""
    total = 0
    try:
        if path.is_file():
            return path.stat().st_size
        
        for root, dirs, files in os.walk(path, onerror=lambda e: None):
            for filename in files:
                try:
                    filepath = Path(root) / filename
                    total += filepath.stat().st_size
                except (FileNotFoundError, PermissionError):
                    continue
    except PermissionError:
        return 0
    return total

# ---------- Planning ----------

def build_plan(disks: List[Disk], units: List[Unit], target_percent: Optional[float],
               headroom_percent: float, strategy: str = 'size') -> Plan:
    # Compute targets
    # If target_percent provided, aim each disk to be <= target_percent and also
    # try to raise low disks to (100 - headroom_percent)
    # Otherwise, compute equalizing average used across disks with headroom.
    total_size = sum(d.size_bytes for d in disks)
    total_used = sum(d.used_bytes for d in disks)

    if target_percent is not None:
        target_used_per_disk = [min(d.size_bytes * (target_percent / 100.0), d.size_bytes) for d in disks]
    else:
        avg_used = total_used / len(disks) if disks else 0
        # leave some breathing room
        target_used_per_disk = [min(avg_used, d.size_bytes * (1 - headroom_percent / 100.0)) for d in disks]

    # Classify disks
    donors: Dict[str, float] = {}  # disk -> bytes to shed
    recipients: Dict[str, float] = {}  # disk -> bytes it can take (up to target)
    for d, tgt in zip(disks, target_used_per_disk):
        if d.used_bytes > tgt:
            donors[d.name] = d.used_bytes - tgt
        elif d.used_bytes < tgt:
            recipients[d.name] = tgt - d.used_bytes

    # Sort units from donors based on strategy
    donor_units = [u for u in units if u.src_disk in donors]

    # Create disk lookup for efficiency
    disk_lookup = {d.name: d for d in disks}

    if strategy == 'size':
        # Original sorting: by size (largest first for fewer moves)
        donor_units.sort(key=lambda u: u.size_bytes, reverse=True)
    elif strategy == 'space':
        # New sorting: prioritize units from high-fill disks, then by size
        donor_units.sort(key=lambda u: (
            disk_lookup[u.src_disk].fill_percentage,
            u.size_bytes
        ), reverse=True)
    else:
        raise ValueError(f"Unknown sorting strategy: {strategy}")

    # Sort recipients by most capacity needed first
    recipient_list = sorted(recipients.items(), key=lambda kv: kv[1], reverse=True)

    moves: List[Move] = []
    
    # Greedy assignment: place each unit on the recipient that needs it most and fits
    for unit in donor_units:
        # Refresh recipient order each time
        recipient_list.sort(key=lambda kv: kv[1], reverse=True)
        placed = False
        for rdisk, need_bytes in recipient_list:
            if need_bytes <= 0:  # Skip if recipient is full
                continue
                
            # Ensure destination has free space for the unit plus 1 GiB margin
            dest_disk = disk_lookup[rdisk]
            margin = 1 * 1024**3  # 1 GiB safety margin
            
            if unit.size_bytes + margin <= dest_disk.free_bytes + recipients[rdisk]:
                moves.append(Move(unit=unit, dest_disk=rdisk))
                # Update bookkeeping: donor sheds, recipient fills
                donors[unit.src_disk] -= unit.size_bytes
                recipients[rdisk] -= unit.size_bytes
                placed = True
                break
        
        # If not placed (e.g., unit too large for any recipient), skip
        if not placed:
            continue

    summary = {
        "total_moves": len(moves),
        "total_bytes": float(sum(m.unit.size_bytes for m in moves)),
    }
    return Plan(moves=moves, summary=summary)

# ---------- Execution ----------

def get_rsync_flags(mode: str) -> List[str]:
    """Get rsync flags for the specified performance mode."""
    if mode not in RSYNC_MODES:
        raise ValueError(f"Unknown rsync mode '{mode}'. Available modes: {', '.join(RSYNC_MODES.keys())}")
    return RSYNC_MODES[mode]["flags"].copy()


def perform_plan(plan: Plan, execute: bool, rsync_extra: List[str], allow_merge: bool, 
                 rsync_mode: str = "fast", monitor: Optional[PerformanceMonitor] = None, 
                 show_progress: bool = False) -> int:
    failures = 0
    
    for idx, m in enumerate(plan.moves, 1):
        src = m.unit.src_abs()
        dst = m.unit.dest_abs(m.dest_disk)
        
        # Start tracking this transfer if monitoring is enabled
        transfer = None
        if monitor:
            transfer = monitor.start_transfer(m.unit, m.dest_disk)
        
        # Pre-transfer validation
        validation_passed = True
        validation_warnings = []

        # Basic validation checks
        if not src.exists():
            print(f"[ERROR] Source path does not exist: {src}")
            logging.error(f"Source validation failed: {src}")
            failures += 1
            if transfer:
                monitor.complete_transfer(transfer, False, "Source path does not exist")
            continue

        # Ensure parent exists on destination
        dst_parent = dst.parent
        if not dst_parent.exists():
            if execute:
                try:
                    dst_parent.mkdir(parents=True, exist_ok=True)
                    logging.info(f"Created destination parent directory: {dst_parent}")
                except Exception as e:
                    print(f"[ERROR] Failed to create destination parent directory: {e}")
                    logging.error(f"Destination parent creation failed: {e}")
                    failures += 1
                    if transfer:
                        monitor.complete_transfer(transfer, False, f"Failed to create destination parent: {e}")
                    continue

        # Check disk space (simplified)
        if execute:
            try:
                import shutil
                source_size = 0
                if src.is_file():
                    source_size = src.stat().st_size
                elif src.is_dir():
                    source_size = sum(f.stat().st_size for f in src.rglob('*') if f.is_file())

                dest_usage = shutil.disk_usage(dst_parent)
                available_space = dest_usage.free

                if source_size > 0 and available_space < source_size * 1.1:  # 10% buffer
                    print(f"[ERROR] Insufficient disk space: need {source_size:,} bytes, have {available_space:,} bytes")
                    logging.error(f"Disk space validation failed: need {source_size}, have {available_space}")
                    failures += 1
                    if transfer:
                        monitor.complete_transfer(transfer, False, "Insufficient disk space")
                    continue
                elif source_size > 0 and available_space < source_size * 2:  # Warning if tight
                    warning_msg = f"Disk space is tight: {available_space:,} bytes available for {source_size:,} byte transfer"
                    print(f"[WARNING] {warning_msg}")
                    logging.warning(warning_msg)
                    validation_warnings.append(warning_msg)

            except Exception as e:
                logging.warning(f"Could not check disk space: {e}")

        # Check for same-disk transfer (warning)
        try:
            src_parts = str(src).split('/')
            dst_parts = str(dst).split('/')
            if len(src_parts) >= 3 and len(dst_parts) >= 3:
                src_disk = src_parts[2]  # /mnt/disk1/... -> disk1
                dst_disk = dst_parts[2]  # /mnt/disk2/... -> disk2
                if src_disk == dst_disk:
                    warning_msg = f"Source and destination are on same disk: {src_disk}"
                    print(f"[WARNING] {warning_msg}")
                    logging.warning(warning_msg)
                    validation_warnings.append(warning_msg)
        except Exception:
            pass

        # If destination exists and not allowed to merge, skip
        if dst.exists() and not allow_merge:
            print(f"[SKIP] Destination exists and --allow-merge not set: {dst}")
            if transfer:
                monitor.complete_transfer(transfer, False, "Destination exists and merge not allowed")
            continue

        # rsync path handling
        if src.is_dir():
            # For directories, use the parent directory as destination to avoid nesting issues
            # This ensures 'rsync source_dir parent_dir/' moves source_dir into parent_dir/
            src_r = str(src)
            dst_r = str(dst.parent) + "/"  # Trailing slash ensures it's treated as directory
        else:
            # For files, use the exact destination path
            src_r = str(src)
            dst_r = str(dst)

        # Use atomic rsync operation with --remove-source-files
        rsync_flags = get_rsync_flags(rsync_mode)

        # Add --remove-source-files for atomic move operation
        atomic_flags = rsync_flags + ["--remove-source-files"]
        cmd = ["rsync"] + atomic_flags + rsync_extra + [src_r, dst_r]

        # Display progress information
        progress_info = ""
        if monitor and show_progress:
            progress = monitor.get_progress_info()
            progress_info = (f" | Progress: {progress['progress_percent']:.1f}% "
                           f"| Rate: {progress['current_transfer_rate_mbps']:.1f} MB/s "
                           f"| CPU: {progress['current_cpu_percent']:.1f}%")

            if progress['eta_seconds']:
                eta_mins = int(progress['eta_seconds'] / 60)
                progress_info += f" | ETA: {eta_mins}m"

        print(f"\n[{idx}/{len(plan.moves)}] Moving {m.unit.share}/{m.unit.rel_path} "
              f"from {m.unit.src_disk} -> {m.dest_disk} ({human_bytes(m.unit.size_bytes)}){progress_info}")

        # Execute atomic rsync move with enhanced error handling
        rc = run(cmd, dry_run=not execute)

        if rc != 0:
            # Enhanced error handling with detailed categorization
            error_msg = f"Atomic rsync failed with return code {rc}"

            # Log detailed error information
            logging.error(f"Rsync command failed: {' '.join(cmd)}")
            logging.error(f"Return code: {rc}")

            # Categorize error severity
            if rc in [1, 2, 4, 5, 6, 22]:  # Configuration/critical errors
                print(f"[CRITICAL ERROR] {error_msg} - Non-recoverable rsync error")
                logging.critical(f"Non-recoverable rsync error {rc} for {src} -> {dst}")
            elif rc in [23, 24]:  # Partial transfer errors
                print(f"[WARNING] {error_msg} - Partial transfer, may retry")
                logging.warning(f"Partial transfer error {rc} for {src} -> {dst}")
            else:  # Other errors
                print(f"[ERROR] {error_msg}")
                logging.error(f"Rsync error {rc} for {src} -> {dst}")

            # Check for specific error conditions and attempt recovery
            if rc == 23:  # Partial transfer due to error
                logging.info("Partial transfer detected - destination may contain partial data")
                # In dry-run mode, this is expected and not an error
                if not execute:
                    logging.debug("Partial transfer in dry-run mode is normal")
            elif rc == 24:  # Partial transfer due to vanished source files
                logging.warning("Source files vanished during transfer - checking source state")
                if not src.exists():
                    logging.error(f"Source path no longer exists: {src}")

            failures += 1
            if transfer:
                monitor.complete_transfer(transfer, False, error_msg)
            continue

        # Atomic operation completed successfully
        # The --remove-source-files flag ensures source is removed after successful transfer
        logging.info(f"Successfully completed atomic transfer: {src} -> {dst}")

        # Verify atomic transfer completion (only in execute mode)
        if execute:
            if not src.exists() and dst.exists():
                logging.debug(f"Atomic transfer verification passed: source removed, destination exists")
            elif src.exists() and dst.exists():
                logging.warning(f"Source still exists after atomic transfer - may be partial: {src}")
            elif not dst.exists():
                logging.error(f"Destination does not exist after atomic transfer: {dst}")
                failures += 1
                if transfer:
                    monitor.complete_transfer(transfer, False, "Destination verification failed")
                continue
        
        # Mark transfer as completed successfully
        if transfer:
            monitor.complete_transfer(transfer, True)
    
    return failures

# ---------- CLI ----------

def main():
    p = argparse.ArgumentParser(description="Rebalance Unraid data drives by moving directory/file units between /mnt/disk*.")
    p.add_argument("--include-disks", help="Comma list of disk names to include (e.g., disk1,disk2)")
    p.add_argument("--exclude-disks", help="Comma list of disk names to exclude")
    p.add_argument("--include-shares", help="Comma list of shares to include (default: all)")
    p.add_argument("--exclude-shares", help="Comma list of shares to exclude (e.g., appdata,System)")
    p.add_argument("--exclude-globs", default="", help="Comma list of globs relative to share root to skip (e.g., 'appdata/*,System/*')")
    p.add_argument("--unit-depth", type=int, default=1, help="Allocation unit depth under each share (0 = whole share on a disk, 1 = share's immediate children [default], 2 = grandchildren, etc.)")
    p.add_argument("--min-unit-size", type=parse_size, default=parse_size("1GiB"), help="Only move units >= this size (default 1GiB)")
    p.add_argument("--target-percent", type=float, default=80.0, help="Target maximum fill percent per disk (default 80). Use -1 to auto-even with headroom.")
    p.add_argument("--headroom-percent", type=float, default=5.0, help="Headroom percent when auto-evening (ignored if target-percent >= 0)")
    p.add_argument("--prioritize-low-space", action="store_true", help="Prioritize moves from drives with least free space first")
    p.add_argument("--save-plan", help="Write plan JSON to this path")
    p.add_argument("--load-plan", help="Load plan from JSON and skip planning")
    p.add_argument("--execute", action="store_true", help="Execute moves (default is dry-run)")
    p.add_argument("--rsync-extra", default="", help="Extra args to pass to rsync (comma-separated, e.g., '--bwlimit=50M,--checksum')")
    p.add_argument("--rsync-mode", choices=list(RSYNC_MODES.keys()), default="fast", 
                   help="Rsync performance mode: fast (minimal CPU), balanced (moderate features), integrity (full features)")
    p.add_argument("--list-rsync-modes", action="store_true", help="List available rsync modes and exit")
    p.add_argument("--allow-merge", action="store_true", help="Allow merging into existing destination directories if present")
    p.add_argument("--verbose", "-v", action="store_true", help="Enable verbose logging")
    p.add_argument("--log-file", help="Write logs to this file (default: stderr only)")
    
    # Performance metrics options
    p.add_argument("--metrics", action="store_true", help="Enable detailed performance metrics collection")
    p.add_argument("--metrics-file", help="Save performance metrics to this JSON file")
    p.add_argument("--metrics-dir", default="./metrics", help="Directory to save/load metrics files (default: ./metrics)")
    p.add_argument("--database-path", help="Path to SQLite database file (default: metrics/rebalancer_metrics.db)")
    p.add_argument("--migrate-json", action="store_true", help="Migrate existing JSON files to SQLite database")
    p.add_argument("--show-progress", action="store_true", help="Show real-time progress information during transfers")
    p.add_argument("--report-format", choices=["text", "json", "csv"], default="text", help="Output format for reports")
    p.add_argument("--show-history", action="store_true", help="Display historical performance data and exit")
    p.add_argument("--compare-runs", action="store_true", help="Compare performance across recent operations and exit")
    p.add_argument("--metrics-summary", action="store_true", help="Show quick performance summary and exit")
    p.add_argument("--export-metrics", help="Export metrics from specified file to CSV format")
    p.add_argument("--sample-interval", type=float, default=5.0, help="System monitoring sample interval in seconds (default: 5.0)")
    p.add_argument("--cleanup-database", action="store_true", help="Perform database maintenance (VACUUM and ANALYZE)")
    p.add_argument("--database-stats", action="store_true", help="Show database statistics and exit")
    p.add_argument("--retention-days", type=int, default=90, help="Days to retain operation data (default: 90)")
    p.add_argument("--apply-retention", action="store_true", help="Apply data retention policies and exit")
    p.add_argument("--compress-metrics", action="store_true", help="Compress old system metrics and exit")
    p.add_argument("--performance-trends", type=int, metavar="DAYS", help="Show performance trends over specified days")
    p.add_argument("--disk-comparison", type=int, metavar="DAYS", help="Compare disk performance over specified days")
    p.add_argument("--rsync-comparison", type=int, metavar="DAYS", help="Compare rsync mode performance over specified days")
    p.add_argument("--db-connections", type=int, default=10, help="Maximum database connection pool size (default: 10)")
    p.add_argument("--metrics-compression-days", type=int, default=7, help="Compress system metrics older than this many days (default: 7)")
    p.add_argument("--metrics-sample-rate", type=int, default=10, help="Sample rate for metrics compression (keep 1 in N samples, default: 10)")
    p.add_argument("--backup-database", help="Create database backup at specified path")
    p.add_argument("--verify-database", action="store_true", help="Verify database integrity and exit")
    p.add_argument("--repair-database", action="store_true", help="Attempt to repair database issues and exit")
    
    # Advanced scheduling options
    p.add_argument("--schedule", help="Create a new schedule with given name")
    p.add_argument("--cron", help="Cron expression for schedule (e.g., '0 2 * * *' for 2 AM daily)")
    p.add_argument("--daily", type=int, metavar="HOUR", help="Schedule daily at specified hour (0-23)")
    p.add_argument("--weekly", type=int, nargs=2, metavar=("DAY", "HOUR"), help="Schedule weekly on day (0-6, 0=Sunday) at hour")
    p.add_argument("--monthly", type=int, nargs=2, metavar=("DAY", "HOUR"), help="Schedule monthly on day (1-31) at hour")
    p.add_argument("--schedule-id", help="Schedule ID for scheduled operations (internal use)")
    p.add_argument("--max-runtime", type=int, default=6, help="Maximum runtime in hours for scheduled operations")
    p.add_argument("--list-schedules", action="store_true", help="List all configured schedules and exit")
    p.add_argument("--remove-schedule", help="Remove schedule by ID and exit")
    p.add_argument("--enable-schedule", help="Enable schedule by ID and exit")
    p.add_argument("--disable-schedule", help="Disable schedule by ID and exit")
    p.add_argument("--sync-schedules", action="store_true", help="Synchronize schedules with cron and exit")
    p.add_argument("--test-schedule", help="Test schedule configuration by ID and exit")
    
    # Schedule monitoring and control options
    p.add_argument("--list-executions", action="store_true", help="List recent schedule executions and exit")
    p.add_argument("--execution-history", help="Show execution history for specific schedule ID")
    p.add_argument("--schedule-stats", help="Show statistics for specific schedule ID")
    p.add_argument("--running-executions", action="store_true", help="Show currently running executions and exit")
    p.add_argument("--cancel-execution", help="Cancel running execution by ID")
    p.add_argument("--suspend-schedule", help="Suspend schedule by ID")
    p.add_argument("--resume-schedule", help="Resume suspended schedule by ID")
    p.add_argument("--suspend-reason", help="Reason for schedule suspension")
    p.add_argument("--cleanup-executions", type=int, metavar="DAYS", help="Clean up execution records older than specified days")
    p.add_argument("--emergency-stop", action="store_true", help="Emergency stop all running schedule executions")
    
    # Enhanced error handling and safety options
    p.add_argument("--health-check", help="Check health of specific schedule ID")
    p.add_argument("--system-health", action="store_true", help="Get overall system health report")
    p.add_argument("--retry-failed", help="Retry failed execution by ID")
    p.add_argument("--configure-notifications", action="store_true", help="Configure notification settings")
    p.add_argument("--test-notifications", action="store_true", help="Test notification configuration")
    p.add_argument("--force-retry", help="Force retry of execution regardless of retry limits")
    p.add_argument("--reset-failures", help="Reset failure count for schedule ID")
    p.add_argument("--auto-suspend-threshold", type=int, default=3, help="Number of consecutive failures before auto-suspension")
    
    # Unraid System Integration options
    p.add_argument("--array-status", action="store_true", help="Show Unraid array status and exit")
    p.add_argument("--disk-details", action="store_true", help="Show detailed disk information and exit")
    p.add_argument("--user-shares", action="store_true", help="Show user share configuration and exit")
    p.add_argument("--system-report", action="store_true", help="Generate comprehensive system status report and exit")
    p.add_argument("--safety-check", action="store_true", help="Perform pre-rebalance safety checks and exit")
    p.add_argument("--docker-status", action="store_true", help="Show Docker container status and exit")
    p.add_argument("--vm-status", action="store_true", help="Show VM status and exit")
    p.add_argument("--send-notification", nargs=2, metavar=("TITLE", "MESSAGE"), help="Send Unraid notification with title and message")
    p.add_argument("--notification-level", choices=["normal", "warning", "alert", "critical"], default="normal", help="Notification level for --send-notification")
    
    # Unraid User Scripts Integration
    p.add_argument("--list-user-scripts", action="store_true", help="List available Unraid user scripts and exit")
    p.add_argument("--create-user-script", help="Create user script for schedule (provide schedule name)")
    p.add_argument("--maintenance-window", action="store_true", help="Check if currently in maintenance window and exit")
    
    # Scheduling Templates
    p.add_argument("--list-templates", action="store_true", help="List available scheduling templates and exit")
    p.add_argument("--create-from-template", help="Create schedule from template (provide template name)")
    p.add_argument("--template-name", help="Custom name for schedule created from template")

    args = p.parse_args()
    
    # Handle --list-rsync-modes
    if args.list_rsync_modes:
        print("Available rsync performance modes:\n")
        for mode, config in RSYNC_MODES.items():
            flags_str = " ".join(config["flags"])
            features_str = ", ".join(config.get("features", []))
            print(f"  {mode:>9}: {config['description']}")
            print(f"           Flags: {flags_str}")
            print(f"           Features: {features_str}")
            print(f"           Target: {config.get('target_hardware', 'General purpose')}\n")
        return 0
    
    # Set up metrics directory and database
    metrics_dir = Path(args.metrics_dir)
    if args.metrics or args.metrics_file:
        metrics_dir.mkdir(parents=True, exist_ok=True)
    
    # Set up database path
    if args.database_path:
        database_path = Path(args.database_path)
    else:
        database_path = metrics_dir / "rebalancer_metrics.db"
    
    # Initialize database for metrics commands
    database = None
    if MetricsDatabase:
        try:
            database = MetricsDatabase(database_path, max_connections=args.db_connections)
        except Exception as e:
            logging.error(f"Failed to initialize database: {e}")
            print(f"Error: Could not initialize metrics database at {database_path}")
            return 1
    
    # Handle database-only commands
    if args.migrate_json:
        if not database:
            print("Database not available for migration")
            return 1
        
        migrator = JSONToSQLiteMigrator(database)
        success_count, total_count = migrator.migrate_directory(metrics_dir)
        print(f"Migration complete: {success_count}/{total_count} files migrated successfully")
        return 0 if success_count == total_count else 1
    
    if args.database_stats:
        if not database:
            print("Database not available")
            return 1
        
        stats = database.get_database_stats()
        print("DATABASE STATISTICS:")
        print("=" * 40)
        for key, value in stats.items():
            if key.endswith('_bytes'):
                print(f"{key.replace('_', ' ').title()}: {human_bytes(value)}")
            elif key.endswith('_count'):
                print(f"{key.replace('_', ' ').title()}: {value:,}")
            elif 'operation' in key:
                if isinstance(value, (int, float)):
                    print(f"{key.replace('_', ' ').title()}: {datetime.fromtimestamp(value).strftime('%Y-%m-%d %H:%M:%S')}")
                else:
                    print(f"{key.replace('_', ' ').title()}: {value}")
            else:
                print(f"{key.replace('_', ' ').title()}: {value}")
        return 0
    
    if args.cleanup_database:
        if not database:
            print("Database not available")
            return 1
        
        print("Performing database maintenance...")
        database.vacuum_database()
        print("Database maintenance complete")
        return 0
    
    if args.apply_retention:
        if not database:
            print("Database not available")
            return 1
        
        print(f"Applying retention policy (keeping {args.retention_days} days)...")
        retention_config = {
            'operations': args.retention_days,
            'system_metrics': max(7, args.retention_days // 4),  # Keep system metrics for shorter period
            'errors': args.retention_days * 2  # Keep errors longer
        }
        results = database.apply_retention_policy(retention_config)
        
        print("RETENTION POLICY RESULTS:")
        for data_type, count in results.items():
            print(f"  {data_type}: {count} records deleted")
        return 0
    
    if args.compress_metrics:
        if not database:
            print("Database not available")
            return 1
        
        print(f"Compressing system metrics older than {args.metrics_compression_days} days (keeping 1 in {args.metrics_sample_rate} samples)...")
        deleted_count = database.compress_old_system_metrics(
            days_threshold=args.metrics_compression_days, 
            sample_rate=args.metrics_sample_rate
        )
        print(f"Compressed system metrics: {deleted_count} samples removed")
        return 0
    
    if args.performance_trends:
        if not database:
            print("Database not available")
            return 1
        
        trends = database.get_performance_trends(args.performance_trends)
        if args.report_format == "json":
            print(json.dumps(trends, indent=2, default=str))
        else:
            print(f"PERFORMANCE TRENDS ({args.performance_trends} days)")
            print("=" * 60)
            
            print("\nTransfer Rate Trends:")
            for trend in trends['transfer_trends']:
                print(f"  {trend['date']}: {trend['avg_rate']:.1f} MB/s avg, "
                      f"{trend['max_rate']:.1f} MB/s peak, {trend['operation_count']} operations")
            
            print("\nResource Usage Trends:")
            for trend in trends['resource_trends']:
                print(f"  {trend['date']}: CPU {trend['avg_cpu']:.1f}%, "
                      f"Memory {trend['avg_memory']:.1f}%, I/O {human_bytes(trend['avg_disk_io'])}/s")
        return 0
    
    if args.disk_comparison:
        if not database:
            print("Database not available")
            return 1
        
        comparison = database.get_disk_performance_comparison(args.disk_comparison)
        if args.report_format == "json":
            print(json.dumps(comparison, indent=2, default=str))
        else:
            print(f"DISK PERFORMANCE COMPARISON ({args.disk_comparison} days)")
            print("=" * 60)
            
            print("\nSource Disk Performance:")
            for disk in comparison['source_disk_performance']:
                print(f"  {disk['src_disk']}: {disk['avg_rate']:.1f} MB/s avg, "
                      f"{disk['success_rate']:.1f}% success, {disk['transfer_count']} transfers")
            
            print("\nDestination Disk Performance:")
            for disk in comparison['destination_disk_performance']:
                print(f"  {disk['dest_disk']}: {disk['avg_rate']:.1f} MB/s avg, "
                      f"{disk['success_rate']:.1f}% success, {disk['transfer_count']} transfers")
        return 0
    
    if args.rsync_comparison:
        if not database:
            print("Database not available")
            return 1
        
        comparison = database.get_rsync_mode_comparison(args.rsync_comparison)
        if args.report_format == "json":
            print(json.dumps(comparison, indent=2, default=str))
        else:
            print(f"RSYNC MODE COMPARISON ({args.rsync_comparison} days)")
            print("=" * 60)
            
            for mode in comparison['mode_comparison']:
                print(f"\n{mode['rsync_mode'].upper()} Mode:")
                print(f"  Transfer Rate: {mode['avg_transfer_rate']:.1f} MB/s avg")
                print(f"  Success Rate: {mode['avg_success_rate']:.1f}%")
                print(f"  Duration: {mode['avg_duration']:.1f}s avg")
                print(f"  CPU Usage: {mode['avg_cpu_usage']:.1f}%")
                print(f"  Operations: {mode['operation_count']}")
        return 0
    
    if args.backup_database:
        if not database:
            print("Database not available")
            return 1
        
        backup_path = Path(args.backup_database)
        print(f"Creating database backup at {backup_path}...")
        
        if database.backup_database(backup_path):
            print("Database backup completed successfully")
            return 0
        else:
            print("Database backup failed")
            return 1
    
    if args.verify_database:
        if not database:
            print("Database not available")
            return 1
        
        print("Verifying database integrity...")
        report = database.verify_database_integrity()
        
        print("DATABASE INTEGRITY REPORT:")
        print("=" * 40)
        print(f"Integrity Check: {'PASS' if report['integrity_check'] else 'FAIL'}")
        print(f"Foreign Key Check: {'PASS' if report['foreign_key_check'] else 'FAIL'}")
        print(f"Schema Version: {'VALID' if report['schema_version_valid'] else 'INVALID'}")
        
        if 'current_schema_version' in report:
            print(f"Current Schema: {report['current_schema_version']}")
            print(f"Expected Schema: {report['expected_schema_version']}")
        
        print(f"\nTable Counts:")
        for table, count in report['table_counts'].items():
            print(f"  {table}: {count:,}")
        
        if report['issues']:
            print(f"\nIssues Found:")
            for issue in report['issues']:
                print(f"  • {issue}")
            return 1
        else:
            print(f"\nNo issues found.")
            return 0
    
    if args.repair_database:
        if not database:
            print("Database not available")
            return 1
        
        print("Attempting database repair...")
        
        # Create backup before repair
        backup_path = database.db_path.with_suffix('.backup')
        print(f"Creating backup at {backup_path} before repair...")
        
        if not database.backup_database(backup_path):
            print("Failed to create backup - aborting repair")
            return 1
        
        if database.repair_database():
            print("Database repair completed successfully")
            return 0
        else:
            print("Database repair failed")
            print(f"Original database backed up to: {backup_path}")
            return 1
    
    # Initialize scheduling engine if available
    scheduling_engine = None
    if SchedulingEngine:
        try:
            script_path = Path(__file__).absolute()
            scheduling_engine = SchedulingEngine(script_path)
        except Exception as e:
            logging.warning(f"Failed to initialize scheduling engine: {e}")
    
    # Handle schedule management commands
    if args.list_schedules:
        if not scheduling_engine:
            print("Scheduling system not available")
            return 1
        
        schedules = scheduling_engine.schedule_manager.list_schedules()
        if not schedules:
            print("No schedules configured")
            return 0
        
        print("CONFIGURED SCHEDULES:")
        print("=" * 60)
        for schedule in schedules:
            status = "ENABLED" if schedule.enabled else "DISABLED"
            print(f"ID: {schedule.schedule_id}")
            print(f"Name: {schedule.name}")
            print(f"Status: {status}")
            print(f"Cron: {schedule.cron_expression}")
            print(f"Target: {schedule.target_percent}%")
            print(f"Created: {datetime.fromtimestamp(schedule.created_at).strftime('%Y-%m-%d %H:%M')}")
            if schedule.description:
                print(f"Description: {schedule.description}")
            print("-" * 40)
        return 0
    
    if args.remove_schedule:
        if not scheduling_engine:
            print("Scheduling system not available")
            return 1
        
        if scheduling_engine.delete_schedule(args.remove_schedule):
            print(f"Schedule '{args.remove_schedule}' removed successfully")
            return 0
        else:
            print(f"Failed to remove schedule '{args.remove_schedule}'")
            return 1
    
    if args.enable_schedule:
        if not scheduling_engine:
            print("Scheduling system not available")
            return 1
        
        if scheduling_engine.enable_schedule(args.enable_schedule):
            print(f"Schedule '{args.enable_schedule}' enabled successfully")
            return 0
        else:
            print(f"Failed to enable schedule '{args.enable_schedule}'")
            return 1
    
    if args.disable_schedule:
        if not scheduling_engine:
            print("Scheduling system not available")
            return 1
        
        if scheduling_engine.disable_schedule(args.disable_schedule):
            print(f"Schedule '{args.disable_schedule}' disabled successfully")
            return 0
        else:
            print(f"Failed to disable schedule '{args.disable_schedule}'")
            return 1
    
    if args.sync_schedules:
        if not scheduling_engine:
            print("Scheduling system not available")
            return 1
        
        if scheduling_engine.sync_schedules():
            print("Schedules synchronized with cron successfully")
            return 0
        else:
            print("Failed to synchronize schedules")
            return 1
    
    if args.test_schedule:
        if not scheduling_engine:
            print("Scheduling system not available")
            return 1
        
        schedule = scheduling_engine.schedule_manager.get_schedule(args.test_schedule)
        if not schedule:
            print(f"Schedule '{args.test_schedule}' not found")
            return 1
        
        print(f"TESTING SCHEDULE: {schedule.name}")
        print("=" * 50)
        
        # Test cron expression
        if schedule.cron_expression:
            is_valid = CronExpressionValidator.validate_cron_expression(schedule.cron_expression)
            print(f"Cron Expression: {schedule.cron_expression} ({'VALID' if is_valid else 'INVALID'})")
        else:
            print("Cron Expression: Not specified")
        
        # Show generated command
        if schedule.cron_expression:
            cron_manager = scheduling_engine.cron_manager
            command = cron_manager._generate_cron_command(schedule)
            print(f"Generated Command: {command}")
        
        print(f"Status: {'ENABLED' if schedule.enabled else 'DISABLED'}")
        print(f"Max Runtime: {schedule.max_runtime_hours} hours")
        
        return 0
    
    # Handle schedule creation
    if args.schedule:
        if not scheduling_engine:
            print("Scheduling system not available")
            return 1
        
        # Generate schedule ID from name
        schedule_id = re.sub(r'[^a-zA-Z0-9_-]', '_', args.schedule.lower())
        
        # Determine cron expression
        cron_expression = ""
        if args.cron:
            cron_expression = args.cron
        elif args.daily is not None:
            if not (0 <= args.daily <= 23):
                print("Daily hour must be between 0-23")
                return 1
            cron_expression = CronExpressionValidator.create_daily_expression(args.daily)
        elif args.weekly:
            day, hour = args.weekly
            if not (0 <= day <= 6 and 0 <= hour <= 23):
                print("Weekly day must be 0-6 (0=Sunday) and hour 0-23")
                return 1
            cron_expression = CronExpressionValidator.create_weekly_expression(day, hour)
        elif args.monthly:
            day, hour = args.monthly
            if not (1 <= day <= 31 and 0 <= hour <= 23):
                print("Monthly day must be 1-31 and hour 0-23")
                return 1
            cron_expression = CronExpressionValidator.create_monthly_expression(day, hour)
        else:
            print("No schedule timing specified. Use --cron, --daily, --weekly, or --monthly")
            return 1
        
        # Create schedule configuration
        schedule_config = ScheduleConfig(
            schedule_id=schedule_id,
            name=args.schedule,
            cron_expression=cron_expression,
            target_percent=args.target_percent,
            headroom_percent=args.headroom_percent,
            min_unit_size=args.min_unit_size,
            rsync_mode=args.rsync_mode,
            max_runtime_hours=args.max_runtime,
            include_disks=args.include_disks.split(",") if args.include_disks else [],
            exclude_disks=args.exclude_disks.split(",") if args.exclude_disks else [],
            include_shares=args.include_shares.split(",") if args.include_shares else [],
            exclude_shares=args.exclude_shares.split(",") if args.exclude_shares else [],
            exclude_globs=[g.strip() for g in args.exclude_globs.split(",") if g.strip()] if args.exclude_globs else []
        )
        
        if scheduling_engine.create_and_install_schedule(schedule_config):
            print(f"Schedule '{args.schedule}' created successfully")
            print(f"Schedule ID: {schedule_id}")
            print(f"Cron Expression: {cron_expression}")
            print(f"Status: {'ENABLED' if schedule_config.enabled else 'DISABLED'}")
            return 0
        else:
            print(f"Failed to create schedule '{args.schedule}'")
            return 1
    
    # Handle schedule monitoring and control commands
    if args.list_executions:
        if not scheduling_engine or not ScheduleMonitor:
            print("Schedule monitoring not available")
            return 1
        
        monitor = ScheduleMonitor()
        executions = monitor.get_running_executions()
        
        if not executions:
            print("No running schedule executions")
            return 0
        
        print("RUNNING SCHEDULE EXECUTIONS:")
        print("=" * 60)
        for execution in executions:
            duration = time.time() - execution.start_time if execution.start_time else 0
            print(f"Execution ID: {execution.execution_id}")
            print(f"Schedule: {execution.schedule_id}")
            print(f"Status: {execution.status.value}")
            print(f"Started: {datetime.fromtimestamp(execution.start_time).strftime('%Y-%m-%d %H:%M:%S') if execution.start_time else 'Unknown'}")
            print(f"Duration: {duration:.1f} seconds")
            if execution.operation_id:
                print(f"Operation ID: {execution.operation_id}")
            print("-" * 40)
        return 0
    
    if args.execution_history:
        if not scheduling_engine or not ScheduleMonitor:
            print("Schedule monitoring not available")
            return 1
        
        monitor = ScheduleMonitor()
        history = monitor.get_execution_history(args.execution_history, limit=20)
        
        if not history:
            print(f"No execution history found for schedule '{args.execution_history}'")
            return 0
        
        print(f"EXECUTION HISTORY FOR '{args.execution_history}':")
        print("=" * 60)
        for execution in history:
            duration = (execution.end_time - execution.start_time) if execution.end_time and execution.start_time else None
            print(f"Execution ID: {execution.execution_id}")
            print(f"Status: {execution.status.value}")
            print(f"Started: {datetime.fromtimestamp(execution.start_time).strftime('%Y-%m-%d %H:%M:%S') if execution.start_time else 'Unknown'}")
            if execution.end_time:
                print(f"Ended: {datetime.fromtimestamp(execution.end_time).strftime('%Y-%m-%d %H:%M:%S')}")
            if duration:
                print(f"Duration: {duration:.1f} seconds")
            if execution.error_message:
                print(f"Error: {execution.error_message}")
            print("-" * 40)
        return 0
    
    if args.schedule_stats:
        if not scheduling_engine or not ScheduleMonitor:
            print("Schedule monitoring not available")
            return 1
        
        monitor = ScheduleMonitor()
        stats = monitor.get_schedule_statistics(args.schedule_stats)
        
        if not stats:
            print(f"No statistics found for schedule '{args.schedule_stats}'")
            return 0
        
        print(f"STATISTICS FOR '{args.schedule_stats}':")
        print("=" * 50)
        print(f"Total Executions: {stats.total_executions}")
        print(f"Successful: {stats.successful_executions}")
        print(f"Failed: {stats.failed_executions}")
        print(f"Success Rate: {stats.success_rate:.1f}%")
        if stats.average_duration:
            print(f"Average Duration: {stats.average_duration:.1f} seconds")
        if stats.last_execution_time:
            print(f"Last Execution: {datetime.fromtimestamp(stats.last_execution_time).strftime('%Y-%m-%d %H:%M:%S')}")
        if stats.last_success_time:
            print(f"Last Success: {datetime.fromtimestamp(stats.last_success_time).strftime('%Y-%m-%d %H:%M:%S')}")
        return 0
    
    if args.cancel_execution:
        if not scheduling_engine or not ScheduleMonitor:
            print("Schedule monitoring not available")
            return 1
        
        monitor = ScheduleMonitor()
        if monitor.cancel_execution(args.cancel_execution):
            print(f"Execution '{args.cancel_execution}' cancelled successfully")
            return 0
        else:
            print(f"Failed to cancel execution '{args.cancel_execution}'")
            return 1
    
    if args.suspend_schedule:
        if not scheduling_engine or not ScheduleMonitor:
            print("Schedule monitoring not available")
            return 1
        
        monitor = ScheduleMonitor()
        reason = args.suspend_reason or "Manual suspension"
        if monitor.suspend_schedule(args.suspend_schedule, reason):
            print(f"Schedule '{args.suspend_schedule}' suspended successfully")
            print(f"Reason: {reason}")
            return 0
        else:
            print(f"Failed to suspend schedule '{args.suspend_schedule}'")
            return 1
    
    if args.resume_schedule:
        if not scheduling_engine or not ScheduleMonitor:
            print("Schedule monitoring not available")
            return 1
        
        monitor = ScheduleMonitor()
        if monitor.resume_schedule(args.resume_schedule):
            print(f"Schedule '{args.resume_schedule}' resumed successfully")
            return 0
        else:
            print(f"Failed to resume schedule '{args.resume_schedule}'")
            return 1
    
    if args.cleanup_executions:
        if not scheduling_engine or not ScheduleMonitor:
            print("Schedule monitoring not available")
            return 1
        
        monitor = ScheduleMonitor()
        days = args.cleanup_executions
        cleaned = monitor.cleanup_old_executions(days)
        print(f"Cleaned up {cleaned} execution records older than {days} days")
        return 0
    
    if args.emergency_stop:
        if not scheduling_engine or not ScheduleMonitor:
            print("Schedule monitoring not available")
            return 1
        
        monitor = ScheduleMonitor()
        running_executions = monitor.get_running_executions()
        
        if not running_executions:
            print("No running executions to stop")
            return 0
        
        print(f"EMERGENCY STOP: Cancelling {len(running_executions)} running executions...")
        cancelled = 0
        for execution in running_executions:
            if monitor.cancel_execution(execution.execution_id):
                cancelled += 1
        
        print(f"Emergency stop completed: {cancelled}/{len(running_executions)} executions cancelled")
        return 0
    
    # Enhanced Error Handling and Safety Features
    if args.health_check:
        if not ScheduleHealthMonitor:
            print("Schedule health monitoring not available")
            return 1
        
        health_monitor = ScheduleHealthMonitor()
        health_status = health_monitor.check_schedule_health(args.health_check)
        
        if health_status:
            print(f"Schedule '{args.health_check}' health status:")
            print(f"  Status: {health_status.get('status', 'Unknown')}")
            print(f"  Last execution: {health_status.get('last_execution', 'Never')}")
            print(f"  Success rate: {health_status.get('success_rate', 0):.1%}")
            print(f"  Consecutive failures: {health_status.get('consecutive_failures', 0)}")
            if health_status.get('issues'):
                print("  Issues:")
                for issue in health_status['issues']:
                    print(f"    - {issue}")
        else:
            print(f"Schedule '{args.health_check}' not found")
            return 1
        return 0
    
    if args.system_health:
        if not ScheduleHealthMonitor:
            print("Schedule health monitoring not available")
            return 1
        
        health_monitor = ScheduleHealthMonitor()
        system_health = health_monitor.get_system_health()
        
        print("System Health Report:")
        print(f"  Total schedules: {system_health.get('total_schedules', 0)}")
        print(f"  Active schedules: {system_health.get('active_schedules', 0)}")
        print(f"  Failed schedules: {system_health.get('failed_schedules', 0)}")
        print(f"  Overall success rate: {system_health.get('overall_success_rate', 0):.1%}")
        
        if system_health.get('critical_issues'):
            print("  Critical Issues:")
            for issue in system_health['critical_issues']:
                print(f"    - {issue}")
        
        if system_health.get('warnings'):
            print("  Warnings:")
            for warning in system_health['warnings']:
                print(f"    - {warning}")
        return 0
    
    if args.retry_failed:
        if not ErrorRecoveryManager:
            print("Error recovery not available")
            return 1
        
        recovery_manager = ErrorRecoveryManager()
        retried = recovery_manager.retry_failed_executions(args.retry_failed)
        
        if retried:
            print(f"Retrying {len(retried)} failed executions for schedule '{args.retry_failed}'")
            for execution_id in retried:
                print(f"  - Execution {execution_id} queued for retry")
        else:
            print(f"No failed executions found for schedule '{args.retry_failed}'")
        return 0
    
    if args.configure_notifications:
        if not NotificationManager:
            print("Notification system not available")
            return 1
        
        # Parse notification configuration
        config_parts = args.configure_notifications.split(',')
        if len(config_parts) < 2:
            print("Invalid notification configuration. Format: email,smtp_server[,port,username,password]")
            return 1
        
        notification_type = config_parts[0].strip()
        if notification_type == 'email':
            if len(config_parts) < 2:
                print("Email configuration requires: email,smtp_server[,port,username,password]")
                return 1
            
            smtp_server = config_parts[1].strip()
            port = int(config_parts[2]) if len(config_parts) > 2 else 587
            username = config_parts[3].strip() if len(config_parts) > 3 else None
            password = config_parts[4].strip() if len(config_parts) > 4 else None
            
            config = NotificationConfig(
                email_enabled=True,
                smtp_server=smtp_server,
                smtp_port=port,
                smtp_username=username,
                smtp_password=password
            )
            
            notification_manager = NotificationManager(config)
            print(f"Email notifications configured: {smtp_server}:{port}")
        else:
            print(f"Unsupported notification type: {notification_type}")
            return 1
        return 0
    
    if args.test_notifications:
        if not NotificationManager:
            print("Notification system not available")
            return 1
        
        # Use default configuration for testing
        config = NotificationConfig(email_enabled=True)
        notification_manager = NotificationManager(config)
        
        success = notification_manager.send_test_notification(args.test_notifications)
        if success:
            print(f"Test notification sent successfully to {args.test_notifications}")
        else:
            print(f"Failed to send test notification to {args.test_notifications}")
        return 0 if success else 1
    
    if args.force_retry:
        if not ErrorRecoveryManager:
            print("Error recovery not available")
            return 1
        
        recovery_manager = ErrorRecoveryManager()
        success = recovery_manager.force_retry_execution(args.force_retry)
        
        if success:
            print(f"Execution {args.force_retry} queued for immediate retry")
        else:
            print(f"Failed to queue execution {args.force_retry} for retry")
        return 0 if success else 1
    
    if args.reset_failures:
        if not ErrorRecoveryManager:
            print("Error recovery not available")
            return 1
        
        recovery_manager = ErrorRecoveryManager()
        reset_count = recovery_manager.reset_schedule_failures(args.reset_failures)
        
        print(f"Reset {reset_count} failure records for schedule '{args.reset_failures}'")
        return 0
    
    # Initialize health monitoring if available (but don't exit early)
    if ScheduleHealthMonitor:
        health_monitor = ScheduleHealthMonitor()
        health_monitor.set_auto_suspend_threshold(args.auto_suspend_threshold)
        print(f"Auto-suspension threshold set to {args.auto_suspend_threshold} consecutive failures")
    
    # Unraid Integration Commands
    if args.array_status:
        if not UnraidSystemMonitor:
            print("Unraid integration not available")
            return 1
        
        monitor = UnraidSystemMonitor()
        array_info = monitor.get_array_status()
        
        print(f"Array Status: {array_info.status.value}")
        print(f"Total Disks: {len(array_info.disks)}")
        print(f"Data Disks: {len([d for d in array_info.disks if d.type == 'data'])}")
        print(f"Parity Disks: {len([d for d in array_info.disks if d.type == 'parity'])}")
        print(f"Cache Disks: {len([d for d in array_info.disks if d.type == 'cache'])}")
        
        if array_info.status != ArrayStatus.STARTED:
            print(f"Warning: Array is not started (status: {array_info.status.value})")
        
        return 0
    
    if args.disk_details:
        if not UnraidSystemMonitor:
            print("Unraid integration not available")
            return 1
        
        monitor = UnraidSystemMonitor()
        disks = monitor.get_disk_details()
        
        print("\nDisk Details:")
        print("-" * 80)
        for disk in disks:
            status_icon = "✓" if disk.status == DiskStatus.ACTIVE else "✗"
            print(f"{status_icon} {disk.name:<8} {disk.type:<8} {disk.device:<12} {disk.filesystem:<8} {disk.size_gb:>8.1f}GB {disk.used_percent:>6.1f}%")
        
        return 0
    
    if args.user_shares:
        if not UnraidSystemMonitor:
            print("Unraid integration not available")
            return 1
        
        monitor = UnraidSystemMonitor()
        shares = monitor.get_user_shares()
        
        print("\nUser Shares:")
        print("-" * 60)
        for share in shares:
            print(f"{share.name:<20} {share.allocation_method:<12} {share.size_gb:>8.1f}GB")
            if share.included_disks:
                print(f"  Included: {', '.join(share.included_disks)}")
            if share.excluded_disks:
                print(f"  Excluded: {', '.join(share.excluded_disks)}")
        
        return 0
    
    if args.system_report:
        if not UnraidSystemMonitor:
            print("Unraid integration not available")
            return 1
        
        monitor = UnraidSystemMonitor()
        report = monitor.generate_system_report()
        
        print("\nUnraid System Report")
        print("=" * 50)
        print(report)
        return 0
    
    if args.safety_check:
        if not UnraidIntegrationManager:
            print("Unraid integration not available")
            return 1
        
        integration = UnraidIntegrationManager()
        
        print("Performing pre-rebalance safety checks...")
        checks_passed = integration.perform_pre_rebalance_checks()
        
        if checks_passed:
            print("✓ All safety checks passed")
            return 0
        else:
            print("✗ Safety checks failed - rebalancing not recommended")
            return 1
    
    if args.docker_status:
        if not UnraidSystemMonitor:
            print("Unraid integration not available")
            return 1
        
        monitor = UnraidSystemMonitor()
        containers = monitor.get_docker_containers()
        
        print("\nDocker Containers:")
        print("-" * 60)
        for container in containers:
            status_icon = "🟢" if container['status'] == 'running' else "🔴"
            print(f"{status_icon} {container['name']:<20} {container['image']:<30} {container['status']}")
        
        return 0
    
    if args.vm_status:
        if not UnraidSystemMonitor:
            print("Unraid integration not available")
            return 1
        
        monitor = UnraidSystemMonitor()
        vms = monitor.get_vms()
        
        print("\nVirtual Machines:")
        print("-" * 60)
        for vm in vms:
            status_icon = "🟢" if vm['status'] == 'running' else "🔴"
            print(f"{status_icon} {vm['name']:<20} {vm['status']:<10} CPU: {vm.get('cpu', 'N/A')} RAM: {vm.get('memory', 'N/A')}")
        
        return 0
    
    if args.list_user_scripts:
        if not UnraidIntegrationManager:
            print("Unraid integration not available")
            return 1
        
        manager = UnraidIntegrationManager()
        scripts = manager.get_user_scripts()
        
        print("\nUnraid User Scripts:")
        print("-" * 60)
        for script in scripts:
            print(f"📄 {script['name']:<30} {script['path']}")
        
        return 0
    
    if args.create_user_script:
        if not UnraidIntegrationManager:
            print("Unraid integration not available")
            return 1
        
        manager = UnraidIntegrationManager()
        script_path = manager.create_rebalancer_user_script()
        
        if script_path:
            print(f"✅ Created rebalancer user script: {script_path}")
            print("Script can be configured and scheduled through Unraid's User Scripts plugin")
        else:
            print("❌ Failed to create user script")
            return 1
        
        return 0
    
    if args.maintenance_window:
        if not UnraidIntegrationManager:
            print("Unraid integration not available")
            return 1
        
        manager = UnraidIntegrationManager()
        in_maintenance = manager.is_maintenance_window()
        
        if in_maintenance:
            print("🔧 System is currently in maintenance window")
        else:
            print("✅ System is not in maintenance window")
        
        return 0
    
    if args.list_templates:
        if not UnraidIntegrationManager:
            print("Unraid integration not available")
            return 1
        
        manager = UnraidIntegrationManager()
        templates = manager.get_scheduling_templates()
        
        print("\nAvailable Scheduling Templates:")
        print("-" * 60)
        for template in templates:
            print(f"📋 {template['name']:<20} - {template['description']}")
            print(f"   Schedule: {template['schedule']}")
            print(f"   Options: {', '.join(template['options'])}")
            print()
        
        return 0
    
    if args.create_from_template:
        if not UnraidIntegrationManager:
            print("Unraid integration not available")
            return 1
        
        manager = UnraidIntegrationManager()
        success = manager.create_template_schedule(args.create_from_template)
        
        if success:
            print(f"✅ Created schedule from template: {args.create_from_template}")
        else:
            print(f"❌ Failed to create schedule from template: {args.create_from_template}")
            return 1
        
        return 0
    
    if args.send_notification:
        if not UnraidIntegrationManager:
            print("Unraid integration not available")
            return 1
        
        integration = UnraidIntegrationManager()
        
        # Parse notification level
        level = UnraidNotificationLevel.NORMAL
        if hasattr(args, 'notification_level') and args.notification_level:
            try:
                level = UnraidNotificationLevel(args.notification_level.upper())
            except ValueError:
                print(f"Invalid notification level: {args.notification_level}")
                return 1
        
        success = integration.send_notification(
            title="Unraid Rebalancer Notification",
            message=args.send_notification,
            level=level
        )
        
        if success:
            print("Notification sent successfully")
            return 0
        else:
            print("Failed to send notification")
            return 1
    
    # Handle metrics-only commands that don't require disk scanning
    if args.export_metrics:
        try:
            metrics_path = Path(args.export_metrics)
            csv_path = metrics_path.with_suffix('.csv')
            operation = MetricsReporter.load_metrics_from_file(metrics_path)
            monitor = PerformanceMonitor("export", metrics_enabled=False)
            monitor.operation = operation
            monitor.export_csv(csv_path)
            print(f"Metrics exported to {csv_path}")
            return 0
        except Exception as e:
            logging.error(f"Failed to export metrics: {e}")
            return 1
    
    if args.show_history:
        analyzer = HistoricalAnalyzer(metrics_dir, database)
        operations = analyzer.load_all_operations()
        if not operations:
            print("No historical metrics data found.")
            return 0
        
        if args.report_format == "json":
            trends = analyzer.analyze_trends()
            print(json.dumps(trends, indent=2, default=str))
        else:
            reporter = MetricsReporter(database)
            report = reporter.compare_operations(operations)
            print(report)
        return 0
    
    if args.compare_runs:
        analyzer = HistoricalAnalyzer(metrics_dir, database)
        operations = analyzer.load_all_operations()
        if len(operations) < 2:
            print("Need at least 2 operations for comparison.")
            return 0
        
        recent_operations = operations[-5:]  # Last 5 operations
        reporter = MetricsReporter(database)
        report = reporter.compare_operations(recent_operations)
        print(report)
        
        # Show recommendations
        recommendations = analyzer.generate_recommendations()
        print("\nRECOMMENDATIONS:")
        print("-" * 40)
        for rec in recommendations:
            print(f"• {rec}")
        return 0
    
    if args.metrics_summary:
        analyzer = HistoricalAnalyzer(metrics_dir, database)
        operations = analyzer.load_all_operations()
        if not operations:
            print("No historical metrics data found.")
            return 0
        
        latest_operation = operations[-1]
        if args.report_format == "json":
            print(json.dumps(latest_operation.to_dict(), indent=2, default=str))
        else:
            reporter = MetricsReporter(database)
            report = reporter.generate_summary_report(latest_operation)
            print(report)
            
            # Show performance charts
            charts = reporter.generate_performance_charts(latest_operation)
            if charts:
                print("\nPERFORMANCE CHARTS:")
                print("=" * 80)
                print(charts)
        return 0
    
    # Configure logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    log_format = '%(asctime)s - %(levelname)s - %(message)s'
    
    if args.log_file:
        logging.basicConfig(level=log_level, format=log_format, 
                          handlers=[
                              logging.FileHandler(args.log_file),
                              logging.StreamHandler(sys.stderr)
                          ])
    else:
        logging.basicConfig(level=log_level, format=log_format, stream=sys.stderr)

    include_disks = args.include_disks.split(",") if args.include_disks else None
    exclude_disks = args.exclude_disks.split(",") if args.exclude_disks else None
    include_shares = args.include_shares.split(",") if args.include_shares else None
    exclude_shares = args.exclude_shares.split(",") if args.exclude_shares else None
    exclude_globs = [g.strip() for g in args.exclude_globs.split(",") if g.strip()]
    rsync_extra = [s for s in args.rsync_extra.split(",") if s]

    # Step 1: Discover disks and their usage
    disks = discover_disks(include_disks, exclude_disks)
    if not disks:
        print("No /mnt/disk* data disks found. Are you running on Unraid?")
        return 2

    print("Discovered disks:")
    for d in disks:
        print(f"  {d.name}: used={human_bytes(d.used_bytes)} ({d.used_pct:.1f}%), free={human_bytes(d.free_bytes)}")

    if args.load_plan:
        try:
            plan = Plan.from_json(Path(args.load_plan).read_text())
            print(f"Loaded plan with {len(plan.moves)} moves totaling {human_bytes(int(plan.summary.get('total_bytes', 0)))}")
            logging.info(f"Successfully loaded plan from {args.load_plan}")
        except Exception as e:
            logging.error(f"Failed to load plan from {args.load_plan}: {e}")
            return 1
    else:
        # Step 2: Scan units
        print("\nScanning allocation units (this can take a while)...")
        units: List[Unit] = []
        for d in disks:
            for u in iter_units_on_disk(
                disk=d,
                unit_depth=args.unit_depth,
                include_shares=include_shares,
                exclude_shares=exclude_shares,
                min_unit_size=args.min_unit_size,
                exclude_globs=exclude_globs,
            ):
                units.append(u)
        total_units = len(units)
        total_bytes = sum(u.size_bytes for u in units)
        print(f"Found {total_units} units totaling {human_bytes(total_bytes)}")

        # Step 3: Build plan
        target_percent = None if args.target_percent < 0 else args.target_percent
        strategy = 'space' if args.prioritize_low_space else 'size'
        plan = build_plan(disks, units, target_percent=target_percent, headroom_percent=args.headroom_percent, strategy=strategy)
        print(f"\nPlan: {len(plan.moves)} moves, {human_bytes(int(plan.summary['total_bytes']))} to re-distribute.")
        # Preview first few moves
        for i, m in enumerate(plan.moves[:20], 1):
            print(f"  {i:>3}. {m.unit.share}/{m.unit.rel_path} | {human_bytes(m.unit.size_bytes)} | {m.unit.src_disk} -> {m.dest_disk}")
        if len(plan.moves) > 20:
            print(f"  ... and {len(plan.moves)-20} more")

        if args.save_plan:
            try:
                Path(args.save_plan).write_text(plan.to_json())
                print(f"Saved plan to {args.save_plan}")
                logging.info(f"Plan saved to {args.save_plan}")
            except Exception as e:
                logging.error(f"Failed to save plan to {args.save_plan}: {e}")
                return 1

    # Step 4: Execute (or dry-run) with optional performance monitoring
    mode = "EXECUTE" if args.execute else "DRY-RUN"
    print(f"\n=== {mode} {len(plan.moves)} planned move(s) ===")
    print(f"Using rsync mode: {args.rsync_mode} - {RSYNC_MODES[args.rsync_mode]['description']}")
    
    # Initialize performance monitor if metrics are enabled
    monitor = None
    if args.metrics or args.metrics_file or args.show_progress:
        operation_id = f"rebalance_{int(time.time())}"
        monitor = PerformanceMonitor(
            operation_id=operation_id,
            rsync_mode=args.rsync_mode,
            sample_interval=args.sample_interval,
            metrics_enabled=args.metrics or args.metrics_file,
            database_path=database_path
        )
        if monitor.metrics_enabled:
            monitor.start_monitoring()
            print(f"Performance monitoring enabled (Operation ID: {operation_id})")
    
    try:
        failures = perform_plan(
            plan, 
            execute=args.execute, 
            rsync_extra=rsync_extra, 
            allow_merge=args.allow_merge, 
            rsync_mode=args.rsync_mode,
            monitor=monitor,
            show_progress=args.show_progress
        )
    finally:
        if monitor and monitor.metrics_enabled:
            monitor.stop_monitoring()
            
            # Save metrics to file
            if args.metrics_file:
                metrics_path = Path(args.metrics_file)
            else:
                timestamp = datetime.fromtimestamp(monitor.operation.start_time).strftime("%Y%m%d_%H%M%S")
                metrics_path = metrics_dir / f"metrics_{timestamp}_{monitor.operation.operation_id}.json"
            
            monitor.save_metrics(metrics_path)
            
            # Generate and display summary report
            if args.report_format == "text":
                reporter = MetricsReporter(database)
                print(f"\n{reporter.generate_summary_report(monitor.operation)}")
                
                # Show performance charts if verbose
                if args.verbose:
                    charts = reporter.generate_performance_charts(monitor.operation)
                    if charts:
                        print("\nPERFORMANCE CHARTS:")
                        print("=" * 80)
                        print(charts)
            elif args.report_format == "json":
                print(json.dumps(monitor.operation.to_dict(), indent=2, default=str))
            elif args.report_format == "csv" and args.metrics_file:
                csv_path = Path(args.metrics_file).with_suffix('.csv')
                monitor.export_csv(csv_path)
                print(f"Metrics exported to CSV: {csv_path}")

    if failures:
        print(f"\nCompleted with {failures} failure(s). Review the log above.")
        return 1
    else:
        print("\nCompleted successfully.")
        return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\nInterrupted.")
        sys.exit(130)
