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
        CronExpressionValidator, SchedulingEngine
    )
except ImportError:
    ScheduleConfig = None
    SchedulingEngine = None
    logging.warning("Scheduling system not available - scheduling features disabled")
    logging.warning("SQLite metrics storage not available - falling back to JSON")

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
        "flags": ["-av", "--partial", "--inplace", "--numeric-ids", "--no-compress"],
        "description": "Fastest transfers, minimal CPU overhead (recommended for lower-end CPUs)"
    },
    "balanced": {
        "flags": ["-avPR", "-X", "--partial", "--inplace", "--numeric-ids"],
        "description": "Balanced speed and features with extended attributes (good for mid-range CPUs)"
    },
    "integrity": {
        "flags": ["-aHAX", "--info=progress2", "--partial", "--inplace", "--numeric-ids"],
        "description": "Full integrity checking with hard links, ACLs, and progress (for high-end CPUs)"
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
               headroom_percent: float) -> Plan:
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

    # Sort units from donors by size (largest first for fewer moves)
    donor_units = [u for u in units if u.src_disk in donors]
    donor_units.sort(key=lambda u: u.size_bytes, reverse=True)

    # Sort recipients by most capacity needed first
    recipient_list = sorted(recipients.items(), key=lambda kv: kv[1], reverse=True)

    moves: List[Move] = []

    # Create disk lookup for efficiency
    disk_lookup = {d.name: d for d in disks}
    
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
        
        # Ensure parent exists on destination
        if execute:
            dst_parent = dst.parent
            dst_parent.mkdir(parents=True, exist_ok=True)

        # If destination exists and not allowed to merge, skip
        if dst.exists() and not allow_merge:
            print(f"[SKIP] Destination exists and --allow-merge not set: {dst}")
            if transfer:
                monitor.complete_transfer(transfer, False, "Destination exists and merge not allowed")
            continue

        # rsync path handling
        if src.is_dir():
            # Trailing slash to copy contents of dir into directory (rsync semantics)
            src_r = str(src) + "/"
            dst_r = str(dst)
        else:
            src_r = str(src)
            dst_r = str(dst)

        rsync_flags = get_rsync_flags(rsync_mode)
        cmd = ["rsync"] + rsync_flags + rsync_extra + [src_r, dst_r]

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
        
        rc = run(cmd, dry_run=not execute)
        
        if rc != 0:
            error_msg = f"rsync returned {rc}"
            print(f"[ERROR] {error_msg}")
            failures += 1
            if transfer:
                monitor.complete_transfer(transfer, False, error_msg)
            continue

        if execute:
            # After successful copy, remove source files
            if src.is_dir():
                # Remove files that have been copied; then clean up empty dirs
                rm_cmd = ["rsync", "-aHAX", "--remove-source-files", str(src) + "/", str(dst)]
                rc2 = run(rm_cmd, dry_run=False)
                if rc2 != 0:
                    print(f"[WARN] cleanup rsync returned {rc2}")
                    logging.warning(f"Failed to remove source files from {src}")
                
                # Remove empty directories
                try:
                    for root, _, files in os.walk(src, topdown=False):
                        if not files:
                            try:
                                os.rmdir(root)
                            except OSError:
                                pass  # Directory not empty or permission error
                except Exception:
                    logging.warning(f"Error during directory cleanup for {src}")
            else:
                try:
                    os.remove(src)
                except FileNotFoundError:
                    logging.debug(f"Source file {src} already removed")
                except Exception as e:
                    logging.error(f"Failed to remove source file {src}: {e}")
        
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

    args = p.parse_args()
    
    # Handle --list-rsync-modes
    if args.list_rsync_modes:
        print("Available rsync performance modes:\n")
        for mode, config in RSYNC_MODES.items():
            flags_str = " ".join(config["flags"])
            print(f"  {mode:>9}: {config['description']}")
            print(f"           Flags: {flags_str}\n")
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
        plan = build_plan(disks, units, target_percent=target_percent, headroom_percent=args.headroom_percent)
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
