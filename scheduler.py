#!/usr/bin/env python3
"""
Advanced Scheduling System for Unraid Rebalancer

Provides comprehensive scheduling capabilities including cron integration,
resource-aware scheduling, and flexible timing options for automated rebalancing operations.
"""

import json
import logging
import os
import re
import signal
import subprocess
import tempfile
import time
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Union, Any, Tuple
from dataclasses import dataclass, asdict
from enum import Enum


class ScheduleType(Enum):
    """Types of supported schedules."""
    ONE_TIME = "one_time"
    RECURRING = "recurring"
    CONDITIONAL = "conditional"


class TriggerType(Enum):
    """Types of scheduling triggers."""
    TIME_BASED = "time_based"
    RESOURCE_BASED = "resource_based"
    DISK_USAGE = "disk_usage"
    SYSTEM_IDLE = "system_idle"


class ExecutionStatus(Enum):
    """Status of schedule execution."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    SUSPENDED = "suspended"


@dataclass
class ScheduleExecution:
    """Record of a schedule execution."""
    execution_id: str
    schedule_id: str
    start_time: float
    end_time: Optional[float] = None
    status: ExecutionStatus = ExecutionStatus.PENDING
    exit_code: Optional[int] = None
    error_message: str = ""
    operation_id: str = ""
    files_moved: int = 0
    bytes_moved: int = 0
    duration_seconds: float = 0.0
    pid: Optional[int] = None
    
    def __post_init__(self):
        """Initialize execution record."""
        if not self.execution_id:
            self.execution_id = f"exec_{int(self.start_time)}_{self.schedule_id}_{uuid.uuid4().hex[:8]}"
        if not self.operation_id:
            self.operation_id = f"rebalance_{int(self.start_time)}_{uuid.uuid4().hex[:8]}"
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        result = asdict(self)
        result['status'] = self.status.value
        return result
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ScheduleExecution':
        """Create from dictionary."""
        if 'status' in data and isinstance(data['status'], str):
            data['status'] = ExecutionStatus(data['status'])
        return cls(**data)


@dataclass
class ScheduleStatistics:
    """Statistics for a schedule."""
    schedule_id: str
    total_executions: int = 0
    successful_executions: int = 0
    failed_executions: int = 0
    cancelled_executions: int = 0
    last_execution_time: Optional[float] = None
    last_success_time: Optional[float] = None
    last_failure_time: Optional[float] = None
    average_duration_seconds: float = 0.0
    total_files_moved: int = 0
    total_bytes_moved: int = 0
    
    @property
    def success_rate(self) -> float:
        """Calculate success rate as percentage."""
        if self.total_executions == 0:
            return 0.0
        return (self.successful_executions / self.total_executions) * 100.0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ScheduleStatistics':
        """Create from dictionary."""
        return cls(**data)


@dataclass
class ResourceThresholds:
    """System resource thresholds for scheduling decisions."""
    max_cpu_percent: float = 50.0
    max_memory_percent: float = 80.0
    max_disk_io_mbps: float = 100.0
    min_idle_minutes: int = 15


@dataclass
class ScheduleConfig:
    """Configuration for a scheduled rebalancing operation."""
    
    # Basic schedule information
    schedule_id: str
    name: str
    description: str = ""
    created_at: float = 0.0
    last_modified: float = 0.0
    
    # Schedule timing
    schedule_type: ScheduleType = ScheduleType.RECURRING
    cron_expression: str = ""  # Standard cron format: "0 2 * * *" (2 AM daily)
    
    # Trigger conditions
    trigger_type: TriggerType = TriggerType.TIME_BASED
    resource_thresholds: ResourceThresholds = None
    disk_usage_threshold: float = 85.0  # Trigger when disk usage exceeds this percent
    
    # Rebalancing operation parameters
    target_percent: float = 80.0
    headroom_percent: float = 5.0
    min_unit_size: int = 1073741824  # 1GiB in bytes
    rsync_mode: str = "balanced"
    include_disks: List[str] = None
    exclude_disks: List[str] = None
    include_shares: List[str] = None
    exclude_shares: List[str] = None
    exclude_globs: List[str] = None
    
    # Schedule management
    enabled: bool = True
    max_runtime_hours: int = 6
    retry_count: int = 3
    retry_delay_minutes: int = 30
    
    # Notification settings
    notify_on_success: bool = False
    notify_on_failure: bool = True
    notification_email: str = ""
    
    # Execution tracking
    last_execution_time: Optional[float] = None
    last_execution_status: Optional[ExecutionStatus] = None
    execution_count: int = 0
    success_count: int = 0
    failure_count: int = 0
    suspended: bool = False
    suspend_reason: str = ""
    current_execution_id: Optional[str] = None
    current_pid: Optional[int] = None
    
    def __post_init__(self):
        """Initialize default values after dataclass creation."""
        if self.created_at == 0.0:
            self.created_at = time.time()
        if self.last_modified == 0.0:
            self.last_modified = time.time()
        if self.resource_thresholds is None:
            self.resource_thresholds = ResourceThresholds()
        if self.include_disks is None:
            self.include_disks = []
        if self.exclude_disks is None:
            self.exclude_disks = []
        if self.include_shares is None:
            self.include_shares = []
        if self.exclude_shares is None:
            self.exclude_shares = []
        if self.exclude_globs is None:
            self.exclude_globs = []


class CronExpressionValidator:
    """Validates and parses cron expressions."""
    
    # Standard cron format: minute hour day_of_month month day_of_week
    CRON_PATTERN = re.compile(
        r'^'
        r'([0-9*,-/]+)\s+'     # minute (0-59)
        r'([0-9*,-/]+)\s+'     # hour (0-23)
        r'([0-9*,-/]+)\s+'     # day of month (1-31)
        r'([0-9*,-/]+)\s+'     # month (1-12)
        r'([0-9*,-/]+)'        # day of week (0-7, 0 and 7 are Sunday)
        r'$'
    )
    
    @classmethod
    def validate_cron_expression(cls, expression: str) -> bool:
        """Validate a cron expression format."""
        if not expression or not isinstance(expression, str):
            return False
        
        expression = expression.strip()
        if not cls.CRON_PATTERN.match(expression):
            return False
        
        parts = expression.split()
        if len(parts) != 5:
            return False
        
        # Validate individual parts
        try:
            minute, hour, day, month, dow = parts
            
            # Basic range validation (simplified)
            if not cls._validate_field(minute, 0, 59):
                return False
            if not cls._validate_field(hour, 0, 23):
                return False
            if not cls._validate_field(day, 1, 31):
                return False
            if not cls._validate_field(month, 1, 12):
                return False
            if not cls._validate_field(dow, 0, 7):
                return False
                
            return True
            
        except Exception:
            return False
    
    @classmethod
    def _validate_field(cls, field: str, min_val: int, max_val: int) -> bool:
        """Validate a single cron field."""
        if field == "*":
            return True
        
        # Handle ranges, lists, and steps
        for part in field.split(','):
            if '/' in part:
                # Step values like "*/5" or "0-20/5"
                range_part, step = part.split('/', 1)
                try:
                    step_val = int(step)
                    if step_val <= 0:
                        return False
                except ValueError:
                    return False
                part = range_part
            
            if '-' in part:
                # Range like "0-20"
                try:
                    start, end = part.split('-', 1)
                    start_val = int(start)
                    end_val = int(end)
                    if not (min_val <= start_val <= max_val and min_val <= end_val <= max_val):
                        return False
                    if start_val > end_val:
                        return False
                except ValueError:
                    return False
            elif part != "*":
                # Single value
                try:
                    val = int(part)
                    if not (min_val <= val <= max_val):
                        return False
                except ValueError:
                    return False
        
        return True
    
    @classmethod
    def create_daily_expression(cls, hour: int, minute: int = 0) -> str:
        """Create a daily cron expression."""
        return f"{minute} {hour} * * *"
    
    @classmethod
    def create_weekly_expression(cls, day_of_week: int, hour: int, minute: int = 0) -> str:
        """Create a weekly cron expression."""
        return f"{minute} {hour} * * {day_of_week}"
    
    @classmethod
    def create_monthly_expression(cls, day_of_month: int, hour: int, minute: int = 0) -> str:
        """Create a monthly cron expression."""
        return f"{minute} {hour} {day_of_month} * *"


class ScheduleManager:
    """Manages scheduling configurations and persistence."""
    
    def __init__(self, config_dir: Union[str, Path] = "./schedules"):
        self.config_dir = Path(config_dir)
        self.config_dir.mkdir(parents=True, exist_ok=True)
        self.schedules: Dict[str, ScheduleConfig] = {}
        self.load_schedules()
    
    def create_schedule(self, schedule: ScheduleConfig) -> bool:
        """Create a new schedule configuration."""
        try:
            # Validate cron expression if provided
            if schedule.cron_expression and not CronExpressionValidator.validate_cron_expression(schedule.cron_expression):
                logging.error(f"Invalid cron expression: {schedule.cron_expression}")
                return False
            
            # Ensure unique ID
            if schedule.schedule_id in self.schedules:
                logging.error(f"Schedule ID {schedule.schedule_id} already exists")
                return False
            
            schedule.last_modified = time.time()
            self.schedules[schedule.schedule_id] = schedule
            self.save_schedule(schedule)
            
            logging.info(f"Created schedule '{schedule.name}' with ID {schedule.schedule_id}")
            return True
            
        except Exception as e:
            logging.error(f"Failed to create schedule: {e}")
            return False
    
    def update_schedule(self, schedule_id: str, schedule: ScheduleConfig) -> bool:
        """Update an existing schedule configuration."""
        try:
            if schedule_id not in self.schedules:
                logging.error(f"Schedule ID {schedule_id} not found")
                return False
            
            # Validate cron expression if provided
            if schedule.cron_expression and not CronExpressionValidator.validate_cron_expression(schedule.cron_expression):
                logging.error(f"Invalid cron expression: {schedule.cron_expression}")
                return False
            
            schedule.schedule_id = schedule_id  # Ensure ID consistency
            schedule.last_modified = time.time()
            self.schedules[schedule_id] = schedule
            self.save_schedule(schedule)
            
            logging.info(f"Updated schedule '{schedule.name}' with ID {schedule_id}")
            return True
            
        except Exception as e:
            logging.error(f"Failed to update schedule: {e}")
            return False
    
    def delete_schedule(self, schedule_id: str) -> bool:
        """Delete a schedule configuration."""
        try:
            if schedule_id not in self.schedules:
                logging.error(f"Schedule ID {schedule_id} not found")
                return False
            
            # Remove from memory
            schedule = self.schedules.pop(schedule_id)
            
            # Remove from disk
            config_file = self.config_dir / f"{schedule_id}.json"
            if config_file.exists():
                config_file.unlink()
            
            logging.info(f"Deleted schedule '{schedule.name}' with ID {schedule_id}")
            return True
            
        except Exception as e:
            logging.error(f"Failed to delete schedule: {e}")
            return False
    
    def get_schedule(self, schedule_id: str) -> Optional[ScheduleConfig]:
        """Get a schedule configuration by ID."""
        return self.schedules.get(schedule_id)
    
    def list_schedules(self) -> List[ScheduleConfig]:
        """List all schedule configurations."""
        return list(self.schedules.values())
    
    def list_enabled_schedules(self) -> List[ScheduleConfig]:
        """List only enabled schedule configurations."""
        return [schedule for schedule in self.schedules.values() if schedule.enabled]
    
    def save_schedule(self, schedule: ScheduleConfig) -> bool:
        """Save a schedule configuration to disk."""
        try:
            config_file = self.config_dir / f"{schedule.schedule_id}.json"
            schedule_dict = asdict(schedule)
            
            with open(config_file, 'w') as f:
                json.dump(schedule_dict, f, indent=2)
            
            return True
            
        except Exception as e:
            logging.error(f"Failed to save schedule {schedule.schedule_id}: {e}")
            return False
    
    def load_schedules(self) -> bool:
        """Load all schedule configurations from disk."""
        try:
            self.schedules.clear()
            
            for config_file in self.config_dir.glob("*.json"):
                try:
                    with open(config_file, 'r') as f:
                        schedule_dict = json.load(f)
                    
                    # Convert back to ScheduleConfig object
                    # Handle enum conversions
                    if 'schedule_type' in schedule_dict:
                        schedule_dict['schedule_type'] = ScheduleType(schedule_dict['schedule_type'])
                    if 'trigger_type' in schedule_dict:
                        schedule_dict['trigger_type'] = TriggerType(schedule_dict['trigger_type'])
                    
                    # Handle nested ResourceThresholds
                    if 'resource_thresholds' in schedule_dict and schedule_dict['resource_thresholds']:
                        schedule_dict['resource_thresholds'] = ResourceThresholds(**schedule_dict['resource_thresholds'])
                    
                    schedule = ScheduleConfig(**schedule_dict)
                    self.schedules[schedule.schedule_id] = schedule
                    
                except Exception as e:
                    logging.warning(f"Failed to load schedule from {config_file}: {e}")
                    continue
            
            logging.info(f"Loaded {len(self.schedules)} schedule configurations")
            return True
            
        except Exception as e:
            logging.error(f"Failed to load schedules: {e}")
            return False


class CronManager:
    """Manages cron job creation and removal for scheduled operations."""
    
    def __init__(self, script_path: Union[str, Path]):
        self.script_path = Path(script_path).absolute()
        self.cron_comment_prefix = "# Unraid Rebalancer Schedule:"
    
    def install_schedule(self, schedule: ScheduleConfig) -> bool:
        """Install a schedule as a cron job."""
        try:
            if not schedule.cron_expression:
                logging.error(f"No cron expression for schedule {schedule.schedule_id}")
                return False
            
            # Generate the cron job command
            cron_command = self._generate_cron_command(schedule)
            cron_line = f"{schedule.cron_expression} {cron_command}"
            cron_comment = f"{self.cron_comment_prefix} {schedule.schedule_id}"
            
            # Get current crontab
            current_crontab = self._get_current_crontab()
            
            # Remove any existing entry for this schedule
            self._remove_schedule_from_crontab(schedule.schedule_id, current_crontab)
            
            # Add new entry
            current_crontab.append(cron_comment)
            current_crontab.append(cron_line)
            
            # Install updated crontab
            if self._install_crontab(current_crontab):
                logging.info(f"Installed cron job for schedule {schedule.schedule_id}")
                return True
            else:
                logging.error(f"Failed to install cron job for schedule {schedule.schedule_id}")
                return False
                
        except Exception as e:
            logging.error(f"Failed to install schedule {schedule.schedule_id}: {e}")
            return False
    
    def remove_schedule(self, schedule_id: str) -> bool:
        """Remove a schedule's cron job."""
        try:
            current_crontab = self._get_current_crontab()
            original_length = len(current_crontab)
            
            self._remove_schedule_from_crontab(schedule_id, current_crontab)
            
            if len(current_crontab) < original_length:
                if self._install_crontab(current_crontab):
                    logging.info(f"Removed cron job for schedule {schedule_id}")
                    return True
                else:
                    logging.error(f"Failed to update crontab when removing schedule {schedule_id}")
                    return False
            else:
                logging.info(f"No cron job found for schedule {schedule_id}")
                return True
                
        except Exception as e:
            logging.error(f"Failed to remove schedule {schedule_id}: {e}")
            return False
    
    def list_installed_schedules(self) -> List[str]:
        """List schedule IDs that have installed cron jobs."""
        try:
            current_crontab = self._get_current_crontab()
            schedule_ids = []
            
            for line in current_crontab:
                if line.startswith(self.cron_comment_prefix):
                    # Extract schedule ID from comment
                    schedule_id = line[len(self.cron_comment_prefix):].strip()
                    if schedule_id:
                        schedule_ids.append(schedule_id)
            
            return schedule_ids
            
        except Exception as e:
            logging.error(f"Failed to list installed schedules: {e}")
            return []
    
    def _generate_cron_command(self, schedule: ScheduleConfig) -> str:
        """Generate the command to run for a scheduled operation."""
        cmd_parts = [str(self.script_path)]
        
        # Add rebalancing parameters
        cmd_parts.extend([
            "--target-percent", str(schedule.target_percent),
            "--headroom-percent", str(schedule.headroom_percent),
            "--min-unit-size", str(schedule.min_unit_size),
            "--rsync-mode", schedule.rsync_mode,
            "--execute",  # Always execute for scheduled operations
            "--metrics",  # Enable metrics for scheduled operations
            "--log-file", f"/tmp/rebalancer_schedule_{schedule.schedule_id}.log"
        ])
        
        # Add disk filters
        if schedule.include_disks:
            cmd_parts.extend(["--include-disks", ",".join(schedule.include_disks)])
        if schedule.exclude_disks:
            cmd_parts.extend(["--exclude-disks", ",".join(schedule.exclude_disks)])
        
        # Add share filters
        if schedule.include_shares:
            cmd_parts.extend(["--include-shares", ",".join(schedule.include_shares)])
        if schedule.exclude_shares:
            cmd_parts.extend(["--exclude-shares", ",".join(schedule.exclude_shares)])
        
        # Add glob filters
        if schedule.exclude_globs:
            cmd_parts.extend(["--exclude-globs", ",".join(schedule.exclude_globs)])
        
        # Add schedule-specific parameters
        cmd_parts.extend([
            "--schedule-id", schedule.schedule_id,
            "--max-runtime", str(schedule.max_runtime_hours)
        ])
        
        return " ".join(f'"{part}"' if " " in part else part for part in cmd_parts)
    
    def _get_current_crontab(self) -> List[str]:
        """Get the current user's crontab as a list of lines."""
        try:
            result = subprocess.run(
                ["crontab", "-l"], 
                capture_output=True, 
                text=True, 
                check=False
            )
            
            if result.returncode == 0:
                return [line.strip() for line in result.stdout.split('\n') if line.strip()]
            else:
                # No crontab exists yet
                return []
                
        except Exception as e:
            logging.warning(f"Failed to read current crontab: {e}")
            return []
    
    def _remove_schedule_from_crontab(self, schedule_id: str, crontab_lines: List[str]):
        """Remove all lines related to a specific schedule from crontab list."""
        comment_line = f"{self.cron_comment_prefix} {schedule_id}"
        
        # Remove comment and following command line
        i = 0
        while i < len(crontab_lines):
            if crontab_lines[i] == comment_line:
                # Remove comment line
                crontab_lines.pop(i)
                # Remove following command line if it exists
                if i < len(crontab_lines) and not crontab_lines[i].startswith("#"):
                    crontab_lines.pop(i)
            else:
                i += 1
    
    def _install_crontab(self, crontab_lines: List[str]) -> bool:
        """Install a new crontab from a list of lines."""
        try:
            # Create temporary file with new crontab content
            with tempfile.NamedTemporaryFile(mode='w', suffix='.cron', delete=False) as f:
                for line in crontab_lines:
                    f.write(line + '\n')
                temp_file = f.name
            
            try:
                # Install the new crontab
                result = subprocess.run(
                    ["crontab", temp_file],
                    capture_output=True,
                    text=True,
                    check=True
                )
                
                return result.returncode == 0
                
            finally:
                # Clean up temporary file
                Path(temp_file).unlink(missing_ok=True)
                
        except subprocess.CalledProcessError as e:
            logging.error(f"Failed to install crontab: {e.stderr}")
            return False
        except Exception as e:
            logging.error(f"Failed to install crontab: {e}")
            return False


class SchedulingEngine:
    """Main scheduling engine that coordinates schedule management and execution."""
    
    def __init__(self, script_path: Union[str, Path], config_dir: Union[str, Path] = "./schedules"):
        self.script_path = Path(script_path)
        self.schedule_manager = ScheduleManager(config_dir)
        self.cron_manager = CronManager(script_path)
        self.monitor = ScheduleMonitor(config_dir)
    
    def create_and_install_schedule(self, schedule: ScheduleConfig) -> bool:
        """Create a schedule and install it as a cron job."""
        if not self.schedule_manager.create_schedule(schedule):
            return False
        
        if schedule.enabled and schedule.cron_expression:
            return self.cron_manager.install_schedule(schedule)
        
        return True
    
    def update_and_reinstall_schedule(self, schedule_id: str, schedule: ScheduleConfig) -> bool:
        """Update a schedule and reinstall its cron job."""
        # Remove old cron job
        self.cron_manager.remove_schedule(schedule_id)
        
        # Update schedule
        if not self.schedule_manager.update_schedule(schedule_id, schedule):
            return False
        
        # Install new cron job if enabled
        if schedule.enabled and schedule.cron_expression:
            return self.cron_manager.install_schedule(schedule)
        
        return True
    
    def delete_schedule(self, schedule_id: str) -> bool:
        """Delete a schedule and remove its cron job."""
        # Remove cron job first
        self.cron_manager.remove_schedule(schedule_id)
        
        # Remove schedule configuration
        return self.schedule_manager.delete_schedule(schedule_id)
    
    def enable_schedule(self, schedule_id: str) -> bool:
        """Enable a schedule and install its cron job."""
        schedule = self.schedule_manager.get_schedule(schedule_id)
        if not schedule:
            return False
        
        schedule.enabled = True
        schedule.last_modified = time.time()
        self.schedule_manager.save_schedule(schedule)
        
        if schedule.cron_expression:
            return self.cron_manager.install_schedule(schedule)
        
        return True
    
    def disable_schedule(self, schedule_id: str) -> bool:
        """Disable a schedule and remove its cron job."""
        schedule = self.schedule_manager.get_schedule(schedule_id)
        if not schedule:
            return False
        
        schedule.enabled = False
        schedule.last_modified = time.time()
        self.schedule_manager.save_schedule(schedule)
        
        return self.cron_manager.remove_schedule(schedule_id)
    
    def sync_schedules(self) -> bool:
        """Synchronize schedule configurations with installed cron jobs."""
        try:
            enabled_schedules = self.schedule_manager.list_enabled_schedules()
            installed_schedule_ids = set(self.cron_manager.list_installed_schedules())
            
            success = True
            
            # Install missing cron jobs
            for schedule in enabled_schedules:
                if schedule.cron_expression and schedule.schedule_id not in installed_schedule_ids:
                    if not self.cron_manager.install_schedule(schedule):
                        success = False
            
            # Remove orphaned cron jobs
            valid_schedule_ids = {s.schedule_id for s in enabled_schedules if s.cron_expression}
            for schedule_id in installed_schedule_ids:
                if schedule_id not in valid_schedule_ids:
                    if not self.cron_manager.remove_schedule(schedule_id):
                        success = False
            
            return success
            
        except Exception as e:
            logging.error(f"Failed to sync schedules: {e}")
            return False


class SystemResourceMonitor:
    """Monitors system resources for scheduling decisions."""
    
    def __init__(self):
        self._last_check = 0
        self._cache_duration = 30  # Cache results for 30 seconds
        self._cached_metrics = {}
    
    def get_current_usage(self) -> Dict[str, float]:
        """Get current system resource usage."""
        current_time = time.time()
        
        # Return cached results if recent
        if current_time - self._last_check < self._cache_duration and self._cached_metrics:
            return self._cached_metrics
        
        try:
            import psutil
            
            # CPU usage (average over 1 second)
            cpu_percent = psutil.cpu_percent(interval=1.0)
            
            # Memory usage
            memory = psutil.virtual_memory()
            memory_percent = memory.percent
            
            # Disk I/O
            disk_io = psutil.disk_io_counters()
            if hasattr(self, '_prev_disk_io') and self._prev_disk_io:
                time_delta = current_time - self._last_check
                read_bps = (disk_io.read_bytes - self._prev_disk_io.read_bytes) / time_delta
                write_bps = (disk_io.write_bytes - self._prev_disk_io.write_bytes) / time_delta
                disk_io_bps = read_bps + write_bps
            else:
                disk_io_bps = 0
            
            self._prev_disk_io = disk_io
            
            # Network I/O
            try:
                net_io = psutil.net_io_counters()
                if hasattr(self, '_prev_net_io') and self._prev_net_io:
                    time_delta = current_time - self._last_check
                    net_sent_bps = (net_io.bytes_sent - self._prev_net_io.bytes_sent) / time_delta
                    net_recv_bps = (net_io.bytes_recv - self._prev_net_io.bytes_recv) / time_delta
                else:
                    net_sent_bps = net_recv_bps = 0
                self._prev_net_io = net_io
            except:
                net_sent_bps = net_recv_bps = 0
            
            self._cached_metrics = {
                'cpu_percent': cpu_percent,
                'memory_percent': memory_percent,
                'disk_io_bps': disk_io_bps,
                'disk_io_mbps': disk_io_bps / (1024 * 1024),
                'network_sent_bps': net_sent_bps,
                'network_recv_bps': net_recv_bps,
                'timestamp': current_time
            }
            
            self._last_check = current_time
            return self._cached_metrics
            
        except ImportError:
            logging.warning("psutil not available for resource monitoring")
            return {
                'cpu_percent': 0,
                'memory_percent': 0,
                'disk_io_bps': 0,
                'disk_io_mbps': 0,
                'network_sent_bps': 0,
                'network_recv_bps': 0,
                'timestamp': current_time
            }
        except Exception as e:
            logging.error(f"Failed to get system resources: {e}")
            return self._cached_metrics if self._cached_metrics else {}
    
    def check_resource_thresholds(self, thresholds: ResourceThresholds) -> bool:
        """Check if system resources are within specified thresholds."""
        try:
            usage = self.get_current_usage()
            
            if usage['cpu_percent'] > thresholds.max_cpu_percent:
                logging.debug(f"CPU usage {usage['cpu_percent']:.1f}% exceeds threshold {thresholds.max_cpu_percent}%")
                return False
            
            if usage['memory_percent'] > thresholds.max_memory_percent:
                logging.debug(f"Memory usage {usage['memory_percent']:.1f}% exceeds threshold {thresholds.max_memory_percent}%")
                return False
            
            if usage['disk_io_mbps'] > thresholds.max_disk_io_mbps:
                logging.debug(f"Disk I/O {usage['disk_io_mbps']:.1f} MB/s exceeds threshold {thresholds.max_disk_io_mbps} MB/s")
                return False
            
            return True
            
        except Exception as e:
            logging.error(f"Failed to check resource thresholds: {e}")
            # If we can't check, assume resources are available
            return True
    
    def get_idle_time_minutes(self) -> float:
        """Get approximate system idle time in minutes."""
        try:
            # Simple heuristic: if CPU usage has been low for a while, system is likely idle
            usage = self.get_current_usage()
            if usage['cpu_percent'] < 10:  # Less than 10% CPU usage
                return max(0, (time.time() - self._last_check) / 60.0)
            return 0
        except:
            return 0


class ConditionalScheduler:
    """Handles conditional scheduling based on system state and resources."""
    
    def __init__(self):
        self.resource_monitor = SystemResourceMonitor()
    
    def should_execute_schedule(self, schedule: ScheduleConfig) -> Tuple[bool, str]:
        """Determine if a schedule should execute based on its conditions."""
        
        # Always execute time-based schedules (handled by cron)
        if schedule.trigger_type == TriggerType.TIME_BASED:
            return True, "Time-based schedule"
        
        # Check resource-based conditions
        if schedule.trigger_type == TriggerType.RESOURCE_BASED:
            if not self.resource_monitor.check_resource_thresholds(schedule.resource_thresholds):
                return False, "System resources exceed thresholds"
            return True, "Resource conditions met"
        
        # Check system idle conditions
        if schedule.trigger_type == TriggerType.SYSTEM_IDLE:
            idle_time = self.resource_monitor.get_idle_time_minutes()
            if idle_time < schedule.resource_thresholds.min_idle_minutes:
                return False, f"System not idle long enough ({idle_time:.1f} < {schedule.resource_thresholds.min_idle_minutes} min)"
            
            if not self.resource_monitor.check_resource_thresholds(schedule.resource_thresholds):
                return False, "System not idle (resource usage too high)"
            
            return True, f"System idle for {idle_time:.1f} minutes"
        
        # Check disk usage triggers
        if schedule.trigger_type == TriggerType.DISK_USAGE:
            # This would need integration with disk discovery
            # For now, always return True
            return True, "Disk usage trigger (not implemented yet)"
        
        return True, "Default execution"
    
    def get_next_execution_recommendation(self, schedule: ScheduleConfig) -> Optional[datetime]:
        """Get recommended next execution time for conditional schedules."""
        if schedule.trigger_type == TriggerType.TIME_BASED:
            # Handled by cron
            return None
        
        current_time = datetime.now()
        
        # For resource-based and idle-based schedules, suggest checking again soon
        if schedule.trigger_type in [TriggerType.RESOURCE_BASED, TriggerType.SYSTEM_IDLE]:
            # Check again in 15 minutes
            return current_time + timedelta(minutes=15)
        
        # For disk usage triggers, check daily
        if schedule.trigger_type == TriggerType.DISK_USAGE:
            return current_time + timedelta(hours=24)
        
        return None


class ScheduleMonitor:
    """Monitors and controls schedule execution with logging and emergency controls."""
    
    def __init__(self, config_dir: Union[str, Path] = "./schedules"):
        self.config_dir = Path(config_dir)
        self.executions_dir = self.config_dir / "executions"
        self.executions_dir.mkdir(parents=True, exist_ok=True)
        self.running_executions: Dict[str, ScheduleExecution] = {}
        
    def start_execution(self, schedule_id: str, pid: Optional[int] = None) -> ScheduleExecution:
        """Start tracking a new schedule execution."""
        execution = ScheduleExecution(
            execution_id="",
            schedule_id=schedule_id,
            start_time=time.time(),
            status=ExecutionStatus.RUNNING,
            pid=pid
        )
        
        # Store running execution
        self.running_executions[execution.execution_id] = execution
        
        # Save execution record
        self._save_execution(execution)
        
        logging.info(f"Started execution {execution.execution_id} for schedule {schedule_id}")
        return execution
    
    def complete_execution(self, execution_id: str, exit_code: int = 0, 
                          files_moved: int = 0, bytes_moved: int = 0, 
                          error_message: str = "") -> bool:
        """Mark an execution as completed."""
        if execution_id not in self.running_executions:
            logging.warning(f"Execution {execution_id} not found in running executions")
            return False
        
        execution = self.running_executions[execution_id]
        execution.end_time = time.time()
        execution.duration_seconds = execution.end_time - execution.start_time
        execution.exit_code = exit_code
        execution.files_moved = files_moved
        execution.bytes_moved = bytes_moved
        execution.error_message = error_message
        
        # Determine status based on exit code
        if exit_code == 0:
            execution.status = ExecutionStatus.COMPLETED
        else:
            execution.status = ExecutionStatus.FAILED
        
        # Remove from running executions
        del self.running_executions[execution_id]
        
        # Save final execution record
        self._save_execution(execution)
        
        logging.info(f"Completed execution {execution_id} with status {execution.status.value}")
        return True
    
    def cancel_execution(self, execution_id: str, reason: str = "User cancelled") -> bool:
        """Cancel a running execution."""
        if execution_id not in self.running_executions:
            logging.warning(f"Execution {execution_id} not found in running executions")
            return False
        
        execution = self.running_executions[execution_id]
        
        # Try to kill the process if PID is available
        if execution.pid:
            try:
                os.kill(execution.pid, signal.SIGTERM)
                logging.info(f"Sent SIGTERM to process {execution.pid}")
                
                # Wait a bit, then force kill if still running
                time.sleep(5)
                try:
                    os.kill(execution.pid, signal.SIGKILL)
                    logging.info(f"Sent SIGKILL to process {execution.pid}")
                except ProcessLookupError:
                    pass  # Process already terminated
            except ProcessLookupError:
                logging.info(f"Process {execution.pid} already terminated")
            except PermissionError:
                logging.error(f"Permission denied to kill process {execution.pid}")
                return False
        
        # Update execution record
        execution.end_time = time.time()
        execution.duration_seconds = execution.end_time - execution.start_time
        execution.status = ExecutionStatus.CANCELLED
        execution.error_message = reason
        
        # Remove from running executions
        del self.running_executions[execution_id]
        
        # Save execution record
        self._save_execution(execution)
        
        logging.info(f"Cancelled execution {execution_id}: {reason}")
        return True
    
    def suspend_schedule(self, schedule_id: str, reason: str = "Manual suspension") -> bool:
        """Suspend a schedule and cancel any running execution."""
        # Find and cancel any running executions for this schedule
        for execution_id, execution in list(self.running_executions.items()):
            if execution.schedule_id == schedule_id:
                self.cancel_execution(execution_id, f"Schedule suspended: {reason}")
        
        logging.info(f"Suspended schedule {schedule_id}: {reason}")
        return True
    
    def resume_schedule(self, schedule_id: str) -> bool:
        """Resume a suspended schedule."""
        logging.info(f"Resumed schedule {schedule_id}")
        return True
    
    def get_running_executions(self) -> List[ScheduleExecution]:
        """Get all currently running executions."""
        return list(self.running_executions.values())
    
    def get_execution_history(self, schedule_id: Optional[str] = None, limit: int = 50) -> List[ScheduleExecution]:
        """Get execution history, optionally filtered by schedule ID."""
        executions = []
        
        for execution_file in sorted(self.executions_dir.glob("*.json"), reverse=True):
            if len(executions) >= limit:
                break
                
            try:
                with open(execution_file, 'r') as f:
                    execution_data = json.load(f)
                execution = ScheduleExecution.from_dict(execution_data)
                
                if schedule_id is None or execution.schedule_id == schedule_id:
                    executions.append(execution)
            except Exception as e:
                logging.warning(f"Failed to load execution from {execution_file}: {e}")
        
        return executions
    
    def get_schedule_statistics(self, schedule_id: str) -> ScheduleStatistics:
        """Get statistics for a specific schedule."""
        executions = self.get_execution_history(schedule_id, limit=1000)
        
        stats = ScheduleStatistics(schedule_id=schedule_id)
        total_duration = 0.0
        
        for execution in executions:
            stats.total_executions += 1
            stats.total_files_moved += execution.files_moved
            stats.total_bytes_moved += execution.bytes_moved
            
            if execution.status == ExecutionStatus.COMPLETED:
                stats.successful_executions += 1
                if stats.last_success_time is None or execution.start_time > stats.last_success_time:
                    stats.last_success_time = execution.start_time
            elif execution.status == ExecutionStatus.FAILED:
                stats.failed_executions += 1
                if stats.last_failure_time is None or execution.start_time > stats.last_failure_time:
                    stats.last_failure_time = execution.start_time
            elif execution.status == ExecutionStatus.CANCELLED:
                stats.cancelled_executions += 1
            
            if stats.last_execution_time is None or execution.start_time > stats.last_execution_time:
                stats.last_execution_time = execution.start_time
            
            if execution.duration_seconds > 0:
                total_duration += execution.duration_seconds
        
        if stats.total_executions > 0:
            stats.average_duration_seconds = total_duration / stats.total_executions
        
        return stats
    
    def cleanup_old_executions(self, days_to_keep: int = 30) -> int:
        """Clean up execution records older than specified days."""
        cutoff_time = time.time() - (days_to_keep * 24 * 60 * 60)
        removed_count = 0
        
        for execution_file in self.executions_dir.glob("*.json"):
            try:
                with open(execution_file, 'r') as f:
                    execution_data = json.load(f)
                
                if execution_data.get('start_time', 0) < cutoff_time:
                    execution_file.unlink()
                    removed_count += 1
            except Exception as e:
                logging.warning(f"Failed to process execution file {execution_file}: {e}")
        
        logging.info(f"Cleaned up {removed_count} old execution records")
        return removed_count
    
    def _save_execution(self, execution: ScheduleExecution) -> bool:
        """Save execution record to disk."""
        try:
            execution_file = self.executions_dir / f"{execution.execution_id}.json"
            with open(execution_file, 'w') as f:
                json.dump(execution.to_dict(), f, indent=2)
            return True
        except Exception as e:
            logging.error(f"Failed to save execution {execution.execution_id}: {e}")
            return False


class ScheduleTemplateManager:
    """Manages predefined schedule templates for common scenarios."""
    
    @staticmethod
    def get_nightly_template(hour: int = 2) -> ScheduleConfig:
        """Get template for nightly rebalancing."""
        return ScheduleConfig(
            schedule_id="nightly_rebalance",
            name="Nightly Rebalance",
            description="Automatic nightly rebalancing at low-traffic hours",
            cron_expression=CronExpressionValidator.create_daily_expression(hour),
            target_percent=80.0,
            rsync_mode="balanced",
            max_runtime_hours=4,
            resource_thresholds=ResourceThresholds(
                max_cpu_percent=70.0,
                max_memory_percent=80.0,
                max_disk_io_mbps=50.0
            )
        )
    
    @staticmethod
    def get_weekly_template(day: int = 0, hour: int = 3) -> ScheduleConfig:
        """Get template for weekly rebalancing."""
        return ScheduleConfig(
            schedule_id="weekly_rebalance",
            name="Weekly Rebalance",
            description="Weekly comprehensive rebalancing on weekends",
            cron_expression=CronExpressionValidator.create_weekly_expression(day, hour),
            target_percent=75.0,
            rsync_mode="integrity",
            max_runtime_hours=8,
            resource_thresholds=ResourceThresholds(
                max_cpu_percent=80.0,
                max_memory_percent=85.0,
                max_disk_io_mbps=100.0
            )
        )
    
    @staticmethod
    def get_idle_template() -> ScheduleConfig:
        """Get template for idle-based rebalancing."""
        return ScheduleConfig(
            schedule_id="idle_rebalance",
            name="Idle System Rebalance",
            description="Rebalance when system is idle",
            trigger_type=TriggerType.SYSTEM_IDLE,
            target_percent=85.0,
            rsync_mode="fast",
            max_runtime_hours=2,
            resource_thresholds=ResourceThresholds(
                max_cpu_percent=30.0,
                max_memory_percent=70.0,
                max_disk_io_mbps=25.0,
                min_idle_minutes=30
            )
        )
    
    @staticmethod
    def get_disk_usage_template(threshold: float = 90.0) -> ScheduleConfig:
        """Get template for disk usage threshold rebalancing."""
        return ScheduleConfig(
            schedule_id="disk_usage_rebalance",
            name="High Disk Usage Rebalance",
            description=f"Rebalance when disk usage exceeds {threshold}%",
            trigger_type=TriggerType.DISK_USAGE,
            disk_usage_threshold=threshold,
            target_percent=75.0,
            rsync_mode="balanced",
            max_runtime_hours=6,
            resource_thresholds=ResourceThresholds(
                max_cpu_percent=60.0,
                max_memory_percent=75.0,
                max_disk_io_mbps=75.0
            )
        )