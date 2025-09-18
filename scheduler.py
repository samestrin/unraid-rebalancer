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
import smtplib
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Union, Any, Tuple, Callable
from dataclasses import dataclass, asdict
from enum import Enum
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import threading
import random


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
    RETRYING = "retrying"


class FailureType(Enum):
    """Types of execution failures."""
    SYSTEM_ERROR = "system_error"
    RESOURCE_EXHAUSTION = "resource_exhaustion"
    PERMISSION_DENIED = "permission_denied"
    DISK_ERROR = "disk_error"
    TIMEOUT = "timeout"
    USER_CANCELLED = "user_cancelled"
    CONFIGURATION_ERROR = "configuration_error"
    NETWORK_ERROR = "network_error"
    UNKNOWN = "unknown"


class RetryStrategy(Enum):
    """Retry strategies for failed executions."""
    NONE = "none"
    FIXED_DELAY = "fixed_delay"
    EXPONENTIAL_BACKOFF = "exponential_backoff"
    LINEAR_BACKOFF = "linear_backoff"


class NotificationLevel(Enum):
    """Notification severity levels."""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


@dataclass
class FailureRecord:
    """Record of an execution failure."""
    failure_id: str
    execution_id: str
    schedule_id: str
    failure_type: FailureType
    error_message: str
    stack_trace: str = ""
    timestamp: float = 0.0
    retry_attempt: int = 0
    
    def __post_init__(self):
        if self.timestamp == 0.0:
            self.timestamp = time.time()
        if not self.failure_id:
            self.failure_id = str(uuid.uuid4())


@dataclass
class RetryConfig:
    """Configuration for retry behavior."""
    strategy: RetryStrategy = RetryStrategy.EXPONENTIAL_BACKOFF
    max_attempts: int = 3
    base_delay_seconds: int = 60
    max_delay_seconds: int = 3600
    backoff_multiplier: float = 2.0
    jitter: bool = True
    
    def calculate_delay(self, attempt: int) -> int:
        """Calculate delay for given retry attempt."""
        if self.strategy == RetryStrategy.NONE:
            return 0
        elif self.strategy == RetryStrategy.FIXED_DELAY:
            delay = self.base_delay_seconds
        elif self.strategy == RetryStrategy.LINEAR_BACKOFF:
            delay = self.base_delay_seconds * attempt
        elif self.strategy == RetryStrategy.EXPONENTIAL_BACKOFF:
            delay = min(self.base_delay_seconds * (self.backoff_multiplier ** (attempt - 1)), 
                       self.max_delay_seconds)
        else:
            delay = self.base_delay_seconds
        
        # Add jitter to prevent thundering herd
        if self.jitter and delay > 0:
            delay = int(delay * (0.5 + random.random() * 0.5))
        
        return max(delay, 1)


@dataclass
class NotificationConfig:
    """Configuration for notifications."""
    enabled: bool = False
    email_enabled: bool = False
    smtp_server: str = ""
    smtp_port: int = 587
    smtp_username: str = ""
    smtp_password: str = ""
    smtp_use_tls: bool = True
    from_email: str = ""
    to_emails: List[str] = None
    webhook_url: str = ""
    webhook_enabled: bool = False
    
    def __post_init__(self):
        if self.to_emails is None:
            self.to_emails = []


@dataclass
class ScheduleExecution:
    """Represents a single execution of a scheduled operation."""
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
    failure_type: Optional[FailureType] = None
    retry_attempt: int = 0
    max_retries: int = 0
    next_retry_time: Optional[float] = None
    failure_records: List[FailureRecord] = None
    
    def __post_init__(self):
        """Initialize execution record."""
        if not self.execution_id:
            self.execution_id = f"exec_{int(self.start_time)}_{self.schedule_id}_{uuid.uuid4().hex[:8]}"
        if not self.operation_id:
            self.operation_id = f"rebalance_{int(self.start_time)}_{uuid.uuid4().hex[:8]}"
        if self.failure_records is None:
            self.failure_records = []
    
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
    
    def to_dict(self) -> dict:
        """Convert ScheduleConfig to dictionary for serialization."""
        data = asdict(self)
        
        # Convert enums to their string values
        data['schedule_type'] = self.schedule_type.value
        data['trigger_type'] = self.trigger_type.value
        
        # Convert ExecutionStatus enum if present
        if self.last_execution_status is not None:
            data['last_execution_status'] = self.last_execution_status.value
        
        return data
    
    def save_to_file(self, file_path: Path) -> bool:
        """Save schedule configuration to JSON file."""
        try:
            # Ensure parent directories exist
            file_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Convert to dictionary and save as JSON
            data = self.to_dict()
            with open(file_path, 'w') as f:
                json.dump(data, f, indent=2)
            
            return True
            
        except Exception as e:
            logging.error(f"Failed to save schedule to {file_path}: {e}")
            return False
    
    def is_valid(self) -> bool:
        """Validate schedule configuration."""
        # Check required fields
        if not self.schedule_id or not self.name:
            return False
        
        # Validate schedule type
        if not isinstance(self.schedule_type, ScheduleType):
            return False
        
        # Validate cron expression if provided
        if self.cron_expression:
            if not CronExpressionValidator.validate_cron_expression(self.cron_expression):
                return False
        
        # Validate resource thresholds if set
        if self.resource_thresholds:
            if (self.resource_thresholds.max_cpu_percent < 0 or 
                self.resource_thresholds.max_cpu_percent > 100):
                return False
            if (self.resource_thresholds.max_memory_percent < 0 or 
                self.resource_thresholds.max_memory_percent > 100):
                return False
            if self.resource_thresholds.max_disk_io_mbps < 0:
                return False
        
        # Validate target_percent
        if self.target_percent < 0 or self.target_percent > 100:
            return False
        
        # Validate runtime limits
        if self.max_runtime_hours <= 0:
            return False
        
        return True
    
    def conflicts_with(self, other_schedule: 'ScheduleConfig') -> bool:
        """Check if this schedule conflicts with another schedule."""
        # Check for schedule ID conflicts
        if self.schedule_id == other_schedule.schedule_id:
            return True
        
        # Check for time-based conflicts if both are time-based and recurring
        if (self.trigger_type == TriggerType.TIME_BASED and 
            other_schedule.trigger_type == TriggerType.TIME_BASED and
            self.schedule_type == ScheduleType.RECURRING and
            other_schedule.schedule_type == ScheduleType.RECURRING):
            
            # If both have cron expressions, check for time overlap
            if self.cron_expression and other_schedule.cron_expression:
                # Simple conflict detection: same cron expression
                if self.cron_expression == other_schedule.cron_expression:
                    return True
        
        # Check for resource threshold conflicts
        if (self.resource_thresholds and other_schedule.resource_thresholds):
            # If both schedules have very low resource thresholds, they might conflict
            if (self.resource_thresholds.max_cpu_percent < 30 and 
                other_schedule.resource_thresholds.max_cpu_percent < 30):
                return True
        
        # Check for ONE_TIME vs RECURRING conflicts at same time
        if (self.schedule_type == ScheduleType.ONE_TIME and 
            other_schedule.schedule_type == ScheduleType.RECURRING):
            # Could add more sophisticated time conflict detection here
            pass
        
        return False


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
        
        try:
            # Handle comma-separated values
            for part in field.split(','):
                if '/' in part:
                    # Handle step values (e.g., */5, 1-10/2)
                    range_part, step_part = part.split('/', 1)
                    
                    # Validate step value
                    try:
                        step = int(step_part)
                        if step <= 0:
                            return False
                    except ValueError:
                        return False
                    
                    # Validate range part
                    if range_part == '*':
                        # */step is valid
                        pass
                    elif '-' in range_part:
                        # range/step format
                        try:
                            start, end = map(int, range_part.split('-', 1))
                            if not (min_val <= start <= max_val and min_val <= end <= max_val and start <= end):
                                return False
                        except ValueError:
                            return False
                    else:
                        # single_value/step format
                        try:
                            value = int(range_part)
                            if not (min_val <= value <= max_val):
                                return False
                        except ValueError:
                            return False
                
                elif '-' in part:
                    # Handle ranges (e.g., 1-5)
                    try:
                        start, end = map(int, part.split('-', 1))
                        if not (min_val <= start <= max_val and min_val <= end <= max_val and start <= end):
                            return False
                    except ValueError:
                        return False
                
                else:
                    # Handle single values
                    try:
                        value = int(part)
                        if not (min_val <= value <= max_val):
                            return False
                    except ValueError:
                        return False
            
            return True
            
        except Exception:
            return False
    
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
    
    @classmethod
    def parse_expression(cls, expression: str) -> Dict[str, Any]:
        """Parse a cron expression into its components."""
        if not cls.validate_cron_expression(expression):
            raise ValueError(f"Invalid cron expression: {expression}")
        
        parts = expression.strip().split()
        if len(parts) != 5:
            raise ValueError(f"Cron expression must have 5 parts, got {len(parts)}")
        
        return {
            'minute': parts[0],
            'hour': parts[1], 
            'day_of_month': parts[2],
            'month': parts[3],
            'day_of_week': parts[4],
            'original': expression
        }
    
    @classmethod
    def _parse_cron_field(cls, field: str, min_val: int, max_val: int) -> set:
        """Parse a single cron field into a set of valid values."""
        values = set()
        
        if field == '*':
            return set(range(min_val, max_val + 1))
        
        # Handle comma-separated values
        for part in field.split(','):
            if '/' in part:
                # Handle step values (e.g., */5, 1-10/2)
                range_part, step = part.split('/', 1)
                step = int(step)
                
                if range_part == '*':
                    start, end = min_val, max_val
                elif '-' in range_part:
                    start, end = map(int, range_part.split('-', 1))
                else:
                    start = end = int(range_part)
                
                values.update(range(start, end + 1, step))
            
            elif '-' in part:
                # Handle ranges (e.g., 1-5)
                start, end = map(int, part.split('-', 1))
                values.update(range(start, end + 1))
            
            else:
                # Handle single values
                values.add(int(part))
        
        # Filter values to be within valid range
        return {v for v in values if min_val <= v <= max_val}
    
    @classmethod
    def get_next_execution(cls, expression: str, from_time: Optional[datetime] = None) -> Optional[datetime]:
        """Calculate the next execution time for a cron expression."""
        if not cls.validate_cron_expression(expression):
            return None
        
        if from_time is None:
            from_time = datetime.now()
        
        try:
            parts = expression.strip().split()
            if len(parts) != 5:
                return None
            
            minute_field, hour_field, day_field, month_field, dow_field = parts
            
            # Parse each field into sets of valid values
            minutes = cls._parse_cron_field(minute_field, 0, 59)
            hours = cls._parse_cron_field(hour_field, 0, 23)
            days = cls._parse_cron_field(day_field, 1, 31)
            months = cls._parse_cron_field(month_field, 1, 12)
            dows = cls._parse_cron_field(dow_field, 0, 7)
            
            # Convert Sunday from 7 to 0 for consistency
            if 7 in dows:
                dows.remove(7)
                dows.add(0)
            
            # Start from the next minute to avoid immediate execution
            candidate = from_time.replace(second=0, microsecond=0) + timedelta(minutes=1)
            
            # Search for the next valid execution time (max 4 years)
            max_iterations = 4 * 365 * 24 * 60
            iterations = 0
            
            while iterations < max_iterations:
                # Check if current candidate matches all cron fields
                if (candidate.minute in minutes and
                    candidate.hour in hours and
                    candidate.month in months):
                    
                    # Handle day of month vs day of week logic
                    # In cron, if both day and dow are specified (not *), 
                    # the condition is OR, not AND
                    day_match = candidate.day in days
                    dow_match = (candidate.weekday() + 1) % 7 in dows  # Convert to cron weekday
                    
                    if day_field == '*' and dow_field == '*':
                        # Both are wildcards - always match
                        return candidate
                    elif day_field == '*':
                        # Only check day of week
                        if dow_match:
                            return candidate
                    elif dow_field == '*':
                        # Only check day of month
                        if day_match:
                            return candidate
                    else:
                        # Both specified - OR condition
                        if day_match or dow_match:
                            return candidate
                
                # Move to next minute
                candidate += timedelta(minutes=1)
                iterations += 1
            
            return None  # No valid time found within reasonable range
            
        except (ValueError, IndexError) as e:
            logging.error(f"Error calculating next execution for cron expression '{expression}': {e}")
            return None


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
                return []
        except Exception:
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
        """Install the given crontab lines."""
        try:
            # Create temporary file with crontab content
            with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.crontab') as f:
                for line in crontab_lines:
                    f.write(line + '\n')
                temp_file = f.name
            
            try:
                # Install the crontab
                result = subprocess.run(["crontab", temp_file], capture_output=True, text=True)
                if result.returncode == 0:
                    logging.info("Successfully installed crontab")
                    return True
                else:
                    logging.error(f"Failed to install crontab: {result.stderr}")
                    return False
            finally:
                # Clean up temporary file
                try:
                    os.unlink(temp_file)
                except Exception as e:
                    logging.warning(f"Failed to remove temporary crontab file {temp_file}: {e}")
                    
        except Exception as e:
            logging.error(f"Failed to install crontab: {e}")
            return False


class NotificationManager:
    """Manages notifications for schedule events and failures."""
    
    def __init__(self, config: NotificationConfig):
        self.config = config
        self.logger = logging.getLogger(__name__)
    
    def send_notification(self, level: NotificationLevel, subject: str, message: str, 
                         schedule_id: str = "", execution_id: str = "") -> bool:
        """Send notification via configured channels."""
        success = True
        
        if self.config.email_enabled:
            success &= self._send_email(level, subject, message, schedule_id, execution_id)
        
        if self.config.webhook_enabled:
            success &= self._send_webhook(level, subject, message, schedule_id, execution_id)
        
        return success
    
    def _send_email(self, level: NotificationLevel, subject: str, message: str,
                   schedule_id: str, execution_id: str) -> bool:
        """Send email notification."""
        try:
            if not self.config.to_emails:
                return False
            
            msg = MIMEMultipart()
            msg['From'] = self.config.from_email
            msg['To'] = ', '.join(self.config.to_emails)
            msg['Subject'] = f"[{level.value.upper()}] {subject}"
            
            body = f"""
Schedule ID: {schedule_id}
Execution ID: {execution_id}
Level: {level.value}
Timestamp: {datetime.now().isoformat()}

{message}
"""
            
            msg.attach(MIMEText(body, 'plain'))
            
            with smtplib.SMTP(self.config.smtp_server, self.config.smtp_port) as server:
                if self.config.smtp_use_tls:
                    server.starttls()
                if self.config.smtp_username and self.config.smtp_password:
                    server.login(self.config.smtp_username, self.config.smtp_password)
                server.send_message(msg)
            
            self.logger.info(f"Email notification sent for {schedule_id}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to send email notification: {e}")
            return False
    
    def _send_webhook(self, level: NotificationLevel, subject: str, message: str,
                     schedule_id: str, execution_id: str) -> bool:
        """Send webhook notification."""
        try:
            import requests
            
            payload = {
                'level': level.value,
                'subject': subject,
                'message': message,
                'schedule_id': schedule_id,
                'execution_id': execution_id,
                'timestamp': datetime.now().isoformat()
            }
            
            response = requests.post(self.config.webhook_url, json=payload, timeout=10)
            response.raise_for_status()
            
            self.logger.info(f"Webhook notification sent for {schedule_id}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to send webhook notification: {e}")
            return False


class ScheduleHealthMonitor:
    """Monitors schedule health and detects issues."""
    
    def __init__(self, config_dir: Union[str, Path] = "./schedules"):
        self.config_dir = Path(config_dir)
        self.logger = logging.getLogger(__name__)
        self.schedule_manager = ScheduleManager(config_dir)
        self.monitor = ScheduleMonitor(config_dir)
        self.auto_suspend_threshold = 5  # Default threshold
    
    def check_schedule_health(self, schedule_id: str) -> Dict[str, Any]:
        """Check health of a specific schedule."""
        schedule = self.schedule_manager.get_schedule(schedule_id)
        if not schedule:
            return {'healthy': False, 'issues': ['Schedule not found']}
        
        issues = []
        warnings = []
        
        # Check if schedule is enabled but suspended
        if schedule.enabled and schedule.suspended:
            warnings.append(f"Schedule is enabled but suspended: {schedule.suspend_reason}")
        
        # Check failure rate
        stats = self.monitor.get_schedule_statistics(schedule_id)
        if stats.total_executions > 0:
            failure_rate = (stats.failed_executions / stats.total_executions) * 100
            if failure_rate > 50:
                issues.append(f"High failure rate: {failure_rate:.1f}%")
            elif failure_rate > 25:
                warnings.append(f"Elevated failure rate: {failure_rate:.1f}%")
        
        # Check for stuck executions
        running_executions = self.monitor.get_running_executions()
        for execution in running_executions:
            if execution.schedule_id == schedule_id:
                runtime = time.time() - execution.start_time
                max_runtime = schedule.max_runtime_hours * 3600
                if runtime > max_runtime:
                    issues.append(f"Execution {execution.execution_id} exceeded max runtime")
        
        # Check cron expression validity
        if schedule.schedule_type == ScheduleType.RECURRING:
            if not CronExpressionValidator.validate_cron_expression(schedule.cron_expression):
                issues.append("Invalid cron expression")
        
        return {
            'healthy': len(issues) == 0,
            'issues': issues,
            'warnings': warnings,
            'last_execution': schedule.last_execution_time,
            'success_rate': stats.success_rate if stats.total_executions > 0 else None
        }
    
    def get_system_health_report(self) -> Dict[str, Any]:
        """Get overall system health report."""
        schedules = self.schedule_manager.list_schedules()
        total_schedules = len(schedules)
        enabled_schedules = len([s for s in schedules if s.enabled])
        suspended_schedules = len([s for s in schedules if s.suspended])
        
        unhealthy_schedules = []
        warning_schedules = []
        
        for schedule in schedules:
            health = self.check_schedule_health(schedule.schedule_id)
            if not health['healthy']:
                unhealthy_schedules.append({
                    'schedule_id': schedule.schedule_id,
                    'issues': health['issues']
                })
            elif health['warnings']:
                warning_schedules.append({
                    'schedule_id': schedule.schedule_id,
                    'warnings': health['warnings']
                })
        
        running_executions = self.monitor.get_running_executions()
        
        return {
            'timestamp': time.time(),
            'total_schedules': total_schedules,
            'enabled_schedules': enabled_schedules,
            'suspended_schedules': suspended_schedules,
            'running_executions': len(running_executions),
            'unhealthy_schedules': unhealthy_schedules,
            'warning_schedules': warning_schedules,
            'system_healthy': len(unhealthy_schedules) == 0
        }
    
    def set_auto_suspend_threshold(self, threshold: int):
        """Set the auto-suspend threshold for consecutive failures."""
        self.auto_suspend_threshold = threshold
        self.logger.info(f"Auto-suspend threshold set to {threshold} consecutive failures")


class ErrorRecoveryManager:
    """Manages error recovery and retry logic for failed executions."""
    
    def __init__(self, config_dir: Union[str, Path] = "./schedules"):
        self.config_dir = Path(config_dir)
        self.logger = logging.getLogger(__name__)
        self.schedule_manager = ScheduleManager(config_dir)
        self.monitor = ScheduleMonitor(config_dir)
        self.notification_manager = None
    
    def set_notification_manager(self, notification_manager: NotificationManager):
        """Set notification manager for error notifications."""
        self.notification_manager = notification_manager
    
    def handle_execution_failure(self, execution: ScheduleExecution, 
                               failure_type: FailureType, error_message: str,
                               stack_trace: str = "") -> bool:
        """Handle execution failure with retry logic."""
        schedule = self.schedule_manager.get_schedule(execution.schedule_id)
        if not schedule:
            self.logger.error(f"Schedule {execution.schedule_id} not found for failed execution")
            return False
        
        # Create failure record
        failure_record = FailureRecord(
            failure_id=str(uuid.uuid4()),
            execution_id=execution.execution_id,
            schedule_id=execution.schedule_id,
            failure_type=failure_type,
            error_message=error_message,
            stack_trace=stack_trace,
            timestamp=time.time(),
            retry_attempt=execution.retry_attempt
        )
        
        # Add failure record to execution
        if execution.failure_records is None:
            execution.failure_records = []
        execution.failure_records.append(failure_record)
        
        # Update execution status
        execution.failure_type = failure_type
        execution.status = ExecutionStatus.FAILED
        
        # Determine if retry is possible
        retry_config = RetryConfig()  # Use default retry config
        should_retry = self._should_retry_execution(execution, failure_type, retry_config)
        
        if should_retry:
            return self._schedule_retry(execution, schedule, retry_config)
        else:
            return self._handle_final_failure(execution, schedule, failure_record)
    
    def _should_retry_execution(self, execution: ScheduleExecution, 
                              failure_type: FailureType, retry_config: RetryConfig) -> bool:
        """Determine if execution should be retried."""
        # Check if we've exceeded max retry attempts
        if execution.retry_attempt >= retry_config.max_attempts:
            return False
        
        # Check if failure type is retryable
        non_retryable_failures = {
            FailureType.PERMISSION_DENIED,
            FailureType.CONFIGURATION_ERROR,
            FailureType.USER_CANCELLED
        }
        
        if failure_type in non_retryable_failures:
            return False
        
        return True
    
    def _schedule_retry(self, execution: ScheduleExecution, schedule: ScheduleConfig,
                       retry_config: RetryConfig) -> bool:
        """Schedule execution retry."""
        try:
            # Calculate retry delay
            delay = retry_config.calculate_delay(execution.retry_attempt)
            next_retry_time = time.time() + delay
            
            # Update execution for retry
            execution.retry_attempt += 1
            execution.next_retry_time = next_retry_time
            execution.status = ExecutionStatus.RETRYING
            
            # Save updated execution
            self.monitor._save_execution(execution)
            
            # Schedule retry using threading
            retry_thread = threading.Thread(
                target=self._execute_retry,
                args=(execution, schedule, delay),
                daemon=True
            )
            retry_thread.start()
            
            self.logger.info(f"Scheduled retry for execution {execution.execution_id} in {delay} seconds")
            
            # Send notification
            if self.notification_manager:
                self.notification_manager.send_notification(
                    NotificationLevel.WARNING,
                    f"Execution Retry Scheduled",
                    f"Execution {execution.execution_id} will be retried in {delay} seconds (attempt {execution.retry_attempt})",
                    execution.schedule_id,
                    execution.execution_id
                )
            
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to schedule retry for execution {execution.execution_id}: {e}")
            return False
    
    def _execute_retry(self, execution: ScheduleExecution, schedule: ScheduleConfig, delay: int):
        """Execute retry after delay."""
        try:
            # Wait for retry delay
            time.sleep(delay)
            
            # Create new execution for retry
            new_execution = self.monitor.start_execution(
                schedule.schedule_id,
                pid=None  # Will be set when process starts
            )
            
            # Copy retry information
            new_execution.retry_attempt = execution.retry_attempt
            new_execution.max_retries = execution.max_retries
            
            # Execute the rebalancing operation
            # This would typically spawn a new process
            self.logger.info(f"Executing retry for schedule {schedule.schedule_id}")
            
            # Note: Actual process spawning would happen here
            # For now, we'll just log the retry attempt
            
        except Exception as e:
            self.logger.error(f"Failed to execute retry for {execution.execution_id}: {e}")
    
    def _handle_final_failure(self, execution: ScheduleExecution, schedule: ScheduleConfig,
                            failure_record: FailureRecord) -> bool:
        """Handle final failure when no more retries are possible."""
        try:
            # Update schedule statistics
            schedule.failure_count += 1
            schedule.last_execution_status = ExecutionStatus.FAILED
            
            # Check if schedule should be suspended due to repeated failures
            if self._should_suspend_schedule(schedule):
                schedule.suspended = True
                schedule.suspend_reason = f"Suspended due to repeated failures (last: {failure_record.failure_type.value})"
                self.logger.warning(f"Schedule {schedule.schedule_id} suspended due to repeated failures")
            
            # Save updated schedule
            self.schedule_manager.save_schedule(schedule)
            
            # Send failure notification
            if self.notification_manager:
                self.notification_manager.send_notification(
                    NotificationLevel.ERROR,
                    f"Schedule Execution Failed",
                    f"Schedule {schedule.name} failed after {execution.retry_attempt} retry attempts.\n\nError: {failure_record.error_message}",
                    execution.schedule_id,
                    execution.execution_id
                )
            
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to handle final failure for execution {execution.execution_id}: {e}")
            return False
    
    def _should_suspend_schedule(self, schedule: ScheduleConfig) -> bool:
        """Determine if schedule should be suspended due to failures."""
        # Suspend if last 3 executions failed
        if schedule.failure_count >= 3:
            recent_executions = self.monitor.get_execution_history(schedule.schedule_id, limit=3)
            if len(recent_executions) >= 3:
                all_failed = all(exec.status == ExecutionStatus.FAILED for exec in recent_executions)
                return all_failed
        
        return False
    
    def _classify_failure_type(self, error_message: str, exit_code: Optional[int] = None, 
                              stack_trace: str = "") -> FailureType:
        """Classify the type of failure based on error information."""
        if not error_message:
            return FailureType.UNKNOWN
        
        error_lower = error_message.lower()
        
        # Check for permission errors (including variations)
        if any(keyword in error_lower for keyword in ['permission denied', 'access denied', 'forbidden', 'unauthorized', 'operation not permitted', 'insufficient privileges', 'access is denied']):
            return FailureType.PERMISSION_DENIED
        
        # Check for disk/storage errors
        if any(keyword in error_lower for keyword in ['no space left', 'disk full', 'i/o error', 'read-only', 'disk error']):
            return FailureType.DISK_ERROR
        
        # Check for timeout first (more specific than network)
        if any(keyword in error_lower for keyword in ['timeout', 'timed out', 'deadline exceeded']):
            return FailureType.TIMEOUT
        
        # Check for network errors (including specific network error messages)
        if any(keyword in error_lower for keyword in ['network', 'connection refused', 'connection reset', 'unreachable', 'dns', 'host not found', 'no route to host']):
            return FailureType.NETWORK_ERROR
        
        # Check for resource exhaustion (including memory and resource errors)
        if any(keyword in error_lower for keyword in ['out of memory', 'memory error', 'memory allocation failed', 'resource temporarily unavailable']):
            return FailureType.RESOURCE_EXHAUSTION
        
        # Check for configuration errors
        if any(keyword in error_lower for keyword in ['config', 'configuration', 'invalid argument', 'bad option']):
            return FailureType.CONFIGURATION_ERROR
        
        # Check for user cancellation
        if any(keyword in error_lower for keyword in ['cancelled', 'canceled', 'interrupted', 'sigterm', 'sigint']):
            return FailureType.USER_CANCELLED
        
        # Check stack trace for additional clues
        if stack_trace:
            stack_lower = stack_trace.lower()
            if 'permissionerror' in stack_lower:
                return FailureType.PERMISSION_DENIED
            elif 'memoryerror' in stack_lower:
                return FailureType.RESOURCE_EXHAUSTION
            elif 'timeouterror' in stack_lower:
                return FailureType.TIMEOUT
            elif 'oserror' in stack_lower and 'no space left' in stack_lower:
                return FailureType.DISK_ERROR
        
        # Check exit codes if available (after message analysis)
        if exit_code is not None:
            if exit_code == 2:
                return FailureType.CONFIGURATION_ERROR
            elif exit_code == 126:
                return FailureType.PERMISSION_DENIED
            elif exit_code == 127:
                return FailureType.CONFIGURATION_ERROR
            elif exit_code == 130:  # SIGINT
                return FailureType.USER_CANCELLED
            elif exit_code == 143:  # SIGTERM
                return FailureType.USER_CANCELLED
            elif exit_code == 1:
                 # Exit code 1 is generic, check for specific unknown error patterns
                 generic_messages = ['unknown error', 'error', '', 'unexpected error occurred', 
                                   'internal server error', 'something went wrong', 'error code 500']
                 if not error_message or error_message.lower().strip() in generic_messages:
                     return FailureType.UNKNOWN
                 # For exit code 1 with specific error messages, return SYSTEM_ERROR
                 return FailureType.SYSTEM_ERROR
        
        # Default to unknown if we can't classify it
        return FailureType.UNKNOWN
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

    def install_schedule(self, schedule: ScheduleConfig) -> bool:
        """Install a schedule (alias for create_and_install_schedule)."""
        return self.create_and_install_schedule(schedule)
    
    def update_schedule(self, schedule: ScheduleConfig) -> bool:
        """Update a schedule (alias for update_and_reinstall_schedule)."""
        return self.update_and_reinstall_schedule(schedule.schedule_id, schedule)
    
    def list_installed_schedules(self) -> List[ScheduleConfig]:
        """List all schedules that have installed cron jobs."""
        installed_ids = self.cron_manager.list_installed_schedules()
        schedules = []
        for schedule_id in installed_ids:
            schedule = self.schedule_manager.get_schedule(schedule_id)
            if schedule:
                schedules.append(schedule)
            else:
                # Create a minimal schedule config for cron entries without saved schedules
                mock_schedule = ScheduleConfig(
                    schedule_id=schedule_id,
                    name=schedule_id,  # Use schedule_id as name for test compatibility
                    description=f"Installed cron job: {schedule_id}"
                )
                schedules.append(mock_schedule)
        return schedules
    
    def backup_crontab(self, backup_file: Path) -> bool:
        """Backup current crontab to a file."""
        try:
            current_crontab = self.cron_manager._get_current_crontab()
            with open(backup_file, 'w') as f:
                for line in current_crontab:
                    f.write(line + '\n')
            return True
        except Exception as e:
            logging.error(f"Failed to backup crontab: {e}")
            return False
    
    def restore_crontab(self, backup_file: Path) -> bool:
        """Restore crontab from a backup file."""
        try:
            if not backup_file.exists():
                logging.error(f"Backup file does not exist: {backup_file}")
                return False
            
            with open(backup_file, 'r') as f:
                crontab_lines = [line.rstrip('\n') for line in f.readlines()]
            
            return self.cron_manager._install_crontab(crontab_lines)
        except Exception as e:
            logging.error(f"Failed to restore crontab: {e}")
            return False

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
    
    # Cron Expression Helper Methods
    def create_daily_cron(self, hour: int, minute: int = 0) -> str:
        """Create a daily cron expression.
        
        Args:
            hour: Hour of day (0-23)
            minute: Minute of hour (0-59)
            
        Returns:
            Cron expression string for daily execution
        """
        if not (0 <= hour <= 23):
            raise ValueError(f"Hour must be between 0 and 23, got {hour}")
        if not (0 <= minute <= 59):
            raise ValueError(f"Minute must be between 0 and 59, got {minute}")
        return CronExpressionValidator.create_daily_expression(hour, minute)
    
    def create_weekly_cron(self, day_of_week: int, hour: int, minute: int = 0) -> str:
        """Create a weekly cron expression.
        
        Args:
            day_of_week: Day of week (0=Sunday, 1=Monday, ..., 6=Saturday)
            hour: Hour of day (0-23)
            minute: Minute of hour (0-59)
            
        Returns:
            Cron expression string for weekly execution
        """
        if not (0 <= day_of_week <= 6):
            raise ValueError(f"Day of week must be between 0 and 6, got {day_of_week}")
        if not (0 <= hour <= 23):
            raise ValueError(f"Hour must be between 0 and 23, got {hour}")
        if not (0 <= minute <= 59):
            raise ValueError(f"Minute must be between 0 and 59, got {minute}")
        return CronExpressionValidator.create_weekly_expression(day_of_week, hour, minute)
    
    def create_monthly_cron(self, day: int, hour: int, minute: int = 0) -> str:
        """Create a monthly cron expression.
        
        Args:
            day: Day of month (1-31)
            hour: Hour of day (0-23)
            minute: Minute of hour (0-59)
            
        Returns:
            Cron expression string for monthly execution
        """
        if not (1 <= day <= 31):
            raise ValueError(f"Day must be between 1 and 31, got {day}")
        if not (0 <= hour <= 23):
            raise ValueError(f"Hour must be between 0 and 23, got {hour}")
        if not (0 <= minute <= 59):
            raise ValueError(f"Minute must be between 0 and 59, got {minute}")
        return CronExpressionValidator.create_monthly_expression(day, hour, minute)
    
    def create_interval_cron(self, minutes: int = None, hours: int = None, days: int = None) -> str:
        """Create an interval-based cron expression.
        
        Args:
            minutes: Interval in minutes (mutually exclusive with hours/days)
            hours: Interval in hours (mutually exclusive with minutes/days)
            days: Interval in days (mutually exclusive with minutes/hours)
            
        Returns:
            Cron expression string for interval execution
        """
        # Validate that only one interval type is specified
        interval_count = sum(x is not None for x in [minutes, hours, days])
        if interval_count != 1:
            raise ValueError("Exactly one of minutes, hours, or days must be specified")
        
        if minutes is not None:
            if not (1 <= minutes <= 59):
                raise ValueError(f"Minutes interval must be between 1 and 59, got {minutes}")
            return f"*/{minutes} * * * *"
        elif hours is not None:
            if not (1 <= hours <= 23):
                raise ValueError(f"Hours interval must be between 1 and 23, got {hours}")
            return f"0 */{hours} * * *"
        elif days is not None:
            if not (1 <= days <= 31):
                raise ValueError(f"Days interval must be between 1 and 31, got {days}")
            return f"0 0 */{days} * *"
    
    def generate_cron_line(self, schedule: ScheduleConfig) -> str:
        """Generate a complete cron line for a schedule.
        
        Args:
            schedule: Schedule configuration
            
        Returns:
            Complete cron line with comment and command
        """
        if not schedule.cron_expression:
            raise ValueError(f"Schedule {schedule.schedule_id} has no cron expression")
        
        # Generate the command
        command = self.cron_manager._generate_cron_command(schedule)
        
        # Create the cron line with comment
        comment = f"# Unraid Rebalancer Schedule: {schedule.name}"
        cron_line = f"{schedule.cron_expression} {command}"
        
        return f"{comment}\n{cron_line}"
    
    def parse_cron_line(self, cron_line: str) -> Optional[dict]:
        """Parse a cron line into its components.
        
        Args:
            cron_line: Complete cron line string
            
        Returns:
            Dictionary with cron_expression, command, and valid fields, or None if invalid
        """
        try:
            lines = cron_line.strip().split('\n')
            
            # Handle single line (no comment)
            if len(lines) == 1:
                line = lines[0].strip()
                if line.startswith('#'):
                    return None
                
                # Split into cron expression and command
                parts = line.split(None, 5)  # Split on first 5 whitespace groups
                if len(parts) < 6:
                    return None
                
                cron_expr = ' '.join(parts[:5])
                command = parts[5]
                
                # Only return valid cron expressions
                if not CronExpressionValidator.validate_cron_expression(cron_expr):
                    return None
                
                return {
                    'cron_expression': cron_expr,
                    'command': command,
                    'valid': True
                }
            
            # Handle multi-line (with comment)
            elif len(lines) == 2:
                comment_line = lines[0].strip()
                cron_line = lines[1].strip()
                
                if not comment_line.startswith('#'):
                    return None
                
                # Parse the actual cron line
                parts = cron_line.split(None, 5)
                if len(parts) < 6:
                    return None
                
                cron_expr = ' '.join(parts[:5])
                command = parts[5]
                
                # Only return valid cron expressions
                if not CronExpressionValidator.validate_cron_expression(cron_expr):
                    return None
                
                return {
                    'cron_expression': cron_expr,
                    'command': command,
                    'valid': True
                }
            
            else:
                return None
                
        except Exception:
            return None


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