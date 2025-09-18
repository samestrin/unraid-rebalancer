# Scheduling System Technical Documentation

This document provides technical details about the implementation of the Unraid Rebalancer's scheduling system, including architecture, algorithms, and security considerations.

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Cron Integration Implementation](#cron-integration-implementation)
3. [Resource Monitoring Algorithms](#resource-monitoring-algorithms)
4. [Schedule Management System](#schedule-management-system)
5. [Error Handling and Recovery](#error-handling-and-recovery)
6. [Security Considerations](#security-considerations)
7. [Performance Optimization](#performance-optimization)
8. [Troubleshooting Guide](#troubleshooting-guide)
9. [API Reference](#api-reference)

## Architecture Overview

### System Components

The scheduling system consists of several interconnected components using a hybrid architecture:

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│  CLI Interface  │────│ Scheduling      │────│ Threading       │
│                 │    │ Engine          │    │ Execution       │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         │                       │                       │
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│ Cron Manager    │    │ Resource        │    │ Error Recovery  │
│ (System Cron)   │    │ Monitor         │    │ Manager         │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         │                       │                       │
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│ Schedule        │    │ Health Monitor  │    │ Notification    │
│ Monitor         │    │                 │    │ Manager         │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

### Hybrid Architecture

The system uses a **hybrid scheduling approach**:
- **Cron Integration**: System crontab for time-based schedule triggers
- **Threading Layer**: Python threading for retries, concurrent execution, and background monitoring
- **Resource Awareness**: Conditional scheduling based on system resource thresholds

### Core Classes

#### SchedulingEngine
- **Purpose**: Central orchestrator for all scheduling operations
- **Responsibilities**: Schedule CRUD operations, cron integration, execution coordination
- **Key Methods**: `create_and_install_schedule()`, `update_and_reinstall_schedule()`, `sync_schedules()`

#### ScheduleConfig
- **Purpose**: Comprehensive configuration dataclass for schedules
- **Responsibilities**: Schedule definition, validation, rebalancer parameters
- **Key Properties**: `cron_expression`, `schedule_type`, `trigger_type`, `target_percent`, `rsync_mode`, `resource_thresholds`
- **Schedule Types**: `ONE_TIME`, `RECURRING`, `CONDITIONAL`
- **Trigger Types**: `TIME_BASED`, `RESOURCE_BASED`, `DISK_USAGE`, `SYSTEM_IDLE`

#### SystemResourceMonitor
- **Purpose**: System resource monitoring and threshold management
- **Responsibilities**: CPU/memory/IO monitoring, idle time detection
- **Key Methods**: `get_current_usage()`, `check_resource_thresholds()`, `get_idle_time_minutes()`

#### ErrorRecoveryManager
- **Purpose**: Handle execution failures and retry logic using threading
- **Responsibilities**: Failure classification, retry scheduling, schedule suspension
- **Key Methods**: `handle_execution_failure()`, `_schedule_retry()`, `_execute_retry()`
- **Threading**: Uses daemon threads for retry execution with configurable backoff strategies

#### ScheduleMonitor
- **Purpose**: Track schedule executions and maintain execution history
- **Responsibilities**: Execution lifecycle management, statistics tracking
- **Key Methods**: `start_execution()`, `complete_execution()`, `get_running_executions()`

## Threading Implementation

### Retry Execution Threading

The system uses Python threading for retry execution and background operations:

```python
class ErrorRecoveryManager:
    """Handle execution failures with threading-based retries."""
    
    def _schedule_retry(self, execution: ScheduleExecution, schedule: ScheduleConfig,
                       retry_config: RetryConfig) -> bool:
        """Schedule execution retry using daemon thread."""
        try:
            # Calculate retry delay
            delay = retry_config.calculate_delay(execution.retry_attempt)
            next_retry_time = time.time() + delay
            
            # Update execution for retry
            execution.retry_attempt += 1
            execution.next_retry_time = next_retry_time
            execution.status = ExecutionStatus.RETRYING
            
            # Schedule retry using threading
            retry_thread = threading.Thread(
                target=self._execute_retry,
                args=(execution, schedule, delay),
                daemon=True  # Daemon thread for automatic cleanup
            )
            retry_thread.start()
            
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to schedule retry: {e}")
            return False
    
    def _execute_retry(self, execution: ScheduleExecution, schedule: ScheduleConfig, delay: int):
        """Execute retry after delay in separate thread."""
        try:
            # Wait for retry delay
            time.sleep(delay)
            
            # Create new execution for retry
            new_execution = self.monitor.start_execution(
                schedule.schedule_id,
                pid=None
            )
            
            # Copy retry information
            new_execution.retry_attempt = execution.retry_attempt
            new_execution.max_retries = execution.max_retries
            
            # Execute the rebalancing operation
            self.logger.info(f"Executing retry for schedule {schedule.schedule_id}")
            
        except Exception as e:
            self.logger.error(f"Failed to execute retry: {e}")
```

### Background System Monitoring

The main rebalancer also uses threading for background system monitoring:

```python
class UnraidRebalancer:
    """Main rebalancer with background monitoring."""
    
    def start_monitoring(self):
        """Start background system monitoring."""
        if not self.metrics_enabled or self._monitoring_thread:
            return
        
        self._stop_monitoring.clear()
        self._monitoring_thread = threading.Thread(
            target=self._monitor_system, 
            daemon=True
        )
        self._monitoring_thread.start()
```

### Thread Safety Considerations

- **Daemon Threads**: All background threads are marked as daemon threads for automatic cleanup
- **Thread Isolation**: Each retry execution runs in its own thread to prevent blocking
- **Resource Management**: Threads are designed to be short-lived and self-cleaning
- **Concurrent Execution**: Multiple schedules can execute concurrently through threading

## Cron Integration Implementation

### Cron Expression Parsing

The system uses a custom cron parser that supports standard 5-field expressions:

```python
class CronParser:
    """Parse and validate cron expressions."""
    
    FIELD_RANGES = {
        'minute': (0, 59),
        'hour': (0, 23),
        'day': (1, 31),
        'month': (1, 12),
        'weekday': (0, 6)
    }
    
    def parse(self, expression: str) -> CronSchedule:
        """Parse cron expression into schedule object."""
        fields = expression.strip().split()
        if len(fields) != 5:
            raise ValueError("Cron expression must have 5 fields")
        
        return CronSchedule(
            minute=self._parse_field(fields[0], 'minute'),
            hour=self._parse_field(fields[1], 'hour'),
            day=self._parse_field(fields[2], 'day'),
            month=self._parse_field(fields[3], 'month'),
            weekday=self._parse_field(fields[4], 'weekday')
        )
    
    def _parse_field(self, field: str, field_type: str) -> Set[int]:
        """Parse individual cron field."""
        min_val, max_val = self.FIELD_RANGES[field_type]
        
        if field == '*':
            return set(range(min_val, max_val + 1))
        
        if '/' in field:
            return self._parse_step_values(field, min_val, max_val)
        
        if ',' in field:
            return self._parse_list_values(field, min_val, max_val)
        
        if '-' in field:
            return self._parse_range_values(field, min_val, max_val)
        
        # Single value
        value = int(field)
        if not (min_val <= value <= max_val):
            raise ValueError(f"Value {value} out of range for {field_type}")
        
        return {value}
```

### Next Execution Calculation

The system calculates next execution times using an efficient algorithm:

```python
class CronSchedule:
    """Represents a parsed cron schedule."""
    
    def next_execution(self, after: datetime = None) -> datetime:
        """Calculate next execution time after given datetime."""
        if after is None:
            after = datetime.now()
        
        # Start from next minute
        candidate = after.replace(second=0, microsecond=0) + timedelta(minutes=1)
        
        # Find next valid execution time
        for _ in range(366 * 24 * 60):  # Max iterations (1 year)
            if self._matches_schedule(candidate):
                return candidate
            candidate += timedelta(minutes=1)
        
        raise ValueError("No valid execution time found within 1 year")
    
    def _matches_schedule(self, dt: datetime) -> bool:
        """Check if datetime matches cron schedule."""
        return (
            dt.minute in self.minute and
            dt.hour in self.hour and
            dt.day in self.day and
            dt.month in self.month and
            dt.weekday() in self.weekday
        )
```

### Crontab Integration

For system-level cron integration:

```python
class CronManager:
    """Manage system crontab entries."""
    
    def __init__(self, script_path: Union[str, Path]):
        """Initialize with path to rebalancer script."""
        self.script_path = Path(script_path).resolve()
        self.logger = logging.getLogger(__name__)
    
    def install_schedule(self, schedule: ScheduleConfig) -> bool:
        """Install schedule in system crontab."""
        try:
            # Validate cron expression
            if not CronExpressionValidator.validate_cron_expression(schedule.cron_expression):
                self.logger.error(f"Invalid cron expression: {schedule.cron_expression}")
                return False
            
            # Get current crontab
            current_crontab = self._get_current_crontab()
            
            # Remove existing entry for this schedule
            filtered_crontab = [line for line in current_crontab 
                              if not line.strip().endswith(f"# {schedule.schedule_id}")]
            
            # Add new entry
            new_entry = self._generate_cron_command(schedule)
            updated_crontab = filtered_crontab + [new_entry]
            
            # Install updated crontab
            return self._install_crontab(updated_crontab)
            
        except Exception as e:
            self.logger.error(f"Failed to install schedule {schedule.schedule_id}: {e}")
            return False
    
    def _generate_cron_command(self, schedule: ScheduleConfig) -> str:
        """Generate cron command for schedule."""
        # Build command arguments
        cmd_args = [
            'python3',
            str(self.script_path),
            '--execute-schedule-id',
            schedule.schedule_id
        ]
        
        # Add rebalancer-specific arguments
        if schedule.target_percent != 80.0:
            cmd_args.extend(['--target-percent', str(schedule.target_percent)])
        
        if schedule.rsync_mode != 'balanced':
            cmd_args.extend(['--rsync-mode', schedule.rsync_mode])
        
        if schedule.min_unit_size != 1073741824:
            cmd_args.extend(['--min-unit-size', str(schedule.min_unit_size)])
        
        if schedule.include_disks:
            cmd_args.extend(['--include-disks'] + schedule.include_disks)
        
        if schedule.exclude_disks:
            cmd_args.extend(['--exclude-disks'] + schedule.exclude_disks)
        
        # Add logging redirection
        log_dir = Path('/var/log/unraid-rebalancer')
        log_file = log_dir / f'{schedule.schedule_id}.log'
        
        command = ' '.join(cmd_args)
        command += f' >> {log_file} 2>&1'
        
        return f"{schedule.cron_expression} {command} # {schedule.schedule_id}"
    
    def _install_crontab(self, entries: List[str]) -> bool:
        """Install crontab entries."""
        try:
            crontab_content = '\n'.join(entries) + '\n'
            
            # Use subprocess to install crontab
            process = subprocess.Popen(
                ['crontab', '-'],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            
            stdout, stderr = process.communicate(input=crontab_content)
            
            if process.returncode != 0:
                logger.error(f"Crontab installation failed: {stderr}")
                return False
            
            return True
            
        except Exception as e:
            logger.error(f"Exception during crontab installation: {e}")
            return False
```

## Resource Monitoring Algorithms

### System Resource Collection

The resource monitor collects metrics using multiple methods:

```python
class SystemResourceMonitor:
    """Monitor system resource usage."""
    
    def __init__(self):
        self.cpu_samples = deque(maxlen=60)  # 1 minute of samples
        self.memory_samples = deque(maxlen=60)
        self.io_samples = deque(maxlen=60)
        self.collection_interval = 1.0  # seconds
    
    def get_cpu_usage(self) -> float:
        """Get current CPU usage percentage."""
        try:
            # Use psutil for cross-platform compatibility
            import psutil
            return psutil.cpu_percent(interval=0.1)
        except ImportError:
            # Fallback to /proc/stat on Linux
            return self._get_cpu_usage_proc()
    
    def _get_cpu_usage_proc(self) -> float:
        """Get CPU usage from /proc/stat."""
        try:
            with open('/proc/stat', 'r') as f:
                line = f.readline()
            
            # Parse CPU line: cpu user nice system idle iowait irq softirq
            fields = line.split()[1:8]
            idle = int(fields[3]) + int(fields[4])  # idle + iowait
            total = sum(int(field) for field in fields)
            
            if hasattr(self, '_prev_idle') and hasattr(self, '_prev_total'):
                idle_delta = idle - self._prev_idle
                total_delta = total - self._prev_total
                
                if total_delta > 0:
                    usage = 100.0 * (1.0 - idle_delta / total_delta)
                else:
                    usage = 0.0
            else:
                usage = 0.0
            
            self._prev_idle = idle
            self._prev_total = total
            
            return max(0.0, min(100.0, usage))
            
        except Exception as e:
            logger.warning(f"Failed to get CPU usage: {e}")
            return 0.0
    
    def get_memory_usage(self) -> float:
        """Get current memory usage percentage."""
        try:
            import psutil
            memory = psutil.virtual_memory()
            return memory.percent
        except ImportError:
            return self._get_memory_usage_proc()
    
    def _get_memory_usage_proc(self) -> float:
        """Get memory usage from /proc/meminfo."""
        try:
            with open('/proc/meminfo', 'r') as f:
                meminfo = f.read()
            
            # Parse memory information
            lines = meminfo.strip().split('\n')
            memory_data = {}
            
            for line in lines:
                if ':' in line:
                    key, value = line.split(':', 1)
                    # Extract numeric value (remove 'kB' suffix)
                    value_kb = int(value.strip().split()[0])
                    memory_data[key.strip()] = value_kb
            
            total = memory_data.get('MemTotal', 0)
            available = memory_data.get('MemAvailable', 0)
            
            if total > 0:
                used = total - available
                return 100.0 * used / total
            
            return 0.0
            
        except Exception as e:
            logger.warning(f"Failed to get memory usage: {e}")
            return 0.0
    
    def get_io_usage(self) -> float:
        """Get current disk I/O usage percentage."""
        try:
            import psutil
            
            # Get I/O statistics
            io_counters = psutil.disk_io_counters()
            if io_counters is None:
                return 0.0
            
            current_time = time.time()
            current_io = io_counters.read_bytes + io_counters.write_bytes
            
            if hasattr(self, '_prev_io_time') and hasattr(self, '_prev_io_bytes'):
                time_delta = current_time - self._prev_io_time
                io_delta = current_io - self._prev_io_bytes
                
                if time_delta > 0:
                    # Calculate I/O rate in MB/s
                    io_rate = (io_delta / time_delta) / (1024 * 1024)
                    
                    # Convert to percentage based on estimated disk bandwidth
                    # Assume 100 MB/s as baseline for 100% usage
                    io_percentage = min(100.0, (io_rate / 100.0) * 100.0)
                else:
                    io_percentage = 0.0
            else:
                io_percentage = 0.0
            
            self._prev_io_time = current_time
            self._prev_io_bytes = current_io
            
            return io_percentage
            
        except Exception as e:
            logger.warning(f"Failed to get I/O usage: {e}")
            return 0.0
```

### Adaptive Scheduling Algorithm

The adaptive scheduling algorithm adjusts execution timing based on resource availability:

```python
class AdaptiveScheduler:
    """Implement adaptive scheduling based on resource usage."""
    
    def __init__(self, resource_monitor: SystemResourceMonitor):
        self.resource_monitor = resource_monitor
        self.backoff_multiplier = 1.5
        self.max_delay = 3600  # 1 hour maximum delay
        self.min_delay = 60    # 1 minute minimum delay
    
    def should_execute_now(self, schedule: ScheduleConfig) -> Tuple[bool, Optional[int]]:
        """Determine if schedule should execute now or be delayed.
        
        Returns:
            Tuple of (should_execute, delay_seconds)
        """
        if not schedule.adaptive_scheduling:
            return True, None
        
        # Get current resource usage
        cpu_usage = self.resource_monitor.get_cpu_usage()
        memory_usage = self.resource_monitor.get_memory_usage()
        io_usage = self.resource_monitor.get_io_usage()
        
        # Check against thresholds
        thresholds_exceeded = []
        
        if cpu_usage > schedule.cpu_threshold:
            thresholds_exceeded.append(('CPU', cpu_usage, schedule.cpu_threshold))
        
        if memory_usage > schedule.memory_threshold:
            thresholds_exceeded.append(('Memory', memory_usage, schedule.memory_threshold))
        
        if io_usage > schedule.io_threshold:
            thresholds_exceeded.append(('I/O', io_usage, schedule.io_threshold))
        
        if not thresholds_exceeded:
            return True, None
        
        # Calculate delay based on resource pressure
        delay = self._calculate_adaptive_delay(
            thresholds_exceeded, 
            schedule.get_consecutive_delays()
        )
        
        logger.info(
            f"Delaying schedule {schedule.name} for {delay}s due to resource pressure: "
            f"{', '.join(f'{name}={usage:.1f}%>{threshold:.1f}%' for name, usage, threshold in thresholds_exceeded)}"
        )
        
        return False, delay
    
    def _calculate_adaptive_delay(self, exceeded_thresholds: List[Tuple[str, float, float]], 
                                consecutive_delays: int) -> int:
        """Calculate delay based on resource pressure and delay history."""
        # Base delay calculation
        max_pressure = max(
            (usage - threshold) / threshold 
            for _, usage, threshold in exceeded_thresholds
        )
        
        # Base delay proportional to pressure
        base_delay = self.min_delay * (1 + max_pressure)
        
        # Apply exponential backoff for consecutive delays
        backoff_delay = base_delay * (self.backoff_multiplier ** consecutive_delays)
        
        # Cap at maximum delay
        final_delay = min(backoff_delay, self.max_delay)
        
        return int(final_delay)
    
    def predict_next_execution(self, schedule: ScheduleConfig) -> datetime:
        """Predict when schedule will actually execute considering resource constraints."""
        base_next = schedule.next_execution_time()
        
        if not schedule.adaptive_scheduling:
            return base_next
        
        # Use historical resource patterns to predict execution time
        historical_delays = self._get_historical_delays(schedule)
        
        if historical_delays:
            avg_delay = sum(historical_delays) / len(historical_delays)
            predicted_delay = timedelta(seconds=avg_delay)
        else:
            predicted_delay = timedelta(0)
        
        return base_next + predicted_delay
```

### Resource Prediction

The system includes basic resource usage prediction:

```python
class ResourcePredictor:
    """Predict future resource usage patterns."""
    
    def __init__(self, monitor: SystemResourceMonitor):
        self.monitor = monitor
        self.history_window = 3600  # 1 hour of history
        self.prediction_window = 1800  # 30 minutes into future
    
    def predict_resource_usage(self, target_time: datetime) -> Dict[str, float]:
        """Predict resource usage at target time."""
        current_time = datetime.now()
        time_delta = (target_time - current_time).total_seconds()
        
        if time_delta <= 0:
            # Return current usage for past/present times
            return {
                'cpu': self.monitor.get_cpu_usage(),
                'memory': self.monitor.get_memory_usage(),
                'io': self.monitor.get_io_usage()
            }
        
        # Simple prediction based on recent trends
        cpu_trend = self._calculate_trend(self.monitor.cpu_samples)
        memory_trend = self._calculate_trend(self.monitor.memory_samples)
        io_trend = self._calculate_trend(self.monitor.io_samples)
        
        # Project trends forward
        cpu_prediction = self._project_trend(
            self.monitor.get_cpu_usage(), cpu_trend, time_delta
        )
        memory_prediction = self._project_trend(
            self.monitor.get_memory_usage(), memory_trend, time_delta
        )
        io_prediction = self._project_trend(
            self.monitor.get_io_usage(), io_trend, time_delta
        )
        
        return {
            'cpu': max(0, min(100, cpu_prediction)),
            'memory': max(0, min(100, memory_prediction)),
            'io': max(0, min(100, io_prediction))
        }
    
    def _calculate_trend(self, samples: deque) -> float:
        """Calculate trend from recent samples using linear regression."""
        if len(samples) < 2:
            return 0.0
        
        # Simple linear regression
        n = len(samples)
        x_values = list(range(n))
        y_values = list(samples)
        
        x_mean = sum(x_values) / n
        y_mean = sum(y_values) / n
        
        numerator = sum((x - x_mean) * (y - y_mean) for x, y in zip(x_values, y_values))
        denominator = sum((x - x_mean) ** 2 for x in x_values)
        
        if denominator == 0:
            return 0.0
        
        slope = numerator / denominator
        return slope
    
    def _project_trend(self, current_value: float, trend: float, time_delta: float) -> float:
        """Project trend forward in time."""
        # Convert time delta to sample periods (assuming 1 second samples)
        periods = time_delta
        
        # Project trend forward
        projected_value = current_value + (trend * periods)
        
        return projected_value
```

## Schedule Management System

### Schedule Persistence

Schedules are persisted using JSON with atomic writes:

```python
class SchedulePersistence:
    """Handle schedule persistence and recovery."""
    
    def __init__(self, config_file: str):
        self.config_file = Path(config_file)
        self.backup_file = self.config_file.with_suffix('.bak')
        self.lock_file = self.config_file.with_suffix('.lock')
    
    def save_schedules(self, schedules: Dict[str, ScheduleConfig]) -> bool:
        """Save schedules to persistent storage with atomic write."""
        try:
            # Acquire file lock
            with self._acquire_lock():
                # Prepare data for serialization
                schedule_data = {
                    'version': '1.0',
                    'timestamp': time.time(),
                    'schedules': {
                        name: schedule.to_dict()
                        for name, schedule in schedules.items()
                    }
                }
                
                # Write to temporary file first
                temp_file = self.config_file.with_suffix('.tmp')
                
                with open(temp_file, 'w') as f:
                    json.dump(schedule_data, f, indent=2, default=str)
                
                # Create backup of current file
                if self.config_file.exists():
                    shutil.copy2(self.config_file, self.backup_file)
                
                # Atomic move
                temp_file.replace(self.config_file)
                
                logger.info(f"Saved {len(schedules)} schedules to {self.config_file}")
                return True
                
        except Exception as e:
            logger.error(f"Failed to save schedules: {e}")
            return False
    
    def load_schedules(self) -> Dict[str, ScheduleConfig]:
        """Load schedules from persistent storage."""
        try:
            if not self.config_file.exists():
                logger.info("No existing schedule configuration found")
                return {}
            
            with open(self.config_file, 'r') as f:
                data = json.load(f)
            
            # Validate data format
            if 'schedules' not in data:
                raise ValueError("Invalid configuration format")
            
            # Load schedules
            schedules = {}
            for name, schedule_data in data['schedules'].items():
                try:
                    schedule = ScheduleConfig.from_dict(schedule_data)
                    schedules[name] = schedule
                except Exception as e:
                    logger.error(f"Failed to load schedule {name}: {e}")
            
            logger.info(f"Loaded {len(schedules)} schedules from {self.config_file}")
            return schedules
            
        except Exception as e:
            logger.error(f"Failed to load schedules: {e}")
            
            # Try to recover from backup
            if self.backup_file.exists():
                logger.info("Attempting to recover from backup")
                try:
                    shutil.copy2(self.backup_file, self.config_file)
                    return self.load_schedules()
                except Exception as backup_error:
                    logger.error(f"Backup recovery failed: {backup_error}")
            
            return {}
    
    @contextmanager
    def _acquire_lock(self):
        """Acquire file lock for atomic operations."""
        lock_acquired = False
        try:
            # Simple file-based locking
            for attempt in range(10):
                try:
                    with open(self.lock_file, 'x') as f:
                        f.write(str(os.getpid()))
                    lock_acquired = True
                    break
                except FileExistsError:
                    time.sleep(0.1)
            
            if not lock_acquired:
                raise RuntimeError("Could not acquire file lock")
            
            yield
            
        finally:
            if lock_acquired and self.lock_file.exists():
                self.lock_file.unlink()
```

### Schedule Validation

Comprehensive validation ensures schedule integrity:

```python
class ScheduleValidator:
    """Validate schedule configurations."""
    
    def validate_schedule(self, schedule: ScheduleConfig) -> List[str]:
        """Validate schedule configuration and return list of errors."""
        errors = []
        
        # Validate name
        errors.extend(self._validate_name(schedule.name))
        
        # Validate cron expression
        errors.extend(self._validate_cron_expression(schedule.cron_expression))
        
        # Validate command
        errors.extend(self._validate_command(schedule.command))
        
        # Validate resource thresholds
        errors.extend(self._validate_resource_thresholds(schedule))
        
        # Validate timeout
        errors.extend(self._validate_timeout(schedule.timeout_seconds))
        
        # Validate notification configuration
        if schedule.notification_config:
            errors.extend(self._validate_notification_config(schedule.notification_config))
        
        return errors
    
    def _validate_name(self, name: str) -> List[str]:
        """Validate schedule name."""
        errors = []
        
        if not name:
            errors.append("Schedule name cannot be empty")
        elif len(name) > 64:
            errors.append("Schedule name cannot exceed 64 characters")
        elif not re.match(r'^[a-zA-Z0-9_-]+$', name):
            errors.append("Schedule name can only contain letters, numbers, underscores, and hyphens")
        
        return errors
    
    def _validate_cron_expression(self, expression: str) -> List[str]:
        """Validate cron expression."""
        errors = []
        
        try:
            parser = CronParser()
            parser.parse(expression)
        except ValueError as e:
            errors.append(f"Invalid cron expression: {e}")
        
        return errors
    
    def _validate_command(self, command: List[str]) -> List[str]:
        """Validate command configuration."""
        errors = []
        
        if not command:
            errors.append("Command cannot be empty")
        elif not isinstance(command, list):
            errors.append("Command must be a list of strings")
        else:
            # Check if executable exists
            executable = command[0]
            if not shutil.which(executable) and not os.path.isfile(executable):
                errors.append(f"Executable not found: {executable}")
        
        return errors
    
    def _validate_resource_thresholds(self, schedule: ScheduleConfig) -> List[str]:
        """Validate resource threshold values."""
        errors = []
        
        thresholds = [
            ('CPU threshold', schedule.cpu_threshold),
            ('Memory threshold', schedule.memory_threshold),
            ('I/O threshold', schedule.io_threshold)
        ]
        
        for name, value in thresholds:
            if not (0 <= value <= 100):
                errors.append(f"{name} must be between 0 and 100")
        
        return errors
```

## Error Handling and Recovery

### Retry Mechanisms

The system implements sophisticated retry logic:

```python
class RetryManager:
    """Manage retry logic for failed executions."""
    
    def __init__(self):
        self.failure_classifiers = [
            self._classify_timeout_error,
            self._classify_resource_error,
            self._classify_permission_error,
            self._classify_network_error,
            self._classify_transient_error
        ]
    
    def should_retry(self, execution: ScheduleExecution, 
                    retry_config: RetryConfig) -> bool:
        """Determine if execution should be retried."""
        if execution.attempt >= retry_config.max_attempts:
            return False
        
        failure_type = self.classify_failure(execution.error_message)
        
        # Don't retry permanent failures
        if failure_type == FailureType.PERMANENT:
            return False
        
        # Always retry transient failures (within attempt limit)
        if failure_type == FailureType.TRANSIENT:
            return True
        
        # Retry resource failures with longer delays
        if failure_type == FailureType.RESOURCE:
            return execution.attempt <= (retry_config.max_attempts // 2)
        
        return False
    
    def classify_failure(self, error_message: str) -> FailureType:
        """Classify failure type based on error message."""
        if not error_message:
            return FailureType.UNKNOWN
        
        error_lower = error_message.lower()
        
        for classifier in self.failure_classifiers:
            failure_type = classifier(error_lower)
            if failure_type != FailureType.UNKNOWN:
                return failure_type
        
        return FailureType.UNKNOWN
    
    def _classify_timeout_error(self, error_message: str) -> FailureType:
        """Classify timeout-related errors."""
        timeout_indicators = ['timeout', 'timed out', 'deadline exceeded']
        
        if any(indicator in error_message for indicator in timeout_indicators):
            return FailureType.TRANSIENT
        
        return FailureType.UNKNOWN
    
    def _classify_resource_error(self, error_message: str) -> FailureType:
        """Classify resource-related errors."""
        resource_indicators = [
            'no space left', 'disk full', 'out of memory',
            'resource temporarily unavailable', 'too many open files'
        ]
        
        if any(indicator in error_message for indicator in resource_indicators):
            return FailureType.RESOURCE
        
        return FailureType.UNKNOWN
    
    def _classify_permission_error(self, error_message: str) -> FailureType:
        """Classify permission-related errors."""
        permission_indicators = [
            'permission denied', 'access denied', 'not permitted',
            'operation not allowed', 'insufficient privileges'
        ]
        
        if any(indicator in error_message for indicator in permission_indicators):
            return FailureType.PERMANENT
        
        return FailureType.UNKNOWN
```

### Circuit Breaker Pattern

Implement circuit breaker for failing schedules:

```python
class ScheduleCircuitBreaker:
    """Implement circuit breaker pattern for schedule execution."""
    
    def __init__(self, failure_threshold: int = 5, 
                 recovery_timeout: int = 300):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.schedule_states = {}  # schedule_name -> CircuitState
    
    def can_execute(self, schedule_name: str) -> bool:
        """Check if schedule can execute based on circuit breaker state."""
        state = self.schedule_states.get(schedule_name)
        
        if state is None:
            # First execution - allow
            self.schedule_states[schedule_name] = CircuitState()
            return True
        
        current_time = time.time()
        
        if state.state == CircuitBreakerState.CLOSED:
            # Normal operation
            return True
        
        elif state.state == CircuitBreakerState.OPEN:
            # Circuit is open - check if recovery timeout has passed
            if current_time - state.last_failure_time >= self.recovery_timeout:
                # Move to half-open state
                state.state = CircuitBreakerState.HALF_OPEN
                logger.info(f"Circuit breaker for {schedule_name} moved to HALF_OPEN")
                return True
            else:
                # Still in recovery period
                return False
        
        elif state.state == CircuitBreakerState.HALF_OPEN:
            # Allow one execution to test recovery
            return True
        
        return False
    
    def record_success(self, schedule_name: str):
        """Record successful execution."""
        state = self.schedule_states.get(schedule_name)
        if state is None:
            return
        
        if state.state == CircuitBreakerState.HALF_OPEN:
            # Recovery successful - close circuit
            state.state = CircuitBreakerState.CLOSED
            state.failure_count = 0
            logger.info(f"Circuit breaker for {schedule_name} CLOSED after recovery")
        
        # Reset failure count on success
        state.failure_count = 0
    
    def record_failure(self, schedule_name: str):
        """Record failed execution."""
        state = self.schedule_states.get(schedule_name)
        if state is None:
            state = CircuitState()
            self.schedule_states[schedule_name] = state
        
        state.failure_count += 1
        state.last_failure_time = time.time()
        
        if state.failure_count >= self.failure_threshold:
            if state.state != CircuitBreakerState.OPEN:
                state.state = CircuitBreakerState.OPEN
                logger.warning(
                    f"Circuit breaker for {schedule_name} OPENED after "
                    f"{state.failure_count} failures"
                )
        elif state.state == CircuitBreakerState.HALF_OPEN:
            # Failed during recovery - back to open
            state.state = CircuitBreakerState.OPEN
            logger.warning(f"Circuit breaker for {schedule_name} back to OPEN after recovery failure")
```

## Security Considerations

### Command Injection Prevention

```python
class SecureCommandExecutor:
    """Secure command execution with injection prevention."""
    
    def __init__(self):
        self.allowed_executables = {
            'python3', 'python', '/usr/bin/python3',
            '/boot/config/plugins/unraid-rebalancer/unraid_rebalancer.py'
        }
        self.dangerous_chars = set(';&|`$(){}[]<>*?')
    
    def validate_command(self, command: List[str]) -> bool:
        """Validate command for security issues."""
        if not command:
            return False
        
        # Check executable
        executable = command[0]
        if not self._is_allowed_executable(executable):
            logger.warning(f"Executable not in allowlist: {executable}")
            return False
        
        # Check for dangerous characters in arguments
        for arg in command[1:]:
            if self._contains_dangerous_chars(arg):
                logger.warning(f"Dangerous characters in argument: {arg}")
                return False
        
        # Check for shell injection patterns
        full_command = ' '.join(command)
        if self._contains_injection_patterns(full_command):
            logger.warning(f"Potential injection pattern detected: {full_command}")
            return False
        
        return True
    
    def _is_allowed_executable(self, executable: str) -> bool:
        """Check if executable is in allowlist."""
        # Direct match
        if executable in self.allowed_executables:
            return True
        
        # Check if it's the rebalancer script
        if executable.endswith('unraid_rebalancer.py'):
            return True
        
        # Check if it's a Python interpreter
        if os.path.basename(executable) in {'python', 'python3'}:
            return True
        
        return False
    
    def _contains_dangerous_chars(self, arg: str) -> bool:
        """Check for dangerous shell characters."""
        return any(char in arg for char in self.dangerous_chars)
    
    def _contains_injection_patterns(self, command: str) -> bool:
        """Check for common injection patterns."""
        injection_patterns = [
            r'\$\(',  # Command substitution
            r'`[^`]*`',  # Backtick command substitution
            r'&&|\|\|',  # Command chaining
            r'[;&]',  # Command separation
            r'\|',  # Pipes
            r'>>?',  # Redirection
            r'<',  # Input redirection
        ]
        
        for pattern in injection_patterns:
            if re.search(pattern, command):
                return True
        
        return False
    
    def execute_command(self, command: List[str], 
                       timeout: int = None) -> subprocess.CompletedProcess:
        """Execute command securely."""
        if not self.validate_command(command):
            raise SecurityError(f"Command failed security validation: {command}")
        
        try:
            # Use subprocess with explicit argument list (no shell)
            result = subprocess.run(
                command,
                capture_output=True,
                text=True,
                timeout=timeout,
                shell=False,  # Critical: never use shell=True
                env=self._get_safe_environment()
            )
            
            return result
            
        except subprocess.TimeoutExpired as e:
            logger.warning(f"Command timed out after {timeout}s: {command}")
            raise
        except Exception as e:
            logger.error(f"Command execution failed: {e}")
            raise
    
    def _get_safe_environment(self) -> Dict[str, str]:
        """Get safe environment variables for command execution."""
        # Start with minimal environment
        safe_env = {
            'PATH': '/usr/local/bin:/usr/bin:/bin',
            'HOME': '/tmp',
            'USER': 'nobody',
            'SHELL': '/bin/sh'
        }
        
        # Add specific variables needed for Unraid
        current_env = os.environ
        unraid_vars = [
            'UNRAID_VERSION', 'UNRAID_ROOT', 'UNRAID_BOOT',
            'PYTHONPATH', 'LANG', 'LC_ALL'
        ]
        
        for var in unraid_vars:
            if var in current_env:
                safe_env[var] = current_env[var]
        
        return safe_env
```

### File System Security

```python
class SecureFileManager:
    """Secure file operations for schedule management."""
    
    def __init__(self):
        self.allowed_paths = {
            '/boot/config/plugins/unraid-rebalancer/',
            '/var/log/unraid-rebalancer/',
            '/tmp/unraid-rebalancer/',
            '/boot/config/plugins/user.scripts/scripts/'
        }
        self.forbidden_paths = {
            '/etc/', '/bin/', '/sbin/', '/usr/bin/', '/usr/sbin/',
            '/boot/config/shadow', '/boot/config/passwd'
        }
    
    def validate_path(self, path: str) -> bool:
        """Validate file path for security."""
        try:
            # Resolve path to prevent directory traversal
            resolved_path = os.path.realpath(path)
            
            # Check for forbidden paths
            for forbidden in self.forbidden_paths:
                if resolved_path.startswith(forbidden):
                    logger.warning(f"Access denied to forbidden path: {resolved_path}")
                    return False
            
            # Check for allowed paths
            for allowed in self.allowed_paths:
                if resolved_path.startswith(allowed):
                    return True
            
            logger.warning(f"Access denied to non-allowed path: {resolved_path}")
            return False
            
        except Exception as e:
            logger.error(f"Path validation error: {e}")
            return False
    
    def secure_write(self, path: str, content: str, mode: int = 0o644) -> bool:
        """Write file securely with path validation."""
        if not self.validate_path(path):
            return False
        
        try:
            # Ensure directory exists
            os.makedirs(os.path.dirname(path), mode=0o755, exist_ok=True)
            
            # Write with secure permissions
            with open(path, 'w') as f:
                f.write(content)
            
            # Set explicit permissions
            os.chmod(path, mode)
            
            return True
            
        except Exception as e:
            logger.error(f"Secure write failed for {path}: {e}")
            return False
    
    def secure_read(self, path: str) -> Optional[str]:
        """Read file securely with path validation."""
        if not self.validate_path(path):
            return None
        
        try:
            with open(path, 'r') as f:
                return f.read()
        except Exception as e:
            logger.error(f"Secure read failed for {path}: {e}")
            return None
```

## Performance Optimization

### Efficient Schedule Processing

```python
class OptimizedScheduleProcessor:
    """Optimized processing for large numbers of schedules."""
    
    def __init__(self):
        self.schedule_cache = {}
        self.next_execution_cache = {}
        self.cache_ttl = 60  # 1 minute cache TTL
    
    def get_next_executions(self, schedules: Dict[str, ScheduleConfig], 
                           limit: int = 100) -> List[Tuple[str, datetime]]:
        """Get next execution times for schedules efficiently."""
        current_time = datetime.now()
        next_executions = []
        
        for name, schedule in schedules.items():
            if not schedule.enabled:
                continue
            
            # Check cache first
            cache_key = f"{name}:{schedule.cron_expression}"
            cached_result = self.next_execution_cache.get(cache_key)
            
            if cached_result and (current_time - cached_result['timestamp']).seconds < self.cache_ttl:
                next_time = cached_result['next_time']
            else:
                # Calculate and cache
                next_time = schedule.next_execution_time(current_time)
                self.next_execution_cache[cache_key] = {
                    'next_time': next_time,
                    'timestamp': current_time
                }
            
            next_executions.append((name, next_time))
        
        # Sort by execution time and limit results
        next_executions.sort(key=lambda x: x[1])
        return next_executions[:limit]
    
    def batch_update_schedules(self, updates: List[Tuple[str, ScheduleConfig]]) -> bool:
        """Update multiple schedules efficiently."""
        try:
            # Group updates by type
            cron_updates = []
            config_updates = []
            
            for name, schedule in updates:
                if self._needs_cron_update(name, schedule):
                    cron_updates.append((name, schedule))
                config_updates.append((name, schedule))
            
            # Batch cron updates
            if cron_updates:
                self._batch_cron_updates(cron_updates)
            
            # Batch configuration updates
            self._batch_config_updates(config_updates)
            
            # Clear relevant caches
            self._clear_caches([name for name, _ in updates])
            
            return True
            
        except Exception as e:
            logger.error(f"Batch update failed: {e}")
            return False
    
    def _needs_cron_update(self, name: str, schedule: ScheduleConfig) -> bool:
        """Check if schedule needs cron system update."""
        cached_schedule = self.schedule_cache.get(name)
        
        if cached_schedule is None:
            return True
        
        return (
            cached_schedule.cron_expression != schedule.cron_expression or
            cached_schedule.enabled != schedule.enabled
        )
    
    def _batch_cron_updates(self, updates: List[Tuple[str, ScheduleConfig]]):
        """Perform batch cron updates."""
        # Get current crontab
        current_crontab = self._get_current_crontab()
        
        # Remove old entries
        filtered_crontab = current_crontab
        for name, _ in updates:
            filtered_crontab = self._remove_schedule_entry(filtered_crontab, name)
        
        # Add new entries
        for name, schedule in updates:
            if schedule.enabled:
                entry = self._create_crontab_entry(schedule)
                filtered_crontab.append(entry)
        
        # Install updated crontab
        self._install_crontab(filtered_crontab)
```

## Troubleshooting Guide

### Common Issues and Solutions

#### 1. Schedules Not Executing

**Symptoms:**
- Schedules appear enabled but don't execute
- No execution history entries

**Diagnosis:**
```bash
# Check if cron service is running
systemctl status cron

# Check crontab entries
crontab -l | grep unraid-rebalancer

# Check schedule status
python3 unraid_rebalancer.py --get-schedule SCHEDULE_NAME

# Check system logs
journalctl -u cron | grep unraid-rebalancer
```

**Solutions:**
1. Restart cron service: `systemctl restart cron`
2. Reinstall schedule: `python3 unraid_rebalancer.py --reinstall-schedule SCHEDULE_NAME`
3. Check file permissions: `chmod +x unraid_rebalancer.py`

#### 2. Resource Threshold Issues

**Symptoms:**
- Schedules constantly delayed
- "Resource threshold exceeded" messages

**Diagnosis:**
```bash
# Monitor current resource usage
python3 unraid_rebalancer.py --monitor-resources --duration 300

# Check schedule thresholds
python3 unraid_rebalancer.py --get-schedule SCHEDULE_NAME

# Review execution history
python3 unraid_rebalancer.py --execution-history SCHEDULE_NAME
```

**Solutions:**
1. Adjust thresholds: `python3 unraid_rebalancer.py --update-schedule SCHEDULE_NAME --cpu-threshold 90`
2. Disable adaptive scheduling: `python3 unraid_rebalancer.py --update-schedule SCHEDULE_NAME --no-adaptive-scheduling`
3. Schedule during low-usage periods

#### 3. Permission Errors

**Symptoms:**
- "Permission denied" errors
- Crontab installation failures

**Diagnosis:**
```bash
# Check file permissions
ls -la unraid_rebalancer.py

# Check crontab permissions
crontab -l

# Check log file permissions
ls -la /var/log/unraid-rebalancer/
```

**Solutions:**
1. Fix script permissions: `chmod +x unraid_rebalancer.py`
2. Create log directory: `mkdir -p /var/log/unraid-rebalancer && chmod 755 /var/log/unraid-rebalancer`
3. Run with appropriate user privileges

### Debugging Tools

#### Enable Debug Logging

```bash
# Enable debug mode for schedule creation
python3 unraid_rebalancer.py --create-schedule \
  --name "debug_schedule" \
  --cron "0 2 * * *" \
  --debug

# Enable verbose logging
export UNRAID_REBALANCER_LOG_LEVEL=DEBUG
python3 unraid_rebalancer.py --list-schedules
```

#### Validate Configuration

```bash
# Validate all schedules
python3 unraid_rebalancer.py --validate-schedules

# Validate specific schedule
python3 unraid_rebalancer.py --validate-schedule SCHEDULE_NAME

# Test cron expression
python3 unraid_rebalancer.py --test-cron "0 2 * * *"
```

#### Monitor System Health

```bash
# Check scheduler health
python3 unraid_rebalancer.py --scheduler-health

# Monitor resource usage
python3 unraid_rebalancer.py --monitor-resources --duration 60

# Check execution statistics
python3 unraid_rebalancer.py --execution-stats
```

## API Reference

### Core Classes

#### SchedulingEngine

```python
class SchedulingEngine:
    def create_schedule(self, schedule: ScheduleConfig) -> bool
    def update_schedule(self, schedule: ScheduleConfig) -> bool
    def delete_schedule(self, name: str) -> bool
    def get_schedule(self, name: str) -> Optional[ScheduleConfig]
    def list_schedules(self, enabled_only: bool = False) -> List[ScheduleConfig]
    def execute_schedule(self, schedule: ScheduleConfig) -> ScheduleExecution
    def enable_schedule(self, name: str) -> bool
    def disable_schedule(self, name: str) -> bool
```

#### ScheduleConfig

```python
class ScheduleConfig:
    name: str
    cron_expression: str
    command: List[str]
    enabled: bool = True
    timeout_seconds: int = 3600
    cpu_threshold: float = 80.0
    memory_threshold: float = 90.0
    io_threshold: float = 70.0
    adaptive_scheduling: bool = False
    max_consecutive_failures: int = 3
    respect_maintenance_windows: bool = False
    notification_config: Optional[NotificationConfig] = None
```

#### ResourceMonitor

```python
class ResourceMonitor:
    def get_cpu_usage(self) -> float
    def get_memory_usage(self) -> float
    def get_io_usage(self) -> float
    def get_system_load(self) -> float
    def check_thresholds(self, schedule: ScheduleConfig) -> bool
    def start_monitoring(self, interval: float = 1.0)
    def stop_monitoring(self)
```

For complete API documentation, see the inline docstrings in the source code.