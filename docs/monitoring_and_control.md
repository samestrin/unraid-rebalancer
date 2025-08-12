# Schedule Monitoring and Control

This document describes the monitoring and control features for the Unraid Rebalancer's advanced scheduling system.

## Overview

The monitoring and control system provides comprehensive tracking, management, and emergency controls for scheduled rebalancing operations. It includes execution logging, status reporting, schedule history, and emergency controls.

## Features

### Execution Tracking

- **Real-time Monitoring**: Track running executions with process IDs
- **Execution History**: Complete history of all schedule executions
- **Status Tracking**: Monitor execution states (pending, running, completed, failed, cancelled, suspended)
- **Performance Metrics**: Track files moved, bytes transferred, and execution duration

### Schedule Statistics

- **Success Rates**: Calculate success/failure rates for each schedule
- **Performance Analytics**: Track total files moved and bytes transferred
- **Execution Counts**: Monitor total, successful, and failed executions
- **Timing Analysis**: Track average execution duration and last execution times

### Emergency Controls

- **Execution Cancellation**: Cancel individual running executions
- **Schedule Suspension**: Temporarily suspend schedules with reason tracking
- **Emergency Stop**: Cancel all running executions simultaneously
- **Process Management**: Graceful termination with SIGTERM followed by SIGKILL

## CLI Commands

### List Running Executions

```bash
# List all currently running executions
python unraid_rebalancer.py --list-executions
```

### View Execution History

```bash
# View execution history for a specific schedule
python unraid_rebalancer.py --execution-history SCHEDULE_ID

# View execution history for all schedules
python unraid_rebalancer.py --execution-history
```

### View Schedule Statistics

```bash
# View statistics for a specific schedule
python unraid_rebalancer.py --schedule-stats SCHEDULE_ID
```

### Cancel Execution

```bash
# Cancel a specific execution
python unraid_rebalancer.py --cancel-execution EXECUTION_ID
```

### Suspend/Resume Schedules

```bash
# Suspend a schedule with reason
python unraid_rebalancer.py --suspend-schedule SCHEDULE_ID --suspend-reason "Maintenance window"

# Resume a suspended schedule
python unraid_rebalancer.py --resume-schedule SCHEDULE_ID
```

### Cleanup Old Records

```bash
# Clean up execution records older than 30 days (default)
python unraid_rebalancer.py --cleanup-executions

# Clean up execution records older than specified days
python unraid_rebalancer.py --cleanup-executions --cleanup-days 60
```

### Emergency Stop

```bash
# Cancel all running executions immediately
python unraid_rebalancer.py --emergency-stop
```

## Data Structures

### ScheduleExecution

Tracks individual execution instances:

```python
@dataclass
class ScheduleExecution:
    execution_id: str              # Unique execution identifier
    schedule_id: str               # Associated schedule ID
    start_time: float              # Execution start timestamp
    end_time: Optional[float]      # Execution end timestamp
    status: ExecutionStatus        # Current execution status
    exit_code: Optional[int]       # Process exit code
    error_message: str             # Error details if failed
    operation_id: str              # Operation identifier
    files_moved: int               # Number of files moved
    bytes_moved: int               # Total bytes transferred
    duration_seconds: float        # Execution duration
    pid: Optional[int]             # Process ID if running
```

### ScheduleStatistics

Aggregated statistics for schedules:

```python
@dataclass
class ScheduleStatistics:
    schedule_id: str               # Schedule identifier
    total_executions: int          # Total execution count
    successful_executions: int     # Successful execution count
    failed_executions: int         # Failed execution count
    cancelled_executions: int      # Cancelled execution count
    last_execution_time: Optional[float]  # Last execution timestamp
    last_success_time: Optional[float]    # Last successful execution
    last_failure_time: Optional[float]    # Last failed execution
    average_duration_seconds: float       # Average execution duration
    total_files_moved: int         # Total files moved across all executions
    total_bytes_moved: int         # Total bytes transferred
    
    @property
    def success_rate(self) -> float:  # Calculated success rate percentage
```

### ExecutionStatus

Execution state enumeration:

```python
class ExecutionStatus(Enum):
    PENDING = "pending"        # Scheduled but not started
    RUNNING = "running"        # Currently executing
    COMPLETED = "completed"    # Successfully completed
    FAILED = "failed"          # Failed with error
    CANCELLED = "cancelled"    # Manually cancelled
    SUSPENDED = "suspended"    # Schedule suspended
```

## Storage and Persistence

### Execution Records

- Stored as JSON files in `{config_dir}/executions/`
- One file per execution: `{execution_id}.json`
- Automatic cleanup of old records
- Serialization includes all execution metadata

### Running Executions

- Maintained in memory during application runtime
- Automatically restored on application restart
- Process ID tracking for active executions

## Integration with SchedulingEngine

The monitoring system is integrated with the main scheduling engine:

```python
class SchedulingEngine:
    def __init__(self, script_path, config_dir="./schedules"):
        self.schedule_manager = ScheduleManager(config_dir)
        self.cron_manager = CronManager(script_path)
        self.monitor = ScheduleMonitor(config_dir)  # Monitoring integration
```

## Error Handling

### Process Management

- Graceful termination with SIGTERM (15)
- Force termination with SIGKILL (9) after timeout
- Handle process lookup errors for already terminated processes
- Permission error handling for process termination

### Data Persistence

- Robust JSON serialization/deserialization
- File system error handling
- Automatic directory creation
- Cleanup of corrupted execution files

## Security Considerations

### Process Control

- Only processes started by the scheduler can be terminated
- Process ID validation before termination
- Permission checks for process management
- Logging of all process control actions

### Data Access

- Execution records stored in protected configuration directory
- No sensitive information in execution logs
- Secure handling of error messages and process information

## Performance Considerations

### Memory Usage

- Running executions maintained in memory for fast access
- Historical data loaded on demand
- Automatic cleanup of old execution records
- Configurable retention periods

### Disk Usage

- Compact JSON storage for execution records
- Automatic cleanup prevents disk space issues
- Configurable cleanup intervals and retention policies

## Monitoring Best Practices

### Regular Maintenance

1. **Monitor Success Rates**: Check schedule statistics regularly
2. **Review Failed Executions**: Investigate failure patterns
3. **Cleanup Old Records**: Run cleanup commands periodically
4. **Monitor Resource Usage**: Check system resources during executions

### Emergency Procedures

1. **Emergency Stop**: Use `--emergency-stop` for immediate halt
2. **Schedule Suspension**: Suspend problematic schedules temporarily
3. **Execution Cancellation**: Cancel specific problematic executions
4. **Log Review**: Check execution logs for error patterns

### Performance Optimization

1. **Execution Timing**: Monitor execution durations for optimization
2. **Resource Thresholds**: Adjust resource limits based on statistics
3. **Schedule Frequency**: Optimize schedule timing based on success rates
4. **Cleanup Frequency**: Balance retention needs with storage efficiency

## Troubleshooting

### Common Issues

1. **Stuck Executions**: Use cancellation or emergency stop
2. **High Failure Rates**: Review schedule configuration and system resources
3. **Missing Execution Records**: Check file permissions and disk space
4. **Process Termination Failures**: Verify process permissions and system state

### Diagnostic Commands

```bash
# Check running executions
python unraid_rebalancer.py --list-executions

# Review recent failures
python unraid_rebalancer.py --execution-history --limit 10

# Check schedule health
python unraid_rebalancer.py --schedule-stats SCHEDULE_ID

# Emergency diagnostics
python unraid_rebalancer.py --emergency-stop
```

This monitoring and control system provides comprehensive oversight and management capabilities for the Unraid Rebalancer's scheduling system, ensuring reliable operation and easy troubleshooting.