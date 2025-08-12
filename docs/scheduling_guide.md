# Unraid Rebalancer Scheduling Guide

This guide covers the advanced scheduling features of the Unraid Rebalancer, including automated scheduling, resource monitoring, and system integration.

## Table of Contents

1. [Overview](#overview)
2. [Quick Start](#quick-start)
3. [CLI Scheduling Options](#cli-scheduling-options)
4. [Schedule Management](#schedule-management)
5. [Resource Monitoring](#resource-monitoring)
6. [Unraid Integration](#unraid-integration)
7. [Examples and Use Cases](#examples-and-use-cases)
8. [Troubleshooting](#troubleshooting)

## Overview

The Unraid Rebalancer includes a comprehensive scheduling system that allows you to:

- **Automate rebalancing operations** with flexible timing options
- **Monitor system resources** and adapt scheduling based on load
- **Integrate with Unraid** user scripts and maintenance windows
- **Manage multiple schedules** with different configurations
- **Handle errors gracefully** with retry mechanisms and notifications

### Key Features

- **Cron-based scheduling** with standard cron expressions
- **Resource-aware execution** that respects system load
- **Maintenance window support** to avoid conflicts with Unraid operations
- **User script integration** for seamless Unraid workflow
- **Comprehensive monitoring** with execution history and statistics
- **Error recovery** with configurable retry policies

## Quick Start

### Creating Your First Schedule

```bash
# Create a daily rebalancing schedule at 2 AM
python3 unraid_rebalancer.py --create-schedule \
  --name "daily_rebalance" \
  --cron "0 2 * * *" \
  --target 80 \
  --mode balanced

# List all schedules
python3 unraid_rebalancer.py --list-schedules

# Execute a schedule manually
python3 unraid_rebalancer.py --execute-schedule "daily_rebalance"
```

### Using Templates

```bash
# List available templates
python3 unraid_rebalancer.py --list-templates

# Create schedule from template
python3 unraid_rebalancer.py --create-from-template \
  --template "daily_rebalance" \
  --name "my_daily_rebalance" \
  --param target_percent=85
```

## CLI Scheduling Options

### Schedule Creation

```bash
python3 unraid_rebalancer.py --create-schedule [OPTIONS]
```

**Required Options:**
- `--name NAME`: Unique name for the schedule
- `--cron EXPRESSION`: Cron expression for timing (e.g., "0 2 * * *")

**Rebalancing Options:**
- `--target PERCENT`: Target fill percentage (default: 80)
- `--mode MODE`: Rebalancing mode (fast|balanced|integrity, default: balanced)
- `--min-unit-size SIZE`: Minimum unit size to move (default: 1GB)
- `--max-unit-size SIZE`: Maximum unit size to move (default: 100GB)
- `--exclude PATTERN`: Exclude patterns (can be used multiple times)

**Scheduling Options:**
- `--enabled/--disabled`: Enable or disable the schedule (default: enabled)
- `--timeout SECONDS`: Execution timeout in seconds (default: 3600)
- `--max-failures COUNT`: Maximum consecutive failures before suspension (default: 3)
- `--respect-maintenance`: Respect Unraid maintenance windows
- `--create-user-script`: Create corresponding Unraid user script

**Resource Monitoring:**
- `--cpu-threshold PERCENT`: CPU usage threshold (default: 80)
- `--memory-threshold PERCENT`: Memory usage threshold (default: 90)
- `--io-threshold PERCENT`: Disk I/O threshold (default: 70)
- `--adaptive-scheduling`: Enable adaptive scheduling based on load

**Notification Options:**
- `--notify-success/--no-notify-success`: Notify on successful completion
- `--notify-failure/--no-notify-failure`: Notify on failures (default: enabled)
- `--notify-timeout/--no-notify-timeout`: Notify on timeouts (default: enabled)
- `--email RECIPIENT`: Email recipient for notifications
- `--webhook URL`: Webhook URL for notifications
- `--use-unraid-notifications`: Use Unraid's built-in notification system

### Schedule Management

```bash
# List all schedules
python3 unraid_rebalancer.py --list-schedules [--enabled-only] [--type TYPE]

# Get schedule details
python3 unraid_rebalancer.py --get-schedule NAME

# Update existing schedule
python3 unraid_rebalancer.py --update-schedule NAME [OPTIONS]

# Enable/disable schedule
python3 unraid_rebalancer.py --enable-schedule NAME
python3 unraid_rebalancer.py --disable-schedule NAME

# Delete schedule
python3 unraid_rebalancer.py --delete-schedule NAME

# Execute schedule manually
python3 unraid_rebalancer.py --execute-schedule NAME
```

### Monitoring and Statistics

```bash
# View execution history
python3 unraid_rebalancer.py --execution-history NAME [--limit COUNT]

# Get schedule statistics
python3 unraid_rebalancer.py --schedule-stats NAME

# Check schedule health
python3 unraid_rebalancer.py --schedule-health [NAME]

# Monitor system resources
python3 unraid_rebalancer.py --monitor-resources [--duration SECONDS]
```

## Schedule Management

### Cron Expressions

The scheduling system uses standard cron expressions with five fields:

```
┌───────────── minute (0 - 59)
│ ┌─────────── hour (0 - 23)
│ │ ┌───────── day of month (1 - 31)
│ │ │ ┌─────── month (1 - 12)
│ │ │ │ ┌───── day of week (0 - 6) (Sunday to Saturday)
│ │ │ │ │
* * * * *
```

**Common Examples:**
- `0 2 * * *` - Daily at 2:00 AM
- `0 2 * * 0` - Weekly on Sunday at 2:00 AM
- `0 2 1 * *` - Monthly on the 1st at 2:00 AM
- `*/30 * * * *` - Every 30 minutes
- `0 2,14 * * *` - Daily at 2:00 AM and 2:00 PM
- `0 2 * * 1-5` - Weekdays at 2:00 AM

### Schedule States

- **Enabled**: Schedule is active and will execute at specified times
- **Disabled**: Schedule exists but will not execute
- **Suspended**: Schedule automatically disabled due to repeated failures
- **Running**: Schedule is currently executing

### Execution Status

- **Success**: Execution completed successfully
- **Failed**: Execution failed with an error
- **Timeout**: Execution exceeded the specified timeout
- **Skipped**: Execution skipped (e.g., during maintenance window)
- **Cancelled**: Execution was manually cancelled

## Resource Monitoring

### Adaptive Scheduling

When adaptive scheduling is enabled, the system monitors:

- **CPU Usage**: Current CPU utilization percentage
- **Memory Usage**: Current memory utilization percentage
- **Disk I/O**: Current disk I/O load percentage
- **System Load**: Overall system load average

**Behavior:**
- Schedules are delayed if resource thresholds are exceeded
- Execution is retried when resources become available
- Resource checks occur before and during execution
- Configurable thresholds for each resource type

### Resource Thresholds

```bash
# Set custom resource thresholds
python3 unraid_rebalancer.py --create-schedule \
  --name "resource_aware" \
  --cron "0 2 * * *" \
  --cpu-threshold 70 \
  --memory-threshold 85 \
  --io-threshold 60 \
  --adaptive-scheduling
```

### Monitoring Commands

```bash
# Check current resource usage
python3 unraid_rebalancer.py --monitor-resources

# Monitor resources for 5 minutes
python3 unraid_rebalancer.py --monitor-resources --duration 300

# Get resource usage history
python3 unraid_rebalancer.py --resource-history --hours 24
```

## Unraid Integration

### User Scripts Integration

The scheduler can create and manage Unraid user scripts:

```bash
# List existing user scripts
python3 unraid_rebalancer.py --list-user-scripts

# Create user script from schedule
python3 unraid_rebalancer.py --create-user-script \
  --schedule "daily_rebalance" \
  --description "Daily rebalancing at 2 AM"

# Create user script with cron integration
python3 unraid_rebalancer.py --create-user-script \
  --schedule "daily_rebalance" \
  --include-cron
```

### Maintenance Windows

Respect Unraid maintenance windows to avoid conflicts:

```bash
# Check if currently in maintenance window
python3 unraid_rebalancer.py --check-maintenance

# Create schedule that respects maintenance windows
python3 unraid_rebalancer.py --create-schedule \
  --name "maintenance_aware" \
  --cron "0 * * * *" \
  --respect-maintenance
```

**Maintenance Window Configuration:**
Create `/boot/config/maintenance.conf`:
```
# Format: day_of_week:start_hour:end_hour
# 0=Sunday, 1=Monday, etc.
0:02:06  # Sunday 2 AM to 6 AM
3:01:05  # Wednesday 1 AM to 5 AM
```

### Unraid Notifications

```bash
# Enable Unraid notifications
python3 unraid_rebalancer.py --create-schedule \
  --name "with_notifications" \
  --cron "0 2 * * *" \
  --use-unraid-notifications \
  --notify-failure
```

### Templates

```bash
# List available Unraid templates
python3 unraid_rebalancer.py --list-templates

# Create from daily rebalance template
python3 unraid_rebalancer.py --create-from-template \
  --template "daily_rebalance" \
  --name "my_schedule" \
  --param target_percent=85 \
  --param hour=3

# Create from weekly cleanup template
python3 unraid_rebalancer.py --create-from-template \
  --template "weekly_cleanup" \
  --name "weekend_cleanup"
```

## Examples and Use Cases

### Basic Daily Rebalancing

```bash
# Simple daily rebalancing at 2 AM
python3 unraid_rebalancer.py --create-schedule \
  --name "daily_rebalance" \
  --cron "0 2 * * *" \
  --target 80 \
  --mode balanced
```

### Resource-Aware Scheduling

```bash
# Rebalancing that waits for low system load
python3 unraid_rebalancer.py --create-schedule \
  --name "smart_rebalance" \
  --cron "0 2 * * *" \
  --target 85 \
  --adaptive-scheduling \
  --cpu-threshold 60 \
  --memory-threshold 80 \
  --io-threshold 50
```

### Weekly Deep Rebalancing

```bash
# Comprehensive weekly rebalancing on Sunday
python3 unraid_rebalancer.py --create-schedule \
  --name "weekly_deep_rebalance" \
  --cron "0 1 * * 0" \
  --target 75 \
  --mode integrity \
  --min-unit-size 500MB \
  --max-unit-size 50GB \
  --timeout 7200
```

### Maintenance-Aware Scheduling

```bash
# Hourly rebalancing that respects maintenance windows
python3 unraid_rebalancer.py --create-schedule \
  --name "hourly_maintenance_aware" \
  --cron "0 * * * *" \
  --target 80 \
  --respect-maintenance \
  --create-user-script
```

### High-Frequency Monitoring

```bash
# Every 15 minutes during business hours
python3 unraid_rebalancer.py --create-schedule \
  --name "business_hours_rebalance" \
  --cron "*/15 9-17 * * 1-5" \
  --target 90 \
  --mode fast \
  --adaptive-scheduling
```

### Conditional Rebalancing

```bash
# Only rebalance if specific conditions are met
python3 unraid_rebalancer.py --create-schedule \
  --name "conditional_rebalance" \
  --cron "0 3 * * *" \
  --target 80 \
  --min-imbalance 10 \
  --only-if-needed
```

### Multi-Target Scheduling

```bash
# Different targets for different times
# Aggressive during night
python3 unraid_rebalancer.py --create-schedule \
  --name "night_aggressive" \
  --cron "0 2 * * *" \
  --target 75 \
  --mode integrity

# Conservative during day
python3 unraid_rebalancer.py --create-schedule \
  --name "day_conservative" \
  --cron "0 14 * * *" \
  --target 90 \
  --mode fast \
  --adaptive-scheduling
```

### Notification Examples

```bash
# Email notifications for failures
python3 unraid_rebalancer.py --create-schedule \
  --name "email_notifications" \
  --cron "0 2 * * *" \
  --target 80 \
  --notify-failure \
  --email admin@example.com

# Webhook notifications
python3 unraid_rebalancer.py --create-schedule \
  --name "webhook_notifications" \
  --cron "0 2 * * *" \
  --target 80 \
  --notify-success \
  --notify-failure \
  --webhook https://hooks.slack.com/services/YOUR/WEBHOOK/URL

# Unraid notifications
python3 unraid_rebalancer.py --create-schedule \
  --name "unraid_notifications" \
  --cron "0 2 * * *" \
  --target 80 \
  --use-unraid-notifications \
  --notify-failure
```

## Troubleshooting

### Common Issues

#### Schedule Not Executing

1. **Check if schedule is enabled:**
   ```bash
   python3 unraid_rebalancer.py --get-schedule SCHEDULE_NAME
   ```

2. **Verify cron expression:**
   ```bash
   # Test cron expression online or use:
   python3 unraid_rebalancer.py --validate-cron "0 2 * * *"
   ```

3. **Check system cron service:**
   ```bash
   # On most systems:
   systemctl status cron
   # or
   service cron status
   ```

#### Resource Threshold Issues

1. **Check current resource usage:**
   ```bash
   python3 unraid_rebalancer.py --monitor-resources
   ```

2. **Adjust thresholds:**
   ```bash
   python3 unraid_rebalancer.py --update-schedule SCHEDULE_NAME \
     --cpu-threshold 90 \
     --memory-threshold 95
   ```

3. **Disable adaptive scheduling temporarily:**
   ```bash
   python3 unraid_rebalancer.py --update-schedule SCHEDULE_NAME \
     --no-adaptive-scheduling
   ```

#### Execution Failures

1. **Check execution history:**
   ```bash
   python3 unraid_rebalancer.py --execution-history SCHEDULE_NAME --limit 10
   ```

2. **Review error messages:**
   ```bash
   python3 unraid_rebalancer.py --schedule-stats SCHEDULE_NAME
   ```

3. **Test manual execution:**
   ```bash
   python3 unraid_rebalancer.py --execute-schedule SCHEDULE_NAME
   ```

#### Permission Issues

1. **Check crontab permissions:**
   ```bash
   crontab -l
   ```

2. **Verify script permissions:**
   ```bash
   ls -la unraid_rebalancer.py
   chmod +x unraid_rebalancer.py
   ```

3. **Check Unraid user script permissions:**
   ```bash
   ls -la /boot/config/plugins/user.scripts/scripts/
   ```

### Debugging Commands

```bash
# Enable debug logging
python3 unraid_rebalancer.py --create-schedule \
  --name "debug_schedule" \
  --cron "0 2 * * *" \
  --debug

# Check scheduler status
python3 unraid_rebalancer.py --scheduler-status

# Validate configuration
python3 unraid_rebalancer.py --validate-config

# Test notifications
python3 unraid_rebalancer.py --test-notifications
```

### Log Files

- **Scheduler logs:** `/var/log/unraid-rebalancer/scheduler.log`
- **Execution logs:** `/var/log/unraid-rebalancer/executions/`
- **System logs:** Check system cron logs (`/var/log/cron` or `journalctl -u cron`)

### Getting Help

```bash
# Show all scheduling options
python3 unraid_rebalancer.py --help-scheduling

# Show specific command help
python3 unraid_rebalancer.py --create-schedule --help

# Show examples
python3 unraid_rebalancer.py --scheduling-examples
```

For additional support, check the project documentation or open an issue on the project repository.