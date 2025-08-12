#!/usr/bin/env python3
"""
Unraid System Integration Module

Provides deep integration with Unraid system features including:
- Array status monitoring
- Parity check integration
- User share management
- System notification integration
- Docker container awareness
- VM status monitoring
- Plugin integration
"""

import json
import logging
import os
import re
import subprocess
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Union, Any, Tuple
from dataclasses import dataclass
from enum import Enum


class ArrayStatus(Enum):
    """Unraid array status states."""
    STARTED = "started"
    STOPPED = "stopped"
    STARTING = "starting"
    STOPPING = "stopping"
    PARITY_CHECK = "parity_check"
    PARITY_SYNC = "parity_sync"
    REBUILDING = "rebuilding"
    ERROR = "error"


class DiskStatus(Enum):
    """Individual disk status states."""
    ACTIVE = "active"
    STANDBY = "standby"
    SPUN_DOWN = "spun_down"
    DISABLED = "disabled"
    MISSING = "missing"
    ERROR = "error"
    NEW = "new"


class NotificationLevel(Enum):
    """Unraid notification levels."""
    NORMAL = "normal"
    WARNING = "warning"
    ALERT = "alert"
    CRITICAL = "critical"


@dataclass
class UnraidDisk:
    """Extended disk information with Unraid-specific details."""
    name: str
    device: str
    mount_point: str
    size_bytes: int
    used_bytes: int
    free_bytes: int
    status: DiskStatus
    temperature: Optional[int] = None
    spin_down_delay: Optional[int] = None
    file_system: Optional[str] = None
    last_check: Optional[datetime] = None
    errors: int = 0
    
    @property
    def used_percent(self) -> float:
        """Calculate used percentage."""
        if self.size_bytes == 0:
            return 0.0
        return (self.used_bytes / self.size_bytes) * 100


@dataclass
class ArrayInfo:
    """Unraid array information."""
    status: ArrayStatus
    num_devices: int
    num_disabled: int
    num_missing: int
    parity_valid: bool
    parity_sync_progress: Optional[float] = None
    last_parity_check: Optional[datetime] = None
    uptime: Optional[int] = None
    

@dataclass
class UserShare:
    """Unraid user share information."""
    name: str
    allocation_method: str  # high-water, fill-up, most-free
    included_disks: List[str]
    excluded_disks: List[str]
    split_level: int
    use_cache: bool
    cache_pool: Optional[str] = None
    

class UnraidSystemMonitor:
    """Monitor Unraid system status and integration points."""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self._proc_path = Path("/proc")
        self._sys_path = Path("/sys")
        self._var_path = Path("/var/local/emhttp")
        
    def get_array_status(self) -> ArrayInfo:
        """Get current array status from Unraid."""
        try:
            # Read array status from Unraid's state files
            state_file = self._var_path / "var.ini"
            if not state_file.exists():
                self.logger.warning("Unraid state file not found, using fallback detection")
                return self._get_array_status_fallback()
            
            config = self._parse_ini_file(state_file)
            
            status_str = config.get("mdState", "STOPPED").upper()
            try:
                status = ArrayStatus(status_str.lower())
            except ValueError:
                status = ArrayStatus.ERROR
            
            return ArrayInfo(
                status=status,
                num_devices=int(config.get("mdNumDevices", "0")),
                num_disabled=int(config.get("mdNumDisabled", "0")),
                num_missing=int(config.get("mdNumMissing", "0")),
                parity_valid=config.get("mdResync", "0") == "0",
                parity_sync_progress=self._get_parity_progress(),
                last_parity_check=self._get_last_parity_check(),
                uptime=self._get_system_uptime()
            )
            
        except Exception as e:
            self.logger.error(f"Failed to get array status: {e}")
            return self._get_array_status_fallback()
    
    def _get_array_status_fallback(self) -> ArrayInfo:
        """Fallback method to detect array status."""
        # Check if /mnt/disk* directories exist and are mounted
        disk_paths = list(Path("/mnt").glob("disk*"))
        mounted_disks = [p for p in disk_paths if p.is_mount()]
        
        if mounted_disks:
            status = ArrayStatus.STARTED
        else:
            status = ArrayStatus.STOPPED
            
        return ArrayInfo(
            status=status,
            num_devices=len(disk_paths),
            num_disabled=0,
            num_missing=0,
            parity_valid=True  # Assume valid if we can't check
        )
    
    def get_disk_details(self) -> List[UnraidDisk]:
        """Get detailed information about all disks."""
        disks = []
        
        # Scan for disk mount points
        mnt_path = Path("/mnt")
        for disk_path in sorted(mnt_path.glob("disk*")):
            if not disk_path.is_dir():
                continue
                
            disk_name = disk_path.name
            
            # Get disk usage
            try:
                stat = os.statvfs(disk_path)
                size_bytes = stat.f_blocks * stat.f_frsize
                free_bytes = stat.f_bavail * stat.f_frsize
                used_bytes = size_bytes - free_bytes
            except OSError:
                size_bytes = used_bytes = free_bytes = 0
            
            # Determine disk status
            if disk_path.is_mount():
                status = DiskStatus.ACTIVE
            else:
                status = DiskStatus.STANDBY
            
            # Get device information
            device = self._get_disk_device(disk_name)
            temperature = self._get_disk_temperature(device)
            file_system = self._get_disk_filesystem(disk_path)
            
            disks.append(UnraidDisk(
                name=disk_name,
                device=device or f"/dev/unknown_{disk_name}",
                mount_point=str(disk_path),
                size_bytes=size_bytes,
                used_bytes=used_bytes,
                free_bytes=free_bytes,
                status=status,
                temperature=temperature,
                file_system=file_system
            ))
        
        return disks
    
    def get_user_shares(self) -> List[UserShare]:
        """Get user share configuration."""
        shares = []
        share_config_path = Path("/boot/config/shares")
        
        if not share_config_path.exists():
            self.logger.warning("User share configuration not found")
            return shares
        
        for share_file in share_config_path.glob("*.cfg"):
            try:
                config = self._parse_ini_file(share_file)
                share_name = share_file.stem
                
                shares.append(UserShare(
                    name=share_name,
                    allocation_method=config.get("shareAllocator", "high-water"),
                    included_disks=config.get("shareInclude", "").split(","),
                    excluded_disks=config.get("shareExclude", "").split(","),
                    split_level=int(config.get("shareSplitLevel", "1")),
                    use_cache=config.get("shareUseCache", "no") == "yes",
                    cache_pool=config.get("shareCachePool")
                ))
                
            except Exception as e:
                self.logger.error(f"Failed to parse share config {share_file}: {e}")
        
        return shares
    
    def is_parity_check_running(self) -> bool:
        """Check if a parity check is currently running."""
        try:
            # Check /proc/mdstat for parity operations
            mdstat_path = Path("/proc/mdstat")
            if mdstat_path.exists():
                content = mdstat_path.read_text()
                return "check" in content or "resync" in content
        except Exception as e:
            self.logger.error(f"Failed to check parity status: {e}")
        
        return False
    
    def is_safe_for_rebalancing(self) -> Tuple[bool, List[str]]:
        """Check if it's safe to perform rebalancing operations."""
        issues = []
        
        # Check array status
        array_info = self.get_array_status()
        if array_info.status != ArrayStatus.STARTED:
            issues.append(f"Array not started (status: {array_info.status.value})")
        
        # Check for parity operations
        if self.is_parity_check_running():
            issues.append("Parity check/sync in progress")
        
        # Check for missing or disabled disks
        if array_info.num_missing > 0:
            issues.append(f"{array_info.num_missing} disks missing")
        
        if array_info.num_disabled > 0:
            issues.append(f"{array_info.num_disabled} disks disabled")
        
        # Check disk health
        disks = self.get_disk_details()
        error_disks = [d.name for d in disks if d.errors > 0]
        if error_disks:
            issues.append(f"Disks with errors: {', '.join(error_disks)}")
        
        # Check for high temperatures
        hot_disks = [d.name for d in disks if d.temperature and d.temperature > 50]
        if hot_disks:
            issues.append(f"High temperature disks: {', '.join(hot_disks)}")
        
        return len(issues) == 0, issues
    
    def send_unraid_notification(self, title: str, message: str, 
                                level: NotificationLevel = NotificationLevel.NORMAL) -> bool:
        """Send notification through Unraid's notification system."""
        try:
            # Use Unraid's notification system if available
            notify_cmd = [
                "/usr/local/emhttp/webGui/scripts/notify",
                "-e", title,
                "-s", message,
                "-i", level.value
            ]
            
            result = subprocess.run(notify_cmd, capture_output=True, text=True, timeout=10)
            if result.returncode == 0:
                self.logger.info(f"Sent Unraid notification: {title}")
                return True
            else:
                self.logger.error(f"Failed to send notification: {result.stderr}")
                
        except Exception as e:
            self.logger.error(f"Failed to send Unraid notification: {e}")
        
        return False
    
    def get_docker_containers(self) -> List[Dict[str, Any]]:
        """Get information about running Docker containers."""
        containers = []
        
        try:
            # Use docker command to get container info
            result = subprocess.run(
                ["docker", "ps", "--format", "json"],
                capture_output=True, text=True, timeout=30
            )
            
            if result.returncode == 0:
                for line in result.stdout.strip().split("\n"):
                    if line:
                        containers.append(json.loads(line))
                        
        except Exception as e:
            self.logger.error(f"Failed to get Docker containers: {e}")
        
        return containers
    
    def get_vm_status(self) -> List[Dict[str, Any]]:
        """Get information about running VMs."""
        vms = []
        
        try:
            # Check for libvirt VMs
            result = subprocess.run(
                ["virsh", "list", "--all"],
                capture_output=True, text=True, timeout=30
            )
            
            if result.returncode == 0:
                lines = result.stdout.strip().split("\n")[2:]  # Skip header
                for line in lines:
                    if line.strip():
                        parts = line.split()
                        if len(parts) >= 3:
                            vms.append({
                                "id": parts[0],
                                "name": parts[1],
                                "state": " ".join(parts[2:])
                            })
                            
        except Exception as e:
            self.logger.error(f"Failed to get VM status: {e}")
        
        return vms
    
    def _parse_ini_file(self, file_path: Path) -> Dict[str, str]:
        """Parse simple INI-style configuration file."""
        config = {}
        
        try:
            content = file_path.read_text()
            for line in content.split("\n"):
                line = line.strip()
                if line and "=" in line and not line.startswith("#"):
                    key, value = line.split("=", 1)
                    config[key.strip()] = value.strip().strip('"')
        except Exception as e:
            self.logger.error(f"Failed to parse {file_path}: {e}")
        
        return config
    
    def _get_disk_device(self, disk_name: str) -> Optional[str]:
        """Get the device path for a disk."""
        try:
            # Try to find device from mount info
            result = subprocess.run(
                ["findmnt", "-n", "-o", "SOURCE", f"/mnt/{disk_name}"],
                capture_output=True, text=True, timeout=5
            )
            
            if result.returncode == 0:
                return result.stdout.strip()
                
        except Exception as e:
            self.logger.debug(f"Failed to get device for {disk_name}: {e}")
        
        return None
    
    def _get_disk_temperature(self, device: Optional[str]) -> Optional[int]:
        """Get disk temperature using smartctl."""
        if not device:
            return None
            
        try:
            result = subprocess.run(
                ["smartctl", "-A", device],
                capture_output=True, text=True, timeout=10
            )
            
            if result.returncode == 0:
                for line in result.stdout.split("\n"):
                    if "Temperature_Celsius" in line:
                        parts = line.split()
                        if len(parts) >= 10:
                            return int(parts[9])
                            
        except Exception as e:
            self.logger.debug(f"Failed to get temperature for {device}: {e}")
        
        return None
    
    def _get_disk_filesystem(self, disk_path: Path) -> Optional[str]:
        """Get filesystem type for a disk."""
        try:
            result = subprocess.run(
                ["findmnt", "-n", "-o", "FSTYPE", str(disk_path)],
                capture_output=True, text=True, timeout=5
            )
            
            if result.returncode == 0:
                return result.stdout.strip()
                
        except Exception as e:
            self.logger.debug(f"Failed to get filesystem for {disk_path}: {e}")
        
        return None
    
    def _get_parity_progress(self) -> Optional[float]:
        """Get parity check/sync progress percentage."""
        try:
            mdstat_path = Path("/proc/mdstat")
            if mdstat_path.exists():
                content = mdstat_path.read_text()
                
                # Look for progress indicators
                for line in content.split("\n"):
                    if "%" in line and ("check" in line or "resync" in line):
                        match = re.search(r'(\d+\.\d+)%', line)
                        if match:
                            return float(match.group(1))
                            
        except Exception as e:
            self.logger.debug(f"Failed to get parity progress: {e}")
        
        return None
    
    def _get_last_parity_check(self) -> Optional[datetime]:
        """Get timestamp of last parity check."""
        try:
            # Check Unraid's parity check log
            log_path = Path("/var/log/parity.log")
            if log_path.exists():
                content = log_path.read_text()
                lines = content.strip().split("\n")
                if lines:
                    last_line = lines[-1]
                    # Parse timestamp from log entry
                    match = re.match(r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})', last_line)
                    if match:
                        return datetime.strptime(match.group(1), "%Y-%m-%d %H:%M:%S")
                        
        except Exception as e:
            self.logger.debug(f"Failed to get last parity check: {e}")
        
        return None
    
    def _get_system_uptime(self) -> Optional[int]:
        """Get system uptime in seconds."""
        try:
            uptime_path = Path("/proc/uptime")
            if uptime_path.exists():
                content = uptime_path.read_text().strip()
                return int(float(content.split()[0]))
                
        except Exception as e:
            self.logger.debug(f"Failed to get uptime: {e}")
        
        return None


class UnraidIntegrationManager:
    """Main integration manager for Unraid system features."""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.monitor = UnraidSystemMonitor()
        
    def pre_rebalance_checks(self) -> Tuple[bool, List[str]]:
        """Perform comprehensive pre-rebalance safety checks."""
        self.logger.info("Performing pre-rebalance safety checks...")
        
        all_issues = []
        
        # Basic system safety checks
        safe, issues = self.monitor.is_safe_for_rebalancing()
        all_issues.extend(issues)
        
        # Check for active Docker containers that might be affected
        containers = self.monitor.get_docker_containers()
        active_containers = [c for c in containers if c.get("State") == "running"]
        if active_containers:
            container_names = [c.get("Names", "unknown") for c in active_containers]
            self.logger.warning(f"Active Docker containers: {', '.join(container_names)}")
            # This is a warning, not a blocking issue
        
        # Check for running VMs
        vms = self.monitor.get_vm_status()
        running_vms = [vm for vm in vms if "running" in vm.get("state", "").lower()]
        if running_vms:
            vm_names = [vm.get("name", "unknown") for vm in running_vms]
            self.logger.warning(f"Running VMs: {', '.join(vm_names)}")
            # This is a warning, not a blocking issue
        
        return len(all_issues) == 0, all_issues
    
    def post_rebalance_actions(self, success: bool, summary: Dict[str, Any]) -> None:
        """Perform post-rebalance actions and notifications."""
        if success:
            title = "Rebalance Completed Successfully"
            message = f"Moved {summary.get('files_moved', 0)} files, {summary.get('bytes_moved', 0)} bytes"
            level = NotificationLevel.NORMAL
        else:
            title = "Rebalance Failed"
            message = f"Rebalance operation failed: {summary.get('error', 'Unknown error')}"
            level = NotificationLevel.ALERT
        
        # Send Unraid notification
        self.monitor.send_unraid_notification(title, message, level)
        
        self.logger.info(f"Post-rebalance notification sent: {title}")
    
    def get_system_status_report(self) -> Dict[str, Any]:
        """Generate comprehensive system status report."""
        array_info = self.monitor.get_array_status()
        disks = self.monitor.get_disk_details()
        shares = self.monitor.get_user_shares()
        containers = self.monitor.get_docker_containers()
        vms = self.monitor.get_vm_status()
        
        return {
            "timestamp": datetime.now().isoformat(),
            "array": {
                "status": array_info.status.value,
                "devices": array_info.num_devices,
                "disabled": array_info.num_disabled,
                "missing": array_info.num_missing,
                "parity_valid": array_info.parity_valid,
                "parity_progress": array_info.parity_sync_progress,
                "uptime": array_info.uptime
            },
            "disks": [
                {
                    "name": d.name,
                    "device": d.device,
                    "status": d.status.value,
                    "used_percent": d.used_percent,
                    "temperature": d.temperature,
                    "filesystem": d.file_system,
                    "errors": d.errors
                }
                for d in disks
            ],
            "shares": [
                {
                    "name": s.name,
                    "allocation": s.allocation_method,
                    "use_cache": s.use_cache,
                    "split_level": s.split_level
                }
                for s in shares
            ],
            "containers": len(containers),
            "vms": len(vms),
            "active_containers": len([c for c in containers if c.get("State") == "running"]),
            "running_vms": len([vm for vm in vms if "running" in vm.get("state", "").lower()])
        }