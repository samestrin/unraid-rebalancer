#!/usr/bin/env python3
"""
Progress Reporting Implementation

This module provides enhanced progress reporting and monitoring capabilities
for atomic rsync operations across all performance modes.
"""

import re
import time
import threading
import subprocess
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any, Callable
from dataclasses import dataclass, field
from enum import Enum


class ProgressPhase(Enum):
    """Phases of a transfer operation."""
    VALIDATING = "validating"
    STARTING = "starting"
    TRANSFERRING = "transferring"
    VERIFYING = "verifying"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class TransferProgress:
    """Progress information for a single transfer operation."""
    transfer_id: str
    source_path: Path
    destination_path: Path
    total_bytes: int
    transferred_bytes: int = 0
    current_file: Optional[str] = None
    current_file_bytes: int = 0
    current_file_total: int = 0
    files_transferred: int = 0
    total_files: Optional[int] = None
    transfer_rate_bps: float = 0.0
    phase: ProgressPhase = ProgressPhase.STARTING
    start_time: float = field(default_factory=time.time)
    last_update: float = field(default_factory=time.time)
    estimated_completion: Optional[float] = None
    error_message: Optional[str] = None

    @property
    def progress_percent(self) -> float:
        """Calculate progress percentage."""
        if self.total_bytes == 0:
            return 0.0
        return min(100.0, (self.transferred_bytes / self.total_bytes) * 100.0)

    @property
    def elapsed_time(self) -> float:
        """Calculate elapsed time in seconds."""
        return time.time() - self.start_time

    @property
    def eta_seconds(self) -> Optional[float]:
        """Calculate estimated time to completion."""
        if self.transfer_rate_bps <= 0 or self.total_bytes == 0:
            return None

        remaining_bytes = self.total_bytes - self.transferred_bytes
        if remaining_bytes <= 0:
            return 0.0

        return remaining_bytes / self.transfer_rate_bps

    @property
    def transfer_rate_mbps(self) -> float:
        """Get transfer rate in MB/s."""
        return self.transfer_rate_bps / (1024 * 1024)

    def update_progress(self, transferred_bytes: int = None, current_file: str = None,
                       current_file_bytes: int = None, current_file_total: int = None,
                       transfer_rate_bps: float = None):
        """Update progress information."""
        self.last_update = time.time()

        if transferred_bytes is not None:
            self.transferred_bytes = transferred_bytes
        if current_file is not None:
            self.current_file = current_file
        if current_file_bytes is not None:
            self.current_file_bytes = current_file_bytes
        if current_file_total is not None:
            self.current_file_total = current_file_total
        if transfer_rate_bps is not None:
            self.transfer_rate_bps = transfer_rate_bps

    def format_status_line(self, include_file_info: bool = True) -> str:
        """Format a status line for display."""
        status_parts = [
            f"{self.progress_percent:.1f}%",
            f"{self.transfer_rate_mbps:.1f} MB/s"
        ]

        if self.eta_seconds is not None:
            eta_mins = int(self.eta_seconds / 60)
            eta_secs = int(self.eta_seconds % 60)
            status_parts.append(f"ETA {eta_mins}m{eta_secs}s")

        if include_file_info and self.current_file:
            file_name = Path(self.current_file).name
            if len(file_name) > 30:
                file_name = file_name[:27] + "..."
            status_parts.append(f"File: {file_name}")

        return " | ".join(status_parts)


class RsyncProgressParser:
    """Parser for rsync progress output to extract progress information."""

    # Regex patterns for different rsync progress formats
    PROGRESS_PATTERNS = {
        # --info=progress2 format: "1,234,567  45%   10.50MB/s    0:01:23"
        "progress2": re.compile(r'(\d+(?:,\d+)*)\s+(\d+)%\s+([\d.]+)(MB|KB|GB)/s\s+(\d+):(\d+):(\d+)'),

        # --progress format: "1234567  100%   10.50MB/s    0:01:23 (xfr#123, to-chk=456/789)"
        "progress": re.compile(r'(\d+)\s+(\d+)%\s+([\d.]+)(MB|KB|GB)/s\s+(\d+):(\d+):(\d+)'),

        # File transfer line: "filename"
        "file_transfer": re.compile(r'^([^/\s][^\n\r]*?)$'),

        # Total size line from rsync: "Total transferred file size: 1,234,567 bytes"
        "total_size": re.compile(r'Total transferred file size: ([\d,]+) bytes')
    }

    @staticmethod
    def parse_progress_line(line: str) -> Dict[str, Any]:
        """
        Parse a line of rsync progress output.

        Args:
            line: Line of rsync output

        Returns:
            Dictionary with parsed progress information
        """
        line = line.strip()
        if not line:
            return {}

        # Try progress2 format first
        match = RsyncProgressParser.PROGRESS_PATTERNS["progress2"].search(line)
        if match:
            transferred_str, percent, rate_str, rate_unit, hours, minutes, seconds = match.groups()

            # Parse transferred bytes
            transferred_bytes = int(transferred_str.replace(',', ''))

            # Parse transfer rate
            rate_multiplier = {"KB": 1024, "MB": 1024**2, "GB": 1024**3}
            transfer_rate_bps = float(rate_str) * rate_multiplier.get(rate_unit, 1)

            # Parse time
            total_seconds = int(hours) * 3600 + int(minutes) * 60 + int(seconds)

            return {
                "type": "progress",
                "transferred_bytes": transferred_bytes,
                "progress_percent": int(percent),
                "transfer_rate_bps": transfer_rate_bps,
                "elapsed_seconds": total_seconds
            }

        # Try standard progress format
        match = RsyncProgressParser.PROGRESS_PATTERNS["progress"].search(line)
        if match:
            transferred_str, percent, rate_str, rate_unit, hours, minutes, seconds = match.groups()

            transferred_bytes = int(transferred_str)
            rate_multiplier = {"KB": 1024, "MB": 1024**2, "GB": 1024**3}
            transfer_rate_bps = float(rate_str) * rate_multiplier.get(rate_unit, 1)
            total_seconds = int(hours) * 3600 + int(minutes) * 60 + int(seconds)

            return {
                "type": "progress",
                "transferred_bytes": transferred_bytes,
                "progress_percent": int(percent),
                "transfer_rate_bps": transfer_rate_bps,
                "elapsed_seconds": total_seconds
            }

        # Check for file transfer lines
        if "/" in line and not line.startswith("Total"):
            return {
                "type": "file",
                "current_file": line.strip()
            }

        # Check for total size information
        match = RsyncProgressParser.PROGRESS_PATTERNS["total_size"].search(line)
        if match:
            total_bytes = int(match.group(1).replace(',', ''))
            return {
                "type": "total_size",
                "total_bytes": total_bytes
            }

        return {}


class EnhancedProgressReporter:
    """Enhanced progress reporting for rsync operations across all performance modes."""

    def __init__(self, logger=None, update_interval: float = 1.0):
        """
        Initialize enhanced progress reporter.

        Args:
            logger: Logger instance for progress reporting
            update_interval: Interval between progress updates in seconds
        """
        self.logger = logger
        self.update_interval = update_interval
        self.active_transfers: Dict[str, TransferProgress] = {}
        self.progress_callbacks: List[Callable[[TransferProgress], None]] = []
        self._lock = threading.Lock()

    def register_progress_callback(self, callback: Callable[[TransferProgress], None]):
        """Register a callback function for progress updates."""
        self.progress_callbacks.append(callback)

    def start_transfer_monitoring(self, transfer_id: str, source: Path, destination: Path,
                                total_bytes: int = 0) -> TransferProgress:
        """
        Start monitoring a transfer operation.

        Args:
            transfer_id: Unique identifier for the transfer
            source: Source path
            destination: Destination path
            total_bytes: Total bytes to transfer (if known)

        Returns:
            TransferProgress instance for the transfer
        """
        with self._lock:
            progress = TransferProgress(
                transfer_id=transfer_id,
                source_path=source,
                destination_path=destination,
                total_bytes=total_bytes,
                phase=ProgressPhase.STARTING
            )

            self.active_transfers[transfer_id] = progress

            if self.logger:
                self.logger.info(f"Started monitoring transfer {transfer_id}: {source} -> {destination}")

            return progress

    def update_transfer_progress(self, transfer_id: str, **kwargs) -> Optional[TransferProgress]:
        """
        Update progress for a transfer.

        Args:
            transfer_id: Transfer identifier
            **kwargs: Progress update parameters

        Returns:
            Updated TransferProgress instance or None if not found
        """
        with self._lock:
            if transfer_id not in self.active_transfers:
                return None

            progress = self.active_transfers[transfer_id]
            progress.update_progress(**kwargs)

            # Notify callbacks
            for callback in self.progress_callbacks:
                try:
                    callback(progress)
                except Exception as e:
                    if self.logger:
                        self.logger.warning(f"Progress callback failed: {e}")

            return progress

    def complete_transfer(self, transfer_id: str, success: bool = True, error_message: str = None):
        """
        Mark a transfer as completed.

        Args:
            transfer_id: Transfer identifier
            success: Whether the transfer succeeded
            error_message: Error message if transfer failed
        """
        with self._lock:
            if transfer_id not in self.active_transfers:
                return

            progress = self.active_transfers[transfer_id]
            progress.phase = ProgressPhase.COMPLETED if success else ProgressPhase.FAILED
            progress.error_message = error_message

            if self.logger:
                if success:
                    self.logger.info(f"Transfer {transfer_id} completed successfully")
                else:
                    self.logger.error(f"Transfer {transfer_id} failed: {error_message}")

            # Keep completed transfers for a short time for final reporting
            # In a real implementation, you might clean these up after some time

    def get_transfer_progress(self, transfer_id: str) -> Optional[TransferProgress]:
        """Get current progress for a transfer."""
        with self._lock:
            return self.active_transfers.get(transfer_id)

    def get_all_active_transfers(self) -> List[TransferProgress]:
        """Get all currently active transfers."""
        with self._lock:
            return [p for p in self.active_transfers.values()
                   if p.phase in [ProgressPhase.STARTING, ProgressPhase.TRANSFERRING, ProgressPhase.VERIFYING]]

    def get_overall_progress(self) -> Dict[str, Any]:
        """Get overall progress across all active transfers."""
        with self._lock:
            active_transfers = self.get_all_active_transfers()

            if not active_transfers:
                return {
                    "active_transfers": 0,
                    "total_bytes": 0,
                    "transferred_bytes": 0,
                    "overall_progress_percent": 0.0,
                    "average_transfer_rate_mbps": 0.0
                }

            total_bytes = sum(t.total_bytes for t in active_transfers)
            transferred_bytes = sum(t.transferred_bytes for t in active_transfers)
            total_rate = sum(t.transfer_rate_bps for t in active_transfers)

            return {
                "active_transfers": len(active_transfers),
                "total_bytes": total_bytes,
                "transferred_bytes": transferred_bytes,
                "overall_progress_percent": (transferred_bytes / total_bytes * 100.0) if total_bytes > 0 else 0.0,
                "average_transfer_rate_mbps": total_rate / (1024 * 1024)
            }

    def format_progress_summary(self) -> str:
        """Format a summary of all active transfers."""
        overall = self.get_overall_progress()

        if overall["active_transfers"] == 0:
            return "No active transfers"

        return (f"Active: {overall['active_transfers']} transfers | "
                f"Overall: {overall['overall_progress_percent']:.1f}% | "
                f"Rate: {overall['average_transfer_rate_mbps']:.1f} MB/s")


class RsyncProgressMonitor:
    """Real-time monitor for rsync process progress output."""

    def __init__(self, progress_reporter: EnhancedProgressReporter):
        """
        Initialize rsync progress monitor.

        Args:
            progress_reporter: Progress reporter to update with parsed information
        """
        self.progress_reporter = progress_reporter
        self.parser = RsyncProgressParser()

    def monitor_rsync_process(self, process: subprocess.Popen, transfer_id: str) -> bool:
        """
        Monitor rsync process output and update progress.

        Args:
            process: Running rsync subprocess
            transfer_id: Transfer identifier

        Returns:
            True if monitoring completed successfully
        """
        try:
            # Read stderr for progress information (rsync outputs progress to stderr)
            while True:
                line = process.stderr.readline()
                if not line:
                    break

                line = line.decode('utf-8', errors='replace').strip()
                if not line:
                    continue

                # Parse progress information
                progress_info = self.parser.parse_progress_line(line)
                if progress_info:
                    self._update_progress_from_parsed(transfer_id, progress_info)

            # Wait for process completion
            return_code = process.wait()
            return return_code == 0

        except Exception as e:
            if self.progress_reporter.logger:
                self.progress_reporter.logger.error(f"Error monitoring rsync progress: {e}")
            return False

    def _update_progress_from_parsed(self, transfer_id: str, progress_info: Dict[str, Any]):
        """Update progress reporter with parsed information."""
        if progress_info["type"] == "progress":
            self.progress_reporter.update_transfer_progress(
                transfer_id,
                transferred_bytes=progress_info.get("transferred_bytes"),
                transfer_rate_bps=progress_info.get("transfer_rate_bps"),
                phase=ProgressPhase.TRANSFERRING
            )
        elif progress_info["type"] == "file":
            self.progress_reporter.update_transfer_progress(
                transfer_id,
                current_file=progress_info.get("current_file")
            )
        elif progress_info["type"] == "total_size":
            # Update total bytes if we didn't have it before
            progress = self.progress_reporter.get_transfer_progress(transfer_id)
            if progress and progress.total_bytes == 0:
                progress.total_bytes = progress_info.get("total_bytes", 0)


# Utility functions for integration
def create_progress_reporter(logger=None, update_interval: float = 1.0) -> EnhancedProgressReporter:
    """Create a configured progress reporter instance."""
    return EnhancedProgressReporter(logger=logger, update_interval=update_interval)


def create_progress_monitor(progress_reporter: EnhancedProgressReporter) -> RsyncProgressMonitor:
    """Create a progress monitor instance."""
    return RsyncProgressMonitor(progress_reporter)


# Example usage
if __name__ == "__main__":
    import logging

    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    # Create progress reporter
    reporter = create_progress_reporter(logger)

    # Add a simple progress callback
    def print_progress(progress: TransferProgress):
        print(f"Transfer {progress.transfer_id}: {progress.format_status_line()}")

    reporter.register_progress_callback(print_progress)

    # Simulate a transfer
    transfer_id = "test_transfer_1"
    source = Path("/mnt/disk1/test")
    dest = Path("/mnt/disk2/test")

    progress = reporter.start_transfer_monitoring(transfer_id, source, dest, total_bytes=1000000)

    # Simulate progress updates
    for i in range(1, 11):
        time.sleep(0.1)
        reporter.update_transfer_progress(
            transfer_id,
            transferred_bytes=i * 100000,
            transfer_rate_bps=100000,  # 100 KB/s
            current_file=f"file_{i}.txt"
        )

    reporter.complete_transfer(transfer_id, success=True)
    print("Transfer completed!")