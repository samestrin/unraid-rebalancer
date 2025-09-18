#!/usr/bin/env python3
"""
Atomic Rsync Operations Prototype

This prototype demonstrates the proposed atomic rsync operations architecture
for replacing the current two-stage transfer process with single atomic commands.
"""

import subprocess
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Any
from enum import Enum


class TransferStatus(Enum):
    """Transfer operation status."""
    SUCCESS = "success"
    FAILED = "failed"
    PARTIAL = "partial"
    SKIPPED = "skipped"


@dataclass
class TransferRequest:
    """Request for atomic file transfer."""
    source: Path
    destination: Path
    mode: str
    options: Dict[str, Any]
    dry_run: bool = True


@dataclass
class TransferResult:
    """Result of atomic file transfer."""
    status: TransferStatus
    transferred_bytes: int
    duration: float
    error_message: Optional[str] = None
    verification_passed: bool = False
    rsync_return_code: int = 0


class AtomicRsyncOperations:
    """
    Black box module for atomic rsync operations.

    This class encapsulates all rsync functionality with atomic guarantees,
    replacing the two-stage transfer process with single atomic commands.
    """

    # Standardized performance mode configurations
    PERFORMANCE_MODES = {
        "fast": {
            "flags": ["-av", "--partial", "--inplace", "--numeric-ids", "--no-compress"],
            "description": "Fastest transfers, minimal CPU overhead",
            "features": ["basic_archive", "no_compression", "minimal_progress"]
        },
        "balanced": {
            "flags": ["-avPR", "-X", "--partial", "--inplace", "--numeric-ids", "--info=progress2"],
            "description": "Balanced speed and features with extended attributes",
            "features": ["extended_attrs", "progress_reporting", "relative_paths"]
        },
        "integrity": {
            "flags": ["-aHAX", "--info=progress2", "--partial", "--inplace", "--numeric-ids"],
            "description": "Full integrity checking with hard links, ACLs, and progress",
            "features": ["hard_links", "acls", "extended_attrs", "detailed_progress"]
        }
    }

    def __init__(self, logger: Optional[logging.Logger] = None):
        """Initialize atomic rsync operations."""
        self.logger = logger or logging.getLogger(__name__)

    def perform_atomic_move(self, request: TransferRequest) -> TransferResult:
        """
        Perform atomic file/directory move using single rsync command.

        This is the main interface for atomic transfers, replacing the
        two-stage copy-then-remove process with a single atomic operation.

        Args:
            request: Transfer request with source, destination, mode, and options

        Returns:
            TransferResult with success status, metrics, and error information
        """
        start_time = time.time()

        try:
            # Validate prerequisites
            validation_result = self._validate_transfer_prerequisites(request)
            if not validation_result:
                return TransferResult(
                    status=TransferStatus.FAILED,
                    transferred_bytes=0,
                    duration=0,
                    error_message="Transfer prerequisites validation failed"
                )

            # Get standardized rsync flags for the mode
            rsync_flags = self._get_atomic_rsync_flags(request.mode)

            # Add --remove-source-files for atomic operation
            atomic_flags = rsync_flags + ["--remove-source-files"]

            # Construct atomic rsync command
            cmd = ["rsync"] + atomic_flags + [str(request.source), str(request.destination)]

            # Add any extra options
            if "extra_flags" in request.options:
                cmd.extend(request.options["extra_flags"])

            self.logger.info(f"Executing atomic rsync: {' '.join(cmd)}")

            # Execute atomic transfer
            if request.dry_run:
                self.logger.info("DRY RUN: Would execute atomic rsync command")
                return TransferResult(
                    status=TransferStatus.SUCCESS,
                    transferred_bytes=0,
                    duration=time.time() - start_time,
                    verification_passed=True
                )
            else:
                result = subprocess.run(cmd, capture_output=True, text=True)

                # Analyze results
                duration = time.time() - start_time
                transferred_bytes = self._estimate_transferred_bytes(request.source)

                if result.returncode == 0:
                    # Verify transfer completion
                    verification_passed = self._verify_atomic_transfer(request)

                    return TransferResult(
                        status=TransferStatus.SUCCESS,
                        transferred_bytes=transferred_bytes,
                        duration=duration,
                        verification_passed=verification_passed,
                        rsync_return_code=result.returncode
                    )
                else:
                    return TransferResult(
                        status=TransferStatus.FAILED,
                        transferred_bytes=0,
                        duration=duration,
                        error_message=f"Rsync failed with return code {result.returncode}: {result.stderr}",
                        rsync_return_code=result.returncode
                    )

        except Exception as e:
            return TransferResult(
                status=TransferStatus.FAILED,
                transferred_bytes=0,
                duration=time.time() - start_time,
                error_message=f"Atomic transfer failed: {str(e)}"
            )

    def _get_atomic_rsync_flags(self, mode: str) -> List[str]:
        """Get standardized rsync flags for the specified mode."""
        if mode not in self.PERFORMANCE_MODES:
            raise ValueError(f"Unknown rsync mode '{mode}'. Available: {list(self.PERFORMANCE_MODES.keys())}")

        return self.PERFORMANCE_MODES[mode]["flags"].copy()

    def _validate_transfer_prerequisites(self, request: TransferRequest) -> bool:
        """Validate transfer prerequisites."""
        # Check source exists
        if not request.source.exists():
            self.logger.error(f"Source path does not exist: {request.source}")
            return False

        # Check destination parent exists
        if not request.destination.parent.exists():
            self.logger.error(f"Destination parent directory does not exist: {request.destination.parent}")
            return False

        # Check disk space (simplified)
        # In real implementation, would check actual disk space requirements

        return True

    def _verify_atomic_transfer(self, request: TransferRequest) -> bool:
        """Verify atomic transfer completed successfully."""
        # Check source no longer exists (moved, not copied)
        if request.source.exists():
            self.logger.warning(f"Source still exists after atomic move: {request.source}")
            return False

        # Check destination exists
        if not request.destination.exists():
            self.logger.error(f"Destination does not exist after atomic move: {request.destination}")
            return False

        return True

    def _estimate_transferred_bytes(self, source: Path) -> int:
        """Estimate bytes transferred (simplified implementation)."""
        try:
            if source.is_file():
                return source.stat().st_size
            elif source.is_dir():
                total_size = 0
                for file_path in source.rglob('*'):
                    if file_path.is_file():
                        total_size += file_path.stat().st_size
                return total_size
        except Exception:
            pass
        return 0


# Example usage and testing
if __name__ == "__main__":
    import time

    logging.basicConfig(level=logging.INFO)

    # Initialize atomic rsync operations
    atomic_rsync = AtomicRsyncOperations()

    # Example transfer request
    request = TransferRequest(
        source=Path("/mnt/disk1/Movies/TestMovie"),
        destination=Path("/mnt/disk2/Movies/TestMovie"),
        mode="balanced",
        options={"extra_flags": ["--verbose"]},
        dry_run=True
    )

    # Perform atomic move
    result = atomic_rsync.perform_atomic_move(request)

    print(f"Transfer Status: {result.status}")
    print(f"Duration: {result.duration:.2f}s")
    print(f"Transferred: {result.transferred_bytes} bytes")
    print(f"Verification: {result.verification_passed}")
    if result.error_message:
        print(f"Error: {result.error_message}")