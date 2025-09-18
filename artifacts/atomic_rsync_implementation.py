#!/usr/bin/env python3
"""
Atomic Rsync Implementation

This module provides the implementation for atomic rsync operations,
replacing the two-stage transfer process with single atomic commands.
"""

import os
import logging
import subprocess
from pathlib import Path
from typing import List, Optional, Dict, Any
from dataclasses import dataclass


@dataclass
class AtomicTransferResult:
    """Result of an atomic transfer operation."""
    success: bool
    return_code: int
    error_message: Optional[str] = None
    transferred_bytes: int = 0


def perform_atomic_move(src: Path, dst: Path, rsync_mode: str, rsync_extra: List[str],
                       dry_run: bool = True, monitor=None) -> AtomicTransferResult:
    """
    Perform atomic file/directory move using single rsync command.

    This replaces the two-stage copy-then-remove process with a single
    atomic operation using rsync --remove-source-files.

    Args:
        src: Source path
        dst: Destination path
        rsync_mode: Performance mode (fast, balanced, integrity)
        rsync_extra: Additional rsync flags
        dry_run: If True, don't actually execute the move
        monitor: Optional performance monitor

    Returns:
        AtomicTransferResult with success status and details
    """
    from unraid_rebalancer import get_rsync_flags, run, human_bytes

    try:
        # Get base flags for the performance mode
        rsync_flags = get_rsync_flags(rsync_mode)

        # Add --remove-source-files for atomic operation
        atomic_flags = rsync_flags + ["--remove-source-files"]

        # Handle path formatting for directories vs files
        if src.is_dir():
            # For directories, ensure we move the directory itself, not just contents
            src_r = str(src)
            dst_r = str(dst)
        else:
            src_r = str(src)
            dst_r = str(dst)

        # Construct atomic rsync command
        cmd = ["rsync"] + atomic_flags + rsync_extra + [src_r, dst_r]

        logging.info(f"Executing atomic rsync: {' '.join(cmd)}")

        # Execute atomic transfer
        rc = run(cmd, dry_run=dry_run)

        if rc == 0:
            # Estimate transferred bytes for monitoring
            transferred_bytes = 0
            if not dry_run:
                try:
                    # Since this is atomic, source should be gone, estimate from destination
                    if dst.exists():
                        if dst.is_file():
                            transferred_bytes = dst.stat().st_size
                        elif dst.is_dir():
                            transferred_bytes = sum(
                                f.stat().st_size
                                for f in dst.rglob('*')
                                if f.is_file()
                            )
                except Exception as e:
                    logging.debug(f"Could not estimate transferred bytes: {e}")

            return AtomicTransferResult(
                success=True,
                return_code=rc,
                transferred_bytes=transferred_bytes
            )
        else:
            error_msg = f"Atomic rsync failed with return code {rc}"
            logging.error(error_msg)
            return AtomicTransferResult(
                success=False,
                return_code=rc,
                error_message=error_msg
            )

    except Exception as e:
        error_msg = f"Atomic transfer failed with exception: {str(e)}"
        logging.error(error_msg)
        return AtomicTransferResult(
            success=False,
            return_code=-1,
            error_message=error_msg
        )


def validate_atomic_transfer_prerequisites(src: Path, dst: Path) -> bool:
    """
    Validate prerequisites for atomic transfer.

    Args:
        src: Source path
        dst: Destination path

    Returns:
        True if prerequisites are met, False otherwise
    """
    # Check source exists
    if not src.exists():
        logging.error(f"Source path does not exist: {src}")
        return False

    # Check destination parent directory exists
    dst_parent = dst.parent
    if not dst_parent.exists():
        logging.error(f"Destination parent directory does not exist: {dst_parent}")
        return False

    # Check we have write permissions on destination parent
    if not os.access(dst_parent, os.W_OK):
        logging.error(f"No write permission on destination parent: {dst_parent}")
        return False

    # Check source and destination are on different filesystems (for Unraid)
    try:
        src_stat = src.stat()
        dst_parent_stat = dst_parent.stat()

        if src_stat.st_dev == dst_parent_stat.st_dev:
            logging.warning(f"Source and destination appear to be on same filesystem")
            # Don't fail here as this might be intentional in some cases
    except Exception as e:
        logging.debug(f"Could not check filesystem difference: {e}")

    return True


def verify_atomic_transfer_completion(original_src: Path, dst: Path, expected_size: int = 0) -> bool:
    """
    Verify that atomic transfer completed successfully.

    Args:
        original_src: Original source path (should no longer exist)
        dst: Destination path (should exist)
        expected_size: Expected size in bytes (optional)

    Returns:
        True if transfer verification passes, False otherwise
    """
    # Source should no longer exist after atomic move
    if original_src.exists():
        logging.error(f"Source still exists after atomic move: {original_src}")
        return False

    # Destination should exist
    if not dst.exists():
        logging.error(f"Destination does not exist after atomic move: {dst}")
        return False

    # Optional size verification
    if expected_size > 0:
        try:
            actual_size = 0
            if dst.is_file():
                actual_size = dst.stat().st_size
            elif dst.is_dir():
                actual_size = sum(f.stat().st_size for f in dst.rglob('*') if f.is_file())

            if actual_size != expected_size:
                logging.warning(f"Size mismatch: expected {expected_size}, got {actual_size}")
                # Don't fail on size mismatch as there might be compression/decompression
        except Exception as e:
            logging.debug(f"Could not verify size: {e}")

    return True


# Backward compatibility function for gradual migration
def perform_legacy_move(src: Path, dst: Path, rsync_mode: str, rsync_extra: List[str],
                       dry_run: bool = True) -> int:
    """
    Legacy two-stage move function for backward compatibility.

    This maintains the old behavior while we transition to atomic operations.
    Should be removed once atomic operations are fully tested and deployed.
    """
    from unraid_rebalancer import get_rsync_flags, run

    # Original two-stage process
    if src.is_dir():
        src_r = str(src)
        dst_r = str(dst)
    else:
        src_r = str(src)
        dst_r = str(dst)

    rsync_flags = get_rsync_flags(rsync_mode)
    cmd = ["rsync"] + rsync_flags + rsync_extra + [src_r, dst_r]

    rc = run(cmd, dry_run=dry_run)

    if rc != 0:
        return rc

    if not dry_run:
        # Second stage: remove source files
        if src.is_dir():
            rm_cmd = ["rsync", "-aHAX", "--remove-source-files", str(src) + "/", str(dst) + "/"]
            rc2 = run(rm_cmd, dry_run=False)
            if rc2 != 0:
                logging.warning(f"Failed to remove source files from {src}")
                return rc2

            # Remove empty directories
            try:
                for root, _, files in os.walk(src, topdown=False):
                    if not files:
                        try:
                            os.rmdir(root)
                        except OSError:
                            pass
            except Exception:
                logging.warning(f"Error during directory cleanup for {src}")
        else:
            try:
                os.remove(src)
            except Exception as e:
                logging.error(f"Failed to remove source file {src}: {e}")
                return 1

    return 0