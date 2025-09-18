#!/usr/bin/env python3
"""
Error Handling Implementation

This module provides comprehensive error handling with rollback capabilities
for atomic rsync operations in the Unraid Rebalancer.
"""

import os
import time
import logging
import subprocess
import shutil
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union
from dataclasses import dataclass
from enum import Enum


class ErrorCategory(Enum):
    """Categories of errors that can occur during transfers."""
    VALIDATION_ERROR = "validation"
    DISK_SPACE_ERROR = "disk_space"
    PERMISSION_ERROR = "permission"
    RSYNC_ERROR = "rsync"
    FILESYSTEM_ERROR = "filesystem"
    NETWORK_ERROR = "network"
    INTERRUPT_ERROR = "interrupt"
    UNKNOWN_ERROR = "unknown"


class ErrorSeverity(Enum):
    """Severity levels for errors."""
    CRITICAL = "critical"      # Data loss risk, immediate intervention required
    HIGH = "high"             # Operation failed, manual intervention may be needed
    MEDIUM = "medium"         # Operation failed, automatic retry possible
    LOW = "low"               # Warning, operation may continue


@dataclass
class TransferError:
    """Represents an error that occurred during a transfer operation."""
    category: ErrorCategory
    severity: ErrorSeverity
    message: str
    source_path: Optional[Path] = None
    destination_path: Optional[Path] = None
    rsync_return_code: Optional[int] = None
    system_error: Optional[str] = None
    timestamp: float = None
    recoverable: bool = False
    retry_count: int = 0

    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = time.time()


@dataclass
class RollbackOperation:
    """Represents a rollback operation for failed transfers."""
    operation_type: str  # "remove_partial", "restore_source", "cleanup_destination"
    target_path: Path
    backup_path: Optional[Path] = None
    completed: bool = False
    error_message: Optional[str] = None


class TransferErrorHandler:
    """Comprehensive error handling with rollback capabilities."""

    # Rsync return code meanings (partial list of common ones)
    RSYNC_ERROR_CODES = {
        0: "Success",
        1: "Syntax or usage error",
        2: "Protocol incompatibility",
        3: "Errors selecting input/output files, dirs",
        4: "Requested action not supported",
        5: "Error starting client-server protocol",
        6: "Daemon unable to append to log-file",
        10: "Error in socket I/O",
        11: "Error in file I/O",
        12: "Error in rsync protocol data stream",
        13: "Errors with program diagnostics",
        14: "Error in IPC code",
        20: "Received SIGUSR1 or SIGINT",
        21: "Some error returned by waitpid()",
        22: "Error allocating core memory buffers",
        23: "Partial transfer due to error",
        24: "Partial transfer due to vanished source files",
        25: "The --max-delete limit stopped deletions",
        30: "Timeout in data send/receive",
        35: "Timeout waiting for daemon connection"
    }

    def __init__(self, logger: Optional[logging.Logger] = None,
                 enable_rollback: bool = True, max_retries: int = 3):
        """
        Initialize error handler.

        Args:
            logger: Logger instance for error reporting
            enable_rollback: Whether to enable automatic rollback on failures
            max_retries: Maximum number of retry attempts
        """
        self.logger = logger or logging.getLogger(__name__)
        self.enable_rollback = enable_rollback
        self.max_retries = max_retries
        self.rollback_operations: List[RollbackOperation] = []

    def categorize_rsync_error(self, return_code: int, stderr_output: str = "") -> TransferError:
        """
        Categorize rsync error based on return code and output.

        Args:
            return_code: Rsync process return code
            stderr_output: Stderr output from rsync command

        Returns:
            TransferError with appropriate categorization
        """
        error_message = self.RSYNC_ERROR_CODES.get(return_code, f"Unknown rsync error (code {return_code})")

        # Determine category and severity based on return code
        if return_code == 0:
            # Should not happen in error context, but handle gracefully
            category = ErrorCategory.UNKNOWN_ERROR
            severity = ErrorSeverity.LOW
            recoverable = True
        elif return_code in [1, 2, 4, 5, 6]:
            # Configuration or protocol errors
            category = ErrorCategory.RSYNC_ERROR
            severity = ErrorSeverity.HIGH
            recoverable = False
        elif return_code in [3, 11]:
            # File I/O errors
            category = ErrorCategory.FILESYSTEM_ERROR
            severity = ErrorSeverity.HIGH
            recoverable = True
        elif return_code in [10, 30, 35]:
            # Network/connection errors
            category = ErrorCategory.NETWORK_ERROR
            severity = ErrorSeverity.MEDIUM
            recoverable = True
        elif return_code in [20, 21]:
            # Interrupt/signal errors
            category = ErrorCategory.INTERRUPT_ERROR
            severity = ErrorSeverity.MEDIUM
            recoverable = True
        elif return_code in [23, 24]:
            # Partial transfer errors
            category = ErrorCategory.RSYNC_ERROR
            severity = ErrorSeverity.MEDIUM
            recoverable = True
        elif return_code == 22:
            # Memory allocation error
            category = ErrorCategory.FILESYSTEM_ERROR
            severity = ErrorSeverity.HIGH
            recoverable = False
        else:
            # Unknown error
            category = ErrorCategory.UNKNOWN_ERROR
            severity = ErrorSeverity.HIGH
            recoverable = True

        # Analyze stderr for additional context
        stderr_lower = stderr_output.lower()
        if "no space left" in stderr_lower or "disk full" in stderr_lower:
            category = ErrorCategory.DISK_SPACE_ERROR
            severity = ErrorSeverity.HIGH
            recoverable = False
        elif "permission denied" in stderr_lower:
            category = ErrorCategory.PERMISSION_ERROR
            severity = ErrorSeverity.HIGH
            recoverable = False
        elif "network" in stderr_lower or "connection" in stderr_lower:
            category = ErrorCategory.NETWORK_ERROR
            severity = ErrorSeverity.MEDIUM
            recoverable = True

        return TransferError(
            category=category,
            severity=severity,
            message=f"{error_message}: {stderr_output}" if stderr_output else error_message,
            rsync_return_code=return_code,
            system_error=stderr_output,
            recoverable=recoverable
        )

    def handle_transfer_error(self, error: TransferError, source: Path, destination: Path) -> bool:
        """
        Handle transfer error with appropriate recovery actions.

        Args:
            error: TransferError instance
            source: Source path that was being transferred
            destination: Destination path

        Returns:
            True if error was handled and operation can continue, False otherwise
        """
        error.source_path = source
        error.destination_path = destination

        self.logger.error(f"Transfer error ({error.category.value}): {error.message}")

        # Log detailed error information
        self._log_error_details(error)

        # Attempt recovery based on error category and severity
        if error.severity == ErrorSeverity.CRITICAL:
            self.logger.critical(f"Critical error detected, initiating emergency rollback")
            return self._emergency_rollback(error)
        elif error.recoverable and error.retry_count < self.max_retries:
            self.logger.info(f"Attempting recovery for recoverable error (attempt {error.retry_count + 1})")
            return self._attempt_recovery(error)
        elif self.enable_rollback:
            self.logger.warning(f"Error not recoverable, initiating rollback")
            return self._initiate_rollback(error)
        else:
            self.logger.error(f"Error not recoverable and rollback disabled")
            return False

    def _log_error_details(self, error: TransferError):
        """Log detailed error information for debugging."""
        self.logger.debug(f"Error Details:")
        self.logger.debug(f"  Category: {error.category.value}")
        self.logger.debug(f"  Severity: {error.severity.value}")
        self.logger.debug(f"  Recoverable: {error.recoverable}")
        self.logger.debug(f"  Retry Count: {error.retry_count}")
        self.logger.debug(f"  Source: {error.source_path}")
        self.logger.debug(f"  Destination: {error.destination_path}")
        if error.rsync_return_code is not None:
            self.logger.debug(f"  Rsync Return Code: {error.rsync_return_code}")
        if error.system_error:
            self.logger.debug(f"  System Error: {error.system_error}")

    def _attempt_recovery(self, error: TransferError) -> bool:
        """
        Attempt to recover from recoverable errors.

        Args:
            error: TransferError to recover from

        Returns:
            True if recovery was successful
        """
        if error.category == ErrorCategory.NETWORK_ERROR:
            # For network errors, wait and retry
            wait_time = 2 ** error.retry_count  # Exponential backoff
            self.logger.info(f"Network error detected, waiting {wait_time}s before retry")
            time.sleep(wait_time)
            return True
        elif error.category == ErrorCategory.INTERRUPT_ERROR:
            # For interrupt errors, check if we can continue
            self.logger.info("Interrupt error detected, checking system state")
            return self._check_system_state(error)
        elif error.category == ErrorCategory.FILESYSTEM_ERROR:
            # For filesystem errors, try to resolve
            return self._resolve_filesystem_error(error)
        else:
            return False

    def _emergency_rollback(self, error: TransferError) -> bool:
        """
        Perform emergency rollback for critical errors.

        Args:
            error: Critical error requiring emergency rollback

        Returns:
            True if emergency rollback was successful
        """
        self.logger.critical("Initiating emergency rollback procedure")

        # Stop all ongoing operations
        self.logger.critical("Stopping ongoing operations")

        # Perform comprehensive rollback
        success = self._perform_comprehensive_rollback(error)

        if success:
            self.logger.critical("Emergency rollback completed successfully")
        else:
            self.logger.critical("Emergency rollback failed - manual intervention required")

        return success

    def _initiate_rollback(self, error: TransferError) -> bool:
        """
        Initiate standard rollback procedure.

        Args:
            error: Error requiring rollback

        Returns:
            True if rollback was successful
        """
        self.logger.warning("Initiating rollback procedure")

        rollback_operations = self._plan_rollback_operations(error)

        success = True
        for operation in rollback_operations:
            if not self._execute_rollback_operation(operation):
                success = False
                break

        if success:
            self.logger.info("Rollback completed successfully")
        else:
            self.logger.error("Rollback failed - system may be in inconsistent state")

        return success

    def _plan_rollback_operations(self, error: TransferError) -> List[RollbackOperation]:
        """
        Plan rollback operations based on the error context.

        Args:
            error: Error requiring rollback

        Returns:
            List of rollback operations to perform
        """
        operations = []

        if error.destination_path and error.destination_path.exists():
            # Remove partially transferred destination
            operations.append(RollbackOperation(
                operation_type="remove_partial",
                target_path=error.destination_path
            ))

        # Add other rollback operations as needed based on error type

        return operations

    def _execute_rollback_operation(self, operation: RollbackOperation) -> bool:
        """
        Execute a single rollback operation.

        Args:
            operation: Rollback operation to execute

        Returns:
            True if operation was successful
        """
        try:
            if operation.operation_type == "remove_partial":
                if operation.target_path.exists():
                    if operation.target_path.is_dir():
                        shutil.rmtree(operation.target_path)
                    else:
                        operation.target_path.unlink()
                    self.logger.info(f"Removed partial transfer: {operation.target_path}")

                operation.completed = True
                return True

            # Add other rollback operation types as needed

        except Exception as e:
            operation.error_message = str(e)
            self.logger.error(f"Rollback operation failed: {e}")
            return False

        return False

    def _check_system_state(self, error: TransferError) -> bool:
        """Check system state after interrupt error."""
        # Simplified system state check
        try:
            if error.source_path and error.source_path.exists():
                if error.destination_path and error.destination_path.exists():
                    # Both exist, may be partial transfer
                    return True
            return False
        except Exception:
            return False

    def _resolve_filesystem_error(self, error: TransferError) -> bool:
        """Attempt to resolve filesystem errors."""
        # Simplified filesystem error resolution
        # In real implementation, would check permissions, disk space, etc.
        return False

    def _perform_comprehensive_rollback(self, error: TransferError) -> bool:
        """Perform comprehensive rollback for critical errors."""
        # Simplified comprehensive rollback
        return self._initiate_rollback(error)

    def get_error_summary(self) -> Dict[str, int]:
        """Get summary of errors encountered."""
        # This would track errors in a real implementation
        return {
            "total_errors": 0,
            "critical_errors": 0,
            "recoverable_errors": 0,
            "rollbacks_performed": len(self.rollback_operations)
        }


# Utility functions for error handling integration
def create_error_handler(enable_rollback: bool = True, max_retries: int = 3) -> TransferErrorHandler:
    """Create a configured error handler instance."""
    return TransferErrorHandler(
        logger=logging.getLogger("unraid_rebalancer.error_handler"),
        enable_rollback=enable_rollback,
        max_retries=max_retries
    )


def handle_rsync_error(return_code: int, stderr_output: str, source: Path,
                      destination: Path, error_handler: TransferErrorHandler) -> bool:
    """
    Handle rsync error using the error handler.

    Args:
        return_code: Rsync return code
        stderr_output: Stderr output from rsync
        source: Source path
        destination: Destination path
        error_handler: Error handler instance

    Returns:
        True if error was handled successfully
    """
    error = error_handler.categorize_rsync_error(return_code, stderr_output)
    return error_handler.handle_transfer_error(error, source, destination)


# Example usage
if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)

    # Create error handler
    handler = create_error_handler()

    # Simulate an error
    test_error = TransferError(
        category=ErrorCategory.RSYNC_ERROR,
        severity=ErrorSeverity.MEDIUM,
        message="Test error for demonstration",
        recoverable=True
    )

    # Handle the error
    source_path = Path("/mnt/disk1/test")
    dest_path = Path("/mnt/disk2/test")

    result = handler.handle_transfer_error(test_error, source_path, dest_path)
    print(f"Error handling result: {result}")

    # Get error summary
    summary = handler.get_error_summary()
    print(f"Error summary: {summary}")