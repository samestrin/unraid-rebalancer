#!/usr/bin/env python3
"""
Drive Performance Models for Unraid Rebalancer

Provides generic performance models for different drive types to enable
initial ETA estimates and performance-based optimizations.
"""

from typing import Dict, Any

# Drive performance models based on typical hardware specifications
DRIVE_PERFORMANCE_MODELS: Dict[str, Dict[str, Any]] = {
    "7200_rpm_sata": {
        "sequential_read_mbps": 150,
        "sequential_write_mbps": 140,
        "random_read_mbps": 80,
        "random_write_mbps": 75,
        "description": "Typical 7200 RPM SATA drive performance",
        "typical_use": "General storage, media files",
        "reliability_factor": 0.85  # Conservative factor for real-world performance
    },
    "5400_rpm_sata": {
        "sequential_read_mbps": 100,
        "sequential_write_mbps": 95,
        "random_read_mbps": 50,
        "random_write_mbps": 45,
        "description": "Typical 5400 RPM SATA drive performance",
        "typical_use": "Archival storage, lower power consumption",
        "reliability_factor": 0.80
    },
    "ssd": {
        "sequential_read_mbps": 500,
        "sequential_write_mbps": 450,
        "random_read_mbps": 400,
        "random_write_mbps": 350,
        "description": "Typical SSD performance",
        "typical_use": "Cache drives, high-performance storage",
        "reliability_factor": 0.90
    },
    "nvme": {
        "sequential_read_mbps": 3000,
        "sequential_write_mbps": 2500,
        "random_read_mbps": 2000,
        "random_write_mbps": 1800,
        "description": "Typical NVMe SSD performance",
        "typical_use": "High-speed cache, system drives",
        "reliability_factor": 0.95
    },
    "default": {
        "sequential_read_mbps": 120,
        "sequential_write_mbps": 110,
        "random_read_mbps": 60,
        "random_write_mbps": 55,
        "description": "Conservative default performance model",
        "typical_use": "Fallback when drive type unknown",
        "reliability_factor": 0.75  # Most conservative factor
    }
}


def get_performance_model(drive_type: str = "default") -> Dict[str, Any]:
    """
    Get performance model for a specific drive type.

    Args:
        drive_type: Type of drive (7200_rpm_sata, 5400_rpm_sata, ssd, nvme, default)

    Returns:
        Dictionary containing performance characteristics
    """
    return DRIVE_PERFORMANCE_MODELS.get(drive_type, DRIVE_PERFORMANCE_MODELS["default"])


def estimate_transfer_rate_mbps(drive_type: str = "default", operation: str = "sequential_write") -> float:
    """
    Estimate transfer rate for a given drive type and operation.

    Args:
        drive_type: Type of drive
        operation: Type of operation (sequential_read, sequential_write, random_read, random_write)

    Returns:
        Estimated transfer rate in MB/s with reliability factor applied
    """
    model = get_performance_model(drive_type)
    base_rate = model.get(operation, model.get("sequential_write", 110))
    reliability_factor = model.get("reliability_factor", 0.75)

    return base_rate * reliability_factor


def get_conservative_write_rate(drive_type: str = "default") -> float:
    """
    Get conservative write rate for ETA calculations.

    Args:
        drive_type: Type of drive

    Returns:
        Conservative write rate in MB/s (80% of estimated rate)
    """
    estimated_rate = estimate_transfer_rate_mbps(drive_type, "sequential_write")
    return estimated_rate * 0.8  # Additional 20% safety margin for ETA estimates


def detect_drive_type(device_path: str, size_bytes: int) -> str:
    """
    Attempt to detect drive type based on device path and size.

    Args:
        device_path: Path to the device (e.g., /dev/sda)
        size_bytes: Drive size in bytes

    Returns:
        Detected drive type string

    Note:
        This is a basic heuristic-based detection. For production use,
        consider using smartctl or other tools for more accurate detection.
    """
    # Basic heuristics - could be enhanced with smartctl integration
    size_gb = size_bytes / (1024 ** 3)

    # Very large drives are typically slower RPM
    if size_gb > 8000:  # > 8TB typically 5400 RPM
        return "5400_rpm_sata"

    # Small drives might be SSDs
    if size_gb < 500:  # < 500GB might be SSD
        return "ssd"

    # Default to 7200 RPM for medium-sized drives
    return "7200_rpm_sata"


def list_available_models() -> Dict[str, str]:
    """
    Get a summary of available performance models.

    Returns:
        Dictionary mapping model names to descriptions
    """
    return {
        model_name: model_data["description"]
        for model_name, model_data in DRIVE_PERFORMANCE_MODELS.items()
    }


def get_model_performance_summary(drive_type: str = "default") -> str:
    """
    Get a formatted summary of performance characteristics for a drive type.

    Args:
        drive_type: Type of drive

    Returns:
        Formatted string with performance summary
    """
    model = get_performance_model(drive_type)

    return f"""Drive Type: {drive_type}
Description: {model['description']}
Sequential Read: {model['sequential_read_mbps']} MB/s
Sequential Write: {model['sequential_write_mbps']} MB/s
Random Read: {model['random_read_mbps']} MB/s
Random Write: {model['random_write_mbps']} MB/s
Reliability Factor: {model['reliability_factor']}
Conservative Write Rate: {get_conservative_write_rate(drive_type):.1f} MB/s"""


if __name__ == "__main__":
    # Example usage and testing
    print("Available Drive Performance Models:")
    print("=" * 40)

    for model_name in DRIVE_PERFORMANCE_MODELS.keys():
        print(f"\n{get_model_performance_summary(model_name)}")
        print("-" * 40)