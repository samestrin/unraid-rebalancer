#!/usr/bin/env python3
"""
Performance Mode Standards

This module defines standardized rsync flag configurations for all performance modes,
ensuring consistent behavior and addressing the inconsistencies identified in research.
"""

# Standardized RSYNC_MODES with consistent flag usage
STANDARDIZED_RSYNC_MODES = {
    "fast": {
        "flags": [
            "-av",                    # Archive mode with verbose output
            "--partial",              # Keep partially transferred files
            "--inplace",              # Update files in-place (saves disk space)
            "--numeric-ids",          # Use numeric user/group IDs
            "--no-compress",          # Disable compression for minimal CPU overhead
            "--info=progress2"        # Add progress reporting (was missing)
        ],
        "description": "Fastest transfers, minimal CPU overhead with progress reporting",
        "features": [
            "basic_archive",          # Basic file attribute preservation
            "no_compression",         # No compression for speed
            "progress_reporting",     # Now includes progress reporting
            "minimal_cpu"             # Optimized for low CPU usage
        ],
        "target_hardware": "Lower-end CPUs, slower storage"
    },
    "balanced": {
        "flags": [
            "-av",                    # Archive mode with verbose output
            "-X",                     # Preserve extended attributes
            "--partial",              # Keep partially transferred files
            "--inplace",              # Update files in-place
            "--numeric-ids",          # Use numeric user/group IDs
            "--info=progress2"        # Detailed progress information
        ],
        "description": "Balanced speed and features with extended attributes",
        "features": [
            "extended_attrs",         # Extended attribute preservation
            "progress_reporting",     # Detailed progress reporting
            "moderate_features",      # Good balance of features vs performance
            "mid_range_cpu"           # Suitable for mid-range CPUs
        ],
        "target_hardware": "Mid-range CPUs, mixed storage types"
    },
    "integrity": {
        "flags": [
            "-aHAX",                  # Archive with hard links, ACLs, extended attrs
            "--partial",              # Keep partially transferred files
            "--inplace",              # Update files in-place
            "--numeric-ids",          # Use numeric user/group IDs
            "--info=progress2",       # Detailed progress information
            "--checksum"              # Add checksum verification for integrity
        ],
        "description": "Maximum integrity checking with hard links, ACLs, and checksums",
        "features": [
            "hard_links",             # Hard link preservation
            "acls",                   # Access Control List preservation
            "extended_attrs",         # Extended attribute preservation
            "checksum_verification",  # File integrity verification
            "detailed_progress",      # Comprehensive progress reporting
            "maximum_integrity"       # Highest data integrity guarantees
        ],
        "target_hardware": "High-end CPUs, fast storage, integrity-critical operations"
    }
}

# Performance mode validation and recommendations
PERFORMANCE_RECOMMENDATIONS = {
    "fast": {
        "cpu_usage": "Low",
        "memory_usage": "Low",
        "network_usage": "Medium",
        "disk_io": "High",
        "best_for": [
            "Large file transfers",
            "Lower-end hardware",
            "Network-attached storage",
            "Quick rebalancing operations"
        ],
        "avoid_when": [
            "Hard links must be preserved",
            "ACLs are critical",
            "Maximum data integrity required"
        ]
    },
    "balanced": {
        "cpu_usage": "Medium",
        "memory_usage": "Medium",
        "network_usage": "Medium",
        "disk_io": "Medium",
        "best_for": [
            "General purpose rebalancing",
            "Mid-range hardware",
            "Mixed file types",
            "Regular maintenance operations"
        ],
        "avoid_when": [
            "Hard links must be preserved",
            "Maximum performance required",
            "Very low-end hardware"
        ]
    },
    "integrity": {
        "cpu_usage": "High",
        "memory_usage": "Medium",
        "network_usage": "Low",
        "disk_io": "Medium",
        "best_for": [
            "Critical data preservation",
            "Hard link heavy directories",
            "ACL preservation required",
            "Maximum data integrity"
        ],
        "avoid_when": [
            "Low-end hardware",
            "Speed is critical",
            "Simple file structures"
        ]
    }
}

def get_standardized_rsync_flags(mode: str) -> list:
    """
    Get standardized rsync flags for the specified performance mode.

    Args:
        mode: Performance mode name (fast, balanced, integrity)

    Returns:
        List of rsync flags for the mode

    Raises:
        ValueError: If mode is not recognized
    """
    if mode not in STANDARDIZED_RSYNC_MODES:
        available_modes = ', '.join(STANDARDIZED_RSYNC_MODES.keys())
        raise ValueError(f"Unknown rsync mode '{mode}'. Available modes: {available_modes}")

    return STANDARDIZED_RSYNC_MODES[mode]["flags"].copy()

def get_mode_description(mode: str) -> str:
    """Get description for the specified performance mode."""
    if mode not in STANDARDIZED_RSYNC_MODES:
        return f"Unknown mode: {mode}"
    return STANDARDIZED_RSYNC_MODES[mode]["description"]

def get_mode_features(mode: str) -> list:
    """Get list of features for the specified performance mode."""
    if mode not in STANDARDIZED_RSYNC_MODES:
        return []
    return STANDARDIZED_RSYNC_MODES[mode]["features"].copy()

def recommend_mode_for_hardware(cpu_cores: int = None, available_memory_gb: int = None,
                               storage_type: str = None) -> str:
    """
    Recommend optimal performance mode based on hardware characteristics.

    Args:
        cpu_cores: Number of CPU cores available
        available_memory_gb: Available memory in GB
        storage_type: Storage type (ssd, hdd, network)

    Returns:
        Recommended mode name
    """
    # Simple heuristic-based recommendations
    if cpu_cores and cpu_cores >= 8 and available_memory_gb and available_memory_gb >= 16:
        return "integrity"
    elif cpu_cores and cpu_cores >= 4 and available_memory_gb and available_memory_gb >= 8:
        return "balanced"
    else:
        return "fast"

def validate_mode_compatibility(mode: str, file_characteristics: dict) -> tuple:
    """
    Validate if the selected mode is compatible with file characteristics.

    Args:
        mode: Selected performance mode
        file_characteristics: Dict with keys like 'has_hard_links', 'has_acls', etc.

    Returns:
        Tuple of (is_compatible: bool, warnings: list)
    """
    warnings = []
    is_compatible = True

    if mode == "fast":
        if file_characteristics.get('has_hard_links', False):
            warnings.append("Fast mode does not preserve hard links - consider 'integrity' mode")
            is_compatible = False
        if file_characteristics.get('has_acls', False):
            warnings.append("Fast mode does not preserve ACLs - consider 'balanced' or 'integrity' mode")
            is_compatible = False

    elif mode == "balanced":
        if file_characteristics.get('has_hard_links', False):
            warnings.append("Balanced mode does not preserve hard links - consider 'integrity' mode")
            is_compatible = False

    return is_compatible, warnings

def list_available_modes() -> dict:
    """Return all available modes with their descriptions."""
    return {
        mode: config["description"]
        for mode, config in STANDARDIZED_RSYNC_MODES.items()
    }

# Flag explanation for documentation and debugging
FLAG_EXPLANATIONS = {
    "-a": "Archive mode: preserves permissions, times, ownership, etc. (equivalent to -rlptgoD)",
    "-v": "Verbose: increase verbosity of output",
    "-H": "Hard-links: preserve hard links",
    "-A": "ACLs: preserve Access Control Lists",
    "-X": "eXtended attributes: preserve extended file attributes",
    "--partial": "Keep partially transferred files to enable resumption",
    "--inplace": "Update destination files in-place (saves disk space)",
    "--numeric-ids": "Transfer numeric user/group IDs rather than names",
    "--no-compress": "Disable compression to reduce CPU usage",
    "--info=progress2": "Show detailed progress information",
    "--checksum": "Skip files based on checksum, not mod-time & size"
}

def explain_flags(mode: str) -> dict:
    """Return explanations for all flags used in the specified mode."""
    if mode not in STANDARDIZED_RSYNC_MODES:
        return {}

    flags = STANDARDIZED_RSYNC_MODES[mode]["flags"]
    explanations = {}

    for flag in flags:
        # Handle combined flags like -aHAX
        if flag.startswith('-') and len(flag) > 2 and not flag.startswith('--'):
            # Split combined short flags
            for char in flag[1:]:
                short_flag = f"-{char}"
                if short_flag in FLAG_EXPLANATIONS:
                    explanations[short_flag] = FLAG_EXPLANATIONS[short_flag]
        elif flag in FLAG_EXPLANATIONS:
            explanations[flag] = FLAG_EXPLANATIONS[flag]

    return explanations

# Example usage and testing
if __name__ == "__main__":
    # Test standardized modes
    for mode_name in STANDARDIZED_RSYNC_MODES:
        print(f"\n{mode_name.upper()} Mode:")
        print(f"  Description: {get_mode_description(mode_name)}")
        print(f"  Flags: {' '.join(get_standardized_rsync_flags(mode_name))}")
        print(f"  Features: {', '.join(get_mode_features(mode_name))}")

        flag_explanations = explain_flags(mode_name)
        if flag_explanations:
            print("  Flag Explanations:")
            for flag, explanation in flag_explanations.items():
                print(f"    {flag}: {explanation}")