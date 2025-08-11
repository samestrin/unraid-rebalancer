#!/bin/bash
# Basic Unraid Rebalancer Examples
# 
# This script demonstrates common usage patterns for the Unraid Rebalancer.
# Always review the dry-run output before executing with --execute!

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REBALANCER="${SCRIPT_DIR}/../unraid_rebalancer.py"

# Check if running as root
if [[ $EUID -ne 0 ]]; then
   echo -e "${RED}Error: This script must be run as root (use sudo)${NC}"
   exit 1
fi

# Check if rebalancer exists
if [[ ! -f "$REBALANCER" ]]; then
   echo -e "${RED}Error: Rebalancer script not found at $REBALANCER${NC}"
   exit 1
fi

echo -e "${BLUE}Unraid Rebalancer - Example Usage${NC}"
echo "======================================"
echo

# Function to run with confirmation
run_with_confirmation() {
    local description="$1"
    local command="$2"
    local execute="${3:-false}"
    
    echo -e "${YELLOW}Example: $description${NC}"
    echo "Command: $command"
    echo
    
    if [[ "$execute" == "true" ]]; then
        echo -e "${RED}WARNING: This will actually move data!${NC}"
        read -p "Are you sure you want to proceed? (yes/no): " confirm
        if [[ "$confirm" != "yes" ]]; then
            echo "Skipped."
            echo
            return
        fi
    fi
    
    eval "$command"
    echo
    echo "Press Enter to continue..."
    read
}

# Example 1: Basic dry-run with 80% target
run_with_confirmation \
    "Basic dry-run targeting 80% fill per disk" \
    "python3 '$REBALANCER' --target-percent 80"

# Example 2: Exclude system shares
run_with_confirmation \
    "Exclude system shares (appdata, System)" \
    "python3 '$REBALANCER' --target-percent 80 --exclude-shares appdata,System"

# Example 3: Only move large files
run_with_confirmation \
    "Only move allocation units >= 5 GiB" \
    "python3 '$REBALANCER' --target-percent 80 --min-unit-size 5GiB"

# Example 4: Work with specific disks
run_with_confirmation \
    "Work only with specific disks (disk1, disk2, disk3)" \
    "python3 '$REBALANCER' --target-percent 80 --include-disks disk1,disk2,disk3"

# Example 5: Auto-balance with headroom
run_with_confirmation \
    "Auto-balance with 10% headroom" \
    "python3 '$REBALANCER' --target-percent -1 --headroom-percent 10"

# Example 6: Save plan for later
run_with_confirmation \
    "Generate and save a rebalancing plan" \
    "python3 '$REBALANCER' --target-percent 80 --save-plan /tmp/rebalance_plan.json"

# Example 7: Load and preview saved plan
if [[ -f "/tmp/rebalance_plan.json" ]]; then
    run_with_confirmation \
        "Load and preview saved plan" \
        "python3 '$REBALANCER' --load-plan /tmp/rebalance_plan.json"
fi

# Example 8: Verbose logging with bandwidth limit
run_with_confirmation \
    "Verbose logging with 50MB/s bandwidth limit" \
    "python3 '$REBALANCER' --target-percent 80 --verbose --rsync-extra '--bwlimit=50M' --log-file /tmp/rebalance.log"

echo -e "${GREEN}Examples completed!${NC}"
echo
echo -e "${YELLOW}Important Notes:${NC}"
echo "- All examples above were dry-runs (no data moved)"
echo "- To actually execute moves, add --execute to any command"
echo "- Always review the plan before executing"
echo "- Keep backups of important data"
echo "- Stop heavy disk activity before rebalancing"
echo
echo -e "${BLUE}To execute a plan:${NC}"
echo "sudo python3 '$REBALANCER' --target-percent 80 --execute"
echo
echo -e "${RED}Remember: Use --execute only after reviewing the dry-run output!${NC}"