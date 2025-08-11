# Unraid Rebalancer

![Version 0.0.1](https://img.shields.io/badge/Version-0.0.1-blue) [![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg) ](https://opensource.org/licenses/MIT) [![Built with Python](https://img.shields.io/badge/Built%20with-Python-green)](https://www.python.org/)

An intelligent disk rebalancing tool for Unraid servers that redistributes data across drives to optimize storage utilization and balance fill levels.

## Features

- **Intelligent Planning**: Analyzes disk usage and creates an optimal redistribution plan
- **Safe Operations**: Dry-run mode by default, only moves data when explicitly requested
- **Flexible Targeting**: Target specific fill percentages or auto-balance with headroom
- **Granular Control**: Configure allocation units at different directory depths
- **Resume Support**: Uses rsync with partial transfer support for interrupted operations
- **Plan Management**: Save and load redistribution plans as JSON files
- **Comprehensive Filtering**: Include/exclude specific disks, shares, and file patterns
- **Progress Tracking**: Real-time progress reporting during operations
- **Logging**: Configurable logging with file output support

## Key Benefits

- **Avoids User Share Copy Bug**: Works exclusively with `/mnt/disk*` paths
- **Preserves File Attributes**: Maintains permissions, timestamps, and hardlinks
- **Efficient Transfers**: Uses rsync for reliable, resumable data movement
- **Safety First**: Multiple safeguards prevent data loss
- **Flexible Configuration**: Extensive options for customization

## Installation

### Prerequisites

- Python 3.8 or higher
- rsync (typically pre-installed on Unraid)
- Root access (required for disk operations)

### Quick Install

```bash
# Clone the repository
git clone https://github.com/samestrin/unraid-rebalancer.git
cd unraid-rebalancer

# Make executable
chmod +x unraid_rebalancer.py

# Run (dry-run mode by default)
sudo ./unraid_rebalancer.py --target-percent 80
```

## Usage

### Basic Examples

```bash
# Plan only (dry-run, no data moved)
sudo ./unraid_rebalancer.py --target-percent 80

# Execute the rebalancing plan
sudo ./unraid_rebalancer.py --target-percent 80 --execute

# Auto-balance with 5% headroom
sudo ./unraid_rebalancer.py --target-percent -1 --headroom-percent 5 --execute

# Exclude system shares and only move large files
sudo ./unraid_rebalancer.py --exclude-shares appdata,System --min-unit-size 5GiB --execute
```

### Advanced Usage

```bash
# Save plan for later execution
sudo ./unraid_rebalancer.py --target-percent 80 --save-plan rebalance_plan.json

# Load and execute saved plan
sudo ./unraid_rebalancer.py --load-plan rebalance_plan.json --execute

# Limit bandwidth and enable verbose logging
sudo ./unraid_rebalancer.py --target-percent 80 --execute \
  --rsync-extra "--bwlimit=50M" --verbose --log-file rebalance.log

# Work with specific disks only
sudo ./unraid_rebalancer.py --include-disks disk1,disk2,disk3 --target-percent 75 --execute
```

## Command Line Options

| Option | Description | Default |
|--------|-------------|----------|
| `--target-percent` | Target maximum fill percentage per disk | 80.0 |
| `--headroom-percent` | Headroom percentage for auto-balancing | 5.0 |
| `--execute` | Actually perform moves (default is dry-run) | False |
| `--include-disks` | Comma-separated list of disks to include | All |
| `--exclude-disks` | Comma-separated list of disks to exclude | None |
| `--include-shares` | Comma-separated list of shares to include | All |
| `--exclude-shares` | Comma-separated list of shares to exclude | None |
| `--exclude-globs` | Glob patterns to exclude (e.g., `temp/*,cache/*`) | None |
| `--unit-depth` | Directory depth for allocation units | 1 |
| `--min-unit-size` | Minimum size for units to move | 1GiB |
| `--save-plan` | Save redistribution plan to JSON file | None |
| `--load-plan` | Load plan from JSON file | None |
| `--rsync-extra` | Additional rsync options | None |
| `--allow-merge` | Allow merging into existing directories | False |
| `--verbose`, `-v` | Enable verbose logging | False |
| `--log-file` | Write logs to file | stderr only |

## How It Works

1. **Discovery**: Scans `/mnt/disk*` mounts to identify available disks and their usage
2. **Analysis**: Builds allocation units based on configured depth and filters
3. **Planning**: Creates an optimal redistribution plan using a greedy algorithm
4. **Execution**: Uses rsync to safely move data between disks
5. **Cleanup**: Removes source files after successful transfers

### Allocation Units

The tool works with "allocation units" - directories or files that are moved as a whole:

- **Depth 0**: Entire share content on a disk
- **Depth 1**: Direct children of share root (default)
- **Depth 2+**: Deeper directory levels

## Safety Features

- **Dry-run by default**: No data is moved unless `--execute` is specified
- **Pre-flight checks**: Validates disk mounts and available space
- **Atomic operations**: Uses rsync for reliable, resumable transfers
- **Error handling**: Comprehensive error checking and logging
- **Space margins**: Maintains 1GiB safety margin on destination disks
- **Plan preview**: Shows detailed plan before execution

## Best Practices

### Before Running

1. **Stop heavy writers**: Pause downloads, backups, and other disk-intensive operations
2. **Run from console**: Use SSH with screen/tmux, avoid web terminal
3. **Review the plan**: Always examine the dry-run output first
4. **Backup critical data**: Ensure you have recent backups
5. **Check disk health**: Verify all disks are healthy before rebalancing

### During Operation

1. **Monitor progress**: Watch for errors or unusual behavior
2. **Don't interrupt**: Let operations complete naturally
3. **Check logs**: Review log output for warnings or errors

### After Completion

1. **Verify results**: Check that files moved correctly
2. **Update shares**: Refresh user shares if needed
3. **Monitor performance**: Ensure balanced access patterns

## Troubleshooting

### Common Issues

**"No /mnt/disk* data disks found"**
- Ensure you're running on an Unraid system
- Check that data disks are mounted
- Verify disk naming follows Unraid conventions

**"Permission denied" errors**
- Run with `sudo` for proper disk access
- Check file permissions on source directories

**Rsync failures**
- Check available disk space
- Verify destination disk is writable
- Review rsync error messages in logs

**Plan shows no moves**
- Disks may already be balanced
- Try adjusting `--target-percent` or `--min-unit-size`
- Check if filters are excluding too much data

### Getting Help

1. Run with `--verbose` for detailed logging
2. Check the log file for specific error messages
3. Verify your command-line options
4. Test with a small subset using `--include-disks`

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request. For major changes, please open an issue first to discuss what you would like to change.

### Development Setup

```bash
git clone https://github.com/samestrin/unraid-rebalancer.git
cd unraid-rebalancer

# Install development dependencies (if any)
# pip install -r requirements-dev.txt

# Run tests
# python -m pytest tests/
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Disclaimer

**Use at your own risk.** While this tool includes multiple safety features, always ensure you have current backups before performing any disk operations. The authors are not responsible for any data loss that may occur.

## Acknowledgments

- Inspired by the Unraid community's need for better disk balancing tools
- Built with safety and reliability as primary concerns
- Thanks to all contributors and testers

---

**Note**: This tool is designed specifically for Unraid systems and requires root access for disk operations. Always test with dry-run mode first and maintain current backups of important data.