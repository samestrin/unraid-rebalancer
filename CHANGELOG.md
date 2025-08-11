# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.0.1] - 2025-07-25

### Added
- Initial release of Unraid Rebalancer
- Intelligent disk rebalancing with configurable target percentages
- Dry-run mode by default for safety
- Support for saving and loading redistribution plans as JSON
- Comprehensive filtering options (disks, shares, globs)
- Configurable allocation unit depth
- Progress tracking and detailed logging
- Rsync-based transfers with resume support
- Safety margins and error handling
- Verbose logging and log file support
- Command-line interface with extensive options

### Features
- **Safe Operations**: Dry-run by default, only moves data when `--execute` is specified
- **Flexible Targeting**: Target specific fill percentages or auto-balance with headroom
- **Plan Management**: Save plans as JSON for later execution or review
- **Granular Control**: Configure allocation units at different directory depths
- **Comprehensive Filtering**: Include/exclude specific disks, shares, and file patterns
- **Resume Support**: Uses rsync with partial transfer support for interrupted operations
- **Progress Tracking**: Real-time progress reporting during operations
- **Logging**: Configurable logging with file output support

### Technical Details
- Python 3.8+ compatibility
- Uses only standard library modules (no external dependencies)
- Preserves file attributes, permissions, and hardlinks
- Avoids Unraid user share copy bug by using `/mnt/disk*` paths only
- Implements greedy algorithm for optimal redistribution planning
- 1GiB safety margin on destination disks
- Comprehensive error handling and recovery

### Safety Features
- Multiple pre-flight checks
- Atomic operations using rsync
- Space validation before transfers
- Detailed plan preview before execution
- Comprehensive error logging
- Graceful handling of interruptions

---

## Future Releases

### Planned Features
- [ ] Web interface for easier management
- [ ] Integration with Unraid notifications
- [ ] Advanced scheduling options
- [ ] Performance metrics and reporting
- [ ] Support for custom balancing algorithms
- [ ] Integration with Unraid array management
- [ ] Automated health checks before rebalancing
- [ ] Support for SSD cache optimization

### Under Consideration
- [ ] GUI application
- [ ] Plugin for Unraid Community Applications
- [ ] Docker container deployment
- [ ] API for external integrations
- [ ] Machine learning-based optimization
- [ ] Real-time monitoring dashboard

---

## Version History

- **0.0.1** - Initial release with core functionality

---

## Contributing

When contributing to this project, please:

1. Update this changelog with your changes
2. Follow semantic versioning for version numbers
3. Include detailed descriptions of new features or fixes
4. Add appropriate tags for the type of change (Added, Changed, Deprecated, Removed, Fixed, Security)

## Links

- [Repository](https://github.com/samestrin/unraid-rebalancer)
- [Issues](https://github.com/samestrin/unraid-rebalancer/issues)
- [Releases](https://github.com/samestrin/unraid-rebalancer/releases)