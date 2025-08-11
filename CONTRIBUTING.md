# Contributing to Unraid Rebalancer

Thank you for your interest in contributing to Unraid Rebalancer! This document provides guidelines and information for contributors.

## Table of Contents

- [Code of Conduct](#code-of-conduct)
- [Getting Started](#getting-started)
- [Development Setup](#development-setup)
- [Contributing Guidelines](#contributing-guidelines)
- [Pull Request Process](#pull-request-process)
- [Coding Standards](#coding-standards)
- [Testing](#testing)
- [Documentation](#documentation)
- [Reporting Issues](#reporting-issues)

## Code of Conduct

This project adheres to a code of conduct that we expect all contributors to follow. Please be respectful and constructive in all interactions.

### Our Standards

- Use welcoming and inclusive language
- Be respectful of differing viewpoints and experiences
- Gracefully accept constructive criticism
- Focus on what is best for the community
- Show empathy towards other community members

## Getting Started

### Prerequisites

- Python 3.8 or higher
- Git
- Basic understanding of Unraid systems
- Familiarity with command-line tools

### Development Setup

1. **Fork the repository**
   ```bash
   # Fork on GitHub, then clone your fork
   git clone https://github.com/YOUR_USERNAME/unraid-rebalancer.git
   cd unraid-rebalancer
   ```

2. **Set up the development environment**
   ```bash
   # Add upstream remote
   git remote add upstream https://github.com/samestrin/unraid-rebalancer.git
   
   # Create a virtual environment (optional but recommended)
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install development dependencies**
   ```bash
   # Currently no external dependencies required
   # Future: pip install -r requirements-dev.txt
   ```

4. **Verify the setup**
   ```bash
   # Run the tool in dry-run mode
   python unraid_rebalancer.py --help
   ```

## Contributing Guidelines

### Types of Contributions

We welcome several types of contributions:

- **Bug Reports**: Help us identify and fix issues
- **Feature Requests**: Suggest new functionality
- **Code Contributions**: Implement fixes or new features
- **Documentation**: Improve or expand documentation
- **Testing**: Add or improve test coverage
- **Performance**: Optimize existing code

### Before You Start

1. **Check existing issues**: Look for existing issues or discussions about your idea
2. **Create an issue**: For significant changes, create an issue to discuss the approach
3. **Get feedback**: Engage with maintainers and community members
4. **Plan your work**: Break large changes into smaller, manageable pieces

## Pull Request Process

### 1. Create a Branch

```bash
# Update your fork
git fetch upstream
git checkout main
git merge upstream/main

# Create a feature branch
git checkout -b feature/your-feature-name
# or
git checkout -b fix/issue-number-description
```

### 2. Make Your Changes

- Follow the coding standards outlined below
- Write clear, concise commit messages
- Include tests for new functionality
- Update documentation as needed

### 3. Test Your Changes

```bash
# Run basic functionality tests
python unraid_rebalancer.py --help
python unraid_rebalancer.py --target-percent 80  # Dry run

# Test with various options
python unraid_rebalancer.py --verbose --log-file test.log

# Future: Run automated tests
# python -m pytest tests/
```

### 4. Commit Your Changes

```bash
# Stage your changes
git add .

# Commit with a descriptive message
git commit -m "Add feature: description of what you added"

# Push to your fork
git push origin feature/your-feature-name
```

### 5. Submit a Pull Request

1. Go to your fork on GitHub
2. Click "New Pull Request"
3. Select the appropriate base and compare branches
4. Fill out the pull request template
5. Submit the pull request

### Pull Request Requirements

- [ ] Clear description of changes
- [ ] Reference related issues
- [ ] Tests pass (when available)
- [ ] Documentation updated
- [ ] Changelog updated for significant changes
- [ ] Code follows project standards

## Coding Standards

### Python Style

- Follow PEP 8 style guidelines
- Use type hints for function parameters and return values
- Write docstrings for all functions and classes
- Use meaningful variable and function names
- Keep functions focused and under 50-100 lines

### Code Organization

- Group related functionality together
- Use clear section comments (e.g., `# ---------- Utilities ----------`)
- Maintain consistent indentation (4 spaces)
- Limit line length to 100 characters when practical

### Error Handling

- Use appropriate exception types
- Provide helpful error messages
- Log errors appropriately
- Handle edge cases gracefully

### Example Code Style

```python
def calculate_disk_usage(disk_path: Path) -> Tuple[int, int, int]:
    """Calculate disk usage statistics.
    
    Args:
        disk_path: Path to the disk mount point
        
    Returns:
        Tuple of (total_bytes, used_bytes, free_bytes)
        
    Raises:
        OSError: If disk statistics cannot be retrieved
    """
    try:
        stat = os.statvfs(disk_path)
        total = stat.f_frsize * stat.f_blocks
        free = stat.f_frsize * stat.f_bavail
        used = total - free
        return total, used, free
    except OSError as e:
        logging.error(f"Failed to get disk stats for {disk_path}: {e}")
        raise
```

## Testing

### Current Testing

Currently, testing is primarily manual. When contributing:

1. Test your changes thoroughly
2. Test edge cases and error conditions
3. Verify functionality on different scenarios
4. Test both dry-run and execution modes

### Future Testing Framework

We plan to implement:

- Unit tests for individual functions
- Integration tests for complete workflows
- Mock testing for disk operations
- Performance benchmarks

### Testing Guidelines

- Test both success and failure scenarios
- Use descriptive test names
- Include edge cases
- Mock external dependencies
- Ensure tests are reproducible

## Documentation

### Types of Documentation

- **Code Comments**: Explain complex logic
- **Docstrings**: Document function/class behavior
- **README**: User-facing documentation
- **CHANGELOG**: Track version changes
- **Contributing Guide**: This document

### Documentation Standards

- Use clear, concise language
- Include examples where helpful
- Keep documentation up-to-date with code changes
- Use proper markdown formatting
- Include code examples for complex features

## Reporting Issues

### Before Reporting

1. **Search existing issues**: Check if the issue already exists
2. **Reproduce the issue**: Ensure you can consistently reproduce it
3. **Gather information**: Collect relevant details about your environment

### Issue Template

When reporting issues, please include:

- **Description**: Clear description of the problem
- **Steps to Reproduce**: Detailed steps to reproduce the issue
- **Expected Behavior**: What you expected to happen
- **Actual Behavior**: What actually happened
- **Environment**: Python version, OS, Unraid version
- **Command Used**: The exact command that caused the issue
- **Logs**: Relevant log output (use `--verbose` for detailed logs)

### Example Issue Report

```markdown
## Description
The tool fails when encountering symbolic links in share directories.

## Steps to Reproduce
1. Create a symbolic link in a share directory
2. Run `sudo ./unraid_rebalancer.py --target-percent 80 --execute`
3. Observe the error

## Expected Behavior
The tool should handle symbolic links gracefully, either by following them or skipping them with a warning.

## Actual Behavior
The tool crashes with a FileNotFoundError.

## Environment
- Python: 3.9.2
- OS: Unraid 6.10.3
- Tool Version: 1.0.0

## Command Used
```bash
sudo ./unraid_rebalancer.py --target-percent 80 --execute --verbose
```

## Logs
[Include relevant log output here]
```

## Recognition

Contributors will be recognized in:

- The project README
- Release notes for significant contributions
- The project's contributor list

## Questions?

If you have questions about contributing:

1. Check existing documentation
2. Search closed issues for similar questions
3. Open a new issue with the "question" label
4. Reach out to maintainers

Thank you for contributing to Unraid Rebalancer!