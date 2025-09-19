# Test Drive Prioritization
Created: September 19, 2025
Updated: September 19, 2025

## Summary
Unit tests for the drive prioritization functionality in the Unraid Rebalancer.

## Test Structure

### Test Modules
- Test drive prioritization logic
- Test plan generation with prioritization
- Test CLI integration for prioritization options
- Test edge cases and error conditions

### Test Data
- Mock disk configurations with varying fill percentages
- Test plans with different move priorities
- Sample rebalance scenarios

### Test Cases

#### 1. Basic Prioritization Tests
- Verify disks are correctly sorted by fill percentage
- Test that moves from high-fill disks are prioritized
- Confirm that destination disks are selected appropriately

#### 2. Edge Case Tests
- Empty disk arrays
- Single disk scenarios
- All disks at same fill percentage
- Extremely high or low fill percentages

#### 3. Integration Tests
- Test with actual Disk objects
- Verify integration with existing plan generation
- Test CLI argument parsing for prioritization

### Code Structure

```python
import unittest
from unittest.mock import patch, MagicMock
import sys
import os

# Add parent directory to path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

class TestDrivePrioritization(unittest.TestCase):
    def setUp(self):
        # Setup test data
        pass

    def test_high_fill_disk_prioritization(self):
        # Test that high-fill disks are prioritized for moves
        pass

    def test_plan_generation_with_prioritization(self):
        # Test that plan generation respects prioritization
        pass

    def test_cli_prioritization_argument(self):
        # Test CLI argument for prioritization
        pass

if __name__ == '__main__':
    unittest.main()
```

### Running Tests
```bash
python -m pytest tests/unit/test_drive_prioritization.py -v
```

### Test Coverage Goals
- 90%+ code coverage for prioritization logic
- Test all public methods and functions
- Include negative test cases
- Test with various disk configurations
