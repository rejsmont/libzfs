# Testing Guide for libzfseasy

This directory contains the comprehensive test suite for the libzfseasy library.

## Overview

The test suite is organized into three main categories:

- **Unit Tests** (`test_types.py`, `test_commands.py`) - Test individual components in isolation
- **Integration Tests** (`test_integration.py`) - Test complete workflows and interactions
- **Fixtures** (`conftest.py`) - Shared test fixtures and configuration

## Installation

First, install the development dependencies:

```bash
poetry install --with dev
```

Or if using pip:

```bash
pip install -e ".[dev]"
```

## Running Tests

### Run all tests
```bash
pytest
```

### Run with coverage report
```bash
pytest --cov=libzfseasy --cov-report=html
```

The coverage report will be available in `htmlcov/index.html`.

### Run specific test categories

Run only unit tests:
```bash
pytest -m unit
```

Run only integration tests:
```bash
pytest -m integration
```

Run tests that mock subprocess:
```bash
pytest -m subprocess
```

### Run specific test files

```bash
pytest tests/test_types.py
pytest tests/test_commands.py
pytest tests/test_integration.py
```

### Run specific test classes or functions

```bash
pytest tests/test_types.py::TestValidate
pytest tests/test_types.py::TestValidate::test_zfsname_valid
```

### Run in parallel (faster)

```bash
pytest -n auto
```

This uses all available CPU cores.

### Run with verbose output

```bash
pytest -v
```

### Run and stop at first failure

```bash
pytest -x
```

## Test Structure

### test_types.py

Tests for the `types` module including:
- `Validate` - Input validation functions
- `Property` - Property value objects
- `Dataset`, `Filesystem`, `Volume` - Dataset type classes
- `Snapshot`, `SnapshotRange`, `Bookmark` - Snapshot and bookmark classes

### test_commands.py

Tests for all ZFS command classes:
- `ListCommand` - Listing datasets
- `CreateCommand` - Creating filesystems and volumes
- `SnapshotCommand` - Creating snapshots
- `BookmarkCommand` - Creating bookmarks
- `DestroyCommand` - Destroying datasets
- `RenameCommand` - Renaming datasets
- `CloneCommand` - Cloning snapshots
- `GetCommand` - Getting properties
- `SetCommand` - Setting properties
- `InheritCommand` - Inheriting properties
- `SendCommand` - Sending snapshots
- `ReceiveCommand` - Receiving snapshots
- `MountCommand` - Mounting filesystems
- `UnMountCommand` - Unmounting filesystems

### test_integration.py

End-to-end workflow tests:
- Basic create/list/destroy workflows
- Snapshot creation and cloning
- Incremental snapshots
- Bookmark management
- Property management (get/set/inherit)
- Send/receive operations
- Mount/unmount operations
- Complex multi-step workflows
- Error handling

### conftest.py

Shared test fixtures and configuration:
- `mock_subprocess` - Mock subprocess calls
- Sample ZFS objects (pool, dataset, filesystem, volume, snapshot, bookmark)
- Mock command outputs
- Custom pytest markers

## Test Markers

Tests are marked with custom markers for categorization:

- `@pytest.mark.unit` - Unit tests
- `@pytest.mark.integration` - Integration tests
- `@pytest.mark.subprocess` - Tests that mock subprocess calls
- `@pytest.mark.slow` - Tests that take longer to run

## Writing New Tests

### Unit Test Example

```python
import pytest
from libzfseasy.types import Dataset

class TestMyFeature:
    @pytest.mark.unit
    def test_dataset_creation(self, sample_pool):
        """Test creating a dataset."""
        ds = Dataset(f'{sample_pool}/test')
        assert ds.name == f'{sample_pool}/test'
```

### Integration Test Example

```python
import pytest
import libzfseasy as zfs

class TestMyWorkflow:
    @pytest.mark.integration
    @pytest.mark.subprocess
    def test_create_and_destroy(self, mock_subprocess, sample_pool):
        """Test creating and destroying a filesystem."""
        # Setup mock
        mock_subprocess.setup()
        
        # Create filesystem
        fs = zfs.create.filesystem(f'{sample_pool}/test')
        assert fs.name == f'{sample_pool}/test'
        
        # Setup mock for destroy
        mock_subprocess.setup(stdout=['destroy\\ttestpool/test\\n'])
        
        # Destroy filesystem
        result = zfs.destroy.dataset(fs, destroy=True)
        assert len(result) == 1
```

## Mocking Subprocess

The `mock_subprocess` fixture provides an easy way to mock ZFS commands:

```python
def test_with_mock(mock_subprocess):
    # Setup mock to return specific output
    mock_subprocess.setup(
        stdout=['line1\\n', 'line2\\n'],
        stderr='',
        returncode=0
    )
    
    # Your test code here
    # The mocked subprocess will return the configured output
```

## Continuous Integration

The test suite is designed to run in CI environments without requiring actual ZFS utilities:

- All subprocess calls are mocked
- No root privileges required
- No ZFS pools or datasets need to exist
- Tests run completely in isolation

## Coverage Goals

Target coverage goals:
- Overall: >90%
- Critical paths: 100%
- Error handling: >85%

View current coverage:
```bash
pytest --cov=libzfseasy --cov-report=term-missing
```

## Troubleshooting

### Tests fail with "fixture not found"

Make sure you're running pytest from the project root:
```bash
cd /path/to/libzfs
pytest
```

### Import errors

Ensure the package is installed in development mode:
```bash
poetry install
```

### Subprocess mocking issues

If tests hang or fail with subprocess errors, ensure you're using the `mock_subprocess` fixture and calling `.setup()` before each command.

## Contributing

When adding new features:

1. Write unit tests for new functions/classes
2. Write integration tests for new workflows
3. Ensure all tests pass: `pytest`
4. Check coverage: `pytest --cov`
5. Run with multiple markers to verify categorization

## Additional Resources

- [pytest documentation](https://docs.pytest.org/)
- [pytest-mock documentation](https://pytest-mock.readthedocs.io/)
- [pytest-cov documentation](https://pytest-cov.readthedocs.io/)
- [pytest-xdist documentation](https://pytest-xdist.readthedocs.io/)
