# libZFSeasy
Python interface to the ZFS utilities

This library provides a Python interface to ZFS functions using either `zfs` and `zpool` command line utilities
or `libzfs` bindings from TrueNAS.

## Testing

The library includes a comprehensive test suite with three types of tests:

1. **Unit Tests** - Test individual components with mocked subprocess calls
2. **Integration Tests** - Test complete workflows with mocked subprocess calls  
3. **Real ZFS Tests** - Test against actual ZFS commands (requires ZFS installation)

### Running Tests

```bash
# Run all mocked tests (default)
pytest

# Run only real ZFS integration tests (requires test pool)
pytest -m real_zfs
```

### Setting Up Real ZFS Tests

Real ZFS tests require a test pool. Use the provided helper script:

```bash
# Create a test pool
./setup_test_pool.sh

# Run real ZFS tests
pytest -m real_zfs

# Clean up when done
./cleanup_test_pool.sh
```

See [tests/README.md](tests/README.md) for detailed testing documentation.

