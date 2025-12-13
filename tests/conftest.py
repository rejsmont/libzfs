"""Pytest configuration and shared fixtures for libzfseasy tests."""

import pytest
from unittest.mock import Mock, MagicMock
from libzfseasy.types import Dataset, Filesystem, Volume, Snapshot, Bookmark, Property


@pytest.fixture
def mock_subprocess(mocker):
    """Mock subprocess.Popen to avoid actual ZFS commands."""
    mock_popen = mocker.patch('subprocess.Popen')
    
    def setup_mock(stdout='', stderr='', returncode=0):
        """Helper to configure mock subprocess behavior."""
        process_mock = MagicMock()
        
        # Setup stdout
        if isinstance(stdout, list):
            process_mock.stdout.readline.side_effect = stdout + ['']
        else:
            process_mock.stdout.readline.return_value = stdout
        
        # Setup stderr
        if isinstance(stderr, list):
            process_mock.stderr.readline.side_effect = stderr + ['']
        else:
            process_mock.stderr.readline.return_value = stderr
        
        # Setup return code
        process_mock.poll.side_effect = [None] * len(stdout) + [returncode] if isinstance(stdout, list) else [returncode]
        
        mock_popen.return_value = process_mock
        return process_mock
    
    mock_popen.setup = setup_mock
    return mock_popen


@pytest.fixture
def sample_pool():
    """Return a sample pool name for testing."""
    return 'testpool'


@pytest.fixture
def sample_dataset(sample_pool):
    """Return a sample dataset object."""
    return Dataset(f'{sample_pool}/dataset')


@pytest.fixture
def sample_filesystem(sample_pool):
    """Return a sample filesystem object."""
    return Filesystem(f'{sample_pool}/filesystem')


@pytest.fixture
def sample_volume(sample_pool):
    """Return a sample volume object."""
    return Volume(f'{sample_pool}/volume')


@pytest.fixture
def sample_snapshot(sample_filesystem):
    """Return a sample snapshot object."""
    return Snapshot(sample_filesystem, 'snap1')


@pytest.fixture
def sample_bookmark(sample_filesystem):
    """Return a sample bookmark object."""
    return Bookmark(sample_filesystem, 'bookmark1')


@pytest.fixture
def mock_zfs_list_output():
    """Mock output from 'zfs list' command."""
    return [
        'testpool/filesystem\tfilesystem\n',
        'testpool/volume\tvolume\n',
        'testpool/filesystem@snap1\tsnapshot\n',
    ]


@pytest.fixture
def mock_zfs_get_output():
    """Mock output from 'zfs get' command."""
    return [
        'testpool/filesystem\tname\ttestpool/filesystem\t-\t-\n',
        'testpool/filesystem\ttype\tfilesystem\t-\t-\n',
        'testpool/filesystem\tcompression\tlz4\tlocal\t-\n',
        'testpool/filesystem\tmountpoint\t/mnt/test\tlocal\t-\n',
    ]


@pytest.fixture
def sample_properties():
    """Return a sample properties dictionary."""
    return {
        'compression': 'lz4',
        'mountpoint': '/mnt/test',
        'quota': '10G'
    }


@pytest.fixture
def sample_property_objects():
    """Return sample Property objects."""
    return {
        'compression': Property('lz4', source='local'),
        'mountpoint': Property('/mnt/test', source='local'),
        'quota': Property('10G', source='inherited')
    }


# Test markers
def pytest_configure(config):
    """Register custom markers."""
    config.addinivalue_line(
        "markers", "unit: mark test as a unit test"
    )
    config.addinivalue_line(
        "markers", "integration: mark test as an integration test"
    )
    config.addinivalue_line(
        "markers", "slow: mark test as slow running"
    )
    config.addinivalue_line(
        "markers", "subprocess: mark test as mocking subprocess"
    )
