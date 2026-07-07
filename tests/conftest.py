"""Pytest configuration and shared fixtures for libzfseasy tests."""

import pytest
from unittest.mock import Mock, MagicMock
from libzfseasy.types import Dataset, Filesystem, Volume, Snapshot, Bookmark, Property


@pytest.fixture
def mock_subprocess(mocker):
    """Mock subprocess.Popen to avoid actual ZFS commands."""
    mock_popen = mocker.patch('subprocess.Popen')
    
    # Queue to hold multiple mock configurations
    mock_queue = []
    
    def setup_mock(stdout='', stderr='', returncode=0):
        """Helper to configure mock subprocess behavior for a single call."""
        process_mock = MagicMock()
        
        # Setup stdout
        if isinstance(stdout, list):
            process_mock.stdout.readline.side_effect = stdout + ['']
        else:
            process_mock.stdout.readline.return_value = stdout
        
        # Setup stderr
        if isinstance(stderr, list):
            process_mock.stderr.readline.side_effect = stderr + ['']
        elif stderr == '':
            # Empty string already signals EOF on the very first read.
            process_mock.stderr.readline.return_value = ''
        else:
            # libzfseasy drains stderr on a dedicated background thread that
            # loops "while readline() != ''". A constant non-empty
            # return_value would make that thread (and any stderr_thread.join())
            # spin/hang forever, so terminate after a single line.
            process_mock.stderr.readline.side_effect = [stderr, '']
        
        # Setup return code
        process_mock.poll.side_effect = [None] * len(stdout) + [returncode] if isinstance(stdout, list) else [returncode]

        # Command._exec_capture() now falls back to process.wait() when
        # stdout hits EOF but poll() still returns None (rather than
        # spin-looping). Stub it to resolve to the same returncode that
        # poll() would eventually report, so that fallback path works too.
        process_mock.wait.return_value = returncode

        # Set as return_value and also add to queue
        mock_popen.return_value = process_mock
        mock_queue.append(process_mock)
        return process_mock
    
    def setup_multi(*configs):
        """Helper to configure multiple subprocess calls in sequence.
        
        Args:
            *configs: Tuples of (stdout, stderr, returncode) for each subprocess call
        """
        mock_queue.clear()
        mock_popen.side_effect = popen_side_effect  # Enable side_effect for multi-call
        for config in configs:
            if len(config) == 1:
                setup_mock(stdout=config[0])
            elif len(config) == 2:
                setup_mock(stdout=config[0], stderr=config[1])
            else:
                setup_mock(stdout=config[0], stderr=config[1], returncode=config[2])
    
    # Return mocks from queue in order
    def popen_side_effect(*args, **kwargs):
        if mock_queue:
            return mock_queue.pop(0)
        # Fallback - return a mock with poll() returning 0 immediately
        fallback = MagicMock()
        fallback.stdout.readline.return_value = ''
        fallback.stderr.readline.return_value = ''
        fallback.poll.return_value = 0
        return fallback
    
    mock_popen.setup = setup_mock
    mock_popen.setup_multi = setup_multi
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
        'testpool/filesystem\tcompression\tlz4\t-\tlocal\n',
        'testpool/filesystem\tmountpoint\t/mnt/test\t-\tlocal\n',
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
def dataset_properties():
    """Return properties valid for generic Dataset objects (no fs/volume extras)."""
    return {
        'compression': 'lz4',
        'reservation': '1G'
    }


@pytest.fixture
def sample_property_objects():
    """Return sample Property objects."""
    return {
        'compression': Property('lz4', source='local'),
        'mountpoint': Property('/mnt/test', source='local'),
        'quota': Property('10G', source='inherited')
    }


@pytest.fixture
def sample_backup_config():
    """Minimal in-memory BackupConfig for zfsbackup tests."""
    from zfsbackup.config import BackupConfig, DatasetConfig, RetentionRule
    from datetime import timedelta
    return BackupConfig(
        datasets=[
            DatasetConfig(
                name='pool/data',
                frequency=timedelta(hours=1),
                retention_rules=[RetentionRule(timedelta(days=1), timedelta(days=30))],
            ),
        ],
        snapshot_prefix='autosnap',
    )


@pytest.fixture
def config_with_remote(tmp_path):
    """BackupConfig with remote_backup enabled, writing config to a temp YAML file."""
    from zfsbackup.config import BackupConfig, DatasetConfig, RemoteServerConfig
    from datetime import timedelta
    cfg_file = tmp_path / 'config.yaml'
    cfg_file.write_text(
        "datasets:\n"
        "  - name: pool/data\n"
        "remote_backup:\n"
        "  target_dataset: pool/backups\n"
        "  enabled: true\n"
    )
    return BackupConfig.from_file(cfg_file)


@pytest.fixture
def config_yaml_path(tmp_path):
    """Write a minimal valid YAML config to a temp file and return the Path."""
    cfg_file = tmp_path / 'config.yaml'
    cfg_file.write_text(
        "datasets:\n"
        "  - name: pool/data\n"
        "    frequency: 1h\n"
        "    retention:\n"
        "      1d: 30d\n"
    )
    return cfg_file


def _auto_zfs_pool_local(tmp_path_factory):
    """Local pool creation path: file-backed pool via passwordless sudo."""
    import subprocess
    import os
    import stat
    from libzfseasy.zfs import ZFS_BIN, ZPOOL_BIN

    def _has_zfs():
        try:
            subprocess.run([ZFS_BIN, 'list'], capture_output=True, check=True)
            return True
        except (subprocess.CalledProcessError, FileNotFoundError):
            return False

    if not _has_zfs():
        yield None
        return

    pool_name = f'zfsbackup_pytest_{os.getpid()}'
    disk_dir = tmp_path_factory.mktemp('zfs_pool')
    disk_image = str(disk_dir / 'disk.img')

    subprocess.run(['truncate', '-s', '512m', disk_image], check=True)

    result = subprocess.run(
        ['sudo', '-n', 'zpool', 'create', pool_name, disk_image],
        capture_output=True,
    )
    if result.returncode != 0:
        yield None
        return

    current_user = subprocess.run(
        ['id', '-un'], capture_output=True, text=True
    ).stdout.strip()
    perms = (
        'create,destroy,snapshot,clone,rename,mount,bookmark,'
        'receive,userprop,inherit,'
        'compression,mountpoint,quota,reservation,volsize,volblocksize'
    )
    # Delegation is best-effort; some platforms don't support zfs allow on pool root.
    subprocess.run(
        ['sudo', '-n', 'zfs', 'allow', '-u', current_user, perms, pool_name],
        capture_output=True,
    )

    # Check whether delegation actually worked; if not, install sudo wrapper scripts
    # so that _zfs_cmd()/_zpool_cmd() transparently prefix every command with sudo.
    _probe = f'{pool_name}/_probe'
    try:
        subprocess.run([ZFS_BIN, 'create', _probe], check=True, capture_output=True)
        subprocess.run([ZFS_BIN, 'destroy', _probe], check=True, capture_output=True)
    except subprocess.CalledProcessError:
        script_dir = tmp_path_factory.mktemp('sudo_wrappers')
        for tool, bin_path in [('zfs', ZFS_BIN), ('zpool', ZPOOL_BIN)]:
            script = script_dir / tool
            script.write_text(f'#!/bin/bash\nexec sudo {bin_path} "$@"\n')
            script.chmod(script.stat().st_mode | stat.S_IEXEC | stat.S_IXGRP | stat.S_IXOTH)
        os.environ['ZFS_CMD'] = str(script_dir / 'zfs')
        os.environ['ZPOOL_CMD'] = str(script_dir / 'zpool')

    from libzfseasy.types import Dataset
    try:
        yield Dataset.from_name(pool_name)
    finally:
        subprocess.run(
            ['sudo', '-n', 'zpool', 'destroy', '-f', pool_name],
            capture_output=True,
        )
        os.environ.pop('ZFS_CMD', None)
        os.environ.pop('ZPOOL_CMD', None)


def _auto_zfs_pool_multipass(vm_name, tmp_path_factory):
    """Multipass pool creation path: pool lives inside a Linux VM."""
    import subprocess
    import os
    import stat

    # Start VM if suspended/stopped (best-effort; no-op if already running)
    subprocess.run(['multipass', 'start', vm_name], capture_output=True)

    result = subprocess.run(
        ['multipass', 'info', vm_name], capture_output=True, text=True
    )
    if 'Running' not in result.stdout:
        yield None
        return

    # Auto-install ZFS inside the VM if absent
    r = subprocess.run(
        ['multipass', 'exec', vm_name, '--', 'which', 'zfs'],
        capture_output=True,
    )
    if r.returncode != 0:
        subprocess.run(
            ['multipass', 'exec', vm_name, '--',
             'sudo', 'apt-get', 'install', '-y', 'zfsutils-linux'],
            check=True,
        )

    pool_name = f'zfsbackup_pytest_{os.getpid()}'
    disk_image = f'/tmp/{pool_name}.img'

    subprocess.run(
        ['multipass', 'exec', vm_name, '--', 'truncate', '-s', '512m', disk_image],
        check=True,
    )

    r = subprocess.run(
        ['multipass', 'exec', vm_name, '--',
         'sudo', 'zpool', 'create', pool_name, disk_image],
        capture_output=True,
    )
    if r.returncode != 0:
        subprocess.run(
            ['multipass', 'exec', vm_name, '--', 'rm', '-f', disk_image],
            capture_output=True,
        )
        yield None
        return

    # Create wrapper scripts so libzfseasy routes all zfs/zpool calls into the VM
    script_dir = tmp_path_factory.mktemp('multipass_wrappers')
    for tool in ('zfs', 'zpool'):
        script = script_dir / tool
        script.write_text(
            f'#!/bin/bash\nexec multipass exec {vm_name} -- sudo /usr/sbin/{tool} "$@"\n'
        )
        script.chmod(script.stat().st_mode | stat.S_IEXEC | stat.S_IXGRP | stat.S_IXOTH)

    # _zfs_cmd() / _zpool_cmd() re-read these env vars at call time
    os.environ['ZFS_CMD'] = str(script_dir / 'zfs')
    os.environ['ZPOOL_CMD'] = str(script_dir / 'zpool')

    from libzfseasy.types import Dataset
    try:
        yield Dataset.from_name(pool_name)
    finally:
        subprocess.run(
            ['multipass', 'exec', vm_name, '--',
             'sudo', 'zpool', 'destroy', '-f', pool_name],
            capture_output=True,
        )
        subprocess.run(
            ['multipass', 'exec', vm_name, '--', 'rm', '-f', disk_image],
            capture_output=True,
        )
        os.environ.pop('ZFS_CMD', None)
        os.environ.pop('ZPOOL_CMD', None)


@pytest.fixture(scope='session')
def auto_zfs_pool(tmp_path_factory):
    """Auto-create and destroy a ZFS pool for real_zfs integration tests.

    Without MULTIPASS_VM: creates a local file-backed pool via passwordless sudo.
    With MULTIPASS_VM=<name>: creates the pool inside the named Multipass Linux VM,
    routing all zfs/zpool commands through it for the duration of the session.
    Returns None if setup fails; dependent fixtures call pytest.skip().
    """
    import os
    vm = os.environ.get('MULTIPASS_VM')
    if vm:
        yield from _auto_zfs_pool_multipass(vm, tmp_path_factory)
    else:
        yield from _auto_zfs_pool_local(tmp_path_factory)


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
