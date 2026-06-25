"""Real ZFS integration tests for the zfsbackup daemon.

REQUIREMENTS:
- ZFS must be installed and available
- Either passwordless sudo for zpool/zfs OR run ./setup_test_pool.sh first

SETUP (automatic, if you have NOPASSWD sudo for zpool/zfs):
  pytest -m real_zfs

SETUP (manual):
  ./setup_test_pool.sh
  pytest -m real_zfs
  ./cleanup_test_pool.sh
"""

import subprocess
import time
from datetime import datetime, timedelta, timezone
from typing import List

import pytest

import libzfseasy as zfs
from libzfseasy.types import Dataset, Filesystem
from libzfseasy.zfs import _zfs_cmd

from zfsbackup.backup_manager import DatasetManager
from zfsbackup.config import (
    BackupConfig,
    DatasetConfig,
    Destination,
    RemoteDatasetConfig,
    RetentionRule,
)
from zfsbackup.remote import ClientIdentity, RemoteBackupManager


pytestmark = pytest.mark.real_zfs


# ---------------------------------------------------------------------------
# Module-level helpers
# ---------------------------------------------------------------------------

def snap_name_at(hours_ago: float, prefix: str = 'autosnap') -> str:
    """Return a snapshot name encoding a timestamp *hours_ago* hours in the past."""
    dt = datetime.now(timezone.utc) - timedelta(hours=hours_ago)
    return f"{prefix}_{dt.strftime('%Y%m%d%H%M%S')}"


def list_real_snapshot_names(dataset_name: str) -> List[str]:
    """Return short snapshot names (part after @) for *dataset_name* via the CLI."""
    result = subprocess.run(
        [_zfs_cmd(), 'list', '-H', '-t', 'snapshot', '-r', '-o', 'name', dataset_name],
        capture_output=True, text=True,
    )
    return [
        line.strip().split('@', 1)[1]
        for line in result.stdout.splitlines()
        if '@' in line.strip()
    ]


def _make_simple_config(source_fs, frequency_seconds=3600, dry_run=False,
                        retention_hours=1, keep_hours=24, remote=None):
    """Build an in-memory BackupConfig for *source_fs*."""
    return BackupConfig(
        datasets=[
            DatasetConfig(
                name=source_fs.name,
                frequency=timedelta(seconds=frequency_seconds),
                retention_rules=[
                    RetentionRule(
                        timedelta(hours=retention_hours),
                        timedelta(hours=keep_hours),
                    )
                ],
                remote=remote or [],
            )
        ],
        snapshot_prefix='autosnap',
        dry_run=dry_run,
    )


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def source_fs(auto_zfs_pool):
    """Create a real ZFS filesystem for backup source; destroy on teardown."""
    if auto_zfs_pool is None:
        pytest.skip(
            'ZFS pool not available — install ZFS and grant NOPASSWD sudo for zpool/zfs, '
            'or run ./setup_test_pool.sh first'
        )
    name = f'{auto_zfs_pool.name}/zfsbackup_source'
    existing = Filesystem(name)
    if zfs.exists(existing):
        zfs.destroy(existing, destroy=True, recursive=True)
    try:
        fs = zfs.create.filesystem(name)
    except Exception as e:
        pytest.skip(f'ZFS filesystem creation failed (permissions not delegated?): {e}')
    yield fs
    try:
        zfs.destroy(fs, destroy=True, recursive=True)
    except Exception:
        pass


@pytest.fixture
def target_root(auto_zfs_pool):
    """Create a real ZFS filesystem to receive backups; destroy on teardown."""
    if auto_zfs_pool is None:
        pytest.skip(
            'ZFS pool not available — install ZFS and grant NOPASSWD sudo for zpool/zfs, '
            'or run ./setup_test_pool.sh first'
        )
    name = f'{auto_zfs_pool.name}/zfsbackup_target'
    existing = Filesystem(name)
    if zfs.exists(existing):
        zfs.destroy(existing, destroy=True, recursive=True)
    try:
        fs = zfs.create.filesystem(name)
    except Exception as e:
        pytest.skip(f'ZFS filesystem creation failed (permissions not delegated?): {e}')
    yield name
    try:
        zfs.destroy(fs, destroy=True, recursive=True)
    except Exception:
        pass


@pytest.fixture
def source_cfg_path(tmp_path, source_fs):
    """Write a client YAML config pointing to *source_fs* with a remote destination."""
    cfg = tmp_path / 'client.yaml'
    cfg.write_text(
        f"datasets:\n"
        f"  - name: {source_fs.name}\n"
        f"    frequency: 1s\n"
        f"    retention:\n"
        f"      1h: 1d\n"
        f"    remote:\n"
        f"      - destination: server\n"
        f"destinations:\n"
        f"  server:\n"
        f"    url: http://127.0.0.1:18082\n"
        f"snapshot_prefix: autosnap\n"
        f"client_id_file: {tmp_path}/client_id\n"
    )
    return cfg


@pytest.fixture
def server_cfg_path(tmp_path, source_fs, target_root):
    """Write a server YAML config with remote_backup enabled."""
    cfg = tmp_path / 'server.yaml'
    # datasets list must be non-empty; use the source as a disabled placeholder.
    cfg.write_text(
        f"datasets:\n"
        f"  - name: {source_fs.name}\n"
        f"    enabled: false\n"
        f"remote_backup:\n"
        f"  enabled: true\n"
        f"  target_dataset: {target_root}\n"
        f"api_host: 127.0.0.1\n"
        f"api_port: 18082\n"
        f"snapshot_prefix: autosnap\n"
    )
    return cfg


@pytest.fixture
def running_server(server_cfg_path):
    """Start a real ApiWorker on port 18082; wait until ready; tear down after the test."""
    import multiprocessing
    import requests
    from zfsbackup.workers import ApiWorker

    stop = multiprocessing.Event()
    worker = ApiWorker(server_cfg_path, stop, verbose=False)
    worker.start()

    deadline = time.time() + 15
    while time.time() < deadline:
        try:
            if requests.get('http://127.0.0.1:18082/health', timeout=1).ok:
                break
        except Exception:
            pass
        time.sleep(0.3)
    else:
        worker.terminate()
        pytest.fail('Server ApiWorker did not start within 15 seconds')

    yield 'http://127.0.0.1:18082'

    stop.set()
    worker.join(timeout=5)
    if worker.is_alive():
        worker.terminate()


# ---------------------------------------------------------------------------
# TestRealSnapshotManager
# ---------------------------------------------------------------------------

class TestRealSnapshotManager:
    """Verify snapshot creation, listing, and needs_snapshot logic on real ZFS."""

    def test_create_snapshot_visible_in_zfs_list(self, source_fs):
        config = _make_simple_config(source_fs)
        manager = DatasetManager(config)
        dsi = manager.datasets[0]

        manager.create_snapshot(dsi)

        names = list_real_snapshot_names(source_fs.name)
        assert len(names) == 1
        assert names[0].startswith('autosnap_')

    def test_needs_snapshot_false_after_create(self, source_fs):
        config = _make_simple_config(source_fs, frequency_seconds=3600)
        manager = DatasetManager(config)
        dsi = manager.datasets[0]

        manager.create_snapshot(dsi)

        assert manager.needs_snapshot(dsi) is False

    def test_needs_snapshot_true_with_stale_name(self, source_fs):
        # Real snapshot whose name encodes a 2-hour-old timestamp.
        zfs.snapshot(source_fs, snap_name_at(2))

        config = _make_simple_config(source_fs, frequency_seconds=3600)
        manager = DatasetManager(config)
        dsi = manager.datasets[0]

        assert manager.needs_snapshot(dsi) is True

    def test_list_snapshots_sorted_newest_first(self, source_fs):
        # Create 3 snapshots with names encoding 5h, 3h, 1h ago.
        for hours in (5, 3, 1):
            zfs.snapshot(source_fs, snap_name_at(hours))

        config = _make_simple_config(source_fs)
        manager = DatasetManager(config)
        dsi = manager.datasets[0]

        snaps = manager.list_snapshots(dsi)

        assert len(snaps) == 3
        # Each snapshot's parsed age should decrease (newest first).
        assert snaps[0].age < snaps[1].age < snaps[2].age

    def test_only_managed_snapshots_listed(self, source_fs):
        zfs.snapshot(source_fs, snap_name_at(1))  # managed
        zfs.snapshot(source_fs, 'manual_snap')     # unmanaged — no prefix_timestamp pattern

        config = _make_simple_config(source_fs)
        manager = DatasetManager(config)
        dsi = manager.datasets[0]

        snaps = manager.list_snapshots(dsi)

        assert len(snaps) == 1
        assert snaps[0].name.startswith('autosnap_')

    def test_config_property_written_to_zfs(self, source_fs):
        config = _make_simple_config(source_fs)
        manager = DatasetManager(config)
        dsi = manager.datasets[0]

        manager.sync_config_property(dsi)

        result = subprocess.run(
            [_zfs_cmd(), 'get', '-H', '-o', 'value', 'org.zfsbackup:config', source_fs.name],
            capture_output=True, text=True, check=True,
        )
        value = result.stdout.strip()
        assert value and value != '-'

    def test_config_property_roundtrip(self, source_fs):
        config = _make_simple_config(source_fs)
        manager = DatasetManager(config)
        dsi = manager.datasets[0]

        manager.sync_config_property(dsi)
        encoded = manager._get_prop(dsi.dataset, 'org.zfsbackup:config')

        assert encoded is not None
        decoded = DatasetConfig.from_property(encoded)
        assert decoded.name == dsi.config.name
        assert decoded.frequency == dsi.config.frequency
        assert len(decoded.retention_rules) == len(dsi.config.retention_rules)


# ---------------------------------------------------------------------------
# TestRealPruning
# ---------------------------------------------------------------------------

class TestRealPruning:
    """Verify retention policy enforcement against real ZFS snapshots."""

    def test_prune_destroys_excess_snapshots(self, source_fs):
        # 6 snapshots at 0.5h–5.5h ago (half-hour offsets avoid boundary timing issues).
        # Rule keeps one per 1h slot for 3h: slots [0-1h],[1-2h],[2-3h] → keep 3; prune 3.
        for h in [0.5, 1.5, 2.5, 3.5, 4.5, 5.5]:
            zfs.snapshot(source_fs, snap_name_at(h))

        config = BackupConfig(
            datasets=[
                DatasetConfig(
                    name=source_fs.name,
                    frequency=timedelta(hours=1),
                    retention_rules=[RetentionRule(timedelta(hours=1), timedelta(hours=3))],
                )
            ],
            snapshot_prefix='autosnap',
        )
        manager = DatasetManager(config)
        dsi = manager.datasets[0]

        manager.prune_snapshots(dsi)

        remaining = list_real_snapshot_names(source_fs.name)
        assert len(remaining) == 3

    def test_prune_keeps_newest_always(self, source_fs):
        # Snapshot so old that no retention tier covers it — must still survive.
        zfs.snapshot(source_fs, snap_name_at(100))

        config = BackupConfig(
            datasets=[
                DatasetConfig(
                    name=source_fs.name,
                    frequency=timedelta(hours=1),
                    retention_rules=[RetentionRule(timedelta(hours=1), timedelta(hours=3))],
                )
            ],
            snapshot_prefix='autosnap',
        )
        manager = DatasetManager(config)
        dsi = manager.datasets[0]

        manager.prune_snapshots(dsi)

        remaining = list_real_snapshot_names(source_fs.name)
        assert len(remaining) == 1

    def test_prune_respects_anchor_on_real_dataset(self, source_fs):
        # 3 snapshots; anchor set to oldest; rule only covers < 6h; oldest should survive.
        snap_old = snap_name_at(25)
        for h in (1, 5, 25):
            zfs.snapshot(source_fs, snap_name_at(h))

        config = BackupConfig(
            datasets=[
                DatasetConfig(
                    name=source_fs.name,
                    frequency=timedelta(hours=1),
                    retention_rules=[RetentionRule(timedelta(hours=1), timedelta(hours=6))],
                    remote=[RemoteDatasetConfig(destination='offsite')],
                )
            ],
            snapshot_prefix='autosnap',
            destinations={'offsite': Destination(url='http://localhost')},
        )
        manager = DatasetManager(config)
        dsi = manager.datasets[0]

        # Write the anchor as a real ZFS user property.
        manager.set_anchor(dsi, 'offsite', snap_old)

        manager.prune_snapshots(dsi)

        remaining = list_real_snapshot_names(source_fs.name)
        assert snap_old in remaining

    def test_dry_run_leaves_all_snapshots(self, source_fs):
        for h in range(1, 6):
            zfs.snapshot(source_fs, snap_name_at(h * 10))

        config = BackupConfig(
            datasets=[
                DatasetConfig(
                    name=source_fs.name,
                    frequency=timedelta(hours=1),
                    retention_rules=[RetentionRule(timedelta(hours=1), timedelta(hours=3))],
                )
            ],
            snapshot_prefix='autosnap',
            dry_run=True,
        )
        manager = DatasetManager(config)
        dsi = manager.datasets[0]

        manager.prune_snapshots(dsi)

        remaining = list_real_snapshot_names(source_fs.name)
        assert len(remaining) == 5


# ---------------------------------------------------------------------------
# TestRealSendReceive
# ---------------------------------------------------------------------------

class TestRealSendReceive:
    """End-to-end backup tests using two real daemon processes (client + server ApiWorker)."""

    def _setup_client(self, source_cfg_path):
        config = BackupConfig.from_file(source_cfg_path)
        manager = DatasetManager(config)
        dsi = manager.datasets[0]
        remote_mgr = RemoteBackupManager(config, manager)
        return config, manager, dsi, remote_mgr

    def _server_snap_names(self, target_root, source_fs, config):
        """Return short snapshot names present on the server-side target dataset."""
        client_id = ClientIdentity(config.client_id_file).client_id
        relative = source_fs.name.split('/', 1)[1]
        server_ds = f'{target_root}/{client_id}/{relative}'
        return list_real_snapshot_names(server_ds)

    def test_full_initial_backup_transfers_snapshot(
        self, source_fs, target_root, source_cfg_path, running_server
    ):
        config, manager, dsi, remote_mgr = self._setup_client(source_cfg_path)

        manager.create_snapshot(dsi)
        snaps = manager.list_snapshots(dsi)
        assert len(snaps) == 1

        result = remote_mgr.backup_dataset(dsi, dsi.config.remote[0])
        assert result is True

        server_snaps = self._server_snap_names(target_root, source_fs, config)
        assert snaps[0].name in server_snaps

    def test_incremental_backup_transfers_second_snapshot(
        self, source_fs, target_root, source_cfg_path, running_server
    ):
        config, manager, dsi, remote_mgr = self._setup_client(source_cfg_path)

        # First backup (full send).
        manager.create_snapshot(dsi)
        remote_mgr.backup_dataset(dsi, dsi.config.remote[0])

        # Wait so the second snapshot gets a distinct 1s-aligned name.
        time.sleep(2)
        manager.create_snapshot(dsi)
        snaps = manager.list_snapshots(dsi)
        assert len(snaps) == 2

        # Second backup (incremental send).
        result = remote_mgr.backup_dataset(dsi, dsi.config.remote[0])
        assert result is True

        server_snaps = self._server_snap_names(target_root, source_fs, config)
        assert len(server_snaps) == 2

    def test_anchor_set_to_latest_snapshot_name(
        self, source_fs, target_root, source_cfg_path, running_server
    ):
        config, manager, dsi, remote_mgr = self._setup_client(source_cfg_path)

        manager.create_snapshot(dsi)
        snaps = manager.list_snapshots(dsi)
        latest_name = snaps[0].name

        remote_mgr.backup_dataset(dsi, dsi.config.remote[0])

        anchor = manager.get_anchor(dsi, 'server')
        assert anchor == latest_name

    def test_fresh_anchor_skips_backup_in_worker(
        self, source_fs, target_root, source_cfg_path, running_server
    ):
        """RemoteBackupWorker._process_dataset should skip transfer when anchor is fresh."""
        from zfsbackup.workers import RemoteBackupWorker

        config, manager, dsi, remote_mgr = self._setup_client(source_cfg_path)

        manager.create_snapshot(dsi)
        remote_mgr.backup_dataset(dsi, dsi.config.remote[0])
        first_server_snaps = self._server_snap_names(target_root, source_fs, config)
        assert len(first_server_snaps) == 1

        # Simulate a second worker cycle immediately — anchor age < frequency (1s) → skip.
        worker = RemoteBackupWorker.__new__(RemoteBackupWorker)
        worker._remote_manager = remote_mgr
        worker._process_dataset(manager, dsi)

        second_server_snaps = self._server_snap_names(target_root, source_fs, config)
        assert len(second_server_snaps) == 1  # no new snapshot transferred

    def test_negotiate_reports_common_snapshot_after_full_backup(
        self, source_fs, target_root, source_cfg_path, running_server
    ):
        import requests as req

        config, manager, dsi, remote_mgr = self._setup_client(source_cfg_path)

        manager.create_snapshot(dsi)
        snaps = manager.list_snapshots(dsi)
        remote_mgr.backup_dataset(dsi, dsi.config.remote[0])

        client_id = ClientIdentity(config.client_id_file).client_id
        url = f'{running_server}/backup/{client_id}/{source_fs.name}/negotiate'
        resp = req.post(url, json={
            'snapshots': [s.name for s in snaps],
            'config': dsi.config.to_property(),
        })

        assert resp.status_code == 200
        data = resp.json()
        assert data['common_snapshot'] == snaps[0].name
