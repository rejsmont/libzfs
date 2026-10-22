"""Real ZFS integration tests for the zfsbackup daemon.

REQUIREMENTS:
- ZFS must be installed and available
- Either passwordless sudo for zpool/zfs OR run ./scenarios/setup_test_pool.sh first

SETUP (automatic, if you have NOPASSWD sudo for zpool/zfs):
  pytest -m real_zfs

SETUP (manual):
  ./scenarios/setup_test_pool.sh
  pytest -m real_zfs
  ./scenarios/cleanup_test_pool.sh
"""

import contextlib
import logging
import subprocess
import time
from unittest import mock
from datetime import datetime, timedelta, timezone
from typing import List

import pytest

import libzfseasy as zfs
from libzfseasy.types import Dataset, Filesystem, Snapshot
from libzfseasy.zfs import _zfs_cmd

from zfsbackup.backup_manager import (
    PROP_ANCHOR_PREFIX,
    PROP_CLIENT_ID,
    PROP_CONFIG,
    PROP_INFLIGHT_PREFIX,
    PROP_SOURCE_DATASET,
    DatasetManager,
)
from zfsbackup.config import (
    BackupConfig,
    DatasetConfig,
    Destination,
    RemoteDatasetConfig,
    RetentionRule,
)
from zfsbackup.remote import RemoteBackupManager


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
            'or run ./scenarios/setup_test_pool.sh first'
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
            'or run ./scenarios/setup_test_pool.sh first'
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


# --- end-to-end send/receive rig (B-7) ---------------------------------------
#
# A real, mTLS-only `ApiWorker` on loopback serves a config database whose
# `remote_backup.target_dataset` is a dataset on the test pool; a client identity
# is pinned in that database exactly as `zfsbackup-config enroll` would pin it
# (`store.trust`), and a client `RemoteBackupManager` whose destination carries
# the server's fingerprint backs a real source dataset up to it. Both ends run
# real `zfs send` / `zfs receive`.

#: The id the server binds to the pinned client certificate. It names the
#: server-side dataset: `<target>/<CLIENT_ID>/<source dataset without its pool>`.
CLIENT_ID = 'b7-client'
DEST = 'server'


@pytest.fixture
def api_port():
    """A free loopback port for this test's API worker."""
    import socket

    with socket.socket() as s:
        s.bind(('127.0.0.1', 0))
        return s.getsockname()[1]


@pytest.fixture
def server_cfg_path(tmp_path, source_fs, target_root, api_port):
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
        f"api_port: {api_port}\n"
        f"snapshot_prefix: autosnap\n"
    )
    return cfg


@pytest.fixture
def server_config_db(tmp_path, server_cfg_path):
    """Import `server_cfg_path` into a fresh SQLite config store under
    `tmp_path` and return the `ResolvedConfigPath` addressing it.

    The server config is read from this store with `read_config` and handed to
    a real `ApiWorker` (workers never open the DB); the same database holds the
    client pins (`store.trust`) and has its TLS directory (`<db>.tls`).
    """
    from zfsbackup.config.store import import_yaml, resolve_config_path

    db_path = tmp_path / 'server_config.db'
    resolved = resolve_config_path(str(db_path))
    import_yaml(server_cfg_path, resolved)
    return resolved


class _KilledStream:
    """The client's real `zfs send` handle, cut off after `limit` bytes: the
    next read raises, which is what a transport that died mid-transfer looks
    like to `RemoteBackupManager` (it closes the handle -- killing `zfs send`
    through the closed pipe -- and drops the websocket without a `done`)."""

    def __init__(self, handle, limit):
        self._handle, self._limit, self.sent = handle, limit, 0

    def read(self, size=-1):
        if self.sent >= self._limit:
            raise RuntimeError('test: the stream was killed')
        want = self._limit - self.sent if size is None or size < 0 else min(size, self._limit - self.sent)
        data = self._handle.read(want)
        self.sent += len(data)
        return data

    def close(self):
        return self._handle.close()


class E2E:
    """An enrolled client and a running server over a real pool.

    `source` is the client's dataset (one managed `DatasetManager` + a real
    `RemoteBackupManager`); `server_ds` is where its backups land."""

    def __init__(self, source, target_root, base, server_fp, client, client_tls,
                 manager, remote_mgr, caplog, monkeypatch):
        self.source = source
        self.target_root = target_root
        self.base = base
        self.server_fp = server_fp
        self.client = client                # the pinned client `Identity`
        self.client_tls = client_tls
        self.manager = manager
        self.dsi = manager.datasets[0]
        self.remote_mgr = remote_mgr
        self.caplog = caplog
        self.monkeypatch = monkeypatch
        relative = source.name.split('/', 1)[1]
        self.server_ds = f'{target_root}/{CLIENT_ID}/{relative}'

    # -- the client's data ------------------------------------------------------
    def snapshot(self, hours_ago):
        """A real managed snapshot named for `hours_ago`; returns its short name."""
        name = snap_name_at(hours_ago)
        zfs.snapshot(self.source, name)
        return name

    def write(self, name, mib):
        """Write `mib` MiB of incompressible data to `name` in the source's mountpoint."""
        mountpoint = _zfs_get(self.source.name, 'mountpoint')
        target = f'{mountpoint}/{name}'
        cmd = ['dd', 'if=/dev/urandom', f'of={target}', 'bs=1048576', f'count={mib}']
        result = subprocess.run(cmd, capture_output=True)
        if result.returncode != 0:      # a root-owned mountpoint (sudo-wrapped pool)
            result = subprocess.run(['sudo', '-n'] + cmd, capture_output=True)
        if result.returncode != 0:
            pytest.skip(f'cannot write into the test dataset at {mountpoint}: '
                        f'{result.stderr.decode(errors="replace").strip()}')

    # -- the transfer ------------------------------------------------------------
    def cycle(self):
        """One worker-style pass: `begin_cycle()`, then `backup_dataset` to the destination."""
        self.remote_mgr.begin_cycle()
        return self.remote_mgr.backup_dataset(self.dsi, self.dsi.config.remote[0])

    @contextlib.contextmanager
    def killed_streams(self, limit):
        """Within the block every `zfs send` the client starts dies after `limit`
        bytes. Yields the list of `_KilledStream`s started."""
        real = zfs.send.snapshot
        streams = []

        def killed(*args, **kwargs):
            stream = _KilledStream(real(*args, **kwargs), limit)
            streams.append(stream)
            return stream

        # `patch.object`, not `monkeypatch.setattr`: `snapshot` is a classmethod looked up through
        # the INSTANCE, and monkeypatch's undo would leave the bound method behind as an instance
        # attribute (shadowing the class); `patch.object` deletes what it added.
        with mock.patch.object(zfs.send, 'snapshot', killed):
            yield streams

    # -- what is on each side --------------------------------------------------------
    def local_snaps(self):
        return _snapshot_guids(self.source.name)

    def server_snaps(self):
        return _snapshot_guids(self.server_ds)

    def server_prop(self, prop):
        return _zfs_get(self.server_ds, prop)

    def server_local(self, prop):
        """`prop` on the server dataset if set LOCALLY there, else None."""
        return _zfs_get_local(self.server_ds, prop)

    @contextlib.contextmanager
    def properties_in_streams(self):
        """Within the block every `zfs send` the client starts is a `-p` stream: the
        source dataset's properties (the forged ones below included) travel in it. The
        real client never does this; a hostile one can."""
        real = zfs.send.snapshot

        def with_properties(*args, **kwargs):
            kwargs['properties'] = True
            return real(*args, **kwargs)

        with mock.patch.object(zfs.send, 'snapshot', with_properties):
            yield

    def forge_on_the_source(self):
        """Put hostile values for the server's ownership and config properties on the
        client's dataset, so that a `-p` stream carries them; -> the forged config."""
        evil = DatasetConfig(
            name='victim/data', frequency=timedelta(hours=1),
            retention_rules=[RetentionRule(timedelta(seconds=1), timedelta(seconds=1))],
        ).to_property()
        zfs.set(self.source, {
            PROP_CLIENT_ID: 'attacker',
            PROP_SOURCE_DATASET: 'victim/data',
            PROP_CONFIG: evil,
        })
        return evil

    def wait_for_resume_token(self, timeout=30):
        """The server's `receive_resume_token` once the aborted receive has saved it."""
        deadline = time.monotonic() + timeout
        token = '-'
        while time.monotonic() < deadline:
            if subprocess.run([_zfs_cmd(), 'list', '-H', self.server_ds],
                              capture_output=True).returncode == 0:
                token = self.server_prop('receive_resume_token')
                if token not in ('-', ''):
                    return token
            time.sleep(0.2)
        return token

    def log_lines(self):
        return [r.getMessage() for r in self.caplog.records if r.name.startswith('zfsbackup.remote')]


def _zfs_get(dataset, prop):
    result = subprocess.run(
        [_zfs_cmd(), 'get', '-H', '-o', 'value', prop, dataset],
        capture_output=True, text=True,
    )
    return result.stdout.strip() if result.returncode == 0 else None


def _zfs_get_local(dataset, prop):
    """`prop` of `dataset` if it is set LOCALLY (`zfs get -s local`), else `None`.

    `zfs get` without `-s` also reports an inherited or received value; the ownership
    properties must be local on the server (plan B-1b) or a stream could replace them."""
    result = subprocess.run(
        [_zfs_cmd(), 'get', '-H', '-o', 'value', '-s', 'local', prop, dataset],
        capture_output=True, text=True,
    )
    value = result.stdout.strip() if result.returncode == 0 else ''
    return value or None


def _zfs_get_source(dataset, prop):
    """The SOURCE column of `prop` on `dataset` (`local`, `received`, `inherited from ...`, `-`), else `None`."""
    result = subprocess.run(
        [_zfs_cmd(), 'get', '-H', '-o', 'source', prop, dataset],
        capture_output=True, text=True,
    )
    return result.stdout.strip() if result.returncode == 0 else None


def _zfs_get_received(dataset, prop):
    """`prop` of `dataset` if a RECEIVED value is recorded for it (`zfs get -s received`), else `None`."""
    result = subprocess.run(
        [_zfs_cmd(), 'get', '-H', '-o', 'value', '-s', 'received', prop, dataset],
        capture_output=True, text=True,
    )
    value = result.stdout.strip() if result.returncode == 0 else ''
    return value or None


def _snapshot_guids(dataset):
    """`{short snapshot name: guid}` of `dataset`'s own snapshots (empty when it does not exist)."""
    result = subprocess.run(
        [_zfs_cmd(), 'list', '-H', '-p', '-t', 'snapshot', '-d', '1', '-o', 'name,guid', dataset],
        capture_output=True, text=True,
    )
    guids = {}
    for line in result.stdout.splitlines():
        name, _, guid = line.partition('\t')
        if '@' in name:
            guids[name.split('@', 1)[1]] = int(guid)
    return guids


@pytest.fixture
def api_server(server_config_db, tmp_path, source_fs, target_root):
    """Start a real `ApiWorker` (HTTPS with required client certificates) for
    `server_config_db`, with an enrolled client pinned in that database; wait
    until it answers; tear it down after the test.

    -> `SimpleNamespace(base, port, server_fp, client, client_tls, other_client_tls)`:
    the `https://127.0.0.1:<port>` URL, the server certificate's fingerprint, the
    pinned client `Identity` with its `<db>.tls`-style directory, and a directory
    for a second, NOT enrolled client identity.
    """
    import os
    import types

    from _tls_support import client_identity, ensure_server_identity, pin_client, try_get
    from zfsbackup.config.store import dispose_all, read_config
    from zfsbackup.stopflag import StopFlag
    from zfsbackup.workers import ApiWorker

    if os.environ.get('MULTIPASS_VM'):
        pytest.skip('the end-to-end tests write into the pool mountpoints from this host; '
                    'run them on the machine that owns the pool')

    # What the supervisor would have done at startup / what `enroll` does.
    server = ensure_server_identity(server_config_db)
    client_tls = tmp_path / 'client.db.tls'
    client = client_identity(client_tls)
    pin_client(server_config_db, client, CLIENT_ID, server_fp=server.fingerprint)
    snapshot = read_config(server_config_db)
    dispose_all()                       # nothing in this process holds the database open

    port = snapshot.config.api_port
    stop = StopFlag()
    worker = ApiWorker(snapshot.config, stop, verbose=False, config_db=server_config_db)
    worker.start()
    try:
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            if not worker.is_alive():
                pytest.fail(f'the API worker exited early ({worker.exitcode})')
            answer = try_get(port, '/health')
            if isinstance(answer, tuple) and answer[0] == 200:
                break
            time.sleep(0.2)
        else:
            pytest.fail('the API worker did not answer /health within 30 seconds')

        yield types.SimpleNamespace(
            base=f'https://127.0.0.1:{port}', port=port, server_fp=server.fingerprint,
            client=client, client_tls=client_tls, other_client_tls=tmp_path / 'other.db.tls',
        )
    finally:
        stop.set()
        worker.join(timeout=30)
        if worker.is_alive():
            worker.terminate()
            worker.join(timeout=10)
        if worker.is_alive():
            worker.kill()
            worker.join(timeout=10)


def _client_config(source_fs, base, server_fp):
    """The client's config: `source_fs` backed up to the enrolled destination `server`."""
    return BackupConfig(
        datasets=[
            DatasetConfig(
                name=source_fs.name,
                frequency=timedelta(hours=24),
                retention_rules=[RetentionRule(timedelta(hours=1), timedelta(days=30))],
                remote=[RemoteDatasetConfig(destination=DEST)],
            )
        ],
        destinations={DEST: Destination(url=base, server_fingerprint=server_fp)},
        snapshot_prefix='autosnap',
    )


@pytest.fixture
def e2e(api_server, source_fs, target_root, caplog, monkeypatch):
    """The enrolled client against the running server, on real datasets."""
    caplog.set_level(logging.DEBUG, logger='zfsbackup.remote')
    config = _client_config(source_fs, api_server.base, api_server.server_fp)
    manager = DatasetManager(config)
    remote_mgr = RemoteBackupManager(
        config, manager, client_id=CLIENT_ID, tls_dir=api_server.client_tls
    )
    rig = E2E(source_fs, target_root, api_server.base, api_server.server_fp, api_server.client,
              api_server.client_tls, manager, remote_mgr, caplog, monkeypatch)
    try:
        yield rig
    finally:
        remote_mgr.close()
        # A killed stream leaves partial receive state on the server dataset;
        # `target_root`'s recursive destroy removes it with the rest.


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
            destinations={'offsite': Destination(url='https://localhost')},
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

MIB = 1024 * 1024


class TestRealSendReceive:
    """End-to-end backups over real mTLS: a client enrolled in the server's
    config database sends a real dataset to a real `ApiWorker`, which runs a real
    `zfs receive`.

    The bookkeeping contract checked after every transfer: the snapshots on the
    server carry the SAME guids as the client's (a byte-for-byte replica, not a
    look-alike), the anchor names the snapshot actually sent, and the
    `inflight` pin is cleared once the anchor is written.
    """

    def _assert_replicated(self, e2e, *names):
        local, server = e2e.local_snaps(), e2e.server_snaps()
        for name in names:
            assert name in server, f'{name} is not on the server: {sorted(server)}'
            assert server[name] == local[name], f'{name}: guid differs between client and server'

    # -- full and incremental ------------------------------------------------
    def test_full_send_replicates_the_snapshot(self, e2e):
        e2e.write('data.bin', 4)
        s1 = e2e.snapshot(2)

        assert e2e.cycle() is True

        assert list(e2e.server_snaps()) == [s1]
        self._assert_replicated(e2e, s1)
        assert any(f'Sending full snapshot {e2e.source.name}@{s1}' in m for m in e2e.log_lines())
        # The data arrived, not just the name.
        assert (_zfs_get(f'{e2e.server_ds}@{s1}', 'logicalreferenced')
                == _zfs_get(f'{e2e.source.name}@{s1}', 'logicalreferenced'))
        assert e2e.server_prop('receive_resume_token') == '-'

    def test_anchor_and_inflight_after_a_full_send(self, e2e):
        e2e.write('data.bin', 1)
        s1 = e2e.snapshot(2)

        assert e2e.cycle() is True

        assert e2e.manager.get_anchor(e2e.dsi, DEST) == s1
        assert e2e.manager.get_inflight(e2e.dsi, DEST) is None
        assert _zfs_get(e2e.source.name, f'{PROP_INFLIGHT_PREFIX}{DEST}') == '-'

    def test_server_dataset_is_stamped_with_its_owner(self, e2e):
        e2e.write('data.bin', 1)
        e2e.snapshot(2)

        assert e2e.cycle() is True

        assert e2e.server_prop(PROP_CLIENT_ID) == CLIENT_ID
        assert e2e.server_prop(PROP_SOURCE_DATASET) == e2e.source.name
        stored = DatasetConfig.from_property(e2e.server_prop(PROP_CONFIG))
        assert stored.name == e2e.source.name
        # The client's own bookkeeping properties are not part of the stream.
        assert e2e.server_prop(f'{PROP_ANCHOR_PREFIX}{DEST}') == '-'
        assert e2e.server_prop(f'{PROP_INFLIGHT_PREFIX}{DEST}') == '-'

    # -- the server's own properties are LOCAL and survive a hostile stream (plan B-1b) -------------
    def _assert_local_ownership(self, e2e):
        assert e2e.server_local(PROP_CLIENT_ID) == CLIENT_ID
        assert e2e.server_local(PROP_SOURCE_DATASET) == e2e.source.name

    def test_ownership_properties_are_local_on_the_server_after_each_receive(self, e2e):
        e2e.write('data.bin', 1)
        s1 = e2e.snapshot(3)
        assert e2e.cycle() is True
        self._assert_local_ownership(e2e)
        assert e2e.server_local(PROP_CONFIG) == e2e.dsi.config.to_property(DEST)

        e2e.write('more.bin', 1)
        s2 = e2e.snapshot(2)
        assert e2e.cycle() is True
        assert set(e2e.server_snaps()) == {s1, s2}
        self._assert_local_ownership(e2e)

    def test_nothing_received_is_recorded_for_the_excluded_properties(self, e2e):
        """`zfs receive -o <name>=<server's value>` for the three owned names (plan B-1b; `-x` does
        not protect a local value on a full `-F` receive of a `-p` stream): even a stream that
        carries them leaves no RECEIVED value behind."""
        e2e.forge_on_the_source()
        e2e.write('data.bin', 1)
        e2e.snapshot(2)
        with e2e.properties_in_streams():
            assert e2e.cycle() is True
        for prop in (PROP_CLIENT_ID, PROP_SOURCE_DATASET, PROP_CONFIG):
            assert _zfs_get_received(e2e.server_ds, prop) is None, prop

    def test_a_full_properties_stream_cannot_rewrite_the_ownership(self, e2e):
        e2e.forge_on_the_source()
        e2e.write('data.bin', 1)
        s1 = e2e.snapshot(2)
        with e2e.properties_in_streams():
            assert e2e.cycle() is True
        assert list(e2e.server_snaps()) == [s1]
        # local, and the BOUND id / the real client dataset -- not the forged ones
        assert e2e.server_local(PROP_CLIENT_ID) == CLIENT_ID
        assert e2e.server_local(PROP_SOURCE_DATASET) == e2e.source.name
        assert e2e.server_prop(PROP_CLIENT_ID) == CLIENT_ID
        assert e2e.server_prop(PROP_SOURCE_DATASET) == e2e.source.name

    def test_an_incremental_properties_stream_cannot_rewrite_the_ownership(self, e2e):
        e2e.write('data.bin', 1)
        s1 = e2e.snapshot(3)
        assert e2e.cycle() is True
        e2e.forge_on_the_source()
        e2e.write('more.bin', 1)
        s2 = e2e.snapshot(2)
        with e2e.properties_in_streams():
            assert e2e.cycle() is True
        assert set(e2e.server_snaps()) == {s1, s2}
        assert e2e.server_local(PROP_CLIENT_ID) == CLIENT_ID
        assert e2e.server_local(PROP_SOURCE_DATASET) == e2e.source.name

    def test_a_properties_stream_cannot_replace_the_config_the_server_prunes_by(self, e2e):
        """The negotiate writes the canonical config before the stream; the stream's own copy must
        not be the one in effect afterwards (the server prunes received datasets by it)."""
        evil = e2e.forge_on_the_source()
        e2e.write('data.bin', 1)
        e2e.snapshot(2)
        with e2e.properties_in_streams():
            assert e2e.cycle() is True
        assert e2e.server_prop(PROP_CONFIG) != evil, 'the forged config is the one in effect'
        assert e2e.server_local(PROP_CONFIG) == e2e.dsi.config.to_property(DEST)

    def test_the_next_cycle_restores_the_config_whatever_the_stream_carried(self, e2e):
        """Negotiate writes the config on every cycle, so the window is one cycle at most."""
        e2e.forge_on_the_source()
        e2e.write('data.bin', 1)
        e2e.snapshot(3)
        with e2e.properties_in_streams():
            assert e2e.cycle() is True
        e2e.write('more.bin', 1)
        e2e.snapshot(2)
        assert e2e.cycle() is True                           # an ordinary client this time
        assert e2e.server_local(PROP_CONFIG) == e2e.dsi.config.to_property(DEST)
        self._assert_local_ownership(e2e)

    # -- B-1b: the three owned values are in effect, as LOCAL values, right after any `-p` stream ------
    def _owned_expected(self, e2e):
        return {
            PROP_CONFIG: e2e.dsi.config.to_property(DEST),
            PROP_CLIENT_ID: CLIENT_ID,
            PROP_SOURCE_DATASET: e2e.source.name,
        }

    def _assert_owned_values_in_effect(self, e2e, forged_config):
        """`config`, `client_id` and `source_dataset` are LOCAL on the server dataset and equal to
        the server's own values (the stored canonical config, the bound id, the URL's dataset) --
        the effective value, the local value and the source column all agree, whatever the stream
        carried."""
        for prop, expected in self._owned_expected(e2e).items():
            assert e2e.server_local(prop) == expected, f'{prop}: local value'
            assert e2e.server_prop(prop) == expected, f'{prop}: effective value'
            assert _zfs_get_source(e2e.server_ds, prop) == 'local', f'{prop}: source'
        assert e2e.server_prop(PROP_CONFIG) != forged_config

    def test_owned_values_are_local_right_after_a_full_properties_stream(self, e2e):
        forged = e2e.forge_on_the_source()
        e2e.write('data.bin', 1)
        s1 = e2e.snapshot(2)
        with e2e.properties_in_streams():
            assert e2e.cycle() is True
        assert list(e2e.server_snaps()) == [s1]
        self._assert_owned_values_in_effect(e2e, forged)

    def test_owned_values_are_local_right_after_an_incremental_properties_stream(self, e2e):
        e2e.write('data.bin', 1)
        s1 = e2e.snapshot(3)
        assert e2e.cycle() is True
        forged = e2e.forge_on_the_source()
        e2e.write('more.bin', 1)
        s2 = e2e.snapshot(2)
        with e2e.properties_in_streams():
            assert e2e.cycle() is True
        assert set(e2e.server_snaps()) == {s1, s2}
        self._assert_owned_values_in_effect(e2e, forged)

    def test_owned_values_are_local_right_after_a_resumed_full_properties_stream(self, e2e):
        """The first `-p` send is killed; the resumed one (`zfs send -t`, the flags are in the
        token) completes it."""
        forged = e2e.forge_on_the_source()
        e2e.write('data.bin', 24)
        s1 = e2e.snapshot(3)
        with e2e.properties_in_streams(), e2e.killed_streams(4 * MIB) as streams:
            assert e2e.cycle() is False
        assert streams and streams[0].sent == 4 * MIB
        assert e2e.wait_for_resume_token() not in ('-', ''), 'the server saved no partial receive state'
        # the aborted receive already carried the server's values
        assert e2e.server_local(PROP_CLIENT_ID) == CLIENT_ID
        assert e2e.cycle() is True                           # resumes
        assert any(f'Resuming interrupted transfer for {e2e.source.name}@{s1}' in m for m in e2e.log_lines())
        assert list(e2e.server_snaps()) == [s1]
        self._assert_owned_values_in_effect(e2e, forged)

    def test_owned_values_are_local_right_after_a_resumed_incremental_properties_stream(self, e2e):
        e2e.write('base.bin', 1)
        s1 = e2e.snapshot(5)
        assert e2e.cycle() is True
        forged = e2e.forge_on_the_source()
        e2e.write('data.bin', 24)
        s2 = e2e.snapshot(3)
        with e2e.properties_in_streams(), e2e.killed_streams(4 * MIB) as streams:
            assert e2e.cycle() is False
        assert streams
        assert e2e.wait_for_resume_token() not in ('-', ''), 'the server saved no partial receive state'
        assert e2e.cycle() is True
        assert any(f'Resuming interrupted transfer for {e2e.source.name}@{s2}' in m for m in e2e.log_lines())
        assert set(e2e.server_snaps()) == {s1, s2}
        self._assert_owned_values_in_effect(e2e, forged)

    def test_owned_values_are_local_after_an_ordinary_stream_too(self, e2e):
        e2e.write('data.bin', 1)
        e2e.snapshot(2)
        assert e2e.cycle() is True
        self._assert_owned_values_in_effect(e2e, forged_config='')

    def test_incremental_send_transfers_the_second_snapshot(self, e2e):
        e2e.write('data.bin', 2)
        s1 = e2e.snapshot(3)
        assert e2e.cycle() is True

        e2e.write('more.bin', 2)
        s2 = e2e.snapshot(2)
        assert e2e.cycle() is True

        assert set(e2e.server_snaps()) == {s1, s2}
        self._assert_replicated(e2e, s1, s2)
        assert any(f'incremental from {s1}' in m for m in e2e.log_lines())
        assert e2e.manager.get_anchor(e2e.dsi, DEST) == s2
        assert e2e.manager.get_inflight(e2e.dsi, DEST) is None
        assert (_zfs_get(f'{e2e.server_ds}@{s2}', 'logicalreferenced')
                == _zfs_get(f'{e2e.source.name}@{s2}', 'logicalreferenced'))

    def test_incremental_send_across_an_intermediate_snapshot(self, e2e):
        # B-5 will make every managed intermediate arrive; for now the contract is
        # the base and the newest snapshot are on the server with matching guids.
        e2e.write('data.bin', 2)
        s1 = e2e.snapshot(4)
        assert e2e.cycle() is True

        e2e.write('more.bin', 1)
        e2e.snapshot(3)
        e2e.write('even_more.bin', 1)
        s3 = e2e.snapshot(2)
        assert e2e.cycle() is True

        self._assert_replicated(e2e, s1, s3)
        assert e2e.manager.get_anchor(e2e.dsi, DEST) == s3

    def test_a_cycle_with_nothing_new_sends_nothing(self, e2e):
        e2e.write('data.bin', 1)
        s1 = e2e.snapshot(2)
        assert e2e.cycle() is True
        before = e2e.server_snaps()

        assert e2e.cycle() is True

        assert e2e.server_snaps() == before
        assert e2e.manager.get_anchor(e2e.dsi, DEST) == s1
        assert e2e.manager.get_inflight(e2e.dsi, DEST) is None

    # -- resume after a killed stream ----------------------------------------
    def _kill_a_first_send(self, e2e, snapshot_hours_ago=3, data_mib=24, after=4 * MIB):
        """Write data, snapshot it, and kill the first send `after` bytes in.
        Leaves the server holding a resume token. -> the snapshot's short name."""
        e2e.write('data.bin', data_mib)
        s1 = e2e.snapshot(snapshot_hours_ago)
        with e2e.killed_streams(after) as streams:
            assert e2e.cycle() is False
        assert streams and streams[0].sent == after
        token = e2e.wait_for_resume_token()
        assert token not in ('-', ''), 'the server saved no partial receive state'
        return s1

    def test_a_killed_stream_leaves_the_pin_and_no_anchor(self, e2e):
        s1 = self._kill_a_first_send(e2e)

        assert e2e.server_snaps() == {}
        assert e2e.manager.get_anchor(e2e.dsi, DEST) is None
        # The pin survives a failed send: both ends stay protected from the pruner.
        assert e2e.manager.get_inflight(e2e.dsi, DEST) == ('', s1)
        assert any('Transfer failed' in m for m in e2e.log_lines())

    def test_resume_after_a_killed_stream_completes_the_snapshot(self, e2e):
        s1 = self._kill_a_first_send(e2e)

        assert e2e.cycle() is True

        logs = e2e.log_lines()
        assert any(f'Resuming interrupted transfer for {e2e.source.name}@{s1}' in m for m in logs)
        assert not any('sending fresh' in m for m in logs)
        assert list(e2e.server_snaps()) == [s1]
        self._assert_replicated(e2e, s1)
        assert e2e.server_prop('receive_resume_token') == '-'
        assert e2e.manager.get_anchor(e2e.dsi, DEST) == s1
        assert e2e.manager.get_inflight(e2e.dsi, DEST) is None
        assert (_zfs_get(f'{e2e.server_ds}@{s1}', 'logicalreferenced')
                == _zfs_get(f'{e2e.source.name}@{s1}', 'logicalreferenced'))

    def test_resume_is_followed_by_a_send_to_the_newest_snapshot(self, e2e):
        s1 = self._kill_a_first_send(e2e, snapshot_hours_ago=3)
        e2e.write('newer.bin', 1)
        s2 = e2e.snapshot(2)

        assert e2e.cycle() is True

        logs = e2e.log_lines()
        assert any(f'Resumed {e2e.source.name}@{s1}' in m for m in logs)
        assert any(f'incremental from {s1}' in m for m in logs)
        assert set(e2e.server_snaps()) == {s1, s2}
        self._assert_replicated(e2e, s1, s2)
        assert e2e.manager.get_anchor(e2e.dsi, DEST) == s2
        assert e2e.manager.get_inflight(e2e.dsi, DEST) is None

    def test_a_token_for_a_destroyed_snapshot_is_discarded_for_a_fresh_send(self, e2e):
        s1 = self._kill_a_first_send(e2e, snapshot_hours_ago=3)
        zfs.destroy(Snapshot(e2e.source, s1), destroy=True)
        e2e.write('newer.bin', 1)
        s2 = e2e.snapshot(2)

        assert e2e.cycle() is True

        logs = e2e.log_lines()
        assert any('Not resuming' in m and 'sending fresh' in m for m in logs)
        assert not any('Resuming interrupted transfer' in m for m in logs)
        assert list(e2e.server_snaps()) == [s2]
        self._assert_replicated(e2e, s2)
        assert e2e.server_prop('receive_resume_token') == '-'
        assert e2e.manager.get_anchor(e2e.dsi, DEST) == s2

    # -- the worker's scheduling -----------------------------------------------
    def _worker(self, e2e):
        from zfsbackup.stopflag import StopFlag
        from zfsbackup.workers import RemoteBackupWorker

        worker = RemoteBackupWorker(
            e2e.manager.config, StopFlag(), tls_dir=e2e.client_tls, client_id=CLIENT_ID
        )
        worker._remote_manager = e2e.remote_mgr
        return worker

    def test_fresh_anchor_skips_the_backup_in_the_worker(self, e2e):
        e2e.write('data.bin', 1)
        s1 = e2e.snapshot(3)
        assert e2e.cycle() is True
        e2e.snapshot(2)     # newer, but the anchor (3h) is younger than the 24h frequency

        self._worker(e2e)._process_dataset(e2e.manager, e2e.dsi)

        assert list(e2e.server_snaps()) == [s1]

    def test_stale_anchor_makes_the_worker_send(self, e2e):
        e2e.write('data.bin', 1)
        s1 = e2e.snapshot(3)
        assert e2e.cycle() is True
        s2 = e2e.snapshot(2)
        e2e.dsi.config.remote[0].frequency = timedelta(minutes=1)   # anchor is 3h old

        worker = self._worker(e2e)
        worker._remote_manager.begin_cycle()
        worker._process_dataset(e2e.manager, e2e.dsi)

        assert set(e2e.server_snaps()) == {s1, s2}
        self._assert_replicated(e2e, s1, s2)

    # -- protocol and trust ------------------------------------------------------
    def test_negotiate_reports_the_common_snapshot(self, e2e):
        from urllib.parse import quote

        from zfsbackup import identity
        from zfsbackup.protocol import protocol_headers

        e2e.write('data.bin', 1)
        s1 = e2e.snapshot(2)
        assert e2e.cycle() is True
        url = f'{e2e.base}/backup/{CLIENT_ID}/{quote(e2e.source.name, safe="/")}/negotiate'

        session = identity.pinned_session(e2e.client, e2e.server_fp, timeout=15)
        session.headers.update(protocol_headers())          # every endpoint but /health needs it (S-5)
        try:
            known = session.post(url, json={
                'snapshots': [s1], 'config': e2e.dsi.config.to_property(),
            })
            unknown = session.post(url, json={
                'snapshots': ['autosnap_19990101000000'], 'config': e2e.dsi.config.to_property(),
            })
        finally:
            session.close()

        assert known.status_code == 200
        assert known.json()['common_snapshot'] == s1
        assert known.json()['server_dataset'] == e2e.server_ds
        assert unknown.status_code == 200
        assert unknown.json()['common_snapshot'] is None

    def test_a_client_that_is_not_enrolled_gets_nothing_onto_the_pool(self, e2e, api_server):
        from _tls_support import client_identity

        client_identity(api_server.other_client_tls)        # a valid identity, but never pinned
        stranger = RemoteBackupManager(
            e2e.manager.config, e2e.manager, client_id=CLIENT_ID, tls_dir=api_server.other_client_tls
        )
        try:
            e2e.write('data.bin', 1)
            e2e.snapshot(2)

            stranger.begin_cycle()
            assert stranger.backup_dataset(e2e.dsi, e2e.dsi.config.remote[0]) is False
        finally:
            stranger.close()

        errors = [r.getMessage() for r in e2e.caplog.records
                  if r.name.startswith('zfsbackup.remote') and r.levelno >= logging.ERROR]
        assert any('does not trust this client' in m or 'connection reset after TLS handshake' in m
                   for m in errors), errors
        assert _zfs_get(e2e.server_ds, 'name') is None
        assert e2e.manager.get_anchor(e2e.dsi, DEST) is None
