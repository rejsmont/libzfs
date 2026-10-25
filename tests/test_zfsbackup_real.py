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


def list_real_snapshot_names_exact(dataset_name: str) -> List[str]:
    """Short names of the snapshots of *dataset_name* itself (not its descendants)."""
    result = subprocess.run(
        [_zfs_cmd(), 'list', '-H', '-t', 'snapshot', '-d', '1', '-o', 'name', dataset_name],
        capture_output=True, text=True,
    )
    return [l.strip().split('@', 1)[1] for l in result.stdout.splitlines() if '@' in l.strip()]


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

    def cycle_after_a_cut(self, attempts=20):
        """`cycle()` once the server has finished with a cut stream. The resume token is visible
        before the server's handler has released its claim on the dataset, and a negotiate in that
        window is answered `busy` (a retryable 503): the client skips the cycle, which is right.
        Retry while that is all that happened (the skip is logged); any other outcome is returned."""
        for _ in range(attempts):
            seen = len(self.log_lines())
            if self.cycle():
                return True
            if not any('skipping' in m for m in self.log_lines()[seen:]):
                return False
            time.sleep(0.5)
        return False

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

    -> `SimpleNamespace(base, port, server_fp, client, client_tls, other_client_tls, restart)`:
    the `https://127.0.0.1:<port>` URL, the server certificate's fingerprint, the
    pinned client `Identity` with its `<db>.tls`-style directory, a directory
    for a second, NOT enrolled client identity, and `restart()`, which replaces the
    worker with a fresh one on the same port.
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
    running = {}

    def launch():
        stop = StopFlag()
        worker = ApiWorker(snapshot.config, stop, config_db=server_config_db)
        worker.start()
        running.update(stop=stop, worker=worker)
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            if not worker.is_alive():
                pytest.fail(f'the API worker exited early ({worker.exitcode})')
            answer = try_get(port, '/health')
            if isinstance(answer, tuple) and answer[0] == 200:
                return
            time.sleep(0.2)
        pytest.fail('the API worker did not answer /health within 30 seconds')

    def shutdown():
        stop, worker = running.pop('stop', None), running.pop('worker', None)
        if worker is None:
            return
        stop.set()
        worker.join(timeout=30)
        if worker.is_alive():
            worker.terminate()
            worker.join(timeout=10)
        if worker.is_alive():
            worker.kill()
            worker.join(timeout=10)

    def restart():
        """Stop the API worker and start a new one on the same port: everything the old one held in
        memory (the pending configurations of datasets not yet received) is gone."""
        shutdown()
        launch()

    try:
        launch()
        yield types.SimpleNamespace(
            base=f'https://127.0.0.1:{port}', port=port, server_fp=server.fingerprint,
            client=client, client_tls=client_tls, other_client_tls=tmp_path / 'other.db.tls',
            restart=restart,
        )
    finally:
        shutdown()


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
        # Created oldest first: on a normal pool creation order and name order agree. (Since S-1
        # "the newest" is the newest by createtxg, so the order of creation matters here.)
        for h in [5.5, 4.5, 3.5, 2.5, 1.5, 0.5]:
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
        for h in (25, 5, 1):                 # oldest created first (the newest is the newest by createtxg)
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
# TestRealPruneHeldAndCloned (B-4: batch fallback, held/cloned pre-skip)
# ---------------------------------------------------------------------------

def _zfs_run(*args):
    """Run a real `zfs` subcommand; skip (never error) when this account may not run it."""
    r = subprocess.run([_zfs_cmd(), *args], capture_output=True, text=True)
    if r.returncode != 0:
        if 'permission denied' in r.stderr.lower():
            pytest.skip(f'zfs {args[0]} not permitted for this account: {r.stderr.strip()}')
        raise AssertionError(f'zfs {" ".join(args)} failed ({r.returncode}): {r.stderr.strip()}')
    return r.stdout


def _prune_records(caplog, level):
    return [r.getMessage() for r in caplog.records
            if r.levelno == level and r.name == 'zfsbackup.backup_manager']


def _pruning_manager(source_fs, recursive=False, dry_run=False):
    config = BackupConfig(
        datasets=[DatasetConfig(
            name=source_fs.name, frequency=timedelta(hours=1), recursive=recursive,
            retention_rules=[RetentionRule(timedelta(hours=1), timedelta(hours=2))],
        )],
        snapshot_prefix='autosnap', dry_run=dry_run,
    )
    manager = DatasetManager(config)
    return manager, manager.datasets[0]


def _old_snapshots(source_fs, count, recursive=False):
    """`count` snapshots a 1h/2h retention prunes, plus a fresh one it keeps; oldest created first (on a
    normal pool creation order and name order agree). -> `(fresh, olds oldest first)`."""
    olds = [snap_name_at(3 + i) for i in reversed(range(count))]
    fresh = snap_name_at(0.2)
    for name in olds + [fresh]:
        zfs.snapshot(source_fs, name, recursive=recursive)
    return fresh, olds


class TestRealPruneHeldAndCloned:
    """A held snapshot fails `zfs destroy` as `dataset is busy`/`it's being held`, a cloned one as `has
    dependent clones`, and a failed call destroys nothing: the prune must keep exactly those names and
    still destroy the rest of its 20+ snapshot batch."""

    def test_held_and_cloned_snapshots_are_kept_while_the_rest_of_the_batch_goes(
            self, source_fs, caplog):
        fresh, olds = _old_snapshots(source_fs, 24)
        held, cloned = olds[5], olds[17]
        clone = f"{source_fs.name.split('/')[0]}/test_b4_clone"
        manager, dsi = _pruning_manager(source_fs)
        try:
            _zfs_run('hold', 'keep', f'{source_fs.name}@{held}')
            _zfs_run('clone', f'{source_fs.name}@{cloned}', clone)
            with mock.patch('zfsbackup.backup_manager.zfs.destroy', wraps=zfs.destroy) as destroy:
                with caplog.at_level(logging.INFO, logger='zfsbackup.backup_manager'):
                    manager.prune_snapshots(dsi)
            assert sorted(list_real_snapshot_names(source_fs.name)) == sorted([fresh, held, cloned])
            assert [len(c.args[0]) for c in destroy.call_args_list] == [20, 2]     # no failed call at all
            assert _prune_records(caplog, logging.ERROR) == []
            warnings = _prune_records(caplog, logging.WARNING)
            assert len(warnings) == 2
            assert any('zfs holds' in w and held in w for w in warnings)
            assert any(clone in w and cloned in w for w in warnings)
        finally:
            subprocess.run([_zfs_cmd(), 'release', 'keep', f'{source_fs.name}@{held}'], capture_output=True)
            subprocess.run([_zfs_cmd(), 'destroy', clone], capture_output=True)

    def test_a_dry_run_with_a_cloned_snapshot_makes_no_destroy_call(self, source_fs, caplog):
        fresh, olds = _old_snapshots(source_fs, 4)
        clone = f"{source_fs.name.split('/')[0]}/test_b4_clone_dry"
        manager, dsi = _pruning_manager(source_fs, dry_run=True)
        try:
            _zfs_run('clone', f'{source_fs.name}@{olds[1]}', clone)
            with mock.patch('zfsbackup.backup_manager.zfs.destroy', wraps=zfs.destroy) as destroy:
                with caplog.at_level(logging.INFO, logger='zfsbackup.backup_manager'):
                    manager.prune_snapshots(dsi)
            destroy.assert_not_called()
            assert _prune_records(caplog, logging.ERROR) == []
            assert len(list_real_snapshot_names(source_fs.name)) == 5
        finally:
            subprocess.run([_zfs_cmd(), 'destroy', clone], capture_output=True)

    def test_a_hold_taken_after_the_listing_fails_the_batch_and_the_others_still_go(
            self, source_fs, caplog):
        fresh, olds = _old_snapshots(source_fs, 22)
        late = olds[9]
        manager, dsi = _pruning_manager(source_fs)
        real_destroy = zfs.destroy
        state = {'held': False}

        def racing_destroy(snaps, *a, **kw):
            if not state['held'] and isinstance(snaps, list) and len(snaps) > 1:
                state['held'] = True
                _zfs_run('hold', 'keep', f'{source_fs.name}@{late}')
            return real_destroy(snaps, *a, **kw)

        try:
            with mock.patch('zfsbackup.backup_manager.zfs.destroy', side_effect=racing_destroy) as destroy:
                with caplog.at_level(logging.INFO, logger='zfsbackup.backup_manager'):
                    manager.prune_snapshots(dsi)
            assert sorted(list_real_snapshot_names(source_fs.name)) == sorted([fresh, late])
            sizes = [len(c.args[0]) for c in destroy.call_args_list]
            assert sizes[0] == 20 and 20 not in sizes[1:]       # the first batch failed; the rest went alone
            assert len(_prune_records(caplog, logging.ERROR)) >= 1
            warnings = _prune_records(caplog, logging.WARNING)
            assert len(warnings) == 1 and late in warnings[0] and 'zfs holds' in warnings[0]
            # next cycle: still held, retried alone, no second warning
            with caplog.at_level(logging.INFO, logger='zfsbackup.backup_manager'):
                manager.prune_snapshots(dsi)
            assert len(_prune_records(caplog, logging.WARNING)) == 1
        finally:
            subprocess.run([_zfs_cmd(), 'release', 'keep', f'{source_fs.name}@{late}'], capture_output=True)

    def test_a_pin_set_between_the_failed_batch_and_the_retry_keeps_the_snapshot(self, source_fs, caplog):
        """The remote worker pins a pair while the fallback retries a failed batch: the single retry of a
        pinned name must not be issued (the pruner re-reads the pins right before every destroy)."""
        fresh, olds = _old_snapshots(source_fs, 8)
        late, pinned = olds[2], olds[5]
        manager, dsi = _pruning_manager(source_fs)
        real_destroy = zfs.destroy
        state = {'raced': False}

        def racing_destroy(snaps, *a, **kw):
            if not state['raced'] and isinstance(snaps, list) and len(snaps) > 1:
                state['raced'] = True
                _zfs_run('hold', 'keep', f'{source_fs.name}@{late}')               # the batch will fail ...
                _zfs_run('set', f'{PROP_INFLIGHT_PREFIX}offsite={pinned},{olds[6]}', source_fs.name)  # ... and this pins two
            return real_destroy(snaps, *a, **kw)

        try:
            with mock.patch('zfsbackup.backup_manager.zfs.destroy', side_effect=racing_destroy) as destroy:
                with caplog.at_level(logging.INFO, logger='zfsbackup.backup_manager'):
                    manager.prune_snapshots(dsi)
            remaining = list_real_snapshot_names(source_fs.name)
            assert sorted(remaining) == sorted([fresh, late, pinned, olds[6]])
            retried = [c.args[0][0].short for c in destroy.call_args_list[1:]]
            assert pinned not in retried and olds[6] not in retried
            assert len([m for m in _prune_records(caplog, logging.INFO)
                        if 'became an anchor or in-flight snapshot' in m]) == 2
        finally:
            subprocess.run([_zfs_cmd(), 'release', 'keep', f'{source_fs.name}@{late}'], capture_output=True)

    def test_releasing_the_hold_lets_the_snapshot_go_on_the_next_cycle(self, source_fs, caplog):
        fresh, olds = _old_snapshots(source_fs, 3)
        held = olds[1]
        manager, dsi = _pruning_manager(source_fs)
        try:
            _zfs_run('hold', 'keep', f'{source_fs.name}@{held}')
            manager.prune_snapshots(dsi)
            assert sorted(list_real_snapshot_names(source_fs.name)) == sorted([fresh, held])
            _zfs_run('release', 'keep', f'{source_fs.name}@{held}')
            with caplog.at_level(logging.INFO, logger='zfsbackup.backup_manager'):
                manager.prune_snapshots(dsi)
            assert list_real_snapshot_names(source_fs.name) == [fresh]
            assert any(held in m for m in _prune_records(caplog, logging.INFO)
                       if 'could not be destroyed earlier' in m)
        finally:
            subprocess.run([_zfs_cmd(), 'release', 'keep', f'{source_fs.name}@{held}'], capture_output=True)

    def test_a_recursive_dataset_with_a_child_snapshot_hold_prunes_every_name_but_that_one(
            self, source_fs, caplog):
        child = zfs.create.filesystem(f'{source_fs.name}/child')
        fresh, olds = _old_snapshots(source_fs, 6, recursive=True)
        held = olds[2]
        manager, dsi = _pruning_manager(source_fs, recursive=True)
        try:
            _zfs_run('hold', 'keep', f'{child.name}@{held}')
            with caplog.at_level(logging.INFO, logger='zfsbackup.backup_manager'):
                manager.prune_snapshots(dsi)
            for ds in (source_fs.name, child.name):
                assert sorted(list_real_snapshot_names_exact(ds)) == sorted([fresh, held]), ds
            warnings = _prune_records(caplog, logging.WARNING)
            assert len(warnings) == 1 and held in warnings[0] and 'descendant' in warnings[0]
            # releasing the child's hold lets the name go (with the child's copy) on the next cycle
            _zfs_run('release', 'keep', f'{child.name}@{held}')
            manager.prune_snapshots(dsi)
            for ds in (source_fs.name, child.name):
                assert list_real_snapshot_names_exact(ds) == [fresh], ds
        finally:
            subprocess.run([_zfs_cmd(), 'release', 'keep', f'{child.name}@{held}'], capture_output=True)


# ---------------------------------------------------------------------------
# TestRealForgedReceivedProperty (B-1d: a newline in a received property)
# ---------------------------------------------------------------------------

class TestRealForgedReceivedProperty:
    """A `zfs send -p` stream carries the sender's `org.zfsbackup:anchor.*` property into the receiving
    dataset as a RECEIVED value, and a value may hold a newline, which real `zfs get -H` prints as extra lines.
    Under a recursive client-side ancestor the descendants' anchors are read in one `zfs get -r`: the read
    must be refused (the ancestor is skipped this cycle, nothing destroyed, one ERROR) and prune again once
    the property is fixed. Needs `zfs send`/`receive` and `set` permission on the test pool; skips otherwise.

    NOT RUN in the authoring session (no pool on the author's machine): run it with `pytest -m real_zfs`."""

    @staticmethod
    def _receive(snapshot, target):
        """`zfs send -p snapshot | zfs receive target`, or skip when this account may not."""
        send = subprocess.Popen([_zfs_cmd(), 'send', '-p', snapshot], stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE)
        recv = subprocess.run([_zfs_cmd(), 'receive', target], stdin=send.stdout,
                              capture_output=True, text=True)
        send.stdout.close()
        send_err = send.stderr.read().decode(errors='replace')
        send.stderr.close()
        send.wait()
        failed = send.returncode != 0 or recv.returncode != 0
        if failed:
            text = (send_err + recv.stderr).lower()
            if 'permission denied' in text or 'not permitted' in text:
                pytest.skip(f'zfs send/receive not permitted for this account: {text.strip()}')
            raise AssertionError(f'send | receive failed: {send_err.strip()} {recv.stderr.strip()}')

    def test_a_received_anchor_with_a_newline_skips_the_recursive_ancestor_until_it_is_fixed(
            self, source_fs, target_root, caplog):
        parent = f'{target_root}/anc'
        child = f'{parent}/rx'
        evil = 'org.zfsbackup:anchor.evil'
        # the sender's dataset holds a hostile value; `zfs set` refuses a newline only on some versions
        r = subprocess.run([_zfs_cmd(), 'set', f'{evil}=junk\npool/x\ttype\tfilesystem\t-', source_fs.name],
                           capture_output=True, text=True)
        if r.returncode != 0:
            pytest.skip(f'this zfs refuses a newline in a user property: {r.stderr.strip()}')
        zfs.snapshot(source_fs, snap_name_at(0.1))
        sent = list_real_snapshot_names_exact(source_fs.name)[-1]
        try:
            zfs.create.filesystem(parent)
            self._receive(f'{source_fs.name}@{sent}', child)
            source_line = _zfs_run('get', '-H', '-o', 'source', evil, child).strip()
            assert source_line == 'received', 'the premise: the hostile value arrived as a RECEIVED property'

            olds = [snap_name_at(3 + i) for i in reversed(range(3))]
            for name in olds + [snap_name_at(0.2)]:
                zfs.snapshot(Filesystem(parent), name)
            config = BackupConfig(
                datasets=[DatasetConfig(
                    name=parent, frequency=timedelta(hours=1), recursive=True,
                    retention_rules=[RetentionRule(timedelta(hours=1), timedelta(hours=2))],
                )],
                snapshot_prefix='autosnap',
            )
            manager = DatasetManager(config)
            dsi = manager.datasets[0]

            with caplog.at_level(logging.INFO, logger='zfsbackup.backup_manager'):
                manager.prune_snapshots(dsi)
                manager.prune_snapshots(dsi)               # a second cycle: still skipped, and quiet
            for name in olds:
                assert name in list_real_snapshot_names_exact(parent), f'{name} was destroyed'
            errors = [m for m in _prune_records(caplog, logging.ERROR) if 'Cannot read anchors' in m]
            assert len(errors) == 1 and parent in errors[0]

            # fixed: a LOCAL value replaces the received one, the strict read is clean, pruning resumes
            _zfs_run('set', f'{evil}=fine', child)
            caplog.clear()
            with caplog.at_level(logging.INFO, logger='zfsbackup.backup_manager'):
                manager.prune_snapshots(dsi)
            remaining = list_real_snapshot_names_exact(parent)
            assert [o for o in olds if o in remaining] != olds, 'pruning did not resume'
        finally:
            _destroy_quietly(parent)


# ---------------------------------------------------------------------------
# TestRealNewlinePropertiesAndPruning (the two-phase org.zfsbackup:* property read)
# ---------------------------------------------------------------------------

class TestRealNewlinePropertiesAndPruning:
    """`zfs get -H` prints a value raw, so a value with a newline is several lines. The anchor read is two-phase:
    which `org.zfsbackup:*` properties a dataset has is read without any value column, then only those are read as
    values. So a newline in a property that is not ours -- on the pool root (inherited by everything below it) or
    on the backed-up dataset itself -- no longer stops pruning, while a newline in one of OUR values (an anchor)
    still makes the read refuse, once, until it is fixed. Needs `set` permission on the test pool and a zfs that
    accepts a newline in a user property; skips otherwise (never errors). Everything set here is cleared in
    `finally` (the pool root's property with `zfs inherit`); only the test pool and `source_fs` are touched.

    NOT RUN in the authoring session: run it with `ZFSB_REAL_ZFS=1 pytest -m real_zfs -k NewlineProperties`."""

    NEWLINE = 'first line\nsecond line'

    @staticmethod
    def _set_or_skip(dataset, prop, value):
        r = subprocess.run([_zfs_cmd(), 'set', f'{prop}={value}', dataset], capture_output=True, text=True)
        if r.returncode != 0:
            pytest.skip(f'zfs set {prop} on {dataset} not possible here (permission, or this zfs refuses a '
                        f'newline in a user property): {r.stderr.strip()}')

    @staticmethod
    def _inherit_quietly(dataset, prop):
        subprocess.run([_zfs_cmd(), 'inherit', prop, dataset], capture_output=True)

    @staticmethod
    def _six_snapshots(source_fs):
        for h in [5.5, 4.5, 3.5, 2.5, 1.5, 0.5]:          # oldest first, as in TestRealPruning
            zfs.snapshot(source_fs, snap_name_at(h))

    @staticmethod
    def _manager(source_fs):
        config = BackupConfig(
            datasets=[DatasetConfig(
                name=source_fs.name, frequency=timedelta(hours=1),
                retention_rules=[RetentionRule(timedelta(hours=1), timedelta(hours=3))],
            )],
            snapshot_prefix='autosnap',
        )
        manager = DatasetManager(config)
        return manager, manager.datasets[0]

    def test_a_newline_on_the_pool_root_and_on_an_unrelated_property_does_not_stop_pruning(
            self, source_fs, auto_zfs_pool, caplog):
        root = auto_zfs_pool.name
        note, own, rooted = 'user:zfsb_note', 'user:zfsb_own', 'org.zfsbackup:anchor.rootonly'
        try:
            self._set_or_skip(root, note, self.NEWLINE)
            # an org.zfsbackup name the dataset only INHERITS (no descendant sets it): not discovered, not read
            self._set_or_skip(root, rooted, self.NEWLINE)
            self._set_or_skip(source_fs.name, own, self.NEWLINE)
            premise = _zfs_run('get', '-H', '-o', 'source', note, source_fs.name).strip()
            assert premise.startswith('inherited from'), 'the premise: the dataset inherits the root\'s value'
            self._six_snapshots(source_fs)
            manager, dsi = self._manager(source_fs)
            with caplog.at_level(logging.INFO, logger='zfsbackup.backup_manager'):
                manager.prune_snapshots(dsi)
            assert [m for m in _prune_records(caplog, logging.ERROR) if 'Cannot read anchors' in m] == []
            assert len(list_real_snapshot_names_exact(source_fs.name)) == 3, 'pruning did not go ahead'
        finally:
            self._inherit_quietly(root, note)
            self._inherit_quietly(root, rooted)
            self._inherit_quietly(source_fs.name, own)

    def test_a_newline_in_an_anchor_value_still_refuses_the_read_once_and_pruning_resumes_when_fixed(
            self, source_fs, caplog):
        anchor = 'org.zfsbackup:anchor.offsite'
        try:
            self._six_snapshots(source_fs)
            self._set_or_skip(source_fs.name, anchor, self.NEWLINE)
            manager, dsi = self._manager(source_fs)
            with caplog.at_level(logging.INFO, logger='zfsbackup.backup_manager'):
                manager.prune_snapshots(dsi)
                manager.prune_snapshots(dsi)                # still broken: skipped again, and quiet
            assert len(list_real_snapshot_names_exact(source_fs.name)) == 6, 'something was destroyed'
            errors = [m for m in _prune_records(caplog, logging.ERROR) if 'Cannot read anchors' in m]
            assert len(errors) == 1 and source_fs.name in errors[0]

            self._set_or_skip(source_fs.name, anchor, 'one line')       # fixed
            caplog.clear()
            with caplog.at_level(logging.INFO, logger='zfsbackup.backup_manager'):
                manager.prune_snapshots(dsi)
            assert len(list_real_snapshot_names_exact(source_fs.name)) < 6, 'pruning did not resume'
        finally:
            self._inherit_quietly(source_fs.name, anchor)


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
        assert e2e.cycle_after_a_cut() is True                           # resumes
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
        assert e2e.cycle_after_a_cut() is True   # the token shows before the server lets go: a busy 503 is possible
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

        assert e2e.cycle_after_a_cut() is True

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

        assert e2e.cycle_after_a_cut() is True

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

        assert e2e.cycle_after_a_cut() is True

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


# ---------------------------------------------------------------------------
# S-1: ordering by createtxg. A snapshot's NAME is a label (and can be hand-made,
# or dated by a wrong clock); the order of creation is `createtxg`. Each test makes
# the two disagree on a real pool.
# ---------------------------------------------------------------------------

FUTURE = 'autosnap_20990101000000'        # hand-made: dated far in the future, created FIRST
PAST = 'autosnap_20200101000000'          # created LATER, dated in the past

NTP_MESSAGE = 'system clock is behind the newest snapshot; check NTP'


def _txg(dataset: str, short: str) -> int:
    return int(_zfs_get(f'{dataset}@{short}', 'createtxg'))


def _hourly_manager(source_fs, rules=None, frequency=timedelta(hours=1), **kwargs):
    config = BackupConfig(
        datasets=[DatasetConfig(
            name=source_fs.name, frequency=frequency,
            retention_rules=rules if rules is not None else [],
        )],
        snapshot_prefix='autosnap', **kwargs,
    )
    return DatasetManager(config)


class TestRealCreatetxgOrder:
    def test_the_listing_carries_the_columns_and_orders_by_txg(self, source_fs):
        zfs.snapshot(source_fs, FUTURE)
        zfs.snapshot(source_fs, PAST)
        manager = _hourly_manager(source_fs)
        dsi = manager.datasets[0]

        snaps = manager.list_snapshots(dsi)

        assert [s.name for s in snaps] == [PAST, FUTURE]            # created later = newer
        assert snaps[0].createtxg == _txg(source_fs.name, PAST) > snaps[1].createtxg
        assert snaps[1].createtxg == _txg(source_fs.name, FUTURE)
        assert all(isinstance(s.guid, int) and s.guid > 0 for s in snaps)
        assert snaps[0].guid != snaps[1].guid
        assert [s.userrefs for s in snaps] == [0, 0]
        assert [s.clones for s in snaps] == [(), ()]
        assert [s.encrypted for s in snaps] == [False, False]
        assert dsi.snapshots == snaps

    def test_the_guids_are_the_ones_zfs_reports(self, source_fs):
        zfs.snapshot(source_fs, snap_name_at(2))
        manager = _hourly_manager(source_fs)
        (snap,) = manager.list_snapshots(manager.datasets[0])
        assert snap.guid == _snapshot_guids(source_fs.name)[snap.name]

    def test_a_future_dated_newest_is_due_once_and_a_new_snapshot_ends_it(self, source_fs, caplog):
        zfs.snapshot(source_fs, FUTURE)
        manager = _hourly_manager(source_fs)
        dsi = manager.datasets[0]

        with caplog.at_level(logging.WARNING, logger='zfsbackup.backup_manager'):
            assert manager.needs_snapshot(dsi) is True
            assert manager.needs_snapshot(dsi) is True
            assert len([r for r in caplog.records if NTP_MESSAGE in r.getMessage()]) == 1

            created = manager.create_snapshot(dsi)

            assert created is not None
            assert created.createtxg == _txg(source_fs.name, created.name)
            assert dsi.snapshots[0].name == created.name             # the newest by txg, though dated earlier
            assert manager.needs_snapshot(dsi) is False
            assert manager.needs_snapshot(dsi) is False
            assert len([r for r in caplog.records if NTP_MESSAGE in r.getMessage()]) == 1

    def test_a_past_dated_newer_snapshot_is_the_newest(self, source_fs):
        """Dated 2020 but created last: due (its name is years old), and ONE snapshot ends it."""
        zfs.snapshot(source_fs, snap_name_at(0.1))
        zfs.snapshot(source_fs, PAST)
        manager = _hourly_manager(source_fs)
        dsi = manager.datasets[0]

        assert manager.needs_snapshot(dsi) is True
        assert manager.list_snapshots(dsi)[0].name == PAST
        manager.create_snapshot(dsi)
        assert manager.needs_snapshot(dsi) is False

    def test_a_backwards_clock_that_regenerates_a_taken_name_is_not_due(self, source_fs, caplog):
        manager = _hourly_manager(source_fs, frequency=timedelta(hours=24))
        dsi = manager.datasets[0]
        taken = manager._generate_snapshot_name(dsi)
        zfs.snapshot(source_fs, taken)
        zfs.snapshot(source_fs, 'autosnap_20990101000000')           # the txg-newest: dated in the future
        if manager._generate_snapshot_name(dsi) != taken:
            pytest.skip('crossed a period boundary while setting up')
        before = list_real_snapshot_names(source_fs.name)

        with caplog.at_level(logging.WARNING, logger='zfsbackup.backup_manager'):
            for _ in range(3):
                assert manager.needs_snapshot(dsi) is False

        assert sorted(list_real_snapshot_names(source_fs.name)) == sorted(before)
        collisions = [r for r in caplog.records if 'already exists' in r.getMessage()]
        assert len(collisions) == 1 and taken in collisions[0].getMessage()

    def test_a_suffixed_name_is_unmanaged_and_never_pruned(self, source_fs):
        suffixed = 'autosnap_20200101000000_keep'
        zfs.snapshot(source_fs, suffixed)
        zfs.snapshot(source_fs, snap_name_at(0.5))
        manager = _hourly_manager(
            source_fs, rules=[RetentionRule(timedelta(hours=1), timedelta(hours=3))])
        dsi = manager.datasets[0]

        assert suffixed not in [s.name for s in manager.list_snapshots(dsi)]
        manager.prune_snapshots(dsi)

        assert suffixed in list_real_snapshot_names(source_fs.name)

    def test_create_snapshot_relists_and_returns_the_real_txg(self, source_fs):
        manager = _hourly_manager(source_fs)
        dsi = manager.datasets[0]
        created = manager.create_snapshot(dsi)
        assert created is not None
        assert created.createtxg == _txg(source_fs.name, created.name)
        assert created.guid == _snapshot_guids(source_fs.name)[created.name]
        assert dsi.snapshots[0] is created


class TestRealRetentionOrder:
    RULES = [RetentionRule(timedelta(hours=1), timedelta(hours=6))]

    def test_the_oldest_snapshot_of_a_slot_is_kept_and_so_is_the_newest(self, source_fs):
        """Three snapshots in one hourly slot, then a recent one (the newest). B9: slots by name,
        the OLDEST of a slot is kept; the newest by txg is always kept."""
        hour = (datetime.now(timezone.utc) - timedelta(hours=2)).replace(minute=0, second=0, microsecond=0)
        names = [f"autosnap_{(hour + timedelta(minutes=m)).strftime('%Y%m%d%H%M%S')}" for m in (5, 25, 45)]
        for name in names:                                            # created in ascending order
            zfs.snapshot(source_fs, name)
        recent = snap_name_at(0.5)
        zfs.snapshot(source_fs, recent)
        manager = _hourly_manager(source_fs, rules=self.RULES)

        manager.prune_snapshots(manager.datasets[0])

        assert sorted(list_real_snapshot_names(source_fs.name)) == sorted([names[0], recent])

    def test_the_newest_by_txg_survives_whatever_its_name_says(self, source_fs):
        """`FUTURE` (created first) is NOT the newest; `PAST` (created last, dated years back,
        past every retention tier) is, and survives the prune."""
        zfs.snapshot(source_fs, FUTURE)
        zfs.snapshot(source_fs, PAST)
        manager = _hourly_manager(source_fs, rules=self.RULES)

        manager.prune_snapshots(manager.datasets[0])

        assert sorted(list_real_snapshot_names(source_fs.name)) == sorted([FUTURE, PAST])


class TestRealSendOrderFollowsTxg:
    """The snapshot SENT is the newest by createtxg, whatever its name says."""

    def test_the_snapshot_created_last_is_sent_though_its_name_is_older(self, e2e):
        e2e.write('data.bin', 2)
        first = e2e.snapshot(1)                    # created first, newest NAME
        last = e2e.snapshot(5)                     # created last: the newest by txg

        assert e2e.cycle() is True

        assert list(e2e.server_snaps()) == [last]
        assert e2e.server_snaps()[last] == e2e.local_snaps()[last]
        assert e2e.manager.get_anchor(e2e.dsi, DEST) == last
        assert any(f'Sending full snapshot {e2e.source.name}@{last}' in m for m in e2e.log_lines())
        assert first not in e2e.server_snaps()

    def test_a_past_dated_snapshot_created_later_is_sent_incrementally(self, e2e):
        """The old by-name logic called the first snapshot 'latest' forever, saw it on the server,
        and never sent the one made after it."""
        e2e.write('data.bin', 2)
        first = e2e.snapshot(2)
        assert e2e.cycle() is True

        e2e.write('more.bin', 2)
        later = e2e.snapshot(8)                    # dated 8h ago, created after `first`
        assert e2e.cycle() is True

        assert set(e2e.server_snaps()) == {first, later}
        assert e2e.server_snaps()[later] == e2e.local_snaps()[later]
        assert e2e.manager.get_anchor(e2e.dsi, DEST) == later
        assert any(f'incremental from {first}' in m for m in e2e.log_lines())

    def test_a_cycle_with_nothing_new_by_txg_sends_nothing(self, e2e):
        e2e.write('data.bin', 1)
        e2e.snapshot(1)
        last = e2e.snapshot(5)
        assert e2e.cycle() is True
        before = e2e.server_snaps()

        assert e2e.cycle() is True

        assert e2e.server_snaps() == before
        assert e2e.manager.get_anchor(e2e.dsi, DEST) == last


# ---------------------------------------------------------------------------
# Milestone B: R0 (the receive creates the leaf), S6-d (`server_copy`), over real encryption.
#
# zfs refuses a raw full stream, and a plain full under an encrypted target, onto a dataset that
# already exists (capture M5/Z1), so negotiate only makes the parent chain and the first full stream
# CREATES the server dataset, carrying the server's own values with `-o` (LOCAL on the result).
# Each class below is one source/target layout; the tests are shared (`_NewDatasetTests`).
# Passphrase keyfiles live in `tmp_path`: nothing ever prompts.
# ---------------------------------------------------------------------------

PASSPHRASE = 'zfsb-r0-test-passphrase'


def _create_dataset(name, *, encrypted, keyfile=None):
    """`zfs create` a test dataset (encrypted with a passphrase keyfile when asked). Skips -- never
    errors -- when this account may not create it."""
    cmd = ['create']
    if encrypted:
        cmd += ['-o', 'encryption=aes-256-gcm', '-o', 'keyformat=passphrase',
                '-o', f'keylocation=file://{keyfile}']
    _zfs_run(*cmd, name)
    return Filesystem(name)


def _destroy_quietly(name):
    subprocess.run([_zfs_cmd(), 'destroy', '-r', name], capture_output=True)


class _NewDatasetTests:
    """Shared by the layout classes. Set `SOURCE_ENCRYPTED`, `TARGET_ENCRYPTED`, `COPY` (`'raw'` or
    `'plain'`: what the server then holds), `ROOT` (`'leaf'`, `'target'` or None: its encryption root)
    and `OVERWRITE_REFUSED` (zfs refuses a full stream over an existing placeholder)."""

    SOURCE_ENCRYPTED = False
    TARGET_ENCRYPTED = False
    COPY = 'plain'
    ROOT = None
    OVERWRITE_REFUSED = False

    # -- the layout -----------------------------------------------------------------
    @pytest.fixture
    def keyfile(self, tmp_path):
        path = tmp_path / 'zfsb-test.key'
        path.write_text(PASSPHRASE)
        path.chmod(0o600)
        return path

    @pytest.fixture
    def source_fs(self, auto_zfs_pool, keyfile):
        if auto_zfs_pool is None:
            pytest.skip('ZFS pool not available')
        name = f'{auto_zfs_pool.name}/zfsbackup_source'
        if zfs.exists(Filesystem(name)):
            zfs.destroy(Filesystem(name), destroy=True, recursive=True)
        fs = _create_dataset(name, encrypted=self.SOURCE_ENCRYPTED, keyfile=keyfile)
        yield fs
        _destroy_quietly(name)

    @pytest.fixture
    def target_root(self, auto_zfs_pool, keyfile):
        if auto_zfs_pool is None:
            pytest.skip('ZFS pool not available')
        name = f'{auto_zfs_pool.name}/zfsbackup_target'
        if zfs.exists(Filesystem(name)):
            zfs.destroy(Filesystem(name), destroy=True, recursive=True)
        _create_dataset(name, encrypted=self.TARGET_ENCRYPTED, keyfile=keyfile)
        yield name
        _destroy_quietly(name)

    # -- helpers --------------------------------------------------------------------
    def negotiate(self, e2e, snapshots):
        """A raw `POST .../negotiate` as the enrolled client; -> the JSON answer."""
        from urllib.parse import quote

        from zfsbackup import identity
        from zfsbackup.protocol import protocol_headers

        url = f'{e2e.base}/backup/{CLIENT_ID}/{quote(e2e.source.name, safe="/")}/negotiate'
        session = identity.pinned_session(e2e.client, e2e.server_fp, timeout=15)
        session.headers.update(protocol_headers())
        try:
            resp = session.post(url, json={
                'snapshots': list(snapshots), 'config': e2e.dsi.config.to_property(DEST)})
        finally:
            session.close()
        assert resp.status_code == 200, resp.text
        return resp.json()

    def assert_owned_locally(self, e2e):
        for prop, expected in ((PROP_CLIENT_ID, CLIENT_ID), (PROP_SOURCE_DATASET, e2e.source.name),
                               (PROP_CONFIG, e2e.dsi.config.to_property(DEST))):
            assert _zfs_get_source(e2e.server_ds, prop) == 'local', prop
            assert e2e.server_local(prop) == expected, prop
            assert _zfs_get_received(e2e.server_ds, prop) is None, prop

    def assert_encryption_shape(self, e2e):
        if self.ROOT is None:
            assert e2e.server_prop('encryption') == 'off'
            return
        assert e2e.server_prop('encryption') == 'aes-256-gcm'
        root = e2e.server_ds if self.ROOT == 'leaf' else e2e.target_root
        assert e2e.server_prop('encryptionroot') == root
        # a raw copy arrives without its key; a plain one under an encrypted target is keyed by the target
        assert e2e.server_prop('keystatus') == ('unavailable' if self.ROOT == 'leaf' else 'available')

    def first_send(self, e2e):
        e2e.write('data.bin', 2)
        return e2e.snapshot(2)

    # -- negotiate and the first full -----------------------------------------------
    def test_negotiate_of_a_missing_leaf_makes_the_parent_only_and_says_none(self, e2e):
        self.first_send(e2e)
        parent = e2e.server_ds.rsplit('/', 1)[0]

        answer = self.negotiate(e2e, [])

        assert answer['server_copy'] == 'none'
        assert answer['common_snapshot'] is None
        assert answer['server_dataset'] == e2e.server_ds
        assert _zfs_get(parent, 'name') == parent                  # the parent chain exists ...
        assert _zfs_get(e2e.server_ds, 'name') is None             # ... the leaf does not (R0)

    def test_the_first_full_creates_the_leaf_with_the_owned_values_local(self, e2e):
        s1 = self.first_send(e2e)

        assert e2e.cycle() is True

        assert list(e2e.server_snaps()) == [s1]
        assert e2e.server_snaps()[s1] == e2e.local_snaps()[s1]
        self.assert_owned_locally(e2e)
        self.assert_encryption_shape(e2e)
        assert e2e.server_prop('receive_resume_token') == '-'
        assert e2e.manager.get_anchor(e2e.dsi, DEST) == s1
        assert any(f'Sending full snapshot {e2e.source.name}@{s1}' in m for m in e2e.log_lines())

    def test_server_copy_is_reported_for_what_the_server_holds(self, e2e):
        s1 = self.first_send(e2e)
        assert self.negotiate(e2e, [s1])['server_copy'] == 'none'          # nothing there yet
        assert e2e.cycle() is True

        answer = self.negotiate(e2e, [s1])

        assert answer['server_copy'] == self.COPY
        assert answer['common_snapshot'] == s1

    def test_a_placeholder_without_snapshots_is_none(self, e2e):
        s1 = self.first_send(e2e)
        _zfs_run('create', '-p', e2e.server_ds)                    # what a pre-creating server left behind

        assert self.negotiate(e2e, [s1])['server_copy'] == 'none'

    def test_an_incremental_follows_and_the_owned_values_stay_local(self, e2e):
        s1 = self.first_send(e2e)
        assert e2e.cycle() is True
        e2e.write('more.bin', 1)
        s2 = e2e.snapshot(1)

        assert e2e.cycle() is True

        assert set(e2e.server_snaps()) == {s1, s2}
        assert e2e.server_snaps()[s2] == e2e.local_snaps()[s2]
        self.assert_owned_locally(e2e)
        self.assert_encryption_shape(e2e)
        assert any(f'incremental from {s1}' in m for m in e2e.log_lines())
        assert self.negotiate(e2e, [s2])['server_copy'] == self.COPY

    # -- a cut first receive ------------------------------------------------------------
    def _cut_first_receive(self, e2e):
        e2e.write('data.bin', 24)
        s1 = e2e.snapshot(3)
        with e2e.killed_streams(4 * MIB) as streams:
            assert e2e.cycle() is False
        assert streams and streams[0].sent == 4 * MIB
        token = e2e.wait_for_resume_token()
        assert token not in ('-', ''), 'the server saved no partial receive state'
        return s1

    def test_a_cut_first_receive_leaves_a_visible_dataset_with_the_values_already_local(self, e2e):
        """Z3: the partial NEW dataset exists, carries the receive token itself, and has the `-o` values."""
        self._cut_first_receive(e2e)

        assert _zfs_get(e2e.server_ds, 'name') == e2e.server_ds
        assert e2e.server_snaps() == {}
        self.assert_owned_locally(e2e)

    def test_a_fresh_stream_after_a_cut_first_receive_recreates_the_dataset_with_the_values(self, e2e):
        """`receive -A` destroys the whole partial dataset; the full stream recreates it, no `-F`, and the
        values read before the abort are LOCAL on the new dataset."""
        s1 = self._cut_first_receive(e2e)
        zfs.destroy(Snapshot(e2e.source, s1), destroy=True)        # the token is dead: a fresh send
        e2e.write('newer.bin', 1)
        s2 = e2e.snapshot(2)

        assert e2e.cycle_after_a_cut() is True

        logs = e2e.log_lines()
        assert any('Not resuming' in m and 'sending fresh' in m for m in logs)
        assert list(e2e.server_snaps()) == [s2]
        assert e2e.server_snaps()[s2] == e2e.local_snaps()[s2]
        assert e2e.server_prop('receive_resume_token') == '-'
        self.assert_owned_locally(e2e)
        self.assert_encryption_shape(e2e)
        assert e2e.manager.get_anchor(e2e.dsi, DEST) == s2

    def test_the_cut_receive_resumes_into_the_same_dataset(self, e2e):
        s1 = self._cut_first_receive(e2e)

        assert e2e.cycle_after_a_cut() is True

        assert any(f'Resuming interrupted transfer for {e2e.source.name}@{s1}' in m for m in e2e.log_lines())
        assert list(e2e.server_snaps()) == [s1]
        self.assert_owned_locally(e2e)
        self.assert_encryption_shape(e2e)

    # -- a restart between negotiate and stream ------------------------------------------
    def test_a_restart_between_negotiate_and_stream_is_refused_with_negotiate_again(
            self, e2e, api_server, monkeypatch):
        """The configuration of a dataset that does not exist yet lives in the API worker's memory only."""
        self.first_send(e2e)
        real_negotiate = e2e.remote_mgr._negotiate
        restarted = []

        def negotiate_then_restart(*args, **kwargs):
            answer = real_negotiate(*args, **kwargs)
            api_server.restart()
            restarted.append(True)
            return answer

        monkeypatch.setattr(e2e.remote_mgr, '_negotiate', negotiate_then_restart)

        assert e2e.cycle() is False

        assert restarted == [True]
        assert any('negotiate again' in m for m in e2e.log_lines()), e2e.log_lines()
        assert _zfs_get(e2e.server_ds, 'name') is None             # nothing was received or created
        assert e2e.server_snaps() == {}
        assert e2e.manager.get_anchor(e2e.dsi, DEST) is None

        monkeypatch.undo()
        assert e2e.cycle() is True                                 # the next cycle negotiates again
        assert len(e2e.server_snaps()) == 1
        self.assert_owned_locally(e2e)

    # -- a pre-existing dataset ----------------------------------------------------------
    def test_a_full_stream_over_an_existing_placeholder(self, e2e):
        """A dataset that exists without snapshots (the parent made for a child that was backed up
        first, or a pre-creating server's leaf). Where zfs refuses an encrypted full over it, the
        client is told why in the server's words; where it does not (plain, unencrypted target), the
        full replaces it."""
        s1 = self.first_send(e2e)
        _zfs_run('create', '-p', e2e.server_ds)

        ok = e2e.cycle()

        if self.OVERWRITE_REFUSED:
            assert ok is False
            assert any('placeholder' in m and 'overwrite an unencrypted one with an encrypted one' in m
                       for m in e2e.log_lines()), e2e.log_lines()
            assert e2e.server_snaps() == {}
            # the operator moves the placeholder aside; the next cycle sends the full stream
            _zfs_run('rename', e2e.server_ds, f'{e2e.server_ds}_aside')
            assert e2e.cycle() is True
            assert list(e2e.server_snaps()) == [s1]
            self.assert_owned_locally(e2e)
            self.assert_encryption_shape(e2e)
        else:
            assert ok is True
            assert list(e2e.server_snaps()) == [s1]
            self.assert_owned_locally(e2e)


class TestRealRawFullIntoAMissingLeafUnderAnUnencryptedTarget(_NewDatasetTests):
    SOURCE_ENCRYPTED, TARGET_ENCRYPTED = True, False
    COPY, ROOT, OVERWRITE_REFUSED = 'raw', 'leaf', True


class TestRealRawFullIntoAMissingLeafUnderAnEncryptedTarget(_NewDatasetTests):
    SOURCE_ENCRYPTED, TARGET_ENCRYPTED = True, True
    COPY, ROOT, OVERWRITE_REFUSED = 'raw', 'leaf', True


class TestRealPlainFullIntoAMissingLeafUnderAnEncryptedTarget(_NewDatasetTests):
    """The leaf inherits the target's encryption (its encryption root is the target) and its key."""

    SOURCE_ENCRYPTED, TARGET_ENCRYPTED = False, True
    COPY, ROOT, OVERWRITE_REFUSED = 'plain', 'target', True


class TestRealPlainFullIntoAMissingLeafUnderAnUnencryptedTarget(_NewDatasetTests):
    """The control: nothing encrypted anywhere; a full over an existing placeholder still works."""

    SOURCE_ENCRYPTED, TARGET_ENCRYPTED = False, False
    COPY, ROOT, OVERWRITE_REFUSED = 'plain', None, False


# ---------------------------------------------------------------------------
# Milestone B (B5-b): the `-i` chain over a real pool. One cycle sends every managed snapshot newer
# than the common one, oldest first, and each reaches the server with the guid it has on the client
# (an equal guid is the same snapshot). Plain: `-L -c`; raw (an encrypted source with its key
# unloaded): `-w`. Self-contained -- its own fixtures and helpers -- so it does not depend on the
# layout classes above.
# ---------------------------------------------------------------------------

CHAIN_PASSPHRASE = 'zfsb-chain-test-passphrase'


def _chain_run(*args):
    """A real `zfs` subcommand for this class; skip (never error) when the account may not run it."""
    return _zfs_run(*args)


class _ChainTests:
    ENCRYPTED_SOURCE = False

    @pytest.fixture
    def source_fs(self, auto_zfs_pool, tmp_path):
        if auto_zfs_pool is None:
            pytest.skip('ZFS pool not available')
        name = f'{auto_zfs_pool.name}/zfsbackup_source'
        if zfs.exists(Filesystem(name)):
            zfs.destroy(Filesystem(name), destroy=True, recursive=True)
        cmd = ['create']
        if self.ENCRYPTED_SOURCE:
            keyfile = tmp_path / 'chain.key'
            keyfile.write_text(CHAIN_PASSPHRASE)
            keyfile.chmod(0o600)
            cmd += ['-o', 'encryption=aes-256-gcm', '-o', 'keyformat=passphrase',
                    '-o', f'keylocation=file://{keyfile}']
        _chain_run(*cmd, name)
        yield Filesystem(name)
        subprocess.run([_zfs_cmd(), 'destroy', '-r', name], capture_output=True)

    def snapshots(self, e2e, hours_ago, mib=1):
        """One data file and one snapshot per entry of `hours_ago` (oldest first); -> the names."""
        names = []
        for i, hours in enumerate(hours_ago):
            e2e.write(f'f{hours}.bin', mib)
            names.append(e2e.snapshot(hours))
        return names

    def assert_on_the_server(self, e2e, names):
        local, server = e2e.local_snaps(), e2e.server_snaps()
        assert set(names) <= set(server), (names, sorted(server))
        for name in names:
            assert server[name] == local[name], f'{name}: the server holds another snapshot of that name'


class TestRealChain(_ChainTests):
    """A plain dataset: every snapshot made since the last cycle reaches the server."""

    def test_a_chain_of_five_snapshots_every_one_reaches_the_server_with_its_guid(self, e2e):
        first = self.snapshots(e2e, [10])[0]
        assert e2e.cycle() is True                                 # the full send of the only snapshot
        assert list(e2e.server_snaps()) == [first]

        more = self.snapshots(e2e, [8, 6, 4, 2])
        assert e2e.cycle() is True

        self.assert_on_the_server(e2e, [first] + more)
        assert e2e.manager.get_anchor(e2e.dsi, DEST) == more[-1]
        assert e2e.manager.get_inflight(e2e.dsi, DEST) is None
        lines = e2e.log_lines()
        assert any(f'Sending 4 snapshots of {e2e.source.name} to {DEST}' in m for m in lines)
        for prev, name in zip([first] + more, more):
            assert any(f'incremental from {prev}' in m and name in m for m in lines), (prev, name)
        assert not [r for r in e2e.caplog.records if r.name.startswith('zfsbackup.remote') and r.levelno >= logging.ERROR]

    def test_a_full_send_is_only_the_newest_and_the_next_cycle_continues_from_it(self, e2e):
        snaps = self.snapshots(e2e, [9, 7, 5])
        assert e2e.cycle() is True
        assert list(e2e.server_snaps()) == [snaps[-1]]              # no backfill of the older ones
        later = self.snapshots(e2e, [3, 1])
        assert e2e.cycle() is True
        self.assert_on_the_server(e2e, [snaps[-1]] + later)
        assert set(e2e.server_snaps()) == {snaps[-1]} | set(later)

    def test_the_chain_follows_createtxg_when_the_names_disagree(self, e2e):
        """Created in the order a, b, c but named newest-first: the chain is the creation order."""
        a = self.snapshots(e2e, [1])[0]
        assert e2e.cycle() is True
        b = self.snapshots(e2e, [9])[0]                             # dated older, created after `a`
        c = self.snapshots(e2e, [5])[0]
        assert e2e.cycle() is True
        self.assert_on_the_server(e2e, [a, b, c])
        assert e2e.manager.get_anchor(e2e.dsi, DEST) == c

    def test_a_chain_cut_in_the_middle_continues_from_the_last_snapshot_that_arrived(self, e2e):
        first = self.snapshots(e2e, [10])[0]
        assert e2e.cycle() is True
        more = self.snapshots(e2e, [8, 6, 4], mib=8)

        calls = []
        real = zfs.send.snapshot

        def cut_the_second(*args, **kwargs):
            calls.append(1)
            stream = real(*args, **kwargs)
            return _KilledStream(stream, 2 * 1048576) if len(calls) == 2 else stream

        with mock.patch.object(zfs.send, 'snapshot', cut_the_second):
            assert e2e.cycle() is False                            # step 2 of 3 died
        assert set(e2e.server_snaps()) >= {first, more[0]}
        assert more[2] not in e2e.server_snaps()
        assert e2e.manager.get_anchor(e2e.dsi, DEST) == more[0]    # the last good step
        assert e2e.wait_for_resume_token() not in ('-', '')        # the server kept the cut receive

        assert e2e.cycle_after_a_cut() is True                 # the next cycle resumes it, then goes on
        self.assert_on_the_server(e2e, [first] + more)
        assert e2e.manager.get_anchor(e2e.dsi, DEST) == more[2]


class TestRealRawChain(_ChainTests):
    """An encrypted source whose key is unloaded: the chain is raw (`-w`) and the server holds a raw
    copy (its own encryption root, no key)."""

    ENCRYPTED_SOURCE = True

    def test_a_raw_chain_with_the_key_unloaded_reaches_the_server_with_its_guids(self, e2e):
        first = self.snapshots(e2e, [10])[0]
        assert e2e.cycle() is True                                 # raw full (the key is loaded; auto = raw)
        more = self.snapshots(e2e, [8, 6, 4])
        _chain_run('unmount', e2e.source.name)
        _chain_run('unload-key', e2e.source.name)
        assert _zfs_get(e2e.source.name, 'keystatus') == 'unavailable'

        assert e2e.cycle() is True

        self.assert_on_the_server(e2e, [first] + more)
        assert e2e.server_prop('encryption') == 'aes-256-gcm'
        assert e2e.server_prop('encryptionroot') == e2e.server_ds   # a raw copy is its own root ...
        assert e2e.server_prop('keystatus') == 'unavailable'        # ... and arrives without its key
        assert e2e.manager.get_anchor(e2e.dsi, DEST) == more[-1]
        assert any(f'Sending 3 snapshots of {e2e.source.name} to {DEST}' in m for m in e2e.log_lines())
        assert not [r for r in e2e.caplog.records if r.name.startswith('zfsbackup.remote') and r.levelno >= logging.ERROR]


# ---------------------------------------------------------------------------
# The size of a user-property value: `api.ZFS_USER_PROPERTY_MAXLEN` (8192, refused at `>=`) is where
# the canonical config stops fitting `org.zfsbackup:config` (negotiate's `zfs set`, the receive's
# `-o`). Pinned here against the real thing on every supported OpenZFS.
# ---------------------------------------------------------------------------

class TestRealUserPropertyLimit:
    PROP = 'org.zfsbackup:probe'

    def _set(self, dataset, length):
        cmd = [_zfs_cmd(), 'set', f'{self.PROP}={"a" * length}', dataset]
        return subprocess.run(cmd, capture_output=True, text=True)

    def test_the_limit_is_where_the_server_puts_it(self, source_fs):
        from zfsbackup.api import ZFS_USER_PROPERTY_MAXLEN

        assert ZFS_USER_PROPERTY_MAXLEN == 8192
        ok = self._set(source_fs.name, ZFS_USER_PROPERTY_MAXLEN - 1)
        if ok.returncode != 0 and 'permission denied' in ok.stderr.lower():
            pytest.skip(f'zfs set not permitted for this account: {ok.stderr.strip()}')
        assert ok.returncode == 0, ok.stderr
        assert len(_zfs_get(source_fs.name, self.PROP)) == ZFS_USER_PROPERTY_MAXLEN - 1

        refused = self._set(source_fs.name, ZFS_USER_PROPERTY_MAXLEN)
        assert refused.returncode != 0, 'zfs accepted a value of 8192 bytes: ZFS_USER_PROPERTY_MAXLEN is too small'
        assert len(_zfs_get(source_fs.name, self.PROP)) == ZFS_USER_PROPERTY_MAXLEN - 1     # unchanged

    def test_a_longer_value_is_refused_too(self, source_fs):
        refused = self._set(source_fs.name, 8193)
        assert refused.returncode != 0

    def test_the_largest_canonical_config_negotiate_accepts_fits_a_property(self, source_fs):
        """A canonical config of 8188 characters (the largest base64 length under the limit) is storable."""
        from zfsbackup.api import canonical_dataset_config

        cfg = None
        for pad in range(5800, 6300):
            candidate = DatasetConfig(name='pool/' + 'x' * pad)
            if len(candidate.to_property()) == 8188:
                cfg = candidate
                break
        assert cfg is not None
        value = canonical_dataset_config(cfg.to_property())
        assert len(value) == 8188
        zfs.set(source_fs, {PROP_CONFIG: value})
        assert _zfs_get_local(source_fs.name, PROP_CONFIG) == value


# ---------------------------------------------------------------------------
# TestRealCheck (C13: `zfsbackup-config check` against real snapshots)
# ---------------------------------------------------------------------------

class TestRealCheck:
    """`check` reads real `zfs list` / `zfs get` output: a dataset with a managed snapshot is OK, the lack of a
    daemon is CRIT (exit 1), a backdated newest snapshot is CRIT, and `--prometheus --output` writes a
    parseable file. Read-only: the database is only imported, nothing is signalled."""

    @staticmethod
    def _database(tmp_path, source_fs):
        from click.testing import CliRunner
        from zfsbackup.cli.main import cli

        source = tmp_path / 'check.yaml'
        source.write_text(
            'snapshot_prefix: "autosnap"\n'
            'check_interval: "5m"\n'
            'datasets:\n'
            f'  - name: "{source_fs.name}"\n'
            '    frequency: "1h"\n'
            '    retention:\n'
            '      "1d": "30d"\n'
        )
        database = tmp_path / 'cfg' / 'config.db'
        result = CliRunner().invoke(cli, ['-c', str(database), 'import', str(source)], prog_name='zfsbackup-config')
        assert result.exit_code == 0, result.output
        return database

    @staticmethod
    def _check(database, *args):
        from click.testing import CliRunner
        from zfsbackup.cli.main import cli

        result = CliRunner().invoke(
            cli, ['-c', str(database), 'check', *args], prog_name='zfsbackup-config')
        return result

    @staticmethod
    def _entries(result):
        import json as _json

        data = _json.loads(result.stdout)
        return data, {(e['source'], e['subject']): e for e in data['entries']}

    def test_a_fresh_managed_snapshot_is_ok_and_no_daemon_is_crit(self, source_fs, tmp_path):
        database = self._database(tmp_path, source_fs)
        zfs.snapshot(source_fs, snap_name_at(0.2))
        result = self._check(database, '--json')
        data, entries = self._entries(result)
        assert entries[('snapshot', source_fs.name)]['state'] == 'OK'
        assert entries[('snapshot', source_fs.name)]['age_seconds'] < 3600
        assert entries[('daemon', 'process')]['state'] == 'CRIT'
        assert data['state'] == 'CRIT'
        assert result.exit_code == 1

    def test_a_backdated_newest_snapshot_is_crit(self, source_fs, tmp_path):
        database = self._database(tmp_path, source_fs)
        zfs.snapshot(source_fs, snap_name_at(5))      # 5 h old against a 1 h frequency: past the 4 h crit
        result = self._check(database, '--json')
        _, entries = self._entries(result)
        assert entries[('snapshot', source_fs.name)]['state'] == 'CRIT'
        assert result.exit_code == 1
        assert self._check(database, '--nagios').exit_code == 2

    def test_no_managed_snapshot_is_crit(self, source_fs, tmp_path):
        database = self._database(tmp_path, source_fs)
        zfs.snapshot(source_fs, 'manual_snap')        # unmanaged: does not count
        _, entries = self._entries(self._check(database, '--json'))
        assert entries[('snapshot', source_fs.name)]['state'] == 'CRIT'
        assert 'no managed snapshots' in entries[('snapshot', source_fs.name)]['reason']

    def test_prometheus_output_file_is_written_and_parseable(self, source_fs, tmp_path):
        import os
        import re

        database = self._database(tmp_path, source_fs)
        zfs.snapshot(source_fs, snap_name_at(0.2))
        target = tmp_path / 'prom' / 'zfsbackup.prom'
        target.parent.mkdir()
        result = self._check(database, '--prometheus', '--output', str(target))
        assert result.exit_code == 0                  # with --output the exit code says the file was written
        assert result.stdout == ''
        assert os.stat(target).st_mode & 0o7777 == 0o644
        assert [p.name for p in target.parent.iterdir()] == ['zfsbackup.prom']
        sample = re.compile(r'^[a-zA-Z_:][a-zA-Z0-9_:]*(\{([a-z_]+="([^"\\\n]|\\.)*",?)*\})? -?[0-9.e+-]+$')
        samples = [l for l in target.read_text().splitlines() if not l.startswith('#')]
        assert samples and all(sample.match(s) for s in samples), samples
        assert f'zfsbackup_snapshot_check_state{{dataset="{source_fs.name}"}} 0' in samples
        assert 'zfsbackup_check_success 0' in samples            # ... and the verdict (no daemon) is in the file
        assert any(s.startswith(f'zfsbackup_snapshot_age_seconds{{dataset="{source_fs.name}"}} ') for s in samples)

    def test_check_changes_nothing_on_the_pool(self, source_fs, tmp_path):
        database = self._database(tmp_path, source_fs)
        zfs.snapshot(source_fs, snap_name_at(0.2))
        before = list_real_snapshot_names(source_fs.name)
        self._check(database)
        self._check(database, '--json')
        assert list_real_snapshot_names(source_fs.name) == before
        assert _zfs_get_local(source_fs.name, PROP_CONFIG) in (None, '-', '')


# ---------------------------------------------------------------------------
# B-2b: the server never rolls back history the client cannot account for
# ---------------------------------------------------------------------------
#
# `zfs receive -F` of an incremental stream destroys every snapshot of the destination newer than the
# stream's base (tests/fixtures/openzfs/*/M7.txt). The server therefore passes `-F` only when that
# destroys nothing, or exactly the snapshots the client names (`discard`, by guid). The client names one
# only for the snapshot it lost while it was being sent (the backstop) and otherwise refuses, once.

class TestRealNoRollback(_ChainTests):

    def prime(self, e2e, hours=(10, 8, 6)):
        """Send `hours` (oldest first) to the server: a full send of the first, then a chain.
        -> the three short names; the local anchor is the last."""
        first = self.snapshots(e2e, [hours[0]])[0]
        assert e2e.cycle() is True
        rest = self.snapshots(e2e, list(hours[1:]))
        assert e2e.cycle() is True
        names = [first] + rest
        self.assert_on_the_server(e2e, names)
        assert e2e.manager.get_anchor(e2e.dsi, DEST) == names[-1]
        return names

    def rollback(self, e2e, to):
        """`zfs rollback -r` of the client's dataset: skips (never errors) when not permitted."""
        _zfs_run('rollback', '-r', f'{e2e.source.name}@{to}')

    def errors(self, e2e, since=0):
        """The client's ERROR messages (`zfsbackup.remote`) logged after the first `since` of them."""
        return [r.getMessage() for r in e2e.caplog.records
                if r.name.startswith('zfsbackup.remote') and r.levelno >= logging.ERROR][since:]

    def n_errors(self, e2e):
        return len(self.errors(e2e))

    # -- a rolled-back client ----------------------------------------------------------------

    @pytest.mark.parametrize('keep', [0, 1], ids=['rollback-to-the-first', 'rollback-to-the-second'])
    def test_a_rollback_is_refused_and_the_server_keeps_every_snapshot(self, e2e, keep):
        names = self.prime(e2e)
        before = e2e.server_snaps()
        self.rollback(e2e, names[keep])
        e2e.snapshot(1)                                            # new history after the rollback
        mark = self.n_errors(e2e)

        assert e2e.cycle() is False

        assert e2e.server_snaps() == before                        # same names, same guids
        assert len(self.errors(e2e, mark)) == 1
        assert 'zfs rename' in self.errors(e2e, mark)[0]
        # `zfs rollback -r` leaves the dataset's own properties: the anchor still names a lost snapshot
        assert e2e.manager.get_local_anchor(e2e.dsi, DEST) == names[-1]
        assert e2e.server_prop('receive_resume_token') == '-'
        # ...and the same the next cycle, quietly
        assert e2e.cycle() is False
        assert len(self.errors(e2e, mark)) == 1
        assert e2e.server_snaps() == before

    @pytest.mark.parametrize('keep, destroy', [(0, '%s%%%s'), (1, '%s')], ids=['range', 'single'])
    def test_after_the_operator_destroys_the_newer_snapshots_the_next_cycle_continues(self, e2e, keep, destroy):
        names = self.prime(e2e)
        self.rollback(e2e, names[keep])
        newest = e2e.snapshot(1)
        assert e2e.cycle() is False

        lost = names[keep + 1:]
        _zfs_run('destroy', f'{e2e.server_ds}@' + (destroy % ((lost[0], lost[-1]) if len(lost) > 1 else (lost[0],))))
        assert e2e.cycle() is True

        server = e2e.server_snaps()
        assert set(server) == set(names[:keep + 1]) | {newest}
        self.assert_on_the_server(e2e, names[:keep + 1] + [newest])
        assert e2e.manager.get_anchor(e2e.dsi, DEST) == newest

    def test_renaming_the_server_copy_aside_lets_the_next_cycle_send_in_full(self, e2e, auto_zfs_pool):
        names = self.prime(e2e)
        self.rollback(e2e, names[1])
        newest = e2e.snapshot(1)
        assert e2e.cycle() is False

        aside = f'{auto_zfs_pool.name}/zfsbackup_aside'
        _zfs_run('rename', e2e.server_ds, aside)
        try:
            assert e2e.cycle() is True
            assert list(e2e.server_snaps()) == [newest]              # a full stream: only the newest
            assert set(_snapshot_guids(aside)) == set(names)           # the old copy is whole
            assert e2e.manager.get_anchor(e2e.dsi, DEST) == newest
        finally:
            subprocess.run([_zfs_cmd(), 'destroy', '-r', aside], capture_output=True)

    def test_the_server_refuses_on_its_own_when_the_client_does_not_ask(self, e2e):
        """A client that skips its check (a race, an older release): the server compares the base with
        what it holds and refuses before it reads a byte."""
        names = self.prime(e2e)
        before = e2e.server_snaps()
        self.rollback(e2e, names[1])
        e2e.snapshot(1)
        mark = self.n_errors(e2e)
        with mock.patch.object(e2e.remote_mgr, '_rollback_permit', return_value=()):
            assert e2e.cycle() is False

        assert e2e.server_snaps() == before
        assert any('refused' in m for m in self.errors(e2e, mark))
        assert e2e.server_prop('receive_resume_token') == '-'

    # -- the backstop ----------------------------------------------------------------------------

    def test_the_backstop_is_recovered_by_the_next_cycle_and_only_that_snapshot_goes(self, e2e):
        """A sent snapshot destroyed on the client right after the ack: the anchor goes back to the base,
        the server keeps it, and the next cycle lets the server drop exactly that one."""
        first = self.snapshots(e2e, [10])[0]
        assert e2e.cycle() is True
        lost = self.snapshots(e2e, [8])[0]
        real = e2e.manager.set_anchor

        def set_anchor_then_lose_it(dsi, destination, name):
            ok = real(dsi, destination, name)
            if name == lost and lost in e2e.local_snaps():
                zfs.destroy(Snapshot(e2e.source, lost), destroy=True)
            return ok

        with mock.patch.object(e2e.manager, 'set_anchor', set_anchor_then_lose_it):
            e2e.cycle()
        assert any('was destroyed while it was being sent' in m for m in self.errors(e2e))
        assert e2e.manager.get_anchor(e2e.dsi, DEST) == first
        assert set(e2e.server_snaps()) == {first, lost}
        lost_guid = e2e.server_snaps()[lost]
        first_guid = e2e.server_snaps()[first]

        newest = self.snapshots(e2e, [3])[0]
        mark = self.n_errors(e2e)
        assert e2e.cycle() is True

        assert self.errors(e2e, mark) == []
        assert set(e2e.server_snaps()) == {first, newest}            # `lost` went, nothing else
        assert e2e.server_snaps()[first] == first_guid
        assert lost_guid not in e2e.server_snaps().values()
        self.assert_on_the_server(e2e, [first, newest])
        assert e2e.manager.get_anchor(e2e.dsi, DEST) == newest

    # -- -F is still right when it destroys nothing -----------------------------------------------

    def test_a_modified_server_head_with_the_base_the_newest_snapshot_still_succeeds(self, e2e):
        """Someone wrote to the server's copy since its last snapshot: zfs wants `-F` to discard that, and
        the guard must allow it (nothing newer than the base exists)."""
        first = self.snapshots(e2e, [10])[0]
        assert e2e.cycle() is True
        mountpoint = _zfs_get(e2e.server_ds, 'mountpoint')
        mount = subprocess.run([_zfs_cmd(), 'mount', e2e.server_ds], capture_output=True, text=True)
        if mount.returncode != 0:
            pytest.skip(f'cannot mount the server copy: {mount.stderr.strip()}')
        cmd = ['dd', 'if=/dev/urandom', f'of={mountpoint}/dirty.bin', 'bs=65536', 'count=4']
        if subprocess.run(cmd, capture_output=True).returncode != 0 and \
                subprocess.run(['sudo', '-n'] + cmd, capture_output=True).returncode != 0:
            pytest.skip(f'cannot write into the server copy at {mountpoint}')
        subprocess.run(['sync'])
        deadline = time.monotonic() + 15
        while _zfs_get(e2e.server_ds, 'written') == '0' and time.monotonic() < deadline:
            time.sleep(0.5)                                      # `written` follows the transaction group
        assert _zfs_get(e2e.server_ds, 'written') != '0', 'the write never showed in `written`'
        second = self.snapshots(e2e, [6])[0]
        mark = self.n_errors(e2e)

        assert e2e.cycle() is True

        assert self.errors(e2e, mark) == []
        assert set(e2e.server_snaps()) == {first, second}
        self.assert_on_the_server(e2e, [first, second])
        assert e2e.manager.get_anchor(e2e.dsi, DEST) == second

    # -- a full stream onto a copy that has snapshots -------------------------------------------------

    def lose_every_local_snapshot(self, e2e, names):
        for name in names:
            zfs.destroy(Snapshot(e2e.source, name), destroy=True)
        return e2e.snapshot(1)

    def test_a_full_stream_onto_a_copy_with_snapshots_is_refused_by_the_client(self, e2e):
        names = self.prime(e2e)
        before = e2e.server_snaps()
        self.lose_every_local_snapshot(e2e, names)
        mark = self.n_errors(e2e)

        assert e2e.cycle() is False
        assert e2e.cycle() is False

        assert e2e.server_snaps() == before
        assert len(self.errors(e2e, mark)) == 1 and 'zfs rename' in self.errors(e2e, mark)[0]
        assert e2e.server_prop('receive_resume_token') == '-'

    def test_a_full_stream_onto_a_copy_with_snapshots_is_refused_by_the_server(self, e2e):
        names = self.prime(e2e)
        before = e2e.server_snaps()
        self.lose_every_local_snapshot(e2e, names)
        mark = self.n_errors(e2e)
        with mock.patch.object(e2e.remote_mgr, '_rollback_permit', return_value=()):
            assert e2e.cycle() is False

        assert e2e.server_snaps() == before
        assert any('refused' in m for m in self.errors(e2e, mark))
        assert e2e.server_prop('receive_resume_token') == '-'
