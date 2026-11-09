"""Real ZFS tests for the local destination (`url: zfs:<target>`; plan item 3.3).

A real `LocalBackupManager` backs a real source dataset up to a target dataset on this host with real
`zfs send | zfs receive` (the in-process `_Pump`), and a real `RemoteBackupWorker` / `PruningWorker`
cycle runs the same code the daemon does. Nothing here is mocked except where a test says so (the
mid-stream cut, the hook that destroys a snapshot right after its anchor was written).

REQUIREMENTS: as `tests/test_zfsbackup_real.py` -- ZFS, passwordless sudo for `zpool`/`zfs`, and the
opt-in `ZFSB_REAL_ZFS=1`. The session fixture `auto_zfs_pool` provides the pool; the cross-pool layouts
also use `auto_zfs_pool2` (a second file-backed pool, `tests/conftest.py`). Without a pool a test
skips, it never errors. The tests write into the source's mountpoint from THIS host, so they skip
under `MULTIPASS_VM` (run pytest inside the VM, as the other end-to-end real tests).

Every test creates its own datasets (`<pool>/zfsbackup_lsrc` and the target `<pool>/zfsbackup_ldst`,
in the same pool or in the second one) and destroys them on teardown; nothing outside those two trees
is touched. The layout of a copy is `<target>/<client_id>/<pool>/<path>`.

Layouts, each run same-pool and cross-pool:

* plain, raw (encrypted source, `raw` auto), plain into an encrypted target, raw into an encrypted
  target: the first full is the newest snapshot only, a chain of three `-i` steps follows, every
  snapshot arrives with its guid, the anchor advances, the pin is cleared, the owned values
  (`client_id`, `source_dataset`, `config`) and the forced lock-down set are LOCAL on the copy, which is
  never mounted;
* a pump cut mid-stream (the `zfs send` child terminated while the pump copies): the next cycle resumes
  from the resume token; a stale token (its snapshot destroyed) is aborted with `receive -A` and the
  fresh stream recreates the copy; a chain cut in the middle resumes and goes on;
* the rollback refusal (a rolled-back source, or two foreign snapshots on the copy: refused once,
  nothing destroyed) and the backstop (exactly one newer managed snapshot this host lacks is discarded);
* pruning the copies with a real `PruningWorker`: old copy snapshots go, the newest stays, nothing
  outside `<target>/<own id>` is touched (a stranger's tree and a prefix-sibling id with old snapshots),
  never recursive (a child below a copy keeps its snapshots, also for a recursive source config);
* the target pool exported mid-cycle (cross-pool only): the destination is skipped as gone, quietly for
  the rest of the cycle, and recovers after the pool is imported.

Not mutation-checked: written against the production code on a host without ZFS (the real tier needs a
pool); the assertions follow `zfsbackup/local.py`, `chain.py` and `receiver.py` as they are.
"""

import collections
import contextlib
import logging
import os
import signal
import subprocess
import time
import types
from datetime import datetime, timedelta, timezone
from unittest import mock

import pytest

import libzfseasy as zfs
from libzfseasy.types import Filesystem, Snapshot
from libzfseasy.zfs import _zfs_cmd, _zpool_cmd

from zfsbackup import local as local_module
from zfsbackup.backup_manager import (
    PROP_CLIENT_ID,
    PROP_CONFIG,
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
from zfsbackup.health import ensure_status_dir, read_worker_status
from zfsbackup.local import LocalBackupManager
from zfsbackup.receiver import RECEIVE_PROPERTIES_FILESYSTEM, canonical_dataset_config
from zfsbackup.stopflag import StopFlag
from zfsbackup.workers import ExitReason, PruningWorker, RemoteBackupWorker


pytestmark = pytest.mark.real_zfs

MIB = 1024 * 1024

#: This host's client id: the copies live under `<target>/<CLIENT_ID>`.
CLIENT_ID = 'loc-client'
#: The local destination's name in the configuration.
DEST = 'local'
PASSPHRASE = 'zfsb-local-test-passphrase'


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def snap_name_at(hours_ago):
    """A managed snapshot name (`autosnap_<UTC timestamp>`) dated *hours_ago* hours in the past."""
    dt = datetime.now(timezone.utc) - timedelta(hours=hours_ago)
    return f"autosnap_{dt.strftime('%Y%m%d%H%M%S')}"


def _zfs_run(*args):
    """A real `zfs` subcommand; skips (never errors) when this account may not run it."""
    r = subprocess.run([_zfs_cmd(), *args], capture_output=True, text=True)
    if r.returncode != 0:
        if 'permission denied' in r.stderr.lower():
            pytest.skip(f'zfs {args[0]} not permitted for this account: {r.stderr.strip()}')
        raise AssertionError(f'zfs {" ".join(args)} failed ({r.returncode}): {r.stderr.strip()}')
    return r.stdout


def _zfs_get(dataset, prop):
    r = subprocess.run([_zfs_cmd(), 'get', '-H', '-o', 'value', prop, dataset],
                       capture_output=True, text=True)
    return r.stdout.strip() if r.returncode == 0 else None


def _zfs_get_local(dataset, prop):
    """`prop` of `dataset` if it is set LOCALLY (`zfs get -s local`), else `None`."""
    r = subprocess.run([_zfs_cmd(), 'get', '-H', '-o', 'value', '-s', 'local', prop, dataset],
                       capture_output=True, text=True)
    value = r.stdout.strip() if r.returncode == 0 else ''
    return value or None


def _zfs_get_source(dataset, prop):
    """The SOURCE column of `prop` on `dataset` (`local`, `received`, `default`, ...), else `None`."""
    r = subprocess.run([_zfs_cmd(), 'get', '-H', '-o', 'source', prop, dataset],
                       capture_output=True, text=True)
    return r.stdout.strip() if r.returncode == 0 else None


def _snapshot_guids(dataset):
    """`{short snapshot name: guid}` of `dataset`'s own snapshots (empty when it does not exist)."""
    r = subprocess.run(
        [_zfs_cmd(), 'list', '-H', '-p', '-t', 'snapshot', '-d', '1', '-o', 'name,guid', dataset],
        capture_output=True, text=True,
    )
    guids = {}
    for line in r.stdout.splitlines():
        name, _, guid = line.partition('\t')
        if '@' in name:
            guids[name.split('@', 1)[1]] = int(guid)
    return guids


def _destroy_quietly(name):
    subprocess.run([_zfs_cmd(), 'destroy', '-rf', name], capture_output=True)


def _zpool_run(*args):
    """A real `zpool` subcommand (`sudo -n` unless the sudo wrapper already is `ZPOOL_CMD`)."""
    cmd = [_zpool_cmd(), *args]
    if not os.environ.get('ZPOOL_CMD'):
        cmd = ['sudo', '-n'] + cmd
    return subprocess.run(cmd, capture_output=True, text=True)


def _descendants(pid):
    """The pids below `pid` (`pgrep -P`, recursively), parents before their children."""
    found, queue = [], [pid]
    while queue:
        r = subprocess.run(['pgrep', '-P', str(queue.pop(0))], capture_output=True, text=True)
        children = [int(x) for x in r.stdout.split()]
        found += children
        queue += children
    return found


def _kill_zfs_child(handle, sig=signal.SIGTERM):
    """Kill the `zfs` process behind a libzfseasy `StreamHandle`, as `kill <pid of zfs>` would.

    Not `handle.terminate()`: under the sudo wrappers of `auto_zfs_pool` (`ZFS_CMD` is `exec sudo
    /usr/sbin/zfs "$@"`) the handle's child is `sudo`, and sudo does not relay a signal sent from a
    process in its parent's process group -- this one. On Ubuntu 22.04 (sudo 1.9.9) `terminate()` then
    SIGKILLs sudo alone after 5 s, the real `zfs send` keeps the pipe and writes the whole stream, and
    the receive completes (no partial state). So the deepest descendant is signalled (the handle's
    child itself without a wrapper), through `sudo -n kill` when it runs as another user."""
    pids = _descendants(handle.pid) or [handle.pid]
    target = pids[-1]
    try:
        os.kill(target, sig)
    except PermissionError:
        subprocess.run(['sudo', '-n', 'kill', f'-{int(sig)}', str(target)], capture_output=True)
    except ProcessLookupError:
        pass


def _enc_options(keyfile):
    return ['-o', 'encryption=aes-256-gcm', '-o', 'keyformat=passphrase',
            '-o', f'keylocation=file://{keyfile}']


# ---------------------------------------------------------------------------
# Pools: same-pool and cross-pool
# ---------------------------------------------------------------------------

Pools = collections.namedtuple('Pools', 'src dst cross second')


def _pools(cross, request, first):
    if first is None:
        pytest.skip('ZFS pool not available -- install ZFS and grant NOPASSWD sudo for zpool/zfs')
    if os.environ.get('MULTIPASS_VM'):
        pytest.skip('the local-destination tests write into the pool mountpoints from this host; '
                    'run them on the machine that owns the pool (pytest inside the VM)')
    if not cross:
        return Pools(first.name, first.name, False, None)
    second = request.getfixturevalue('auto_zfs_pool2')
    if second is None:
        pytest.skip('a second ZFS pool could not be created (cross-pool layout)')
    return Pools(first.name, second.name, True, second)


@pytest.fixture(params=['same-pool', 'cross-pool'])
def pools(request, auto_zfs_pool):
    """Where the source and the target live: both on the first pool, or the target on the second."""
    return _pools(request.param == 'cross-pool', request, auto_zfs_pool)


@pytest.fixture
def sdir(tmp_path):
    """A worker status directory next to a (stand-in) config database."""
    (tmp_path / 'config.db').write_bytes(b'')
    directory = tmp_path / 'config.db.status'
    ensure_status_dir(directory)
    return directory


def _statuses(sdir, worker):
    status = read_worker_status(sdir, worker)
    return {} if status is None else {r.key: r for r in status.records}


# ---------------------------------------------------------------------------
# The rig
# ---------------------------------------------------------------------------

class Rig:
    """A source dataset, a target and a real `LocalBackupManager` between them."""

    def __init__(self, pools, source, target, config, manager, mgr, caplog):
        self.pools = pools
        self.src = source
        self.source = Filesystem(source)
        self.target = target
        self.config = config
        self.manager = manager
        self.mgr = mgr
        self.caplog = caplog
        self.dsi = manager.datasets[0]
        self.remote_cfg = self.dsi.config.remote[0]
        self.base = f'{target}/{CLIENT_ID}'
        self.parent = f'{target}/{CLIENT_ID}/{pools.src}'
        self.copy = f'{target}/{CLIENT_ID}/{source}'

    # -- the source's data --------------------------------------------------------------------
    def snapshot(self, hours_ago):
        """A real managed snapshot of the source named for `hours_ago`; -> its short name."""
        name = snap_name_at(hours_ago)
        zfs.snapshot(self.source, name)
        return name

    def write(self, name, mib):
        """`mib` MiB of incompressible data into the source's mountpoint."""
        mountpoint = _zfs_get(self.src, 'mountpoint')
        cmd = ['dd', 'if=/dev/urandom', f'of={mountpoint}/{name}', 'bs=1048576', f'count={mib}']
        r = subprocess.run(cmd, capture_output=True)
        if r.returncode != 0:                      # a root-owned mountpoint (sudo-wrapped pool)
            r = subprocess.run(['sudo', '-n'] + cmd, capture_output=True)
        if r.returncode != 0:
            pytest.skip(f'cannot write into the test dataset at {mountpoint}: '
                        f'{r.stderr.decode(errors="replace").strip()}')

    # -- the transfer -------------------------------------------------------------------------
    def cycle(self):
        """One worker-style pass: `begin_cycle()`, then `backup_dataset` to the destination."""
        self.mgr.begin_cycle()
        return self.mgr.backup_dataset(self.dsi, self.remote_cfg)

    @contextlib.contextmanager
    def cut_pump(self, after, which=1):
        """Within the block the `which`th (1-based) stream the pump copies is cut: once `after` bytes
        have been read from its `zfs send`, that `zfs` process is killed (SIGTERM, as `kill` of it would;
        `_kill_zfs_child`, never `terminate()` of the handle, which may be a sudo wrapper), so the
        `zfs receive` gets a truncated stream and keeps its partial state (`-s`). Yields a namespace
        with `done` (the cut happened) and `total` (bytes read when it did)."""
        real = local_module._Pump._fill
        cut = types.SimpleNamespace(pumps=0, total=0, done=False)

        def fill(pump):
            moved = real(pump)
            index = getattr(pump, '_test_index', None)
            if index is None:
                cut.pumps += 1
                index = pump._test_index = cut.pumps
            if index == which and not cut.done:
                cut.total += moved
                if cut.total >= after:
                    cut.done = True
                    _kill_zfs_child(pump.send)
            return moved

        with mock.patch.object(local_module._Pump, '_fill', fill):
            yield cut

    # -- what is on each side -----------------------------------------------------------------
    def local_snaps(self):
        return _snapshot_guids(self.src)

    def copy_snaps(self):
        return _snapshot_guids(self.copy)

    def copy_prop(self, prop):
        return _zfs_get(self.copy, prop)

    def copy_local(self, prop):
        return _zfs_get_local(self.copy, prop)

    def canonical_config(self):
        return canonical_dataset_config(self.dsi.config.to_property(DEST))

    def wait_for_resume_token(self, timeout=30):
        """The copy's `receive_resume_token` once the aborted receive has saved it."""
        deadline = time.monotonic() + timeout
        token = '-'
        while time.monotonic() < deadline:
            token = _zfs_get(self.copy, 'receive_resume_token') or '-'
            if token not in ('-', ''):
                return token
            time.sleep(0.2)
        return token

    # -- the log ------------------------------------------------------------------------------
    def log_lines(self, level=0):
        return [r.getMessage() for r in self.caplog.records
                if r.name.startswith('zfsbackup.local') and r.levelno >= level]

    def errors(self):
        return self.log_lines(logging.ERROR)


# ---------------------------------------------------------------------------
# The shared base: the rig fixture and the assertions
# ---------------------------------------------------------------------------

class _RigTests:
    """Set `SOURCE_ENCRYPTED`, `TARGET_ENCRYPTED`, `RAW` (the destination's `raw`), `RECURSIVE`, and for
    the shape of the copy `COPY` (`'plain'`/`'raw'`) and `ROOT` (its encryption root: `None`, `'leaf'`
    -- the copy itself --, or `'target'`)."""

    SOURCE_ENCRYPTED = False
    TARGET_ENCRYPTED = False
    RAW = None
    RECURSIVE = False
    COPY = 'plain'
    ROOT = None
    #: the target is created with `sharesmb=on`, which a copy inherits unless its own `sharesmb=off` is local
    TARGET_SHARESMB = False

    @pytest.fixture
    def rig(self, pools, tmp_path, caplog):
        keyfile = tmp_path / 'local.key'
        keyfile.write_text(PASSPHRASE)
        keyfile.chmod(0o600)
        src = f'{pools.src}/zfsbackup_lsrc'
        dst = f'{pools.dst}/zfsbackup_ldst'
        for name in (src, dst):
            _destroy_quietly(name)                 # a leftover of a crashed run, in a pool we made
        rig = None
        try:
            _zfs_run('create', *(_enc_options(keyfile) if self.SOURCE_ENCRYPTED else []), src)
            # the target as the operator makes it: never mounted itself
            _zfs_run('create', '-o', 'canmount=off',
                     *(['-o', 'sharesmb=on'] if self.TARGET_SHARESMB else []),
                     *(_enc_options(keyfile) if self.TARGET_ENCRYPTED else []), dst)
            if _zfs_get(src, 'mounted') != 'yes':
                pytest.skip('the source dataset is not mounted; cannot write test data into it')
            config = BackupConfig(
                datasets=[DatasetConfig(
                    name=src, recursive=self.RECURSIVE, frequency=timedelta(hours=24),
                    retention_rules=[RetentionRule(timedelta(hours=1), timedelta(days=30))],
                    remote=[RemoteDatasetConfig(
                        destination=DEST, raw=self.RAW,
                        # what the COPY keeps: 1h slots, nothing older than 2h
                        retention_rules=[RetentionRule(timedelta(hours=1), timedelta(hours=2))],
                    )],
                )],
                destinations={DEST: Destination(url=f'zfs:{dst}')},
                snapshot_prefix='autosnap',
            )
            manager = DatasetManager(config)
            mgr = LocalBackupManager(config, manager, client_id=CLIENT_ID)
            caplog.set_level(logging.DEBUG, logger='zfsbackup.local')
            caplog.set_level(logging.DEBUG, logger='zfsbackup.backup_manager')
            rig = Rig(pools, src, dst, config, manager, mgr, caplog)
            yield rig
        finally:
            if rig is not None:
                rig.mgr.close()
            _destroy_quietly(src)
            _destroy_quietly(dst)

    # -- making history -------------------------------------------------------------------
    def snapshots(self, rig, hours_ago, mib=1):
        """One data file and one snapshot per entry of `hours_ago` (oldest first); -> the names."""
        names = []
        for hours in hours_ago:
            rig.write(f'f{hours}.bin', mib)
            names.append(rig.snapshot(hours))
        return names

    def prime(self, rig, hours=(10, 8, 6)):
        """A full send of the first snapshot, then a chain of the rest; -> the names (the anchor is the last)."""
        first = self.snapshots(rig, [hours[0]])[0]
        assert rig.cycle() is True
        rest = self.snapshots(rig, list(hours[1:]))
        assert rig.cycle() is True
        names = [first] + rest
        self.assert_on_the_copy(rig, names)
        assert rig.manager.get_anchor(rig.dsi, DEST) == names[-1]
        return names

    # -- assertions -----------------------------------------------------------------------
    def assert_on_the_copy(self, rig, names):
        local, copy = rig.local_snaps(), rig.copy_snaps()
        assert set(names) <= set(copy), (names, sorted(copy))
        for name in names:
            assert copy[name] == local[name], f'{name}: the copy holds another snapshot of that name'

    def assert_owned_locally(self, rig):
        for prop, expected in ((PROP_CLIENT_ID, CLIENT_ID), (PROP_SOURCE_DATASET, rig.src),
                               (PROP_CONFIG, rig.canonical_config())):
            assert _zfs_get_source(rig.copy, prop) == 'local', prop
            assert rig.copy_local(prop) == expected, prop

    def assert_locked_down(self, rig):
        """The server's forced set, as LOCAL values; and the copy was never mounted (`receive -u`)."""
        for prop, expected in RECEIVE_PROPERTIES_FILESYSTEM.items():
            assert _zfs_get_source(rig.copy, prop) == 'local', prop
            assert rig.copy_prop(prop) == expected, prop
        assert rig.copy_prop('mounted') == 'no'

    def assert_containers(self, rig):
        """`<target>/<id>` and `<target>/<id>/<pool>` were made by negotiate: owned, never mounted."""
        for container in (rig.base, rig.parent):
            assert _zfs_get_local(container, PROP_CLIENT_ID) == CLIENT_ID, container
            assert _zfs_get_source(container, 'canmount') == 'local', container
            assert _zfs_get(container, 'canmount') == 'noauto', container

    def assert_encryption_shape(self, rig):
        if self.ROOT is None:
            assert rig.copy_prop('encryption') == 'off'
            return
        assert rig.copy_prop('encryption') == 'aes-256-gcm'
        root = rig.copy if self.ROOT == 'leaf' else rig.target
        assert rig.copy_prop('encryptionroot') == root
        # a raw copy arrives without its key; a plain one under an encrypted target is keyed by the target
        assert rig.copy_prop('keystatus') == ('unavailable' if self.ROOT == 'leaf' else 'available')

    def assert_copy_state(self, rig):
        self.assert_owned_locally(rig)
        self.assert_locked_down(rig)
        self.assert_containers(rig)
        self.assert_encryption_shape(rig)
        assert rig.copy_prop('receive_resume_token') == '-'


# ---------------------------------------------------------------------------
# Tests that hold for every layout
# ---------------------------------------------------------------------------

class _SharedTests(_RigTests):

    # -- the chain ------------------------------------------------------------------------
    def test_the_copy_lies_under_the_target_client_id_and_pool(self, rig):
        self.snapshots(rig, [5])
        assert rig.cycle() is True

        assert rig.copy == f'{rig.target}/{CLIENT_ID}/{rig.pools.src}/zfsbackup_lsrc'
        assert _zfs_get(rig.copy, 'name') == rig.copy
        assert rig.copy.split('/')[0] == rig.pools.dst
        assert (rig.pools.src == rig.pools.dst) is (not rig.pools.cross)

    def test_the_first_full_is_the_newest_only_and_a_chain_of_three_follows(self, rig):
        old = self.snapshots(rig, [9, 7, 5])
        assert rig.cycle() is True

        assert list(rig.copy_snaps()) == [old[-1]]                  # no backfill of the older ones
        assert rig.copy_snaps()[old[-1]] == rig.local_snaps()[old[-1]]
        assert rig.manager.get_anchor(rig.dsi, DEST) == old[-1]
        assert rig.manager.get_inflight(rig.dsi, DEST) is None
        assert any(f'Sending full snapshot {rig.src}@{old[-1]}' in m for m in rig.log_lines())
        self.assert_copy_state(rig)

        more = self.snapshots(rig, [3, 2, 1])
        assert rig.cycle() is True

        assert set(rig.copy_snaps()) == {old[-1]} | set(more)
        self.assert_on_the_copy(rig, [old[-1]] + more)
        assert rig.manager.get_anchor(rig.dsi, DEST) == more[-1]
        assert rig.manager.get_inflight(rig.dsi, DEST) is None
        lines = rig.log_lines()
        assert any(f'Sending 3 snapshots of {rig.src} to {DEST}' in m for m in lines)
        for prev, name in zip([old[-1]] + more, more):
            assert any(f'Sending incremental {rig.src}: {prev} -> {name}' in m for m in lines), (prev, name)
        self.assert_copy_state(rig)
        assert rig.errors() == []

    def test_a_cycle_with_nothing_new_changes_nothing(self, rig):
        names = self.prime(rig, hours=(8, 6))
        before = rig.copy_snaps()

        assert rig.cycle() is True

        assert rig.copy_snaps() == before
        assert rig.manager.get_anchor(rig.dsi, DEST) == names[-1]
        self.assert_copy_state(rig)
        assert rig.errors() == []

    # -- a pump cut mid-stream ----------------------------------------------------------------
    def cut_first_full(self, rig):
        """A first full of ~24 MiB whose `zfs send` is terminated after 4 MiB; -> the snapshot's name."""
        rig.write('data.bin', 24)
        s1 = rig.snapshot(3)
        with rig.cut_pump(after=4 * MIB) as cut:
            assert rig.cycle() is False
        assert cut.done, 'the pump never read 4 MiB of the stream'
        token = rig.wait_for_resume_token()
        assert token not in ('-', ''), 'zfs receive saved no partial state'
        return s1

    def test_a_cut_first_full_leaves_a_partial_copy_with_the_values_already_local(self, rig):
        s1 = self.cut_first_full(rig)

        assert _zfs_get(rig.copy, 'name') == rig.copy                # the partial NEW dataset is visible
        assert rig.copy_snaps() == {}
        self.assert_owned_locally(rig)
        assert rig.manager.get_anchor(rig.dsi, DEST) is None
        assert rig.manager.get_inflight(rig.dsi, DEST) == ('', s1)   # the pin of the cut step stays
        assert rig.errors()                                          # and the failure was reported

    def test_the_next_cycle_resumes_the_cut_stream_into_the_same_copy(self, rig):
        s1 = self.cut_first_full(rig)
        mark = len(rig.errors())

        assert rig.cycle() is True

        assert any(f'Resuming interrupted transfer for {rig.src}@{s1}' in m for m in rig.log_lines())
        assert not any('discarding the partial receive state' in m for m in rig.log_lines())
        assert list(rig.copy_snaps()) == [s1]
        assert rig.copy_snaps()[s1] == rig.local_snaps()[s1]
        assert rig.manager.get_anchor(rig.dsi, DEST) == s1
        assert rig.manager.get_inflight(rig.dsi, DEST) is None
        self.assert_copy_state(rig)
        assert rig.errors()[mark:] == []

    def test_a_stale_token_is_aborted_and_the_fresh_stream_recreates_the_copy(self, rig):
        """The token's snapshot is gone: `receive -A` discards the partial copy, the full stream of the
        newest snapshot recreates it (no `-F`), with the owned values local on the new dataset."""
        s1 = self.cut_first_full(rig)
        zfs.destroy(Snapshot(rig.source, s1), destroy=True)           # the token is dead
        rig.write('newer.bin', 1)
        s2 = rig.snapshot(2)

        assert rig.cycle() is True

        lines = rig.log_lines()
        assert any('Not resuming' in m and 'sending fresh' in m for m in lines), lines
        assert any(f'discarding the partial receive state of {rig.copy}' in m for m in lines), lines
        assert list(rig.copy_snaps()) == [s2]
        assert rig.copy_snaps()[s2] == rig.local_snaps()[s2]
        assert rig.manager.get_anchor(rig.dsi, DEST) == s2
        self.assert_copy_state(rig)

    def test_a_chain_cut_in_the_middle_resumes_and_goes_on(self, rig):
        first = self.snapshots(rig, [10])[0]
        assert rig.cycle() is True
        more = self.snapshots(rig, [8, 6, 4], mib=8)

        with rig.cut_pump(after=2 * MIB, which=2) as cut:
            assert rig.cycle() is False                               # step 2 of 3 died
        assert cut.done
        assert set(rig.copy_snaps()) == {first, more[0]}
        assert rig.manager.get_anchor(rig.dsi, DEST) == more[0]       # the last good step
        assert rig.manager.get_inflight(rig.dsi, DEST) == (more[0], more[1])
        assert rig.wait_for_resume_token() not in ('-', '')

        assert rig.cycle() is True                                    # resumes step 2, then step 3

        assert any(f'Resuming interrupted transfer for {rig.src}@{more[1]}' in m for m in rig.log_lines())
        assert set(rig.copy_snaps()) == {first} | set(more)
        self.assert_on_the_copy(rig, [first] + more)
        assert rig.manager.get_anchor(rig.dsi, DEST) == more[2]
        assert rig.manager.get_inflight(rig.dsi, DEST) is None
        self.assert_copy_state(rig)


class TestRealLocalPlain(_SharedTests):
    """An unencrypted source into an unencrypted target: a plain copy (`-L -c`)."""

    COPY, ROOT = 'plain', None

    # -- rollback refusal and backstop ----------------------------------------------------------
    @pytest.mark.parametrize('keep', [0, 1], ids=['rollback-to-the-first', 'rollback-to-the-second'])
    def test_a_rolled_back_source_is_refused_and_the_copy_keeps_every_snapshot(self, rig, keep):
        names = self.prime(rig)
        before = rig.copy_snaps()
        _zfs_run('rollback', '-r', f'{rig.src}@{names[keep]}')
        rig.snapshot(1)                                               # new history after the rollback
        mark = len(rig.errors())

        assert rig.cycle() is False

        assert rig.copy_snaps() == before                             # same names, same guids
        errors = rig.errors()[mark:]
        assert len(errors) == 1 and 'zfs rename' in errors[0], errors
        # `zfs rollback -r` leaves the dataset's own properties: the anchor still names a lost snapshot
        assert rig.manager.get_local_anchor(rig.dsi, DEST) == names[-1]
        assert rig.copy_prop('receive_resume_token') == '-'
        assert rig.cycle() is False                                   # the same, and quiet
        assert len(rig.errors()) == mark + 1
        assert rig.copy_snaps() == before

    def test_two_foreign_snapshots_on_the_copy_are_refused_and_nothing_is_destroyed(self, rig):
        names = self.prime(rig)
        extra = [snap_name_at(0.9), snap_name_at(0.8)]
        for name in extra:
            _zfs_run('snapshot', f'{rig.copy}@{name}')
        new = self.snapshots(rig, [1])[0]
        before = rig.copy_snaps()
        assert set(before) == set(names) | set(extra)
        mark = len(rig.errors())

        assert rig.cycle() is False

        assert rig.copy_snaps() == before
        errors = rig.errors()[mark:]
        assert len(errors) == 1 and 'zfs rename' in errors[0], errors
        assert rig.cycle() is False
        assert len(rig.errors()) == mark + 1

        # the operator destroys them (the command the refusal named): the next cycle continues
        _zfs_run('destroy', f'{rig.copy}@{extra[0]}%{extra[1]}')
        assert rig.cycle() is True
        assert set(rig.copy_snaps()) == set(names) | {new}
        self.assert_on_the_copy(rig, names + [new])

    def test_exactly_one_foreign_snapshot_on_the_copy_is_discarded_by_the_backstop(self, rig):
        names = self.prime(rig)
        extra = snap_name_at(0.9)
        _zfs_run('snapshot', f'{rig.copy}@{extra}')
        extra_guid = rig.copy_snaps()[extra]
        new = self.snapshots(rig, [1])[0]
        mark = len(rig.errors())

        assert rig.cycle() is True

        assert set(rig.copy_snaps()) == set(names) | {new}            # `extra` went, nothing else
        assert extra_guid not in rig.copy_snaps().values()
        self.assert_on_the_copy(rig, names + [new])
        assert any('the receive destroys 1 snapshot(s)' in m for m in rig.log_lines(logging.WARNING))
        assert rig.errors()[mark:] == []
        assert rig.manager.get_anchor(rig.dsi, DEST) == new

    def test_the_backstop_is_recovered_by_the_next_cycle_and_only_that_snapshot_goes(self, rig):
        """A sent snapshot destroyed locally right after its anchor was written: the anchor goes back to the
        base, the copy keeps it, and the next cycle lets the receive drop exactly that one."""
        first = self.snapshots(rig, [10])[0]
        assert rig.cycle() is True
        lost = self.snapshots(rig, [8])[0]
        real = rig.manager.set_anchor

        def set_anchor_then_lose_it(dsi, destination, name):
            ok = real(dsi, destination, name)
            if name == lost and lost in rig.local_snaps():
                zfs.destroy(Snapshot(rig.source, lost), destroy=True)
            return ok

        with mock.patch.object(rig.manager, 'set_anchor', set_anchor_then_lose_it):
            rig.cycle()
        assert any('was destroyed while it was being sent' in m for m in rig.errors())
        assert rig.manager.get_anchor(rig.dsi, DEST) == first
        assert set(rig.copy_snaps()) == {first, lost}
        lost_guid = rig.copy_snaps()[lost]
        first_guid = rig.copy_snaps()[first]

        newest = self.snapshots(rig, [3])[0]
        mark = len(rig.errors())
        assert rig.cycle() is True

        assert rig.errors()[mark:] == []
        assert set(rig.copy_snaps()) == {first, newest}               # `lost` went, nothing else
        assert rig.copy_snaps()[first] == first_guid
        assert lost_guid not in rig.copy_snaps().values()
        self.assert_on_the_copy(rig, [first, newest])
        assert rig.manager.get_anchor(rig.dsi, DEST) == newest

    # -- the workers ----------------------------------------------------------------------------
    def test_a_remote_backup_worker_cycle_sends_to_the_local_destination_and_records_it(
            self, rig, sdir, mocker):
        """The real worker loop: `RemoteBackupWorker` dispatches a `zfs:` destination to the local
        manager and records the outcome under the destination's name."""
        old = self.snapshots(rig, [9, 7, 5])
        worker = RemoteBackupWorker(rig.config, StopFlag(), status_dir=sdir, client_id=CLIENT_ID)
        mocker.patch.object(worker, '_wait', return_value=ExitReason.STOP)

        worker.run()

        assert list(rig.copy_snaps()) == [old[-1]]
        assert rig.copy_snaps()[old[-1]] == rig.local_snaps()[old[-1]]
        assert rig.manager.get_anchor(rig.dsi, DEST) == old[-1]
        record = _statuses(sdir, 'remote-backup-worker')[('send', rig.src, DEST)]
        assert record.result == 'ok'
        self.assert_copy_state(rig)

    # -- pruning the copies ---------------------------------------------------------------------
    def prune(self, rig, mocker, sdir):
        """One real `PruningWorker` cycle over the config of the rig."""
        worker = PruningWorker(rig.config, StopFlag(), status_dir=sdir, client_id=CLIENT_ID)
        mocker.patch.object(worker, '_wait', return_value=ExitReason.STOP)
        worker.run()
        return _statuses(sdir, 'pruning-worker')

    @staticmethod
    def plant_leaf(leaf, owner, source, hours=(9, 8, 0.3)):
        """`leaf` stamped as `owner`'s copy of `source` (a config that keeps 1h/2h), with old snapshots
        that any prune reaching it would destroy. -> `{snapshot: guid}`."""
        config = DatasetConfig(
            name=source, frequency=timedelta(hours=1),
            retention_rules=[RetentionRule(timedelta(hours=1), timedelta(hours=2))],
        ).to_property()
        _zfs_run('create', '-u', '-o', 'canmount=noauto', '-o', f'{PROP_CLIENT_ID}={owner}',
                 '-o', f'{PROP_SOURCE_DATASET}={source}', '-o', f'{PROP_CONFIG}={config}', leaf)
        for h in hours:
            _zfs_run('snapshot', f'{leaf}@{snap_name_at(h)}')
        return _snapshot_guids(leaf)

    def plant_tree(self, rig, client_id, leaf_name):
        """`<target>/<client_id>/<pool>/<leaf_name>` the way a receive makes one (owned containers, a
        stamped leaf). -> `(leaf, {snapshot: guid})`."""
        base = f'{rig.target}/{client_id}'
        parent = f'{base}/{rig.pools.src}'
        for container in (base, parent):
            if _zfs_get(container, 'name') is None:
                _zfs_run('create', '-u', '-o', 'canmount=noauto',
                         '-o', f'{PROP_CLIENT_ID}={client_id}', container)
        leaf = f'{parent}/{leaf_name}'
        return leaf, self.plant_leaf(leaf, client_id, f'{rig.pools.src}/{leaf_name}')

    def test_a_pruning_cycle_prunes_the_old_copy_snapshots_keeps_the_newest_and_touches_nothing_else(
            self, rig, sdir, mocker):
        first = self.snapshots(rig, [8])[0]
        assert rig.cycle() is True
        rest = self.snapshots(rig, [7, 6, 0.2])
        assert rig.cycle() is True
        names = [first] + rest
        self.assert_on_the_copy(rig, names)
        source_before = rig.local_snaps()
        # a stranger's tree, a prefix-sibling id (`loc-client` vs `loc-clientx`), and a leaf inside OUR tree
        # stamped with another client's id: none may be touched
        planted = dict([
            self.plant_tree(rig, 'other-host', 'zfsbackup_stranger'),
            self.plant_tree(rig, f'{CLIENT_ID}x', 'zfsbackup_sibling'),
        ])
        intruder = f'{rig.parent}/zfsbackup_intruder'
        planted[intruder] = self.plant_leaf(
            intruder, 'other-host', f'{rig.pools.src}/zfsbackup_intruder', hours=(9, 8))

        records = self.prune(rig, mocker, sdir)

        assert set(rig.copy_snaps()) == {names[-1]}                   # the old ones went, the newest stayed
        assert rig.copy_snaps()[names[-1]] == rig.local_snaps()[names[-1]]
        assert rig.local_snaps() == source_before                     # the source keeps its own retention
        for leaf, snaps in planted.items():
            assert _snapshot_guids(leaf) == snaps, f'{leaf} was pruned'
        assert records[('discover', rig.target, DEST)].detail == '1 local copy'
        assert records[('discover', rig.target, DEST)].result == 'ok'
        assert records[('prune', rig.copy, None)].result == 'ok'
        assert not [k for k in records if k[1] in planted]
        assert any(intruder in m for m in rig.caplog.messages if 'Skipping' in m)

        # the surviving newest is still the base of the next incremental step
        newer = self.snapshots(rig, [0.1])[0]
        assert rig.cycle() is True
        assert set(rig.copy_snaps()) == {names[-1], newer}
        self.assert_on_the_copy(rig, [names[-1], newer])
        assert any(f'Sending incremental {rig.src}: {names[-1]} -> {newer}' in m for m in rig.log_lines())


class TestRealLocalRaw(_SharedTests):
    """An encrypted source, `raw` auto: the copy is raw (its own encryption root, the key unavailable)."""

    SOURCE_ENCRYPTED = True
    COPY, ROOT = 'raw', 'leaf'

    def test_a_raw_chain_with_the_key_unloaded_reaches_the_copy_with_its_guids(self, rig):
        first = self.snapshots(rig, [10])[0]
        assert rig.cycle() is True                                    # raw full (the key is loaded; auto = raw)
        more = self.snapshots(rig, [8, 6, 4])
        _zfs_run('unmount', rig.src)
        _zfs_run('unload-key', rig.src)
        assert _zfs_get(rig.src, 'keystatus') == 'unavailable'

        assert rig.cycle() is True

        self.assert_on_the_copy(rig, [first] + more)
        assert set(rig.copy_snaps()) == {first} | set(more)
        assert rig.copy_prop('encryptionroot') == rig.copy            # a raw copy is its own root ...
        assert rig.copy_prop('keystatus') == 'unavailable'            # ... and arrives without its key
        assert rig.manager.get_anchor(rig.dsi, DEST) == more[-1]
        assert any(f'Sending 3 snapshots of {rig.src} to {DEST}' in m for m in rig.log_lines())
        self.assert_copy_state(rig)
        assert rig.errors() == []


class TestRealLocalPlainIntoEncryptedTarget(_SharedTests):
    """The copy inherits the target's encryption (its encryption root is the target) and its key."""

    TARGET_ENCRYPTED = True
    COPY, ROOT = 'plain', 'target'


class TestRealLocalRawIntoEncryptedTarget(_SharedTests):
    """A raw copy under an encrypted target is still its own root, with no key of its own."""

    SOURCE_ENCRYPTED = True
    TARGET_ENCRYPTED = True
    COPY, ROOT = 'raw', 'leaf'


class _SharesmbTargetTests(_RigTests):
    """OpenZFS 2.1 skips `-o sharesmb=off` on a RAW receive and exits 0, so a copy under a target with
    `sharesmb=on` would read `on  inherited from <target>`. The manager makes every forced property local
    after each committed step (`_enforce_forced_properties`): the copy reads `sharesmb off local`, after the
    first full and after an incremental one, and the target itself is untouched."""

    TARGET_SHARESMB = True

    def test_the_copy_reads_sharesmb_off_local_under_a_target_with_sharesmb_on(self, rig):
        assert _zfs_get(rig.target, 'sharesmb') == 'on'
        first = self.snapshots(rig, [10])[0]

        assert rig.cycle() is True

        assert set(rig.copy_snaps()) == {first}
        assert _zfs_get_source(rig.copy, 'sharesmb') == 'local'
        assert rig.copy_prop('sharesmb') == 'off'
        self.assert_copy_state(rig)
        rest = self.snapshots(rig, [8, 6])

        assert rig.cycle() is True

        self.assert_on_the_copy(rig, [first] + rest)
        assert _zfs_get_source(rig.copy, 'sharesmb') == 'local'
        assert rig.copy_prop('sharesmb') == 'off'
        self.assert_copy_state(rig)
        assert _zfs_get(rig.target, 'sharesmb') == 'on'
        assert rig.errors() == []


class TestRealLocalRawUnderASharesmbTarget(_SharesmbTargetTests):
    """A raw copy: the case that needs the fix on OpenZFS 2.1."""

    SOURCE_ENCRYPTED = True
    COPY, ROOT = 'raw', 'leaf'


class TestRealLocalPlainUnderASharesmbTarget(_SharesmbTargetTests):
    """The control: a plain receive applies `-o sharesmb=off` itself; nothing is set afterwards."""

    COPY, ROOT = 'plain', None


# ---------------------------------------------------------------------------
# Pruning is never recursive
# ---------------------------------------------------------------------------

class TestRealLocalPruneNeverRecursive(_RigTests):
    """The config stamped on a copy says `recursive: true`, but a copy is pruned by its own snapshots
    only: `zfs destroy copy@old`, never `-r`, so a dataset below the copy keeps what it has."""

    RECURSIVE = True

    def test_a_child_below_a_copy_keeps_its_snapshots(self, rig, sdir, mocker):
        first = self.snapshots(rig, [8])[0]
        assert rig.cycle() is True
        rest = self.snapshots(rig, [7, 6, 0.2])
        assert rig.cycle() is True
        names = [first] + rest
        assert rig.dsi.config.recursive is True
        child = f'{rig.copy}/child'
        _zfs_run('create', '-u', '-o', 'canmount=noauto', child)
        for name in names[:2]:                                        # the very names the copy loses
            _zfs_run('snapshot', f'{child}@{name}')
        child_before = _snapshot_guids(child)
        assert set(child_before) == set(names[:2])

        worker = PruningWorker(rig.config, StopFlag(), status_dir=sdir, client_id=CLIENT_ID)
        mocker.patch.object(worker, '_wait', return_value=ExitReason.STOP)
        worker.run()

        assert set(rig.copy_snaps()) == {names[-1]}
        assert _snapshot_guids(child) == child_before


# ---------------------------------------------------------------------------
# The target pool exported mid-cycle (cross-pool only)
# ---------------------------------------------------------------------------

class TestRealLocalTargetPoolExported(_RigTests):

    @pytest.fixture
    def pools(self, request, auto_zfs_pool):
        return _pools(True, request, auto_zfs_pool)

    def test_a_target_pool_exported_mid_cycle_is_skipped_as_gone_and_recovers_after_the_import(
            self, rig, pools):
        names = self.prime(rig, hours=(10, 8))
        before = rig.copy_snaps()
        new = self.snapshots(rig, [3])[0]
        real_negotiate = rig.mgr._negotiate

        def export_then_negotiate(*args, **kwargs):
            r = _zpool_run('export', pools.second.name)
            if r.returncode != 0:
                pytest.skip(f'cannot export the second pool: {r.stderr.strip()}')
            return real_negotiate(*args, **kwargs)

        try:
            # the target was found to exist at the start of the cycle; the pool goes before the negotiate
            with mock.patch.object(rig.mgr, '_negotiate', export_then_negotiate):
                assert rig.cycle() is False

            errors = rig.errors()
            assert any('is gone' in m and rig.target in m for m in errors), errors
            assert 'is gone' in (rig.mgr.last_problem(rig.src, DEST) or '')
            assert rig.manager.get_anchor(rig.dsi, DEST) == names[-1]    # nothing was recorded
            # the rest of the cycle skips the destination quietly: no second report
            mark = len(errors)
            assert rig.mgr.backup_dataset(rig.dsi, rig.remote_cfg) is False
            assert len(rig.errors()) == mark

            # the next cycle, the pool still exported: skipped before anything is sent
            assert rig.cycle() is False
            assert any(rig.target in m for m in rig.errors()[mark:])
            assert _zfs_get(rig.copy, 'name') is None
        finally:
            _zpool_run('import', '-d', pools.second.image_dir, pools.second.name)

        assert _zfs_get(rig.copy, 'name') == rig.copy                 # the pool is back, the copy intact
        assert rig.copy_snaps() == before

        assert rig.cycle() is True                                    # and the chain continues from the anchor

        assert set(rig.copy_snaps()) == set(before) | {new}
        self.assert_on_the_copy(rig, names + [new])
        assert rig.manager.get_anchor(rig.dsi, DEST) == new
        assert any('is usable again' in m for m in rig.log_lines(logging.INFO))
        self.assert_owned_locally(rig)
