"""The receive-writer gate: never a NEW receive writer beside an OLDER generation's writer that could
still commit into the same tree.

The API worker (client receives) and the remote worker (a local destination's `zfs receive`) can both write
into `<target>/<client id>`, but only while a local destination points at the API's target AND the API does not
reserve the sender's client id. Each worker is respawned as soon as ITS old incarnation exits, so without a gate
the new local sender could decide `-F` against a base while the old API worker, still draining, commits a newer
snapshot (or the reverse). `daemon.api_footprint` / `remote_footprint` / `receive_conflict` decide, `_spawn`
records a footprint per incarnation, and `BackupDaemon._spawn_held_by_receive_writer` holds the spawn.

- ``TestApiFootprint`` / ``TestRemoteFootprint`` / ``TestReceiveConflict``  the three pure functions;
- ``TestHeldBehind``       which transitions hold the NEW REMOTE worker (behind an older-epoch API whose target
                           overlaps a new local target and that does not reserve the id) and which do not; the
                           API worker is never held: loopback turned local, entering and leaving the shared
                           target, a client-id change, an ancestor/descendant target, steady state;
- ``TestHoldMechanics``    bookkeeping: `_restart_at` / `_deferred_spawns`, `applied=`, the lost forkserver
                           orphan, a dead or unrecorded blocker, a second reload during the wait, a retired
                           worker, stop and version-skew, snapshot/pruning unaffected;
- ``TestHoldLogging`` / ``TestLongHoldWarning`` / ``TestReleaseVerb``  one INFO per epoch, the release wording, the
                           WARNING after `RELOAD_DEFER_NOTICE` (repeating every `RELOAD_DEFER_REPEAT`, naming
                           the old api pid), the verb a held spawn is released with;
- ``TestRealProcessesRespectTheGate``  real children under fork/spawn/forkserver
                           (`tests/_gate_workers.py`): the new remote starts after the old API worker exited;
- ``TestRealProcessesNeverHoldTheApi``  the new API double starts while the old remote double is still alive;
- ``TestRemoteWorkerFinish`` / ``TestStuckChildMayWrite`` / ``TestRemoteWorkerStuckWrites``  `RemoteBackupWorker`
                           waits, on a reload or an upgrade drain only, for abandoned children that can WRITE
                           (`stuck_child_may_write`), at most `REMOTE_STUCK_WRITE_WAIT`, ended early by a stop
                           alone.

Mutation counts (scratch copy of the tree, one revert at a time; failing tests plus fixture errors in this file):
the gate never holding 38; overlap reduced to equality 7; overlap as a string prefix 6; an API hold reintroduced 12;
`receive_conflict` ignoring the reserved id 4; a lost reload verb 4; no long-hold WARNING 3; the writer wait not
bounded 2; a gone supervisor ending the writer wait 2; a stop not ending it 2; read-only children waited for 5.

The in-process classes use the scripted `World` of `test_zfsbackup_reload.py` (fake workers, a fake clock, a
scripted database); nothing forks and nothing reaches ZFS.
"""

import itertools
import logging
import multiprocessing
import time
from types import SimpleNamespace

import pytest

import zfsbackup.daemon as daemon_module
from _gate_workers import (
    LateExitApiWorker, LateExitRemoteWorker, StartRecordingApiWorker, StartRecordingRemoteWorker, read_events,
)
from test_zfsbackup_reload import (  # noqa: F401  (fixtures and helpers)
    STORE, World, _debug_logging, _private_home, _restore_signal_handlers, messages,
)
from zfsbackup import workers as workers_module
from zfsbackup.config import (
    BackupConfig, DatasetConfig, Destination, RemoteDatasetConfig, RemoteServerConfig,
)
from zfsbackup.daemon import (
    RECEIVE_GATE_RECHECK, RELOAD_DEFER_NOTICE, RELOAD_DEFER_REPEAT, WORKER_RESTART_DELAY, BackupDaemon,
    Handoff, _datasets_overlap, api_footprint, receive_conflict, remote_footprint,
)
from zfsbackup.stopflag import ReloadEpoch, StopFlag
from zfsbackup.workers import (
    WORKER_EXIT_RELOAD, ExitReason, RemoteBackupWorker, stuck_child_may_write,
)

T = "pool/backups"
T2 = "pool/other"
HOST = "host-1"
HOST2 = "host-2"

HOLD = "the receive targets changed"
RELEASED = "has exited: starting the"
NO_CONFLICT = "no longer conflicts with the"


def make(*, server=None, enabled=True, local=(), https=False, prefix="autosnap") -> BackupConfig:
    """One dataset sent to every destination named here. `server` is `remote_backup.target_dataset` (`None`: no
    server), `local` the `zfs:` targets, `https` adds an off-site destination (so a remote worker exists
    without any local one)."""
    destinations, refs = {}, []
    if https:
        destinations["offsite"] = Destination(url="https://h/pool")
        refs.append(RemoteDatasetConfig(destination="offsite"))
    for index, target in enumerate(local):
        destinations[f"usb{index}"] = Destination(url=f"zfs:{target}")
        refs.append(RemoteDatasetConfig(destination=f"usb{index}"))
    return BackupConfig(
        datasets=[DatasetConfig(name="pool/data", remote=refs)],
        destinations=destinations,
        remote_backup=None if server is None else RemoteServerConfig(target_dataset=server, enabled=enabled),
        snapshot_prefix=prefix,
    )


# ---------------------------------------------------------------------------
# The pure functions
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestApiFootprint:
    def test_a_shared_target_reserves_the_hosts_id(self):
        assert api_footprint(make(server=T, local=[T]), HOST) == (T, HOST)

    def test_an_unshared_target_reserves_nothing(self):
        assert api_footprint(make(server=T, https=True), HOST) == (T, None)
        assert api_footprint(make(server=T, local=[T2]), HOST) == (T, None)

    def test_a_disabled_or_missing_remote_backup_serves_no_target(self):
        assert api_footprint(make(server=T, enabled=False, local=[T]), HOST) == (None, None)
        assert api_footprint(make(), HOST) == (None, None)

    def test_a_trailing_slash_is_normalised(self):
        assert api_footprint(make(server=T + "/", https=True), HOST) == (T, None)
        assert api_footprint(make(server=T + "/", local=[T]), HOST) == (T, HOST)

    def test_without_a_client_id_a_shared_target_reserves_none(self):
        assert api_footprint(make(server=T, local=[T]), None) == (T, None)

    def test_it_is_pure(self, mocker):
        zfs = mocker.patch("subprocess.Popen")
        api_footprint(make(server=T, local=[T]), HOST)
        zfs.assert_not_called()


@pytest.mark.unit
class TestRemoteFootprint:
    def test_every_declared_local_target_with_the_id(self):
        config = make(server=T, local=[T, T2])
        assert remote_footprint(config, HOST) == (frozenset({T, T2}), HOST)

    def test_https_destinations_are_ignored(self):
        assert remote_footprint(make(server=T, https=True), HOST) == (frozenset(), HOST)

    def test_a_local_destination_counts_even_when_no_dataset_uses_it(self):
        config = make(server=T)
        config.destinations["usb"] = Destination(url=f"zfs:{T2}")
        assert remote_footprint(config, HOST) == (frozenset({T2}), HOST)

    def test_targets_are_normalised_and_a_missing_id_is_kept(self):
        assert remote_footprint(make(local=[T + "/"]), None) == (frozenset({T}), None)

    def test_no_destinations(self):
        assert remote_footprint(make(), HOST) == (frozenset(), HOST)


@pytest.mark.unit
class TestReceiveConflict:
    @pytest.mark.parametrize("api, remote, expected", [
        pytest.param((T, None), (frozenset({T}), HOST), True, id="unreserved_api_vs_sender"),
        pytest.param((T, HOST2), (frozenset({T}), HOST), True, id="another_reserved_id"),
        pytest.param((T, HOST), (frozenset({T}), HOST), False, id="the_api_reserves_the_senders_id"),
        pytest.param((T, None), (frozenset({T}), None), False, id="a_sender_without_an_id_writes_nothing"),
        pytest.param((None, None), (frozenset({T}), HOST), False, id="no_api_target"),
        pytest.param((T, None), (frozenset({T2}), HOST), False, id="another_target"),
        pytest.param((T, None), (frozenset(), HOST), False, id="no_local_targets"),
        pytest.param((T, None), (frozenset({T2, T}), HOST), True, id="one_of_several_targets"),
        pytest.param((T, None), (frozenset({T + "/inner"}), HOST), True, id="api_target_is_an_ancestor"),
        pytest.param((T + "/srv", None), (frozenset({T}), HOST), True, id="api_target_is_a_descendant"),
        pytest.param((T + "/srv", HOST), (frozenset({T}), HOST), False, id="overlap_but_the_id_is_reserved"),
        pytest.param(("pool/a", None), (frozenset({"pool/ab"}), HOST), False, id="sibling_prefix_is_no_overlap"),
        pytest.param(("pool/ab", None), (frozenset({"pool/a"}), HOST), False, id="sibling_prefix_reversed"),
        pytest.param(("pool", None), (frozenset({"other"}), HOST), False, id="different_pools"),
        pytest.param((T, None), (frozenset({T}), None), False, id="overlap_without_a_sender_id"),
    ])
    def test_the_matrix(self, api, remote, expected):
        assert receive_conflict(api, remote) is expected

    @pytest.mark.parametrize("a, b, expected", [
        ("pool/a", "pool/a", True),
        ("pool/a", "pool/a/b", True),
        ("pool/a/b/c", "pool/a", True),
        ("pool", "pool/a", True),
        ("pool/a", "pool/ab", False),
        ("pool/ab", "pool/a", False),
        ("pool/a/b", "pool/a/c", False),
        ("pool", "pool2", False),
        ("a", "b", False),
    ])
    def test_the_overlap_is_component_wise_and_symmetric(self, a, b, expected):
        assert _datasets_overlap(a, b) is expected
        assert _datasets_overlap(b, a) is expected

    def test_targets_are_compared_without_a_trailing_slash(self):
        """The footprints normalise; `receive_conflict` compares what it is given exactly."""
        config_api = make(server=T + "/", https=True)
        config_remote = make(local=[T + "//"])
        assert receive_conflict(
            api_footprint(config_api, HOST), remote_footprint(config_remote, HOST)
        ) is True


# ---------------------------------------------------------------------------
# The scripted daemon
# ---------------------------------------------------------------------------


def boot(mocker, tmp_path, config, client_id=HOST) -> World:
    world = World(mocker, tmp_path, config=config, client_id=client_id)
    world.start_workers()
    world.events.clear()
    return world


def reload_to(world, config, *, client_id=None, key=None):
    """The database now holds `config` (and `client_id`); the poll accepts it."""
    if client_id is not None:
        world.db_client_id = client_id
    gen = world.daemon._handoff.key[1] + 1
    world.write(key or (STORE, gen), config=config)
    world.poll()
    assert world.daemon._handoff.key == (key or (STORE, gen)), "the candidate was not accepted"


def worker(world, name):
    return world.daemon._workers[name]


def leave(world, *names):
    """These (old) workers exit for the reload, then the supervisor classifies the deaths."""
    for name in names:
        worker(world, name).die(WORKER_EXIT_RELOAD)
    world.daemon._check_workers()


def later(world, seconds=RECEIVE_GATE_RECHECK + 0.5):
    world.advance(seconds)
    world.daemon._check_workers()


def forked(world, name):
    return world.events.count(f"fork:{name}")


def held(world, name):
    d = world.daemon
    return name in d._restart_at and name in d._deferred_spawns and name not in d._workers


def not_held(world, name):
    d = world.daemon
    return name not in d._restart_at and name not in d._deferred_spawns and name in d._workers


def hold_lines(caplog):
    """The hold lines logged so far, the fixtures' (setup phase) included."""
    records = list(caplog.get_records("setup")) + list(caplog.records)
    return [
        r.getMessage() for r in records
        if r.name == "zfsbackup.daemon" and r.levelno == logging.INFO and HOLD in r.getMessage()
    ]


# ---------------------------------------------------------------------------
# Which transitions hold which worker
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestHeldBehind:
    # -- a loopback destination turned local ------------------------------------------------------------

    @pytest.mark.parametrize("old_remote", [False, True], ids=["new_remote", "remote_turned_over"])
    @pytest.mark.parametrize("server_after", [(None, True), (T2, True)], ids=["server_disabled", "server_moved"])
    def test_a_loopback_turned_local_holds_the_new_remote_behind_the_old_api(
        self, mocker, tmp_path, caplog, old_remote, server_after
    ):
        """Generation 0: the API serves T unreserved (`(T, None)`). Generation 1 sends to `zfs:T` itself while
        the server no longer serves T: the new sender must not start while the old API can still receive into
        `T/<id>`."""
        w = boot(mocker, tmp_path, make(server=T, https=old_remote))
        target = server_after[0]
        new = make(server=target or T, enabled=target is not None, local=[T])
        reload_to(w, new)
        if old_remote:
            leave(w, "remote")
        assert forked(w, "remote") == 0
        assert held(w, "remote")
        assert "remote" not in w.daemon._workers
        assert len(hold_lines(caplog)) == 1
        assert "waiting for the old api worker to finish its receives before starting the remote worker" \
            in hold_lines(caplog)[0]

    def test_the_old_api_leaving_releases_the_remote_on_the_next_due_poll(self, mocker, tmp_path, caplog):
        w = boot(mocker, tmp_path, make(server=T))
        reload_to(w, make(server=T2, local=[T]))
        assert held(w, "remote")
        leave(w, "api")                                    # the old API worker is gone, the new one starts
        assert forked(w, "remote") == 0, "released before it was due"
        assert forked(w, "api") == 1
        later(w)
        assert forked(w, "remote") == 1
        assert not_held(w, "remote")
        assert messages(caplog, logging.INFO, "the old api worker has exited: starting the remote worker")

    # -- entering the shared target ---------------------------------------------------------------------

    @pytest.mark.parametrize("elsewhere", [False, True], ids=["offsite_only", "local_dest_elsewhere"])
    def test_entering_the_shared_target_does_not_hold_the_new_api_behind_the_old_remote(
        self, mocker, tmp_path, caplog, elsewhere
    ):
        """The new API reserves the id, so it can never receive into `T/<id>`: it needs no wait for the old
        sender."""
        old = make(server=T, https=True, local=[T2] if elsewhere else [])
        new = make(server=T, https=True, local=[T2, T] if elsewhere else [T])
        w = boot(mocker, tmp_path, old)
        reload_to(w, new)
        leave(w, "api")
        assert forked(w, "api") == 1
        assert not_held(w, "api")
        assert hold_lines(caplog) == []

    def test_entering_the_shared_target_does_hold_the_new_remote_behind_the_old_unreserved_api(
        self, mocker, tmp_path, caplog
    ):
        """The old API may still be accepting this host's id into `T/<id>`: the new local sender waits."""
        w = boot(mocker, tmp_path, make(server=T, https=True))
        reload_to(w, make(server=T, https=True, local=[T]))
        leave(w, "remote")
        assert forked(w, "remote") == 0 and held(w, "remote")
        leave(w, "api")
        later(w)
        assert forked(w, "remote") == 1

    # -- leaving the shared target ----------------------------------------------------------------------

    def test_leaving_the_shared_target_does_not_hold_the_new_remote_behind_the_old_reserving_api(
        self, mocker, tmp_path, caplog
    ):
        """The old API reserved the id, so it never wrote `T/<id>`; the sender keeps `zfs:T` (the server moved)."""
        w = boot(mocker, tmp_path, make(server=T, local=[T]))
        reload_to(w, make(server=T2, local=[T]))
        leave(w, "remote")
        assert forked(w, "remote") == 1
        assert not_held(w, "remote")
        assert hold_lines(caplog) == []

    def test_leaving_the_shared_target_by_dropping_the_local_destination_holds_nothing_for_the_remote(
        self, mocker, tmp_path, caplog
    ):
        w = boot(mocker, tmp_path, make(server=T, local=[T]))
        reload_to(w, make(server=T, https=True))
        leave(w, "remote")
        assert forked(w, "remote") == 1
        assert hold_lines(caplog) == []

    def test_leaving_the_shared_target_never_holds_the_new_api_behind_the_old_remote(
        self, mocker, tmp_path, caplog
    ):
        """The old sender may still be writing `T/<id>`, but only the host's own remote worker holds a key for
        its own id: holding the API would take the server offline for every client."""
        w = boot(mocker, tmp_path, make(server=T, local=[T]))
        reload_to(w, make(server=T, https=True))
        leave(w, "api")
        assert forked(w, "api") == 1
        assert not_held(w, "api")
        assert hold_lines(caplog) == []
        assert "api" not in w.daemon._receive_gate_held

    # -- a client id change ------------------------------------------------------------------------------

    def test_a_client_id_change_while_shared_holds_the_new_remote_behind_the_old_api(
        self, mocker, tmp_path, caplog
    ):
        """The reservation and the local copies move from `<target>/<old id>` to `<target>/<new id>`: the new
        sender is judged against the old API, which reserved only the OLD id."""
        shared = make(server=T, local=[T])
        w = boot(mocker, tmp_path, shared)
        reload_to(w, shared, client_id=HOST2)
        assert w.daemon._handoff.client_id == HOST2
        leave(w, "remote")
        assert forked(w, "remote") == 0 and held(w, "remote")
        leave(w, "api")
        later(w)
        assert forked(w, "remote") == 1

    def test_a_client_id_change_while_shared_never_holds_the_new_api(self, mocker, tmp_path, caplog):
        shared = make(server=T, local=[T])
        w = boot(mocker, tmp_path, shared)
        reload_to(w, shared, client_id=HOST2)
        leave(w, "api")
        assert forked(w, "api") == 1 and not_held(w, "api")
        assert hold_lines(caplog) == []

    # -- an overlapping (not only an equal) target ------------------------------------------------------

    @pytest.mark.parametrize("old_server, new_local", [
        pytest.param(T, T + "/inner", id="old_api_target_is_an_ancestor"),
        pytest.param(T + "/srv", T, id="old_api_target_is_a_descendant"),
    ])
    def test_a_new_remote_is_held_behind_an_old_api_whose_target_overlaps_the_new_local_target(
        self, mocker, tmp_path, caplog, old_server, new_local
    ):
        w = boot(mocker, tmp_path, make(server=old_server))
        reload_to(w, make(server=T2, local=[new_local]))
        assert forked(w, "remote") == 0 and held(w, "remote")
        assert len(hold_lines(caplog)) == 1
        leave(w, "api")
        later(w)
        assert forked(w, "remote") == 1

    def test_a_sibling_whose_name_only_shares_a_prefix_holds_nothing(self, mocker, tmp_path, caplog):
        w = boot(mocker, tmp_path, make(server="pool/bk"))
        reload_to(w, make(server=T2, local=["pool/bkp"]))
        assert forked(w, "remote") == 1 and not_held(w, "remote")
        assert hold_lines(caplog) == []

    # -- an equal footprint holds nothing ---------------------------------------------------------------

    @pytest.mark.parametrize("name", ["api", "remote"])
    def test_an_unrelated_setting_change_while_shared_holds_nothing(self, mocker, tmp_path, caplog, name):
        w = boot(mocker, tmp_path, make(server=T, local=[T]))
        reload_to(w, make(server=T, local=[T], prefix="changed"))
        leave(w, name)
        assert forked(w, name) == 1 and not_held(w, name)
        assert hold_lines(caplog) == []

    @pytest.mark.parametrize("name", ["api", "remote"])
    def test_an_unrelated_setting_change_without_a_shared_target_holds_nothing(
        self, mocker, tmp_path, caplog, name
    ):
        w = boot(mocker, tmp_path, make(server=T, https=True))
        reload_to(w, make(server=T, https=True, prefix="changed"))
        leave(w, name)
        assert forked(w, name) == 1
        assert hold_lines(caplog) == []

    def test_a_local_destination_elsewhere_never_conflicts_across_a_reload(
        self, mocker, tmp_path, caplog
    ):
        w = boot(mocker, tmp_path, make(server=T, local=[T2]))
        reload_to(w, make(server=T, local=[T2], prefix="changed"))
        leave(w, "api", "remote")
        assert forked(w, "api") == 1 and forked(w, "remote") == 1
        assert hold_lines(caplog) == []

    def test_a_target_change_without_a_local_destination_at_it_holds_nothing(self, mocker, tmp_path, caplog):
        w = boot(mocker, tmp_path, make(server=T, https=True))
        reload_to(w, make(server=T2, https=True))
        leave(w, "api", "remote")
        assert forked(w, "api") == 1 and forked(w, "remote") == 1

    # -- steady state ------------------------------------------------------------------------------------

    @pytest.mark.parametrize("name", ["api", "remote"])
    @pytest.mark.parametrize("config", [make(server=T, local=[T]), make(server=T, https=True)],
                             ids=["shared", "plain"])
    def test_a_crash_restart_on_the_same_epoch_holds_nothing(self, mocker, tmp_path, caplog, name, config):
        w = boot(mocker, tmp_path, config)
        worker(w, name).die(1)
        w.daemon._check_workers()
        assert name in w.daemon._restart_at              # the ordinary backoff, nothing else
        w.advance(WORKER_RESTART_DELAY + 1)
        w.daemon._check_workers()
        assert forked(w, name) == 1
        assert hold_lines(caplog) == [] and not w.daemon._deferred_spawns
        assert w.daemon._failures.get(name) == 1


# ---------------------------------------------------------------------------
# Bookkeeping around a hold
# ---------------------------------------------------------------------------


@pytest.fixture
def held_remote(mocker, tmp_path):
    """Generation 0 serves T (`(T, None)`); generation 1 sends to `zfs:T` with the server moved to T2. The new
    remote worker is held behind the old API worker."""
    w = boot(mocker, tmp_path, make(server=T))
    reload_to(w, make(server=T2, local=[T]))
    assert held(w, "remote")
    return w


@pytest.mark.unit
class TestHoldMechanics:
    def test_a_hold_forks_nothing_defers_applied_and_is_retried_each_poll(self, held_remote, caplog):
        w = held_remote
        d = w.daemon
        assert d._restart_at["remote"] == pytest.approx(w.clock[0] + RECEIVE_GATE_RECHECK)
        assert d._deferred_spawns == frozenset({"remote"})
        for _ in range(4):
            later(w)
        assert forked(w, "remote") == 0
        assert w.lock.state["applied"] != d._handoff.key
        assert d._failures == {}

    def test_applied_is_published_only_once_the_held_worker_exists(self, held_remote):
        w = held_remote
        key = w.daemon._handoff.key
        leave(w, "snapshot", "pruning", "api")
        assert w.lock.state["applied"] != key            # every other worker turned over; remote still held
        later(w)
        assert forked(w, "remote") == 1
        assert w.lock.state["applied"] == key
        assert w.daemon._deferred_spawns == frozenset()

    def test_snapshot_and_pruning_turn_over_immediately_while_the_remote_is_held(self, held_remote):
        w = held_remote
        leave(w, "snapshot", "pruning")
        assert forked(w, "snapshot") == 1 and forked(w, "pruning") == 1
        assert held(w, "remote")
        assert not_held(w, "snapshot") and not_held(w, "pruning")

    def test_a_held_worker_is_not_a_failure_and_costs_no_backoff(self, held_remote):
        w = held_remote
        for _ in range(6):
            later(w)
        assert w.daemon._failures == {}
        assert w.daemon._restart_at["remote"] - w.clock[0] <= RECEIVE_GATE_RECHECK

    # -- a blocker that is not running ------------------------------------------------------------------

    def test_a_dead_old_api_does_not_hold(self, mocker, tmp_path):
        w = boot(mocker, tmp_path, make(server=T))
        worker(w, "api").die(WORKER_EXIT_RELOAD)         # exited, not yet classified
        reload_to(w, make(server=T2, local=[T]))
        assert forked(w, "remote") == 1 and not_held(w, "remote")

    def test_a_missing_footprint_record_does_not_hold(self, mocker, tmp_path):
        """A worker recorded by hand (as other tests do) carries no footprint: nothing is held for it."""
        w = boot(mocker, tmp_path, make(server=T))
        w.daemon._worker_fp.pop("api")
        reload_to(w, make(server=T2, local=[T]))
        assert forked(w, "remote") == 1 and not_held(w, "remote")

    def test_the_footprint_is_recorded_with_the_epoch_at_every_spawn(self, mocker, tmp_path):
        w = boot(mocker, tmp_path, make(server=T, local=[T], https=True))
        assert w.daemon._worker_fp["api"] == (0, (T, HOST))
        assert w.daemon._worker_fp["remote"] == (0, (frozenset({T}), HOST))
        reload_to(w, make(server=T, https=True))
        leave(w, "api", "remote")
        assert w.daemon._worker_fp["api"][0] == 1 and w.daemon._worker_fp["remote"][0] == 1

    def test_a_forgotten_worker_loses_its_footprint(self, held_remote):
        w = held_remote
        leave(w, "api")
        assert w.daemon._worker_fp["api"][0] == 1        # replaced by the new incarnation's
        reload_to(w, make(server=T2))                      # no remote worker any more
        assert "remote" not in w.daemon._worker_fp and "remote" not in w.daemon._receive_gate_held

    # -- a lost forkserver orphan -----------------------------------------------------------------------

    def test_a_lost_orphan_that_still_lives_holds_and_its_exit_releases_without_backoff(
        self, mocker, tmp_path, caplog
    ):
        w = boot(mocker, tmp_path, make(server=T))
        d = w.daemon
        state = {"ident": "match"}
        mocker.patch.object(daemon_module.procident, "identity", side_effect=lambda pid, token: state["ident"])
        d._worker_token["api"] = "token"
        worker(w, "api").die(255)                         # the forkserver is gone: poll() says 255
        d._check_workers()
        assert "api" in d._lost                           # classified: alive although poll() says dead
        reload_to(w, make(server=T2, local=[T]))
        assert held(w, "remote")                          # a lost API worker still counts while its pid lives
        later(w)
        assert forked(w, "remote") == 0
        state["ident"] = "gone"                           # the orphan has exited
        later(w)
        assert forked(w, "remote") == 1 and forked(w, "api") == 1
        assert d._failures == {} and not d._restart_at and not d._deferred_spawns
        assert messages(caplog, logging.INFO, "the old api worker has exited: starting the remote worker")

    # -- a second reload during the wait ----------------------------------------------------------------

    def test_going_back_to_the_old_footprint_releases_at_once_judged_against_the_alive_api(
        self, held_remote, caplog
    ):
        w = held_remote
        reload_to(w, make(server=T, https=True, prefix="back"))    # the remote worker no longer sends locally
        assert forked(w, "remote") == 1
        assert not_held(w, "remote") and w.daemon._deferred_spawns == frozenset()
        assert messages(caplog, logging.INFO, "no longer conflicts with the remote worker")
        assert not messages(caplog, logging.INFO, RELEASED)

    def test_a_second_reload_that_still_conflicts_stays_held_with_one_line_per_epoch(
        self, held_remote, caplog
    ):
        w = held_remote
        reload_to(w, make(server=T2, local=[T], prefix="again"))
        for _ in range(3):
            later(w)
        assert forked(w, "remote") == 0 and held(w, "remote")
        assert len(hold_lines(caplog)) == 2                  # epoch 1 and epoch 2, once each

    def test_a_held_worker_the_next_reload_no_longer_needs_is_forgotten(self, held_remote, caplog):
        w = held_remote
        d = w.daemon
        reload_to(w, make(server=T, prefix="no-remote"))      # no dataset sends anywhere
        assert "remote" not in d._restart_at
        assert d._deferred_spawns == frozenset()
        assert d._receive_gate_held == {} and "remote" not in d._workers
        assert forked(w, "remote") == 0
        assert messages(caplog, logging.INFO, "remote worker retired")
        leave(w, "snapshot", "pruning", "api")
        assert w.lock.state["applied"] == d._handoff.key       # nothing waits for the retired worker
        later(w)
        assert forked(w, "remote") == 0

    # -- stop and version skew --------------------------------------------------------------------------

    def test_a_stop_during_the_wait_forks_nothing(self, held_remote, caplog):
        w = held_remote
        leave(w, "api")
        w.daemon._stop_event.set()
        later(w)
        assert forked(w, "remote") == 0
        assert messages(caplog, logging.INFO, "Shutdown requested; not restarting remaining worker(s): remote")

    def test_a_version_skew_drain_wins_and_adds_no_hold_line(self, mocker, tmp_path, caplog):
        w = boot(mocker, tmp_path, make(server=T))
        d = w.daemon
        d._handoff = Handoff(make(server=T2, local=[T]), 1, (STORE, 1), HOST, None)
        d._version_skew = (None, None)                         # draining for an upgrade
        assert d._spawn("remote") is None
        assert forked(w, "remote") == 0
        assert hold_lines(caplog) == []
        assert "remote" not in d._restart_at and not d._deferred_spawns

    @pytest.mark.parametrize("name, config, record", [
        ("remote", make(server=T2, local=[T]), ("api", (T, None))),
    ])
    def test_an_incarnation_of_the_current_epoch_never_holds_even_with_a_conflicting_record(
        self, mocker, tmp_path, name, config, record
    ):
        """Only OLDER generations are waited for: a record of the epoch being spawned is not a blocker (it is the
        same configuration's own worker, and the same kind of worker waits for its predecessor anyway)."""
        w = boot(mocker, tmp_path, make(server=T))
        d = w.daemon
        d._handoff = Handoff(config, 0, (STORE, 0), HOST, None)
        other, footprint = record
        d._worker_fp[other] = (0, footprint)
        d._workers.setdefault(other, SimpleNamespace(is_alive=lambda: True, pid=1))
        # The same record from an OLDER epoch would hold (the control).
        d._worker_fp[other] = (-1, footprint)
        assert d._spawn_held_by_receive_writer(name) is True
        d._receive_gate_held.clear()
        d._worker_fp[other] = (0, footprint)
        assert d._spawn_held_by_receive_writer(name) is False

    def test_snapshot_and_pruning_are_outside_the_gate(self, held_remote):
        w = held_remote
        assert w.daemon._spawn_held_by_receive_writer("snapshot") is False
        assert w.daemon._spawn_held_by_receive_writer("pruning") is False


# ---------------------------------------------------------------------------
# What the gate says
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestHoldLogging:
    def test_one_hold_line_per_worker_per_epoch_across_polls(self, held_remote, caplog):
        w = held_remote
        for _ in range(5):
            later(w)
        assert len(hold_lines(caplog)) == 1
        line = hold_lines(caplog)[0]
        assert "remote worker on generation" in line

    def test_both_old_writers_leaving_together_hold_nothing(self, mocker, tmp_path, caplog):
        """The API turns over first (it is never held) and its record is the NEW epoch's by the time the remote
        is asked: nothing older is left to wait for."""
        shared = make(server=T, local=[T])
        w = boot(mocker, tmp_path, shared)
        reload_to(w, shared, client_id=HOST2)
        leave(w, "api", "remote")
        assert forked(w, "api") == 1 and forked(w, "remote") == 1
        assert hold_lines(caplog) == []

    def test_the_held_remote_says_it_once_while_the_api_is_still_alive(self, mocker, tmp_path, caplog):
        shared = make(server=T, local=[T])
        w = boot(mocker, tmp_path, shared)
        reload_to(w, shared, client_id=HOST2)
        leave(w, "remote")
        for _ in range(3):
            later(w)
        lines = hold_lines(caplog)
        assert len(lines) == 1 and "starting the remote worker" in lines[0]

    def test_a_held_turnover_keeps_the_reloaded_wording(self, mocker, tmp_path, caplog):
        w = boot(mocker, tmp_path, make(server=T, https=True))
        reload_to(w, make(server=T2, local=[T]))
        leave(w, "remote")                                     # a turnover, held behind the old API
        assert held(w, "remote")
        leave(w, "api")
        later(w)
        assert forked(w, "remote") == 1
        started = messages(caplog, logging.INFO, "remote worker (pid=")
        assert started[-1].startswith("Reloaded remote worker (pid=")

    def test_a_newly_needed_held_worker_is_started_not_reloaded_or_restarted(self, held_remote, caplog):
        w = held_remote
        leave(w, "api")
        later(w)
        assert messages(caplog, logging.INFO, "remote worker (pid=")[-1].startswith("Started remote worker (pid=")

    def test_the_release_names_the_generation_and_whether_the_blocker_exited(self, held_remote, caplog):
        w = held_remote
        leave(w, "api")
        later(w)
        (line,) = messages(caplog, logging.INFO, RELEASED)
        assert "the old api worker has exited" in line and "on generation" in line

    def test_a_later_crash_of_the_released_worker_is_an_ordinary_restart(
        self, held_remote, caplog
    ):
        w = held_remote
        leave(w, "api")
        later(w)
        worker(w, "remote").die(1)
        w.daemon._check_workers()
        w.advance(WORKER_RESTART_DELAY + 1)
        w.daemon._check_workers()
        assert forked(w, "remote") == 2
        assert len(hold_lines(caplog)) == 1                    # the ordinary restart added no line
        assert w.daemon._failures.get("remote") == 1


WAITING = "has been waiting"


def waiting_warnings(caplog):
    return messages(caplog, logging.WARNING, WAITING)


@pytest.mark.unit
class TestLongHoldWarning:
    """A held spawn is not in `_workers`, so `_log_deferred_switches` never names it: `_log_receive_holds` does."""

    def test_nothing_before_the_notice_period(self, held_remote, caplog):
        w = held_remote
        w.advance(RELOAD_DEFER_NOTICE - 2)
        w.daemon._check_workers()
        assert waiting_warnings(caplog) == []

    def test_one_warning_at_the_notice_naming_the_old_api_pid(self, held_remote, caplog):
        w = held_remote
        pid = worker(w, "api").pid
        w.advance(RELOAD_DEFER_NOTICE + 1)
        w.daemon._check_workers()
        (line,) = waiting_warnings(caplog)
        assert f"api worker (pid={pid})" in line and "the remote worker" in line
        assert "in-flight receives are never interrupted" in line
        w.advance(5)
        w.daemon._check_workers()
        assert len(waiting_warnings(caplog)) == 1                    # not every poll

    def test_it_repeats_only_after_the_repeat_interval(self, held_remote, caplog):
        w = held_remote
        w.advance(RELOAD_DEFER_NOTICE + 1)
        w.daemon._check_workers()
        w.advance(RELOAD_DEFER_REPEAT - 5)
        w.daemon._check_workers()
        assert len(waiting_warnings(caplog)) == 1
        w.advance(10)
        w.daemon._check_workers()
        assert len(waiting_warnings(caplog)) == 2

    def test_nothing_after_the_release(self, held_remote, caplog):
        w = held_remote
        w.advance(RELOAD_DEFER_NOTICE + 1)
        w.daemon._check_workers()
        assert len(waiting_warnings(caplog)) == 1
        leave(w, "api")
        later(w)
        assert forked(w, "remote") == 1
        w.advance(RELOAD_DEFER_REPEAT * 2)
        w.daemon._check_workers()
        assert len(waiting_warnings(caplog)) == 1

    def test_no_warning_for_a_hold_that_never_happened(self, mocker, tmp_path, caplog):
        w = boot(mocker, tmp_path, make(server=T, https=True))
        reload_to(w, make(server=T, https=True, prefix="changed"))
        w.advance(RELOAD_DEFER_NOTICE * 3)
        w.daemon._check_workers()
        assert waiting_warnings(caplog) == []

    def test_a_retired_hold_stops_warning(self, held_remote, caplog):
        w = held_remote
        reload_to(w, make(server=T, prefix="no-remote"))
        w.advance(RELOAD_DEFER_NOTICE * 3)
        w.daemon._check_workers()
        assert waiting_warnings(caplog) == []


@pytest.mark.unit
class TestReleaseVerb:
    def test_a_held_start_requested_as_started_is_released_as_started(self, held_remote, caplog):
        w = held_remote
        leave(w, "api")
        later(w)
        assert messages(caplog, logging.INFO, "remote worker (pid=")[-1].startswith("Started remote worker (pid=")

    def test_a_held_turnover_asked_again_by_a_reload_stays_reloaded(self, mocker, tmp_path, caplog):
        w = boot(mocker, tmp_path, make(server=T, https=True))
        reload_to(w, make(server=T2, local=[T]))
        leave(w, "remote")                                    # a turnover: asked as 'Reloaded', held
        assert held(w, "remote")
        reload_to(w, make(server=T2, local=[T], prefix="again"))   # `_adjust_worker_set` asks as 'Started'
        assert held(w, "remote")
        leave(w, "api")
        later(w)
        assert forked(w, "remote") == 1
        assert messages(caplog, logging.INFO, "remote worker (pid=")[-1].startswith("Reloaded remote worker (pid=")

    def test_a_released_crash_restart_is_restarted(self, held_remote, caplog):
        w = held_remote
        leave(w, "api")
        later(w)
        worker(w, "remote").die(1)
        w.daemon._check_workers()
        w.advance(WORKER_RESTART_DELAY + 1)
        w.daemon._check_workers()
        assert messages(caplog, logging.INFO, "remote worker (pid=")[-1].startswith("Restarted remote worker (pid=")


# ---------------------------------------------------------------------------
# Real processes
# ---------------------------------------------------------------------------

_START_METHODS = [m for m in ("fork", "spawn", "forkserver") if m in multiprocessing.get_all_start_methods()]


def _bind(cls, method, record_dir, **attrs):
    def factory(config, stop_event, log_level, **kwargs):
        child = cls(config, stop_event, log_level, **kwargs)
        child.start_method = method
        child.record_dir = str(record_dir)
        for key, value in attrs.items():
            setattr(child, key, value)
        return child
    return factory


@pytest.mark.integration
@pytest.mark.slow
@pytest.mark.filterwarnings(
    "ignore:This process \\(pid=.*\\) is multi-threaded, use of fork\\(\\) "
    "may lead to deadlocks in the child:DeprecationWarning"
)
class TestRealProcessesRespectTheGate:
    """A real `BackupDaemon` forks importable doubles (`tests/_gate_workers.py`): an old API worker that
    finishes a receive late, then exits for the reload, and a new remote worker that records its own start."""

    @pytest.mark.parametrize("method", _START_METHODS)
    def test_the_new_remote_starts_after_the_old_api_has_exited(self, sqlite_config_db, tmp_path, mocker, method):
        records = tmp_path / "rec"
        records.mkdir()
        delay = 1.0
        mocker.patch("zfsbackup.daemon.ApiWorker", side_effect=_bind(LateExitApiWorker, method, records, exit_delay=delay))
        mocker.patch(
            "zfsbackup.daemon.RemoteBackupWorker", side_effect=_bind(StartRecordingRemoteWorker, method, records)
        )
        daemon = BackupDaemon(make(server=T), sqlite_config_db, client_id=HOST)
        started = []
        try:
            old_api = daemon._spawn("api")
            started.append(old_api)
            deadline = time.monotonic() + 60
            while ("api-start-0" not in [e for e, _ in read_events(records)]) and time.monotonic() < deadline:
                time.sleep(0.05)
            assert "api-start-0" in [e for e, _ in read_events(records)], f"{method}: the API double never started"

            # The reload: generation 1 sends to `zfs:T` itself while the server moved away.
            daemon._handoff = Handoff(make(server=T2, local=[T]), 1, (STORE, 1), HOST, None)
            daemon._reload_epoch.set(1)
            assert daemon._spawn("remote", verb="Reloaded") is None
            assert "remote" in daemon._deferred_spawns
            assert old_api.is_alive(), "the old API worker exited before the test could see the hold"

            deadline = time.monotonic() + 90
            while "remote" not in daemon._workers and time.monotonic() < deadline:
                daemon._check_workers()
                time.sleep(0.1)
            assert "remote" in daemon._workers, f"{method}: the remote worker was never started"
            started.extend(w for w in daemon._workers.values() if w not in started)

            deadline = time.monotonic() + 60
            while time.monotonic() < deadline:
                names = [e for e, _ in read_events(records)]
                if "remote-start-1" in names:
                    break
                time.sleep(0.05)
            events = dict(read_events(records))
            assert "api-exit-0" in events and "remote-start-1" in events, events
            assert events["remote-start-1"] > events["api-exit-0"], (
                f"{method}: the new remote worker started {events['api-exit-0'] - events['remote-start-1']:.3f} s "
                "BEFORE the old API worker exited"
            )
            assert daemon._failures == {}
        finally:
            daemon._stop_event.set()
            procs = list(daemon._workers.values()) + started
            for proc in procs:
                try:
                    proc.join(10)
                    if proc.is_alive():
                        proc.terminate()
                        proc.join(5)
                    if proc.is_alive():
                        proc.kill()
                        proc.join(5)
                except Exception:
                    pass
            if method == "forkserver":
                daemon._stop_forkserver()


@pytest.mark.integration
@pytest.mark.slow
@pytest.mark.filterwarnings(
    "ignore:This process \\(pid=.*\\) is multi-threaded, use of fork\\(\\) "
    "may lead to deadlocks in the child:DeprecationWarning"
)
class TestRealProcessesNeverHoldTheApi:
    """The API worker is never held behind an old remote worker: a real old remote double stays alive (until
    the test releases it) while the new API double starts."""

    @pytest.mark.parametrize("method", _START_METHODS)
    @pytest.mark.parametrize("old, new", [
        pytest.param(make(server=T, local=[T]), make(server=T, https=True), id="leaving_shared"),
        pytest.param(make(server=T, https=True), make(server=T, https=True, local=[T]), id="entering_shared"),
    ])
    def test_the_new_api_starts_while_the_old_remote_is_still_alive(
        self, sqlite_config_db, tmp_path, mocker, method, old, new
    ):
        records = tmp_path / "rec"
        records.mkdir()
        release = tmp_path / "release"
        mocker.patch(
            "zfsbackup.daemon.RemoteBackupWorker",
            side_effect=_bind(LateExitRemoteWorker, method, records, release_path=str(release)),
        )
        mocker.patch(
            "zfsbackup.daemon.ApiWorker", side_effect=_bind(StartRecordingApiWorker, method, records)
        )
        daemon = BackupDaemon(old, sqlite_config_db, client_id=HOST)
        started = []

        def names():
            return [e for e, _ in read_events(records)]

        def wait_for(event, bound=90):
            deadline = time.monotonic() + bound
            while event not in names() and time.monotonic() < deadline:
                time.sleep(0.05)
            assert event in names(), f"{method}: {event} never happened: {names()}"

        try:
            old_remote = daemon._spawn("remote")
            started.append(old_remote)
            wait_for("remote-start-0")
            daemon._handoff = Handoff(new, 1, (STORE, 1), HOST, None)
            daemon._reload_epoch.set(1)
            new_api = daemon._spawn("api", verb="Reloaded")
            assert new_api is not None, "the new API worker was held"
            started.append(new_api)
            wait_for("api-start-1")
            assert old_remote.is_alive(), "the old remote worker had already exited"
            assert "remote-exit-0" not in names()
            events = dict(read_events(records))
            assert "api-start-1" in events and "remote-exit-0" not in events
            assert daemon._failures == {} and not daemon._deferred_spawns
        finally:
            release.write_text("go")
            daemon._stop_event.set()
            for proc in list(daemon._workers.values()) + started:
                try:
                    proc.join(10)
                    if proc.is_alive():
                        proc.terminate()
                        proc.join(5)
                    if proc.is_alive():
                        proc.kill()
                        proc.join(5)
                except Exception:
                    pass
            if method == "forkserver":
                daemon._stop_forkserver()


# ---------------------------------------------------------------------------
# RemoteBackupWorker._finish
# ---------------------------------------------------------------------------

STUCK_START = "timed-out zfs process(es) to exit before"
STUCK_END = "the timed-out zfs process(es) have exited"
STUCK_ABORT = "stopped waiting for"


class Stuck:
    """A scripted `libzfseasy.stuck_children`: `remaining` calls report one stuck child (labelled `label`), then
    none."""

    def __init__(self, remaining, then=None, label="zfs receive"):
        self.label = label
        self.remaining = remaining
        self.calls = 0
        self.then = then              # called on each call with the call number (to stop mid-wait)

    def __call__(self):
        self.calls += 1
        if self.then is not None:
            self.then(self.calls)
        if self.remaining > 0:
            self.remaining -= 1
            return [SimpleNamespace(pid=424242, label=self.label, since=0.0)]
        return []


@pytest.fixture
def remote_worker(monkeypatch):
    monkeypatch.setattr(workers_module, "STOP_POLL", 0.01)
    stop = StopFlag()
    epoch = ReloadEpoch()
    return RemoteBackupWorker(
        make(server=T, local=[T]), stop, reload_epoch=epoch, born_epoch=0, client_id=HOST,
    )


def patch_stuck(monkeypatch, stuck):
    import libzfseasy

    monkeypatch.setattr(libzfseasy, "stuck_children", stuck)
    return stuck


def finish(worker, reason, caplog):
    log = logging.getLogger("test.remote")
    with caplog.at_level(logging.INFO):
        try:
            worker._finish(reason, log)
        except SystemExit as exit_:
            return exit_.code
    return None


@pytest.mark.unit
class TestRemoteWorkerFinish:
    def test_a_reload_waits_for_a_stuck_child_then_exits_3(self, remote_worker, monkeypatch, caplog):
        stuck = patch_stuck(monkeypatch, Stuck(4))
        remote_worker.reload_epoch.set(1)
        assert finish(remote_worker, ExitReason.RELOAD, caplog) == WORKER_EXIT_RELOAD
        assert stuck.calls >= 5, "it did not keep asking until the registry was empty"
        starts = [r.getMessage() for r in caplog.records if STUCK_START in r.getMessage()]
        assert len(starts) == 1 and "before reloading" in starts[0] and "pid 424242 (zfs receive)" in starts[0]
        assert sum(STUCK_END in r.getMessage() for r in caplog.records) == 1
        assert "exiting to switch configuration" in caplog.text

    def test_an_upgrade_drain_waits_then_returns(self, remote_worker, monkeypatch, caplog):
        from zfsbackup.workers import EPOCH_UPGRADE_DRAIN

        stuck = patch_stuck(monkeypatch, Stuck(3))
        remote_worker.reload_epoch.set(EPOCH_UPGRADE_DRAIN)
        assert finish(remote_worker, ExitReason.UPGRADE, caplog) is None
        assert stuck.calls >= 4
        starts = [r.getMessage() for r in caplog.records if STUCK_START in r.getMessage()]
        assert len(starts) == 1 and "before upgrading" in starts[0]
        assert "exiting so that the daemon can restart" in caplog.text

    @pytest.mark.parametrize("reason", [ExitReason.STOP, ExitReason.SUPERVISOR_GONE])
    def test_a_stop_or_a_gone_supervisor_does_not_wait(self, remote_worker, monkeypatch, caplog, reason):
        stuck = patch_stuck(monkeypatch, Stuck(3))      # a worker that wrongly waited would end the wait itself
        assert finish(remote_worker, reason, caplog) is None
        assert stuck.calls == 0
        assert STUCK_START not in caplog.text
        assert "remote-backup-worker stopped" in caplog.text

    def test_nothing_stuck_exits_at_once_without_a_line(self, remote_worker, monkeypatch, caplog):
        stuck = patch_stuck(monkeypatch, Stuck(0))
        remote_worker.reload_epoch.set(1)
        assert finish(remote_worker, ExitReason.RELOAD, caplog) == WORKER_EXIT_RELOAD
        assert stuck.calls == 1
        assert STUCK_START not in caplog.text and STUCK_END not in caplog.text

    def test_a_stop_during_the_wait_ends_it_and_the_worker_stops_normally(
        self, remote_worker, monkeypatch, caplog
    ):
        def stop_on_third(call):
            if call == 3:
                remote_worker.stop_event.set()

        stuck = patch_stuck(monkeypatch, Stuck(10 ** 6, then=stop_on_third))
        remote_worker.reload_epoch.set(1)
        assert finish(remote_worker, ExitReason.RELOAD, caplog) is None      # exit status 0, not the reload exit
        assert stuck.calls == 3
        assert sum(STUCK_ABORT in r.getMessage() for r in caplog.records) == 1
        assert STUCK_END not in caplog.text
        assert "exiting to switch configuration" not in caplog.text
        assert " stopped" in caplog.text

    def test_a_failing_registry_read_means_no_wait(self, remote_worker, monkeypatch, caplog):
        def broken():
            raise RuntimeError("registry gone")

        patch_stuck(monkeypatch, broken)
        remote_worker.reload_epoch.set(1)
        assert finish(remote_worker, ExitReason.RELOAD, caplog) == WORKER_EXIT_RELOAD
        assert STUCK_START not in caplog.text

    def test_a_failure_in_the_middle_of_the_wait_ends_it(self, remote_worker, monkeypatch, caplog):
        calls = itertools.count(1)

        def flaky():
            if next(calls) == 1:
                return [SimpleNamespace(pid=7, label="zfs receive", since=0.0)]
            raise OSError("boom")

        patch_stuck(monkeypatch, flaky)
        remote_worker.reload_epoch.set(1)
        assert finish(remote_worker, ExitReason.RELOAD, caplog) == WORKER_EXIT_RELOAD

    def test_the_helpers_are_shared_with_the_api_worker(self):
        from zfsbackup.workers import ApiWorker, _SupervisorWatch

        assert RemoteBackupWorker._wait_for_stuck_children is _SupervisorWatch._wait_for_stuck_children
        assert ApiWorker._wait_for_stuck_children is _SupervisorWatch._wait_for_stuck_children
        assert ApiWorker._reload_drain_abort is _SupervisorWatch._reload_drain_abort


@pytest.mark.unit
class TestStuckChildMayWrite:
    @pytest.mark.parametrize("label", [
        "zfs receive", "zfs set", "zfs destroy", "zfs create", "zfs rename", "zfs snapshot",
        "zfs inherit", "zfs promote", "zfs clone", "zfs rollback", "zfs hold", "zfs mount",
        "zpool create", "zpool set",
    ])
    def test_a_mutating_command_is_a_writer(self, label):
        assert stuck_child_may_write(label) is True

    @pytest.mark.parametrize("label", ["zfs list", "zfs get", "zfs send", "zpool status", "zpool list"])
    def test_a_read_only_command_is_not(self, label):
        assert stuck_child_may_write(label) is False

    @pytest.mark.parametrize("label", [
        "zfs frobnicate", "", "zfs", "zfs 'unterminated", "something else", "zfs\x00receive", "ZFS list",
    ])
    def test_unknown_and_unparsable_labels_are_writers(self, label):
        assert stuck_child_may_write(label) is True

    def test_a_path_or_flags_in_the_label_are_not_read_as_a_subcommand(self):
        assert stuck_child_may_write("/sbin/zfs list") is True       # unclassifiable: assume a writer


@pytest.mark.unit
class TestRemoteWorkerStuckWrites:
    """`RemoteBackupWorker._wait_for_stuck_writes`: only children that can write, bounded, and only a STOP ends
    it early."""

    @pytest.fixture
    def bounded(self, monkeypatch):
        monkeypatch.setattr(workers_module, "REMOTE_STUCK_WRITE_WAIT", 0.3)

    @pytest.mark.parametrize("label", ["zfs list", "zfs get", "zfs send", "zpool status"])
    def test_a_stuck_read_only_child_costs_no_delay_and_no_line(
        self, remote_worker, monkeypatch, caplog, bounded, label
    ):
        stuck = patch_stuck(monkeypatch, Stuck(10 ** 6, label=label))
        remote_worker.reload_epoch.set(1)
        started = time.monotonic()
        assert finish(remote_worker, ExitReason.RELOAD, caplog) == WORKER_EXIT_RELOAD
        assert time.monotonic() - started < 2.0
        assert stuck.calls == 1
        assert STUCK_START not in caplog.text and "gave up" not in caplog.text

    def test_a_stuck_receive_is_waited_for_until_it_is_gone(self, remote_worker, monkeypatch, caplog):
        stuck = patch_stuck(monkeypatch, Stuck(5, label="zfs receive"))
        remote_worker.reload_epoch.set(1)
        assert finish(remote_worker, ExitReason.RELOAD, caplog) == WORKER_EXIT_RELOAD
        assert stuck.calls >= 6
        (start,) = [r.getMessage() for r in caplog.records if STUCK_START in r.getMessage()]
        assert "pid 424242 (zfs receive)" in start and "only those that can write" in start
        assert sum(STUCK_END in r.getMessage() for r in caplog.records) == 1
        assert "gave up" not in caplog.text

    def test_a_read_only_child_does_not_extend_the_wait_for_a_writer(
        self, remote_worker, monkeypatch, caplog, bounded
    ):
        mixed = [SimpleNamespace(pid=1, label="zfs list", since=0.0)]
        calls = itertools.count(1)

        def both():
            n = next(calls)
            rows = [SimpleNamespace(pid=2, label="zfs receive", since=0.0)] if n <= 3 else []
            return mixed + rows

        patch_stuck(monkeypatch, both)
        remote_worker.reload_epoch.set(1)
        assert finish(remote_worker, ExitReason.RELOAD, caplog) == WORKER_EXIT_RELOAD
        (start,) = [r.getMessage() for r in caplog.records if STUCK_START in r.getMessage()]
        assert "waiting for 1 timed-out" in start and "pid 2" in start and "pid 1" not in start
        assert STUCK_END in caplog.text

    @pytest.mark.parametrize("reason, epoch, expected", [
        (ExitReason.RELOAD, 1, WORKER_EXIT_RELOAD),
        (ExitReason.UPGRADE, "upgrade", None),
    ])
    def test_at_the_bound_one_warning_naming_the_child_then_the_planned_exit(
        self, remote_worker, monkeypatch, caplog, bounded, reason, epoch, expected
    ):
        from zfsbackup.workers import EPOCH_UPGRADE_DRAIN

        stuck = patch_stuck(monkeypatch, Stuck(10 ** 6, label="zfs receive"))
        remote_worker.reload_epoch.set(EPOCH_UPGRADE_DRAIN if epoch == "upgrade" else epoch)
        started = time.monotonic()
        assert finish(remote_worker, reason, caplog) == expected
        assert 0.25 <= time.monotonic() - started < 5.0
        warnings = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
        assert len(warnings) == 1
        assert "pid 424242 (zfs receive)" in warnings[0] and "gave up" in warnings[0]
        assert "successor may race" in warnings[0]
        assert STUCK_END not in caplog.text
        assert ("exiting to switch configuration" in caplog.text) is (expected == WORKER_EXIT_RELOAD)

    def test_a_gone_supervisor_does_not_end_the_wait(self, remote_worker, monkeypatch, caplog, mocker):
        """Under forkserver a dead forkserver makes the watch say 'gone' while the supervisor lives."""
        mocker.patch.object(remote_worker, "_supervisor_gone", return_value=True)
        stuck = patch_stuck(monkeypatch, Stuck(8, label="zfs receive"))
        assert remote_worker._exit_reason() is ExitReason.SUPERVISOR_GONE
        remote_worker._wait_for_stuck_writes(logging.getLogger("test.remote"), "reloading")
        assert stuck.calls >= 9
        assert STUCK_ABORT not in caplog.text

    def test_a_gone_supervisor_with_a_writer_still_waits_in_finish(self, remote_worker, monkeypatch, caplog, mocker):
        mocker.patch.object(remote_worker, "_supervisor_gone", return_value=True)
        stuck = patch_stuck(monkeypatch, Stuck(6, label="zfs receive"))
        remote_worker.reload_epoch.set(1)
        assert finish(remote_worker, ExitReason.RELOAD, caplog) == WORKER_EXIT_RELOAD
        assert stuck.calls >= 7 and STUCK_END in caplog.text and STUCK_ABORT not in caplog.text

    def test_a_stop_ends_the_wait_at_once_and_the_worker_stops_as_a_stop(
        self, remote_worker, monkeypatch, caplog, bounded
    ):
        def stop_on_third(call):
            if call == 3:
                remote_worker.stop_event.set()

        stuck = patch_stuck(monkeypatch, Stuck(10 ** 6, then=stop_on_third, label="zfs receive"))
        remote_worker.reload_epoch.set(1)
        started = time.monotonic()
        assert finish(remote_worker, ExitReason.RELOAD, caplog) is None
        assert time.monotonic() - started < 2.0
        assert stuck.calls == 3
        assert sum(STUCK_ABORT in r.getMessage() for r in caplog.records) == 1
        assert "gave up" not in caplog.text and "exiting to switch configuration" not in caplog.text
        assert " stopped" in caplog.text

    def test_a_failing_registry_read_means_no_wait(self, remote_worker, monkeypatch, caplog):
        def broken():
            raise RuntimeError("registry gone")

        patch_stuck(monkeypatch, broken)
        remote_worker.reload_epoch.set(1)
        assert finish(remote_worker, ExitReason.RELOAD, caplog) == WORKER_EXIT_RELOAD
        assert STUCK_START not in caplog.text

    def test_the_bound_is_ten_minutes_by_default(self):
        assert workers_module.REMOTE_STUCK_WRITE_WAIT == 600

    @pytest.mark.parametrize("reason", [ExitReason.STOP, ExitReason.SUPERVISOR_GONE])
    def test_a_stop_or_a_gone_supervisor_exit_never_waits(self, remote_worker, monkeypatch, caplog, reason):
        stuck = patch_stuck(monkeypatch, Stuck(3, label="zfs receive"))
        assert finish(remote_worker, reason, caplog) is None
        assert stuck.calls == 0
