"""Shared helpers for the `zfsbackup-config` test files that build a real
database through the CLI itself (`list`, dot-path `get`/`set`/`unset`, `edit`).

`tests/test_zfsbackup_cli.py` predates this module and keeps its own copies;
nothing here is imported by it. Fixtures are re-exported by the test files that
use them (``from _cli_support import isolated, db, run``) -- pytest finds a
fixture by its name in the module namespace.

Every database is created by `import` (so the schema, WAL mode, permissions
and the generation counter are the real ones) and read back through the
store's public API, never through an ORM model.
"""

import hashlib
import os
from pathlib import Path
from typing import List

import pytest
from click.testing import CliRunner

from zfsbackup.cli.main import cli
from zfsbackup.config import BackupConfig
from zfsbackup.config.store import (
    CONFIG_PATH_ENV,
    dispose_all,
    read_config,
    resolve_config_path,
)
from zfsbackup.config.store import paths as store_paths

#: A configuration that exercises every addressable path: dotted dataset names
#: (`tank/vm.01`, `a.retention`), a dotted destination (`dc.two`), a dataset
#: that inherits (`offsite`) and one that overrides (`dc.two`) frequency and
#: retention, a never-prune dataset, a disabled dataset, an unused destination.
#: `a` / `a.retention` make `datasets.a.retention` genuinely ambiguous.
RICH_YAML = """\
snapshot_prefix: "autosnap"
check_interval: "5m"
datasets:
  - name: "tank/data"
    frequency: "1h"
    recursive: true
    retention:
      "1d": "30d"
      "1w": "3M"
    remote:
      - destination: "offsite"
      - destination: "dc.two"
        frequency: "4h"
        retention:
          "1d": "7d"
  - name: "tank/vm.01"
    enabled: false
    frequency: "15m"
    retention: forever
  - name: "a"
  - name: "a.retention"
destinations:
  offsite:
    url: "https://offsite:8080"
  dc.two:
    url: "https://dc2:8080"
  spare:
    url: "https://spare:8080"
"""

#: Dotted names that make destination paths ambiguous too.
AMBIGUOUS_YAML = """\
datasets:
  - name: "a"
    remote:
      - destination: "dc"
      - destination: "dc.retention"
  - name: "a.retention"
destinations:
  dc:
    url: "https://dc:1"
  dc.retention:
    url: "https://dcr:1"
"""

#: A plain configuration with nothing that warns.
PLAIN_YAML = """\
snapshot_prefix: "autosnap"
check_interval: "5m"
datasets:
  - name: "tank/a"
    frequency: "1h"
    retention:
      "1d": "30d"
"""

#: Every semantic warning at once: a never-prune dataset and api_port 0. (A `_`
#: in the prefix used to be a third; it no longer warns -- the snapshot-name
#: parser accepts it -- but "my_snap" stays so the prefix-CHANGE warning has an
#: old value to name.)
WARNY_YAML = """\
snapshot_prefix: "my_snap"
check_interval: "5m"
api_port: 0
datasets:
  - name: "tank/forever"
    frequency: "1h"
    retention: forever
  - name: "tank/ok"
    frequency: "1h"
"""

NEVER_PRUNE_TEXT = "retention is empty ('retention: forever'): this dataset's snapshots will never be pruned and will accumulate until the pool is full"
PORT_TEXT = "api_port is 0: the API will bind an ephemeral port that changes on every start"
NO_AUTH_TEXT = "that API has no authentication"


@pytest.fixture(autouse=True)
def isolated(tmp_path, monkeypatch):
    """No test may see `$ZFSBACKUP_CONFIG` or touch the default database path;
    the working directory is `tmp_path`."""
    monkeypatch.delenv(CONFIG_PATH_ENV, raising=False)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(store_paths, "DEFAULT_CONFIG_DB", tmp_path / "default" / "config.db")
    yield
    dispose_all()


@pytest.fixture
def db(tmp_path) -> Path:
    return tmp_path / "cfg" / "config.db"


class Cli:
    """`CliRunner` with the two checks every call wants: nothing but
    `SystemExit` may escape a command, and stdout / stderr are kept apart."""

    def __init__(self) -> None:
        self.runner = CliRunner()

    def __call__(self, *args, env=None, input=None):
        result = self.runner.invoke(
            cli, [str(a) for a in args], env=env, input=input, prog_name="zfsbackup-config"
        )
        assert result.exception is None or isinstance(result.exception, SystemExit), (
            f"an exception escaped the CLI: {result.exception!r}\n{result.stderr}"
        )
        return result


@pytest.fixture
def run() -> Cli:
    return Cli()


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def db_state(path: Path) -> tuple:
    """Everything a refusal must leave alone: the file's sha256 and which
    sidecar files exist beside it."""
    sidecars = tuple(sorted(
        p.name for suffix in ("-wal", "-shm", "-journal")
        if (p := path.with_name(path.name + suffix)).exists()
    ))
    return digest(path), sidecars


def generation(path: Path) -> int:
    try:
        return read_config(resolve_config_path(str(path))).generation
    finally:
        dispose_all()


def stored(path: Path) -> BackupConfig:
    try:
        return read_config(resolve_config_path(str(path))).config
    finally:
        dispose_all()


def seed_client_id(database: Path, client_id: str = "host-1") -> str:
    """Insert (or replace) the `host_identity` row of an existing database,
    through the ORM model and the store's own engine factory (so foreign keys
    and the CHECK constraints are the real ones). The row is the client id the
    CLI reads into `ConfigSnapshot.client_id`; `import` never writes it. A direct
    insert on purpose: it does not bump the generation, so a test's
    compare-and-swap expectations are unaffected."""
    from zfsbackup.config.store import db as store_db
    from zfsbackup.config.store import models

    url = store_db.url_for_path(database)
    try:
        with store_db.session_scope(url) as session:
            session.query(models.HostIdentity).delete()
            session.add(models.HostIdentity(id=1, client_id=client_id, created_at=1.0))
    finally:
        dispose_all()
    return client_id


def host_client_id(database: Path):
    """The stored `host_identity.client_id`, or `None` when there is no row."""
    from zfsbackup.config.store import db as store_db
    from zfsbackup.config.store import models

    url = store_db.url_for_path(database)
    try:
        with store_db.session_scope(url, readonly=True) as session:
            row = session.query(models.HostIdentity).one_or_none()
            return None if row is None else row.client_id
    finally:
        dispose_all()


def write_yaml(directory: Path, text: str, name: str = "in.yaml") -> Path:
    path = directory / name
    path.write_text(text)
    return path


def make_db(run: Cli, database: Path, text: str = RICH_YAML, *, name: str = "in.yaml") -> Path:
    """Import `text` into a new database at `database`."""
    # `db` lives in tmp_path/cfg/, which the import creates: write the source beside it.
    source = write_yaml(database.parent.parent, text, name)
    result = run("import", source, "-c", database)
    assert result.exit_code == 0, result.stderr
    return database


def stderr_lines(result) -> List[str]:
    return [line for line in result.stderr.splitlines() if line]


def warnings_in(result) -> List[str]:
    return [line for line in stderr_lines(result) if line.startswith("warning: ")]


def count_containing(lines: List[str], fragment: str) -> int:
    return sum(fragment in line for line in lines)


# --------------------------------------------------------------------------
# Scripted editors (`edit`): see tests/_editor_stub.py
# --------------------------------------------------------------------------

import json  # noqa: E402
import shlex  # noqa: E402
import sys  # noqa: E402
import tempfile  # noqa: E402

STUB = Path(__file__).resolve().parent / "_editor_stub.py"
STUB_COMMAND = shlex.join([sys.executable, str(STUB)])


class Stub:
    """A scripted `$EDITOR`. `plan(...)` takes one action list per invocation;
    `env(db)` is what to pass to the CLI so that it runs this stub."""

    def __init__(self, directory: Path, name: str = "plan") -> None:
        self.path = directory / f"{name}.json"

    def plan(self, *invocations) -> "Stub":
        for suffix in ("", ".count", ".log"):
            Path(str(self.path) + suffix).unlink(missing_ok=True)
        self.path.write_text(json.dumps([list(inv) for inv in invocations]))
        return self

    def env(self, db: Path = None, **extra) -> dict:
        env = {
            "EDITOR": STUB_COMMAND,
            "VISUAL": None,
            "ZFSBACKUP_STUB_PLAN": str(self.path),
        }
        if db is not None:
            env["ZFSBACKUP_STUB_DB"] = str(db)
        env.update(extra)
        return env

    @property
    def log(self) -> list:
        log = Path(str(self.path) + ".log")
        if not log.exists():
            return []
        return [json.loads(line) for line in log.read_text().splitlines()]

    @property
    def runs(self) -> int:
        return len(self.log)


@pytest.fixture
def stub(tmp_path) -> Stub:
    return Stub(tmp_path)


@pytest.fixture
def scratch_tmp(tmp_path, monkeypatch) -> Path:
    """Confine `tempfile` to a directory of the test's own, so a leftover edit
    buffer is visible (and cannot leak into the real temp directory)."""
    directory = tmp_path / "tmp"
    directory.mkdir()
    monkeypatch.setattr(tempfile, "tempdir", str(directory))
    return directory


def buffers_in(directory: Path) -> list:
    return sorted(p for p in directory.iterdir() if p.name.startswith("zfsbackup-config-edit-"))


# --------------------------------------------------------------------------
# A fake `zfs` (see tests/_fake_zfs.py). The tests that reach the ZFS side of
# the CLI must use this: nothing here may ever run a real `zfs`.
# --------------------------------------------------------------------------

FAKE_ZFS = Path(__file__).resolve().parent / "_fake_zfs.py"


class FakeZfs:
    """The fake `zfs` for one test: `ZFS_CMD` points at a wrapper script that runs
    `_fake_zfs.py` against `state_path`. `add` creates a dataset with properties."""

    def __init__(self, directory: Path, monkeypatch=None) -> None:
        self._monkeypatch = monkeypatch
        self.dir = directory
        self.state_path = directory / "fakezfs-state.json"
        self.log_path = directory / "fakezfs-log.jsonl"
        self.state_path.write_text("{}")
        self.log_path.write_text("")
        self.wrapper = directory / "zfs-fake"
        self.wrapper.write_text(
            f"#!/bin/sh\nexec {shlex.quote(sys.executable)} {shlex.quote(str(FAKE_ZFS))} \"$@\"\n"
        )
        self.wrapper.chmod(0o755)
        self._fail: list = []

    def env(self) -> dict:
        return {
            "ZFS_CMD": str(self.wrapper),
            "ZPOOL_CMD": str(self.wrapper),
            "FAKEZFS_STATE": str(self.state_path),
            "FAKEZFS_LOG": str(self.log_path),
            "FAKEZFS_FAIL": json.dumps(self._fail),
        }

    def add(self, name: str, props: dict = None) -> "FakeZfs":
        state = self.state
        state[name] = dict(props or {})
        self._save(state)
        return self

    def fail(self, command: str, name: str, prop: str = None) -> "FakeZfs":
        self._fail.append([command, name] + ([prop] if prop else []))
        self._monkeypatch.setenv("FAKEZFS_FAIL", json.dumps(self._fail))
        return self

    @property
    def state(self) -> dict:
        return json.loads(self.state_path.read_text())

    def _save(self, state: dict) -> None:
        self.state_path.write_text(json.dumps(state))

    def props(self, name: str):
        return self.state.get(name)

    @property
    def calls(self) -> list:
        return [json.loads(line) for line in self.log_path.read_text().splitlines()]

    def calls_of(self, *commands: str) -> list:
        return [c for c in self.calls if c and c[0] in commands]

    @property
    def mutations(self) -> list:
        """Every call that changed (or tried to change) ZFS state."""
        return self.calls_of("set", "inherit")


@pytest.fixture
def fake_zfs(tmp_path, monkeypatch) -> FakeZfs:
    fake = FakeZfs(tmp_path, monkeypatch)
    for key, value in fake.env().items():
        monkeypatch.setenv(key, value)
    return fake


ANCHOR = "org.zfsbackup:anchor."
CONFIG_PROP = "org.zfsbackup:config"


# --------------------------------------------------------------------------
# A pretend daemon (live reload, items 19.1 / 19.2)
# --------------------------------------------------------------------------

import fcntl  # noqa: E402

from zfsbackup import daemonlock  # noqa: E402

_UNSET = object()


class FakeDaemon:
    """Holds the daemon lock for `db` IN THIS PROCESS and writes a chosen pid
    file, so `daemonlock.probe` / `request_reload` see "a daemon" without a
    subprocess.

    The flock is taken on a separate open file description, which conflicts
    with the CLI's own `LOCK_SH|LOCK_NB` exactly as a real holder's does. The
    recorded pid is whatever the test says (default 4321): nothing ever
    signals it, because every test that reaches `request_reload` for real
    either gets a refusal before the signal step (`applied` already at the
    key, `start=` absent or empty, an `unverified` platform) or patches
    `request_reload`.

    `start=None` writes the legacy two-line format; any string (including "")
    writes the v2 format. `raw` writes arbitrary bytes instead.
    """

    def __init__(self, db: Path, *, pid=4321, generation=None, start="tok",
                 applied=None, target=None, rejected=None, deferred=None,
                 deferred_reason=None, raw: bytes = None) -> None:
        db.parent.mkdir(parents=True, exist_ok=True)
        self.path = daemonlock.lock_path(db)
        self._fd = os.open(str(self.path), os.O_RDWR | os.O_CREAT, 0o660)
        fcntl.flock(self._fd, fcntl.LOCK_EX)
        self.set(pid=pid, generation=generation, start=start, applied=applied,
                 target=target, rejected=rejected, deferred=deferred,
                 deferred_reason=deferred_reason, raw=raw)

    def set(self, *, pid=_UNSET, generation=_UNSET, start=_UNSET, applied=_UNSET,
            target=_UNSET, rejected=_UNSET, deferred=_UNSET, deferred_reason=_UNSET,
            raw=None) -> None:
        """Rewrite the pid file. Omitted fields keep their previous value."""
        if raw is None:
            for name, value in (("pid", pid), ("generation", generation), ("start", start),
                                ("applied", applied), ("target", target),
                                ("rejected", rejected), ("deferred", deferred),
                                ("deferred_reason", deferred_reason)):
                if value is not _UNSET:
                    setattr(self, name, value)
            data = daemonlock._format(self.pid, self.generation, self.start,
                                      self.applied, self.target, self.rejected,
                                      self.deferred, self.deferred_reason)
        else:
            data = raw
        os.pwrite(self._fd, data, 0)
        os.ftruncate(self._fd, len(data))

    def close(self) -> None:
        fd, self._fd = self._fd, None
        if fd is not None:
            os.close(fd)  # drops the flock

    def __enter__(self) -> "FakeDaemon":
        return self

    def __exit__(self, *exc) -> None:
        self.close()


@pytest.fixture
def fake_daemon():
    """Factory: `fake_daemon(db, pid=..., applied=(store, gen), ...)`. Every
    daemon made is released at teardown."""
    made = []

    def start(db: Path, **kw) -> FakeDaemon:
        daemon = FakeDaemon(db, **kw)
        made.append(daemon)
        return daemon

    yield start
    for daemon in made:
        daemon.close()


@pytest.fixture
def live_daemon():
    """Like `fake_daemon`, but the recorded pid defaults to a LIVE process (this
    one), for tests of what the CLI reports about a supervisor that exists;
    `fake_daemon`'s default 4321 is, on most machines, a pid nobody has."""
    made = []

    def start(db: Path, **kw) -> FakeDaemon:
        from zfsbackup import procident

        kw.setdefault("pid", os.getpid())
        # This process's real start token: the status verdict checks the
        # supervisor's identity, not just that the pid exists.
        if kw["pid"] == os.getpid():
            kw.setdefault("start", procident.start_token(os.getpid()) or "tok")
        daemon = FakeDaemon(db, **kw)
        made.append(daemon)
        return daemon

    yield start
    for daemon in made:
        daemon.close()


def store_key(path: Path) -> tuple:
    """`(store_id, generation)` of the database, read through the store."""
    try:
        snap = read_config(resolve_config_path(str(path)))
        return snap.store_id, snap.generation
    finally:
        dispose_all()


@pytest.fixture
def unsignalable(monkeypatch):
    """Whatever the host, `request_reload` cannot prove that anything is the
    daemon (the lock table reads as unavailable, as on macOS), so it answers
    `unverified` and the CLI never signals the helper processes a test starts:
    on Linux a genuine lock holder IS signalled, and a SIGHUP kills a plain
    `python` helper."""
    monkeypatch.setattr(daemonlock.procident, "flock_write_holders", lambda fd: None)
    # ...and say WHY, as macOS does, so the wording is the platform one on
    # every host (on Linux the real reason would be "unknown").
    monkeypatch.setattr(
        daemonlock.procident, "flock_holders_unavailable_reason",
        lambda fd: daemonlock.procident.UNAVAILABLE_NOT_LINUX,
    )


UNVERIFIED_LINUX_ONLY = (
    "running daemon pid {pid} was not signalled (a daemon can be verified only on "
    "Linux); it picks up the change within 10s"
)
