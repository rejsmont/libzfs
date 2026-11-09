"""Doubles for the tests of `zfsbackup.local` (the local destination, plan item 3.1).

Importable from any test module (`tests/` is on `sys.path`). Nothing here reaches `zfs`: the
commands are a recording fake of the slice of `libzfseasy` that `local.py` and `receiver.py` use
(`FakeHost` / `FakeLib`), and the two streams are objects over real `os.pipe()`s with scripted
exit statuses (`FakeSend` / `FakeReceive`), so the real `_Pump` runs over them with no child.

* `FakeHost` -- a tiny ZFS host: datasets with their local properties, snapshots (`createtxg`,
  `guid`), a resume token, an encryption root. Every command it answers appends an event to
  `host.events` (`('exists', name)`, `('props', name)`, `('type', name)`, `('list-snaps', name,
  strict)`, `('token', name)`, `('abort', name)`, `('set', name, props)`, `('sources', name, sources)`, `('create', name,
  props)`, `('start-receive', ...)`), in the order it was asked, so a test can pin the order of
  checks. `host.fail_on(kind, exc, name=None)` makes a kind of command raise.
* `FakeLib` -- the `libzfseasy` stand-in a test installs as `zfsbackup.local.zfs`; `timeout` and
  `stuck_children` are the real ones except that `host.stuck` is what `stuck_children()` lists.
* `FakeSend` / `FakeReceive` -- the `StreamHandle`s of `zfs send` / `zfs receive`.

`FakeReceive(on_commit=fn)` calls `fn()` when a successful receive is closed (a test adds the
received snapshot to the host's copy there).
"""

import os
import signal
import types
from typing import Dict, List, Optional

import libzfseasy
from libzfseasy.types import Filesystem
from libzfseasy.zfs import PropertySource

from _snapshot_support import NO_TXG, listed_snapshot
from zfsbackup.backup_manager import PROP_CLIENT_ID, PROP_CONFIG, PROP_SOURCE_DATASET

TARGET = 'bk/backups'
CLIENT = 'host1'
DEST = 'here'
DS = 'pool/data'
COPY = f'{TARGET}/{CLIENT}/{DS}'
BASE = f'{TARGET}/{CLIENT}'
PARENT = f'{TARGET}/{CLIENT}/pool'


class ServerRow:
    """A snapshot of a copy: `(name, txg, guid)`; `txg=None` leaves `createtxg` out."""

    def __init__(self, name, txg, guid):
        self.name, self.txg, self.guid = name, txg, guid


class Ds:
    def __init__(self, type='filesystem', props=None, snaps=(), token=None,
                 encryption='off', encryptionroot=None, abort_destroys=False):
        self.type = type
        self.props = dict(props or {})
        self.snaps: List[ServerRow] = list(snaps)
        self.token = token
        self.encryption = encryption
        self.encryptionroot = encryptionroot
        self.abort_destroys = abort_destroys


def owned_props(client=CLIENT, source=DS, config='x'):
    """The local `org.zfsbackup:*` properties of a copy this host made."""
    props = {PROP_CLIENT_ID: client, PROP_SOURCE_DATASET: source}
    if config is not None:
        props[PROP_CONFIG] = config
    return props


class FakeHost:
    def __init__(self):
        self.datasets: Dict[str, Ds] = {}
        self.events: List[tuple] = []
        self.stuck: list = []
        #: `kind -> [(name or None, exception)]`
        self._fail: Dict[str, list] = {}
        #: names whose strict snapshot listing raises (the lenient one answers)
        self.untrusted = set()
        #: what `receive.filesystem` hands out, in order; `start_error` is raised instead when set
        self.receives: list = []
        self.start_error: Optional[BaseException] = None
        self.start_calls: List[dict] = []

    # -- building -----------------------------------------------------------------------------
    def add(self, name, **kw) -> Ds:
        self.datasets[name] = ds = Ds(**kw)
        return ds

    def add_copy(self, snaps=(), props=None, **kw) -> Ds:
        """The copy of `pool/data` and the containers above it, owned by this host."""
        for name in (BASE, PARENT):
            if name not in self.datasets:
                self.add(name, props={PROP_CLIENT_ID: CLIENT})
        return self.add(COPY, props=props if props is not None else owned_props(),
                        snaps=[ServerRow(*s) for s in snaps], **kw)

    def fail_on(self, kind, exc, name=None):
        self._fail.setdefault(kind, []).append((name, exc))

    def kinds(self, *only) -> List[str]:
        return [e[0] for e in self.events if not only or e[0] in only]

    def of(self, kind) -> List[tuple]:
        return [e for e in self.events if e[0] == kind]

    # -- answering ----------------------------------------------------------------------------
    def _note(self, kind, name, *rest):
        self.events.append((kind, name) + rest)
        for fname, exc in self._fail.get(kind, ()):
            if fname is None or fname == name:
                raise exc

    def _ds(self, name) -> Ds:
        if name not in self.datasets:
            raise Exception(f"cannot open '{name}': dataset does not exist")
        return self.datasets[name]

    def read_properties(self, ds, also=()):
        """`backup_manager.read_zfsbackup_properties`: the dataset's `org.zfsbackup:*` values."""
        name = str(ds)
        self._note('props', name)
        return {k: v for k, v in self._ds(name).props.items() if k.startswith('org.zfsbackup:')}


class FakeLib:
    """The `libzfseasy` slice `local.py` and `receiver.py` call (see the module docstring)."""

    def __init__(self, host: FakeHost):
        self.host = host
        self.get = types.SimpleNamespace(value=self._value, property_sources=self._property_sources)
        self.receive = types.SimpleNamespace(abort=self._abort, filesystem=self._receive)
        self.timeout = libzfseasy.timeout

    def exists(self, ds):
        name = str(ds)
        self.host._note('exists', name)
        return name in self.host.datasets

    def stuck_children(self):
        return list(self.host.stuck)

    def set(self, ds, props):
        name = str(ds)
        self.host._note('set', name, dict(props))
        self.host._ds(name).props.update(props)

    def create(self, ds, properties=None, mount=True, **kw):
        name = str(ds)
        self.host._note('create', name, dict(properties or {}), mount, dict(kw))
        if name in self.host.datasets:
            raise Exception(f"cannot create '{name}': dataset already exists")
        self.host.add(name, props=properties)

    def list(self, roots=None, types=None, properties=None, strict=False, **kw):
        name = str(roots)
        if types == 'snapshot' and properties == ['createtxg', 'guid']:
            self.host._note('list-snaps', name, strict)
            if strict and name in self.host.untrusted:
                raise Exception("cannot trust the snapshot listing")
            return [
                listed_snapshot(s.name, name, txg=NO_TXG if s.txg is None else s.txg,
                                guid=NO_TXG if s.guid is None else s.guid)
                for s in self.host._ds(name).snaps
            ]
        if types == 'snapshot':
            self.host._note('list-snaps-any', name)
            return [listed_snapshot(s.name, name) for s in self.host._ds(name).snaps]
        if properties == ['receive_resume_token']:
            self.host._note('token', name)
            token = self.host._ds(name).token
            return [{'receive_resume_token': token if token else None}]
        raise AssertionError(f"unexpected list({roots!r}, types={types!r}, properties={properties!r})")

    def _value(self, ds, prop, sources=None):
        name = str(ds)
        record = self.host._ds
        if prop == 'type':
            self.host._note('type', name)
            return record(name).type
        self.host._note('value', name, prop, sources)
        d = record(name)
        if prop == 'encryption':
            return d.encryption
        if prop == 'encryptionroot':
            return d.encryptionroot
        if prop == 'receive_resume_token':
            return d.token or None
        if prop in (PROP_CLIENT_ID, PROP_CONFIG, PROP_SOURCE_DATASET):
            return d.props.get(prop)
        return None

    def _property_sources(self, ds, types='all', sources='all', **kw):
        """`zfs get -H -o name,property,source -s local`: the dataset's own properties (every
        property of the fake is a local one). Event `('sources', name, sources)`."""
        name = str(ds)
        self.host._note('sources', name, sources)
        return [PropertySource(name, prop, 'local') for prop in self.host._ds(name).props]

    def _abort(self, ds):
        name = str(ds)
        self.host._note('abort', name)
        d = self.host._ds(name)
        d.token = None
        if d.abort_destroys:
            del self.host.datasets[name]

    def _receive(self, fs, props=None, force=False, holds=True, save=False, mount=True, **kw):
        name = str(fs)
        call = dict(name=name, props=dict(props or {}), force=force, holds=holds, save=save,
                    mount=mount, **kw)
        self.host.start_calls.append(call)
        self.host._note('start-receive', name, call)
        if self.host.start_error is not None:
            raise self.host.start_error
        return self.host.receives.pop(0)


# ---------------------------------------------------------------------------------------------
# The two streams
# ---------------------------------------------------------------------------------------------


def _killed(sig=signal.SIGTERM) -> str:
    return f"Command killed by signal {sig.name}"


class FakeSend:
    """The `StreamHandle` of `zfs send`: `data` is readable from a real pipe (what libzfseasy's
    start-up peek has buffered is `data` itself) and the pipe reaches EOF unless `keep_open`.

    `rc`/`stderr`: what `close()` reports (it raises `Exception(stderr)` for a non-zero status,
    as the real handle does). `running`: `poll()` is `None` until `terminate()`. `abandon`:
    `terminate()` reports a child that survived SIGKILL (and `close()` raises at once)."""

    def __init__(self, data=b'', *, rc=0, stderr='', running=False, keep_open=False,
                 abandon=False, pid=40001, events=None):
        assert len(data) <= 8192, "the data must fit a pipe buffer: nothing reads it concurrently"
        self.pid = pid
        self.rc, self.stderr = rc, stderr
        self.running, self.abandon = running, abandon
        self.events = events if events is not None else []
        self.terminated = False
        self.closes = 0
        self.terminates = 0
        r, w = os.pipe()
        if data:
            os.write(w, data)
        self._w = w if keep_open else None
        if not keep_open:
            os.close(w)
        self._stream = os.fdopen(r, 'rb')
        if data:
            self._stream.peek(1)

    def fileno(self):
        return self._stream.fileno()

    def read1(self, n=-1):
        self.events.append(('send-read',))
        return self._stream.read1(n)

    def poll(self):
        if self.terminated:
            return -signal.SIGTERM
        return None if self.running else self.rc

    def terminate(self):
        self.terminates += 1
        self.events.append(('send-terminate',))
        if self.poll() is not None:
            return False
        self.terminated = True
        return self.abandon

    def close(self):
        if self.closes:
            return
        self.closes += 1
        self.events.append(('send-close',))
        self._stream.close()
        if self._w is not None:
            os.close(self._w)
            self._w = None
        if self.abandon and self.terminated:
            raise Exception("zfs send was abandoned in uninterruptible sleep")
        rc = self.poll()
        if rc not in (0, None):
            raise Exception(self.stderr or (_killed() if rc < 0 else f"Command failed with exit code {rc}"))

    def write_more(self, data):
        os.write(self._w, data)

    def end(self):
        if self._w is not None:
            os.close(self._w)
            self._w = None


class FakeReceive:
    """The `StreamHandle` of `zfs receive`, a real pipe the test reads (`received()`).

    `dead_rc`: the child is already gone with that status (the read end is closed, so a write is
    `BrokenPipeError`). `rc`/`stderr`: what `close()` reports. `hang`: `poll()` stays `None` after
    EOF until `terminate()` (a receive that does not commit). `abandon` as in `FakeSend`."""

    def __init__(self, *, rc=0, stderr='', dead_rc=None, hang=False, abandon=False, pid=40002,
                 events=None, on_commit=None):
        self.pid = pid
        self.on_commit = on_commit
        self.rc, self.stderr, self.hang, self.abandon = rc, stderr, hang, abandon
        self.dead_rc = dead_rc
        self.events = events if events is not None else []
        self.terminated = False
        self.closes = 0
        self.terminates = 0
        r, w = os.pipe()
        self._r = r
        self._stream = os.fdopen(w, 'wb')
        if dead_rc is not None:
            os.close(r)
            self._r = None

    def __enter__(self):
        return self._stream

    def fileno(self):
        return self._stream.fileno()

    def poll(self):
        if self.terminated:
            return -signal.SIGTERM
        if self.dead_rc is not None:
            return self.dead_rc
        if self._stream.closed and not self.hang:
            return self.rc
        return None

    def terminate(self):
        self.terminates += 1
        self.events.append(('receive-terminate',))
        if self.poll() is not None:
            return False
        self.terminated = True
        return self.abandon

    def close(self):
        if self.closes:
            return
        self.closes += 1
        self.events.append(('receive-close',))
        try:
            self._stream.close()
        except OSError:
            pass
        if self.abandon and self.terminated:
            raise Exception("zfs receive was abandoned in uninterruptible sleep")
        rc = self.poll()
        if rc not in (0, None):
            raise Exception(self.stderr or (_killed() if rc < 0 else f"Command failed with exit code {rc}"))
        if rc == 0 and self.on_commit is not None:
            self.on_commit()

    def received(self) -> bytes:
        """Everything written so far (the write end is closed by the pump, or call after it)."""
        if self._r is None:
            return b''
        os.set_blocking(self._r, False)
        out = b''
        while True:
            try:
                chunk = os.read(self._r, 65536)
            except BlockingIOError:
                break
            if not chunk:
                break
            out += chunk
        return out

    def release(self):
        for fd in (self._r,):
            if fd is not None:
                try:
                    os.close(fd)
                except OSError:
                    pass
        self._r = None
        try:
            self._stream.close()
        except OSError:
            pass
