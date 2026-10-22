"""Shared helpers for tests that need snapshots as `zfs list` reports them (plan S-1).

Since S-1 every managed snapshot comes from one listing that carries `createtxg`, `guid`,
`userrefs`, `clones`, `encryption` and `keystatus` (`backup_manager.SNAPSHOT_LIST_PROPERTIES`), and
a managed snapshot without a `createtxg` is dropped. A fixture that builds a bare
`libzfseasy.Snapshot(fs, name)` therefore stands for a snapshot the code ignores; these helpers
build the realistic thing. Importable from any test module (`tests/` is on `sys.path`). Nothing
here talks to ZFS.

* `default_txg(name)` -- a txg that orders like the name's timestamp (what a normal pool does:
  names and creation order agree). Tests of a disagreement pass `txg=` explicitly.
* `listed_snapshot` / `listed_info` -- a libzfseasy `Snapshot` / a `SnapshotInfo` carrying the
  listing's properties. `txg=NO_TXG` leaves `createtxg` out, `txg=` an int pins it.
* `list_row` -- the tab-separated `zfs list -H` line (`name, type` plus the six columns) for
  `mock_subprocess`.
"""

import re
from typing import Optional

from libzfseasy.types import Filesystem, Snapshot as ZFSSnapshot
from zfsbackup.backup_manager import SNAPSHOT_LIST_PROPERTIES, SnapshotInfo

#: Pass as `txg=` to leave `createtxg` out of the listing (a snapshot that cannot be ordered).
NO_TXG = object()
#: The same marker for any other column (`guid=ABSENT`).
ABSENT = NO_TXG
#: The default: derive the value from the name.
AUTO = object()


def default_txg(name: str) -> int:
    """A txg that sorts like the 14-digit timestamp in `name` (any name, any prefix); names with
    no 14-digit run sort first."""
    m = re.search(r'([0-9]{14})', name)
    return int(m.group(1)) if m else 1


def default_guid(name: str) -> int:
    """A stable, distinct guid per snapshot name."""
    return 0x1000000000000000 + (hash_name(name) & 0xffffffffffff)


def hash_name(name: str) -> int:
    value = 1469598103934665603
    for ch in name:
        value = ((value ^ ord(ch)) * 1099511628211) & 0xffffffffffffffff
    return value


def _props(name, txg, guid, userrefs, clones, encryption, keystatus) -> dict:
    values = {
        'createtxg': default_txg(name) if txg is AUTO else txg,
        'guid': default_guid(name) if guid is AUTO else guid,
        'userrefs': userrefs,
        'clones': clones,
        'encryption': encryption,
        'keystatus': keystatus,
    }
    return {
        k: str(v) for k, v in values.items()
        if v is not None and v is not NO_TXG
    }


def listed_snapshot(name, dataset='pool/data', *, txg=AUTO, guid=AUTO, userrefs='0',
                    clones='-', encryption='off', keystatus='-') -> ZFSSnapshot:
    """A libzfseasy snapshot with the properties the manager's listing requests."""
    return ZFSSnapshot(
        Filesystem(dataset), name,
        _props(name, txg, guid, userrefs, clones, encryption, keystatus),
    )


def listed_info(name, prefix='autosnap', dataset='pool/data', **kw) -> SnapshotInfo:
    """A `SnapshotInfo` over `listed_snapshot(name, dataset, **kw)`."""
    return SnapshotInfo(listed_snapshot(name, dataset, **kw), prefix)


def list_row(name, dataset='pool/data', *, txg=AUTO, guid=AUTO, userrefs='0', clones='-',
             encryption='off', keystatus='-') -> str:
    """The `zfs list -H -o name,type,<SNAPSHOT_LIST_PROPERTIES>` line for one snapshot."""
    props = _props(name, txg, guid, userrefs, clones, encryption, keystatus)
    cells = [props.get(p, '-') for p in SNAPSHOT_LIST_PROPERTIES]
    return '\t'.join([f'{dataset}@{name}', 'snapshot'] + cells) + '\n'


# -- a frozen clock for `zfsbackup.backup_manager` ---------------------------------------------------

from datetime import datetime, timedelta, timezone  # noqa: E402

import pytest  # noqa: E402

#: Where the frozen clock starts: aligned to the minute, mid-hour, so an hourly generated name is
#: `<prefix>_20261022100000`.
CLOCK_START = datetime(2026, 10, 22, 10, 30, 0, tzinfo=timezone.utc)


class FrozenClock:
    """Pins `datetime.now()` as `zfsbackup.backup_manager` sees it (snapshot age, the aligned
    reference time of a generated name, the clock-tolerance checks, the retention walk).

    `set(dt)` / `advance(td)` move it. It substitutes a `datetime` subclass in that module only;
    names parsed from snapshots are instances of the subclass, so they compare with everything."""

    def __init__(self, mocker, start=CLOCK_START):
        self.now = start
        outer = self

        class _Frozen(datetime):
            @classmethod
            def now(cls, tz=None):
                return outer.now.astimezone(tz) if tz is not None else outer.now.replace(tzinfo=None)

        mocker.patch('zfsbackup.backup_manager.datetime', _Frozen)

    def set(self, when: datetime) -> None:
        self.now = when

    def advance(self, delta: timedelta) -> None:
        self.now = self.now + delta

    def stamp(self, offset: timedelta = timedelta(0), prefix: str = 'autosnap') -> str:
        """A managed snapshot name dated `now + offset`."""
        return f"{prefix}_{(self.now + offset).strftime('%Y%m%d%H%M%S')}"


@pytest.fixture
def frozen_clock(mocker):
    return FrozenClock(mocker)
