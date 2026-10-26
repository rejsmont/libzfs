"""Shared helpers for tests of the server's receive path (milestone D, plan B-2a / B-2c).

Importable from any test module (`tests/` is on `sys.path`). Nothing here talks to ZFS.

* `make_header` -- a synthetic 312-byte `DRR_BEGIN` record (every field settable, either byte
  order), the thing `zfsbackup.api.read_stream_header` / `parse_stream_header` look at.
* `capture_header` / `capture_headers` -- the headers of the REAL `zfs send` streams captured on
  OpenZFS 2.1.5, 2.2.2 and 2.4.1 (`tests/fixtures/openzfs/<ver>/M8.txt`, records `M8/HDR.*`).
* `HEADER` / `VOLUME_HEADER` -- a full filesystem / volume header, what a stream harness sends first.
* `incremental_header` -- the header of an incremental stream from a given source guid.
* `get_value_answers` -- a `zfs.get.value` stand-in that answers by PROPERTY NAME (`type`, the local
  `org.zfsbackup:client_id`, `org.zfsbackup:config`, ...), because the server now asks for several
  properties of one dataset in one request.
"""

import itertools
import json
import struct
from typing import Callable, Dict, List, Optional

from _openzfs_fixtures import VERSIONS, load_section

DRR_RECORD_SIZE = 312
DMU_BACKUP_MAGIC = 0x2F5bacbac
_NAME_SIZE = 256

#: Object-set types (`dmu_objset_type_t`).
OST_NONE, OST_META, OST_ZFS, OST_ZVOL = 0, 1, 2, 3
_OBJSET = {'filesystem': OST_ZFS, 'volume': OST_ZVOL}

#: A plain substream: `DMU_SUBSTREAM` in the low two bits of `versioninfo`, plus the feature
#: bits a real `zfs send -L -c` stream sets (0x80011 | 0x800 ... only the low bits matter).
VERSIONINFO_PLAIN = 0x00000801 | 0x10
VERSIONINFO_COMPOUND = 2

DEFAULT_TO_GUID = 0x2525E778F7EC8955


def make_header(
    kind: str = 'filesystem',
    *,
    to_guid: int = DEFAULT_TO_GUID,
    from_guid: int = 0,
    toname: str = 'pool/data@autosnap_20240101120000',
    hdrtype: int = 1,
    drr_type: int = 0,
    objset: Optional[int] = None,
    magic: int = DMU_BACKUP_MAGIC,
    payloadlen: int = 0,
    versioninfo: Optional[int] = None,
    flags: int = 0,
    creation: int = 1_700_000_000,
    big_endian: bool = False,
    size: int = DRR_RECORD_SIZE,
) -> bytes:
    """A `dmu_replay_record` carrying a `drr_begin`, `size` bytes of it (default the whole 312;
    a smaller `size` cuts it short, a larger one pads with zeros -- stream payload).

    `kind` is `'filesystem'` or `'volume'` unless `objset` gives the raw object-set type;
    `hdrtype` is the low two bits of `drr_versioninfo`; `magic` is written in the chosen byte
    order, so `big_endian=True` makes the header a big-endian sender would write."""
    if objset is None:
        objset = _OBJSET[kind]
    if versioninfo is None:
        versioninfo = (VERSIONINFO_PLAIN & ~3) | hdrtype
    order = '>' if big_endian else '<'
    record = struct.pack(
        order + 'II' + 'QQQ' + 'II' + 'QQ' + f'{_NAME_SIZE}s',
        drr_type, payloadlen, magic, versioninfo, creation, objset, flags, to_guid, from_guid,
        toname.encode('utf-8'),
    )
    assert len(record) == DRR_RECORD_SIZE
    if size <= DRR_RECORD_SIZE:
        return record[:size]
    return record + b'\0' * (size - DRR_RECORD_SIZE)


#: A full filesystem stream's first record, and a volume's. A stream harness sends one of these
#: first (the server refuses a stream that does not start with a header).
HEADER = make_header('filesystem')
VOLUME_HEADER = make_header('volume')


def incremental_header(from_guid: int, kind: str = 'filesystem', **kwargs) -> bytes:
    """The header of an incremental stream whose source snapshot has guid `from_guid`."""
    return make_header(kind, from_guid=from_guid, **kwargs)


def _hex_line(record) -> str:
    """The hex line of an `M8/HDR.*` capture: the last stdout line (`bytes=312`,
    `payloadlen=N` and `inner_bytes=312` come before it)."""
    return record.stdout_lines[-1].strip()


def capture_header(version: str, name: str) -> bytes:
    """The 312 bytes of the real stream captured as `M8/HDR.<name>` on OpenZFS `version`."""
    raw = bytes.fromhex(_hex_line(load_section(version, 'M8')[f'M8/HDR.{name}']))
    assert len(raw) == DRR_RECORD_SIZE, (version, name, len(raw))
    return raw


def capture_names(version: str = '2.2.2') -> List[str]:
    """The `<name>` of every `M8/HDR.<name>` record of `version`."""
    return [rid.split('HDR.', 1)[1] for rid in load_section(version, 'M8') if rid.startswith('M8/HDR.')]


def capture_headers(name: str) -> Dict[str, bytes]:
    """`{version: header}` of one capture over every captured OpenZFS version."""
    return {v: capture_header(v, name) for v in VERSIONS}


def get_value_answers(
    values: Optional[Dict[str, object]] = None, *, default=None
) -> Callable:
    """A `zfs.get.value(ds, prop, sources=...)` side effect that answers by property NAME.

    `values` maps a property name to its value; a value that is an exception instance is
    raised, a callable is called with `(ds, prop, sources)`. A property not listed gets
    `default`. The `sources` argument is accepted and ignored: the tests that care about the
    source (local vs received) assert the call arguments."""
    values = dict(values or {})

    def answer(ds, prop, *args, sources=None, **kwargs):
        value = values.get(prop, default)
        if isinstance(value, BaseException):
            raise value
        if callable(value):
            return value(ds, prop, sources)
        return value

    answer.values = values
    return answer


# ---------------------------------------------------------------------------
# Websocket scripts
# ---------------------------------------------------------------------------

#: The text message that ends a stream.
DONE = json.dumps({'done': True})


def stream_script(*chunks, header: bytes = HEADER, done: bool = True) -> list:
    """The messages a client sends for one stream, as a `ws.receive.side_effect` list: the
    header record first (the server reads 312 bytes before it spawns anything), then `chunks`,
    then the `done` message (omit it with `done=False`)."""
    return [header, *chunks] + ([DONE] if done else [])


def endless_stream(chunk: bytes = b'chunk', header: bytes = HEADER):
    """A `ws.receive.side_effect` iterator: the header, then `chunk` for ever."""
    return itertools.chain([header], itertools.repeat(chunk))


# ---------------------------------------------------------------------------
# The properties the server forces onto what it receives (plan B-2a)
# ---------------------------------------------------------------------------

#: Pinned here, not imported: a change of the product's property set must fail the tests that
#: name it. Order matters (it is the order of `-o` in the argv).
FILESYSTEM_FORCED = {
    'canmount': 'noauto',
    'readonly': 'on',
    'volmode': 'none',
    'mountpoint': 'none',
    'sharenfs': 'off',
    'sharesmb': 'off',
    'setuid': 'off',
    'exec': 'off',
    'devices': 'off',
}
#: A volume refuses `-o canmount` and the other filesystem-only properties.
VOLUME_FORCED = {'readonly': 'on', 'volmode': 'none'}


def forced_for(kind: str) -> Dict[str, str]:
    return dict(VOLUME_FORCED if kind == 'volume' else FILESYSTEM_FORCED)


def expected_receive_argv(owned: Dict[str, str], kind: str, dataset: str, *, force: bool) -> List[str]:
    """The `zfs receive` argv (without the executable) the server must run for a stream of
    `kind`: the owned `-o` values, then the forced ones, then `[-F] -h -s -u <dataset>`."""
    argv = ['receive']
    for name, value in {**owned, **forced_for(kind)}.items():
        argv += ['-o', f'{name}={value}']
    if force:
        argv.append('-F')
    return argv + ['-h', '-s', '-u', dataset]
