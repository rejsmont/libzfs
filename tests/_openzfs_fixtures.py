"""Loader for the committed OpenZFS output captures (tests/fixtures/openzfs/<version>/<SECTION>.txt).

A section file starts with one `##### SECTION ...` header line, then holds records::

    === CASE <section>/<id>
    --- cmd
    <the command line, one line>
    --- rc
    <exit status>
    --- pipestatus          (pipelines only)
    --- stdout
    --- stderr
    --- send.stderr         (pipelines only)
    === END

`load_section` returns `{record id: Record}` where the id is the whole text after `=== CASE ` (it includes the
section: `M2/i_from_destroyed.8_nv_t`). Each marker's text is exactly the bytes the command printed (every
non-empty capture ends in a newline), so `stdout` and `stderr` are the real strings, to be fed to a mocked `Popen`.
The files are decoded as UTF-8 with `errors='surrogateescape'` (M9 has invalid UTF-8 on purpose), so a record's text
round-trips to its exact bytes with `text.encode('utf-8', 'surrogateescape')`.
"""
import os
from typing import Dict, List, Optional

FIXTURE_ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'fixtures', 'openzfs')
VERSIONS = ('2.1.5', '2.2.2', '2.3.9', '2.4.1')

_MARKERS = ('cmd', 'rc', 'pipestatus', 'stdout', 'stderr', 'send.stderr')


class Record(dict):
    """One captured command: `cmd`, `rc` (int), `stdout`, `stderr` always; `pipestatus` (list of int) and
    `send_stderr` only for pipelines (None otherwise; '' when the sending side printed nothing). Attribute access mirrors the keys."""

    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError:
            raise AttributeError(name) from None

    @property
    def stdout_lines(self) -> List[str]:
        """stdout split the way a text pipe yields it: one string per line, each keeping its newline."""
        return self['stdout'].splitlines(keepends=True)

    @property
    def stderr_lines(self) -> List[str]:
        return self['stderr'].splitlines(keepends=True)

    @property
    def token(self) -> Optional[str]:
        """The resume token a `zfs send ... -t <token>` command line carries (None for other commands)."""
        parts = self['cmd'].split()
        if '-t' in parts:
            i = len(parts) - 1 - parts[::-1].index('-t')
            if i + 1 < len(parts):
                return parts[i + 1]
        return None


def version_dir(version: str) -> str:
    return os.path.join(FIXTURE_ROOT, version)


def parse_section(text: str) -> Dict[str, Record]:
    records: Dict[str, Record] = {}
    lines = text.split('\n')
    i = 0
    while i < len(lines):
        line = lines[i]
        i += 1
        if not line.startswith('=== CASE '):
            continue
        rid = line[len('=== CASE '):]
        body: Dict[str, List[str]] = {}
        marker = None
        while i < len(lines) and lines[i] != '=== END':
            cur = lines[i]
            i += 1
            if cur.startswith('--- ') and cur[4:] in _MARKERS and cur[4:] not in body:
                marker = cur[4:]
                body[marker] = []
            elif marker is not None:
                body[marker].append(cur)
        i += 1  # === END

        def text_of(name):
            chunk = body.get(name)
            if chunk is None:
                return None
            return ''.join(c + '\n' for c in chunk)

        rec = Record(
            id=rid,
            cmd=(text_of('cmd') or '').rstrip('\n'),
            rc=int((text_of('rc') or '-1').strip() or -1),
            stdout=text_of('stdout') or '',
            stderr=text_of('stderr') or '',
            pipestatus=[int(x) for x in text_of('pipestatus').split()] if 'pipestatus' in body else None,
            # an empty send.stderr has no marker in the section file
            send_stderr=(text_of('send.stderr') or '') if 'pipestatus' in body else None,
        )
        records[rid] = rec
    return records


_CACHE: Dict[tuple, Dict[str, Record]] = {}


def load_section(version: str, section: str) -> Dict[str, Record]:
    """Records of `<version>/<section>.txt` (cached; treat as read-only)."""
    key = (version, section)
    if key not in _CACHE:
        # surrogateescape: M9 holds a value with a 0xff byte; encode('utf-8', 'surrogateescape') gives the exact bytes back
        with open(os.path.join(version_dir(version), section + '.txt'), encoding='utf-8', errors='surrogateescape',
                  newline='') as f:
            _CACHE[key] = parse_section(f.read())
    return _CACHE[key]


def load_m1_summary(version: str) -> List[dict]:
    """Rows of M1-SUMMARY.tsv: `case`, `kind` ('full'|'incr'), `send_rc`, `flags` (frozenset of `*ok` names),
    `resume_rc`, `token_after`."""
    rows = []
    with open(os.path.join(version_dir(version), 'M1-SUMMARY.tsv'), encoding='utf-8') as f:
        for line in f:
            line = line.rstrip('\n')
            if not line or line.startswith('#'):
                continue
            case, kind, send_rc, flags, resume_rc, after = line.split('\t')
            rows.append({
                'case': case, 'kind': kind, 'send_rc': int(send_rc),
                'flags': frozenset(flags.split(',')) - {'-'},
                'resume_rc': int(resume_rc), 'token_after': after,
            })
    return rows
