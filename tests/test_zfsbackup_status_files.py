"""S-7a: the worker status files -- `zfsbackup.health`'s status section.

`<db>.status/<worker>.json`, one file per worker, written only by that worker
(`StatusWriter`) and read by `zfsbackup-config check` / `status`
(`read_status_dir`, `read_worker_status`). This file covers the module on its
own, with real files under `tmp_path` and no daemon, no worker process and no
`zfs`:

* atomic replacement (no partial file ever visible, no leftover temporary after
  a failure at write, fsync or rename, the old file intact);
* modes and groups (0640 files and a 0750 directory whatever the umask, the DB's
  group, `ensure_status_dir`'s refusals);
* the tolerant reader (every kind of bad directory, file and record);
* the writer's limits (the write interval, the record cap, `retain`, the 2 MiB
  shrink loop, the failure streak's WARNING/INFO, carrying counters over).

The workers' use of it is `test_zfsbackup_status_workers.py`; the resume-token
ERROR is `test_zfsbackup_remote_token_error.py`.
"""

import errno
import functools
import json
import logging
import os
import shutil
import stat
import threading
import time
from types import SimpleNamespace

import pytest

from zfsbackup import health
from zfsbackup.health import (
    KIND_PRUNE,
    KIND_SEND,
    KIND_SNAPSHOT,
    RESULT_ERROR,
    RESULT_OK,
    STATUS_MAX_BYTES,
    STATUS_MAX_RECORDS,
    STATUS_SCHEMA,
    STATUS_TEXT_LIMIT,
    StatusError,
    StatusWriter,
    ensure_status_dir,
    read_status_dir,
    read_worker_status,
    write_status_file,
)

W = 'snapshot-worker'
ME = os.geteuid()


def doc(worker=W, **over):
    """A valid status document (as a dict)."""
    base = {
        'schema': STATUS_SCHEMA, 'worker': worker, 'pid': 4242,
        'started_at': '2026-10-25T10:00:00Z', 'updated_at': '2026-10-25T10:05:12Z',
        'records': [{
            'kind': 'snapshot', 'dataset': 'pool/data', 'result': 'ok',
            'last_attempt': '2026-10-25T10:05:12Z', 'last_success': '2026-10-25T10:05:12Z',
            'consecutive_failures': 0,
        }],
    }
    base.update(over)
    return base


def put(directory, name, content, mode=0o640):
    """Plant `content` (dict -> JSON, str/bytes as is) at `directory/name`."""
    path = directory / name
    if isinstance(content, dict):
        content = json.dumps(content)
    if isinstance(content, str):
        content = content.encode()
    path.write_bytes(content)
    os.chmod(path, mode)
    return path


@pytest.fixture
def sdir(tmp_path):
    """A real status directory next to a (fake) DB file."""
    (tmp_path / 'config.db').write_bytes(b'')
    directory = tmp_path / 'config.db.status'
    ensure_status_dir(directory)
    return directory


@pytest.fixture
def umask():
    """`umask(value)` sets the process umask for the test; restored afterwards."""
    saved = os.umask(0o022)
    os.umask(saved)

    def setter(value):
        os.umask(value)

    yield setter
    os.umask(saved)


def temporaries(directory):
    return sorted(p.name for p in directory.iterdir() if p.name.endswith('.tmp'))


def warnings(caplog, text=''):
    return [r for r in caplog.records
            if r.name == 'zfsbackup.health' and r.levelno == logging.WARNING and text in r.getMessage()]


class _OtherGid:
    """A real `os.stat_result` that claims to belong to group `gid`."""

    def __init__(self, real, gid):
        self._real, self.st_gid = real, gid

    def __getattr__(self, name):
        return getattr(self._real, name)


def other_group():
    """A group this process may chgrp to that is not its egid, or `None`."""
    for gid in os.getgroups():
        if gid != os.getegid():
            return gid
    return None


# ---------------------------------------------------------------------------
# Names and small helpers
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestPathsAndText:
    def test_the_directory_is_the_db_path_plus_dot_status(self, tmp_path):
        assert health.status_dir(tmp_path / 'config.db') == tmp_path / 'config.db.status'
        assert health.status_dir(str(tmp_path / 'x.sqlite')).name == 'x.sqlite.status'

    def test_the_file_is_the_worker_name_dot_json(self, tmp_path):
        assert health.status_path(tmp_path, W) == tmp_path / 'snapshot-worker.json'

    @pytest.mark.parametrize('bad', ['', '../x', 'A', 'a/b', '1a', 'a' * 41, 'a.json', 'a b', '-a'])
    def test_status_path_refuses_anything_but_a_worker_name(self, tmp_path, bad):
        with pytest.raises(ValueError):
            health.status_path(tmp_path, bad)

    @pytest.mark.parametrize('bad', ['../x', 'A', ''])
    def test_a_writer_refuses_a_bad_worker_name(self, tmp_path, bad):
        with pytest.raises(ValueError):
            StatusWriter(tmp_path, bad)

    def test_clip_text_flattens_control_characters_and_whitespace(self):
        assert health.clip_text('a\nb\tc\x00d  e\r\nf') == 'a b c d e f'

    def test_clip_text_cuts_at_the_limit_with_an_ellipsis(self):
        out = health.clip_text('x' * 1000)
        assert len(out) == STATUS_TEXT_LIMIT == 300
        assert out.endswith('...') and out[:-3] == 'x' * 297
        assert health.clip_text('x' * 300) == 'x' * 300          # exactly at the limit: untouched
        assert health.clip_text('abcdefghij', limit=6) == 'abc...'

    def test_clip_text_accepts_any_object(self):
        assert health.clip_text(ValueError('boom')) == 'boom'
        assert health.clip_text(None) == 'None'


# ---------------------------------------------------------------------------
# 1. Atomicity
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestAtomicWrite:
    def test_a_write_leaves_the_file_and_no_temporary(self, sdir):
        write_status_file(sdir, W, '{"a":1}\n')
        assert (sdir / 'snapshot-worker.json').read_text() == '{"a":1}\n'
        assert [p.name for p in sdir.iterdir()] == ['snapshot-worker.json']

    def test_a_second_write_replaces_the_first_wholly(self, sdir):
        write_status_file(sdir, W, 'x' * 5000)
        write_status_file(sdir, W, 'short')
        assert (sdir / 'snapshot-worker.json').read_text() == 'short'
        assert temporaries(sdir) == []

    @pytest.mark.parametrize('step', ['write', 'fsync', 'rename'])
    def test_a_failure_leaves_the_old_file_intact_and_no_temporary(self, sdir, monkeypatch, step):
        write_status_file(sdir, W, 'OLD')

        def boom(*args, **kwargs):
            raise OSError(errno.EIO, 'injected')

        if step == 'write':
            monkeypatch.setattr(health, '_write_all', boom)
        elif step == 'fsync':
            monkeypatch.setattr(health.os, 'fsync', boom)
        else:
            monkeypatch.setattr(health.os, 'rename', boom)
        with pytest.raises(OSError):
            write_status_file(sdir, W, 'NEW' * 1000)
        monkeypatch.undo()
        assert (sdir / 'snapshot-worker.json').read_text() == 'OLD'
        assert temporaries(sdir) == []

    @pytest.mark.parametrize('step', ['write', 'fsync', 'rename'])
    def test_a_failure_of_the_first_write_leaves_the_directory_empty(self, sdir, monkeypatch, step):
        def boom(*args, **kwargs):
            raise OSError(errno.ENOSPC, 'injected')

        target = {'write': (health, '_write_all'), 'fsync': (health.os, 'fsync'),
                  'rename': (health.os, 'rename')}[step]
        monkeypatch.setattr(*target, boom)
        with pytest.raises(OSError):
            write_status_file(sdir, W, 'NEW')
        monkeypatch.undo()
        assert list(sdir.iterdir()) == []

    def test_the_directory_is_synced_after_the_rename(self, sdir, monkeypatch):
        calls = []
        real_rename, real_fsync = os.rename, os.fsync
        monkeypatch.setattr(health.os, 'rename', lambda *a, **k: (calls.append('rename'), real_rename(*a, **k))[1])
        monkeypatch.setattr(health.os, 'fsync', lambda fd: (calls.append('fsync'), real_fsync(fd))[1])
        write_status_file(sdir, W, 'x')
        assert calls == ['fsync', 'rename', 'fsync']      # file, rename, directory

    def test_the_temporary_is_exclusive_and_never_followed_through_a_symlink(self, sdir, tmp_path, monkeypatch):
        """The temporary name carries the pid and a clock reading; an attacker who guessed
        it could plant a symlink. `O_EXCL | O_NOFOLLOW` refuses it, the writer picks
        another name and the victim file is untouched."""
        victim = tmp_path / 'victim'
        victim.write_text('precious')
        planted = sdir / f'.snapshot-worker.json.{os.getpid()}.42.tmp'
        planted.symlink_to(victim)
        ticks = iter([42, 43, 44])
        monkeypatch.setattr(health.time, 'monotonic_ns', lambda: next(ticks))
        write_status_file(sdir, W, 'NEW')
        assert victim.read_text() == 'precious'
        assert (sdir / 'snapshot-worker.json').read_text() == 'NEW'
        assert planted.is_symlink()                       # not touched, not removed

    def test_four_taken_temporary_names_fail_the_write_instead_of_following_one(
        self, sdir, tmp_path, monkeypatch
    ):
        victim = tmp_path / 'victim'
        victim.write_text('precious')
        (sdir / f'.snapshot-worker.json.{os.getpid()}.7.tmp').symlink_to(victim)
        monkeypatch.setattr(health.time, 'monotonic_ns', lambda: 7)
        with pytest.raises(FileExistsError):
            write_status_file(sdir, W, 'NEW')
        assert victim.read_text() == 'precious'
        assert not (sdir / 'snapshot-worker.json').exists()

    def test_it_refuses_to_write_through_a_symlinked_directory(self, tmp_path):
        real = tmp_path / 'real'
        real.mkdir()
        link = tmp_path / 'config.db.status'
        link.symlink_to(real)
        with pytest.raises(StatusError):
            write_status_file(link, W, 'x')
        assert list(real.iterdir()) == []

    def test_a_concurrent_reader_never_sees_a_partial_document(self, sdir):
        """A writer alternates between two large, different documents while a reader
        parses the file as fast as it can: every read is one document or the other,
        never a truncation, never a mix, and never an error."""
        def big(tag, count):
            return json.dumps(doc(
                pid=count,
                records=[{'kind': 'snapshot', 'dataset': f'pool/{tag}{i}', 'result': 'ok',
                          'consecutive_failures': 0, 'detail': tag * 100} for i in range(count)],
            ))

        a, b = big('a', 1500), big('b', 700)
        write_status_file(sdir, W, a)
        stop = threading.Event()
        errors, seen = [], set()

        def writer():
            try:
                for i in range(120):
                    write_status_file(sdir, W, a if i % 2 else b)
            except Exception as e:                        # pragma: no cover - would fail the test
                errors.append(e)
            finally:
                stop.set()

        t = threading.Thread(target=writer)
        t.start()
        try:
            while not stop.is_set():
                try:
                    status = read_worker_status(sdir, W)
                except Exception as e:
                    errors.append(e)
                    break
                assert status is not None
                seen.add((status.pid, len(status.records)))
        finally:
            t.join(60)
        assert errors == []
        assert seen <= {(1500, 1500), (700, 700)} and seen, seen


# ---------------------------------------------------------------------------
# 2. Modes and groups
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestModes:
    @pytest.mark.parametrize('mask', [0o000, 0o022, 0o077, 0o277, 0o777])
    def test_the_file_is_0640_whatever_the_umask(self, sdir, umask, mask):
        umask(mask)
        write_status_file(sdir, W, 'x')
        assert stat.S_IMODE(os.stat(sdir / 'snapshot-worker.json').st_mode) == 0o640

    @pytest.mark.parametrize('mask', [0o000, 0o022, 0o077, 0o277])
    def test_a_new_directory_is_0750_whatever_the_umask(self, tmp_path, umask, mask):
        umask(mask)
        directory = tmp_path / 'config.db.status'
        ensure_status_dir(directory)
        assert stat.S_IMODE(os.stat(directory).st_mode) == 0o750

    def test_a_replaced_file_keeps_the_mode(self, sdir):
        write_status_file(sdir, W, 'x')
        os.chmod(sdir / 'snapshot-worker.json', 0o600)
        write_status_file(sdir, W, 'y')
        assert stat.S_IMODE(os.stat(sdir / 'snapshot-worker.json').st_mode) == 0o640

    def test_the_file_gets_the_directorys_group_when_it_differs(self, sdir, monkeypatch):
        """The directory's group is the DB's; a file created in a different group is
        chgrp'd to it (fchown to `(-1, dir gid)`)."""
        gid = os.stat(sdir).st_gid
        calls = []
        real_fstat = os.fstat

        def fake_fstat(fd):
            st = real_fstat(fd)
            return _OtherGid(st, gid + 1) if stat.S_ISREG(st.st_mode) else st

        monkeypatch.setattr(health.os, 'fstat', fake_fstat)
        monkeypatch.setattr(health.os, 'fchown', lambda fd, uid, g: calls.append((uid, g)))
        write_status_file(sdir, W, 'x')
        assert calls == [(-1, gid)]

    def test_a_file_in_the_right_group_is_not_chowned(self, sdir, monkeypatch):
        calls = []
        monkeypatch.setattr(health.os, 'fchown', lambda *a: calls.append(a))
        write_status_file(sdir, W, 'x')
        assert calls == []

    def test_a_refused_fchown_does_not_fail_the_write(self, sdir, monkeypatch):
        gid = os.stat(sdir).st_gid
        real_fstat = os.fstat
        monkeypatch.setattr(
            health.os, 'fstat',
            lambda fd: (_OtherGid(real_fstat(fd), gid + 1)
                        if stat.S_ISREG(real_fstat(fd).st_mode) else real_fstat(fd)),
        )

        def refuse(*args):
            raise PermissionError(errno.EPERM, 'not a member')

        monkeypatch.setattr(health.os, 'fchown', refuse)
        write_status_file(sdir, W, 'x')
        assert (sdir / 'snapshot-worker.json').read_text() == 'x'

    @pytest.mark.skipif(other_group() is None, reason='the user belongs to no second group')
    def test_a_real_file_ends_in_the_directorys_group(self, sdir):
        gid = other_group()
        os.chown(sdir, -1, gid)
        write_status_file(sdir, W, 'x')
        assert os.stat(sdir / 'snapshot-worker.json').st_gid == gid


@pytest.mark.unit
class TestEnsureStatusDir:
    def test_it_creates_the_directory_and_is_idempotent(self, tmp_path):
        directory = tmp_path / 'config.db.status'
        ensure_status_dir(directory)
        assert directory.is_dir() and not directory.is_symlink()
        (directory / 'keep').write_text('x')
        ensure_status_dir(directory)
        assert (directory / 'keep').read_text() == 'x'

    def test_it_needs_the_parent_to_exist(self, tmp_path):
        with pytest.raises(StatusError, match='does not exist'):
            ensure_status_dir(tmp_path / 'nope' / 'config.db.status')
        assert not (tmp_path / 'nope').exists()

    def test_a_symlink_is_refused_even_to_a_good_directory(self, tmp_path):
        real = tmp_path / 'real'
        real.mkdir(mode=0o750)
        link = tmp_path / 'config.db.status'
        link.symlink_to(real)
        with pytest.raises(StatusError, match='symlink'):
            ensure_status_dir(link)

    def test_a_dangling_symlink_is_refused(self, tmp_path):
        link = tmp_path / 'config.db.status'
        link.symlink_to(tmp_path / 'gone')
        with pytest.raises(StatusError):
            ensure_status_dir(link)
        assert not (tmp_path / 'gone').exists()

    def test_a_regular_file_is_refused(self, tmp_path):
        path = tmp_path / 'config.db.status'
        path.write_text('x')
        with pytest.raises(StatusError):
            ensure_status_dir(path)
        assert path.read_text() == 'x'

    @pytest.mark.parametrize('mode', [0o770, 0o775, 0o777, 0o720, 0o702, 0o722])
    def test_an_existing_directory_others_can_write_is_refused(self, tmp_path, mode):
        directory = tmp_path / 'config.db.status'
        directory.mkdir()
        os.chmod(directory, mode)
        with pytest.raises(StatusError, match='writable by group or others'):
            ensure_status_dir(directory)
        assert stat.S_IMODE(os.stat(directory).st_mode) == mode        # it does not "fix" it

    @pytest.mark.parametrize('mode', [0o750, 0o755, 0o700, 0o710])
    def test_a_directory_only_the_owner_can_write_is_accepted_as_it_is(self, tmp_path, mode):
        directory = tmp_path / 'config.db.status'
        directory.mkdir()
        os.chmod(directory, mode)
        ensure_status_dir(directory)
        assert stat.S_IMODE(os.stat(directory).st_mode) == mode

    def test_a_new_directory_is_given_the_db_group_by_fchown(self, tmp_path, monkeypatch):
        (tmp_path / 'config.db').write_bytes(b'')
        calls = []
        monkeypatch.setattr(health, '_db_group', lambda path: 31337)
        monkeypatch.setattr(health.os, 'fchown', lambda fd, uid, gid: calls.append((uid, gid)))
        ensure_status_dir(tmp_path / 'config.db.status')
        assert calls == [(-1, 31337)]

    def test_no_chown_when_the_directory_is_already_in_that_group(self, tmp_path, monkeypatch):
        calls = []
        monkeypatch.setattr(health, '_db_group', lambda path: os.getegid() if False else os.stat(tmp_path).st_gid)
        monkeypatch.setattr(health.os, 'fchown', lambda *a: calls.append(a))
        ensure_status_dir(tmp_path / 'config.db.status')
        assert calls == []

    def test_an_existing_directory_is_never_chowned(self, tmp_path, monkeypatch):
        directory = tmp_path / 'config.db.status'
        directory.mkdir(mode=0o750)
        calls = []
        monkeypatch.setattr(health, '_db_group', lambda path: 31337)
        monkeypatch.setattr(health.os, 'fchown', lambda *a: calls.append(a))
        ensure_status_dir(directory)
        assert calls == []

    def test_a_failed_chgrp_is_a_warning_not_an_error(self, tmp_path, monkeypatch, caplog):
        def refuse(*args):
            raise PermissionError(errno.EPERM, 'Operation not permitted')

        monkeypatch.setattr(health, '_db_group', lambda path: 31337)
        monkeypatch.setattr(health.os, 'fchown', refuse)
        with caplog.at_level(logging.WARNING, logger='zfsbackup.health'):
            ensure_status_dir(tmp_path / 'config.db.status')
        assert (tmp_path / 'config.db.status').is_dir()
        assert warnings(caplog, 'could not give')

    def test_the_db_group_is_only_used_when_this_process_may_chgrp_to_it(self, tmp_path, monkeypatch):
        db = tmp_path / 'config.db'
        db.write_bytes(b'')
        directory = tmp_path / 'config.db.status'
        monkeypatch.setattr(health.os, 'geteuid', lambda: 1234)
        monkeypatch.setattr(health.os, 'getegid', lambda: 55)
        monkeypatch.setattr(health.os, 'getgroups', lambda: [55, 66])
        monkeypatch.setattr(health.os, 'stat', lambda p, *a, **k: SimpleNamespace(st_gid=77, st_uid=1234))
        assert health._db_group(directory) is None                    # 77: not a member
        monkeypatch.setattr(health.os, 'stat', lambda p, *a, **k: SimpleNamespace(st_gid=66, st_uid=1234))
        assert health._db_group(directory) == 66                      # a supplementary group
        monkeypatch.setattr(health.os, 'geteuid', lambda: 0)
        monkeypatch.setattr(health.os, 'stat', lambda p, *a, **k: SimpleNamespace(st_gid=77, st_uid=1234))
        assert health._db_group(directory) == 77                      # root may chgrp to anything

    def test_no_db_no_group(self, tmp_path):
        assert health._db_group(tmp_path / 'config.db.status') is None
        assert health._db_group(tmp_path / 'not-a-status-dir') is None

    @pytest.mark.skipif(other_group() is None, reason='the user belongs to no second group')
    def test_a_real_directory_takes_the_db_files_group(self, tmp_path):
        gid = other_group()
        db = tmp_path / 'config.db'
        db.write_bytes(b'')
        os.chown(db, -1, gid)
        directory = tmp_path / 'config.db.status'
        ensure_status_dir(directory)
        assert os.stat(directory).st_gid == gid
        assert stat.S_IMODE(os.stat(directory).st_mode) == 0o750


# ---------------------------------------------------------------------------
# 6. The reader
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestReaderNoData:
    def test_a_missing_directory_is_no_data_not_a_problem(self, tmp_path):
        res = read_status_dir(tmp_path / 'config.db.status')
        assert res.present is False and res.statuses == () and res.problems == ()

    def test_an_empty_directory_is_present_with_nothing(self, sdir):
        res = read_status_dir(sdir)
        assert res.present is True and res.statuses == () and res.problems == ()

    def test_a_single_missing_worker_is_none(self, sdir, tmp_path):
        assert read_worker_status(sdir, W) is None
        assert read_worker_status(tmp_path / 'nope.status', W) is None

    def test_a_bad_worker_name_is_a_value_error(self, sdir):
        with pytest.raises(ValueError):
            read_worker_status(sdir, '../etc')

    def test_a_good_file_is_returned_with_its_fields(self, sdir):
        put(sdir, 'snapshot-worker.json', doc())
        res = read_status_dir(sdir)
        assert res.problems == () and len(res.statuses) == 1
        st = res.statuses[0]
        assert st.worker == W and st.pid == 4242
        assert st.path == str(sdir / 'snapshot-worker.json')
        assert st.started_at < st.updated_at
        (rec,) = st.records
        assert (rec.kind, rec.dataset, rec.destination, rec.result) == ('snapshot', 'pool/data', None, 'ok')
        assert rec.consecutive_failures == 0 and rec.last_success == rec.last_attempt
        assert rec.last_error is None and rec.last_error_at is None and rec.detail is None
        assert rec.key == ('snapshot', 'pool/data', None)

    def test_read_worker_status_agrees_with_the_directory_read(self, sdir):
        put(sdir, 'snapshot-worker.json', doc())
        assert read_worker_status(sdir, W) == read_status_dir(sdir).statuses[0]

    def test_files_come_back_sorted_by_name(self, sdir):
        for name in ('remote-backup-worker', 'pruning-worker', 'snapshot-worker'):
            put(sdir, f'{name}.json', doc(worker=name))
        assert [s.worker for s in read_status_dir(sdir).statuses] == [
            'pruning-worker', 'remote-backup-worker', 'snapshot-worker']

    def test_unknown_keys_are_ignored_at_every_level(self, sdir):
        body = doc(future='x', records=[{
            'kind': 'send', 'dataset': 'pool/a', 'destination': 'off', 'result': 'error',
            'last_error': 'boom', 'consecutive_failures': 4, 'new_field': {'a': [1]},
        }])
        put(sdir, 'remote-backup-worker.json', {**body, 'worker': 'remote-backup-worker'})
        res = read_status_dir(sdir)
        assert res.problems == ()
        (rec,) = res.statuses[0].records
        assert (rec.destination, rec.last_error, rec.consecutive_failures) == ('off', 'boom', 4)

    def test_other_kinds_are_accepted_for_the_consumer_to_ignore(self, sdir):
        put(sdir, 'snapshot-worker.json', doc(records=[{'kind': 'future-kind', 'dataset': 'pool/a'}]))
        (rec,) = read_status_dir(sdir).statuses[0].records
        assert rec.kind == 'future-kind'

    def test_a_very_large_failure_count_is_bounded(self, sdir):
        put(sdir, 'snapshot-worker.json', doc(records=[
            {'kind': 'snapshot', 'dataset': 'a', 'consecutive_failures': 10 ** 30}]))
        assert read_status_dir(sdir).statuses[0].records[0].consecutive_failures == 10 ** 9


@pytest.mark.unit
class TestReaderRefusals:
    """A bad file is a problem line and the good ones are still returned."""

    def two(self, sdir, bad_name='pruning-worker.json'):
        put(sdir, 'snapshot-worker.json', doc())
        res = read_status_dir(sdir)
        assert res.problems == () and [s.worker for s in res.statuses] == [W]
        return bad_name

    def check(self, sdir, bad_name, text):
        res = read_status_dir(sdir)
        assert res.present is True
        assert [s.worker for s in res.statuses] == [W]
        (problem,) = res.problems
        assert bad_name in problem and text in problem, problem
        return problem

    @pytest.mark.parametrize('content', [b'', b'{', b'not json', b'\xff\xfe\x00', b'[1, 2]', b'"x"', b'null', b'42'])
    def test_not_json_or_not_a_document(self, sdir, content):
        name = self.two(sdir)
        put(sdir, name, content)
        res = read_status_dir(sdir)
        assert [s.worker for s in res.statuses] == [W]
        assert len(res.problems) == 1 and name in res.problems[0]
        with pytest.raises(StatusError):
            read_worker_status(sdir, 'pruning-worker')

    def test_a_newer_schema_is_refused_with_advice(self, sdir):
        name = self.two(sdir)
        put(sdir, name, doc(worker='pruning-worker', schema=STATUS_SCHEMA + 1))
        problem = self.check(sdir, name, f'schema {STATUS_SCHEMA + 1}')
        assert 'upgrade zfsbackup-config' in problem

    @pytest.mark.parametrize('schema', [0, -1, True, '1', None, 1.5, [1]])
    def test_an_unusable_schema_is_refused(self, sdir, schema):
        name = self.two(sdir)
        put(sdir, name, doc(worker='pruning-worker', schema=schema))
        self.check(sdir, name, 'schema')

    def test_a_missing_schema_is_refused(self, sdir):
        name = self.two(sdir)
        body = doc(worker='pruning-worker')
        del body['schema']
        put(sdir, name, body)
        self.check(sdir, name, 'schema')

    @pytest.mark.parametrize('claimed', ['snapshot-worker', 'api-worker', None, 7, ''])
    def test_a_file_that_names_another_worker_is_refused(self, sdir, claimed):
        """A stray or copied file must not be read as the worker its name says."""
        name = self.two(sdir)
        put(sdir, name, doc(worker=claimed))
        self.check(sdir, name, 'belongs to worker')

    def test_a_symlink_is_refused_not_followed(self, sdir, tmp_path):
        name = self.two(sdir)
        target = tmp_path / 'elsewhere.json'
        target.write_text(json.dumps(doc(worker='pruning-worker')))
        os.chmod(target, 0o640)
        (sdir / name).symlink_to(target)
        self.check(sdir, name, 'symlink')

    def test_a_dangling_symlink_is_refused(self, sdir, tmp_path):
        name = self.two(sdir)
        (sdir / name).symlink_to(tmp_path / 'gone')
        self.check(sdir, name, 'symlink')

    def test_a_fifo_is_refused_without_blocking(self, sdir):
        name = self.two(sdir)
        os.mkfifo(sdir / name, 0o640)
        self.check(sdir, name, 'not a regular file')

    def test_a_directory_named_like_a_status_file_is_refused(self, sdir):
        name = self.two(sdir)
        (sdir / name).mkdir(mode=0o750)
        self.check(sdir, name, 'not a regular file')

    @pytest.mark.parametrize('mode', [0o660, 0o666, 0o620, 0o602, 0o622])
    def test_a_file_group_or_world_writable_is_refused(self, sdir, mode):
        name = self.two(sdir)
        put(sdir, name, doc(worker='pruning-worker'), mode=mode)
        self.check(sdir, name, 'writable by group or others')

    @pytest.mark.parametrize('mode', [0o640, 0o644, 0o600, 0o400])
    def test_a_file_only_the_owner_can_write_is_accepted(self, sdir, mode):
        put(sdir, 'pruning-worker.json', doc(worker='pruning-worker'), mode=mode)
        res = read_status_dir(sdir)
        assert res.problems == () and len(res.statuses) == 1

    def test_an_oversized_file_is_refused(self, sdir):
        name = self.two(sdir)
        put(sdir, name, b'{"schema":1,"worker":"pruning-worker","x":"' + b'a' * STATUS_MAX_BYTES + b'"}')
        self.check(sdir, name, 'not a status file')

    def test_a_file_of_exactly_the_limit_is_read(self, sdir):
        head = b'{"schema":1,"worker":"pruning-worker","pad":"'
        tail = b'"}'
        body = head + b'a' * (STATUS_MAX_BYTES - len(head) - len(tail)) + tail
        assert len(body) == STATUS_MAX_BYTES
        put(sdir, 'pruning-worker.json', body)
        res = read_status_dir(sdir)
        assert res.problems == () and len(res.statuses) == 1

    def test_a_file_owned_by_an_untrusted_user_is_refused(self, sdir):
        """The member check itself (the directory is refused first when its owner is
        untrusted, see below): a file whose owner is not in `trusted`."""
        put(sdir, 'snapshot-worker.json', doc())
        dfd = os.open(sdir, os.O_RDONLY)
        try:
            with pytest.raises(StatusError, match='not trusted'):
                health._read_member(dfd, 'snapshot-worker.json', sdir, {ME + 1000})
            assert health._read_member(dfd, 'snapshot-worker.json', sdir, {ME}) is not None
        finally:
            os.close(dfd)

    def test_a_directory_owned_by_an_untrusted_user_is_refused_whole(self, sdir):
        res = read_status_dir(sdir, trusted_uids={ME + 1000})          # empty: only the directory is judged
        assert res.present is True and res.statuses == ()
        (problem,) = res.problems
        assert 'status directory' in problem and 'not trusted' in problem
        put(sdir, 'snapshot-worker.json', doc())
        res = read_status_dir(sdir, trusted_uids={ME + 1000})
        assert res.statuses == () and len(res.problems) == 1 and 'status directory' in res.problems[0]
        with pytest.raises(StatusError, match='not trusted'):
            read_worker_status(sdir, W, trusted_uids={ME + 1000})

    def test_trusted_uids_can_add_the_daemons_user(self, sdir):
        put(sdir, 'snapshot-worker.json', doc())
        res = read_status_dir(sdir, trusted_uids={ME, ME + 1000})
        assert res.problems == () and len(res.statuses) == 1

    def test_the_default_trusted_owners_are_root_this_user_and_the_db_owner(self, sdir):
        assert health._default_trusted_uids(sdir) == {0, ME, os.stat(sdir.parent / 'config.db').st_uid}
        assert health._default_trusted_uids(sdir.parent / 'other.status') == {0, ME}

    @pytest.mark.parametrize('mode', [0o770, 0o777, 0o722, 0o702])
    def test_a_directory_others_can_write_is_refused_whole(self, sdir, mode):
        put(sdir, 'snapshot-worker.json', doc())
        os.chmod(sdir, mode)
        res = read_status_dir(sdir)
        assert res.present is True and res.statuses == ()
        (problem,) = res.problems
        assert 'writable by group or others' in problem
        with pytest.raises(StatusError):
            read_worker_status(sdir, W)

    def test_a_symlinked_directory_is_refused_whole(self, tmp_path):
        real = tmp_path / 'real'
        real.mkdir(mode=0o750)
        put(real, 'snapshot-worker.json', doc())
        link = tmp_path / 'config.db.status'
        link.symlink_to(real)
        res = read_status_dir(link)
        assert res.present is True and res.statuses == ()
        assert 'symlink' in res.problems[0]

    def test_a_regular_file_where_the_directory_should_be_is_a_problem(self, tmp_path):
        path = tmp_path / 'config.db.status'
        path.write_text('x')
        res = read_status_dir(path)
        assert res.present is True and res.statuses == () and len(res.problems) == 1

    def test_several_bad_files_are_all_reported_and_the_good_one_survives(self, sdir):
        put(sdir, 'snapshot-worker.json', doc())
        put(sdir, 'pruning-worker.json', b'{')
        put(sdir, 'remote-backup-worker.json', doc(worker='api-worker'))
        res = read_status_dir(sdir)
        assert [s.worker for s in res.statuses] == [W]
        assert len(res.problems) == 2

    def test_a_file_that_vanishes_between_the_listing_and_the_read_is_not_a_problem(self, sdir, monkeypatch):
        put(sdir, 'snapshot-worker.json', doc())
        real = os.listdir
        monkeypatch.setattr(
            health.os, 'listdir',
            lambda fd: real(fd) + (['gone-worker.json'] if isinstance(fd, int) else []),
        )
        res = read_status_dir(sdir)
        assert res.problems == () and [s.worker for s in res.statuses] == [W]


@pytest.mark.unit
class TestReaderIgnoresStrangers:
    @pytest.mark.parametrize('name', [
        '.snapshot-worker.json.123.456.tmp', 'README', 'snapshot-worker.json.bak', 'snapshot-worker.JSON',
        'Snapshot-worker.json', '1bad.json', '-bad.json', '.json', 'a b.json', 'x' * 41 + '.json',
        'snapshot-worker.json~', 'snapshot-worker', '.hidden.json',
    ])
    def test_names_that_are_not_worker_json_are_not_read(self, sdir, name):
        put(sdir, name, b'this is not json and is not even looked at', mode=0o666)
        res = read_status_dir(sdir)
        assert res.statuses == () and res.problems == ()

    def test_a_temporary_next_to_a_good_file_does_not_disturb_it(self, sdir):
        put(sdir, 'snapshot-worker.json', doc())
        put(sdir, f'.snapshot-worker.json.{os.getpid()}.1.tmp', b'{"half')
        res = read_status_dir(sdir)
        assert res.problems == () and [s.worker for s in res.statuses] == [W]


@pytest.mark.unit
class TestReaderRecords:
    def records(self, sdir, *raw):
        put(sdir, 'snapshot-worker.json', doc(records=list(raw)))
        res = read_status_dir(sdir)
        assert res.problems == ()
        return res.statuses[0].records

    @pytest.mark.parametrize('junk', [
        1, 'x', None, [], True, 1.5,
        {}, {'kind': 'snapshot'}, {'dataset': 'a'},
        {'kind': 5, 'dataset': 'a'}, {'kind': 'Snapshot', 'dataset': 'a'}, {'kind': '', 'dataset': 'a'},
        {'kind': 'snapshot', 'dataset': ''}, {'kind': 'snapshot', 'dataset': 5},
        {'kind': 'snapshot', 'dataset': 'a' * 513},
        {'kind': 'send', 'dataset': 'a', 'destination': 5}, {'kind': 'send', 'dataset': 'a', 'destination': ''},
        {'kind': 'send', 'dataset': 'a', 'destination': 'd' * 513},
    ])
    def test_a_malformed_record_is_skipped_and_the_good_ones_kept(self, sdir, junk):
        good = {'kind': 'snapshot', 'dataset': 'pool/ok', 'result': 'ok'}
        recs = self.records(sdir, junk, good, junk)
        assert [r.dataset for r in recs] == ['pool/ok']

    @pytest.mark.parametrize('raw', ['x', 5, None, {'a': 1}, True])
    def test_records_that_is_not_a_list_is_no_records(self, sdir, raw):
        put(sdir, 'snapshot-worker.json', doc(records=raw))
        res = read_status_dir(sdir)
        assert res.problems == () and res.statuses[0].records == ()

    def test_a_missing_records_key_is_no_records(self, sdir):
        body = doc()
        del body['records']
        put(sdir, 'snapshot-worker.json', body)
        assert read_status_dir(sdir).statuses[0].records == ()

    @pytest.mark.parametrize('failures, expected', [
        (-1, 0), (True, 0), ('3', 0), (2.5, 0), (None, 0), (7, 7), (0, 0),
    ])
    def test_consecutive_failures_is_a_non_negative_integer_or_zero(self, sdir, failures, expected):
        (rec,) = self.records(sdir, {'kind': 'snapshot', 'dataset': 'a', 'consecutive_failures': failures})
        assert rec.consecutive_failures == expected

    def test_a_missing_failure_count_is_zero(self, sdir):
        (rec,) = self.records(sdir, {'kind': 'snapshot', 'dataset': 'a'})
        assert rec.consecutive_failures == 0

    @pytest.mark.parametrize('result, expected', [
        ('ok', RESULT_OK), ('error', RESULT_ERROR), ('weird', None), (None, None), (5, None), ('OK', None),
    ])
    def test_result_is_ok_error_or_unknown(self, sdir, result, expected):
        (rec,) = self.records(sdir, {'kind': 'snapshot', 'dataset': 'a', 'result': result})
        assert rec.result == expected

    @pytest.mark.parametrize('stamp', ['yesterday', '2026-10-25', '2026-10-25 10:00:00', '2026-10-25T10:00:00',
                                       '2026-13-45T10:00:00Z', 5, None, [], '2026-10-25T10:00:00+00:00'])
    def test_an_unparsable_timestamp_is_none(self, sdir, stamp):
        (rec,) = self.records(sdir, {'kind': 'snapshot', 'dataset': 'a', 'last_attempt': stamp,
                                     'last_success': stamp, 'last_error_at': stamp})
        assert (rec.last_attempt, rec.last_success, rec.last_error_at) == (None, None, None)

    def test_a_timestamp_is_utc_epoch_seconds(self, sdir):
        (rec,) = self.records(sdir, {'kind': 'snapshot', 'dataset': 'a', 'last_attempt': '1970-01-02T00:00:01Z'})
        assert rec.last_attempt == 86401.0

    def test_text_is_clipped_and_flattened_on_read_too(self, sdir):
        (rec,) = self.records(sdir, {'kind': 'snapshot', 'dataset': 'a', 'last_error': 'x\ny' * 500,
                                     'detail': 'z' * 1000})
        assert len(rec.last_error) <= STATUS_TEXT_LIMIT and '\n' not in rec.last_error
        assert len(rec.detail) == STATUS_TEXT_LIMIT

    @pytest.mark.parametrize('text', ['', None, 5, [], {}])
    def test_empty_or_non_text_error_is_none(self, sdir, text):
        (rec,) = self.records(sdir, {'kind': 'snapshot', 'dataset': 'a', 'last_error': text, 'detail': text})
        assert rec.last_error is None and rec.detail is None

    def test_only_the_first_2048_records_are_read(self, sdir):
        raw = [{'kind': 'snapshot', 'dataset': f'pool/{i}'} for i in range(STATUS_MAX_RECORDS + 50)]
        recs = self.records(sdir, *raw)
        assert len(recs) == STATUS_MAX_RECORDS
        assert recs[-1].dataset == f'pool/{STATUS_MAX_RECORDS - 1}'

    @pytest.mark.parametrize('pid', [0, -4, True, '4242', 1.5, None])
    def test_an_unusable_pid_is_none(self, sdir, pid):
        put(sdir, 'snapshot-worker.json', doc(pid=pid))
        assert read_status_dir(sdir).statuses[0].pid is None

    def test_missing_times_are_none(self, sdir):
        body = doc(started_at='soon', updated_at=None)
        put(sdir, 'snapshot-worker.json', body)
        st = read_status_dir(sdir).statuses[0]
        assert st.started_at is None and st.updated_at is None


# ---------------------------------------------------------------------------
# 8. The writer
# ---------------------------------------------------------------------------


class Clocks:
    """Injectable wall clock and monotonic clock for a `StatusWriter`."""

    def __init__(self, wall=1_800_000_000.0, mono=1000.0):
        self.wall, self.mono = wall, mono

    def advance(self, seconds):
        self.wall += seconds
        self.mono += seconds


@pytest.fixture
def clocks():
    return Clocks()


@pytest.fixture
def make_writer(sdir, clocks):
    def make(worker=W, directory=None, **kw):
        kw.setdefault('clock', lambda: clocks.wall)
        kw.setdefault('monotonic', lambda: clocks.mono)
        kw.setdefault('pid', 99)
        return StatusWriter(sdir if directory is None else directory, worker, **kw)
    return make


def written(sdir, worker=W):
    return json.loads((sdir / f'{worker}.json').read_text())


@pytest.mark.unit
class TestWriterDocument:
    def test_a_success_and_a_failure_are_recorded_with_their_times(self, make_writer, clocks, sdir):
        w = make_writer()
        w.record(KIND_SNAPSHOT, 'pool/a', ok=True, detail='autosnap_20261025100000')
        clocks.advance(60)
        w.record(KIND_SEND, 'pool/a', 'offsite', ok=False, error='connection refused')
        assert w.flush(force=True) is True
        data = written(sdir)
        assert data['schema'] == 1 and data['worker'] == W and data['pid'] == 99
        assert data['started_at'].endswith('Z') and data['updated_at'] > data['started_at']
        send, snap = data['records']                   # sorted by kind, dataset, destination
        assert snap == {
            'kind': 'snapshot', 'dataset': 'pool/a', 'result': 'ok',
            'last_attempt': '2027-01-15T08:00:00Z', 'last_success': '2027-01-15T08:00:00Z',
            'consecutive_failures': 0, 'detail': 'autosnap_20261025100000',
        }
        assert send == {
            'kind': 'send', 'dataset': 'pool/a', 'destination': 'offsite', 'result': 'error',
            'last_attempt': '2027-01-15T08:01:00Z', 'last_error': 'connection refused',
            'last_error_at': '2027-01-15T08:01:00Z', 'consecutive_failures': 1,
        }

    def test_keys_with_no_value_are_left_out(self, make_writer, sdir):
        w = make_writer()
        w.record(KIND_PRUNE, 'pool/a', ok=True)
        w.flush(force=True)
        (rec,) = written(sdir)['records']
        assert set(rec) == {'kind', 'dataset', 'result', 'last_attempt', 'last_success', 'consecutive_failures'}

    def test_failures_count_up_and_a_success_resets_them_but_keeps_the_last_error(self, make_writer, clocks):
        w = make_writer()
        for i in range(3):
            clocks.advance(10)
            w.record(KIND_SEND, 'pool/a', 'off', ok=False, error=f'e{i}')
        (rec,) = w.records
        assert rec.consecutive_failures == 3 and rec.last_error == 'e2' and rec.result == RESULT_ERROR
        assert rec.last_success is None
        clocks.advance(10)
        w.record(KIND_SEND, 'pool/a', 'off', ok=True)
        (rec,) = w.records
        assert rec.consecutive_failures == 0 and rec.result == RESULT_OK
        assert rec.last_success == rec.last_attempt
        assert rec.last_error == 'e2' and rec.last_error_at < rec.last_attempt

    def test_detail_is_replaced_by_each_attempt(self, make_writer):
        w = make_writer()
        w.record(KIND_PRUNE, 'pool/a', ok=True, detail='pruned 3')
        assert w.records[0].detail == 'pruned 3'
        w.record(KIND_PRUNE, 'pool/a', ok=True)
        assert w.records[0].detail is None

    def test_a_failure_without_text_still_says_something(self, make_writer):
        w = make_writer()
        w.record(KIND_SEND, 'pool/a', 'off', ok=False)
        assert w.records[0].last_error == 'failed (see the journal)'

    def test_text_is_clipped_to_300_and_flattened_before_it_is_stored(self, make_writer):
        w = make_writer()
        w.record(KIND_SEND, 'pool/a', 'off', ok=False, error='boom\n' + 'x' * 1000, detail='d\t' * 500)
        rec = w.records[0]
        assert len(rec.last_error) == 300 and '\n' not in rec.last_error and rec.last_error.endswith('...')
        assert len(rec.detail) == 300 and '\t' not in rec.detail

    def test_the_same_dataset_to_two_destinations_is_two_records(self, make_writer):
        w = make_writer()
        w.record(KIND_SEND, 'pool/a', 'one', ok=False, error='x')
        w.record(KIND_SEND, 'pool/a', 'two', ok=True)
        w.record(KIND_SNAPSHOT, 'pool/a', ok=True)
        assert [(r.kind, r.destination, r.consecutive_failures) for r in w.records] == [
            ('send', 'one', 1), ('send', 'two', 0), ('snapshot', None, 0)]

    def test_the_file_is_ascii_json_on_one_line(self, make_writer, sdir):
        w = make_writer()
        w.record(KIND_SNAPSHOT, 'pool/é中', ok=True)
        w.flush(force=True)
        raw = (sdir / f'{W}.json').read_bytes()
        raw.decode('ascii')
        assert raw.count(b'\n') == 1 and raw.endswith(b'\n')
        assert read_status_dir(sdir).statuses[0].records[0].dataset == 'pool/é中'

    def test_a_round_trip_through_the_reader(self, make_writer, clocks, sdir):
        w = make_writer()
        w.record(KIND_SEND, 'pool/a', 'off', ok=False, error='e', detail='d')
        clocks.advance(5)
        w.record(KIND_SEND, 'pool/a', 'off', ok=False, error='e2')
        w.flush(force=True)
        res = read_status_dir(sdir)
        assert res.problems == ()
        (rec,) = res.statuses[0].records
        assert (rec.kind, rec.dataset, rec.destination, rec.result) == ('send', 'pool/a', 'off', 'error')
        assert rec.consecutive_failures == 2 and rec.last_error == 'e2' and rec.detail is None
        assert res.statuses[0].pid == 99


@pytest.mark.unit
class TestWriterDisabled:
    def test_no_directory_means_every_method_is_a_no_op(self, tmp_path):
        w = StatusWriter(None, W)
        assert w.enabled is False
        w.load()
        w.record(KIND_SNAPSHOT, 'pool/a', ok=False, error='x')
        w.retain([('pool/a', None)])
        assert w.records == []
        assert w.flush(force=True) is False
        assert list(tmp_path.iterdir()) == []

    def test_a_writer_with_a_directory_is_enabled(self, make_writer):
        assert make_writer().enabled is True


@pytest.mark.unit
class TestWriterFlushRate:
    def test_nothing_changed_nothing_written(self, make_writer, sdir):
        w = make_writer()
        assert w.flush() is False and w.flush(force=True) is False
        assert not (sdir / f'{W}.json').exists()

    def test_flush_writes_when_dirty_and_then_stays_quiet(self, make_writer, sdir):
        w = make_writer()
        w.record(KIND_SNAPSHOT, 'pool/a', ok=True)
        assert w.flush() is True
        first = (sdir / f'{W}.json').stat().st_mtime_ns
        assert w.flush(force=True) is False                 # clean: no rewrite
        assert (sdir / f'{W}.json').stat().st_mtime_ns == first

    def test_a_flush_within_two_seconds_is_skipped_and_kept_for_later(self, make_writer, clocks, sdir):
        w = make_writer()
        w.record(KIND_SNAPSHOT, 'pool/a', ok=True)
        assert w.flush() is True
        w.record(KIND_SNAPSHOT, 'pool/b', ok=True)
        clocks.advance(1.9)
        assert w.flush() is False
        assert [r['dataset'] for r in written(sdir)['records']] == ['pool/a']
        clocks.advance(0.1)
        assert w.flush() is True
        assert [r['dataset'] for r in written(sdir)['records']] == ['pool/a', 'pool/b']

    def test_force_ignores_the_interval(self, make_writer, clocks, sdir):
        w = make_writer()
        w.record(KIND_SNAPSHOT, 'pool/a', ok=True)
        w.flush()
        w.record(KIND_SNAPSHOT, 'pool/b', ok=True)
        assert w.flush() is False
        assert w.flush(force=True) is True
        assert len(written(sdir)['records']) == 2

    def test_the_interval_is_configurable(self, make_writer, clocks):
        w = make_writer(min_interval=10)
        w.record(KIND_SNAPSHOT, 'pool/a', ok=True)
        assert w.flush() is True
        w.record(KIND_SNAPSHOT, 'pool/b', ok=True)
        clocks.advance(9.9)
        assert w.flush() is False
        clocks.advance(0.2)
        assert w.flush() is True

    def test_a_burst_of_many_records_costs_one_write(self, make_writer, sdir, monkeypatch):
        w = make_writer()
        writes = []
        real = health.write_status_file
        monkeypatch.setattr(health, 'write_status_file', lambda *a: (writes.append(1), real(*a))[1])
        for i in range(200):
            w.record(KIND_SNAPSHOT, f'pool/{i}', ok=True)
            w.flush()
        assert len(writes) == 1

    def test_the_default_interval_is_two_seconds(self):
        assert health.STATUS_WRITE_INTERVAL == 2.0


@pytest.mark.unit
class TestWriterFailureStreak:
    def test_one_warning_per_streak_and_one_info_on_recovery(self, make_writer, clocks, sdir, caplog):
        w = make_writer()
        w.record(KIND_SNAPSHOT, 'pool/a', ok=False, error='x')
        shutil.rmtree(sdir)
        with caplog.at_level(logging.INFO, logger='zfsbackup.health'):
            assert w.flush(force=True) is False
            clocks.advance(5)
            assert w.flush(force=True) is False
            clocks.advance(5)
            assert w.flush() is False
            assert len(warnings(caplog)) == 1
            assert 'cannot write the status file' in warnings(caplog)[0].getMessage()
            assert [r for r in caplog.records if r.levelno == logging.INFO] == []
            ensure_status_dir(sdir)
            clocks.advance(5)
            assert w.flush() is True
            infos = [r for r in caplog.records if r.levelno == logging.INFO and 'can be written again' in r.getMessage()]
            assert len(infos) == 1
            # the records survived the outage in memory
            assert [r['dataset'] for r in written(sdir)['records']] == ['pool/a']
            # and a new streak warns again
            w.record(KIND_SNAPSHOT, 'pool/b', ok=True)
            shutil.rmtree(sdir)
            clocks.advance(5)
            assert w.flush() is False
        assert len(warnings(caplog)) == 2

    def test_a_failed_attempt_is_not_retried_before_the_interval(self, make_writer, clocks, sdir, monkeypatch):
        w = make_writer()
        w.record(KIND_SNAPSHOT, 'pool/a', ok=True)
        attempts = []

        def failing(*a):
            attempts.append(1)
            raise OSError(errno.EIO, 'injected')

        monkeypatch.setattr(health, 'write_status_file', failing)
        assert w.flush() is False
        clocks.advance(1)
        assert w.flush() is False
        assert len(attempts) == 1
        clocks.advance(1.5)
        assert w.flush() is False
        assert len(attempts) == 2

    def test_it_never_raises_whatever_goes_wrong(self, make_writer, monkeypatch):
        w = make_writer()
        w.record(KIND_SNAPSHOT, 'pool/a', ok=True)
        monkeypatch.setattr(health, 'write_status_file', lambda *a: (_ for _ in ()).throw(RuntimeError('odd')))
        assert w.flush(force=True) is False
        monkeypatch.setattr(w, 'document', lambda: (_ for _ in ()).throw(ValueError('odd')))
        assert w.flush(force=True) is False

    def test_the_warning_names_the_file_and_the_cause(self, make_writer, sdir, caplog):
        w = make_writer()
        w.record(KIND_SNAPSHOT, 'pool/a', ok=True)
        shutil.rmtree(sdir)
        with caplog.at_level(logging.WARNING, logger='zfsbackup.health'):
            w.flush(force=True)
        (rec,) = warnings(caplog)
        assert str(sdir / f'{W}.json') in rec.getMessage() and 'FileNotFoundError' in rec.getMessage()


@pytest.mark.unit
class TestWriterLimits:
    def test_over_the_cap_the_oldest_attempt_goes(self, make_writer, clocks):
        w = make_writer(max_records=3)
        for i in range(5):
            clocks.advance(1)
            w.record(KIND_SNAPSHOT, f'pool/{i}', ok=True)
        assert [r.dataset for r in w.records] == ['pool/2', 'pool/3', 'pool/4']

    def test_a_re_attempt_makes_a_record_young_again(self, make_writer, clocks):
        w = make_writer(max_records=3)
        for i in range(3):
            clocks.advance(1)
            w.record(KIND_SNAPSHOT, f'pool/{i}', ok=True)
        clocks.advance(1)
        w.record(KIND_SNAPSHOT, 'pool/0', ok=True)         # the oldest, refreshed
        clocks.advance(1)
        w.record(KIND_SNAPSHOT, 'pool/new', ok=True)
        assert sorted(r.dataset for r in w.records) == ['pool/0', 'pool/2', 'pool/new']

    def test_the_default_cap_is_2048(self, make_writer, clocks):
        assert STATUS_MAX_RECORDS == 2048
        w = make_writer()
        for i in range(2048 + 5):
            clocks.advance(0.001)
            w.record(KIND_SNAPSHOT, f'pool/{i}', ok=True)
        assert len(w.records) == 2048
        assert 'pool/0' not in {r.dataset for r in w.records} and 'pool/2052' in {r.dataset for r in w.records}

    def test_eviction_marks_the_writer_dirty(self, make_writer, clocks, sdir):
        w = make_writer(max_records=1)
        w.record(KIND_SNAPSHOT, 'pool/a', ok=True)
        w.flush(force=True)
        clocks.advance(1)
        w.record(KIND_SNAPSHOT, 'pool/b', ok=True)
        w.flush(force=True)
        assert [r['dataset'] for r in written(sdir)['records']] == ['pool/b']

    def test_retain_drops_what_is_no_longer_configured(self, make_writer, sdir):
        w = make_writer()
        w.record(KIND_SEND, 'pool/a', 'one', ok=False, error='x')
        w.record(KIND_SEND, 'pool/a', 'two', ok=False, error='x')
        w.record(KIND_SEND, 'pool/gone', 'one', ok=False, error='x')
        w.flush(force=True)
        w.retain([('pool/a', 'one')])
        assert [(r.dataset, r.destination) for r in w.records] == [('pool/a', 'one')]
        assert w.flush(force=True) is True               # dropping is a change to write
        assert [r['dataset'] for r in written(sdir)['records']] == ['pool/a']

    def test_retain_matches_on_dataset_and_destination_not_kind(self, make_writer):
        w = make_writer()
        w.record(KIND_SNAPSHOT, 'pool/a', ok=True)
        w.record(KIND_PRUNE, 'pool/a', ok=True)
        w.retain([('pool/a', None)])
        assert len(w.records) == 2
        w.retain([])
        assert w.records == []

    def test_retain_of_everything_present_changes_nothing_and_writes_nothing(self, make_writer):
        w = make_writer()
        w.record(KIND_SNAPSHOT, 'pool/a', ok=True)
        w.flush(force=True)
        w.retain([('pool/a', None), ('pool/b', None)])
        assert w.flush(force=True) is False

    def test_a_document_over_2_mib_is_halved_until_it_fits(self, make_writer, clocks, sdir):
        w = make_writer()
        for i in range(STATUS_MAX_RECORDS):
            clocks.advance(1)
            w.record(KIND_SEND, f'pool/{i:04d}' + 'd' * 470, f'dest{i % 5}' + 'x' * 60,
                     ok=False, error='e' * 400, detail='n' * 400)
        assert len(w.records) == STATUS_MAX_RECORDS
        assert len(w.document()) > STATUS_MAX_BYTES                # the premise
        assert w.flush(force=True) is True
        assert (sdir / f'{W}.json').stat().st_size <= STATUS_MAX_BYTES
        kept = w.records
        assert 1 <= len(kept) < STATUS_MAX_RECORDS
        # the newest survive
        assert {r.dataset[:9] for r in kept} >= {f'pool/{STATUS_MAX_RECORDS - 1:04d}'}
        assert f'pool/0000' not in {r.dataset[:9] for r in kept}
        res = read_status_dir(sdir)
        assert res.problems == () and len(res.statuses[0].records) == len(kept)

    def test_a_single_record_is_never_dropped_by_the_loop(self, make_writer, sdir):
        w = make_writer()
        w.record(KIND_SNAPSHOT, 'pool/a', ok=True)
        w.flush(force=True)
        assert len(written(sdir)['records']) == 1


@pytest.mark.unit
class TestWriterLoad:
    def test_counters_survive_a_restart(self, make_writer, clocks, sdir):
        first = make_writer(pid=1)
        for i in range(2):
            clocks.advance(10)
            first.record(KIND_SEND, 'pool/a', 'off', ok=False, error=f'e{i}')
        clocks.advance(10)
        first.record(KIND_SEND, 'pool/b', 'off', ok=True)
        first.flush(force=True)

        second = make_writer(pid=2)
        assert second.records == []
        second.load()
        by_key = {r.key: r for r in second.records}
        assert by_key[('send', 'pool/a', 'off')].consecutive_failures == 2
        assert by_key[('send', 'pool/a', 'off')].last_error == 'e1'
        assert by_key[('send', 'pool/b', 'off')].last_success is not None
        clocks.advance(10)
        second.record(KIND_SEND, 'pool/a', 'off', ok=False, error='e2')
        second.flush(force=True)
        data = written(sdir)
        assert data['pid'] == 2                                  # the new incarnation
        (a,) = [r for r in data['records'] if r['dataset'] == 'pool/a']
        assert a['consecutive_failures'] == 3

    def test_load_with_no_file_starts_empty_quietly(self, make_writer, caplog):
        w = make_writer()
        with caplog.at_level(logging.DEBUG, logger='zfsbackup.health'):
            w.load()
        assert w.records == [] and not [r for r in caplog.records if r.levelno >= logging.WARNING]

    @pytest.mark.parametrize('how', ['garbage', 'newer', 'foreign', 'writable'])
    def test_a_refused_or_damaged_previous_file_starts_empty_with_one_warning(self, make_writer, sdir, caplog, how):
        if how == 'garbage':
            put(sdir, f'{W}.json', b'{"trunc')
        elif how == 'newer':
            put(sdir, f'{W}.json', doc(schema=STATUS_SCHEMA + 1))
        elif how == 'foreign':
            put(sdir, f'{W}.json', doc(worker='pruning-worker'))
        else:
            put(sdir, f'{W}.json', doc(), mode=0o666)
        w = make_writer()
        with caplog.at_level(logging.WARNING, logger='zfsbackup.health'):
            w.load()
        assert w.records == []
        assert len(warnings(caplog, 'ignoring the previous status file')) == 1
        w.record(KIND_SNAPSHOT, 'pool/a', ok=True)
        assert w.flush(force=True) is True                        # the first write replaces it
        assert read_status_dir(sdir).problems == ()

    def test_load_trims_to_the_cap(self, make_writer, sdir):
        put(sdir, f'{W}.json', doc(records=[
            {'kind': 'snapshot', 'dataset': f'd{i}', 'last_attempt': f'2026-10-25T10:00:{i:02d}Z'} for i in range(10)]))
        w = make_writer(max_records=4)
        w.load()
        assert [r.dataset for r in w.records] == ['d6', 'd7', 'd8', 'd9']

    def test_load_removes_only_this_workers_stale_temporaries(self, make_writer, sdir):
        now = time.time()
        old, fresh = now - 3601, now - 60
        mine_old = put(sdir, f'.{W}.json.11.1.tmp', b'x')
        make_writer = functools.partial(make_writer, clock=time.time)
        mine_fresh = put(sdir, f'.{W}.json.12.2.tmp', b'x')
        theirs_old = put(sdir, '.pruning-worker.json.13.3.tmp', b'x')
        stranger = put(sdir, 'junk.tmp', b'x')
        os.utime(mine_old, (old, old))
        os.utime(theirs_old, (old, old))
        os.utime(stranger, (old, old))
        os.utime(mine_fresh, (fresh, fresh))
        make_writer().load()
        assert not mine_old.exists()
        assert mine_fresh.exists() and theirs_old.exists() and stranger.exists()

    def test_load_never_removes_a_symlink_target(self, make_writer, sdir, tmp_path):
        victim = tmp_path / 'victim'
        victim.write_text('precious')
        link = sdir / f'.{W}.json.14.4.tmp'
        link.symlink_to(victim)
        make_writer().load()
        assert victim.read_text() == 'precious'

    def test_load_is_fine_without_the_directory(self, make_writer, tmp_path):
        w = make_writer(directory=tmp_path / 'nope.status')
        w.load()
        assert w.records == []


# ---------------------------------------------------------------------------
# Concurrency-review hardening (S-7a)
# ---------------------------------------------------------------------------


class _AsUid:
    """A real `os.stat_result` that claims to be owned by `uid`."""

    def __init__(self, real, uid):
        self._real, self.st_uid = real, uid

    def __getattr__(self, name):
        return getattr(self._real, name)


@pytest.mark.unit
class TestDirectoryOwner:
    """`ensure_status_dir` insists the directory is owned by this process's euid or root,
    for a directory it has just made and for one it found."""

    def foreign_owner(self, monkeypatch):
        """Everything in `health` sees directories owned by someone else."""
        real = os.fstat
        monkeypatch.setattr(
            health.os, 'fstat',
            lambda fd: _AsUid(real(fd), ME + 4242) if stat.S_ISDIR(real(fd).st_mode) else real(fd),
        )

    def test_an_existing_directory_owned_by_someone_else_is_refused(self, tmp_path, monkeypatch):
        directory = tmp_path / 'config.db.status'
        directory.mkdir(mode=0o750)
        self.foreign_owner(monkeypatch)
        with pytest.raises(StatusError, match='owned by uid'):
            ensure_status_dir(directory)

    def test_a_directory_made_a_moment_ago_by_someone_else_is_refused_too(self, tmp_path, monkeypatch):
        """Lost the creation race to another user: the same check applies to a path
        `mkdir` reports as created."""
        directory = tmp_path / 'config.db.status'
        self.foreign_owner(monkeypatch)
        with pytest.raises(StatusError, match='owned by uid'):
            ensure_status_dir(directory)

    def test_the_refusal_names_what_it_wanted(self, tmp_path, monkeypatch):
        directory = tmp_path / 'config.db.status'
        directory.mkdir(mode=0o750)
        self.foreign_owner(monkeypatch)
        with pytest.raises(StatusError) as exc:
            ensure_status_dir(directory)
        assert str(ME) in str(exc.value) and 'root' in str(exc.value)

    def test_a_root_owned_directory_is_accepted(self, tmp_path, monkeypatch):
        directory = tmp_path / 'config.db.status'
        directory.mkdir(mode=0o750)
        real = os.fstat
        monkeypatch.setattr(
            health.os, 'fstat',
            lambda fd: _AsUid(real(fd), 0) if stat.S_ISDIR(real(fd).st_mode) else real(fd),
        )
        ensure_status_dir(directory)

    def test_the_owner_check_does_not_use_the_db_owner(self, tmp_path, monkeypatch):
        """The reader trusts the DB's owner too; the supervisor's creating check does not."""
        directory = tmp_path / 'config.db.status'
        directory.mkdir(mode=0o750)
        self.foreign_owner(monkeypatch)
        monkeypatch.setattr(health.os, 'stat', lambda p, *a, **k: _AsUid(os.lstat(p), ME + 4242))
        with pytest.raises(StatusError):
            ensure_status_dir(directory)

    def test_it_can_be_run_again_and_again(self, tmp_path):
        """The supervisor runs it before every spawn: cheap and idempotent."""
        directory = tmp_path / 'config.db.status'
        for _ in range(5):
            ensure_status_dir(directory)
        assert stat.S_IMODE(os.stat(directory).st_mode) == 0o750

    def test_a_directory_deleted_between_runs_is_made_again(self, tmp_path):
        directory = tmp_path / 'config.db.status'
        ensure_status_dir(directory)
        shutil.rmtree(directory)
        ensure_status_dir(directory)
        assert directory.is_dir()

    def test_a_directory_loosened_between_runs_is_refused_not_trusted(self, tmp_path):
        directory = tmp_path / 'config.db.status'
        ensure_status_dir(directory)
        os.chmod(directory, 0o770)
        with pytest.raises(StatusError, match='writable by group or others'):
            ensure_status_dir(directory)
        os.chmod(directory, 0o750)
        ensure_status_dir(directory)


@pytest.mark.unit
class TestDirectoryOpen:
    def test_the_directory_is_opened_without_blocking_and_without_following_symlinks(
        self, sdir, monkeypatch
    ):
        flags = []
        real = os.open

        def spy(path, flag, *args, **kwargs):
            if str(path) == str(sdir):
                flags.append(flag)
            return real(path, flag, *args, **kwargs)

        monkeypatch.setattr(health.os, 'open', spy)
        os.close(health._open_dir(sdir))
        (flag,) = flags
        assert flag & os.O_NONBLOCK and flag & os.O_NOFOLLOW and flag & os.O_DIRECTORY

    def test_a_directory_on_another_device_than_the_db_directory_is_refused(self, sdir, monkeypatch):
        """A mount at `<db>.status` could hang the supervisor or hide the real directory."""
        real = os.stat

        class OtherDevice:
            def __init__(self, st):
                self._st = st
                self.st_dev = st.st_dev + 1

            def __getattr__(self, name):
                return getattr(self._st, name)

        monkeypatch.setattr(
            health.os, 'stat',
            lambda p, *a, **k: OtherDevice(real(p, *a, **k)) if str(p) == str(sdir.parent) else real(p, *a, **k),
        )
        with pytest.raises(StatusError, match='mount point'):
            health._open_dir(sdir)

    def test_the_mount_check_guards_every_user_of_the_directory(self, sdir, monkeypatch):
        put(sdir, 'snapshot-worker.json', doc())
        real = os.stat
        monkeypatch.setattr(
            health.os, 'stat',
            lambda p, *a, **k: type('S', (), {'st_dev': -1})() if str(p) == str(sdir.parent) else real(p, *a, **k),
        )
        with pytest.raises(StatusError, match='mount point'):
            ensure_status_dir(sdir)
        with pytest.raises(StatusError, match='mount point'):
            write_status_file(sdir, W, 'x')
        res = read_status_dir(sdir)
        assert res.present and res.statuses == () and 'mount point' in res.problems[0]
        with pytest.raises(StatusError, match='mount point'):
            read_worker_status(sdir, W)

    def test_a_directory_on_the_same_device_is_fine(self, sdir):
        os.close(health._open_dir(sdir))


@pytest.mark.unit
class TestReaderNeverRaises:
    @pytest.mark.parametrize('shape', ['array', 'object', 'inside_records'])
    def test_deeply_nested_json_is_a_problem_not_a_crash(self, sdir, shape):
        depth = 200_000
        if shape == 'array':
            body = '[' * depth + ']' * depth
        elif shape == 'object':
            body = '{"a":' * depth + '1' + '}' * depth
        else:
            body = '{"schema":1,"worker":"snapshot-worker","records":' + '[' * depth + ']' * depth + '}'
        put(sdir, 'pruning-worker.json', doc(worker='pruning-worker'))
        put(sdir, 'snapshot-worker.json', body)
        res = read_status_dir(sdir)                                 # does not raise
        assert [s.worker for s in res.statuses] == ['pruning-worker']
        (problem,) = res.problems
        assert 'snapshot-worker.json' in problem
        with pytest.raises(StatusError):
            read_worker_status(sdir, W)

    def test_any_exception_while_parsing_becomes_a_problem(self, sdir, monkeypatch):
        put(sdir, 'snapshot-worker.json', doc())
        monkeypatch.setattr(health, '_parse_status_doc', lambda *a: (_ for _ in ()).throw(MemoryError()))
        res = read_status_dir(sdir)
        assert res.statuses == () and len(res.problems) == 1
        with pytest.raises(StatusError):
            read_worker_status(sdir, W)

    def test_a_nested_status_file_does_not_stop_the_writer_loading_it(self, make_writer, sdir, caplog):
        put(sdir, f'{W}.json', '[' * 200_000 + ']' * 200_000)
        w = make_writer()
        with caplog.at_level(logging.WARNING, logger='zfsbackup.health'):
            w.load()
        assert w.records == [] and len(warnings(caplog, 'ignoring the previous status file')) == 1


@pytest.mark.unit
class TestOldIncarnationCannotClobberTheNewOne:
    """A worker the supervisor replaced (reload) can still be finishing its last dataset
    when its successor starts writing: the old one must not overwrite the new file."""

    def test_an_older_writer_refuses_to_replace_a_newer_ones_file(self, make_writer, clocks, sdir, caplog):
        old = make_writer(pid=1)
        old.record(KIND_SEND, 'pool/a', 'off', ok=False, error='old news')
        assert old.flush(force=True) is True
        clocks.advance(30)
        new = make_writer(pid=2)
        new.load()
        new.record(KIND_SEND, 'pool/a', 'off', ok=False, error='new news')
        assert new.flush(force=True) is True
        before = (sdir / f'{W}.json').read_bytes()

        clocks.advance(5)
        old.record(KIND_SEND, 'pool/b', 'off', ok=True)
        with caplog.at_level(logging.INFO, logger='zfsbackup.health'):
            assert old.flush(force=True) is False
            assert old.records == []                                # it drops what it cannot publish
            clocks.advance(5)
            old.record(KIND_SEND, 'pool/c', 'off', ok=True)
            assert old.flush(force=True) is False
        assert (sdir / f'{W}.json').read_bytes() == before
        assert written(sdir)['pid'] == 2
        (rec,) = warnings(caplog, 'newer')
        assert W in rec.getMessage() and 'will not overwrite' in rec.getMessage()

    def test_a_restart_in_the_same_second_still_wins(self, make_writer, clocks, sdir):
        """`load()` moves the new writer's `started_at` past its predecessor's."""
        old = make_writer(pid=1)
        old.record(KIND_SNAPSHOT, 'pool/a', ok=True)
        old.flush(force=True)
        new = make_writer(pid=2)                                    # same wall-clock second
        new.load()
        new.record(KIND_SNAPSHOT, 'pool/a', ok=False, error='x')
        new.flush(force=True)
        old.record(KIND_SNAPSHOT, 'pool/b', ok=True)
        assert old.flush(force=True) is False
        assert written(sdir)['pid'] == 2
        started = written(sdir)['started_at']
        assert started > health._iso(clocks.wall)

    def test_a_writer_keeps_publishing_its_own_file(self, make_writer, clocks, sdir):
        w = make_writer(pid=1)
        for i in range(4):
            clocks.advance(3)
            w.record(KIND_SNAPSHOT, f'pool/{i}', ok=True)
            assert w.flush() is True
        assert len(written(sdir)['records']) == 4

    @pytest.mark.parametrize('their_start', ['older', 'equal'])
    def test_a_file_from_an_older_or_same_start_is_replaced_normally(self, make_writer, clocks, sdir, their_start):
        started = health._iso(clocks.wall if their_start == 'equal' else clocks.wall - 100)
        put(sdir, f'{W}.json', doc(started_at=started, pid=777))
        w = make_writer(pid=1)
        w.record(KIND_SNAPSHOT, 'pool/a', ok=True)
        assert w.flush(force=True) is True
        assert written(sdir)['pid'] == 1

    def test_an_unreadable_file_on_disk_is_not_newer(self, make_writer, sdir):
        put(sdir, f'{W}.json', b'garbage')
        w = make_writer()
        w.record(KIND_SNAPSHOT, 'pool/a', ok=True)
        assert w.flush(force=True) is True
        assert written(sdir)['records'][0]['dataset'] == 'pool/a'

    def test_the_check_is_one_stat_while_the_file_is_the_writers_own(self, make_writer, clocks, sdir, monkeypatch):
        w = make_writer()
        w.record(KIND_SNAPSHOT, 'pool/a', ok=True)
        w.flush(force=True)
        reads = []
        real = health.read_worker_status
        monkeypatch.setattr(health, 'read_worker_status', lambda *a, **k: (reads.append(1), real(*a, **k))[1])
        for i in range(3):
            clocks.advance(3)
            w.record(KIND_SNAPSHOT, f'pool/{i}', ok=True)
            assert w.flush() is True
        assert reads == []


@pytest.mark.unit
class TestHeartbeat:
    """`flush(force, heartbeat=True)` rewrites the file even when nothing changed, so
    `updated_at` says "alive"; a worker that completed a cycle always has a file."""

    def test_a_writer_with_no_records_still_makes_a_file(self, make_writer, sdir):
        w = make_writer()
        assert w.flush(force=True, heartbeat=True) is True
        data = written(sdir)
        assert data['records'] == [] and data['worker'] == W and data['pid'] == 99
        res = read_status_dir(sdir)
        assert res.problems == () and res.statuses[0].records == ()

    def test_without_the_flag_a_clean_writer_writes_nothing(self, make_writer, clocks, sdir):
        w = make_writer()
        w.record(KIND_SNAPSHOT, 'pool/a', ok=True)
        w.flush(force=True)
        first = written(sdir)['updated_at']
        clocks.advance(60)
        assert w.flush(force=True) is False
        assert written(sdir)['updated_at'] == first

    def test_a_heartbeat_refreshes_updated_at_and_nothing_else(self, make_writer, clocks, sdir):
        w = make_writer()
        w.record(KIND_SEND, 'pool/a', 'off', ok=False, error='x')
        w.flush(force=True)
        before = written(sdir)
        clocks.advance(60)
        assert w.flush(force=True, heartbeat=True) is True
        after = written(sdir)
        assert after['updated_at'] > before['updated_at']
        assert {k: v for k, v in after.items() if k != 'updated_at'} == \
            {k: v for k, v in before.items() if k != 'updated_at'}

    def test_a_heartbeat_obeys_the_interval_unless_forced(self, make_writer, clocks, sdir):
        w = make_writer()
        w.record(KIND_SNAPSHOT, 'pool/a', ok=True)
        w.flush()
        clocks.advance(1)
        assert w.flush(heartbeat=True) is False
        assert w.flush(force=True, heartbeat=True) is True
        clocks.advance(2)
        assert w.flush(heartbeat=True) is True

    def test_a_superseded_writer_sends_no_heartbeat(self, make_writer, clocks, sdir):
        old = make_writer(pid=1)
        old.record(KIND_SNAPSHOT, 'pool/a', ok=True)
        old.flush(force=True)
        clocks.advance(10)
        new = make_writer(pid=2)
        new.load()
        new.flush(force=True, heartbeat=True)
        before = (sdir / f'{W}.json').read_bytes()
        clocks.advance(10)
        assert old.flush(force=True, heartbeat=True) is False
        assert (sdir / f'{W}.json').read_bytes() == before
        assert old.flush(force=True, heartbeat=True) is False

    def test_a_disabled_writer_has_no_heartbeat(self, tmp_path):
        assert StatusWriter(None, W).flush(force=True, heartbeat=True) is False
        assert list(tmp_path.iterdir()) == []

    def test_a_failed_heartbeat_is_the_usual_one_warning(self, make_writer, sdir, caplog):
        w = make_writer()
        shutil.rmtree(sdir)
        with caplog.at_level(logging.WARNING, logger='zfsbackup.health'):
            assert w.flush(force=True, heartbeat=True) is False
            assert w.flush(force=True, heartbeat=True) is False
        assert len(warnings(caplog)) == 1
