"""`zfs.snapshot.many(datasets, name, properties=None)`: one atomic `zfs snapshot a@s b@s ...` over several datasets.

What the tests pin:

* the argv exactly (one command, `-o k=v` pairs first, then `ds@name` in input order, never `-r`), the result (the
  `Snapshot`s in input order), and the single form `zfs.snapshot(ds, name, ...)` unchanged,
* every refusal is a `ValueError` (`InvalidNameError` for names) raised BEFORE anything is executed: nothing reaches
  `Popen`,
* the argv bound `MANY_MAX_ARGV_BYTES`, counted in bytes with each argument's NUL, property values included: exactly the
  bound passes, one byte more is refused,
* a failure is `Command._exec`'s bare `Exception(stderr)` (a name collision on one dataset: zfs creates none),
* the recursive single form `zfs.snapshot(ds, name, recursive=True)`: `zfs snapshot -r` runs FIRST and the descendants
  are listed afterwards, best-effort (a failing or timed-out listing is one WARNING, never a raise; the result is always
  `[own] + descendants`),
* `timeout()`: expiry raises `ZFSTimeoutError`, a child stuck after KILL makes every later *timed* call fail fast before
  `Popen`, while untimed calls are never refused.

Mocked children are fed bytes (`feed`) or are real `sys.executable -c ...` processes behind a `Popen` that swaps the
argv (`redirect`); the guard in conftest only objects to a real zfs/zpool. The real-pool tier is `TestRealSnapshotMany`
in tests/test_real_zfs.py.
"""

import io
import logging
import subprocess
import sys

import pytest

import libzfseasy as zfs
from libzfseasy import zfs as zfs_module
from libzfseasy.types import Dataset, Filesystem, InvalidNameError, Snapshot, Volume
from libzfseasy.zfs import SnapshotCommand, ZFSTimeoutError

pytestmark = pytest.mark.unit


# ---------------------------------------------------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------------------------------------------------

class _Proc:
    def __init__(self, stdout=b'', stderr=b'', returncode=0):
        self.stdout = io.BufferedReader(io.BytesIO(stdout))
        self.stderr = io.BufferedReader(io.BytesIO(stderr))
        self.returncode = returncode

    def poll(self):
        return self.returncode

    def wait(self, timeout=None):
        return self.returncode

    def terminate(self):
        pass

    kill = terminate


class _Feed:
    """Replaces `subprocess.Popen`: every call is recorded and gets a fresh finished process."""

    def __init__(self):
        self.calls = []
        self.by_subcommand = {}
        self.set()

    def set(self, stdout=b'', stderr=b'', returncode=0):
        self._args = (stdout, stderr, returncode)
        return self

    def script(self, subcommand, stdout=b'', stderr=b'', returncode=0):
        """Answer calls of one zfs subcommand (`snapshot`, `list`) differently from the default."""
        self.by_subcommand[subcommand] = (stdout, stderr, returncode)
        return self

    def __call__(self, cmd, *args, **kwargs):
        self.calls.append(list(cmd))
        return _Proc(*self.by_subcommand.get(cmd[1], self._args))

    @property
    def subcommands(self):
        return [c[1] for c in self.calls]

    @property
    def argv(self):
        """The argv of the last call, without the leading zfs binary."""
        return self.calls[-1][1:]


@pytest.fixture
def feed(monkeypatch):
    feeder = _Feed()
    monkeypatch.setattr(subprocess, 'Popen', feeder)
    return feeder


class _Redirect:
    """Makes every `Popen` run `sys.executable -c SCRIPT <argv after the binary>`, recording the children."""

    def __init__(self, monkeypatch, script):
        self.procs, self.argvs = [], []
        outer = self
        real_popen = subprocess.Popen

        class Redirected(real_popen):
            def __init__(self, cmd, *args, **kwargs):
                outer.argvs.append(list(cmd))
                super().__init__([sys.executable, '-c', script, *cmd[1:]], *args, **kwargs)
                outer.procs.append(self)

        monkeypatch.setattr(subprocess, 'Popen', Redirected)

    def kill_all(self):
        for proc in self.procs:
            if proc.poll() is None:
                proc.kill()
            proc.wait()


@pytest.fixture
def redirect(monkeypatch):
    made = []

    def make(script):
        redirected = _Redirect(monkeypatch, script)
        made.append(redirected)
        return redirected

    yield make
    for redirected in made:
        redirected.kill_all()


@pytest.fixture(autouse=True)
def clean_stuck_registry():
    """The stuck-child registry is module state: every test starts with it empty and gets the original back."""
    saved = list(zfs_module._stuck)
    zfs_module._stuck.clear()
    yield
    zfs_module._stuck[:] = saved


def fs(*names):
    return [Filesystem(n) for n in names]


def base_bytes():
    """The argv bytes of `[zfs, 'snapshot']` (each argument plus its NUL)."""
    return len(zfs_module._zfs_cmd().encode()) + 1 + len(b'snapshot') + 1


def datasets_for_argv_size(total, name='s', pool='p', properties_bytes=0):
    """Distinct filesystems of the pool whose `ds@name` arguments bring the whole argv to exactly `total` bytes.
    Entries are about 200 bytes each; the last two share the remainder."""
    remaining = total - base_bytes() - properties_bytes
    sizes = []                              # bytes of each `ds@name` argument, NUL included
    while remaining > 400:
        sizes.append(200)
        remaining -= 200
    first = remaining // 2
    sizes += [first, remaining - first]
    out = []
    for i, entry in enumerate(sizes):
        # entry = len(pool + '/' + pad + '@' + name) + 1
        pad_len = entry - 1 - len(pool) - 1 - 1 - len(name)
        assert pad_len > 6, 'the remainder is too small to fill'
        out.append(Filesystem('%s/%05d%s' % (pool, i, 'x' * (pad_len - 5))))
    return out


def argv_bytes(cmd):
    return sum(len(a.encode('utf-8', 'surrogateescape')) + 1 for a in cmd)


# ---------------------------------------------------------------------------------------------------------------------
# the command
# ---------------------------------------------------------------------------------------------------------------------

class TestArgv:
    def test_two_datasets(self, feed):
        zfs.snapshot.many(fs('p/a', 'p/b'), 's')
        assert feed.argv == ['snapshot', 'p/a@s', 'p/b@s']
        assert feed.calls[0][0] == zfs_module._zfs_cmd()

    def test_properties_come_first_as_o_pairs_then_the_snapshots_in_input_order(self, feed):
        zfs.snapshot.many(fs('p/c', 'p/a', 'p/b'), 's', properties={'org.t:one': '1', 'org.t:two': 'two words'})
        assert feed.argv == ['snapshot', '-o', 'org.t:one=1', '-o', 'org.t:two=two words', 'p/c@s', 'p/a@s', 'p/b@s']

    def test_a_single_dataset_is_one_snapshot_argument(self, feed):
        result = zfs.snapshot.many(fs('p/a'), 's')
        assert feed.argv == ['snapshot', 'p/a@s']
        assert [str(s) for s in result] == ['p/a@s']

    def test_exactly_one_command_and_never_dash_r(self, feed):
        zfs.snapshot.many(fs('p', 'p/a', 'p/a/b'), 's', properties={'org.t:x': 'y'})
        assert len(feed.calls) == 1
        assert '-r' not in feed.argv and not any(a.startswith('-') and a != '-o' for a in feed.argv)

    def test_no_list_call_precedes_it(self, feed):
        """The recursive single form lists first; `many` never does."""
        zfs.snapshot.many(fs('p/a', 'p/b'), 's')
        assert [c[1] for c in feed.calls] == ['snapshot']

    def test_the_names_are_sent_verbatim(self, feed):
        zfs.snapshot.many([Volume('p/v'), Dataset('p/d'), Filesystem('p/f')], 'daily_20260101000000')
        assert feed.argv == ['snapshot', 'p/v@daily_20260101000000', 'p/d@daily_20260101000000',
                             'p/f@daily_20260101000000']

    def test_a_leading_at_in_the_name_is_stripped_like_the_single_form(self, feed):
        zfs.snapshot.many(fs('p/a'), '@s')
        assert feed.argv == ['snapshot', 'p/a@s']

    def test_empty_or_none_properties_send_no_o(self, feed):
        for props in (None, {}):
            zfs.snapshot.many(fs('p/a'), 's', properties=props)
            assert feed.argv == ['snapshot', 'p/a@s']

    def test_a_tuple_and_a_generator_of_datasets_are_accepted(self, feed):
        zfs.snapshot.many(tuple(fs('p/a', 'p/b')), 's')
        assert feed.argv == ['snapshot', 'p/a@s', 'p/b@s']
        zfs.snapshot.many((d for d in fs('p/c', 'p/d')), 's')
        assert feed.argv == ['snapshot', 'p/c@s', 'p/d@s']

    def test_callable_on_the_class_and_the_singleton(self, feed):
        SnapshotCommand.many(fs('p/a'), 's')
        zfs.snapshot.many(fs('p/b'), 's')
        assert [c[-1] for c in feed.calls] == ['p/a@s', 'p/b@s']
        assert isinstance(zfs.snapshot, SnapshotCommand)

    def test_the_input_list_is_not_modified(self, feed):
        datasets = fs('p/b', 'p/a')
        zfs.snapshot.many(datasets, 's')
        assert [d.name for d in datasets] == ['p/b', 'p/a']


class TestResult:
    def test_snapshots_in_input_order(self, feed):
        result = zfs.snapshot.many(fs('p/c', 'p/a', 'p/b'), 's')
        assert [s.name for s in result] == ['p/c@s', 'p/a@s', 'p/b@s']

    def test_types_and_links(self, feed):
        datasets = [Filesystem('p/f'), Volume('p/v'), Dataset('p/d')]
        result = zfs.snapshot.many(datasets, 's')
        assert isinstance(result, list) and len(result) == 3
        assert all(isinstance(s, Snapshot) for s in result)
        assert [s.short for s in result] == ['s', 's', 's']
        assert [s.dataset for s in result] == datasets

    def test_properties_are_on_every_snapshot(self, feed):
        result = zfs.snapshot.many(fs('p/a', 'p/b'), 's', properties={'org.t:k': 'v'})
        assert [s['org.t:k'].value for s in result] == ['v', 'v']

    def test_a_single_dataset_returns_a_one_element_list(self, feed):
        result = zfs.snapshot.many(fs('p/a'), 's')
        assert isinstance(result, list) and len(result) == 1 and result[0].name == 'p/a@s'


class TestSingleFormUnchanged:
    def test_one_dataset(self, feed):
        result = zfs.snapshot(Filesystem('p/a'), 's')
        assert feed.argv == ['snapshot', 'p/a@s']
        assert isinstance(result, Snapshot) and result.name == 'p/a@s'

    def test_with_properties(self, feed):
        zfs.snapshot(Filesystem('p/a'), 's', properties={'org.t:k': 'v'})
        assert feed.argv == ['snapshot', '-o', 'org.t:k=v', 'p/a@s']

    def test_recursive_snapshots_first_then_lists(self, feed):
        feed.set(b'p/a\tfilesystem\np/a/b\tfilesystem\n')
        result = zfs.snapshot(Filesystem('p/a'), 's', recursive=True)
        assert feed.subcommands == ['snapshot', 'list']
        assert feed.calls[0][1:] == ['snapshot', '-r', 'p/a@s']
        assert [s.name for s in result] == ['p/a@s', 'p/a/b@s']

    def test_the_package_name_is_one_command_with_many(self):
        assert callable(zfs.snapshot) and callable(zfs.snapshot.many)
        assert SnapshotCommand.MANY_MAX_ARGV_BYTES == 128 * 1024


# ---------------------------------------------------------------------------------------------------------------------
# the recursive single form: `zfs snapshot -r` first, a best-effort listing second
# ---------------------------------------------------------------------------------------------------------------------

LISTING = b'p/a\tfilesystem\np/a/b\tfilesystem\np/a/b/c\tvolume\np/a/d\tfilesystem\n'


def warnings(caplog):
    return [r.getMessage() for r in caplog.records if r.name == 'libzfseasy' and r.levelno == logging.WARNING]


class TestRecursiveSnapshot:
    @pytest.fixture(autouse=True)
    def _caplog_level(self, caplog):
        caplog.set_level(logging.WARNING, logger='libzfseasy')

    def test_exec_first_the_snapshot_is_made_before_anything_is_listed(self, feed):
        feed.script('list', LISTING)
        zfs.snapshot(Filesystem('p/a'), 's', recursive=True)
        assert feed.subcommands == ['snapshot', 'list']
        assert feed.calls[0][1:] == ['snapshot', '-r', 'p/a@s']
        listing = feed.calls[1][1:]
        assert listing[listing.index('-t') + 1] == 'filesystem,volume' and '-r' in listing and listing[-1] == 'p/a'

    def test_properties_go_on_the_snapshot_command_only(self, feed):
        feed.script('list', LISTING)
        zfs.snapshot(Filesystem('p/a'), 's', recursive=True, properties={'org.t:k': 'v'})
        assert feed.calls[0][1:] == ['snapshot', '-r', '-o', 'org.t:k=v', 'p/a@s']
        assert '-o' in feed.calls[1] and 'org.t:k=v' not in feed.calls[1]

    def test_the_descendants_come_in_listing_order_after_the_dataset(self, feed, caplog):
        feed.script('list', LISTING)
        result = zfs.snapshot(Filesystem('p/a'), 's', recursive=True, properties={'org.t:k': 'v'})
        assert [s.name for s in result] == ['p/a@s', 'p/a/b@s', 'p/a/b/c@s', 'p/a/d@s']
        assert all(isinstance(s, Snapshot) for s in result)
        assert isinstance(result[0].dataset, Filesystem) and isinstance(result[2].dataset, Volume)
        assert all(s.properties for s in result)    # the requested properties ride on every Snapshot
        assert warnings(caplog) == []

    def test_the_dataset_is_first_even_when_the_listing_prints_it_later(self, feed, caplog):
        feed.script('list', b'p/a/b\tfilesystem\np/a\tfilesystem\np/a/d\tfilesystem\n')
        result = zfs.snapshot(Filesystem('p/a'), 's', recursive=True)
        assert [s.name for s in result] == ['p/a@s', 'p/a/b@s', 'p/a/d@s']
        assert warnings(caplog) == []

    def test_a_dataset_without_descendants_is_a_one_element_list(self, feed, caplog):
        feed.script('list', b'p/a\tfilesystem\n')
        result = zfs.snapshot(Filesystem('p/a'), 's', recursive=True)
        assert [s.name for s in result] == ['p/a@s'] and warnings(caplog) == []

    def test_a_failing_listing_after_a_good_snapshot_returns_own_with_one_warning(self, feed, caplog):
        feed.script('list', b'', b'cannot open: pool is busy\n', 1)
        result = zfs.snapshot(Filesystem('p/a'), 's', recursive=True)       # does not raise
        assert feed.subcommands == ['snapshot', 'list']
        assert isinstance(result, list) and [s.name for s in result] == ['p/a@s']
        assert isinstance(result[0], Snapshot)
        (message,) = warnings(caplog)
        assert 'p/a@s' in message and 'succeeded' in message and 'pool is busy' in message

    def test_a_listing_that_fails_midway_keeps_what_it_returned_and_warns_once(self, feed, caplog):
        feed.script('list', b'p/a\tfilesystem\np/a/b\tfilesystem\n', b'pool went away\n', 1)
        result = zfs.snapshot(Filesystem('p/a'), 's', recursive=True)
        assert result[0].name == 'p/a@s'
        assert len(warnings(caplog)) == 1

    def test_a_listing_that_drops_the_dataset_keeps_it_first_with_a_warning(self, feed, caplog):
        feed.script('list', b'p/a/b\tfilesystem\np/a/d\tfilesystem\n')
        result = zfs.snapshot(Filesystem('p/a'), 's', recursive=True, properties={'org.t:k': 'v'})
        assert [s.name for s in result] == ['p/a@s', 'p/a/b@s', 'p/a/d@s']
        assert result[0].properties
        (message,) = warnings(caplog)
        assert 'did not contain the dataset itself' in message and 'p/a@s' in message

    def test_an_empty_listing_returns_own_with_a_warning(self, feed, caplog):
        feed.script('list', b'')
        result = zfs.snapshot(Filesystem('p/a'), 's', recursive=True)
        assert [s.name for s in result] == ['p/a@s'] and len(warnings(caplog)) == 1

    def test_a_listing_row_zfs_libzfseasy_cannot_represent_does_not_drop_the_dataset(self, feed, caplog):
        feed.script('list', b'p/a\tfilesystem\np/a/ bad\tfilesystem\np/a/b\tfilesystem\n')
        result = zfs.snapshot(Filesystem('p/a'), 's', recursive=True)
        assert [s.name for s in result] == ['p/a@s', 'p/a/b@s']

    def test_the_snapshot_failing_raises_and_nothing_is_listed(self, feed, caplog):
        feed.script('snapshot', b'', b"cannot create snapshot 'p/a@s': dataset already exists\n", 1)
        feed.script('list', LISTING)
        with pytest.raises(Exception, match='dataset already exists'):
            zfs.snapshot(Filesystem('p/a'), 's', recursive=True)
        assert feed.subcommands == ['snapshot']
        assert warnings(caplog) == []

    def test_a_value_refused_before_zfs_runs_executes_nothing(self, feed):
        with pytest.raises(ValueError):
            zfs.snapshot(Filesystem('p/a'), 's', recursive=True, properties={'org.t:k': 'a\nb'})
        assert feed.calls == []

    def test_the_single_form_never_lists(self, feed):
        result = zfs.snapshot(Filesystem('p/a'), 's')
        assert feed.subcommands == ['snapshot'] and isinstance(result, Snapshot)

    def test_a_listing_timeout_is_swallowed_and_the_snapshot_is_reported(self, redirect, caplog):
        """The snapshot is quick, the listing child sleeps past the timeout: the `ZFSTimeoutError` is a WARNING,
        not a failure of a snapshot that exists."""
        kids = redirect('import sys, time\nif sys.argv[1] == "list":\n    time.sleep(120)\n')
        with zfs.timeout(3):
            result = zfs.snapshot(Filesystem('p/a'), 's', recursive=True)
        assert [argv[1] for argv in kids.argvs] == ['snapshot', 'list']
        assert [s.name for s in result] == ['p/a@s']
        (message,) = warnings(caplog)
        assert 'timed out' in message and 'p/a@s' in message
        assert zfs_module._stuck == []                      # the child died on TERM: nothing stuck

    def test_a_listing_timeout_with_a_stuck_child_fails_the_next_timed_call_fast(self, redirect, monkeypatch,
                                                                                 caplog):
        monkeypatch.setattr(zfs_module, '_EXPIRY_WAIT', 0.2)
        kids = redirect('import sys, time\nif sys.argv[1] == "list":\n    time.sleep(120)\n')
        unkillable = [True]
        real_poll, real_wait = subprocess.Popen.poll, subprocess.Popen.wait
        popen = subprocess.Popen

        class Unkillable(popen):
            """Only the listing child (argv[1] == 'list' after redirection) survives KILL."""

            def _stubborn(self):
                return unkillable[0] and self.args[3] == 'list'

            def poll(self):
                return None if self._stubborn() else real_poll(self)

            def wait(self, timeout=None):
                if self._stubborn() and timeout is not None:
                    raise subprocess.TimeoutExpired(self.args, timeout)
                return real_wait(self, timeout=timeout)

        monkeypatch.setattr(subprocess, 'Popen', Unkillable)
        try:
            with zfs.timeout(3):
                result = zfs.snapshot(Filesystem('p/a'), 's', recursive=True)
                assert [s.name for s in result] == ['p/a@s']
                abandoned, ours = warnings(caplog)      # the library's own abandon notice, then the listing's
                assert 'did not exit after SIGTERM and SIGKILL' in abandoned
                assert 'p/a@s succeeded' in ours and 'did not exit after SIGKILL' in ours
                assert len(zfs_module._stuck) == 1
                started = len(kids.argvs)
                with pytest.raises(ZFSTimeoutError, match='not started'):
                    zfs.snapshot(Filesystem('p/a'), 't', recursive=True)
                assert len(kids.argvs) == started           # fail fast: no second child, no second snapshot
        finally:
            unkillable[0] = False
            for proc in kids.procs:
                try:
                    proc.kill()
                except OSError:
                    pass
                proc.wait(timeout=10)

    def test_the_snapshot_timing_out_raises_zfs_timeout_error_and_nothing_is_listed(self, redirect):
        kids = redirect(SLEEP)
        with zfs.timeout(1):
            with pytest.raises(ZFSTimeoutError, match='snapshot timed out'):
                zfs.snapshot(Filesystem('p/a'), 's', recursive=True)
        assert [argv[1] for argv in kids.argvs] == ['snapshot']


# ---------------------------------------------------------------------------------------------------------------------
# refusals: ValueError, and nothing is executed
# ---------------------------------------------------------------------------------------------------------------------

class TestRefusals:
    @staticmethod
    def refused(feed, datasets, name='s', exc=ValueError, match=None, **kwargs):
        with pytest.raises(exc, match=match) as excinfo:
            zfs.snapshot.many(datasets, name, **kwargs)
        assert feed.calls == []                         # nothing was executed
        return excinfo.value

    def test_an_empty_list(self, feed):
        self.refused(feed, [], match='No datasets')
        self.refused(feed, (), match='No datasets')
        self.refused(feed, (d for d in []), match='No datasets')

    @pytest.mark.parametrize('bad', [None, 5, 1.5, object(), 'p/a', b'p/a', Filesystem('p/a'), Snapshot.from_name('p/a@s')])
    def test_not_a_list_of_datasets(self, feed, bad):
        self.refused(feed, bad, match='Expected a list')

    @pytest.mark.parametrize('bad', [
        'p/a', b'p/a', None, 5, Snapshot.from_name('p/a@s'), zfs.zfs.Bookmark.from_name('p/a#b'),
    ])
    def test_a_non_dataset_entry(self, feed, bad):
        self.refused(feed, [Filesystem('p/ok'), bad], match='Expected Filesystem, Volume or Dataset')

    def test_a_dataset_twice(self, feed):
        message = self.refused(feed, fs('p/a', 'p/b', 'p/a'), match='listed twice')
        assert 'p/a' in str(message)

    def test_the_same_object_twice(self, feed):
        a = Filesystem('p/a')
        self.refused(feed, [a, a], match='listed twice')

    def test_a_filesystem_and_a_dataset_of_the_same_name_are_a_duplicate(self, feed):
        self.refused(feed, [Filesystem('p/a'), Dataset('p/a')], match='listed twice')

    def test_two_pools(self, feed):
        error = self.refused(feed, fs('p/a', 'q/b'), match='An atomic snapshot cannot span pools')
        assert str(error) == 'An atomic snapshot cannot span pools (got p, q)'

    def test_two_pools_are_named_sorted_whatever_the_order(self, feed):
        error = self.refused(feed, fs('q/b', 'p/a', 'q'))
        assert str(error) == 'An atomic snapshot cannot span pools (got p, q)'

    def test_three_pools(self, feed):
        error = self.refused(feed, fs('c/a', 'a/a', 'b/a'))
        assert str(error) == 'An atomic snapshot cannot span pools (got a, b, c)'

    def test_a_pool_root_and_a_child_of_another_pool(self, feed):
        self.refused(feed, fs('p', 'pp/a'), match='cannot span pools')

    def test_a_pool_root_and_its_child_are_one_pool(self, feed):
        zfs.snapshot.many(fs('p', 'p/a'), 's')
        assert feed.argv == ['snapshot', 'p@s', 'p/a@s']

    @pytest.mark.parametrize('name', ['s\n', 's@t', '', '@', 's\t', 's\r', 's\0', 'a#b', 's%t', 's/t', 'a' * 300])
    def test_a_bad_snapshot_name(self, feed, name):
        self.refused(feed, fs('p/a', 'p/b'), name=name)

    def test_a_space_in_the_name_is_valid_zfs_and_sent_as_one_argument(self, feed):
        zfs.snapshot.many(fs('p/a'), 's t')
        assert feed.argv == ['snapshot', 'p/a@s t']

    @pytest.mark.parametrize('name', ['s\n', 's@t', ''])
    def test_a_bad_snapshot_name_is_an_invalid_name_error(self, feed, name):
        self.refused(feed, fs('p/a'), name=name, exc=InvalidNameError)

    @pytest.mark.parametrize('name', [None, 5, b's', ['s']])
    def test_a_name_that_is_not_a_string(self, feed, name):
        self.refused(feed, fs('p/a'), name=name)

    @pytest.mark.parametrize('char', ['\n', '\r', '\t', '\0'])
    def test_a_property_value_zfs_output_could_not_frame(self, feed, char):
        self.refused(feed, fs('p/a', 'p/b'), properties={'org.t:k': 'a' + char + 'b'})

    def test_such_a_value_is_an_invalid_value_error(self, feed):
        self.refused(feed, fs('p/a'), exc=zfs_module.InvalidValueError, properties={'org.t:k': 'x\ny'})

    def test_a_bad_value_in_a_later_property(self, feed):
        self.refused(feed, fs('p/a'), properties={'org.t:ok': 'fine', 'org.t:bad': 'x\ty'})

    @pytest.mark.parametrize('prop', ['-o', 'nocolon', 'org t:x', ''])
    def test_a_bad_property_name(self, feed, prop):
        self.refused(feed, fs('p/a'), properties={prop: 'v'})

    def test_the_first_error_wins_and_runs_nothing_even_with_a_valid_prefix_of_datasets(self, feed):
        self.refused(feed, fs('p/a', 'p/b', 'q/c', 'p/a'))


# ---------------------------------------------------------------------------------------------------------------------
# the argv bound
# ---------------------------------------------------------------------------------------------------------------------

class TestArgvBound:
    def test_the_constant(self):
        assert SnapshotCommand.MANY_MAX_ARGV_BYTES == 131072
        assert zfs.snapshot.MANY_MAX_ARGV_BYTES == 131072

    def test_exactly_the_bound_passes(self, feed):
        bound = SnapshotCommand.MANY_MAX_ARGV_BYTES
        datasets = datasets_for_argv_size(bound)
        result = zfs.snapshot.many(datasets, 's')
        assert len(feed.calls) == 1
        assert argv_bytes(feed.calls[0]) == bound
        assert [s.dataset.name for s in result] == [d.name for d in datasets]

    def test_one_byte_over_is_refused_and_nothing_runs(self, feed):
        bound = SnapshotCommand.MANY_MAX_ARGV_BYTES
        datasets = datasets_for_argv_size(bound + 1)
        with pytest.raises(ValueError, match='Too many datasets for one atomic snapshot command') as excinfo:
            zfs.snapshot.many(datasets, 's')
        assert feed.calls == []
        assert '%d bytes of arguments, at most %d' % (bound + 1, bound) in str(excinfo.value)

    def test_one_byte_under_passes(self, feed):
        zfs.snapshot.many(datasets_for_argv_size(SnapshotCommand.MANY_MAX_ARGV_BYTES - 1), 's')
        assert argv_bytes(feed.calls[0]) == SnapshotCommand.MANY_MAX_ARGV_BYTES - 1

    def test_the_binary_path_counts(self, feed, monkeypatch):
        """The bound is on the whole argv: a longer zfs path leaves less room for the datasets."""
        datasets = datasets_for_argv_size(SnapshotCommand.MANY_MAX_ARGV_BYTES)
        longer = zfs_module._zfs_cmd() + '/' + 'z' * 10
        monkeypatch.setattr(zfs_module, '_zfs_cmd', lambda: longer)
        with pytest.raises(ValueError, match='Too many datasets'):
            zfs.snapshot.many(datasets, 's')
        assert feed.calls == []

    def test_property_values_count_exactly_at_the_bound(self, feed):
        props = {'org.t:k': 'v' * 1000}
        extra = argv_bytes(['-o', 'org.t:k=' + 'v' * 1000])
        datasets = datasets_for_argv_size(SnapshotCommand.MANY_MAX_ARGV_BYTES, properties_bytes=extra)
        zfs.snapshot.many(datasets, 's', properties=props)
        assert argv_bytes(feed.calls[0]) == SnapshotCommand.MANY_MAX_ARGV_BYTES

    def test_property_values_count_one_byte_over(self, feed):
        datasets = datasets_for_argv_size(SnapshotCommand.MANY_MAX_ARGV_BYTES,
                                          properties_bytes=argv_bytes(['-o', 'org.t:k=' + 'v' * 1000]))
        with pytest.raises(ValueError, match='Too many datasets'):
            zfs.snapshot.many(datasets, 's', properties={'org.t:k': 'v' * 1001})
        assert feed.calls == []

    def test_the_count_is_in_bytes_not_characters(self, feed):
        """A two-byte character in a property value counts twice."""
        datasets = datasets_for_argv_size(SnapshotCommand.MANY_MAX_ARGV_BYTES,
                                          properties_bytes=argv_bytes(['-o', 'org.t:k=' + 'é' * 500]))
        zfs.snapshot.many(datasets, 's', properties={'org.t:k': 'é' * 500})
        assert argv_bytes(feed.calls[0]) == SnapshotCommand.MANY_MAX_ARGV_BYTES
        with pytest.raises(ValueError, match='Too many datasets'):
            zfs.snapshot.many(datasets, 's', properties={'org.t:k': 'é' * 501})

    def test_a_very_large_list_is_refused_not_split(self, feed):
        datasets = [Filesystem('p/%06d' % i) for i in range(20000)]
        with pytest.raises(ValueError, match='Too many datasets'):
            zfs.snapshot.many(datasets, 's')
        assert feed.calls == []

    def test_a_thousand_typical_datasets_fit(self, feed):
        datasets = [Filesystem('tank/backups/host%04d/data' % i) for i in range(1000)]
        zfs.snapshot.many(datasets, 'daily_20260101000000')
        assert len(feed.calls) == 1 and len(feed.argv) == 1001


# ---------------------------------------------------------------------------------------------------------------------
# failures
# ---------------------------------------------------------------------------------------------------------------------

COLLISION = (b"cannot create snapshot 'p/b@s': dataset already exists\n"
             b"no snapshots were created\n")


class TestFailure:
    def test_a_collision_raises_a_bare_exception_with_zfss_stderr(self, feed):
        feed.set(b'', COLLISION, 1)
        with pytest.raises(Exception) as excinfo:
            zfs.snapshot.many(fs('p/a', 'p/b'), 's')
        assert type(excinfo.value) is Exception
        message = str(excinfo.value).lower()
        assert "cannot create snapshot 'p/b@s': dataset already exists" in message
        assert 'no snapshots were created' in message

    def test_the_message_is_the_single_forms(self, feed):
        feed.set(b'', COLLISION, 1)
        with pytest.raises(Exception) as many_error:
            zfs.snapshot.many(fs('p/b'), 's')
        with pytest.raises(Exception) as single_error:
            zfs.snapshot(Filesystem('p/b'), 's')
        assert str(many_error.value) == str(single_error.value)

    def test_a_failure_without_stderr_describes_the_exit(self, feed):
        feed.set(b'', b'', 2)
        with pytest.raises(Exception, match='exit code 2'):
            zfs.snapshot.many(fs('p/a'), 's')

    def test_a_failure_is_neither_a_value_error_nor_a_timeout(self, feed):
        feed.set(b'', COLLISION, 1)
        with pytest.raises(Exception) as excinfo:
            zfs.snapshot.many(fs('p/a', 'p/b'), 's')
        assert not isinstance(excinfo.value, (ValueError, ZFSTimeoutError))

    def test_a_failure_returns_no_snapshots_and_runs_the_command_once(self, feed):
        feed.set(b'', COLLISION, 1)
        with pytest.raises(Exception):
            zfs.snapshot.many(fs('p/a', 'p/b'), 's')
        assert len(feed.calls) == 1             # never retried one by one: that would not be atomic

    def test_a_missing_dataset(self, feed):
        feed.set(b'', b"cannot open 'p/nope': dataset does not exist\n", 1)
        with pytest.raises(Exception, match='dataset does not exist'):
            zfs.snapshot.many(fs('p/a', 'p/nope'), 's')


# ---------------------------------------------------------------------------------------------------------------------
# timeout()
# ---------------------------------------------------------------------------------------------------------------------

SLEEP = 'import time\ntime.sleep(120)\n'
QUICK = 'import sys\n'


class _FakeStuck:
    """A child that cannot be reaped: still running, `wait()` always times out."""

    pid = 4242

    def __init__(self):
        self.stdout = self.stderr = _Closable()

    def poll(self):
        return None

    def wait(self, timeout=None):
        raise subprocess.TimeoutExpired('fake', timeout)

    def terminate(self):
        pass

    kill = terminate


class _Closable:
    closed = False

    def close(self):
        self.closed = True


@pytest.mark.subprocess
class TestTimeout:
    def test_a_generous_timeout_does_not_disturb_a_fast_call(self, redirect):
        kids = redirect(QUICK)
        with zfs.timeout(30):
            result = zfs.snapshot.many(fs('p/a', 'p/b'), 's')
        assert [s.name for s in result] == ['p/a@s', 'p/b@s']
        assert kids.argvs[0][1:] == ['snapshot', 'p/a@s', 'p/b@s']
        assert kids.procs[0].poll() == 0

    def test_expiry_raises_zfs_timeout_error_and_reaps_the_child(self, redirect):
        kids = redirect(SLEEP)
        with zfs.timeout(1):
            with pytest.raises(ZFSTimeoutError) as excinfo:
                zfs.snapshot.many(fs('p/a', 'p/b'), 's')
        assert type(excinfo.value) is ZFSTimeoutError
        assert 'snapshot timed out after 1s; outcome unknown' in str(excinfo.value)
        (proc,) = kids.procs
        assert proc.poll() is not None
        assert zfs_module._stuck == []
        assert len(kids.argvs) == 1                 # one command, not retried

    def test_a_timeout_is_not_a_value_error(self):
        assert not issubclass(ZFSTimeoutError, ValueError)

    def test_a_stuck_child_makes_a_later_timed_call_fail_fast_before_popen(self, feed):
        stuck = _FakeStuck()
        zfs_module._register_stuck(stuck, None, 'zfs snapshot')
        with zfs.timeout(5):
            with pytest.raises(ZFSTimeoutError, match='not started: 1 earlier command'):
                zfs.snapshot.many(fs('p/a', 'p/b'), 's')
        assert feed.calls == []
        assert [s.process.pid for s in zfs_module._stuck] == [4242]

    def test_a_stuck_child_never_refuses_an_untimed_call(self, feed):
        zfs_module._register_stuck(_FakeStuck(), None, 'zfs snapshot')
        result = zfs.snapshot.many(fs('p/a'), 's')
        assert [s.name for s in result] == ['p/a@s'] and len(feed.calls) == 1

    def test_validation_still_comes_first_under_a_timeout_with_a_stuck_child(self, feed):
        zfs_module._register_stuck(_FakeStuck(), None, 'zfs snapshot')
        with zfs.timeout(5):
            with pytest.raises(ValueError, match='cannot span pools'):
                zfs.snapshot.many(fs('p/a', 'q/b'), 's')
        assert feed.calls == []

    def test_the_stuck_child_exits_and_timed_calls_work_again(self, feed):
        stuck = _FakeStuck()
        zfs_module._register_stuck(stuck, None, 'zfs snapshot')
        stuck.poll = lambda: 0
        with zfs.timeout(5):
            zfs.snapshot.many(fs('p/a'), 's')
        assert len(feed.calls) == 1 and zfs_module._stuck == []

    def test_the_stuck_child_a_timeout_leaves_is_what_fails_the_next_call_fast(self, redirect, monkeypatch):
        """End to end: an expiry whose child survives KILL registers it, and the next timed `many` is refused."""
        monkeypatch.setattr(zfs_module, '_EXPIRY_WAIT', 0.2)
        kids = redirect(SLEEP)
        unkillable = [True]
        real_poll, real_wait = subprocess.Popen.poll, subprocess.Popen.wait
        popen = subprocess.Popen

        class Unkillable(popen):
            def poll(self):
                return None if unkillable[0] else real_poll(self)

            def wait(self, timeout=None):
                if unkillable[0] and timeout is not None:
                    raise subprocess.TimeoutExpired(self.args, timeout)
                return real_wait(self, timeout=timeout)

        monkeypatch.setattr(subprocess, 'Popen', Unkillable)
        try:
            with zfs.timeout(0.4):
                with pytest.raises(ZFSTimeoutError, match='did not exit after SIGKILL'):
                    zfs.snapshot.many(fs('p/a'), 's')
                assert len(zfs_module._stuck) == 1
                started = len(kids.argvs)
                with pytest.raises(ZFSTimeoutError, match='not started'):
                    zfs.snapshot.many(fs('p/a'), 's')
                assert len(kids.argvs) == started       # no second child
        finally:
            unkillable[0] = False
            for proc in kids.procs:
                try:
                    proc.kill()
                except OSError:
                    pass
                proc.wait(timeout=10)
