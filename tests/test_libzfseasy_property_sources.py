"""`zfs.get.property_sources()`: which properties exist on objects and where each comes from, with no values.

Phase one of a fail-closed read: `zfs get -H -o name,property,source [-s ...] [-r|-d N] -t <types> all [ds ...]`. Every
column it asks for is a dataset name, a property name or a source, none of which can hold a tab or a newline, so a value
(the only column an owner controls freely, and not requested) cannot forge a row. What the tests pin:

* the argv exactly (no value column, so `-s` is allowed; datasets last; `-t` always sent),
* the happy path with every source form, in zfs's order, and names that are well-shaped but not representable,
* the checks, each of which raises `ZFSOutputError` naming the first problem and how many rows were refused, only after
  zfs has exited (the output is drained), and which a non-zero exit takes precedence over,
* scope / type / duplicate / contiguity rules, the `prefix` filter (applied last, so it never hides a bad row),
* `ValueError` for bad arguments before anything runs, `timeout()` support, the exports,
* the forgery argument itself, with a fake `zfs get` that honours `-o` and plants a hostile multi-line value.

Mocked children are fed bytes; real children are `sys.executable -c ...` behind a `Popen` that swaps the argv (the
guard in conftest only objects to a real zfs/zpool). Nothing here needs a pool: the real tier is
`TestRealPropertySources` in tests/test_real_zfs.py.
"""

import io
import subprocess
import sys

import pytest

import libzfseasy
import libzfseasy as zfs
from libzfseasy import zfs as zfs_module
from libzfseasy.types import Bookmark, Filesystem, Snapshot, Volume
from libzfseasy.zfs import GetCommand, PropertySource, ZFSOutputError, ZFSTimeoutError

pytestmark = pytest.mark.unit


# ---------------------------------------------------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------------------------------------------------

class _Proc:
    """A finished child whose pipes hold exactly these bytes."""

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
    """Replaces `subprocess.Popen`: every call gets a fresh process holding the configured bytes."""

    def __init__(self):
        self.calls = []
        self.set()

    def set(self, stdout=b'', stderr=b'', returncode=0):
        self._args = (stdout, stderr, returncode)
        return self

    def __call__(self, cmd, *args, **kwargs):
        self.calls.append(list(cmd))
        return _Proc(*self._args)

    @property
    def argv(self):
        """The argv of the last call, without the leading zfs binary."""
        return self.calls[-1][1:]


@pytest.fixture
def feed(monkeypatch):
    feeder = _Feed()
    monkeypatch.setattr(subprocess, 'Popen', feeder)
    return feeder


def text(*rows):
    """`-H` output from rows given as tuples of columns or ready-made strings."""
    return ''.join(('\t'.join(r) if isinstance(r, tuple) else r) + '\n' for r in rows).encode('utf-8', 'surrogateescape')


def run(feed, *rows, **kwargs):
    """`property_sources(**kwargs)` over these rows."""
    feed.set(text(*rows))
    return zfs.get.property_sources(**kwargs)


def refused(feed, *rows, count=1, problem=None, **kwargs):
    """The call must raise `ZFSOutputError`; check the count and the first problem named in its message."""
    feed.set(text(*rows))
    with pytest.raises(ZFSOutputError) as excinfo:
        zfs.get.property_sources(**kwargs)
    message = str(excinfo.value)
    assert '%d row%s refused' % (count, '' if count == 1 else 's') in message, message
    if problem is not None:
        assert 'the first: ' + problem in message, message
    return message


ROW = ('p', 'compression', 'default')


# ---------------------------------------------------------------------------------------------------------------------
# the result
# ---------------------------------------------------------------------------------------------------------------------

class TestPropertySourceType:
    def test_is_a_three_field_named_tuple_without_a_value(self):
        assert PropertySource._fields == ('name', 'property', 'source')
        row = PropertySource('p', 'compression', 'local')
        assert row == ('p', 'compression', 'local')
        assert (row.name, row.property, row.source) == ('p', 'compression', 'local')
        assert not hasattr(row, 'value') and not hasattr(row, 'received')

    def test_is_not_a_get_row(self):
        assert not issubclass(PropertySource, zfs.GetRow)

    def test_output_error_is_a_plain_exception_not_a_value_error(self):
        assert issubclass(ZFSOutputError, Exception) and not issubclass(ZFSOutputError, ValueError)


class TestHappyPath:
    # every source form, a space in names and in the `inherited from` origin, a snapshot named with a space
    ROWS = [
        ('p', 'type', '-'),
        ('p', 'used', '-'),
        ('p', 'compression', 'default'),
        ('p', 'org.zfsbackup:config', 'local'),
        ('p', 'org.t:r', 'received'),
        ('p', 'mountpoint', 'temporary'),
        ('p/a b', 'type', '-'),
        ('p/a b', 'org.t:v', 'inherited from p'),
        ('p/a b/c', 'org.t:v', 'inherited from p/a b'),
        ('p/a b@s c', 'org.t:v', 'inherited from p/a b'),
        ('p/a b/c#bm', 'type', '-'),
    ]

    def test_every_source_form_in_zfs_order(self, feed):
        got = run(feed, *self.ROWS)
        assert got == [PropertySource(*r) for r in self.ROWS]
        assert all(isinstance(r, PropertySource) for r in got)

    def test_names_sources_and_properties_are_kept_verbatim(self, feed):
        got = run(feed, *self.ROWS)
        assert [r.name for r in got][-3:] == ['p/a b/c', 'p/a b@s c', 'p/a b/c#bm']
        assert got[7].source == 'inherited from p'
        assert got[8].source == 'inherited from p/a b'

    def test_empty_output_is_an_empty_list(self, feed):
        assert run(feed) == []

    def test_a_final_line_without_a_newline_is_read(self, feed):
        feed.set(b'p\tcompression\tdefault')
        assert zfs.get.property_sources() == [PropertySource('p', 'compression', 'default')]

    def test_blank_lines_are_ignored(self, feed):
        # characterisation: a value-less output has no blank line, so one is noise rather than a forged row
        feed.set(b'\np\tcompression\tdefault\n\n')
        assert zfs.get.property_sources() == [PropertySource(*ROW)]

    def test_a_new_native_property_name_is_accepted(self, feed):
        assert run(feed, ('p', 'brand_new_prop', 'default'))[0].property == 'brand_new_prop'

    @pytest.mark.parametrize('prop', ['-x:y', 'Org.Mixed:Case', ':', 'a:' + 'b' * 253, '-:' + 'b' * 253],
                             ids=['leading-dash', 'mixed-case', 'colon-only', 'max-length', 'dash-colon-max'])
    def test_a_well_shaped_property_name_zfs_allows_is_kept(self, feed, prop):
        """Shape only, never `Validate.attribute`: an odd property name must not stop the read."""
        assert run(feed, ('p', prop, 'local'))[0].property == prop

    @pytest.mark.parametrize('name', [
        'p/\u00fcn\u00ef', 'p/-x', 'p/a b', 'p/a  b ', 'p/%x', 'p/a=b', 'p/x\u202ey', 'p/' + 'a' * 100 + '\u00e9',
    ], ids=['non-ascii', 'dash', 'space', 'spaces', 'percent', 'equals', 'bidi', 'long-multibyte'])
    def test_a_well_shaped_name_libzfseasy_cannot_represent_is_kept(self, feed, name):
        """Deliberately not `Validate.dataset`: a caller deciding what is safe to keep must see such an object."""
        assert run(feed, (name, 'compression', 'local'))[0].name == name

    def test_an_unrepresentable_origin_is_kept(self, feed):
        row = ('p/x', 'compression', 'inherited from p/\u00fcn\u00ef')
        assert run(feed, row) == [PropertySource(*row)]

    def test_a_255_byte_name_is_kept(self, feed):
        name = 'p/' + 'a' * 253
        assert len(name.encode()) == 255
        assert run(feed, (name, 'compression', 'local'))[0].name == name

    def test_the_result_is_a_list_of_the_requested_rows_only(self, feed):
        assert type(run(feed, ROW)) is list


# ---------------------------------------------------------------------------------------------------------------------
# argv
# ---------------------------------------------------------------------------------------------------------------------

class TestArgv:
    BASE = ['get', '-H', '-o', 'name,property,source']

    def test_defaults(self, feed):
        run(feed)
        assert feed.argv == self.BASE + ['-t', 'all', 'all']
        assert feed.calls[0][0] == zfs_module._zfs_cmd()

    def test_no_value_and_no_received_column_and_no_parseable_flag(self, feed):
        run(feed, ROW, ds=Filesystem('p'), recursive=True, sources=['local'], types='filesystem')
        argv = feed.argv
        assert argv[argv.index('-o') + 1] == 'name,property,source'
        assert 'value' not in ','.join(argv) and 'received' not in ','.join(argv)
        assert '-p' not in argv and argv.count('-o') == 1

    def test_sources_all_sends_no_dash_s(self, feed):
        run(feed, sources='all')
        assert '-s' not in feed.argv

    @pytest.mark.parametrize('sources', [None, [], '', ['all'], ['local', 'all'], 'ALL'])
    def test_no_filter_means_no_dash_s(self, feed, sources):
        run(feed, sources=sources)
        assert '-s' not in feed.argv

    def test_sources_are_lowercased_and_comma_joined(self, feed):
        run(feed, sources=['local', 'RECEIVED'])
        assert feed.argv == self.BASE + ['-s', 'local,received', '-t', 'all', 'all']

    def test_sources_as_a_comma_string(self, feed):
        run(feed, sources='Local,temporary')
        assert feed.argv[4:6] == ['-s', 'local,temporary']

    def test_inherited_is_passed_to_zfs(self, feed):
        run(feed, sources='inherited')
        assert feed.argv[4:6] == ['-s', 'inherited']

    def test_recursive(self, feed):
        run(feed, recursive=True)
        assert feed.argv == self.BASE + ['-r', '-t', 'all', 'all']

    def test_depth(self, feed):
        run(feed, depth=2)
        assert feed.argv == self.BASE + ['-d', '2', '-t', 'all', 'all']

    def test_depth_zero_sends_no_dash_d(self, feed):
        run(feed, depth=0)
        assert '-d' not in feed.argv

    @pytest.mark.parametrize('types, sent', [
        ('filesystem,volume', 'filesystem,volume'),
        (['snapshot', 'bookmark'], 'snapshot,bookmark'),
        ('snapshot', 'snapshot'),
        (None, 'all'),
        ('', 'all'),
        ([], 'all'),
        ('all', 'all'),
    ])
    def test_types_are_always_sent_explicitly(self, feed, types, sent):
        run(feed, types=types)
        assert feed.argv[feed.argv.index('-t') + 1] == sent

    def test_datasets_come_last_in_the_given_order(self, feed):
        run(feed, ds=[Filesystem('p/b'), Volume('p/a'), Snapshot.from_name('p/c@s')], sources='local', recursive=True,
            depth=3, types='all')
        assert feed.argv == self.BASE + ['-s', 'local', '-r', '-d', '3', '-t', 'all', 'all', 'p/b', 'p/a', 'p/c@s']

    def test_a_single_object_is_accepted_without_a_list(self, feed):
        run(feed, ds=Filesystem('p/a'))
        assert feed.argv[-2:] == ['all', 'p/a']

    def test_a_bookmark_operand(self, feed):
        run(feed, ds=Bookmark.from_name('p/a#b'), types='bookmark')
        assert feed.argv[-3:] == ['bookmark', 'all', 'p/a#b']

    def test_prefix_never_reaches_zfs(self, feed):
        run(feed, prefix='org.zfsbackup:')
        assert feed.argv == self.BASE + ['-t', 'all', 'all']
        assert not any('org.zfsbackup' in a for a in feed.argv)

    def test_runs_exactly_one_command(self, feed):
        run(feed, ROW, ds=Filesystem('p'), recursive=True)
        assert len(feed.calls) == 1


# ---------------------------------------------------------------------------------------------------------------------
# prefix
# ---------------------------------------------------------------------------------------------------------------------

class TestPrefix:
    ROWS = [
        ('p', 'compression', 'default'),
        ('p', 'org.zfsbackup:config', 'local'),
        ('p', 'org.zfsbackup:anchor.d', 'local'),
        ('p', 'org.t:v', 'local'),
        ('p/c', 'org.zfsbackup:config', 'inherited from p'),
        ('p/c', 'used', '-'),
    ]

    def test_a_matching_prefix_keeps_only_matching_rows_in_order(self, feed):
        got = run(feed, *self.ROWS, prefix='org.zfsbackup:')
        assert got == [PropertySource(*r) for r in (self.ROWS[1], self.ROWS[2], self.ROWS[4])]

    def test_a_prefix_nothing_matches_gives_an_empty_list(self, feed):
        assert run(feed, *self.ROWS, prefix='com.example:') == []

    def test_a_list_of_prefixes(self, feed):
        got = run(feed, *self.ROWS, prefix=['org.t:', 'compress'])
        assert [(r.name, r.property) for r in got] == [('p', 'compression'), ('p', 'org.t:v')]

    def test_a_tuple_of_prefixes(self, feed):
        assert len(run(feed, *self.ROWS, prefix=('org.t:', 'used'))) == 2

    def test_a_prefix_is_a_start_not_a_whole_name_and_is_case_sensitive(self, feed):
        assert len(run(feed, *self.ROWS, prefix='org.')) == 4
        assert run(feed, *self.ROWS, prefix='ORG.') == []

    def test_a_string_is_one_prefix_never_a_comma_list(self, feed):
        feed.set(text(*self.ROWS))
        with pytest.raises(ValueError):
            zfs.get.property_sources(prefix='org.t:,compress')

    @pytest.mark.parametrize('prefix', [None, [], ()])
    def test_no_prefix_keeps_everything(self, feed, prefix):
        assert len(run(feed, *self.ROWS, prefix=prefix)) == len(self.ROWS)

    def test_the_filter_does_not_change_which_rows_are_checked(self, feed):
        """A malformed row that the filter would drop is still found: the filter runs after every check."""
        refused(feed, ('p', 'compression', 'default'), ('p', 'used'), prefix='org.zfsbackup:',
                problem='expected 3 columns, got 2')

    def test_a_bogus_source_on_a_filtered_out_row_still_raises(self, feed):
        refused(feed, ('p', 'compression', 'bogus'), ('p', 'org.zfsbackup:config', 'local'), prefix='org.zfsbackup:',
                problem='invalid source')

    def test_a_duplicate_among_filtered_out_rows_still_raises(self, feed):
        refused(feed, ('p', 'compression', 'default'), ('p', 'compression', 'default'), prefix='org.zfsbackup:',
                problem='property')

    def test_an_out_of_scope_filtered_out_row_still_raises(self, feed):
        refused(feed, ('q', 'compression', 'default'), ds=Filesystem('p'), prefix='org.zfsbackup:',
                problem='name outside the requested objects')

    def test_a_malformed_row_after_filtered_out_rows_raises(self, feed):
        refused(feed, *self.ROWS, ('p/c', 'org.zfsbackup:x', 'Local'), prefix='org.zfsbackup:',
                problem='invalid source')

    @pytest.mark.parametrize('prefix', ['a b', '',
                                        5, 0, False, {'a': 1}.keys(),
                                        ['org.', 5], ['ok', 'a b'], ['x y'], 'org.t:,x', 'p/x', b'org.',
                                        '\u00fc', 'a\n', ['org.', '']])
    def test_an_invalid_prefix_is_a_value_error_before_exec(self, feed, prefix):
        with pytest.raises(ValueError):
            zfs.get.property_sources(prefix=prefix)
        assert feed.calls == []


# ---------------------------------------------------------------------------------------------------------------------
# argument errors
# ---------------------------------------------------------------------------------------------------------------------

class TestArgumentErrors:
    @pytest.mark.parametrize('kwargs', [
        {'types': 'bogus'},
        {'types': ['filesystem', 'nope']},
        {'types': 'filesystem,,volume'},
        {'sources': 'bogus'},
        {'sources': ['local', 'nope']},
        {'depth': -1},
        {'depth': True},
        {'depth': '2'},
        {'depth': None},
        {'depth': 1.0},
        {'ds': 'p/a'},
        {'ds': ['p/a']},
        {'ds': [Filesystem('p/a'), 'p/b']},
        {'ds': 5},
    ], ids=repr)
    def test_bad_arguments_raise_value_error_and_run_nothing(self, feed, kwargs):
        with pytest.raises(ValueError):
            zfs.get.property_sources(**kwargs)
        assert feed.calls == []

    def test_valid_edge_arguments_do_not_raise(self, feed):
        run(feed, types='ALL', sources='INHERITED,Local', depth=0, recursive=False, prefix='x')
        run(feed, ds=[], types=None, sources=None)
        assert len(feed.calls) == 2


# ---------------------------------------------------------------------------------------------------------------------
# malformed rows
# ---------------------------------------------------------------------------------------------------------------------

class TestMalformedRows:
    @pytest.mark.parametrize('row, problem', [
        (('p', 'compression'), 'expected 3 columns, got 2'),
        (('p', 'compression', 'default', 'x'), 'expected 3 columns, got 4'),
        (('p', 'compression', 'default', 'x', 'y'), 'expected 3 columns, got 5'),
        (('p',), 'expected 3 columns, got 1'),
        ('p\tcompression\tdefault\t', 'expected 3 columns, got 4'),
        ('\tcompression\tdefault', 'empty name'),
        (('p/a\rb', 'compression', 'default'), 'a control character or an invalid byte in a name'),
        (('p/a\x01b', 'compression', 'default'), 'a control character or an invalid byte in a name'),
        (('p/a\x7fb', 'compression', 'default'), 'a control character or an invalid byte in a name'),
        (('p/a\x00b', 'compression', 'default'), 'a control character or an invalid byte in a name'),
        (('p/\udcff', 'compression', 'default'), 'a control character or an invalid byte in a name'),
        (('p/' + 'a' * 254, 'compression', 'default'), 'a name longer than 255 bytes'),
        (('p/' + '\u00e9' * 128, 'compression', 'default'), 'a name longer than 255 bytes'),
        (('p', 'Bad Prop', 'default'), 'invalid property name'),
        (('p', '', 'default'), 'invalid property name'),
        (('p', 'COMPRESSION', 'default'), 'invalid property name'),
        (('p', 'x y', 'default'), 'invalid property name'),
        (('p', 'a/b', 'default'), 'invalid property name'),
        (('p', 'org.t:v\r', 'default'), 'invalid property name'),
        (('p', 'p' * 256 + ':x', 'default'), 'invalid property name'),
        (('p', 'compression', 'bogus'), 'invalid source'),
        (('p', 'compression', ''), 'invalid source'),
        (('p', 'compression', 'Local'), 'invalid source'),
        (('p', 'compression', 'LOCAL'), 'invalid source'),
        (('p', 'compression', 'local '), 'invalid source'),
        (('p', 'compression', 'local\r'), 'invalid source'),
        (('p', 'compression', 'inherited'), 'invalid source'),
        (('p', 'compression', 'inherited from'), 'invalid source'),
        (('p', 'compression', 'inherited from '), 'invalid source'),
        (('p', 'compression', 'inherited from p@s'), 'invalid source'),
        (('p', 'compression', 'inherited from p#b'), 'invalid source'),
        (('p', 'compression', 'inherited from p\x01'), 'invalid source'),
        (('p', 'compression', 'inherited from p/\udcff'), 'invalid source'),
        (('p', 'compression', 'inherited from ' + 'a' * 256), 'invalid source'),
        (('p', 'compression', 'inheritedfrom p'), 'invalid source'),
        (('p', 'compression', 'default from p'), 'invalid source'),
    ], ids=lambda x: repr(x)[:60])
    def test_each_malformed_row_raises_and_names_its_problem(self, feed, row, problem):
        message = refused(feed, row, problem=problem)
        assert 'could not be trusted' in message

    def test_the_bad_row_is_quoted(self, feed):
        message = refused(feed, ('p', 'compression', 'bogus-source'))
        assert 'bogus-source' in message

    def test_a_good_row_next_to_a_bad_one_does_not_save_the_result(self, feed):
        refused(feed, ROW, ('p', 'used', 'nonsense'), ('p', 'org.t:v', 'local'), problem='invalid source')

    def test_a_forged_four_column_value_row_is_refused(self, feed):
        """What a value-bearing read would let through: a `name,property,value,source` row."""
        refused(feed, ('p', 'org.t:v', 'x', 'local'), problem='expected 3 columns, got 4')

    def test_a_line_that_is_all_garbage(self, feed):
        refused(feed, 'this is not a row at all', problem='expected 3 columns, got 1')

    def test_the_quoted_row_is_clipped(self, feed):
        message = refused(feed, 'x' * 5000)
        assert len(message) < 1500

    def test_url_credentials_in_a_quoted_row_are_hidden(self, feed):
        message = refused(feed, ('p', 'org.t:v', 'https://user:secret@host/path'))
        assert 'secret' not in message


class TestCountAndFirstProblem:
    def test_several_bad_rows_are_counted_and_the_first_is_named(self, feed):
        message = refused(feed, ROW, ('p', 'used'), ('p', 'x y', 'local'), ('p', 'used', 'bogus'), count=3,
                          problem='expected 3 columns, got 2')
        assert 'bogus' not in message and 'x y' not in message

    def test_the_first_is_the_first_in_output_order(self, feed):
        refused(feed, ('p', 'used', 'bogus'), ('p', 'used'), count=2, problem='invalid source')

    def test_one_row_is_singular(self, feed):
        assert '1 row refused' in refused(feed, ('p', 'used', 'bogus'), count=1)

    def test_bad_rows_do_not_split_an_object_apart(self, feed):
        # a refused row in the middle of an object's rows is counted once, not as a second "non-contiguous" object
        refused(feed, ('p', 'a', 'local'), ('p', 'used', 'bogus'), ('p', 'b', 'local'), count=1, problem='invalid source')


# ---------------------------------------------------------------------------------------------------------------------
# scope and type
# ---------------------------------------------------------------------------------------------------------------------

class TestScope:
    A = Filesystem('p/a')

    def test_the_object_itself_is_in_scope(self, feed):
        assert len(run(feed, ('p/a', 'compression', 'local'), ds=self.A)) == 1

    def test_a_row_outside_the_requested_dataset_is_refused(self, feed):
        refused(feed, ('q/a', 'compression', 'local'), ds=self.A, problem='name outside the requested objects')

    def test_the_parent_is_outside_the_scope(self, feed):
        refused(feed, ('p', 'compression', 'local'), ds=self.A, recursive=True, problem='name outside')

    def test_a_string_prefix_sibling_is_outside_the_scope(self, feed):
        refused(feed, ('p/ab', 'compression', 'local'), ds=self.A, recursive=True,
                problem='name outside the requested objects')

    def test_a_string_prefix_sibling_snapshot_is_outside_the_scope(self, feed):
        refused(feed, ('p/ab@s', 'compression', 'local'), ds=self.A, recursive=True, types='all', problem='name outside')

    def test_a_grandchild_without_recursion_is_refused(self, feed):
        refused(feed, ('p/a/b/c', 'compression', 'local'), ds=self.A, problem='name outside')

    def test_a_child_without_recursion_is_refused(self, feed):
        refused(feed, ('p/a/b', 'compression', 'local'), ds=self.A, problem='name outside')

    def test_descendants_with_recursion_are_in_scope(self, feed):
        rows = [('p/a', 'compression', 'local'), ('p/a/b', 'compression', 'inherited from p/a'),
                ('p/a/b/c', 'compression', 'inherited from p/a')]
        assert len(run(feed, *rows, ds=self.A, recursive=True)) == 3

    def test_a_descendant_beyond_depth_one_is_refused(self, feed):
        refused(feed, ('p/a/b/c', 'compression', 'local'), ds=self.A, depth=1, problem='name outside')

    def test_a_descendant_within_depth_one_is_kept(self, feed):
        assert len(run(feed, ('p/a', 'x', 'local'), ('p/a/b', 'x', 'local'), ds=self.A, depth=1)) == 2

    def test_depth_two_reaches_the_grandchild_but_not_further(self, feed):
        assert len(run(feed, ('p/a/b/c', 'x', 'local'), ds=self.A, depth=2)) == 1
        refused(feed, ('p/a/b/c/d', 'x', 'local'), ds=self.A, depth=2, problem='name outside')

    def test_a_snapshot_of_the_requested_dataset_is_in_scope(self, feed):
        assert run(feed, ('p/a@s', 'x', 'local'), ds=self.A, types='snapshot')[0].name == 'p/a@s'

    def test_a_snapshot_of_a_child_needs_recursion(self, feed):
        refused(feed, ('p/a/b@s', 'x', 'local'), ds=self.A, types='snapshot', problem='name outside')
        assert len(run(feed, ('p/a/b@s', 'x', 'local'), ds=self.A, types='snapshot', recursive=True)) == 1

    def test_several_roots(self, feed):
        rows = [('p/a', 'x', 'local'), ('p/b', 'x', 'local')]
        assert len(run(feed, *rows, ds=[self.A, Filesystem('p/b')])) == 2
        refused(feed, ('p/c', 'x', 'local'), ds=[self.A, Filesystem('p/b')], problem='name outside')

    def test_no_dataset_means_every_pool(self, feed):
        assert len(run(feed, ('p', 'x', 'local'), ('q/r/s', 'x', 'local'))) == 2

    def test_no_dataset_with_depth_limits_the_depth_from_the_pool(self, feed):
        assert len(run(feed, ('p', 'x', 'local'), ('p/a', 'x', 'local'), depth=1)) == 2
        refused(feed, ('p/a/b', 'x', 'local'), depth=1, problem='name outside')

    def test_a_snapshot_root_matches_the_same_short_name_on_descendants_with_recursion(self, feed):
        root = Snapshot.from_name('p/a@s')
        assert len(run(feed, ('p/a@s', 'x', 'local'), ('p/a/b@s', 'x', 'local'), ds=root, types='snapshot',
                       recursive=True)) == 2
        refused(feed, ('p/a/b@other', 'x', 'local'), ds=root, types='snapshot', recursive=True, problem='name outside')


class TestTypes:
    def test_a_snapshot_row_when_only_filesystems_were_requested(self, feed):
        refused(feed, ('p@s', 'x', 'local'), types='filesystem', problem='a snapshot was not requested')

    def test_a_bookmark_row_when_only_filesystems_were_requested(self, feed):
        refused(feed, ('p#b', 'x', 'local'), types='filesystem', problem='a bookmark was not requested')

    def test_a_plain_row_when_only_snapshots_were_requested(self, feed):
        refused(feed, ('p', 'x', 'local'), types='snapshot', problem='a dataset was not requested')

    def test_a_plain_row_when_only_bookmarks_were_requested(self, feed):
        refused(feed, ('p', 'x', 'local'), types='bookmark', problem='a dataset was not requested')

    def test_a_snapshot_row_when_only_bookmarks_were_requested(self, feed):
        refused(feed, ('p@s', 'x', 'local'), types='bookmark', problem='a snapshot was not requested')

    def test_a_bookmark_row_when_only_snapshots_were_requested(self, feed):
        refused(feed, ('p#b', 'x', 'local'), types='snapshot', problem='a bookmark was not requested')

    @pytest.mark.parametrize('types', ['filesystem', 'volume', 'filesystem,volume'])
    def test_plain_rows_pass_for_datasets(self, feed, types):
        assert len(run(feed, ('p/a', 'x', 'local'), types=types)) == 1

    @pytest.mark.parametrize('types', ['all', 'snapshot,bookmark,filesystem', ['ALL'], None])
    def test_every_shape_passes_when_requested(self, feed, types):
        rows = [('p', 'x', 'local'), ('p@s', 'x', 'local'), ('p#b', 'x', 'local')]
        assert len(run(feed, *rows, types=types)) == 3

    def test_types_are_checked_case_insensitively(self, feed):
        assert len(run(feed, ('p@s', 'x', 'local'), types='Snapshot')) == 1


# ---------------------------------------------------------------------------------------------------------------------
# duplicates and contiguity
# ---------------------------------------------------------------------------------------------------------------------

class TestDuplicatesAndContiguity:
    def test_the_same_property_twice_for_one_object_is_refused(self, feed):
        refused(feed, ('p', 'org.t:v', 'local'), ('p', 'org.t:v', 'local'), problem="property 'org.t:v' appears twice")

    def test_the_same_property_twice_with_another_in_between_is_refused(self, feed):
        refused(feed, ('p', 'a', 'local'), ('p', 'b', 'local'), ('p', 'a', 'local'), problem="property 'a' appears twice")

    def test_a_returning_object_is_refused(self, feed):
        refused(feed, ('p/a', 'x', 'local'), ('p/b', 'x', 'local'), ('p/a', 'y', 'local'), problem="the rows of 'p/a' are not contiguous")

    def test_interleaved_objects_with_the_same_property_are_refused(self, feed):
        message = refused(feed, ('p/a', 'x', 'local'), ('p/b', 'x', 'local'), ('p/a', 'x', 'local'))
        assert 'not contiguous' in message

    def test_a_returning_object_is_refused_even_after_a_third_one(self, feed):
        refused(feed, ('p/a', 'x', 'local'), ('p/b', 'x', 'local'), ('p/c', 'x', 'local'), ('p/a', 'y', 'local'),
                problem="the rows of 'p/a' are not contiguous")

    def test_two_objects_may_have_the_same_property(self, feed):
        rows = [('p/a', 'x', 'local'), ('p/b', 'x', 'default'), ('p/c', 'x', 'received')]
        assert run(feed, *rows) == [PropertySource(*r) for r in rows]

    def test_a_snapshot_and_its_dataset_are_different_objects(self, feed):
        rows = [('p/a', 'x', 'local'), ('p/a@s', 'x', 'inherited from p/a'), ('p/a#b', 'x', '-')]
        assert len(run(feed, *rows)) == 3

    def test_a_name_that_differs_only_in_case_is_a_different_object(self, feed):
        assert len(run(feed, ('p/a', 'x', 'local'), ('p/A', 'x', 'local'))) == 2


# ---------------------------------------------------------------------------------------------------------------------
# failure precedence
# ---------------------------------------------------------------------------------------------------------------------

class TestFailurePrecedence:
    def test_a_non_zero_exit_is_a_bare_exception_with_stderr(self, feed):
        feed.set(b'', b'cannot open \'p/nope\': dataset does not exist\n', 1)
        with pytest.raises(Exception) as excinfo:
            zfs.get.property_sources(ds=Filesystem('p/nope'))
        assert type(excinfo.value) is Exception
        assert "cannot open 'p/nope': dataset does not exist" in str(excinfo.value).lower()

    def test_a_failure_takes_precedence_over_malformed_rows(self, feed):
        feed.set(text(('p', 'used'), ('p', 'x', 'bogus')), b'boom: pool I/O is currently suspended\n', 1)
        with pytest.raises(Exception) as excinfo:
            zfs.get.property_sources()
        assert not isinstance(excinfo.value, (ZFSOutputError, ValueError))
        assert 'boom' in str(excinfo.value).lower()          # the library capitalises the first letter

    def test_a_failure_with_good_rows_returns_nothing(self, feed):
        feed.set(text(ROW), b'', 1)
        with pytest.raises(Exception, match='exit code 1'):
            zfs.get.property_sources()

    def test_a_failure_without_stderr_describes_the_exit(self, feed):
        feed.set(b'', b'', 2)
        with pytest.raises(Exception, match='exit code 2'):
            zfs.get.property_sources()


# ---------------------------------------------------------------------------------------------------------------------
# real children (mocked zfs binary): drained, reaped, timeouts
# ---------------------------------------------------------------------------------------------------------------------

class _Redirect:
    """Makes every `Popen` run `sys.executable -c SCRIPT <argv after the binary>` instead, recording the children."""

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


# a bad row first, then enough good rows to fill any pipe several times over, then exit 0
DRAIN = (
    'import sys\n'
    'out = sys.stdout\n'
    'out.write("p\\tcompression\\tbogus\\n")\n'
    'for i in range(20000):\n'
    '    out.write("p\\torg.t:p%d\\tlocal\\n" % i)\n'
    'out.flush()\n'
)
FAIL_AFTER_BAD_ROWS = (
    'import sys\n'
    'sys.stdout.write("p\\tcompression\\n"); sys.stdout.flush()\n'
    'sys.stderr.write("zfs: it broke\\n"); sys.exit(1)\n'
)
ROW_THEN_SLEEP = (
    'import sys, time\n'
    'sys.stdout.write("p\\tcompression\\tdefault\\n"); sys.stdout.flush()\n'
    'time.sleep(120)\n'
)
TWO_ROWS = (
    'import sys\n'
    'sys.stdout.write("p\\tcompression\\tdefault\\np\\tused\\t-\\n")\n'
)


@pytest.mark.subprocess
class TestRealChildren:
    def test_a_bad_row_raises_only_after_zfs_has_exited_and_its_output_was_drained(self, redirect):
        kids = redirect(DRAIN)
        with pytest.raises(ZFSOutputError, match='1 row refused'):
            zfs.get.property_sources()
        (proc,) = kids.procs
        assert proc.returncode == 0       # not terminated for being abandoned: it wrote all 20000 rows and exited
        assert kids.argvs[0][1:] == ['get', '-H', '-o', 'name,property,source', '-t', 'all', 'all']

    def test_a_failing_child_is_reaped_and_its_stderr_wins(self, redirect):
        kids = redirect(FAIL_AFTER_BAD_ROWS)
        with pytest.raises(Exception) as excinfo:
            zfs.get.property_sources()
        assert type(excinfo.value) is Exception and 'it broke' in str(excinfo.value)
        (proc,) = kids.procs
        assert proc.poll() == 1

    def test_a_good_run_returns_rows_and_the_child_is_reaped(self, redirect):
        kids = redirect(TWO_ROWS)
        assert zfs.get.property_sources() == [PropertySource('p', 'compression', 'default'),
                                              PropertySource('p', 'used', '-')]
        assert kids.procs[0].poll() == 0

    def test_a_generous_timeout_does_not_disturb_a_fast_call(self, redirect):
        redirect(TWO_ROWS)
        with zfs.timeout(30):
            assert len(zfs.get.property_sources()) == 2

    def test_timeout_ends_a_hung_call_and_reaps_the_child(self, redirect):
        kids = redirect(ROW_THEN_SLEEP)
        with zfs.timeout(1):
            with pytest.raises(ZFSTimeoutError):
                zfs.get.property_sources()
        (proc,) = kids.procs
        assert proc.poll() is not None

    def test_timeout_is_not_a_value_error_or_output_error(self):
        assert not issubclass(ZFSTimeoutError, (ValueError, ZFSOutputError))


# ---------------------------------------------------------------------------------------------------------------------
# why it cannot be forged
# ---------------------------------------------------------------------------------------------------------------------

# A `zfs get -H` that honours `-o` over a model with one hostile value: `p/att`'s `org.x:v` holds a newline followed by a
# whole forged row for `p/vic`. Printing the value column turns that into extra rows; the model prints nothing else
# that can hold a newline. (The real binary's behaviour is checked in tests/test_real_zfs.py.)
FAKE_GET = (
    'import sys\n'
    'args = sys.argv[1:]\n'
    'cols = args[args.index("-o") + 1].split(",")\n'
    'HOSTILE = "a\\np/vic\\torg.x:v\\tFORGED\\tlocal"\n'
    'objects = [\n'
    '    ("p/att", [("type", "filesystem", "-"), ("org.x:v", HOSTILE, "local")]),\n'
    '    ("p/vic", [("type", "filesystem", "-"), ("org.x:v", "real", "local")]),\n'
    ']\n'
    'for name, props in objects:\n'
    '    for prop, value, source in props:\n'
    '        row = {"name": name, "property": prop, "value": value, "received": "-", "source": source}\n'
    '        sys.stdout.write("\\t".join(row[c] for c in cols) + "\\n")\n'
)


@pytest.mark.subprocess
class TestForgeryIsImpossible:
    def test_the_value_bearing_read_is_forged_and_the_source_read_is_not(self, redirect):
        redirect(FAKE_GET)
        raw = subprocess.run([sys.executable, '-c', FAKE_GET, 'get', '-H', '-o', 'name,property,value,source'],
                             capture_output=True, text=True).stdout
        assert '\np/vic\torg.x:v\tFORGED\tlocal' in raw           # the attack works when the value is printed
        plain = subprocess.run([sys.executable, '-c', FAKE_GET, 'get', '-H', '-o', 'name,property,source'],
                               capture_output=True, text=True).stdout
        assert 'FORGED' not in plain and plain.count('\n') == 4
        assert all(line.count('\t') == 2 for line in plain.splitlines())

    def test_property_sources_returns_the_true_rows_and_never_the_hostile_value(self, redirect):
        redirect(FAKE_GET)
        got = zfs.get.property_sources(recursive=True)
        assert got == [
            PropertySource('p/att', 'type', '-'), PropertySource('p/att', 'org.x:v', 'local'),
            PropertySource('p/vic', 'type', '-'), PropertySource('p/vic', 'org.x:v', 'local'),
        ]
        assert not any('FORGED' in field or 'victim' in field for row in got for field in row)

    def test_the_same_data_read_with_values_is_not_trusted(self, redirect):
        redirect(FAKE_GET)
        res = zfs.get.rows(recursive=True, received=False, verify_names=False)
        assert not res.complete          # the forged rows are caught there; here they cannot exist at all

    def test_dash_s_is_safe_here(self, redirect):
        """`-s` drops the `type` rows a value-bearing read needs; with no value to frame it is allowed."""
        kids = redirect(FAKE_GET)
        got = zfs.get.property_sources(sources=['local'])
        assert '-s' in kids.argvs[0] and kids.argvs[0][kids.argvs[0].index('-s') + 1] == 'local'
        assert len(got) == 4


# ---------------------------------------------------------------------------------------------------------------------
# exports
# ---------------------------------------------------------------------------------------------------------------------

class TestExports:
    def test_property_source_is_exported_from_the_package(self):
        assert libzfseasy.PropertySource is zfs_module.PropertySource

    def test_the_singleton_reaches_it(self):
        assert callable(libzfseasy.get.property_sources)
        assert isinstance(libzfseasy.get, GetCommand)
        assert libzfseasy.get.property_sources.__func__ is GetCommand.property_sources.__func__

    def test_it_is_callable_on_the_class_too(self, feed):
        feed.set(text(ROW))
        assert GetCommand.property_sources() == [PropertySource(*ROW)]

    def test_the_signature(self):
        import inspect
        params = inspect.signature(GetCommand.property_sources).parameters
        assert list(params) == ['ds', 'types', 'sources', 'recursive', 'depth', 'prefix']
        assert [p.default for p in params.values()] == [None, 'all', 'all', False, 0, None]
