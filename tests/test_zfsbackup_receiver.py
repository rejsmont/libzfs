"""Direct unit tests of `zfsbackup.receiver` (plan item 1.2): the receive-side helpers the HTTP API
and, later, the local destination share.

Conventions pinned here:

- every function that runs `zfs` takes `zfs_call` first, called `zfs_call(fn, *args, **kwargs)`, and
  whatever it raises (`ZFSTimeoutError` included) propagates;
- `lib=` (keyword-only, default `libzfseasy`) is the module the commands are looked up on;
- `start_receive` is the one exception: it never goes through a `zfs_call` (a receive is never timed);
- the module logs nothing and holds no state.

No test reaches a real `zfs`: the commands are a recording fake (`FakeLib`) that refuses to run
outside a `zfs_call` (`Calls`), or the real `zfs.receive.abort` over the committed OpenZFS captures
with `Popen` mocked.
"""

import inspect
import logging
from types import SimpleNamespace

import pytest

import libzfseasy
from libzfseasy import ZFSTimeoutError
from libzfseasy import zfs as zfs_module
from libzfseasy.types import Dataset, Filesystem

from _openzfs_fixtures import VERSIONS, load_section
from zfsbackup import receiver
from zfsbackup.backup_manager import (
    PROP_CLIENT_ID, PROP_CONFIG, PROP_SOURCE_DATASET, read_zfsbackup_properties,
)
from zfsbackup.receiver import (
    ABORT_ALREADY_CLEAN, BadDatasetName, OwnerVerdict, RECEIVE_PROPERTIES_FILESYSTEM,
    RECEIVE_PROPERTIES_VOLUME, SERVER_OWNED_PROPERTIES, abort_partial_receive,
    ancestor_holds_partial_receive, client_dataset_problem, create_ancestors, local_client_id,
    may_force, owner_verdict, ownership_conflict, receive_properties, resume_token,
    server_copy_kind, server_dataset, start_receive,
)

pytestmark = pytest.mark.unit

TARGET = 'backup/srv'
CLIENT = 'client-1'
SERVER_DS = f'{TARGET}/{CLIENT}/tank/data'


# ---------------------------------------------------------------------------
# Doubles
# ---------------------------------------------------------------------------


class Boom(Exception):
    """A zfs failure that is not a timeout."""


class Calls:
    """A `zfs_call` that records `(name, args, kwargs)` and runs the call. `FakeLib` functions
    insist on being run through it (`Calls.active`), which pins "zfs_call is used for every zfs
    call"."""

    active = False

    def __init__(self, record):
        self.record = record

    def __call__(self, fn, *args, **kwargs):
        self.record.append((getattr(fn, '__name__', repr(fn)), args, kwargs))
        Calls.active = True
        try:
            return fn(*args, **kwargs)
        finally:
            Calls.active = False


class FakeLib:
    """The slice of `libzfseasy` the receiver calls, with scripted answers.

    Each command is a named function (so `Calls` can record it); a value in `fail` is raised, a
    value in `answers` returned. Calls outside a `zfs_call` fail the test."""

    def __init__(self, **answers):
        self.answers = {
            'exists': True, 'list': [], 'value': {}, 'create': None, 'abort': None,
            'filesystem': 'HANDLE',
        }
        self.answers.update(answers)
        self.fail = {}
        self.log = []                       # what the commands themselves saw
        self.outside = []                   # command names run outside a zfs_call

        def exists(ds):
            return self._run('exists', ds)

        def list_(*args, **kwargs):
            return self._run('list', *args, **kwargs)

        def value(ds, prop, **kwargs):
            return self._run('value', ds, prop, **kwargs)

        def create(ds, **kwargs):
            return self._run('create', ds, **kwargs)

        def abort(ds):
            return self._run('abort', ds)

        def filesystem(ds, **kwargs):
            return self._run('filesystem', ds, **kwargs)

        self.exists = exists
        self.list = list_
        self.get = SimpleNamespace(value=value)
        self.create = create
        self.receive = SimpleNamespace(abort=abort, filesystem=filesystem)

    def _run(self, name, *args, **kwargs):
        self.log.append((name, args, kwargs))
        if not Calls.active and name != 'filesystem':
            self.outside.append(name)
        if name in self.fail:
            raise self.fail[name]
        answer = self.answers[name]
        if name == 'value' and isinstance(answer, dict):
            return answer.get(args[1])
        if callable(answer):
            return answer(*args, **kwargs)
        return answer

    def names(self):
        return [entry[0] for entry in self.log]


@pytest.fixture
def record():
    return []


@pytest.fixture
def zc(record):
    return Calls(record)


def called(record):
    return [entry[0] for entry in record]


class Row(dict):
    """A `zfs.list` result: `row['receive_resume_token']` is the property."""


def token_row(value):
    return [Row(receive_resume_token=value)]


# ---------------------------------------------------------------------------
# Module hygiene
# ---------------------------------------------------------------------------


class TestModuleShape:
    def test_every_exported_name_exists(self):
        for name in receiver.__all__:
            assert hasattr(receiver, name), name

    def test_every_zfs_running_function_takes_zfs_call_first(self):
        for name in (
            'server_copy_kind', 'resume_token', 'local_client_id', 'ownership_conflict',
            'ancestor_holds_partial_receive', 'create_ancestors', 'abort_partial_receive',
            'may_force',
        ):
            params = list(inspect.signature(getattr(receiver, name)).parameters.values())
            assert params[0].name == 'zfs_call', name

    def test_lib_is_keyword_only_and_defaults_to_none(self):
        for name in (
            'server_copy_kind', 'resume_token', 'local_client_id', 'ownership_conflict',
            'ancestor_holds_partial_receive', 'create_ancestors', 'abort_partial_receive',
            'may_force', 'start_receive',
        ):
            param = inspect.signature(getattr(receiver, name)).parameters['lib']
            assert param.kind is inspect.Parameter.KEYWORD_ONLY, name
            assert param.default is None, name

    def test_start_receive_has_no_zfs_call_parameter(self):
        assert 'zfs_call' not in inspect.signature(start_receive).parameters

    def test_ownership_conflict_read_properties_is_keyword_only_and_defaults(self):
        param = inspect.signature(ownership_conflict).parameters['read_properties']
        assert param.kind is inspect.Parameter.KEYWORD_ONLY
        assert param.default is read_zfsbackup_properties

    @staticmethod
    def _imported_modules():
        import ast
        tree = ast.parse(inspect.getsource(receiver))
        mods = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                mods.update(a.name for a in node.names)
            elif isinstance(node, ast.ImportFrom):
                mods.add(node.module)
                mods.update(f'{node.module}.{a.name}' for a in node.names)
        return mods

    def test_the_module_imports_nothing_from_the_api(self):
        mods = self._imported_modules()
        assert 'zfsbackup.api' not in mods
        assert not any(m == 'zfsbackup.api' or m.startswith('zfsbackup.api.') for m in mods)

    def test_the_module_logs_nothing_and_holds_no_state(self):
        mods = self._imported_modules()
        assert 'logging' not in mods
        assert not hasattr(receiver, 'logger') and not hasattr(receiver, 'logging')
        # module-level names that are mutable containers: only the documented constants
        mutable = {
            n for n, v in vars(receiver).items()
            if not n.startswith('_') and isinstance(v, (dict, list, set))
        }
        assert mutable == {'RECEIVE_PROPERTIES_FILESYSTEM', 'RECEIVE_PROPERTIES_VOLUME'}

    def test_the_api_reexports_the_same_objects(self):
        from zfsbackup import api
        assert api.RECEIVE_PROPERTIES_FILESYSTEM is RECEIVE_PROPERTIES_FILESYSTEM
        assert api.RECEIVE_PROPERTIES_VOLUME is RECEIVE_PROPERTIES_VOLUME
        assert api.SERVER_OWNED_PROPERTIES is SERVER_OWNED_PROPERTIES
        assert api._ABORT_ALREADY_CLEAN is ABORT_ALREADY_CLEAN
        assert api._BadDatasetName is BadDatasetName
        assert api.client_dataset_problem is client_dataset_problem

    def test_no_function_logs(self, caplog, zc):
        """One pass over the functions with failures and refusals: nothing reaches any logger."""
        lib = FakeLib(value={PROP_CLIENT_ID: None})
        with caplog.at_level(logging.DEBUG):
            with pytest.raises(BadDatasetName):
                server_dataset(TARGET, CLIENT, 'bad\nname')
            owner_verdict(SERVER_DS, None, CLIENT)
            owner_verdict(SERVER_DS, 'other', CLIENT)
            ownership_conflict(zc, SERVER_DS, 'tank/data', CLIENT,
                               read_properties=lambda ds: {}, lib=lib)
            abort_partial_receive(zc, SERVER_DS, lib=FakeLib())
        assert caplog.records == []


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------


class TestConstants:
    def test_owned_properties_are_the_three_server_owned_names(self):
        assert SERVER_OWNED_PROPERTIES == (PROP_CONFIG, PROP_CLIENT_ID, PROP_SOURCE_DATASET)

    def test_forced_sets_are_disjoint_from_the_owned_names(self):
        assert not set(RECEIVE_PROPERTIES_FILESYSTEM) & set(SERVER_OWNED_PROPERTIES)
        assert not set(RECEIVE_PROPERTIES_VOLUME) & set(SERVER_OWNED_PROPERTIES)

    def test_a_volume_set_never_carries_a_filesystem_only_property(self):
        assert set(RECEIVE_PROPERTIES_VOLUME) == {'readonly', 'volmode'}
        assert 'canmount' not in RECEIVE_PROPERTIES_VOLUME
        assert 'mountpoint' not in RECEIVE_PROPERTIES_VOLUME

    def test_the_filesystem_set_never_mounts_or_shares(self):
        assert RECEIVE_PROPERTIES_FILESYSTEM['canmount'] == 'noauto'
        assert RECEIVE_PROPERTIES_FILESYSTEM['mountpoint'] == 'none'
        assert RECEIVE_PROPERTIES_FILESYSTEM['readonly'] == 'on'
        for name in ('sharenfs', 'sharesmb', 'setuid', 'exec', 'devices'):
            assert RECEIVE_PROPERTIES_FILESYSTEM[name] == 'off', name

    def test_already_clean_wordings(self):
        assert ABORT_ALREADY_CLEAN == (
            'does not have any resumable receive state',
            'dataset does not exist',
        )


# ---------------------------------------------------------------------------
# server_dataset / client_dataset_problem
# ---------------------------------------------------------------------------


class TestServerDataset:
    @pytest.mark.parametrize('client_dataset, expected', [
        ('tank/data', f'{TARGET}/{CLIENT}/tank/data'),
        ('tank', f'{TARGET}/{CLIENT}/tank'),
        ('tank/a.b', f'{TARGET}/{CLIENT}/tank/a.b'),          # dots are fine
        ('tank/a.b/c.d.e', f'{TARGET}/{CLIENT}/tank/a.b/c.d.e'),
        ('tank/a b', f'{TARGET}/{CLIENT}/tank/a b'),          # ZFS allows a space
        ('tank/x_y-z:w', f'{TARGET}/{CLIENT}/tank/x_y-z:w'),
    ])
    def test_valid_names(self, client_dataset, expected):
        assert server_dataset(TARGET, CLIENT, client_dataset) == expected

    def test_the_pool_is_kept_so_two_pools_never_meet(self):
        assert server_dataset(TARGET, CLIENT, 'a/x') != server_dataset(TARGET, CLIENT, 'b/x')

    def test_the_result_is_a_valid_filesystem_name(self):
        Filesystem(server_dataset(TARGET, CLIENT, 'tank/data'))

    @pytest.mark.parametrize('bad', [
        '', None, 5, b'tank', '1tank/x', 'tank/', '/tank', 'tank//x', 'tank@snap', 'tank#mark',
        'tank/x%y', 'tank/a\nb', 'tank/a\rb', 'tank/a\tb', 'tank/a\0b',
    ])
    def test_invalid_client_names_raise_bad_dataset_name(self, bad):
        with pytest.raises(BadDatasetName):
            server_dataset(TARGET, CLIENT, bad)

    def test_bad_dataset_name_is_a_value_error(self):
        assert issubclass(BadDatasetName, ValueError)

    @pytest.mark.parametrize('char', ['\n', '\r', '\t', '\0'])
    def test_a_control_character_is_never_repeated_raw(self, char):
        with pytest.raises(BadDatasetName) as exc:
            server_dataset(TARGET, CLIENT, f'tank/a{char}b')
        text = str(exc.value)
        assert char not in text
        assert 'control character' in text

    def test_an_invalid_target_is_refused_naming_the_derived_name(self):
        with pytest.raises(BadDatasetName) as exc:
            server_dataset('bad target', CLIENT, 'tank/data')
        assert "'bad target/client-1/tank/data'" in str(exc.value)

    def test_an_invalid_target_error_has_no_chained_context(self):
        """`raise ... from None`: the library's own message is not chained."""
        with pytest.raises(BadDatasetName) as exc:
            server_dataset('bad target', CLIENT, 'tank/data')
        assert exc.value.__suppress_context__ is True
        assert exc.value.__cause__ is None

    def test_the_client_name_is_checked_before_the_derived_name_is_built(self, monkeypatch):
        """`Filesystem(name)` is never called for a refused client name, so its message (which
        repeats the raw name) is never produced."""
        seen = []

        def spy(name, *args, **kwargs):
            seen.append(name)
            return Filesystem(name, *args, **kwargs)

        monkeypatch.setattr(receiver, 'Filesystem', spy)
        with pytest.raises(BadDatasetName):
            server_dataset(TARGET, CLIENT, 'tank/a\nb')
        assert seen == []                                   # refused on the control character alone
        with pytest.raises(BadDatasetName):
            server_dataset(TARGET, CLIENT, 'tank//x')
        assert seen == ['tank//x']                          # the client name only, never the derived one
        seen.clear()
        assert server_dataset(TARGET, CLIENT, 'tank/ok') == f'{TARGET}/{CLIENT}/tank/ok'
        assert seen == ['tank/ok', f'{TARGET}/{CLIENT}/tank/ok']

    def test_a_valid_derived_name_is_not_enough_if_the_client_name_is_not(self):
        """`1tank/x` is a valid name below `<target>/<id>/` but not on its own."""
        Filesystem(f'{TARGET}/{CLIENT}/1tank/x')
        with pytest.raises(BadDatasetName):
            server_dataset(TARGET, CLIENT, '1tank/x')

    def test_the_client_problem_is_named_in_the_message(self):
        with pytest.raises(BadDatasetName) as exc:
            server_dataset(TARGET, CLIENT, '')
        assert 'empty client dataset name' in str(exc.value)
        with pytest.raises(BadDatasetName) as exc:
            server_dataset(TARGET, CLIENT, 'tank@x')
        assert 'invalid client dataset name' in str(exc.value)


class TestClientDatasetProblem:
    def test_none_for_a_good_name(self):
        assert client_dataset_problem('tank/a.b') is None

    def test_empty_and_non_string(self):
        for bad in ('', None, 0, [], b'x'):
            assert client_dataset_problem(bad) == 'empty client dataset name'

    @pytest.mark.parametrize('char', ['\n', '\r', '\t', '\0'])
    def test_control_characters(self, char):
        assert 'control character' in client_dataset_problem(f'a{char}b')

    def test_control_character_wins_over_invalid_name(self):
        assert 'control character' in client_dataset_problem('1x//\n')

    def test_invalid_name(self):
        assert client_dataset_problem('tank//x') == 'invalid client dataset name'


# ---------------------------------------------------------------------------
# server_copy_kind
# ---------------------------------------------------------------------------


class TestServerCopyKind:
    FS = Filesystem(SERVER_DS)

    def kind(self, zc, lib, fs=None):
        return server_copy_kind(zc, fs or self.FS, lib=lib)

    @pytest.mark.parametrize('encryption', [None, 'off'])
    def test_unencrypted_is_plain_and_asks_once(self, zc, record, encryption):
        lib = FakeLib(value={'encryption': encryption})
        assert self.kind(zc, lib) == 'plain'
        assert called(record) == ['value']
        assert record[0][1] == (self.FS, 'encryption')

    def test_encrypted_own_root_is_raw(self, zc, record):
        lib = FakeLib(value={'encryption': 'aes-256-gcm', 'encryptionroot': SERVER_DS})
        assert self.kind(zc, lib) == 'raw'
        assert [r[1][1] for r in record] == ['encryption', 'encryptionroot']

    @pytest.mark.parametrize('root', [f'{TARGET}', f'{TARGET}/{CLIENT}', f'{SERVER_DS}/child', '-', None])
    def test_encrypted_with_another_root_is_plain(self, zc, root):
        lib = FakeLib(value={'encryption': 'aes-256-gcm', 'encryptionroot': root})
        assert self.kind(zc, lib) == 'plain'

    def test_root_is_compared_to_the_dataset_name(self, zc):
        other = Filesystem(f'{TARGET}/{CLIENT}/tank/other')
        lib = FakeLib(value={'encryption': 'on', 'encryptionroot': SERVER_DS})
        assert self.kind(zc, lib, other) == 'plain'
        assert self.kind(zc, lib, self.FS) == 'raw'

    def test_everything_goes_through_zfs_call(self, zc):
        lib = FakeLib(value={'encryption': 'on', 'encryptionroot': SERVER_DS})
        self.kind(zc, lib)
        assert lib.outside == []

    @pytest.mark.parametrize('failing', ['encryption', 'encryptionroot'])
    def test_failures_propagate(self, zc, failing):
        values = {'encryption': 'on', 'encryptionroot': SERVER_DS}

        def value(ds, prop, **kw):
            if prop == failing:
                raise Boom(prop)
            return values[prop]

        lib = FakeLib(value=value)
        with pytest.raises(Boom):
            self.kind(zc, lib)

    def test_timeout_propagates_from_zfs_call(self, record):
        def timed_out(fn, *a, **k):
            raise ZFSTimeoutError('slow')

        with pytest.raises(ZFSTimeoutError):
            server_copy_kind(timed_out, self.FS, lib=FakeLib())


# ---------------------------------------------------------------------------
# resume_token
# ---------------------------------------------------------------------------


class TestResumeToken:
    def test_missing_dataset_is_none_and_nothing_else_is_asked(self, zc, record):
        lib = FakeLib(exists=False)
        assert resume_token(zc, SERVER_DS, lib=lib) is None
        assert called(record) == ['exists']
        assert lib.names() == ['exists']

    def test_exists_is_asked_with_a_dataset_object(self, zc, record):
        lib = FakeLib(exists=False)
        resume_token(zc, SERVER_DS, lib=lib)
        ds = record[0][1][0]
        assert isinstance(ds, Dataset) and ds.name == SERVER_DS

    def test_token_is_returned_and_list_arguments_are_exact(self, zc, record):
        lib = FakeLib(list=token_row('1-abc-def'))
        assert resume_token(zc, SERVER_DS, lib=lib) == '1-abc-def'
        assert called(record) == ['exists', 'list_']
        _, args, kwargs = record[1]
        assert args == ()
        assert kwargs['properties'] == ['receive_resume_token']
        assert kwargs['roots'].name == SERVER_DS
        assert set(kwargs) == {'roots', 'properties'}

    @pytest.mark.parametrize('value', ['none', None])
    def test_no_token_is_none(self, zc, value):
        lib = FakeLib(list=token_row(value))
        assert resume_token(zc, SERVER_DS, lib=lib) is None

    def test_empty_listing_is_none(self, zc):
        assert resume_token(zc, SERVER_DS, lib=FakeLib(list=[])) is None
        assert resume_token(zc, SERVER_DS, lib=FakeLib(list=None)) is None

    def test_a_property_object_is_stringified(self, zc):
        class Prop:
            def __str__(self):
                return 'tok-9'

        assert resume_token(zc, SERVER_DS, lib=FakeLib(list=token_row(Prop()))) == 'tok-9'

    def test_the_word_none_inside_a_token_is_not_none(self, zc):
        assert resume_token(zc, SERVER_DS, lib=FakeLib(list=token_row('none-1'))) == 'none-1'

    def test_only_the_first_row_counts(self, zc):
        lib = FakeLib(list=[Row(receive_resume_token='first'), Row(receive_resume_token='second')])
        assert resume_token(zc, SERVER_DS, lib=lib) == 'first'

    def test_uses_zfs_call_for_both_calls(self, zc):
        lib = FakeLib(list=token_row('t'))
        resume_token(zc, SERVER_DS, lib=lib)
        assert lib.outside == []

    @pytest.mark.parametrize('failing', ['exists', 'list'])
    def test_failures_propagate(self, zc, failing):
        lib = FakeLib(list=token_row('t'))
        lib.fail[failing] = Boom(failing)
        with pytest.raises(Boom):
            resume_token(zc, SERVER_DS, lib=lib)

    @pytest.mark.parametrize('failing', ['exists', 'list'])
    def test_timeouts_propagate(self, zc, failing):
        lib = FakeLib(list=token_row('t'))
        lib.fail[failing] = ZFSTimeoutError('slow')
        with pytest.raises(ZFSTimeoutError):
            resume_token(zc, SERVER_DS, lib=lib)

    def test_an_invalid_name_raises_before_any_call(self, zc, record):
        with pytest.raises(ValueError):
            resume_token(zc, 'bad//name', lib=FakeLib())
        assert record == []


# ---------------------------------------------------------------------------
# owner_verdict (pure)
# ---------------------------------------------------------------------------


class TestOwnerVerdict:
    def test_no_local_id_is_unowned_and_names_the_adopting_command(self):
        verdict = owner_verdict(SERVER_DS, None, CLIENT)
        assert verdict.unowned is True and verdict.confirmed is False
        assert f'server dataset {SERVER_DS} already exists on the server' in verdict.message
        assert 'refusing to adopt it' in verdict.message
        assert f'zfs set {PROP_CLIENT_ID}={CLIENT} {SERVER_DS}' in verdict.message

    def test_the_hint_quotes_a_dataset_with_a_space(self):
        verdict = owner_verdict(f'{TARGET}/{CLIENT}/tank/a b', None, CLIENT)
        assert f"zfs set {PROP_CLIENT_ID}={CLIENT} '{TARGET}/{CLIENT}/tank/a b'" in verdict.message

    def test_a_different_id_is_refused_without_naming_it(self):
        verdict = owner_verdict(SERVER_DS, 'client-2', CLIENT)
        assert verdict.unowned is False and verdict.confirmed is False
        assert "another client's" in verdict.message
        assert repr(CLIENT) in verdict.message
        assert 'client-2' not in verdict.message
        assert 'zfs set' not in verdict.message

    def test_the_same_id_is_confirmed(self):
        assert owner_verdict(SERVER_DS, CLIENT, CLIENT) == OwnerVerdict(None, False, True, False)

    def test_the_empty_string_is_a_different_id_not_unowned(self):
        verdict = owner_verdict(SERVER_DS, '', CLIENT)
        assert verdict.message is not None and verdict.unowned is False

    def test_unowned_and_confirmed_are_never_both_true(self):
        for local in (None, '', 'x', CLIENT):
            v = owner_verdict(SERVER_DS, local, CLIENT)
            assert not (v.unowned and v.confirmed)

    def test_namedtuple_defaults_and_fields(self):
        assert OwnerVerdict._fields == ('message', 'unowned', 'confirmed', 'source_mismatch')
        assert OwnerVerdict('m') == ('m', False, False, False)
        assert OwnerVerdict(None) == (None, False, False, False)

    def test_only_a_source_mismatch_sets_the_flag(self):
        assert owner_verdict(SERVER_DS, None, CLIENT).source_mismatch is False
        assert owner_verdict(SERVER_DS, 'client-2', CLIENT).source_mismatch is False
        assert owner_verdict(SERVER_DS, CLIENT, CLIENT).source_mismatch is False

    def test_it_is_pure(self, record):
        """No zfs call is possible: the function takes no `zfs_call`."""
        assert 'zfs_call' not in inspect.signature(owner_verdict).parameters


# ---------------------------------------------------------------------------
# local_client_id
# ---------------------------------------------------------------------------


class TestLocalClientId:
    DS = Dataset(SERVER_DS)

    def test_reads_the_local_value_only(self, zc, record):
        lib = FakeLib(value={PROP_CLIENT_ID: CLIENT})
        assert local_client_id(zc, self.DS, lib=lib) == CLIENT
        assert called(record) == ['value']
        _, args, kwargs = record[0]
        assert args == (self.DS, PROP_CLIENT_ID)
        assert kwargs == {'sources': 'local'}
        assert lib.outside == []

    def test_unset_is_none(self, zc):
        assert local_client_id(zc, self.DS, lib=FakeLib(value={})) is None

    def test_failures_and_timeouts_propagate(self, zc):
        lib = FakeLib()
        lib.fail['value'] = Boom('x')
        with pytest.raises(Boom):
            local_client_id(zc, self.DS, lib=lib)
        lib.fail['value'] = ZFSTimeoutError('slow')
        with pytest.raises(ZFSTimeoutError):
            local_client_id(zc, self.DS, lib=lib)


# ---------------------------------------------------------------------------
# ownership_conflict
# ---------------------------------------------------------------------------


class Props:
    """A `read_properties` double recording its calls."""

    def __init__(self, props=None, fail=None):
        self.props, self.fail, self.calls = props or {}, fail, []

    def __call__(self, ds, *args, **kwargs):
        self.calls.append((ds, args, kwargs))
        if self.fail:
            raise self.fail
        return self.props


class TestOwnershipConflict:
    def check(self, zc, lib, props, dataset='tank/data', client=CLIENT):
        reader = props if isinstance(props, Props) else Props(props)
        verdict = ownership_conflict(
            zc, SERVER_DS, dataset, client, read_properties=reader, lib=lib)
        return verdict, reader

    def test_a_missing_dataset_is_no_conflict_and_nothing_else_is_read(self, zc, record):
        lib = FakeLib(exists=False)
        verdict, reader = self.check(zc, lib, {})
        assert verdict == OwnerVerdict(None, False, False)
        assert called(record) == ['exists']
        assert reader.calls == []
        assert lib.names() == ['exists']

    def test_exists_gets_a_dataset_object(self, zc, record):
        self.check(zc, FakeLib(exists=False), {})
        assert isinstance(record[0][1][0], Dataset) and record[0][1][0].name == SERVER_DS

    def test_a_recorded_other_id_wins_without_reading_the_local_value(self, zc, record):
        lib = FakeLib(value={PROP_CLIENT_ID: CLIENT})        # would confirm, if asked
        verdict, _ = self.check(zc, lib, {PROP_CLIENT_ID: 'client-2'})
        assert "another client's" in verdict.message
        assert verdict.unowned is False and verdict.confirmed is False
        assert len(record) == 2 and record[0][0] == 'exists'     # exists, then the property read
        assert lib.names() == ['exists']

    def test_a_recorded_other_id_is_decided_with_that_id(self, zc):
        verdict, _ = self.check(zc, FakeLib(), {PROP_CLIENT_ID: 'client-2'})
        assert verdict == owner_verdict(SERVER_DS, 'client-2', CLIENT)

    def test_unowned_when_there_is_no_local_id(self, zc, record):
        lib = FakeLib(value={})
        verdict, reader = self.check(zc, lib, {})
        assert verdict.unowned is True and verdict.confirmed is False
        assert f'zfs set {PROP_CLIENT_ID}={CLIENT} {SERVER_DS}' in verdict.message
        names = called(record)
        assert names[0] == 'exists' and names[-1] == 'value'
        assert len(names) == 3
        assert record[-1][2] == {'sources': 'local'}
        assert record[-1][1][1] == PROP_CLIENT_ID and record[-1][1][0].name == SERVER_DS

    def test_a_received_only_id_matching_the_client_is_still_unowned(self, zc):
        """The properties read includes received values; the LOCAL read decides ownership."""
        lib = FakeLib(value={})                              # no local value
        verdict, _ = self.check(zc, lib, {PROP_CLIENT_ID: CLIENT})
        assert verdict.unowned is True and verdict.message is not None

    def test_a_different_local_id_is_another_clients(self, zc):
        lib = FakeLib(value={PROP_CLIENT_ID: 'client-2'})
        verdict, _ = self.check(zc, lib, {})
        assert "another client's" in verdict.message and verdict.unowned is False

    def test_a_different_local_id_with_a_matching_recorded_id_is_another_clients(self, zc):
        lib = FakeLib(value={PROP_CLIENT_ID: 'client-2'})
        verdict, _ = self.check(zc, lib, {PROP_CLIENT_ID: CLIENT})
        assert "another client's" in verdict.message

    def test_owned_without_a_source_is_confirmed(self, zc):
        lib = FakeLib(value={PROP_CLIENT_ID: CLIENT})
        verdict, _ = self.check(zc, lib, {PROP_CLIENT_ID: CLIENT})
        assert verdict == OwnerVerdict(None, False, True)

    def test_owned_with_a_matching_source_is_confirmed(self, zc):
        lib = FakeLib(value={PROP_CLIENT_ID: CLIENT})
        verdict, _ = self.check(zc, lib, {PROP_CLIENT_ID: CLIENT, PROP_SOURCE_DATASET: 'tank/data'})
        assert verdict == OwnerVerdict(None, False, True)

    def test_a_container_with_no_source_is_not_a_conflict(self, zc):
        lib = FakeLib(value={PROP_CLIENT_ID: CLIENT})
        verdict, _ = self.check(zc, lib, {PROP_CLIENT_ID: CLIENT, PROP_SOURCE_DATASET: ''})
        assert verdict.message is None

    def test_a_source_mismatch_is_refused_and_keeps_confirmed(self, zc):
        lib = FakeLib(value={PROP_CLIENT_ID: CLIENT})
        verdict, _ = self.check(zc, lib, {PROP_CLIENT_ID: CLIENT, PROP_SOURCE_DATASET: 'tank/other'})
        assert verdict.confirmed is True and verdict.unowned is False
        assert verdict.source_mismatch is True
        assert verdict.message == (
            f"server dataset {SERVER_DS} already holds backups of 'tank/other'; "
            "refusing to mix in 'tank/data'"
        )

    def test_a_source_mismatch_without_a_recorded_id_still_checks_the_source(self, zc):
        lib = FakeLib(value={PROP_CLIENT_ID: CLIENT})
        verdict, _ = self.check(zc, lib, {PROP_SOURCE_DATASET: 'tank/other'})
        assert verdict.confirmed is True and 'already holds backups' in verdict.message
        assert verdict.source_mismatch is True

    def test_ownership_problems_win_over_a_source_mismatch(self, zc):
        lib = FakeLib(value={})
        verdict, _ = self.check(zc, lib, {PROP_SOURCE_DATASET: 'tank/other'})
        assert verdict.unowned is True and 'refusing to adopt it' in verdict.message
        assert verdict.source_mismatch is False
        lib = FakeLib(value={PROP_CLIENT_ID: 'client-2'})
        verdict, _ = self.check(zc, lib, {PROP_SOURCE_DATASET: 'tank/other'})
        assert "another client's" in verdict.message

    def test_the_empty_recorded_id_is_treated_as_absent(self, zc, record):
        lib = FakeLib(value={PROP_CLIENT_ID: CLIENT})
        verdict, _ = self.check(zc, lib, {PROP_CLIENT_ID: ''})
        assert verdict.confirmed is True and verdict.message is None
        assert 'value' in called(record)

    def test_call_order_and_counts_for_an_owned_dataset(self, zc, record):
        lib = FakeLib(value={PROP_CLIENT_ID: CLIENT})
        self.check(zc, lib, {PROP_CLIENT_ID: CLIENT})
        assert len(record) == 3 and record[0][0] == 'exists' and record[2][0] == 'value'
        assert lib.names() == ['exists', 'value']
        assert lib.outside == []

    def test_read_properties_runs_through_zfs_call_with_the_dataset(self, zc, record):
        reader = Props({PROP_CLIENT_ID: CLIENT})
        lib = FakeLib(value={PROP_CLIENT_ID: CLIENT})
        ownership_conflict(zc, SERVER_DS, 'tank/data', CLIENT, read_properties=reader, lib=lib)
        assert len(reader.calls) == 1
        ds, args, kwargs = reader.calls[0]
        assert isinstance(ds, Dataset) and ds.name == SERVER_DS
        assert args == () and kwargs == {}
        # zfs_call saw the reader as the middle callable, between exists and the local read
        assert len(record) == 3 and record[1][1][0].name == SERVER_DS

    def test_read_properties_is_a_zfs_call_argument_not_called_directly(self):
        """A `zfs_call` that does not run its callable gets the reader's name, not its result."""
        seen = []

        def spy(fn, *args, **kwargs):
            seen.append(fn)
            if fn is reader:
                return {PROP_CLIENT_ID: 'client-2'}
            return fn(*args, **kwargs)

        reader = Props(fail=Boom('must not be called directly'))
        verdict = ownership_conflict(
            spy, SERVER_DS, 'tank/data', CLIENT, read_properties=reader, lib=FakeLib())
        assert reader in seen
        assert "another client's" in verdict.message

    def test_read_properties_default_is_the_backup_manager_reader(self, zc, monkeypatch):
        seen = []

        def fake_reader(ds):
            seen.append(ds)
            return {PROP_CLIENT_ID: CLIENT}

        # the default is bound at definition time, so patching the module name must not matter
        monkeypatch.setattr(receiver, 'read_zfsbackup_properties', fake_reader)
        lib = FakeLib(value={PROP_CLIENT_ID: CLIENT})
        calls = []

        def zfs_call(fn, *a, **k):
            calls.append(fn)
            if fn is read_zfsbackup_properties:
                return {PROP_CLIENT_ID: CLIENT}
            return fn(*a, **k)

        verdict = ownership_conflict(zfs_call, SERVER_DS, 'tank/data', CLIENT, lib=lib)
        assert read_zfsbackup_properties in calls
        assert verdict.confirmed is True

    @pytest.mark.parametrize('failing', ['exists', 'reader', 'value'])
    @pytest.mark.parametrize('exc', [Boom('x'), ZFSTimeoutError('slow')])
    def test_failures_and_timeouts_propagate(self, zc, failing, exc):
        lib = FakeLib(value={PROP_CLIENT_ID: CLIENT})
        reader = Props({PROP_CLIENT_ID: CLIENT})
        if failing == 'reader':
            reader.fail = exc
        else:
            lib.fail[failing] = exc
        with pytest.raises(type(exc)):
            ownership_conflict(zc, SERVER_DS, 'tank/data', CLIENT, read_properties=reader, lib=lib)

    def test_a_failed_property_read_does_not_ask_for_the_local_value(self, zc):
        lib = FakeLib(value={PROP_CLIENT_ID: CLIENT})
        with pytest.raises(Boom):
            ownership_conflict(zc, SERVER_DS, 'tank/data', CLIENT,
                               read_properties=Props(fail=Boom('x')), lib=lib)
        assert lib.names() == ['exists']


# ---------------------------------------------------------------------------
# ancestor_holds_partial_receive
# ---------------------------------------------------------------------------


class TestAncestorHoldsPartialReceive:
    DS = Dataset(f'{TARGET}/{CLIENT}/tank')

    def run(self, zc, lib):
        return ancestor_holds_partial_receive(zc, self.DS, lib=lib)

    def test_no_token_is_false_and_no_listing_is_made(self, zc, record):
        lib = FakeLib(value={'receive_resume_token': None})
        assert self.run(zc, lib) is False
        assert lib.names() == ['value']
        assert record[0][1] == (self.DS, 'receive_resume_token')

    def test_a_token_and_no_snapshots_is_true(self, zc, record):
        lib = FakeLib(value={'receive_resume_token': '1-abc'}, list=[])
        assert self.run(zc, lib) is True
        assert lib.names() == ['value', 'list']
        _, args, kwargs = record[1]
        assert args == ()
        assert kwargs == {'roots': self.DS, 'types': 'snapshot', 'properties': ['creation']}

    def test_a_token_and_a_snapshot_is_false(self, zc):
        lib = FakeLib(value={'receive_resume_token': '1-abc'}, list=[Row(creation='x')])
        assert self.run(zc, lib) is False

    def test_the_result_is_a_bool(self, zc):
        assert self.run(zc, FakeLib(value={'receive_resume_token': 't'}, list=[])) is True
        assert self.run(zc, FakeLib(value={'receive_resume_token': 't'}, list=[Row()])) is False
        assert self.run(zc, FakeLib(value={})) is False

    def test_uses_zfs_call_for_both_calls(self, zc):
        lib = FakeLib(value={'receive_resume_token': 't'}, list=[])
        self.run(zc, lib)
        assert lib.outside == []

    @pytest.mark.parametrize('failing', ['value', 'list'])
    @pytest.mark.parametrize('exc', [Boom('x'), ZFSTimeoutError('slow')])
    def test_failures_propagate(self, zc, failing, exc):
        lib = FakeLib(value={'receive_resume_token': 't'}, list=[])
        lib.fail[failing] = exc
        with pytest.raises(type(exc)):
            self.run(zc, lib)


# ---------------------------------------------------------------------------
# create_ancestors
# ---------------------------------------------------------------------------


class TestCreateAncestors:
    NEAREST_FIRST = [f'{TARGET}/{CLIENT}/tank/a/b', f'{TARGET}/{CLIENT}/tank/a', f'{TARGET}/{CLIENT}/tank']

    def test_creates_outermost_first_one_level_at_a_time(self, zc, record):
        lib = FakeLib()
        create_ancestors(zc, list(self.NEAREST_FIRST), CLIENT, lib=lib)
        assert [str(c[1][0]) for c in lib.log] == list(reversed(self.NEAREST_FIRST))
        assert called(record) == ['create'] * 3

    def test_every_create_has_the_same_arguments(self, zc, record):
        lib = FakeLib()
        create_ancestors(zc, self.NEAREST_FIRST, CLIENT, lib=lib)
        for _, args, kwargs in record:
            assert len(args) == 1 and isinstance(args[0], Filesystem)
            assert kwargs == {
                'properties': {'canmount': 'noauto', PROP_CLIENT_ID: CLIENT},
                'mount': False,
            }
        assert lib.outside == []

    def test_no_p_style_option_and_no_size(self, zc, record):
        create_ancestors(zc, self.NEAREST_FIRST, CLIENT, lib=FakeLib())
        for _, _, kwargs in record:
            assert set(kwargs) == {'properties', 'mount'}

    def test_the_input_is_not_mutated(self, zc):
        data = list(self.NEAREST_FIRST)
        create_ancestors(zc, data, CLIENT, lib=FakeLib())
        assert data == self.NEAREST_FIRST

    def test_a_tuple_input_works(self, zc):
        lib = FakeLib()
        create_ancestors(zc, tuple(self.NEAREST_FIRST), CLIENT, lib=lib)
        assert len(lib.log) == 3

    def test_empty_input_creates_nothing(self, zc, record):
        create_ancestors(zc, [], CLIENT, lib=FakeLib())
        assert record == []

    def test_a_single_ancestor(self, zc):
        lib = FakeLib()
        create_ancestors(zc, [self.NEAREST_FIRST[-1]], CLIENT, lib=lib)
        assert [str(c[1][0]) for c in lib.log] == [self.NEAREST_FIRST[-1]]

    def test_stops_at_the_first_failure_leaving_the_earlier_ones(self, zc):
        lib = FakeLib()
        made = []

        def create(ds, **kw):
            if len(made) == 1:
                raise Boom('second fails')
            made.append(str(ds))

        lib.create = create
        with pytest.raises(Boom):
            create_ancestors(zc, self.NEAREST_FIRST, CLIENT, lib=lib)
        assert made == [self.NEAREST_FIRST[-1]]               # the outermost only; nothing after the failure

    def test_a_timeout_propagates(self):
        def timed_out(fn, *a, **k):
            raise ZFSTimeoutError('slow')

        with pytest.raises(ZFSTimeoutError):
            create_ancestors(timed_out, self.NEAREST_FIRST, CLIENT, lib=FakeLib())

    def test_an_invalid_name_raises_before_any_create(self, zc, record):
        with pytest.raises(ValueError):
            create_ancestors(zc, ['bad//name'], CLIENT, lib=FakeLib())
        assert record == []

    def test_the_client_id_is_stamped_verbatim(self, zc, record):
        create_ancestors(zc, self.NEAREST_FIRST[:1], 'Client.9_x-y', lib=FakeLib())
        assert record[0][2]['properties'][PROP_CLIENT_ID] == 'Client.9_x-y'


# ---------------------------------------------------------------------------
# abort_partial_receive
# ---------------------------------------------------------------------------


class TestAbortPartialReceive:
    def test_success_runs_receive_a_on_a_dataset_through_zfs_call(self, zc, record):
        lib = FakeLib()
        assert abort_partial_receive(zc, SERVER_DS, lib=lib) is None
        assert called(record) == ['abort']
        ds = record[0][1][0]
        assert isinstance(ds, Dataset) and ds.name == SERVER_DS
        assert lib.outside == []

    @pytest.mark.parametrize('message', [
        "'x/y' does not have any resumable receive state to abort",
        "cannot open 'x/y': dataset does not exist",
        "Cannot open 'x/y': Dataset Does Not Exist\nusage:\n\treceive -A <filesystem|volume>",
        "DOES NOT HAVE ANY RESUMABLE RECEIVE STATE",
    ])
    def test_already_clean_messages_are_swallowed(self, zc, message):
        lib = FakeLib()
        lib.fail['abort'] = Boom(message)
        assert abort_partial_receive(zc, SERVER_DS, lib=lib) is None

    @pytest.mark.parametrize('message', [
        'permission denied', 'pool I/O is currently suspended', "dataset is busy",
        "cannot open: no such pool", '',
    ])
    def test_other_failures_are_reraised_unchanged(self, zc, message):
        lib = FakeLib()
        error = Boom(message)
        lib.fail['abort'] = error
        with pytest.raises(Boom) as exc:
            abort_partial_receive(zc, SERVER_DS, lib=lib)
        assert exc.value is error

    def test_a_timeout_is_reraised_even_with_an_already_clean_message(self, zc):
        lib = FakeLib()
        error = ZFSTimeoutError("does not have any resumable receive state (dataset does not exist)")
        lib.fail['abort'] = error
        with pytest.raises(ZFSTimeoutError) as exc:
            abort_partial_receive(zc, SERVER_DS, lib=lib)
        assert exc.value is error

    def test_a_timeout_from_zfs_call_itself_propagates(self):
        def timed_out(fn, *a, **k):
            raise ZFSTimeoutError('slow')

        with pytest.raises(ZFSTimeoutError):
            abort_partial_receive(timed_out, SERVER_DS, lib=FakeLib())

    def test_only_one_abort_is_issued(self, zc, record):
        lib = FakeLib()
        lib.fail['abort'] = Boom('does not have any resumable receive state')
        abort_partial_receive(zc, SERVER_DS, lib=lib)
        assert called(record) == ['abort']

    def test_an_invalid_name_raises_before_any_call(self, zc, record):
        with pytest.raises(ValueError):
            abort_partial_receive(zc, 'bad//name', lib=FakeLib())
        assert record == []


class TestAbortAgainstCapturedWordings:
    """The real `zfs.receive.abort` over the committed per-version captures (`Popen` mocked): the
    already-clean wordings are swallowed, an unrelated failure is not."""

    @staticmethod
    def replay(mock, rec):
        proc = mock.setup(stdout=rec.stdout_lines, stderr=rec.stderr_lines, returncode=rec.rc)
        proc.poll.side_effect = None
        proc.poll.return_value = rec.rc
        return proc

    @staticmethod
    def direct(fn, *args, **kwargs):
        return fn(*args, **kwargs)

    @pytest.fixture(params=VERSIONS)
    def version(self, request):
        return request.param

    def aborts(self, version, section):
        return [r for r in load_section(version, section).values() if r.cmd.startswith('zfs receive -A')]

    def test_every_captured_abort_is_idempotent(self, version, mock_subprocess):
        n = 0
        for section in ('M2', 'Z3'):
            for rec in self.aborts(version, section):
                self.replay(mock_subprocess, rec)
                dataset = rec.cmd.split()[-1]
                assert abort_partial_receive(self.direct, dataset) is None, rec.cmd
                n += 1
        assert n >= 8

    def test_the_rc_1_wording_is_present_and_swallowed(self, version, mock_subprocess):
        recs = [r for r in self.aborts(version, 'M2') if r.rc == 1]
        assert len(recs) >= 5
        for rec in recs:
            assert 'does not have any resumable receive state' in rec.stderr
            self.replay(mock_subprocess, rec)
            abort_partial_receive(self.direct, rec.cmd.split()[-1])

    def test_the_rc_2_wording_is_present_and_swallowed(self, version, mock_subprocess):
        recs = [r for r in self.aborts(version, 'Z3') if r.rc == 2]
        assert len(recs) >= 2
        for rec in recs:
            assert 'dataset does not exist' in rec.stderr
            self.replay(mock_subprocess, rec)
            abort_partial_receive(self.direct, rec.cmd.split()[-1])

    def test_an_unrelated_failure_is_reraised(self, mock_subprocess):
        mock_subprocess.setup(stdout=[], stderr=["cannot receive: permission denied\n"], returncode=1)
        with pytest.raises(Exception) as exc:
            abort_partial_receive(self.direct, SERVER_DS)
        assert 'permission denied' in str(exc.value).lower()

    def test_the_successful_capture_returns_quietly(self, version, mock_subprocess):
        recs = [r for r in self.aborts(version, 'Z3') if r.rc == 0]
        assert recs
        self.replay(mock_subprocess, recs[0])
        assert abort_partial_receive(self.direct, recs[0].cmd.split()[-1]) is None


# ---------------------------------------------------------------------------
# receive_properties / may_force / start_receive
# ---------------------------------------------------------------------------


class TestReceiveProperties:
    def test_volume(self):
        assert receive_properties('volume') is RECEIVE_PROPERTIES_VOLUME

    @pytest.mark.parametrize('kind', ['filesystem', 'snapshot', '', 'zvol', None, 'VOLUME'])
    def test_everything_else_is_the_filesystem_set(self, kind):
        assert receive_properties(kind) is RECEIVE_PROPERTIES_FILESYSTEM


class TestMayForce:
    FS = Filesystem(SERVER_DS)

    def test_not_allowed_never_asks_exists(self, zc, record):
        lib = FakeLib(exists=True)
        assert may_force(zc, self.FS, False, lib=lib) is False
        assert record == [] and lib.log == []

    def test_allowed_and_existing(self, zc, record):
        lib = FakeLib(exists=True)
        assert may_force(zc, self.FS, True, lib=lib) is True
        assert called(record) == ['exists']
        assert record[0][1] == (self.FS,)
        assert lib.outside == []

    def test_allowed_but_missing(self, zc):
        assert may_force(zc, self.FS, True, lib=FakeLib(exists=False)) is False

    def test_a_failure_propagates(self, zc):
        lib = FakeLib()
        lib.fail['exists'] = Boom('x')
        with pytest.raises(Boom):
            may_force(zc, self.FS, True, lib=lib)

    def test_a_timeout_propagates(self, zc):
        lib = FakeLib()
        lib.fail['exists'] = ZFSTimeoutError('slow')
        with pytest.raises(ZFSTimeoutError):
            may_force(zc, self.FS, True, lib=lib)

    def test_a_failure_is_not_raised_when_not_allowed(self, zc):
        lib = FakeLib()
        lib.fail['exists'] = Boom('never reached')
        assert may_force(zc, self.FS, False, lib=lib) is False


class TestStartReceive:
    FS = Filesystem(SERVER_DS)
    OWNED = {PROP_CONFIG: 'e30=', PROP_CLIENT_ID: CLIENT, PROP_SOURCE_DATASET: 'tank/data'}

    def start(self, kind='filesystem', force=False, owned=None, lib=None):
        lib = lib or FakeLib()
        handle = start_receive(self.FS, self.OWNED if owned is None else owned, kind, force, lib=lib)
        return handle, lib

    def test_returns_the_handle(self):
        handle, lib = self.start()
        assert handle == 'HANDLE'

    def test_filesystem_arguments(self):
        _, lib = self.start('filesystem', force=False)
        (name, args, kwargs), = lib.log
        assert name == 'filesystem' and args == (self.FS,)
        assert kwargs == {
            'props': {**self.OWNED, **RECEIVE_PROPERTIES_FILESYSTEM},
            'force': False, 'holds': False, 'save': True, 'mount': False,
        }

    def test_volume_arguments(self):
        _, lib = self.start('volume', force=True)
        kwargs = lib.log[0][2]
        assert kwargs['props'] == {**self.OWNED, **RECEIVE_PROPERTIES_VOLUME}
        assert 'canmount' not in kwargs['props'] and 'mountpoint' not in kwargs['props']
        assert kwargs['force'] is True

    @pytest.mark.parametrize('force', [True, False])
    def test_force_is_passed_through(self, force):
        _, lib = self.start(force=force)
        assert lib.log[0][2]['force'] is force

    def test_holds_false_save_true_mount_false_always(self):
        for kind in ('filesystem', 'volume'):
            for force in (True, False):
                _, lib = self.start(kind, force)
                kwargs = lib.log[0][2]
                assert (kwargs['holds'], kwargs['save'], kwargs['mount']) == (False, True, False)
                assert 'unmount' not in kwargs and 'reset' not in kwargs and 'origin' not in kwargs

    def test_is_not_wrapped_in_zfs_call(self):
        """`FakeLib.filesystem` is exempt from the outside-a-zfs_call check: it must be reached
        directly."""
        _, lib = self.start()
        assert lib.outside == [] and Calls.active is False

    def test_owned_keys_are_never_overridden_by_the_forced_set(self):
        """The two key sets are disjoint, so the merge cannot replace a server-owned value."""
        for kind in ('filesystem', 'volume'):
            assert not set(self.OWNED) & set(receive_properties(kind))
            _, lib = self.start(kind)
            props = lib.log[0][2]['props']
            for key, value in self.OWNED.items():
                assert props[key] == value
            assert len(props) == len(self.OWNED) + len(receive_properties(kind))

    def test_the_forced_values_are_the_forced_set(self):
        _, lib = self.start('filesystem')
        props = lib.log[0][2]['props']
        for key, value in RECEIVE_PROPERTIES_FILESYSTEM.items():
            assert props[key] == value

    def test_the_module_dicts_are_not_mutated(self):
        before_fs, before_vol = dict(RECEIVE_PROPERTIES_FILESYSTEM), dict(RECEIVE_PROPERTIES_VOLUME)
        owned = dict(self.OWNED)
        self.start('filesystem')
        self.start('volume')
        assert RECEIVE_PROPERTIES_FILESYSTEM == before_fs
        assert RECEIVE_PROPERTIES_VOLUME == before_vol
        assert owned == self.OWNED

    def test_props_is_a_fresh_dict_each_time(self):
        _, lib = self.start()
        props = lib.log[0][2]['props']
        assert props is not RECEIVE_PROPERTIES_FILESYSTEM
        props['x'] = 'y'
        assert 'x' not in RECEIVE_PROPERTIES_FILESYSTEM

    def test_a_mapping_for_owned_is_accepted(self):
        from types import MappingProxyType
        _, lib = self.start(owned=MappingProxyType(self.OWNED))
        assert lib.log[0][2]['props'][PROP_CLIENT_ID] == CLIENT

    def test_empty_owned(self):
        _, lib = self.start(owned={})
        assert lib.log[0][2]['props'] == RECEIVE_PROPERTIES_FILESYSTEM

    def test_a_startup_failure_raises(self):
        lib = FakeLib()
        lib.fail['filesystem'] = Boom('cannot start')
        with pytest.raises(Boom):
            start_receive(self.FS, self.OWNED, 'filesystem', False, lib=lib)

    def test_it_never_returns_none_on_a_failure(self):
        lib = FakeLib()
        lib.fail['filesystem'] = OSError('no zfs')
        with pytest.raises(OSError):
            start_receive(self.FS, self.OWNED, 'filesystem', False, lib=lib)


# ---------------------------------------------------------------------------
# lib= override and the default
# ---------------------------------------------------------------------------


class TestLibOverride:
    """Each function looks its commands up on `lib` when given, else on the module-level `zfs`
    (patched here so no real `zfs` is reached)."""

    def other(self):
        return FakeLib()

    def patched_default(self, monkeypatch, **answers):
        default = FakeLib(**answers)
        monkeypatch.setattr(receiver, 'zfs', default)
        return default

    def test_the_default_is_libzfseasy(self):
        assert receiver.zfs is libzfseasy

    def test_the_override_is_used_instead_of_the_default(self, zc, monkeypatch):
        default = self.patched_default(monkeypatch)
        lib = FakeLib(value={'encryption': 'off'})
        assert server_copy_kind(zc, Filesystem(SERVER_DS), lib=lib) == 'plain'
        assert lib.names() == ['value'] and default.log == []

    def test_the_default_is_looked_up_at_call_time(self, zc, monkeypatch):
        """A test that replaces the module's `zfs` (the API's does) still reaches the functions."""
        default = self.patched_default(monkeypatch, value={'encryption': 'off'})
        assert server_copy_kind(zc, Filesystem(SERVER_DS)) == 'plain'
        assert default.names() == ['value']

    @pytest.mark.parametrize('name, call, expect', [
        ('server_copy_kind', lambda zc, lib: server_copy_kind(zc, Filesystem(SERVER_DS), lib=lib),
         ['value']),
        ('resume_token', lambda zc, lib: resume_token(zc, SERVER_DS, lib=lib), ['exists', 'list']),
        ('local_client_id', lambda zc, lib: local_client_id(zc, Dataset(SERVER_DS), lib=lib), ['value']),
        ('ownership_conflict',
         lambda zc, lib: ownership_conflict(zc, SERVER_DS, 'tank/data', CLIENT,
                                            read_properties=lambda ds: {}, lib=lib),
         ['exists', 'value']),
        ('ancestor_holds_partial_receive',
         lambda zc, lib: ancestor_holds_partial_receive(zc, Dataset(SERVER_DS), lib=lib), ['value', 'list']),
        ('create_ancestors', lambda zc, lib: create_ancestors(zc, [SERVER_DS], CLIENT, lib=lib),
         ['create']),
        ('abort_partial_receive', lambda zc, lib: abort_partial_receive(zc, SERVER_DS, lib=lib),
         ['abort']),
        ('may_force', lambda zc, lib: may_force(zc, Filesystem(SERVER_DS), True, lib=lib), ['exists']),
    ])
    def test_every_function_honours_lib(self, zc, monkeypatch, name, call, expect):
        default = self.patched_default(monkeypatch)
        lib = FakeLib(value={'receive_resume_token': 't', PROP_CLIENT_ID: None}, list=[Row(
            receive_resume_token='1-x')])
        call(zc, lib)
        assert lib.names() == expect, name
        assert default.log == [], name

    def test_start_receive_honours_lib(self, monkeypatch):
        default = self.patched_default(monkeypatch)
        lib = FakeLib()
        start_receive(Filesystem(SERVER_DS), {}, 'filesystem', False, lib=lib)
        assert lib.names() == ['filesystem'] and default.log == []

    def test_start_receive_uses_the_default_when_no_lib_is_given(self, monkeypatch):
        default = self.patched_default(monkeypatch)
        start_receive(Filesystem(SERVER_DS), {}, 'volume', True)
        assert default.names() == ['filesystem']

    @pytest.mark.parametrize('call', [
        lambda zc: resume_token(zc, SERVER_DS),
        lambda zc: local_client_id(zc, Dataset(SERVER_DS)),
        lambda zc: ownership_conflict(zc, SERVER_DS, 'tank/data', CLIENT, read_properties=lambda ds: {}),
        lambda zc: ancestor_holds_partial_receive(zc, Dataset(SERVER_DS)),
        lambda zc: create_ancestors(zc, [SERVER_DS], CLIENT),
        lambda zc: abort_partial_receive(zc, SERVER_DS),
        lambda zc: may_force(zc, Filesystem(SERVER_DS), True),
    ])
    def test_every_function_falls_back_to_the_module_zfs(self, zc, monkeypatch, call):
        default = self.patched_default(monkeypatch, value={PROP_CLIENT_ID: None}, list=token_row('1-x'))
        call(zc)
        assert default.log, 'the module-level zfs was not used'
        assert default.outside == []

    def test_lib_is_keyword_only(self, zc):
        with pytest.raises(TypeError):
            resume_token(zc, SERVER_DS, FakeLib())


# ---------------------------------------------------------------------------
# zfs_call is the only route to zfs
# ---------------------------------------------------------------------------


class TestZfsCallContract:
    """With a `zfs_call` that substitutes answers without running anything, no real or fake command
    runs: every call a function makes is visible to it."""

    def inert(self, answers):
        seen = []

        def zfs_call(fn, *args, **kwargs):
            seen.append((fn, args, kwargs))
            return answers.pop(0)

        return zfs_call, seen

    def test_server_copy_kind(self):
        lib = FakeLib()
        zfs_call, seen = self.inert(['on', 'x'])
        assert server_copy_kind(zfs_call, Filesystem(SERVER_DS), lib=lib) == 'plain'
        assert [s[0] for s in seen] == [lib.get.value, lib.get.value]
        assert lib.log == []

    def test_resume_token(self):
        lib = FakeLib()
        zfs_call, seen = self.inert([True, token_row('7-z')])
        assert resume_token(zfs_call, SERVER_DS, lib=lib) == '7-z'
        assert [s[0] for s in seen] == [lib.exists, lib.list]
        assert lib.log == []

    def test_ownership_conflict(self):
        lib = FakeLib()
        reader = Props()
        zfs_call, seen = self.inert([True, {PROP_CLIENT_ID: CLIENT}, CLIENT])
        verdict = ownership_conflict(
            zfs_call, SERVER_DS, 'tank/data', CLIENT, read_properties=reader, lib=lib)
        assert verdict.confirmed is True
        assert [s[0] for s in seen] == [lib.exists, reader, lib.get.value]
        assert lib.log == [] and reader.calls == []

    def test_create_and_abort_and_force(self):
        lib = FakeLib()
        zfs_call, seen = self.inert([None, None, True])
        create_ancestors(zfs_call, [SERVER_DS], CLIENT, lib=lib)
        abort_partial_receive(zfs_call, SERVER_DS, lib=lib)
        assert may_force(zfs_call, Filesystem(SERVER_DS), True, lib=lib) is True
        assert [s[0] for s in seen] == [lib.create, lib.receive.abort, lib.exists]
        assert lib.log == []

    def test_the_budget_wrapper_can_swap_the_call(self):
        """A `zfs_call` may pass extra context (a timeout) and see exactly `fn(*args, **kwargs)`."""
        lib = FakeLib(value={PROP_CLIENT_ID: CLIENT})
        seen = []

        def timed(fn, *args, **kwargs):
            seen.append((args, kwargs))
            return fn(*args, **kwargs)

        ds = Dataset(SERVER_DS)
        assert local_client_id(timed, ds, lib=lib) == CLIENT
        assert len(seen) == 1 and seen[0][0][0] is ds
        assert seen[0] == ((ds, PROP_CLIENT_ID), {'sources': 'local'})
