"""Replay of real OpenZFS output (2.1.5, 2.2.2, 2.4.1) through libzfseasy's parsers.

The records come from `tests/fixtures/openzfs/<version>/` (captured by `scenarios/test_openzfs_matrix.sh
--captures`, see the README there). Each test feeds a record's exact stdout / stderr / exit status to a mocked
`subprocess.Popen` and asserts what libzfseasy makes of it. Nothing here starts a real `zfs`.

Where the captured command is not the one libzfseasy builds (it always asks for `name,type,...` and
`name,property,value,received,source`), the stdout is derived from the capture by inserting that column, and the
test says so. Where a test checks the argv libzfseasy ran, it compares against the captured command line, so the
output is only ever replayed for the command it belongs to.
"""

import shlex
from unittest.mock import MagicMock

import pytest

import libzfseasy as zfs
from libzfseasy.types import Filesystem, Snapshot, SnapshotRange, ZFS

from _openzfs_fixtures import VERSIONS, load_m1_summary, load_section

pytestmark = [pytest.mark.unit, pytest.mark.subprocess]

# Versions on which a message differs, pinned per version below.
V21, V22, V24 = VERSIONS


@pytest.fixture(params=VERSIONS)
def version(request):
    return request.param


# --------------------------------------------------------------------------------------------------------------
# helpers
# --------------------------------------------------------------------------------------------------------------

def replay(mock, rec, stdout=None):
    """Make the next Popen print `rec`'s stdout/stderr and exit with its status."""
    proc = mock.setup(stdout=(rec.stdout_lines if stdout is None else stdout), stderr=rec.stderr_lines,
                      returncode=rec.rc)
    # The fixture's scripted poll() runs out after EOF, but a real one keeps answering (and the cleanup of a
    # generator that raised polls once more): from here on it just reports the exit status.
    proc.poll.side_effect = None
    proc.poll.return_value = rec.rc
    return proc


def argv_of(mock):
    """The argv (without the binary) of the last Popen call."""
    return list(mock.call_args_list[-1][0][0])[1:]


def captured_argv(rec):
    """The captured command's argv (without `zfs`), up to the first pipe."""
    return shlex.split(rec.cmd.split(' | ')[0])[1:]


def assert_runs_captured_command(mock, rec):
    """libzfseasy ran the same command the capture ran (option order aside)."""
    assert sorted(argv_of(mock)) == sorted(captured_argv(rec)), rec.id


def failure_message(stderr):
    """The text libzfseasy's exception carries for `stderr`: stripped non-empty lines, the first letter of the
    FIRST line capitalised (continuation lines carry dataset names and are left verbatim)."""
    out = []
    for line in stderr.splitlines():
        line = line.strip()
        if line:
            out.append(line[0].upper() + line[1:] if not out else line)
    return '\n'.join(out)


def records(version, section, prefix='', suffix=''):
    return {i: r for i, r in load_section(version, section).items()
            if i.split('/', 1)[1].startswith(prefix) and i.endswith(suffix)}


def with_type_column(stdout):
    """libzfseasy lists `name,type,...`; the capture of a `-o name,...` listing has no type column. Insert it
    (a name with `@` is a snapshot, anything else a filesystem in these captures)."""
    out = []
    for line in stdout.splitlines():
        name, tab, rest = line.partition('\t')
        out.append('%s\t%s%s%s\n' % (name, 'snapshot' if '@' in name else 'filesystem', tab, rest))
    return out


def stream_failure(mock, stderr, rc):
    """Make the next Popen a send that dies before writing a byte (the streaming commands read bytes)."""
    proc = MagicMock()
    proc.stdout.peek.return_value = b''
    proc.stderr.readline.side_effect = [l.encode() + b'\n' for l in stderr.splitlines()] + [b'']
    proc.poll.return_value = rc
    proc.wait.return_value = rc
    proc.returncode = rc
    mock.return_value = proc
    return proc


def sender_rc(rec):
    """Exit status of the `zfs send` side of a captured pipeline."""
    return rec.pipestatus[0] if rec.pipestatus else rec.rc


# --------------------------------------------------------------------------------------------------------------
# the fixtures themselves
# --------------------------------------------------------------------------------------------------------------

class TestFixtures:
    def test_every_version_has_every_section(self, version):
        for section in ('M1', 'M2', 'M3_Z8', 'M4', 'EP', 'Z4', 'Z7', 'M7'):
            assert load_section(version, section), section

    def test_versions_match_their_directory(self, version):
        import os
        from _openzfs_fixtures import version_dir
        with open(os.path.join(version_dir(version), 'ENV.txt'), encoding='utf-8') as f:
            env = f.read()
        assert 'zfs-' + version in env
        assert 'LC_ALL=C' in env  # the C-locale captures only

    def test_m1_summary_covers_both_stream_kinds_of_every_case(self, version):
        rows = load_m1_summary(version)
        assert len(rows) == 64
        cases = {r['case'] for r in rows}
        assert len(cases) == 32
        for case in cases:
            assert sorted(r['kind'] for r in rows if r['case'] == case) == ['full', 'incr']
        assert all(r['send_rc'] == 0 and r['resume_rc'] == 0 and r['token_after'] == '-' for r in rows)

    def test_records_keep_the_exact_bytes(self, version):
        rec = load_section(version, 'M3_Z8')['M3_Z8/held.11_dry_single']
        assert rec.stdout == 'destroy\tzfsbcapc/M3_Z8/held@s2\nreclaim\t14336\n'
        assert rec.stderr == ''
        assert rec.rc == 0 and rec.pipestatus is None


# --------------------------------------------------------------------------------------------------------------
# M1: resume tokens
# --------------------------------------------------------------------------------------------------------------

# What each send-flag set records (docs: SP capture report, M1). Same on every version.
EXPECTED_FLAGS = {
    # (dataset recordsize, case suffix): flags
    ('128K', 'plain_none'): set(),
    ('128K', 'plain_L'): set(),                       # no large blocks on the dataset: nothing recorded
    ('1M', 'plain_L'): {'largeblockok'},
    ('128K', 'plain_c'): {'compressok'},
    ('1M', 'plain_c'): {'compressok'},
    ('128K', 'plain_e'): {'embedok'},
    ('1M', 'plain_e'): {'embedok'},
    ('128K', 'plain_Lc'): {'compressok'},
    ('1M', 'plain_Lc'): {'compressok', 'largeblockok'},
    ('128K', 'plain_w'): {'compressok', 'embedok'},   # -w on an UNENCRYPTED dataset: no rawok
    ('1M', 'plain_w'): {'compressok', 'embedok', 'largeblockok'},
    ('128K', 'enc_w'): {'compressok', 'rawok'},       # -w on an encrypted dataset: rawok, no embedok
    ('1M', 'enc_w'): {'compressok', 'largeblockok', 'rawok'},
    ('1M', 'enc_Lc'): {'compressok', 'largeblockok'},  # plain -L -c from an encrypted source (key loaded)
    ('128K', 'enc_Lc'): {'compressok'},
}


def _case_parts(case):
    size, comp, rest = case.split('_', 2)
    return size, comp, rest


def _hex(value):
    return int(value, 16)


def _m1_token_records(version, case, kind):
    recs = load_section(version, 'M1')
    nv = recs['M1/%s.%s.3_send_nv_t' % (case, kind)]
    prop = recs['M1/%s.%s.2_token_prop' % (case, kind)]
    return nv, prop


class TestResumeInfoM1:
    """`SendCommand.resume_info` on the contents block of every M1 token."""

    def test_flags_match_the_summary_for_every_token(self, version, mock_subprocess):
        rows = load_m1_summary(version)
        checked = 0
        for row in rows:
            nv, _prop = _m1_token_records(version, row['case'], row['kind'])
            replay(mock_subprocess, nv)
            info = zfs.send.resume_info(nv.token)
            assert argv_of(mock_subprocess) == captured_argv(nv), (row['case'], row['kind'])
            assert info.flags == row['flags'], (row['case'], row['kind'])
            assert isinstance(info.flags, frozenset)
            assert info.error is None, (row['case'], row['kind'])
            checked += 1
        assert checked == 64

    def test_flag_sets_by_send_flags_independent_of_the_summary(self, version, mock_subprocess):
        """The same expectation written out by hand (the SP report's M1 table), so a wrong summary cannot hide."""
        seen = set()
        for row in load_m1_summary(version):
            size, comp, rest = _case_parts(row['case'])
            if (size, rest) not in EXPECTED_FLAGS:
                continue
            nv, _ = _m1_token_records(version, row['case'], row['kind'])
            replay(mock_subprocess, nv)
            assert set(zfs.send.resume_info(nv.token).flags) == EXPECTED_FLAGS[(size, rest)], row['case']
            seen.add((size, rest))
        assert seen == set(EXPECTED_FLAGS)

    def test_compression_off_does_not_change_the_flags(self, version, mock_subprocess):
        """`compressok` is recorded for `-c`, and `embedok` for `-e`, even with compression=off."""
        for size in ('128K', '1M'):
            for rest in ('plain_c', 'plain_e', 'plain_Lc', 'plain_w'):
                got = []
                for comp in ('lz4', 'off'):
                    nv, _ = _m1_token_records(version, '%s_%s_%s' % (size, comp, rest), 'full')
                    replay(mock_subprocess, nv)
                    got.append(zfs.send.resume_info(nv.token).flags)
                assert got[0] == got[1], (size, rest)

    def test_full_and_incremental_tokens_carry_the_same_flags(self, version, mock_subprocess):
        for row in load_m1_summary(version):
            if row['kind'] != 'full':
                continue
            flags = []
            for kind in ('full', 'incr'):
                nv, _ = _m1_token_records(version, row['case'], kind)
                replay(mock_subprocess, nv)
                flags.append(zfs.send.resume_info(nv.token).flags)
            assert flags[0] == flags[1], row['case']

    def test_contents_of_every_token(self, version, mock_subprocess):
        """toname, toguid, fromguid and the offsets are what the capture printed; the GUIDs are those of the
        snapshots `zfs list` showed for the same dataset."""
        recs = load_section(version, 'M1')
        guids = {}
        for rid, rec in recs.items():
            if rid.endswith('.snaps'):
                for line in rec.stdout.splitlines():
                    name, guid, _txg = line.split('\t')
                    guids[name] = int(guid)
        assert guids
        for row in load_m1_summary(version):
            nv, prop = _m1_token_records(version, row['case'], row['kind'])
            assert prop.stdout.split('\t')[2] == nv.token  # the property the receiver exposed IS the token
            replay(mock_subprocess, nv)
            info = zfs.send.resume_info(nv.token)
            where = (row['case'], row['kind'])

            printed = {}
            for line in nv.stdout.splitlines():
                if line.startswith('\t') and ' = ' in line:
                    key, _, value = line.strip().partition(' = ')
                    printed[key] = value
            assert info.toname == printed['toname'], where
            assert info.toguid == _hex(printed['toguid']), where
            assert (info.object, info.offset, info.bytes) == tuple(
                _hex(printed[k]) for k in ('object', 'offset', 'bytes')), where

            dataset, _, snap = info.toname.partition('@')
            assert info.toguid == guids[info.toname], where
            if row['kind'] == 'full':
                assert snap == 'a' and info.fromguid is None, where
            else:
                assert snap == 'b', where
                assert info.fromguid == guids[dataset + '@a'], where
                assert info.fromguid == _hex(printed['fromguid']), where

    def test_the_size_estimate_lines_are_not_mistaken_for_an_error(self, version, mock_subprocess):
        nv, _ = _m1_token_records(version, '128K_lz4_plain_none', 'full')
        assert 'estimated size' in nv.stdout
        replay(mock_subprocess, nv)
        assert zfs.send.resume_info(nv.token).error is None

    @pytest.mark.parametrize('rid,flags', [
        ('Z4/50_send_nv_t_plain_token_key_unloaded', set()),
        ('Z4/52_send_nv_t_raw_token_key_unloaded', {'compressok', 'rawok'}),
    ])
    def test_tokens_of_an_encrypted_dataset_decode_with_its_key_unloaded(self, version, rid, flags, mock_subprocess):
        rec = load_section(version, 'Z4')[rid]
        replay(mock_subprocess, rec)
        info = zfs.send.resume_info(rec.token)
        assert set(info.flags) == flags
        assert info.error is None
        assert info.toname == 'zfsbcapc/Z4/cs/enc@a'


# --------------------------------------------------------------------------------------------------------------
# M2: stale tokens
# --------------------------------------------------------------------------------------------------------------

STALE_ERRORS = {
    # case -> (the error text zfs prints, same on every version)
    'i_from_destroyed': 'Cannot resume send: Invalid cross-device link',
    'i_from_recreated': 'Cannot resume send: Invalid cross-device link',
    'i_sender_moved': 'Cannot resume send: Invalid cross-device link',
    'i_to_destroyed': "Cannot resume send: 'zfsbcapc/M2/s/i_to_destroyed@b' used in the initial send no longer exists",
    'i_to_recreated': "Cannot resume send: 'zfsbcapc/M2/s/i_to_recreated@b' is no longer the same snapshot used in "
                      "the initial send",
    'f_to_destroyed': "Cannot resume send: 'zfsbcapc/M2/s/f_to_destroyed@a' used in the initial send no longer exists",
    'f_to_recreated': "Cannot resume send: 'zfsbcapc/M2/s/f_to_recreated@a' is no longer the same snapshot used in "
                      "the initial send",
}

# Tokens that still resume: `send -nv -t` succeeds.
RESUMABLE = ('i_sender_renamed', 'f_sender_renamed', 'i_receiver_base_gone', 'i_receiver_replaced',
             'f_receiver_replaced')


def _m2_cases(version, suffix):
    return {i.split('/', 1)[1].split('.')[0]: r for i, r in records(version, 'M2', suffix=suffix).items()}


class TestResumeInfoStaleM2:
    def test_every_stale_case_sets_error_with_the_exact_text(self, version, mock_subprocess):
        cases = _m2_cases(version, '.8_nv_t')
        assert set(STALE_ERRORS) <= set(cases)
        for case, text in STALE_ERRORS.items():
            rec = cases[case]
            assert rec.rc == 255, case
            replay(mock_subprocess, rec)
            info = zfs.send.resume_info(rec.token)
            assert argv_of(mock_subprocess) == captured_argv(rec), case
            assert info.error == text, case
            # the contents block is still reported
            assert info.toguid is not None and info.toname.startswith('zfsbcapc/M2/s/'), case

    def test_error_text_follows_the_captured_stderr(self, version, mock_subprocess):
        for case in STALE_ERRORS:
            rec = _m2_cases(version, '.8_nv_t')[case]
            replay(mock_subprocess, rec)
            assert zfs.send.resume_info(rec.token).error == failure_message(rec.stderr), case

    def test_incremental_tokens_keep_fromguid_when_stale(self, version, mock_subprocess):
        for case in ('i_from_destroyed', 'i_to_destroyed', 'i_to_recreated'):
            rec = _m2_cases(version, '.8_nv_t')[case]
            replay(mock_subprocess, rec)
            assert zfs.send.resume_info(rec.token).fromguid is not None, case
        for case in ('f_to_destroyed', 'f_to_recreated'):
            rec = _m2_cases(version, '.8_nv_t')[case]
            replay(mock_subprocess, rec)
            assert zfs.send.resume_info(rec.token).fromguid is None, case

    def test_tokens_that_still_resume_have_no_error(self, version, mock_subprocess):
        cases = _m2_cases(version, '.8_nv_t')
        for case in RESUMABLE:
            rec = cases[case]
            assert rec.rc == 0, case
            replay(mock_subprocess, rec)
            info = zfs.send.resume_info(rec.token)
            assert info.error is None, case
            assert info.toguid is not None, case

    def test_a_renamed_sender_keeps_the_old_toname_but_the_guid_identifies_the_stream(self, version, mock_subprocess):
        """`toname` is stale after a rename within the parent (documented on ResumeToken); `toguid` is not."""
        recs = records(version, 'M2')
        before = recs['M2/i_sender_renamed.1_nv_t_control']
        after = recs['M2/i_sender_renamed.8_nv_t']
        replay(mock_subprocess, before)
        a = zfs.send.resume_info(before.token)
        replay(mock_subprocess, after)
        b = zfs.send.resume_info(after.token)
        assert a.toguid == b.toguid and a.toguid is not None
        assert a.toname == b.toname  # the token is unchanged, so is its name

    def test_the_moved_sender_error_is_the_same_text_as_a_destroyed_source(self, version, mock_subprocess):
        """The capture is non-deterministic for a sender moved to another parent (it also resumes fine), and when it
        fails the text is that of a destroyed incremental source: the text alone cannot tell them apart."""
        cases = _m2_cases(version, '.8_nv_t')
        texts = []
        for case in ('i_sender_moved', 'i_from_destroyed'):
            replay(mock_subprocess, cases[case])
            texts.append(zfs.send.resume_info(cases[case].token).error)
        assert texts[0] == texts[1] == 'Cannot resume send: Invalid cross-device link'

    def test_no_capture_says_incremental_source_no_longer_exists(self, version):
        for rid, rec in records(version, 'M2').items():
            assert 'incremental source' not in rec.stderr.lower(), rid
            assert 'incremental source' not in (rec.send_stderr or '').lower(), rid


class TestRealSendResumeM2:
    """A real `zfs send -t` (not `-nv`) on a token whose incremental source is gone: the wording differs on 2.4."""

    EXPECTED = {
        V21: "Warning: cannot send 'zfsbcapc/M2/s/i_from_destroyed@b': Invalid cross-device link",
        V22: "Warning: cannot send 'zfsbcapc/M2/s/i_from_destroyed@b': Invalid cross-device link",
        V24: "Warning: cannot send 'zfsbcapc/M2/s/i_from_destroyed@b': No such file or directory",
    }

    def test_destroyed_incremental_source_per_version(self, version, mock_subprocess):
        rec = records(version, 'M2')['M2/i_from_destroyed.9_resume_try']
        assert sender_rc(rec) == 255
        stream_failure(mock_subprocess, rec.send_stderr, sender_rc(rec))
        with pytest.raises(Exception) as exc:
            zfs.send.resume(rec.token)
        assert argv_of(mock_subprocess) == captured_argv(rec)
        assert str(exc.value) == self.EXPECTED[version]
        assert str(exc.value) == failure_message(rec.send_stderr)

    def test_2_4_says_no_such_file_where_older_versions_say_invalid_cross_device_link(self):
        for case in ('i_from_destroyed', 'i_from_recreated', 'i_sender_moved'):
            texts = {v: records(v, 'M2')['M2/%s.9_resume_try' % case].send_stderr for v in VERSIONS}
            assert 'Invalid cross-device link' in texts[V21] and 'Invalid cross-device link' in texts[V22], case
            assert 'No such file or directory' in texts[V24], case
            assert 'Invalid cross-device link' not in texts[V24], case

    def test_snapshot_destroyed_or_recreated_has_the_same_text_as_send_nv_t(self, version, mock_subprocess):
        for case in ('i_to_destroyed', 'i_to_recreated', 'f_to_destroyed', 'f_to_recreated'):
            rec = records(version, 'M2')['M2/%s.9_resume_try' % case]
            stream_failure(mock_subprocess, rec.send_stderr, sender_rc(rec))
            with pytest.raises(Exception) as exc:
                zfs.send.resume(rec.token)
            assert str(exc.value) == STALE_ERRORS[case], case


class TestStaleTokenClassification:
    """`zfsbackup.remote._STALE_TOKEN_ERRORS` against what zfs really says (the constant is imported, not copied)."""

    @pytest.fixture
    def remote(self):
        from zfsbackup import remote
        return remote

    DEFINITIVE = ('i_from_destroyed', 'i_from_recreated', 'i_to_destroyed', 'i_to_recreated',
                  'f_to_destroyed', 'f_to_recreated')

    def test_destroyed_source_destroyed_target_and_recreated_target_are_definitive(self, remote, version,
                                                                                     mock_subprocess):
        cases = _m2_cases(version, '.8_nv_t')
        for case in self.DEFINITIVE:
            replay(mock_subprocess, cases[case])
            error = zfs.send.resume_info(cases[case].token).error
            assert remote._token_error_is_definitive(error), (case, error)

    def test_each_definitive_text_is_matched_by_the_constant(self, remote, version):
        """The classification does not need the helper's second clause: a marker of the constant matches."""
        cases = _m2_cases(version, '.8_nv_t')
        for case in self.DEFINITIVE:
            text = failure_message(cases[case].stderr).lower()
            assert any(m in text for m in remote._STALE_TOKEN_ERRORS), case

    def test_the_moved_sender_text_is_classified_like_a_destroyed_source(self, remote, version, mock_subprocess):
        """Known limitation: zfs gives a sender moved to another parent (when it fails) the same error as a destroyed
        incremental source, so a text match calls it definitive too."""
        cases = _m2_cases(version, '.8_nv_t')
        replay(mock_subprocess, cases['i_sender_moved'])
        error = zfs.send.resume_info(cases['i_sender_moved'].token).error
        assert remote._token_error_is_definitive(error) is True

    def test_tokens_that_still_resume_have_no_error_to_classify(self, version, mock_subprocess):
        cases = _m2_cases(version, '.8_nv_t')
        for case in RESUMABLE:
            replay(mock_subprocess, cases[case])
            assert zfs.send.resume_info(cases[case].token).error is None, case

    def test_other_failures_are_not_definitive(self, remote, version):
        """Stderr from the real resume or the receive side is not a stale-token verdict."""
        rec = records(version, 'M2')
        for rid in ('M2/i_from_destroyed.9_resume_try', 'M2/i_receiver_replaced.9_resume_try',
                    'M2/f_receiver_replaced.9_resume_try'):
            assert not remote._token_error_is_definitive(failure_message(rec[rid].stderr)), rid
        assert not remote._token_error_is_definitive('Cannot resume send: Something else')
        assert not remote._token_error_is_definitive('')


# --------------------------------------------------------------------------------------------------------------
# M3 / Z8: zfs destroy -v -p
# --------------------------------------------------------------------------------------------------------------

def destroy_call(rec):
    """(positional target, kwargs) reproducing a captured `zfs destroy -v -p ...` through libzfseasy."""
    parts = shlex.split(rec.cmd)
    assert parts[1] == 'destroy'
    flags, target = set(parts[2:-1]), parts[-1]
    dataset, _, snaps = target.partition('@')
    ds = Filesystem(dataset)
    if '%' in snaps:
        first, last = snaps.split('%')
        arg = SnapshotRange(first=Snapshot(ds, first), last=Snapshot(ds, last))
    elif ',' in snaps:
        arg = [Snapshot(ds, s) for s in snaps.split(',')]
    else:
        arg = Snapshot(ds, snaps)
    return arg, dict(destroy='-n' not in flags, recursive='-r' in flags)


def destroy_records(version, section='M3_Z8'):
    return {i: r for i, r in load_section(version, section).items() if r.cmd.startswith('zfs destroy -')}


def printed_destroy_lines(rec):
    return [l.split('\t', 1)[1] for l in rec.stdout.splitlines() if l.startswith('destroy\t')]


class TestDestroyLinesM3:
    def test_single_range_batch_and_dry_run_lines(self, version, mock_subprocess):
        recs = destroy_records(version)
        ok = {i: r for i, r in recs.items() if r.rc == 0}
        assert len(ok) >= 12
        for rid, rec in ok.items():
            replay(mock_subprocess, rec)
            arg, kwargs = destroy_call(rec)
            result = zfs.destroy(arg, **kwargs)
            assert_runs_captured_command(mock_subprocess, rec)
            assert result == printed_destroy_lines(rec), rid
            assert not any(l.startswith('reclaim') for l in result), rid

    @pytest.mark.parametrize('case,expected', [
        ('held.11_dry_single', ['zfsbcapc/M3_Z8/held@s2']),
        ('held.10_dry_batch', ['zfsbcapc/M3_Z8/held@s%d' % i for i in (1, 2, 3)]),
        ('held.12_dry_range', ['zfsbcapc/M3_Z8/held@s%d' % i for i in (1, 2, 3)]),
        ('rec.10_dry_r', ['zfsbcapc/M3_Z8/rec@t1', 'zfsbcapc/M3_Z8/rec/c@t1']),
        ('rec.11_dry_r_batch', ['zfsbcapc/M3_Z8/rec@t1', 'zfsbcapc/M3_Z8/rec@t2',
                                'zfsbcapc/M3_Z8/rec/c@t1', 'zfsbcapc/M3_Z8/rec/c@t2']),
        ('held.50_control_batch_without_held', ['zfsbcapc/M3_Z8/held@s1', 'zfsbcapc/M3_Z8/held@s3']),
    ])
    def test_literal_lines(self, version, case, expected, mock_subprocess):
        rec = load_section(version, 'M3_Z8')['M3_Z8/' + case]
        replay(mock_subprocess, rec)
        arg, kwargs = destroy_call(rec)
        assert zfs.destroy(arg, **kwargs) == expected

    def test_the_dry_run_succeeds_on_a_held_or_busy_snapshot(self, version, mock_subprocess):
        """A clean dry run is no promise (DestroyCommand's docstring): the hold is only checked when it is real."""
        for case in ('held.10_dry_batch', 'held.11_dry_single', 'busy.10_dry_batch', 'busy.11_dry_single',
                     'rec.10_dry_r', 'rec.11_dry_r_batch'):
            rec = load_section(version, 'M3_Z8')['M3_Z8/' + case]
            assert rec.rc == 0, case
        for case in ('held.20_batch', 'held.30_single', 'busy.20_batch', 'rec.20_r'):
            assert load_section(version, 'M3_Z8')['M3_Z8/' + case].rc == 1, case

    def test_the_dry_run_fails_on_a_snapshot_with_a_clone(self, version, mock_subprocess):
        for case in ('cloned.10_dry_batch', 'cloned.11_dry_single', 'cloned.12_dry_range', 'cloned.61_two_clones_dry'):
            rec = load_section(version, 'M3_Z8')['M3_Z8/' + case]
            assert rec.rc == 1, case
            replay(mock_subprocess, rec)
            arg, kwargs = destroy_call(rec)
            with pytest.raises(Exception) as exc:
                zfs.destroy(arg, **kwargs)
            assert "snapshot has dependent clones" in str(exc.value), case

    def test_keyless_raw_copy_destroys_like_any_dataset(self, version, mock_subprocess):
        """Z7: `destroy -v -p` of a snapshot of a raw copy whose key was never loaded."""
        recs = {i: r for i, r in destroy_records(version, 'Z7').items() if r.rc == 0}
        assert len(recs) >= 5
        for rid, rec in recs.items():
            replay(mock_subprocess, rec)
            arg, kwargs = destroy_call(rec)
            assert zfs.destroy(arg, **kwargs) == printed_destroy_lines(rec), rid


class TestFailedBatchM3:
    FAILURES = ('held.20_batch', 'held.30_single', 'held.40_range', 'cloned.20_batch', 'cloned.30_single',
                'cloned.40_range', 'cloned.60_two_clones_single', 'busy.20_batch', 'busy.30_single', 'busy.40_range',
                'rec.20_r', 'rec.30_r_batch', 'rec.40_child_single')

    def test_list_mode_raises_and_yields_nothing(self, version, mock_subprocess):
        for case in self.FAILURES:
            rec = load_section(version, 'M3_Z8')['M3_Z8/' + case]
            assert rec.rc == 1, case
            replay(mock_subprocess, rec)
            arg, kwargs = destroy_call(rec)
            with pytest.raises(Exception) as exc:
                zfs.destroy(arg, **kwargs)  # list mode: the result is never assigned, so nothing is "returned"
            assert str(exc.value) == failure_message(rec.stderr), case

    def test_lazy_mode_yields_its_lines_before_raising(self, version, mock_subprocess):
        for case in self.FAILURES:
            rec = load_section(version, 'M3_Z8')['M3_Z8/' + case]
            replay(mock_subprocess, rec)
            arg, kwargs = destroy_call(rec)
            gen = zfs.destroy(arg, lazy=True, **kwargs)
            got = []
            with pytest.raises(Exception) as exc:
                for line in gen:
                    got.append(line)
            # zfs printed these `destroy` lines and then failed: nothing was destroyed (the captures' listings after
            # the failure are identical to the ones before it), but they are what the lazy iterator saw.
            assert got == printed_destroy_lines(rec), case
            assert str(exc.value) == failure_message(rec.stderr), case

    def test_nothing_is_destroyed_by_a_failed_batch(self, version):
        """The capture's own `zfs list` after each failed batch equals the one before it."""
        recs = load_section(version, 'M3_Z8')
        for before, after in (('held.01_list_start', 'held.21_list_after_batch'),
                              ('held.01_list_start', 'held.41_list_after_range'),
                              ('cloned.01_list_start', 'cloned.21_list_after_batch'),
                              ('busy.01_list_start', 'busy.21_list_after_batch'),
                              ('rec.01_list_start', 'rec.21_list_after_r'),
                              ('rec.01_list_start', 'rec.31_list_after_r_batch')):
            assert recs['M3_Z8/' + before].stdout == recs['M3_Z8/' + after].stdout, (before, after)

    def test_stdout_lines_of_a_failed_destroy_are_not_a_destruction_report(self, version, mock_subprocess):
        """A caller that read `destroy` lines from a failed command would believe in snapshots that still exist:
        the failed single `destroy` printed its line, and the snapshot is still listed afterwards."""
        recs = load_section(version, 'M3_Z8')
        failed = recs['M3_Z8/held.30_single']
        assert printed_destroy_lines(failed) == ['zfsbcapc/M3_Z8/held@s2']
        assert 'zfsbcapc/M3_Z8/held@s2\t' in recs['M3_Z8/held.31_list_after_single'].stdout

    def test_busy_snapshot_reports_dataset_is_busy_on_every_version(self, version, mock_subprocess):
        rec = load_section(version, 'M3_Z8')['M3_Z8/busy.30_single']
        replay(mock_subprocess, rec)
        arg, kwargs = destroy_call(rec)
        with pytest.raises(Exception) as exc:
            zfs.destroy(arg, **kwargs)
        assert str(exc.value) == 'Cannot destroy snapshot zfsbcapc/M3_Z8/busy@s4: dataset is busy'

    HELD = {
        V21: 'Cannot destroy snapshot zfsbcapc/M3_Z8/held@s2: dataset is busy',
        V22: 'Cannot destroy snapshot zfsbcapc/M3_Z8/held@s2: dataset is busy',
        V24: "Cannot destroy snapshot zfsbcapc/M3_Z8/held@s2: it's being held. "
             "Run 'zfs holds -r zfsbcapc/M3_Z8/held@s2' to see holders.",
    }

    def test_held_snapshot_wording_per_version(self, version, mock_subprocess):
        """2.1 and 2.2 call a hold 'dataset is busy' (the same as a send in progress); 2.4 says it is being held."""
        for case in ('held.20_batch', 'held.30_single', 'held.40_range'):
            rec = load_section(version, 'M3_Z8')['M3_Z8/' + case]
            replay(mock_subprocess, rec)
            arg, kwargs = destroy_call(rec)
            with pytest.raises(Exception) as exc:
                zfs.destroy(arg, **kwargs)
            assert str(exc.value) == self.HELD[version], case

    def test_recursive_destroy_names_the_held_child_snapshot(self, version, mock_subprocess):
        rec = load_section(version, 'M3_Z8')['M3_Z8/rec.20_r']
        replay(mock_subprocess, rec)
        arg, kwargs = destroy_call(rec)
        with pytest.raises(Exception) as exc:
            zfs.destroy(arg, **kwargs)
        assert 'zfsbcapc/M3_Z8/rec/c@t1' in str(exc.value)
        assert str(exc.value).startswith('Cannot destroy snapshot zfsbcapc/M3_Z8/rec/c@t1: ')

    def test_dependent_clones_are_listed_in_the_message_in_zfs_order(self, version, mock_subprocess):
        """The clone list is in an unstable order (compare as a set); every clone is named, one per line."""
        rec = load_section(version, 'M3_Z8')['M3_Z8/cloned.60_two_clones_single']
        replay(mock_subprocess, rec)
        arg, kwargs = destroy_call(rec)
        with pytest.raises(Exception) as exc:
            zfs.destroy(arg, **kwargs)
        lines = str(exc.value).split('\n')
        assert lines[0] == "Cannot destroy 'zfsbcapc/M3_Z8/cloned@s4': snapshot has dependent clones"
        assert lines[1] == "use '-R' to destroy the following datasets:"
        # continuation lines are the real zfs text: clone names keep their case
        assert set(lines[2:]) == {'zfsbcapc/M3_Z8/clone2', 'zfsbcapc/M3_Z8/clone3'}


# --------------------------------------------------------------------------------------------------------------
# M4: zfs list with createtxg, guid, userrefs, clones
# --------------------------------------------------------------------------------------------------------------

M4_PROPS = ['createtxg', 'guid', 'userrefs', 'clones']


def list_call(rec):
    """kwargs reproducing a captured `zfs list -H [-r] -t T -o name,type,... [-s|-S key] roots` call."""
    parts = shlex.split(rec.cmd)[2:]
    kwargs = {}
    i = 0
    roots = []
    while i < len(parts):
        p = parts[i]
        if p == '-H' or p == '-p':
            pass
        elif p == '-r':
            kwargs['recursive'] = True
        elif p == '-t':
            kwargs['types'] = parts[i + 1]
            i += 1
        elif p in ('-s', '-S'):
            kwargs['sort'] = parts[i + 1]
            kwargs['asc'] = p == '-s'
            i += 1
        elif p == '-o':
            cols = parts[i + 1].split(',')
            kwargs['properties'] = [c for c in cols if c not in ('name', 'type')]
            i += 1
        else:
            roots.append(p)
        i += 1
    kwargs['roots'] = [ZFS.from_name(r) if '@' in r else Filesystem(r) for r in roots]
    return kwargs


def expected_rows(rec):
    return [line.split('\t') for line in rec.stdout.splitlines()]


def m4_listings(version):
    out = {}
    for rid, rec in load_section(version, 'M4').items():
        if rec.cmd.startswith('zfs list -H') and 'name,type,createtxg,guid,userrefs,clones' in rec.cmd:
            out[rid] = rec
    return out


class TestListM4:
    def test_rows_parse_with_createtxg_guid_userrefs_and_clones(self, version, mock_subprocess):
        listings = {i: r for i, r in m4_listings(version).items() if ' -p ' not in r.cmd}  # libzfseasy never adds -p
        assert len(listings) >= 10
        for rid, rec in listings.items():
            replay(mock_subprocess, rec)
            result = zfs.list(**list_call(rec))
            assert_runs_captured_command(mock_subprocess, rec)
            rows = expected_rows(rec)
            assert [o.name for o in result] == [r[0] for r in rows], rid
            for obj, (name, dstype, txg, guid, userrefs, clones) in zip(result, rows):
                assert isinstance(obj, Snapshot if dstype == 'snapshot' else Filesystem), (rid, name)
                assert obj['createtxg'].value == txg, (rid, name)
                assert obj['guid'].value == guid, (rid, name)

    def test_dash_is_none_and_a_hold_count_is_kept(self, version, mock_subprocess):
        rec = load_section(version, 'M4')['M4/40_two_holds']
        replay(mock_subprocess, rec)
        by_name = {o.name: o for o in zfs.list(**list_call(rec))}
        held = by_name['zfsbcapc/M4/l@s1']
        assert held['userrefs'].value == '2'
        assert held['clones'] is None
        plain = by_name['zfsbcapc/M4/l@s3']
        assert plain['userrefs'].value == '0'      # a snapshot with no holds is "0", not "-"
        assert plain['clones'].value == 'zfsbcapc/M4/c3'

    def test_a_comma_joined_clones_list_is_one_value_in_zfs_order(self, version, mock_subprocess):
        rec = load_section(version, 'M4')['M4/20_two_clones_same_snapshot']
        replay(mock_subprocess, rec)
        by_name = {o.name: o for o in zfs.list(**list_call(rec))}
        value = by_name['zfsbcapc/M4/l@s2']['clones'].value
        assert set(value.split(',')) == {'zfsbcapc/M4/c1', 'zfsbcapc/M4/c2'}
        assert len(value.split(',')) == 2

    def test_the_clone_order_zfs_prints_is_unstable_and_either_order_parses_alike(self, version, mock_subprocess):
        """The default- and C-locale passes printed `c1,c2` and `c2,c1` for the same snapshot. Whichever order, the
        set of clones is the same; a consumer must not compare the string."""
        rec = load_section(version, 'M4')['M4/20_two_clones_same_snapshot']
        sets = []
        for flip in (False, True):
            lines = []
            for row in expected_rows(rec):
                if flip and ',' in row[5]:
                    row[5] = ','.join(reversed(row[5].split(',')))
                lines.append('\t'.join(row) + '\n')
            mock_subprocess.setup(stdout=lines)
            by_name = {o.name: o for o in zfs.list(**list_call(rec))}
            sets.append(set(by_name['zfsbcapc/M4/l@s2']['clones'].value.split(',')))
        assert sets[0] == sets[1] == {'zfsbcapc/M4/c1', 'zfsbcapc/M4/c2'}

    def test_clone_rows_across_snapshots_keep_their_own_clones(self, version, mock_subprocess):
        rec = load_section(version, 'M4')['M4/30_three_clones_two_snapshots']
        replay(mock_subprocess, rec)
        by_name = {o.name: o for o in zfs.list(**list_call(rec))}
        assert set(by_name['zfsbcapc/M4/l@s2']['clones'].value.split(',')) == {'zfsbcapc/M4/c1', 'zfsbcapc/M4/c2'}
        assert by_name['zfsbcapc/M4/l@s3']['clones'].value == 'zfsbcapc/M4/c3'
        assert by_name['zfsbcapc/M4/l@s1']['clones'] is None
        assert by_name['zfsbcapc/M4/l/child@s2']['clones'] is None

    def test_filesystem_rows_have_none_for_the_snapshot_only_columns(self, version, mock_subprocess):
        rec = load_section(version, 'M4')['M4/03_all_types']
        replay(mock_subprocess, rec)
        result = zfs.list(**list_call(rec))
        fs = [o for o in result if '@' not in o.name]
        assert [o.name for o in fs] == ['zfsbcapc/M4/l', 'zfsbcapc/M4/l/child']
        assert all(isinstance(o, Filesystem) for o in fs)
        assert all(isinstance(o, Snapshot) for o in result if '@' in o.name)
        for o in fs:
            assert o['guid'].value and o['createtxg'].value
            # a filesystem has no such properties: the `-` cells are dropped, and asking for them is an error
            for prop in ('userrefs', 'clones'):
                with pytest.raises(ValueError):
                    o[prop]

    def test_sorted_by_createtxg_ascending_and_descending(self, version, mock_subprocess):
        asc = load_section(version, 'M4')['M4/01_noclones_sorted']
        desc = load_section(version, 'M4')['M4/02_noclones_sorted_rev']
        for rec, reverse in ((asc, False), (desc, True)):
            replay(mock_subprocess, rec)
            result = zfs.list(**list_call(rec))
            txgs = [int(o['createtxg'].value) for o in result]
            assert txgs == sorted(txgs, reverse=reverse)
            assert sorted(argv_of(mock_subprocess)) == sorted(captured_argv(rec))

    def test_snapshots_of_one_snapshot_r_call_share_a_createtxg(self, version, mock_subprocess):
        """`snapshot -r` stamps a parent and its child with the same createtxg: a sort by txg alone is not a total
        order of snapshots."""
        rec = load_section(version, 'M4')['M4/01_noclones_sorted']
        replay(mock_subprocess, rec)
        txgs = {}
        for o in zfs.list(**list_call(rec)):
            txgs.setdefault(o['createtxg'].value, []).append(o.name)
        assert all(len(names) == 2 for names in txgs.values())
        rec = load_section(version, 'M4')['M4/51_equal_txg_two_datasets']
        assert len({row[1] for row in expected_rows(rec)}) == 1

    def test_guids_are_unsigned_64_bit_and_not_truncated(self, version, mock_subprocess):
        rec = load_section(version, 'M4')['M4/00_noclones']
        replay(mock_subprocess, rec)
        guids = [int(o['guid'].value) for o in zfs.list(**list_call(rec))]
        assert max(guids) > 2 ** 63  # these captures do contain guids that a signed int would mangle
        assert guids == [int(r[3]) for r in expected_rows(rec)]

    def test_listing_two_filesystems_by_name_returns_both(self, version, mock_subprocess):
        rec = load_section(version, 'M4')['M4/60_filesystem_rows']
        replay(mock_subprocess, rec)
        result = zfs.list(**list_call(rec))
        assert sorted(o.name for o in result) == ['zfsbcapc/M4/c1', 'zfsbcapc/M4/l']

    def test_zfs_snapshot_with_two_snapshots_of_one_filesystem_is_refused(self, version, mock_subprocess):
        rec = load_section(version, 'M4')['M4/49_two_snapshots_one_fs_one_command']
        assert rec.rc == 1
        assert failure_message(rec.stderr) == \
            'Cannot create snapshots : multiple snapshots of same fs not allowed\nno snapshots were created'


# --------------------------------------------------------------------------------------------------------------
# receive -A
# --------------------------------------------------------------------------------------------------------------

class TestReceiveAbortWordings:
    @pytest.fixture
    def api(self):
        from zfsbackup import api
        return api

    def _abort(self, mock, rec, dataset):
        replay(mock, rec)
        zfs.receive.abort(Filesystem(dataset))
        assert argv_of(mock) == captured_argv(rec)

    def test_success_has_no_output_and_does_not_raise(self, version, mock_subprocess):
        rec = load_section(version, 'M2')['M2/i_from_destroyed.9b_abort']
        assert rec.rc == 0 and rec.stdout == '' and rec.stderr == ''
        self._abort(mock_subprocess, rec, 'zfsbcapc/M2/r/i_from_destroyed')

    def test_nothing_to_abort_matches_the_already_clean_wording(self, version, api, mock_subprocess):
        """rc 1, "does not have any resumable receive state to abort": the normal "already clean" for the API."""
        checked = 0
        for rid, rec in records(version, 'M2', suffix='.9b_abort').items():
            if rec.rc == 0:
                continue
            replay(mock_subprocess, rec)
            with pytest.raises(Exception) as exc:
                zfs.receive.abort(Filesystem(shlex.split(rec.cmd)[-1]))
            assert rec.rc == 1 and 'does not have any resumable receive state to abort' in str(exc.value), rid
            assert any(m in str(exc.value).lower() for m in api._ABORT_ALREADY_CLEAN), rid
            checked += 1
        assert checked >= 5

    def test_a_dataset_that_does_not_exist_matches_too(self, version, api, mock_subprocess):
        """rc 2: the dataset (a full receive's partial state) is gone; zfs prints its usage after the one line."""
        for rid in ('Z3/raw.a.16_abort_again', 'Z3/plain.a.16_abort_again'):
            rec = load_section(version, 'Z3')[rid]
            assert rec.rc == 2
            replay(mock_subprocess, rec)
            with pytest.raises(Exception) as exc:
                zfs.receive.abort(Filesystem(shlex.split(rec.cmd)[-1]))
            text = str(exc.value)
            assert text.startswith("Cannot open 'zfsbcapc/Z3/srv/"), rid
            assert "dataset does not exist" in text.lower()
            assert any(m in text.lower() for m in api._ABORT_ALREADY_CLEAN), rid

    def test_every_non_zero_abort_in_the_captures_is_one_of_the_two_wordings(self, version, api, mock_subprocess):
        n = 0
        for section in ('M2', 'Z3'):
            for rid, rec in load_section(version, section).items():
                if rec.cmd.startswith('zfs receive -A') and rec.rc != 0:
                    n += 1
                    assert any(m in rec.stderr.lower() for m in api._ABORT_ALREADY_CLEAN), rid
        assert n >= 5


# --------------------------------------------------------------------------------------------------------------
# Z7: a raw copy with no key
# --------------------------------------------------------------------------------------------------------------

class TestKeylessRawCopyZ7:
    DS = 'zfsbcapc/Z7/srv/rx'

    def test_get_local_all_lists_the_user_properties_without_the_key(self, version, mock_subprocess):
        """`get -s local all` through zfsbackup's own two-phase reader. The capture's four-column rows
        (`name,property,value,source`, taken WITH `-s local`, so no `type` row) are the real values and sources: the
        discovery (`-o name,property,source`) is the same rows without the value column, and the values read is an
        explicit-list answer (`name`, `type`, then the requested names) built from the same rows."""
        from zfsbackup.backup_manager import read_zfsbackup_properties
        from _get_support import explicit_lines
        rec = load_section(version, 'Z7')['Z7/20_get_local_all']
        assert rec.rc == 0
        rows = [line.split('\t') for line in rec.stdout.splitlines()]
        assert all(len(r) == 4 and r[0] == self.DS for r in rows)
        discovery = [f'{r[0]}\t{r[1]}\t{r[3]}\n' for r in rows]
        ours = [r[1] for r in rows if r[1].startswith('org.zfsbackup:')]
        mock_subprocess.setup_multi(
            (discovery,),
            (explicit_lines(self.DS, ours, {r[1]: (r[2], r[3]) for r in rows}),),
        )
        props = read_zfsbackup_properties(Filesystem(self.DS))
        assert props == {
            'org.zfsbackup:config': 'eyJ2IjoxLCJkYXRhc2V0IjoicG9vbC9kYXRhIiwiZnJlcXVlbmN5IjoiMWgifQ==',
            'org.zfsbackup:client_id': 'client-1',
            'org.zfsbackup:source_dataset': 'pool/data',
            'org.zfsbackup:anchor.d1': 'snap_20260101000000',
        }
        # the native `keylocation=prompt` row of the raw copy is in the output and is not a zfsbackup property
        assert 'keylocation\tprompt\tlocal' in rec.stdout
        assert 'keylocation' not in ''.join(props)

    def test_get_value_of_each_user_property(self, version, mock_subprocess):
        rec = load_section(version, 'Z7')['Z7/25_get_value']
        replay(mock_subprocess, rec)
        value = zfs.get.value(Filesystem(self.DS), 'org.zfsbackup:config')
        assert argv_of(mock_subprocess) == captured_argv(rec)
        assert value == 'eyJ2IjoxLCJkYXRhc2V0IjoicG9vbC9kYXRhIiwiZnJlcXVlbmN5IjoiMWgifQ=='

    def test_get_value_local_only_reads_the_same_value(self, version, mock_subprocess):
        rec = load_section(version, 'Z7')['Z7/25_get_value']
        replay(mock_subprocess, rec)
        assert zfs.get.value(Filesystem(self.DS), 'org.zfsbackup:config', sources='local') == rec.stdout.rstrip('\n')
        assert argv_of(mock_subprocess) == ['get', '-H', '-o', 'value', '-s', 'local', 'org.zfsbackup:config', self.DS]

    def test_wildcard_property_list_is_refused_by_zfs_and_by_libzfseasy_before_zfs_runs(self, version, mock_subprocess):
        rec = load_section(version, 'Z7')['Z7/23_get_local_wildcard']
        assert rec.rc == 2 and "bad property list: invalid property 'org.zfsbackup:*'" in rec.stderr
        with pytest.raises(ValueError):
            zfs.get.value(Filesystem(self.DS), 'org.zfsbackup:*')
        mock_subprocess.assert_not_called()

    def test_keystatus_and_encryptionroot_of_the_copy(self, version, mock_subprocess):
        rec = load_section(version, 'Z7')['Z7/10_keystatus']
        rows = [l.split('\t') for l in rec.stdout.splitlines()]
        expected = {prop: value for _name, prop, value in rows}
        assert expected == {'keystatus': 'unavailable', 'encryptionroot': self.DS}
        for prop, value in expected.items():
            mock_subprocess.setup(stdout=[value + '\n'])  # derived: `get -o value` prints the value column
            assert zfs.get.value(Filesystem(self.DS), prop) == value

    def test_snapshot_listing_with_encryption_columns(self, version, mock_subprocess):
        """Derived: the capture's `-o name,createtxg,...` rows get the `type` column libzfseasy asks for."""
        rec = load_section(version, 'Z7')['Z7/11_list_cols']
        mock_subprocess.setup(stdout=with_type_column(rec.stdout))
        result = zfs.list(roots=Filesystem(self.DS), types='snapshot', recursive=True,
                          properties=['createtxg', 'guid', 'userrefs', 'clones', 'encryption', 'keystatus'])
        assert [o.name for o in result] == [self.DS + '@a', self.DS + '@b', self.DS + '@c']
        for o in result:
            assert o['encryption'].value == 'aes-256-gcm'
            assert o['keystatus'].value == 'unavailable'
            assert o['clones'] is None and o['userrefs'].value == '0'


# --------------------------------------------------------------------------------------------------------------
# EP: encryption properties
# --------------------------------------------------------------------------------------------------------------

EP_PROPS = ['encryption', 'keystatus', 'encryptionroot', 'keyformat', 'keylocation']


def ep_rows(rec):
    """`get -o name,property,value,source` rows: {(name, property): (value, source)}."""
    out = {}
    for line in rec.stdout.splitlines():
        name, prop, value, source = line.split('\t')
        out[(name, prop)] = (value, source)
    return out


class TestEncryptionPropertiesEP:
    E = 'zfsbcapc/EP/e'
    P = 'zfsbcapc/EP/p'

    def test_keystatus_value_reads_exactly_as_zfs_prints_it(self, version, mock_subprocess):
        rec = load_section(version, 'EP')['EP/33_keystatus_value']  # key unloaded
        replay(mock_subprocess, rec)
        assert zfs.get.value(Filesystem(self.E), 'keystatus') == 'unavailable'
        assert argv_of(mock_subprocess) == captured_argv(rec)

    def test_keystatus_of_an_unencrypted_dataset_is_dash_which_reads_as_none(self, version, mock_subprocess):
        rec = load_section(version, 'EP')['EP/12_plain_keystatus_value']
        assert rec.stdout == '-\n'
        replay(mock_subprocess, rec)
        assert zfs.get.value(Filesystem(self.P), 'keystatus') is None
        replay(mock_subprocess, rec)
        assert zfs.get.value(Filesystem(self.P), 'keystatus', raw=True) == '-'
        assert argv_of(mock_subprocess) == captured_argv(rec)

    @pytest.mark.parametrize('rid,ds,state', [
        ('EP/00_fs_loaded', E, 'available'),
        ('EP/30_fs_unloaded', E, 'unavailable'),
    ])
    def test_every_encryption_property_of_an_encrypted_filesystem(self, version, rid, ds, state, mock_subprocess):
        """Derived: one `get -o value` call per row of the capture (the value column of that row)."""
        rows = ep_rows(load_section(version, 'EP')[rid])
        expected = {
            'encryption': 'aes-256-gcm', 'keystatus': state, 'encryptionroot': ds, 'keyformat': 'passphrase',
        }
        for prop, value in expected.items():
            mock_subprocess.setup(stdout=[rows[(ds, prop)][0] + '\n'])
            assert zfs.get.value(Filesystem(ds), prop) == value, prop
        # keylocation is a local `file://` path
        assert rows[(ds, 'keylocation')][1] == 'local'
        assert rows[(ds, 'keylocation')][0].startswith('file://')

    def test_every_encryption_property_of_an_unencrypted_filesystem(self, version, mock_subprocess):
        rows = ep_rows(load_section(version, 'EP')['EP/10_plain_fs'])
        expected = {'encryption': 'off', 'keystatus': None, 'encryptionroot': None, 'keyformat': 'none',
                    'keylocation': 'none'}
        for prop, value in expected.items():
            mock_subprocess.setup(stdout=[rows[(self.P, prop)][0] + '\n'])
            assert zfs.get.value(Filesystem(self.P), prop) == value, prop

    def test_a_snapshot_has_its_datasets_key_state(self, version, mock_subprocess):
        for rid, state in (('EP/01_snap_loaded', 'available'), ('EP/31_snap_unloaded', 'unavailable')):
            rows = ep_rows(load_section(version, 'EP')[rid])
            snap = Snapshot(Filesystem(self.E), 'a')
            for prop, value in (('encryption', 'aes-256-gcm'), ('keystatus', state), ('encryptionroot', self.E)):
                mock_subprocess.setup(stdout=[rows[(snap.name, prop)][0] + '\n'])
                assert zfs.get.value(snap, prop) == value, (rid, prop)
            # keyformat and keylocation are `-` on a snapshot
            assert rows[(snap.name, 'keyformat')][0] == '-' and rows[(snap.name, 'keylocation')][0] == '-'

    @pytest.mark.parametrize('rid,state', [('EP/02_list_loaded', 'available'), ('EP/32_list_unloaded', 'unavailable')])
    def test_list_with_encryption_columns(self, version, rid, state, mock_subprocess):
        """Derived: the capture's `-o name,encryption,...` rows get the `type` column libzfseasy asks for."""
        rec = load_section(version, 'EP')[rid]
        mock_subprocess.setup(stdout=with_type_column(rec.stdout))
        result = zfs.list(roots=Filesystem(self.E), types='all', recursive=True, properties=EP_PROPS)
        assert [o.name for o in result] == [self.E, self.E + '@a', self.E + '@b', self.E + '@c']
        for o in result:
            assert o['encryption'].value == 'aes-256-gcm'
            assert o['keystatus'].value == state
            assert o['encryptionroot'].value == self.E
        assert result[0]['keyformat'].value == 'passphrase'
        assert result[0]['keylocation'].value.startswith('file://')
        for snap in result[1:]:
            for prop in ('keyformat', 'keylocation'):  # `-` on a snapshot, and libzfseasy does not model them there
                with pytest.raises(ValueError):
                    snap[prop]

    def test_list_of_an_unencrypted_dataset(self, version, mock_subprocess):
        rec = load_section(version, 'EP')['EP/13_plain_list']
        mock_subprocess.setup(stdout=with_type_column(rec.stdout))
        result = zfs.list(roots=Filesystem(self.P), types='all', recursive=True, properties=EP_PROPS)
        for o in result:
            assert o['encryption'].value == 'off'
            assert o['keystatus'] is None and o['encryptionroot'] is None

    def test_unload_key_errors(self, version, mock_subprocess):
        for rid, text in (('EP/20_unload_mounted', "Key unload error: 'zfsbcapc/EP/e' is busy."),
                          ('EP/23_unload_again', "Key unload error: Key already unloaded for 'zfsbcapc/EP/e'.")):
            rec = load_section(version, 'EP')[rid]
            assert rec.rc == 255
            replay(mock_subprocess, rec)
            with pytest.raises(Exception) as exc:
                zfs.unload_key(Filesystem(self.E))
            assert str(exc.value) == text, rid


# --------------------------------------------------------------------------------------------------------------
# Z4: a plain send with the key unloaded
# --------------------------------------------------------------------------------------------------------------

class TestKeyUnloadedSendZ4:
    SNAP = Snapshot(Filesystem('zfsbcapc/Z4/cs/enc'), 'a')
    SNAP_B = Snapshot(Filesystem('zfsbcapc/Z4/cs/enc'), 'b')

    PLAIN_FULL = {
        V21: "Warning: cannot send 'zfsbcapc/Z4/cs/enc@a': dataset key must be loaded",
        V22: "Warning: cannot send 'zfsbcapc/Z4/cs/enc@a': dataset key must be loaded",
        # 2.4.1 reports a misleading reason: there is no incremental source in a full send
        V24: "Warning: cannot send 'zfsbcapc/Z4/cs/enc@a': incremental source ((null)) does not exist",
    }
    PLAIN_INCR = {
        V21: "Warning: cannot send 'zfsbcapc/Z4/cs/enc@b': dataset key must be loaded",
        V22: "Warning: cannot send 'zfsbcapc/Z4/cs/enc@b': dataset key must be loaded",
        V24: "Warning: cannot send 'zfsbcapc/Z4/cs/enc@b': incremental source (zfsbcapc/Z4/cs/enc@a) does not exist",
    }

    def test_plain_full_send_fails_at_send_time_with_a_version_specific_message(self, version, mock_subprocess):
        rec = load_section(version, 'Z4')['Z4/40_send_plain_full_wc']
        assert rec.rc == 1
        stream_failure(mock_subprocess, rec.stderr, rec.rc)
        with pytest.raises(Exception) as exc:
            zfs.send.snapshot(self.SNAP)
        assert argv_of(mock_subprocess) == captured_argv(rec)
        assert str(exc.value) == self.PLAIN_FULL[version]

    def test_plain_incremental_send_fails_the_same_way(self, version, mock_subprocess):
        rec = load_section(version, 'Z4')['Z4/41_send_plain_incr_wc']
        stream_failure(mock_subprocess, rec.stderr, rec.rc)
        with pytest.raises(Exception) as exc:
            zfs.send.snapshot(self.SNAP_B, since=self.SNAP)
        assert argv_of(mock_subprocess) == captured_argv(rec)
        assert str(exc.value) == self.PLAIN_INCR[version]

    def test_2_4_does_not_say_the_key_must_be_loaded(self):
        for rid in ('Z4/40_send_plain_full_wc', 'Z4/41_send_plain_incr_wc', 'Z4/44_send_plain_L_c_wc'):
            old = [load_section(v, 'Z4')[rid].stderr for v in (V21, V22)]
            new = load_section(V24, 'Z4')[rid].stderr
            assert all('key must be loaded' in t for t in old), rid
            assert 'key must be loaded' not in new, rid
            assert 'does not exist' in new, rid

    def test_a_dry_run_send_succeeds_with_the_key_unloaded_so_it_is_no_preflight(self, version):
        for rid in ('Z4/42_send_plain_nv', 'Z4/43_send_plain_nv_incr', 'Z4/32_send_w_nv'):
            assert load_section(version, 'Z4')[rid].rc == 0, rid

    def test_a_plain_token_cannot_be_resumed_with_the_key_unloaded_a_raw_one_can(self, version, mock_subprocess):
        plain = load_section(version, 'Z4')['Z4/51_resume_plain_token_key_unloaded']
        stream_failure(mock_subprocess, plain.send_stderr, sender_rc(plain))
        with pytest.raises(Exception) as exc:
            zfs.send.resume(plain.token)
        assert str(exc.value) == "Warning: cannot send 'zfsbcapc/Z4/cs/enc@a': source key must be loaded"

        raw = load_section(version, 'Z4')['Z4/53_resume_raw_token_key_unloaded']
        assert sender_rc(raw) == 0 and raw.send_stderr == ''

    def test_send_with_properties_but_not_raw_is_refused_for_an_encrypted_dataset(self, version, mock_subprocess):
        rec = load_section(version, 'Z4')['Z4/47_send_plain_p_nv']
        assert rec.rc == 1
        stream_failure(mock_subprocess, rec.stderr, rec.rc)
        with pytest.raises(Exception) as exc:
            zfs.send.snapshot(self.SNAP, properties=True)
        assert 'may not be sent with properties without the raw flag' in str(exc.value)


# --------------------------------------------------------------------------------------------------------------
# M7 (B-2b): what `zfs receive -F` does to snapshots newer than the base, and what the server therefore decides
# --------------------------------------------------------------------------------------------------------------

def m7(version):
    return load_section(version, 'M7')


def listed(rec):
    """`[(short name, guid, createtxg)]` of a captured `zfs list -H -t snapshot -o name,guid,createtxg` (note the
    column order: libzfseasy asks for `name,type,createtxg,guid`)."""
    rows = []
    for line in rec.stdout.splitlines():
        name, guid, txg = line.split('\t')
        rows.append((name.split('@', 1)[1], int(guid), int(txg)))
    return rows


def names_of(rec):
    return [n for n, _g, _t in listed(rec)]


def as_server_rows(rec):
    """The capture re-ordered into the columns `api.server_snapshots` asks for (`name,type,createtxg,guid`)."""
    return ['%s\tsnapshot\t%d\t%d\n' % (line.split('\t')[0], int(line.split('\t')[2]), int(line.split('\t')[1]))
            for line in rec.stdout.splitlines()]


def server_view(mock, rec):
    """What the server's `server_snapshots` makes of `rec` (through the real `zfs list` parser)."""
    from zfsbackup.api import server_snapshots
    replay(mock, rec, stdout=as_server_rows(rec))
    return server_snapshots(Filesystem(rec.cmd.split()[-1]))


def decide(rows, base_name, base_guid, discard=()):
    from zfsbackup.api import StreamBase, decide_receive
    return decide_receive('srv/ds', StreamBase(base_name, base_guid, frozenset(discard)), rows)


class TestForcedIncrementalDestroysNewerSnapshotsM7:
    """U1/U2: the premise of the B-2b guard, on every version."""

    def test_u1_f_destroys_the_newer_snapshot_and_without_f_zfs_refuses(self, version):
        recs = m7(version)
        before, after = names_of(recs['M7/U1.a1_before']), names_of(recs['M7/U1.a3_after'])
        assert before == ['old', 'new1'] and after == ['old', 'new2']          # new1 is GONE after -F
        assert recs['M7/U1.a2_F_incr'].rc == 0 and ' receive -F ' in recs['M7/U1.a2_F_incr'].cmd
        refused = recs['M7/U1.b2_noF_incr']
        assert refused.rc == 1 and ' receive -F ' not in refused.cmd
        assert 'has been modified' in refused.stderr and 'since most recent snapshot' in refused.stderr
        assert names_of(recs['M7/U1.b3_after']) == ['old', 'new1']             # untouched

    def test_u1_the_same_when_the_sender_no_longer_has_the_snapshot(self, version):
        """The rollback / restore situation: the sender lost @new1, the receiver (the server) keeps it
        until a `-F` stream from @old removes it."""
        recs = m7(version)
        assert names_of(recs['M7/U1.c0_sender_snaps']) == ['old', 'new2']
        assert names_of(recs['M7/U1.c1_before']) == ['old', 'new1']
        assert recs['M7/U1.c2_F_incr_sender_lacks_new1'].rc == 0
        assert names_of(recs['M7/U1.c3_after']) == ['old', 'new2']

    def test_u1_a_held_newer_snapshot_makes_f_fail_and_nothing_is_destroyed(self, version):
        recs = m7(version)
        assert recs['M7/U1.d2_F_incr_newer_snapshot_held'].rc == 1
        assert 'dataset is busy' in recs['M7/U1.d2_F_incr_newer_snapshot_held'].stderr
        assert names_of(recs['M7/U1.d3_after']) == names_of(recs['M7/U1.d1_before']) == ['old', 'new1']

    def test_u2_the_newer_snapshot_is_still_there_while_a_cut_receive_is_partial(self, version):
        recs = m7(version)
        assert names_of(recs['M7/U2.a3_while_partial']) == ['old', 'new1']
        assert recs['M7/U2.a2_F_incr_cut'].rc == 1 and 'Partially received snapshot is saved' in recs['M7/U2.a2_F_incr_cut'].stderr

    def test_u2_a_forced_resume_destroys_it_and_an_unforced_resume_is_refused(self, version):
        recs = m7(version)
        assert recs['M7/U2.a6_resume_F'].rc == 0
        assert names_of(recs['M7/U2.a7_after_resume']) == ['old', 'new2']      # new1 destroyed by the resume
        noforce = recs['M7/U2.b3_resume_noF']
        assert noforce.rc == 1 and ' receive -F ' not in noforce.cmd
        assert names_of(recs['M7/U2.b4_after_resume']) == ['old', 'new1']

    def test_u2_a_fresh_mode_abort_keeps_the_newer_snapshot(self, version):
        recs = m7(version)
        assert recs['M7/U2.c3_abort'].cmd.startswith('zfs receive -A')
        assert names_of(recs['M7/U2.c4_after_abort']) == ['old', 'new1']


class TestServerDecisionOnTheCapturedHistoryM7:
    """The server's decision (`api.decide_receive`) on the listings the captures hold, read through the real
    list parser."""

    def base_guid(self, recs, name='old'):
        return dict((n, g) for n, g, _t in listed(recs['M7/U1.00_sender_snaps']))[name]

    def test_u1_the_stream_that_would_destroy_new1_is_refused_unless_the_client_names_it(self, version, mock_subprocess):
        recs = m7(version)
        rows = server_view(mock_subprocess, recs['M7/U1.a1_before'])
        old, new1 = self.base_guid(recs), self.base_guid(recs, 'new1')
        refused = decide(rows, 'old', old)
        assert refused.force is False and refused.reason == 'server_newer' and 'new1' in refused.message
        allowed = decide(rows, 'old', old, discard={new1})
        assert allowed.force is True and allowed.rolled_back == ('new1',)
        # the capture says that is exactly what -F then does
        assert names_of(recs['M7/U1.a3_after']) == ['old', 'new2']

    def test_u1_the_negotiate_answer_for_it(self, version, mock_subprocess):
        from zfsbackup.api import newer_than
        recs = m7(version)
        rows = server_view(mock_subprocess, recs['M7/U1.c1_before'])
        assert newer_than('srv/ds', 'old', rows) == [{'name': 'new1', 'guid': str(self.base_guid(recs, 'new1'))}]
        assert newer_than('srv/ds', 'new1', rows) == []

    def test_u2_a_resume_stream_goes_through_the_same_table(self, version, mock_subprocess):
        recs = m7(version)
        rows = server_view(mock_subprocess, recs['M7/U2.a3_while_partial'])      # new1 present while partial
        old = dict((n, g) for n, g, _t in listed(recs['M7/U2.00_sender_snaps']))['old']
        assert decide(rows, 'old', old).reason == 'server_newer'

    def test_u5_a_modified_head_with_the_base_the_newest_snapshot_gets_f(self, version, mock_subprocess):
        """U5 plain and raw: without -F zfs refuses the modified head, with -F it receives and keeps a, b."""
        recs = m7(version)
        for kind in ('plain', 'raw'):
            noforce = recs['M7/U5.%s.write.3_noF' % kind]
            assert noforce.rc == 1 and 'has been modified' in noforce.stderr, kind
            assert recs['M7/U5.%s.write.5_F' % kind].rc == 0, kind
            assert names_of(recs['M7/U5.%s.write.6_snaps_after_F' % kind]) == ['a', 'b', 'c'], kind
            rows = server_view(mock_subprocess, recs['M7/U5.%s.write.4_snaps_after_noF' % kind])
            b_guid = dict((n, g) for n, g, _t in listed(recs['M7/U5.%s.write.4_snaps_after_noF' % kind]))['b']
            decision = decide(rows, 'b', b_guid)
            assert decision.force is True and decision.rolled_back == (), kind

    def test_u5_an_unmodified_head_needs_no_f(self, version):
        recs = m7(version)
        for kind in ('plain', 'raw'):
            assert recs['M7/U5.%s.c0.2_noF_untouched' % kind].rc == 0, kind

    @pytest.mark.parametrize('case', ['same_newer', 'same_existing_name', 'other_same_name', 'other_new_name'])
    def test_u6_a_full_stream_onto_a_copy_with_snapshots_is_refused_by_zfs_with_and_without_f(self, version, mock_subprocess, case):
        recs = m7(version)
        noforce, forced = recs['M7/U6.%s.noF.2_recv' % case], recs['M7/U6.%s.F.2_recv' % case]
        assert noforce.rc == 1 and 'must specify -F to overwrite it' in noforce.stderr
        assert forced.rc == 1 and 'destination has snapshots' in forced.stderr and 'must destroy them to overwrite it' in forced.stderr
        for after in ('noF', 'F'):
            assert names_of(recs['M7/U6.%s.%s.3_after' % (case, after)]) == names_of(recs['M7/U6.%s.%s.1_before' % (case, after)])
        # ...so the server refuses it up front, with a reason, before zfs sees a byte
        rows = server_view(mock_subprocess, recs['M7/U6.%s.F.1_before' % case])
        decision = decide(rows, None, None)
        assert decision.force is False and decision.reason == 'no_common' and 'zfs rename' in decision.message

    def test_u6_raw_is_refused_the_same_way(self, version, mock_subprocess):
        recs = m7(version)
        assert recs['M7/U6.raw.noF.2_recv'].rc == 1 and recs['M7/U6.raw.F.2_recv'].rc == 1
        assert 'destination has snapshots' in recs['M7/U6.raw.F.2_recv'].stderr
        rows = server_view(mock_subprocess, recs['M7/U6.raw.F.1_before'])
        assert decide(rows, None, None).reason == 'no_common'

    def test_u6_an_existing_dataset_without_snapshots_takes_a_forced_full_stream(self, version, mock_subprocess):
        recs = m7(version)
        assert recs['M7/U6.empty.noF.1_recv'].rc == 1 and 'must specify -F' in recs['M7/U6.empty.noF.1_recv'].stderr
        assert recs['M7/U6.empty.F.1_recv'].rc == 0 and names_of(recs['M7/U6.empty.F.2_after']) == ['c']
        rows = server_view(mock_subprocess, recs['M7/U6.empty.noF.2_after'])
        assert rows == [] and decide(rows, None, None).force is True


class TestRollbackKeepsLocalAnchorsM7:
    """U4: `zfs rollback -r` destroys snapshots but leaves the dataset's properties alone -- which is why the
    client can recognise a rollback by an anchor that names a snapshot that no longer exists."""

    def test_rollback_needs_r_and_keeps_every_property(self, version):
        recs = m7(version)
        assert recs['M7/U4.a3_rollback_without_r'].rc == 1 and 'use \'-r\'' in recs['M7/U4.a3_rollback_without_r'].stderr
        assert recs['M7/U4.a4_rollback_r'].rc == 0
        assert names_of(recs['M7/U4.a2_snaps_before']) == ['A', 'B', 'C'] and names_of(recs['M7/U4.a5_snaps_after']) == ['A']
        assert recs['M7/U4.a6_props_after'].stdout == recs['M7/U4.a1_props_before'].stdout
        assert recs['M7/U4.b5_props_after'].stdout == recs['M7/U4.b2_props_before'].stdout

    def anchor_value(self, mock, rec, prop, dataset):
        """`get_local_anchor` on a dataset whose `zfs get -s local` prints what the capture shows for the rows with
        source `local` (derived: zfs filters on the source itself)."""
        from zfsbackup.backup_manager import DatasetManager
        from zfsbackup.config import BackupConfig, DatasetConfig
        lines = []
        for line in rec.stdout.splitlines():
            cols = line.split('\t')
            if cols[1] == prop and cols[-1] == 'local':
                lines.append(cols[2] + '\n')
        replay(mock, rec, stdout=lines)
        manager = DatasetManager(BackupConfig(datasets=[DatasetConfig(name=dataset)]))
        dsi = manager.datasets[0]
        return manager.get_local_anchor(dsi, prop.rsplit('.', 1)[1])

    def test_the_local_anchor_survives_the_rollback(self, version, mock_subprocess):
        recs = m7(version)
        assert self.anchor_value(mock_subprocess, recs['M7/U4.a6_props_after'], 'org.zfsbackup:anchor.x', 'pool/data') == 'snapA'

    def test_a_received_anchor_is_not_a_local_one(self, version, mock_subprocess):
        recs = m7(version)
        rec = recs['M7/U4.b5_props_after']
        assert self.anchor_value(mock_subprocess, rec, 'org.zfsbackup:anchor.y', 'pool/data') is None     # received
        assert self.anchor_value(mock_subprocess, rec, 'org.zfsbackup:anchor.x', 'pool/data') == 'localx'
