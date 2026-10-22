"""`zfsbackup.health` (plan C5): the stdlib-only evaluator and serialisers.

Pure: no ZFS, no clock (every test passes a fixed `now`), no DB. The ZFS-reading half is in
`test_zfsbackup_health_collect.py`.
"""

import json
import os
import re
import stat
import subprocess
import sys
from datetime import datetime, timedelta, timezone

import pytest

from zfsbackup import health
from zfsbackup.health import (
    CheckEntry, LagEntry, Observation,
    STATE_CRIT, STATE_OK, STATE_WARN,
    SOURCE_ANCHOR, SOURCE_RECEIVED, SOURCE_SNAPSHOT,
    evaluate, overall_state, to_json, to_prometheus, write_text_atomic,
)

NOW = 1_800_000_000          # a fixed epoch second (2027-01-15)
HOUR = 3600.0


def obs(source=SOURCE_SNAPSHOT, *, age=None, expected=HOUR, oldest_age=None, now=NOW, **kw):
    """An `Observation` whose newest snapshot is `age` seconds old at `now`."""
    return Observation(
        source=source, subject=kw.pop('subject', 'pool/data'), expected_seconds=expected,
        newest_ts=None if age is None else now - age,
        oldest_ts=None if oldest_age is None else now - oldest_age,
        **kw,
    )


def one(observation, **kw):
    (entry,) = evaluate([observation], NOW, **kw)
    return entry


# ---------------------------------------------------------------------------------------------
# evaluate: the threshold table
# ---------------------------------------------------------------------------------------------

class TestEvaluateBoundaries:
    @pytest.mark.parametrize('factor, state', [
        (0.0, STATE_OK),
        (0.5, STATE_OK),
        (1.0, STATE_OK),
        (1.9, STATE_OK),
        (2.0, STATE_WARN),      # inclusive: >= 2x is WARN
        (2.1, STATE_WARN),
        (3.9, STATE_WARN),
        (4.0, STATE_CRIT),      # inclusive: >= 4x is CRIT
        (4.1, STATE_CRIT),
        (40.0, STATE_CRIT),
    ])
    def test_default_factors(self, factor, state):
        entry = one(obs(age=round(factor * HOUR)))
        assert entry.state == state
        assert entry.age_seconds == round(factor * HOUR)

    @pytest.mark.parametrize('source', [SOURCE_SNAPSHOT, SOURCE_ANCHOR, SOURCE_RECEIVED])
    @pytest.mark.parametrize('factor, state', [(1.9, STATE_OK), (2.0, STATE_WARN), (4.0, STATE_CRIT)])
    def test_every_lag_source_grades_the_same_way(self, source, factor, state):
        assert one(obs(source, age=round(factor * HOUR))).state == state

    def test_one_second_either_side_of_each_threshold(self):
        assert one(obs(age=7199)).state == STATE_OK
        assert one(obs(age=7200)).state == STATE_WARN
        assert one(obs(age=14399)).state == STATE_WARN
        assert one(obs(age=14400)).state == STATE_CRIT

    def test_the_entry_carries_the_thresholds_it_used(self):
        entry = one(obs(age=10, expected=600.0))
        assert isinstance(entry, LagEntry)
        assert (entry.expected_seconds, entry.warn_seconds, entry.crit_seconds) == (600.0, 1200.0, 2400.0)

    def test_the_entry_names_what_the_observation_named(self):
        entry = one(obs(SOURCE_ANCHOR, age=10, subject='pool/x', peer='offsite'))
        assert (entry.source, entry.subject, entry.peer) == (SOURCE_ANCHOR, 'pool/x', 'offsite')

    def test_the_reason_gives_age_expectation_and_threshold(self):
        entry = one(obs(age=2 * 3600 + 120))
        assert entry.state == STATE_WARN
        assert '2h2m old' in entry.reason and 'expected every 1h' in entry.reason
        assert 'warn at 2h' in entry.reason
        crit = one(obs(age=5 * 3600))
        assert 'crit at 4h' in crit.reason

    def test_detail_is_appended_to_every_state(self):
        for age in (10, 2 * 3600, 5 * 3600):
            assert one(obs(age=age, detail='autosnap_X')).reason.endswith('(autosnap_X)')

    def test_a_zero_age_is_ok(self):
        entry = one(obs(age=0))
        assert entry.state == STATE_OK and entry.age_seconds == 0

    def test_a_datetime_now_is_the_same_as_the_epoch(self):
        when = datetime.fromtimestamp(NOW, tz=timezone.utc)
        assert evaluate([obs(age=7200)], when) == evaluate([obs(age=7200)], NOW)

    def test_a_naive_datetime_now_is_taken_as_utc(self):
        naive = datetime.fromtimestamp(NOW, tz=timezone.utc).replace(tzinfo=None)
        assert evaluate([obs(age=7200)], naive) == evaluate([obs(age=7200)], NOW)

    def test_a_float_or_int_now(self):
        assert evaluate([obs(age=7200)], float(NOW)) == evaluate([obs(age=7200)], NOW)

    def test_evaluate_is_pure(self):
        observations = [obs(age=7200), obs(SOURCE_ANCHOR, age=1, peer='d')]
        before = list(observations)
        assert evaluate(observations, NOW) == evaluate(observations, NOW)
        assert observations == before


class TestEvaluateFactors:
    def test_custom_factors_move_the_boundaries(self):
        kw = dict(warn_factor=1.0, crit_factor=2.0)
        assert one(obs(age=3599), **kw).state == STATE_OK
        assert one(obs(age=3600), **kw).state == STATE_WARN
        assert one(obs(age=7199), **kw).state == STATE_WARN
        assert one(obs(age=7200), **kw).state == STATE_CRIT

    def test_custom_factors_set_the_reported_thresholds(self):
        entry = one(obs(age=1), warn_factor=3.0, crit_factor=6.0)
        assert (entry.warn_seconds, entry.crit_seconds) == (3 * HOUR, 6 * HOUR)

    def test_equal_factors_are_allowed_and_the_crit_wins(self):
        assert one(obs(age=3599), warn_factor=1.0, crit_factor=1.0).state == STATE_OK
        assert one(obs(age=3600), warn_factor=1.0, crit_factor=1.0).state == STATE_CRIT

    def test_fractional_factors(self):
        assert one(obs(age=1800), warn_factor=0.5, crit_factor=0.75).state == STATE_WARN
        assert one(obs(age=2700), warn_factor=0.5, crit_factor=0.75).state == STATE_CRIT

    def test_the_factors_move_the_no_anchor_grace_too(self):
        # oldest snapshot 2h old, nothing sent: a crit factor of 2 makes that CRIT
        entry = one(obs(SOURCE_ANCHOR, oldest_age=2 * 3600), warn_factor=1.0, crit_factor=2.0)
        assert entry.state == STATE_CRIT

    @pytest.mark.parametrize('warn, crit', [
        (4.0, 2.0),          # warn > crit
        (2.0, 1.99),
        (0, 4.0),            # zero
        (2.0, 0),
        (0, 0),
        (-1.0, 4.0),         # negative
        (2.0, -4.0),
        (-4.0, -2.0),
        (float('nan'), 4.0),
        (2.0, float('nan')),
    ])
    def test_unusable_factors_raise_value_error(self, warn, crit):
        with pytest.raises(ValueError):
            evaluate([obs(age=1)], NOW, warn_factor=warn, crit_factor=crit)

    def test_unusable_factors_raise_even_with_nothing_to_grade(self):
        with pytest.raises(ValueError):
            evaluate([], NOW, warn_factor=5.0, crit_factor=1.0)


class TestEvaluateNothingToMeasure:
    def test_a_local_dataset_with_no_snapshots_is_crit(self):
        entry = one(obs(SOURCE_SNAPSHOT))
        assert entry.state == STATE_CRIT
        assert entry.reason == 'no managed snapshots'
        assert entry.age_seconds is None

    def test_a_local_dataset_with_no_snapshots_is_crit_whatever_the_oldest(self):
        assert one(obs(SOURCE_SNAPSHOT, oldest_age=5.0)).state == STATE_CRIT

    @pytest.mark.parametrize('source', [SOURCE_ANCHOR, SOURCE_RECEIVED])
    def test_no_snapshot_at_all_on_an_anchor_or_received_is_warn(self, source):
        entry = one(obs(source, peer='p'))
        assert entry.state == STATE_WARN
        assert entry.age_seconds is None

    def test_anchor_with_no_snapshot_to_send_says_so(self):
        assert 'no managed snapshot to send' in one(obs(SOURCE_ANCHOR)).reason

    def test_received_with_nothing_says_so(self):
        assert 'no managed snapshots yet' in one(obs(SOURCE_RECEIVED)).reason

    def test_no_anchor_with_a_young_oldest_snapshot_is_warn_not_yet_sent(self):
        entry = one(obs(SOURCE_ANCHOR, oldest_age=600))
        assert entry.state == STATE_WARN
        assert entry.reason == 'not yet sent'
        assert entry.age_seconds is None

    def test_no_anchor_grace_runs_to_the_crit_threshold_inclusive(self):
        assert one(obs(SOURCE_ANCHOR, oldest_age=14399)).state == STATE_WARN
        crit = one(obs(SOURCE_ANCHOR, oldest_age=14400))
        assert crit.state == STATE_CRIT
        assert 'not yet sent' in crit.reason and '4h' in crit.reason

    def test_no_anchor_crit_with_a_very_old_oldest(self):
        assert one(obs(SOURCE_ANCHOR, oldest_age=30 * 86400)).state == STATE_CRIT

    def test_received_nothing_yet_follows_the_same_grace(self):
        assert one(obs(SOURCE_RECEIVED, oldest_age=100)).state == STATE_WARN
        assert one(obs(SOURCE_RECEIVED, oldest_age=14400)).state == STATE_CRIT

    def test_detail_is_appended_to_the_nothing_sent_states(self):
        assert one(obs(SOURCE_ANCHOR, oldest_age=10, detail='anchor X is gone')).reason \
            == 'not yet sent (anchor X is gone)'

    def test_the_thresholds_are_reported_even_with_no_age(self):
        entry = one(obs(SOURCE_ANCHOR, oldest_age=10))
        assert (entry.warn_seconds, entry.crit_seconds) == (2 * HOUR, 4 * HOUR)


class TestEvaluateAnchors:
    def test_an_anchor_equal_to_the_newest_snapshot_is_ok(self):
        # snapshot and anchor name the same instant: identical age, identical verdict
        snapshot = one(obs(SOURCE_SNAPSHOT, age=120))
        anchor = one(obs(SOURCE_ANCHOR, age=120, peer='d'))
        assert snapshot.state == anchor.state == STATE_OK
        assert snapshot.age_seconds == anchor.age_seconds

    def test_a_fresh_anchor_with_a_send_in_flight_is_ok(self):
        entry = one(obs(SOURCE_ANCHOR, age=600, peer='d',
                        detail='autosnap_X; send in flight: autosnap_X -> autosnap_Y'))
        assert entry.state == STATE_OK
        assert 'send in flight' in entry.reason

    def test_a_stale_anchor_is_not_rescued_by_an_inflight_note(self):
        entry = one(obs(SOURCE_ANCHOR, age=5 * 3600, peer='d', detail='send in flight: a -> b'))
        assert entry.state == STATE_CRIT

    def test_a_slow_remote_expectation_widens_the_thresholds(self):
        # the collector hands over max(dataset, remote): 1 day. An anchor 1d1h old is fine.
        day = 86400.0
        entry = one(obs(SOURCE_ANCHOR, age=day + 3600, expected=day, peer='d'))
        assert entry.state == STATE_OK
        assert one(obs(SOURCE_ANCHOR, age=2 * day, expected=day, peer='d')).state == STATE_WARN

    def test_received_grading_uses_the_expectation_it_is_given(self):
        # a server's expectation is the largest client frequency
        week = 7 * 86400.0
        assert one(obs(SOURCE_RECEIVED, age=3 * 86400, expected=week, peer='c')).state == STATE_OK
        assert one(obs(SOURCE_RECEIVED, age=4 * 86400, expected=86400.0, peer='c')).state == STATE_CRIT


class TestEvaluateFutureDates:
    def test_beyond_tolerance_is_crit_clock_behind(self):
        entry = one(obs(age=-(300 + 1), tolerance_seconds=300.0))
        assert entry.state == STATE_CRIT
        assert entry.reason.startswith('clock behind newest snapshot')
        assert 'in the future' in entry.reason
        assert entry.age_seconds == -301          # the negative age is kept

    def test_exactly_at_tolerance_is_age_zero_and_ok(self):
        entry = one(obs(age=-300, tolerance_seconds=300.0))
        assert entry.state == STATE_OK
        assert entry.age_seconds == 0.0

    def test_within_tolerance_is_age_zero_and_ok(self):
        entry = one(obs(age=-299, tolerance_seconds=300.0))
        assert entry.state == STATE_OK and entry.age_seconds == 0.0

    def test_the_tolerance_is_the_observations_own(self):
        assert one(obs(age=-3000, tolerance_seconds=3600.0)).state == STATE_OK
        assert one(obs(age=-3601, tolerance_seconds=3600.0)).state == STATE_CRIT

    def test_the_default_tolerance_is_five_minutes(self):
        assert one(obs(age=-299)).state == STATE_OK
        assert one(obs(age=-301)).state == STATE_CRIT

    @pytest.mark.parametrize('source', [SOURCE_SNAPSHOT, SOURCE_ANCHOR, SOURCE_RECEIVED])
    def test_every_source_applies_it(self, source):
        assert one(obs(source, age=-10_000, peer='p')).state == STATE_CRIT

    def test_detail_is_appended_to_the_clock_state(self):
        assert one(obs(age=-10_000, detail='autosnap_F')).reason.endswith('(autosnap_F)')


class TestEvaluateErrorsAndPassThrough:
    def test_an_error_is_crit(self):
        entry = one(obs(error='cannot open pool/data'))
        assert entry.state == STATE_CRIT
        assert entry.reason == 'cannot read: cannot open pool/data'
        assert entry.age_seconds is None

    @pytest.mark.parametrize('source', [SOURCE_SNAPSHOT, SOURCE_ANCHOR, SOURCE_RECEIVED])
    def test_an_error_is_crit_for_every_source(self, source):
        assert one(obs(source, error='x', peer='p')).state == STATE_CRIT

    def test_an_error_wins_over_a_fresh_timestamp(self):
        assert one(obs(age=1, error='half-read')).state == STATE_CRIT

    def test_an_error_keeps_the_thresholds(self):
        entry = one(obs(error='x'))
        assert entry.warn_seconds == 2 * HOUR and entry.crit_seconds == 4 * HOUR

    def test_a_check_entry_passes_through_unchanged(self):
        check = CheckEntry('config', 'dry_run', STATE_WARN, 'dry_run is set')
        (out,) = evaluate([check], NOW)
        assert out is check

    def test_order_is_preserved_across_a_mixture(self):
        a = obs(SOURCE_SNAPSHOT, age=10, subject='pool/a')
        check = CheckEntry('daemon', 'supervisor', STATE_OK, 'running')
        b = obs(SOURCE_ANCHOR, age=10, subject='pool/a', peer='d')
        c = obs(SOURCE_RECEIVED, age=10, subject='pool/r', peer='cl')
        out = evaluate([check, a, b, check, c], NOW)
        assert [(e.source, e.subject) for e in out] == [
            ('daemon', 'supervisor'), ('snapshot', 'pool/a'), ('anchor', 'pool/a'),
            ('daemon', 'supervisor'), ('received', 'pool/r'),
        ]
        assert out[0] is check and out[3] is check
        assert isinstance(out[1], LagEntry) and not isinstance(out[0], LagEntry)

    def test_a_generator_is_accepted(self):
        assert len(evaluate((obs(age=i) for i in range(3)), NOW)) == 3

    def test_empty_in_empty_out(self):
        assert evaluate([], NOW) == []


class TestOverallState:
    def test_no_entries_is_ok(self):
        assert overall_state([]) == STATE_OK

    @pytest.mark.parametrize('states, worst', [
        ([STATE_OK], STATE_OK),
        ([STATE_OK, STATE_OK], STATE_OK),
        ([STATE_OK, STATE_WARN], STATE_WARN),
        ([STATE_WARN, STATE_OK], STATE_WARN),
        ([STATE_WARN, STATE_WARN], STATE_WARN),
        ([STATE_OK, STATE_CRIT], STATE_CRIT),
        ([STATE_CRIT, STATE_OK], STATE_CRIT),
        ([STATE_WARN, STATE_CRIT, STATE_OK], STATE_CRIT),
        ([STATE_CRIT, STATE_WARN], STATE_CRIT),
    ])
    def test_the_worst_wins(self, states, worst):
        entries = [CheckEntry('s', f'x{i}', st, '') for i, st in enumerate(states)]
        assert overall_state(entries) == worst

    def test_an_unknown_state_fails_closed(self):
        assert overall_state([CheckEntry('s', 'x', 'BOGUS', '')]) == STATE_CRIT
        assert overall_state([CheckEntry('s', 'x', STATE_OK, ''), CheckEntry('s', 'y', 'BOGUS', '')]) == STATE_CRIT

    def test_state_values_order_worst_highest(self):
        assert health.STATE_VALUES == {'OK': 0, 'WARN': 1, 'CRIT': 2}

    def test_works_on_evaluated_entries(self):
        entries = evaluate([obs(age=10), obs(age=7200), obs(age=100)], NOW)
        assert overall_state(entries) == STATE_WARN


class TestFormatDuration:
    @pytest.mark.parametrize('seconds, text', [
        (0, '0m'), (59, '0m'), (60, '1m'), (3600, '1h'), (3660, '1h1m'),
        (86400, '1d'), (90061, '1d1h1m'), (-90061, '-1d1h1m'), (7 * 86400, '7d'),
    ])
    def test_format(self, seconds, text):
        assert health.format_duration(seconds) == text


# ---------------------------------------------------------------------------------------------
# JSON
# ---------------------------------------------------------------------------------------------

def _lag(source=SOURCE_SNAPSHOT, **kw):
    kw.setdefault('age', 10)
    return one(obs(source, **kw))


class TestJson:
    def test_shape(self):
        entries = [_lag(), _lag(SOURCE_ANCHOR, peer='offsite')]
        doc = json.loads(to_json(entries, NOW, version='1.2.3'))
        assert doc['schema'] == 1
        assert doc['version'] == '1.2.3'
        assert doc['state'] == 'OK'
        assert doc['generated_at'] == '2027-01-15T08:00:00Z'
        assert [e['source'] for e in doc['entries']] == ['snapshot', 'anchor']

    def test_top_level_keys_are_exactly_the_contract(self):
        assert set(json.loads(to_json([], NOW))) == {'schema', 'version', 'generated_at', 'state', 'entries'}

    def test_schema_is_an_int_one(self):
        assert json.loads(to_json([], NOW))['schema'] == 1 == health.JSON_SCHEMA
        assert type(json.loads(to_json([], NOW))['schema']) is int

    def test_no_entries_is_ok_and_empty(self):
        doc = json.loads(to_json([], NOW))
        assert doc['state'] == 'OK' and doc['entries'] == []

    def test_version_defaults_to_null(self):
        assert json.loads(to_json([], NOW))['version'] is None

    def test_state_is_the_worst_entry(self):
        doc = json.loads(to_json([_lag(), _lag(age=7200), _lag(age=100)], NOW))
        assert doc['state'] == 'WARN'
        doc = json.loads(to_json([_lag(), _lag(age=7200), _lag(age=20000)], NOW))
        assert doc['state'] == 'CRIT'

    def test_lag_entry_fields(self):
        (entry,) = json.loads(to_json([_lag(age=7200)], NOW))['entries']
        assert entry == {
            'source': 'snapshot', 'subject': 'pool/data', 'state': 'WARN',
            'reason': entry['reason'],
            'age_seconds': 7200, 'expected_seconds': 3600.0,
            'warn_seconds': 7200.0, 'crit_seconds': 14400.0,
        }
        assert 'peer' not in entry

    def test_peer_is_present_only_when_set(self):
        entries = json.loads(to_json([_lag(), _lag(SOURCE_ANCHOR, peer='offsite')], NOW))['entries']
        assert 'peer' not in entries[0]
        assert entries[1]['peer'] == 'offsite'

    def test_plain_check_entry_has_no_lag_numbers(self):
        check = CheckEntry('config', 'dry_run', STATE_WARN, 'dry_run is set')
        (entry,) = json.loads(to_json([check], NOW))['entries']
        assert entry == {'source': 'config', 'subject': 'dry_run', 'state': 'WARN', 'reason': 'dry_run is set'}

    def test_a_check_entry_with_a_peer(self):
        check = CheckEntry('daemon', 'worker', STATE_OK, 'ok', peer='x')
        (entry,) = json.loads(to_json([check], NOW))['entries']
        assert entry['peer'] == 'x' and 'age_seconds' not in entry

    def test_a_null_age_is_written_as_null(self):
        entries = evaluate([obs(SOURCE_SNAPSHOT)], NOW)          # no snapshots
        (entry,) = json.loads(to_json(entries, NOW))['entries']
        assert entry['age_seconds'] is None
        assert entry['warn_seconds'] == 7200.0

    def test_a_negative_age_is_kept(self):
        entries = evaluate([obs(age=-10_000)], NOW)
        (entry,) = json.loads(to_json(entries, NOW))['entries']
        assert entry['age_seconds'] == -10_000 and entry['state'] == 'CRIT'

    def test_unknown_keys_can_be_ignored_by_a_consumer(self):
        # the contract: additive changes only; a consumer reads the keys it knows
        doc = json.loads(to_json([_lag()], NOW))
        doc['future_key'] = {'a': 1}
        doc['entries'][0]['future_field'] = 7
        assert doc['schema'] == 1 and doc['entries'][0]['state'] == 'OK'

    def test_a_datetime_now(self):
        when = datetime(2027, 1, 15, 8, 0, 0, tzinfo=timezone.utc)
        assert json.loads(to_json([], when))['generated_at'] == '2027-01-15T08:00:00Z'

    def test_a_non_utc_datetime_is_written_in_utc(self):
        when = datetime(2027, 1, 15, 10, 0, 0, tzinfo=timezone(timedelta(hours=2)))
        assert json.loads(to_json([], when))['generated_at'] == '2027-01-15T08:00:00Z'

    def test_output_is_ascii_and_round_trips_odd_names(self):
        name = 'pool/d\u00e4ta "q" \\ \n tab\t'
        text = to_json([_lag(subject=name)], NOW)
        assert text.isascii()
        assert json.loads(text)['entries'][0]['subject'] == name

    def test_indent_none_is_one_line(self):
        assert '\n' not in to_json([_lag()], NOW, indent=None)
        assert '\n' in to_json([_lag()], NOW)

    def test_entry_dict_matches_the_document(self):
        entry = _lag(SOURCE_ANCHOR, peer='d')
        assert health.entry_dict(entry) == json.loads(to_json([entry], NOW))['entries'][0]


# ---------------------------------------------------------------------------------------------
# Prometheus: a small strict parser of the text exposition format
# ---------------------------------------------------------------------------------------------

_NAME = r'[a-zA-Z_:][a-zA-Z0-9_:]*'


class Exposition:
    """The parsed text: `samples` is a list of `(name, labels, value)`, `help`/`type` count the
    `# HELP`/`# TYPE` lines per metric name."""

    def __init__(self, text):
        assert text.endswith('\n'), 'the exposition must end with a newline'
        assert '\r' not in text
        self.samples = []
        self.help, self.type, self.kind = {}, {}, {}
        self.order = []            # metric names in first-seen order (comments and samples)
        for line in text[:-1].split('\n'):
            assert line != '', 'no blank lines'
            if line.startswith('# HELP '):
                name, _, doc = line[len('# HELP '):].partition(' ')
                assert re.fullmatch(_NAME, name) and doc
                self.help[name] = self.help.get(name, 0) + 1
            elif line.startswith('# TYPE '):
                name, _, kind = line[len('# TYPE '):].partition(' ')
                assert re.fullmatch(_NAME, name)
                assert kind in ('counter', 'gauge', 'histogram', 'summary', 'untyped')
                self.type[name] = self.type.get(name, 0) + 1
                self.kind[name] = kind
                self.order.append(name)
            else:
                assert not line.startswith('#'), f'unknown comment {line!r}'
                self.samples.append(self._sample(line))
        for name in {n for n, _, _ in self.samples}:
            assert self.type.get(name) == 1 and self.help.get(name) == 1, f'{name}: HELP/TYPE once'

    @staticmethod
    def _sample(line):
        m = re.match(_NAME, line)
        assert m, f'bad metric name in {line!r}'
        name, rest = m.group(0), line[m.end():]
        labels = {}
        if rest.startswith('{'):
            i = 1
            while rest[i] != '}':
                m = re.match(r'([a-zA-Z_][a-zA-Z0-9_]*)="', rest[i:])
                assert m, f'bad label in {line!r}'
                key, i = m.group(1), i + m.end()
                value = []
                while True:
                    ch = rest[i]
                    assert ch != '\n', 'a raw newline inside a label value'
                    if ch == '\\':
                        esc = rest[i + 1]
                        assert esc in '\\"n', f'bad escape \\{esc} in {line!r}'
                        value.append({'\\': '\\', '"': '"', 'n': '\n'}[esc])
                        i += 2
                    elif ch == '"':
                        i += 1
                        break
                    else:
                        value.append(ch)
                        i += 1
                assert key not in labels, f'duplicate label {key}'
                labels[key] = ''.join(value)
                if rest[i] == ',':
                    i += 1
            rest = rest[i + 1:]
        assert rest.startswith(' '), f'no value separator in {line!r}'
        number = rest[1:]
        assert re.fullmatch(r'-?[0-9]+(\.[0-9]+)?([eE][-+]?[0-9]+)?|[-+]?Inf|NaN', number), number
        return name, labels, float(number)

    def values(self, name, **labels):
        """The values of `name` samples whose labels include `labels`."""
        return [v for n, lab, v in self.samples
                if n == name and all(lab.get(k) == x for k, x in labels.items())]

    def one(self, name, **labels):
        found = self.values(name, **labels)
        assert len(found) == 1, f'{name}{labels}: {found}'
        return found[0]

    def names(self):
        return {n for n, _, _ in self.samples}


def prom(entries, now=NOW, version='9.9'):
    return Exposition(to_prometheus(entries, now, version))


class TestPrometheusNames:
    def test_names_per_source(self):
        text = prom([_lag(SOURCE_SNAPSHOT), _lag(SOURCE_ANCHOR, peer='d'), _lag(SOURCE_RECEIVED, peer='c')])
        assert text.names() >= {
            'zfsbackup_snapshot_age_seconds', 'zfsbackup_snapshot_threshold_seconds',
            'zfsbackup_snapshot_check_state',
            'zfsbackup_anchor_age_seconds', 'zfsbackup_anchor_threshold_seconds',
            'zfsbackup_anchor_check_state',
            'zfsbackup_received_age_seconds', 'zfsbackup_received_threshold_seconds',
            'zfsbackup_received_check_state',
        }

    def test_a_source_with_no_entries_emits_no_metrics(self):
        names = prom([_lag(SOURCE_SNAPSHOT)]).names()
        assert not any(n.startswith(('zfsbackup_anchor_', 'zfsbackup_received_')) for n in names)

    def test_every_metric_carries_the_zfsbackup_prefix(self):
        text = prom([_lag(), _lag(SOURCE_ANCHOR, peer='d'), CheckEntry('config', 'dry_run', STATE_WARN, 'x')])
        assert all(n.startswith('zfsbackup_') for n in text.names())

    def test_summary_metrics_with_no_entries(self):
        text = prom([])
        assert text.names() == {
            'zfsbackup_check_overall_state', 'zfsbackup_check_success',
            'zfsbackup_check_timestamp_seconds', 'zfsbackup_build_info',
        }

    def test_the_age_value(self):
        text = prom([_lag(age=1234)])
        assert text.one('zfsbackup_snapshot_age_seconds', dataset='pool/data') == 1234

    def test_the_check_state_value(self):
        text = prom([_lag(age=10, subject='a'), _lag(age=7200, subject='b'), _lag(age=20000, subject='c')])
        assert [text.one('zfsbackup_snapshot_check_state', dataset=d) for d in 'abc'] == [0, 1, 2]

    def test_the_timestamp(self):
        assert prom([]).one('zfsbackup_check_timestamp_seconds') == NOW

    def test_a_datetime_now(self):
        when = datetime.fromtimestamp(NOW, tz=timezone.utc)
        assert Exposition(to_prometheus([], when)).one('zfsbackup_check_timestamp_seconds') == NOW


class TestPrometheusLabels:
    def test_the_level_label_carries_the_thresholds(self):
        text = prom([_lag(age=10)])
        assert text.one('zfsbackup_snapshot_threshold_seconds', dataset='pool/data', level='warn') == 7200
        assert text.one('zfsbackup_snapshot_threshold_seconds', dataset='pool/data', level='crit') == 14400

    def test_two_threshold_samples_per_entry(self):
        text = prom([_lag(), _lag(subject='pool/other')])
        assert len(text.values('zfsbackup_snapshot_threshold_seconds')) == 4

    def test_the_snapshot_metrics_carry_only_dataset(self):
        text = prom([_lag()])
        for name, labels, _ in text.samples:
            if name.startswith('zfsbackup_snapshot_'):
                assert set(labels) - {'level'} == {'dataset'}

    def test_anchor_metrics_carry_a_destination_label(self):
        text = prom([_lag(SOURCE_ANCHOR, peer='offsite')])
        for name, labels, _ in text.samples:
            if name.startswith('zfsbackup_anchor_'):
                assert labels['destination'] == 'offsite'
                assert labels['dataset'] == 'pool/data'
                assert 'client' not in labels and 'peer' not in labels

    def test_received_metrics_carry_a_client_label(self):
        text = prom([_lag(SOURCE_RECEIVED, peer='laptop', subject='pool/b/laptop/x')])
        for name, labels, _ in text.samples:
            if name.startswith('zfsbackup_received_'):
                assert labels['client'] == 'laptop'
                assert labels['dataset'] == 'pool/b/laptop/x'
                assert 'destination' not in labels

    def test_two_destinations_are_two_series(self):
        text = prom([_lag(SOURCE_ANCHOR, peer='a', age=10), _lag(SOURCE_ANCHOR, peer='b', age=7200)])
        assert text.one('zfsbackup_anchor_check_state', destination='a') == 0
        assert text.one('zfsbackup_anchor_check_state', destination='b') == 1

    def test_non_lag_entries_are_check_state_with_source_and_subject(self):
        text = prom([CheckEntry('config', 'dry_run', STATE_WARN, 'x')])
        assert text.one('zfsbackup_check_state', source='config', subject='dry_run') == 1
        (labels,) = [lab for n, lab, _ in text.samples if n == 'zfsbackup_check_state']
        assert set(labels) == {'source', 'subject'}

    def test_a_non_lag_entry_with_a_peer_gets_a_peer_label(self):
        text = prom([CheckEntry('daemon', 'worker', STATE_CRIT, 'x', peer='w1')])
        assert text.one('zfsbackup_check_state', source='daemon', subject='worker', peer='w1') == 2

    def test_no_age_sample_for_a_none_age(self):
        entries = evaluate([obs(SOURCE_SNAPSHOT, subject='pool/none'), obs(age=10, subject='pool/some')], NOW)
        text = prom(entries)
        assert text.values('zfsbackup_snapshot_age_seconds', dataset='pool/none') == []
        assert text.values('zfsbackup_snapshot_age_seconds', dataset='pool/some') == [10]
        # but the state and the thresholds are still reported
        assert text.one('zfsbackup_snapshot_check_state', dataset='pool/none') == 2
        assert text.one('zfsbackup_snapshot_threshold_seconds', dataset='pool/none', level='crit') == 14400

    def test_no_age_metric_at_all_when_no_entry_has_an_age(self):
        entries = evaluate([obs(SOURCE_SNAPSHOT)], NOW)
        text = to_prometheus(entries, NOW)
        assert 'zfsbackup_snapshot_age_seconds' not in text
        Exposition(text)

    def test_an_age_gets_a_sample_per_aged_entry_only(self):
        entries = evaluate([obs(SOURCE_ANCHOR, peer='a', oldest_age=10), obs(SOURCE_ANCHOR, peer='b', age=5)], NOW)
        text = prom(entries)
        assert len(text.values('zfsbackup_anchor_age_seconds')) == 1
        assert len(text.values('zfsbackup_anchor_check_state')) == 2


class TestPrometheusEscaping:
    NASTY = 'a\\b"c\nd'

    def test_the_raw_text_escapes_backslash_quote_and_newline(self):
        text = to_prometheus([_lag(subject=self.NASTY)], NOW)
        assert 'dataset="a\\\\b\\"c\\nd"' in text
        assert 'c\nd' not in text           # no raw newline inside a sample

    def test_the_parser_gets_the_value_back(self):
        text = prom([_lag(subject=self.NASTY, age=5)])
        assert text.one('zfsbackup_snapshot_age_seconds', dataset=self.NASTY) == 5

    @pytest.mark.parametrize('subject', [
        '\\', '"', '\n', '\\\\', '\\"', '\\n', '"""', '\n\n', 'ends with backslash\\',
        'pool/d\u00e4ta', 'tab\there', 'brace}{', 'comma,equals=', '# not a comment',
    ])
    def test_odd_label_values_round_trip(self, subject):
        text = prom([_lag(subject=subject), _lag(SOURCE_ANCHOR, subject=subject, peer=subject),
                     CheckEntry('config', subject, STATE_WARN, 'x', peer=subject)])
        assert text.one('zfsbackup_snapshot_check_state', dataset=subject) == 0
        assert text.one('zfsbackup_anchor_check_state', dataset=subject, destination=subject) == 0
        assert text.one('zfsbackup_check_state', source='config', subject=subject, peer=subject) == 1

    def test_the_version_label_is_escaped_too(self):
        text = Exposition(to_prometheus([], NOW, version='1"\\\n'))
        assert text.one('zfsbackup_build_info', version='1"\\\n') == 1

    def test_escape_label_value_directly(self):
        assert health.escape_label_value('\\') == '\\\\'
        assert health.escape_label_value('"') == '\\"'
        assert health.escape_label_value('\n') == '\\n'
        assert health.escape_label_value('plain') == 'plain'
        assert health.escape_label_value('\\"\n') == '\\\\\\"\\n'     # the backslash first, once

    def test_the_cross_check_with_prometheus_client(self):
        parser = pytest.importorskip('prometheus_client.parser')
        text = to_prometheus(
            [_lag(subject=self.NASTY), _lag(SOURCE_ANCHOR, peer='d"x'), CheckEntry('c', 's\\', STATE_WARN, 'r')],
            NOW, version='1.0',
        )
        families = {f.name: f for f in parser.text_string_to_metric_families(text)}
        assert 'zfsbackup_snapshot_age_seconds' in families
        (sample,) = families['zfsbackup_snapshot_age_seconds'].samples
        assert sample.labels == {'dataset': self.NASTY} and sample.value == 10
        assert families['zfsbackup_check_success'].samples[0].value == 0      # the WARN entry


class TestPrometheusMetadata:
    def test_help_and_type_appear_once_per_metric(self):
        entries = [_lag(subject=f'pool/d{i}', age=10 * i) for i in range(4)] + \
                  [_lag(SOURCE_ANCHOR, peer=f'd{i}') for i in range(3)] + \
                  [CheckEntry('config', f'c{i}', STATE_OK, '') for i in range(3)]
        text = to_prometheus(entries, NOW)
        parsed = Exposition(text)        # also asserts HELP/TYPE once for every sampled metric
        for name in parsed.names():
            assert text.count(f'# HELP {name} ') == 1
            assert text.count(f'# TYPE {name} ') == 1
        assert all(count == 1 for count in parsed.help.values())
        assert all(count == 1 for count in parsed.type.values())

    def test_type_comes_before_the_samples(self):
        lines = to_prometheus([_lag()], NOW).splitlines()
        for name in {l.split('{')[0].split(' ')[0] for l in lines if not l.startswith('#')}:
            first_sample = next(i for i, l in enumerate(lines) if l.startswith(name) and not l.startswith('#'))
            type_line = lines.index(next(l for l in lines if l.startswith(f'# TYPE {name} ')))
            assert type_line < first_sample

    def test_every_metric_is_a_gauge(self):
        parsed = prom([_lag(), CheckEntry('c', 's', STATE_OK, '')])
        assert set(parsed.kind.values()) == {'gauge'}

    def test_ends_with_a_single_newline(self):
        text = to_prometheus([_lag()], NOW)
        assert text.endswith('\n') and not text.endswith('\n\n')

    def test_build_info(self):
        text = prom([], version='4.5.6')
        assert text.one('zfsbackup_build_info', version='4.5.6') == 1

    def test_build_info_without_a_version_is_unknown(self):
        assert Exposition(to_prometheus([], NOW)).one('zfsbackup_build_info', version='unknown') == 1

    def test_the_parser_rejects_a_duplicated_help_line(self):
        with pytest.raises(AssertionError):
            Exposition('# HELP a_b x\n# HELP a_b x\n# TYPE a_b gauge\na_b 1\n')


class TestPrometheusSuccess:
    def test_all_ok_is_success_and_overall_zero(self):
        text = prom([_lag(), _lag(SOURCE_ANCHOR, peer='d')])
        assert text.one('zfsbackup_check_success') == 1
        assert text.one('zfsbackup_check_overall_state') == 0

    def test_no_entries_is_success(self):
        assert prom([]).one('zfsbackup_check_success') == 1

    def test_warn_makes_success_zero(self):
        text = prom([_lag(), _lag(age=7200, subject='pool/w')])
        assert text.one('zfsbackup_check_success') == 0
        assert text.one('zfsbackup_check_overall_state') == 1

    def test_crit_makes_success_zero(self):
        text = prom([_lag(age=99999)])
        assert text.one('zfsbackup_check_success') == 0
        assert text.one('zfsbackup_check_overall_state') == 2

    def test_a_non_lag_warn_makes_success_zero(self):
        text = prom([CheckEntry('config', 'dry_run', STATE_WARN, 'x')])
        assert text.one('zfsbackup_check_success') == 0 and text.one('zfsbackup_check_overall_state') == 1

    def test_overall_state_is_the_worst(self):
        text = prom([_lag(age=7200, subject='w'), _lag(age=99999, subject='c'), _lag(subject='o')])
        assert text.one('zfsbackup_check_overall_state') == 2


# ---------------------------------------------------------------------------------------------
# write_text_atomic
# ---------------------------------------------------------------------------------------------

class TestWriteTextAtomic:
    def test_writes_the_text(self, tmp_path):
        target = tmp_path / 'out.prom'
        write_text_atomic(target, 'hello \u00e4\n')
        assert target.read_text(encoding='utf-8') == 'hello \u00e4\n'

    def test_accepts_a_str_path(self, tmp_path):
        write_text_atomic(str(tmp_path / 'o'), 'x')
        assert (tmp_path / 'o').read_text() == 'x'

    def test_default_mode_is_0644_whatever_the_umask(self, tmp_path):
        old = os.umask(0o077)
        try:
            write_text_atomic(tmp_path / 'o', 'x')
        finally:
            os.umask(old)
        assert stat.S_IMODE((tmp_path / 'o').stat().st_mode) == 0o644

    def test_an_explicit_mode(self, tmp_path):
        old = os.umask(0)
        try:
            write_text_atomic(tmp_path / 'o', 'x', mode=0o600)
        finally:
            os.umask(old)
        assert stat.S_IMODE((tmp_path / 'o').stat().st_mode) == 0o600

    def test_replaces_an_existing_file_and_its_mode(self, tmp_path):
        target = tmp_path / 'o'
        target.write_text('old')
        target.chmod(0o600)
        write_text_atomic(target, 'new')
        assert target.read_text() == 'new'
        assert stat.S_IMODE(target.stat().st_mode) == 0o644

    def test_a_reader_with_the_old_file_open_keeps_the_old_content(self, tmp_path):
        target = tmp_path / 'o'
        target.write_text('old content')
        with open(target) as reader:
            write_text_atomic(target, 'new')
            assert reader.read() == 'old content'          # a new inode, not a truncate
        assert target.read_text() == 'new'

    def test_leaves_no_temporary_file(self, tmp_path):
        write_text_atomic(tmp_path / 'o', 'x')
        assert [p.name for p in tmp_path.iterdir()] == ['o']

    def test_the_temporary_file_is_in_the_target_directory(self, tmp_path, monkeypatch):
        seen = []
        real = os.replace
        monkeypatch.setattr(os, 'replace', lambda src, dst: (seen.append((src, dst)), real(src, dst))[1])
        write_text_atomic(tmp_path / 'o', 'x')
        (src, dst), = seen
        assert os.path.dirname(src) == os.path.dirname(dst) == str(tmp_path)

    def test_no_temporary_file_after_a_failed_replace(self, tmp_path, monkeypatch):
        target = tmp_path / 'o'
        target.write_text('old')

        def boom(src, dst):
            raise OSError('replace failed')

        monkeypatch.setattr(os, 'replace', boom)
        with pytest.raises(OSError, match='replace failed'):
            write_text_atomic(target, 'new')
        assert [p.name for p in tmp_path.iterdir()] == ['o']
        assert target.read_text() == 'old'                    # the old file is untouched

    def test_no_temporary_file_after_a_failed_write(self, tmp_path, monkeypatch):
        target = tmp_path / 'o'
        monkeypatch.setattr(os, 'fsync', lambda fd: (_ for _ in ()).throw(OSError('disk full')))
        with pytest.raises(OSError, match='disk full'):
            write_text_atomic(target, 'new')
        assert list(tmp_path.iterdir()) == []

    def test_no_temporary_file_after_a_failed_chmod(self, tmp_path, monkeypatch):
        monkeypatch.setattr(os, 'fchmod', lambda fd, mode: (_ for _ in ()).throw(OSError('no chmod')))
        with pytest.raises(OSError):
            write_text_atomic(tmp_path / 'o', 'x')
        assert list(tmp_path.iterdir()) == []

    def test_no_temporary_file_after_a_keyboard_interrupt(self, tmp_path, monkeypatch):
        monkeypatch.setattr(os, 'fsync', lambda fd: (_ for _ in ()).throw(KeyboardInterrupt()))
        with pytest.raises(KeyboardInterrupt):
            write_text_atomic(tmp_path / 'o', 'x')
        assert list(tmp_path.iterdir()) == []

    def test_a_missing_directory_raises_and_creates_nothing(self, tmp_path):
        with pytest.raises(OSError):
            write_text_atomic(tmp_path / 'nope' / 'o', 'x')
        assert list(tmp_path.iterdir()) == []

    def test_a_relative_path_uses_the_cwd(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        write_text_atomic('rel.prom', 'x')
        assert (tmp_path / 'rel.prom').read_text() == 'x'

    def test_a_large_text_is_written_whole(self, tmp_path):
        text = 'x' * 5_000_000
        write_text_atomic(tmp_path / 'o', text)
        assert (tmp_path / 'o').stat().st_size == 5_000_000

    def test_the_exposition_round_trips_through_a_file(self, tmp_path):
        text = to_prometheus([_lag()], NOW)
        write_text_atomic(tmp_path / 'z.prom', text)
        assert (tmp_path / 'z.prom').read_text() == text


# ---------------------------------------------------------------------------------------------
# import weight
# ---------------------------------------------------------------------------------------------

class TestImportWeight:
    def test_health_loads_only_itself_and_the_package(self):
        """`python -I -c 'import zfsbackup.health'`: no libzfseasy, no SQLAlchemy, no config model --
        the module is for a status helper that must stay cheap and dependency free."""
        root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        probe = (
            f"import sys; sys.path.insert(0, {root!r}); before = set(sys.modules); import zfsbackup.health; "
            "print('\\n'.join(sorted(m for m in set(sys.modules) - before if m.split('.')[0] == 'zfsbackup'))); "
            "print('LIBZFSEASY', 'libzfseasy' in sys.modules); "
            "print('SQLALCHEMY', 'sqlalchemy' in sys.modules); "
            "print('YAML', 'yaml' in sys.modules)"
        )
        done = subprocess.run([sys.executable, '-I', '-c', probe], cwd=root, env={},
                              capture_output=True, text=True, timeout=60)
        assert done.returncode == 0, done.stderr
        loaded = [l for l in done.stdout.splitlines() if l.startswith('zfsbackup')]
        assert loaded == ['zfsbackup', 'zfsbackup.health'], done.stdout
        assert 'LIBZFSEASY False' in done.stdout and 'SQLALCHEMY False' in done.stdout
        assert 'YAML False' in done.stdout

    def test_module_imports_are_stdlib_only(self):
        """The same guarantee read off the source: every import in the module is a stdlib one."""
        import ast
        tree = ast.parse(open(health.__file__, encoding='utf-8').read())
        roots = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                roots |= {a.name.split('.')[0] for a in node.names}
            elif isinstance(node, ast.ImportFrom) and node.level == 0:
                roots.add((node.module or '').split('.')[0])
        assert roots <= set(sys.stdlib_module_names) | {'__future__'}, roots
