"""Milestone D, plan B-2a: the server reads the stream's first record before it spawns `zfs receive`.

A client's stream decides what the server's `zfs receive` is told: the 312-byte `DRR_BEGIN` record
(`zfsbackup.api.parse_stream_header`) says whether the stream is a filesystem or a volume, whether it
is a plain substream or a compound one (`zfs send -p`/`-R`, refused: it carries an nvlist of the
client's properties), and which snapshot it is incremental from (`drr_fromguid`, compared with the
base the client declared: `stream_base_problem`). The receive then runs with the server's forced
`-o` values for that type, `-h`, no `-x`, and the header bytes are its first input, unchanged.

Layout:

* `TestParseCaptures` -- every `M8/HDR.*` capture of OpenZFS 2.1.5, 2.2.2 and 2.4.1 through the parser.
* `TestParseSynthetic` -- bad magic, not a begin record, object-set types, header types, short input,
  a big-endian sender, a non-zero payload length.
* `TestReadStreamHeader` -- the websocket reader (one chunk, split, extra bytes, `done`, timeout, close).
* `TestStreamBaseProblem` -- the declared base against `drr_fromguid`, every combination.
* `TestForcedProperties` -- the two forced property sets, pinned.
* `TestReceiveCall` / `TestReceiveArgv` -- what `zfs receive` gets, per type and mode, through the harness
  and as a real argv (only `Popen` is mocked).
* `TestHeaderBytesAreFedFirst`, `TestHeaderRefusals`, `TestBaseMismatch`, `TestTypeMismatch`,
  `TestHeaderPhaseEnds` -- the refusals: a reason and no retry, nothing spawned or aborted, the claim
  released before the reply.
"""

import json
import logging
import struct
from unittest.mock import MagicMock

import pytest
from simple_websocket import ConnectionClosed

from _api_support import Authed
from _openzfs_fixtures import VERSIONS
from _stream_support import (
    DONE, DRR_RECORD_SIZE, FILESYSTEM_FORCED, HEADER, VOLUME_FORCED, VOLUME_HEADER, capture_header,
    capture_names, expected_receive_argv, forced_for, incremental_header, make_header,
    stream_script,
)
from test_zfsbackup_api import (  # noqa: F401  (Stream: a harness, not a test; the rest: fixtures)
    GOOD_CONFIG, SERVER_DS, STREAM_PATH, TOKEN, Stream, _remote_config, _spy_releases, source_props,
    stored_config,
)
from zfsbackup.api import (
    DRR_RECORD_SIZE as API_RECORD_SIZE, RECEIVE_PROPERTIES_FILESYSTEM, RECEIVE_PROPERTIES_VOLUME,
    SERVER_OWNED_PROPERTIES, StreamBase, StreamHeader, StreamHeaderError, canonical_dataset_config,
    parse_stream_header, read_stream_header, stream_base_problem,
)
from zfsbackup.backup_manager import PROP_CLIENT_ID, PROP_CONFIG, PROP_SOURCE_DATASET

STORED = canonical_dataset_config(GOOD_CONFIG)
OWNED = {PROP_CONFIG: STORED, PROP_CLIENT_ID: "client1", PROP_SOURCE_DATASET: "pool/data"}


@pytest.fixture
def stream(mocker):
    return Stream(mocker)


# ---------------------------------------------------------------------------
# The parser, over the real captures
# ---------------------------------------------------------------------------

#: name -> (kind, the capture it is incremental from or None, accepted).
#: `*_p` are what `zfs send -p` writes FIRST (a compound stream, refused); `*_p_inner` is the real
#: substream behind its nvlist payload (accepted if it were ever sent alone).
CAPTURES = {
    "plain_full": ("filesystem", None, True),
    "plain_c": ("filesystem", None, True),
    "plain_L": ("filesystem", None, True),
    "plain_Lc": ("filesystem", None, True),
    "plain_incr": ("filesystem", "plain_full", True),
    "plain_Lc_incr": ("filesystem", "plain_full", True),
    "plain_p": (None, None, False),
    "plain_p_incr": (None, None, False),
    "plain_p_inner": ("filesystem", None, True),
    "plain_p_incr_inner": ("filesystem", "plain_full", True),
    "plain_Lc_p_inner": ("filesystem", None, True),
    "raw_full": ("filesystem", None, True),
    "raw_incr": ("filesystem", "raw_full", True),
    "raw_p": (None, None, False),
    "raw_p_incr": (None, None, False),
    "raw_p_inner": ("filesystem", None, True),
    "raw_p_incr_inner": ("filesystem", "raw_full", True),
    "volume_full": ("volume", None, True),
    "volume_Lc_full": ("volume", None, True),
    "volume_incr": ("volume", "volume_full", True),
    "volume_raw_full": ("volume", None, True),
    "volume_raw_incr": ("volume", "volume_raw_full", True),
    "volume_p_inner": ("volume", None, True),
    "volume_raw_p_inner": ("volume", None, True),
    "resume_plain_full": ("filesystem", None, True),
    "resume_plain_incr": ("filesystem", "plain_full", True),
    "resume_raw_full": ("filesystem", None, True),
    "resume_raw_incr": ("filesystem", "raw_full", True),
    "resume_volume_full": ("volume", None, True),
}


class TestParseCaptures:
    def test_the_table_covers_every_capture_of_every_version(self):
        for version in VERSIONS:
            assert sorted(capture_names(version)) == sorted(CAPTURES), version

    def test_every_capture_is_one_312_byte_record(self):
        for version in VERSIONS:
            for name in CAPTURES:
                assert len(capture_header(version, name)) == DRR_RECORD_SIZE == API_RECORD_SIZE

    @pytest.mark.parametrize("version", VERSIONS)
    @pytest.mark.parametrize("name", [n for n, (_, _, ok) in CAPTURES.items() if ok])
    def test_a_plain_substream_is_accepted_with_its_type(self, version, name):
        kind, _, _ = CAPTURES[name]
        header = parse_stream_header(capture_header(version, name))
        assert isinstance(header, StreamHeader)
        assert header.kind == kind
        assert header.big_endian is False
        assert header.to_guid != 0 and header.to_guid != header.from_guid

    @pytest.mark.parametrize("version", VERSIONS)
    @pytest.mark.parametrize("name", [n for n, (_, _, ok) in CAPTURES.items() if not ok])
    def test_a_compound_stream_is_refused(self, version, name):
        with pytest.raises(StreamHeaderError, match=r"compound streams \(zfs send -p, -R or -I\)"):
            parse_stream_header(capture_header(version, name))

    @pytest.mark.parametrize("version", VERSIONS)
    @pytest.mark.parametrize("name", [n for n, (_, base, ok) in CAPTURES.items() if ok])
    def test_a_full_stream_has_no_source_and_an_incremental_one_names_the_preceding_full(
        self, version, name
    ):
        _, base, _ = CAPTURES[name]
        header = parse_stream_header(capture_header(version, name))
        if base is None:
            assert header.from_guid == 0
        else:
            assert header.from_guid != 0
            assert header.from_guid == parse_stream_header(capture_header(version, base)).to_guid

    @pytest.mark.parametrize("version", VERSIONS)
    def test_the_inner_substream_of_a_compound_stream_is_the_plain_one(self, version):
        """`zfs send -p fsp@a` = a compound stub, then the substream `zfs send fsp@a` writes."""
        for inner, plain in (("plain_p_inner", "plain_full"), ("raw_p_inner", "raw_full"),
                             ("plain_p_incr_inner", "plain_incr"), ("raw_p_incr_inner", "raw_incr")):
            got = parse_stream_header(capture_header(version, inner))
            want = parse_stream_header(capture_header(version, plain))
            assert (got.kind, got.to_guid, got.from_guid) == (want.kind, want.to_guid, want.from_guid), inner

    @pytest.mark.parametrize("version", VERSIONS)
    def test_the_flag_variants_of_one_snapshot_are_the_same_snapshot(self, version):
        guids = {parse_stream_header(capture_header(version, n)).to_guid
                 for n in ("plain_full", "plain_c", "plain_L", "plain_Lc")}
        assert len(guids) == 1

    @pytest.mark.parametrize("version", VERSIONS)
    def test_the_compound_stub_is_what_the_refusal_is_about(self, version):
        """Not a reading of the parser: the capture itself (type 0, objset 0, header type 2, an
        nvlist payload) so the table above cannot drift from the real stream."""
        raw = capture_header(version, "plain_p")
        drr_type, payloadlen = struct.unpack_from("<II", raw, 0)
        versioninfo, objset = struct.unpack_from("<Q", raw, 16)[0], struct.unpack_from("<I", raw, 32)[0]
        assert (drr_type, versioninfo & 3, objset) == (0, 2, 0)
        assert payloadlen > 0

    @pytest.mark.parametrize("version", VERSIONS)
    def test_resumed_and_raw_streams_carry_a_payload_the_parser_ignores(self, version):
        for name in ("resume_plain_full", "resume_raw_full", "resume_volume_full"):
            raw = capture_header(version, name)
            assert struct.unpack_from("<I", raw, 4)[0] > 0, name           # drr_payloadlen
            assert parse_stream_header(raw).to_guid != 0

    @pytest.mark.parametrize("version", VERSIONS)
    def test_the_to_name_is_the_snapshot_the_sender_named(self, version):
        header = parse_stream_header(capture_header(version, "plain_full"))
        assert header.to_name.endswith("/M8/fsp@a")
        assert parse_stream_header(capture_header(version, "volume_full")).to_name.endswith("/M8/zvp@a")


# ---------------------------------------------------------------------------
# The parser, synthetic headers
# ---------------------------------------------------------------------------


class TestParseSynthetic:
    def test_a_filesystem_and_a_volume(self):
        fs = parse_stream_header(make_header("filesystem", to_guid=11, from_guid=0, toname="p/ds@a"))
        vol = parse_stream_header(make_header("volume", to_guid=12, from_guid=11, toname="p/zv@b"))
        assert (fs.kind, fs.to_guid, fs.from_guid, fs.to_name) == ("filesystem", 11, 0, "p/ds@a")
        assert (vol.kind, vol.to_guid, vol.from_guid, vol.to_name) == ("volume", 12, 11, "p/zv@b")

    def test_the_module_constants_are_the_harness_headers(self):
        assert parse_stream_header(HEADER).kind == "filesystem"
        assert parse_stream_header(VOLUME_HEADER).kind == "volume"

    @pytest.mark.parametrize("size", [0, 1, 7, 8, 56, 311])
    def test_fewer_than_312_bytes_is_refused_with_the_count(self, size):
        with pytest.raises(StreamHeaderError, match=f"ended after {size} bytes, before its 312-byte header"):
            parse_stream_header(make_header(size=size))

    def test_exactly_312_bytes_is_enough_and_more_is_ignored(self):
        assert parse_stream_header(make_header()).kind == "filesystem"
        assert parse_stream_header(make_header(size=DRR_RECORD_SIZE + 4096)).kind == "filesystem"

    @pytest.mark.parametrize("magic", [0, 1, 0x2F5BACBAB, 0x2F5BACBAD, 0xFFFFFFFFFFFFFFFF])
    def test_a_bad_magic_is_refused(self, magic):
        with pytest.raises(StreamHeaderError, match="not a zfs send stream"):
            parse_stream_header(make_header(magic=magic))

    def test_the_magic_is_checked_before_anything_else_is_believed(self):
        """A garbage record whose other fields would pass as a header."""
        with pytest.raises(StreamHeaderError, match="bad magic"):
            parse_stream_header(make_header(magic=0x1234, drr_type=0, hdrtype=1))

    @pytest.mark.parametrize("drr_type", [1, 2, 3, 4, 5, 6, 7, 0xFFFFFFFF])
    def test_a_first_record_that_is_not_a_begin_is_refused(self, drr_type):
        with pytest.raises(StreamHeaderError, match="does not start with a begin record"):
            parse_stream_header(make_header(drr_type=drr_type))

    @pytest.mark.parametrize("objset, kind", [(2, "filesystem"), (3, "volume")])
    def test_the_accepted_object_set_types(self, objset, kind):
        assert parse_stream_header(make_header(objset=objset)).kind == kind

    @pytest.mark.parametrize("objset", [0, 1, 4, 5, 255, 0xFFFFFFFF])
    def test_any_other_object_set_type_is_refused_by_number(self, objset):
        with pytest.raises(StreamHeaderError, match=rf"unsupported object set type \({objset}\)"):
            parse_stream_header(make_header(objset=objset))

    def test_a_compound_header_type_is_refused_as_compound(self):
        with pytest.raises(StreamHeaderError, match="compound streams"):
            parse_stream_header(make_header(hdrtype=2))

    @pytest.mark.parametrize("hdrtype", [0, 3])
    def test_the_other_header_types_are_refused_by_number(self, hdrtype):
        with pytest.raises(StreamHeaderError, match=rf"unsupported stream header type \({hdrtype}\)"):
            parse_stream_header(make_header(hdrtype=hdrtype))

    def test_only_the_low_two_bits_of_versioninfo_are_the_header_type(self):
        """The rest of `drr_versioninfo` is the feature flags (large blocks, compression, raw, ...)."""
        assert parse_stream_header(make_header(versioninfo=0x00000000_00480801 | 0x1000)).kind == "filesystem"
        with pytest.raises(StreamHeaderError, match="compound"):
            parse_stream_header(make_header(versioninfo=0xFFFFFFFF_FFFFFFFE))

    def test_the_header_type_is_checked_before_the_object_set(self):
        """A compound stub also has objset 0: it is named a compound stream, not an unsupported type."""
        with pytest.raises(StreamHeaderError, match="compound"):
            parse_stream_header(make_header(hdrtype=2, objset=0))

    def test_the_begin_check_is_before_the_header_type(self):
        with pytest.raises(StreamHeaderError, match="begin record"):
            parse_stream_header(make_header(drr_type=1, hdrtype=2))

    @pytest.mark.parametrize("kind", ["filesystem", "volume"])
    def test_a_big_endian_sender_is_read_in_its_byte_order(self, kind):
        header = parse_stream_header(make_header(kind, to_guid=0x0102030405060708, from_guid=9, big_endian=True))
        assert header.big_endian is True
        assert (header.kind, header.to_guid, header.from_guid) == (kind, 0x0102030405060708, 9)

    def test_a_big_endian_sender_is_checked_like_any_other(self):
        with pytest.raises(StreamHeaderError, match="compound"):
            parse_stream_header(make_header(hdrtype=2, big_endian=True))
        with pytest.raises(StreamHeaderError, match=r"object set type \(4\)"):
            parse_stream_header(make_header(objset=4, big_endian=True))

    def test_the_little_endian_reading_is_not_mistaken_for_big(self):
        header = parse_stream_header(make_header(to_guid=0x0102030405060708))
        assert header.big_endian is False and header.to_guid == 0x0102030405060708

    @pytest.mark.parametrize("payloadlen", [1, 96, 1048, 0xFFFFFFFF])
    def test_a_non_zero_payload_length_is_accepted(self, payloadlen):
        """Raw and resumed substreams carry a payload after the record; it is passed on untouched."""
        assert parse_stream_header(make_header(payloadlen=payloadlen)).kind == "filesystem"

    def test_the_to_name_stops_at_the_first_nul_and_survives_bad_utf8(self):
        raw = bytearray(make_header(toname="pool/ds@a"))
        raw[56 + 12:56 + 14] = b"\xff\xfe"                                   # after the NUL
        assert parse_stream_header(bytes(raw)).to_name == "pool/ds@a"
        raw = bytearray(make_header(toname="pool/\xe9s@a"))
        raw[56 + 5] = 0xFF
        assert "�" in parse_stream_header(bytes(raw)).to_name           # replaced, never raised

    def test_a_255_byte_name_fills_the_field(self):
        name = "p/" + "d" * 240 + "@" + "s" * 12
        assert parse_stream_header(make_header(toname=name)).to_name == name

    def test_the_error_text_carries_no_stream_bytes(self):
        """It is sent to the client and logged: counts and names only."""
        for bad in (make_header(magic=0xDEADBEEF), make_header(drr_type=1), make_header(objset=9),
                    make_header(hdrtype=3), make_header(size=100)):
            with pytest.raises(StreamHeaderError) as caught:
                parse_stream_header(bad)
            assert all(ord(c) < 128 for c in str(caught.value))
            assert "deadbeef" not in str(caught.value).lower()

    def test_a_stream_header_error_is_a_value_error(self):
        assert issubclass(StreamHeaderError, ValueError)


# ---------------------------------------------------------------------------
# The websocket reader
# ---------------------------------------------------------------------------


def ws_of(*messages):
    ws = MagicMock()
    ws.receive.side_effect = list(messages)
    return ws


class TestReadStreamHeader:
    def test_one_chunk_of_exactly_the_header(self):
        ws = ws_of(HEADER)
        assert read_stream_header(ws, 30) == HEADER
        assert ws.receive.call_count == 1

    def test_the_timeout_is_passed_to_every_receive(self):
        ws = ws_of(HEADER[:100], HEADER[100:])
        read_stream_header(ws, 12.5)
        assert [c.kwargs for c in ws.receive.call_args_list] == [{"timeout": 12.5}] * 2

    def test_a_header_split_across_messages(self):
        ws = ws_of(HEADER[:100], HEADER[100:200], HEADER[200:])
        assert read_stream_header(ws, 30) == HEADER
        assert ws.receive.call_count == 3

    def test_a_header_one_byte_at_a_time(self):
        ws = ws_of(*[HEADER[i:i + 1] for i in range(DRR_RECORD_SIZE)])
        assert read_stream_header(ws, 30) == HEADER

    def test_extra_bytes_in_the_last_message_are_kept(self):
        data = HEADER + b"payload-bytes"
        assert read_stream_header(ws_of(data), 30) == data
        assert read_stream_header(ws_of(HEADER[:300], HEADER[300:] + b"tail"), 30) == HEADER + b"tail"

    def test_it_stops_reading_as_soon_as_the_header_is_complete(self):
        ws = ws_of(HEADER[:200], HEADER[200:], b"never read")
        read_stream_header(ws, 30)
        assert ws.receive.call_count == 2

    def test_a_text_message_other_than_done_is_ignored(self):
        ws = ws_of(HEADER[:50], json.dumps({"hello": 1}), "not json at all", "[]", json.dumps({"done": False}),
                   json.dumps({"done": None}), json.dumps({"done": 0}), HEADER[50:])
        assert read_stream_header(ws, 30) == HEADER

    @pytest.mark.parametrize("sent", [0, 100, 311])
    def test_done_before_the_header_is_complete_is_a_header_error_with_the_count(self, sent):
        ws = ws_of(*([HEADER[:sent]] if sent else []), DONE)
        with pytest.raises(StreamHeaderError, match=f"ended after {sent} bytes, before its 312-byte header"):
            read_stream_header(ws, 30)

    def test_a_timeout_is_a_timeout_error_naming_the_seconds(self):
        ws = ws_of(HEADER[:10], None)
        with pytest.raises(TimeoutError, match="no data from the client for 600s"):
            read_stream_header(ws, 600)

    def test_a_timeout_before_anything_arrives(self):
        with pytest.raises(TimeoutError):
            read_stream_header(ws_of(None), 5)

    @pytest.mark.parametrize("before", [0, 1])
    def test_a_closed_connection_propagates(self, before):
        ws = ws_of(*([HEADER[:20]] if before else []), ConnectionClosed(None, None))
        with pytest.raises(ConnectionClosed):
            read_stream_header(ws, 30)

    def test_any_other_receive_failure_propagates(self):
        with pytest.raises(RuntimeError, match="boom"):
            read_stream_header(ws_of(RuntimeError("boom")), 30)

    def test_it_returns_bytes_it_was_given_without_touching_them(self):
        data = HEADER + bytes(range(256)) * 3
        assert read_stream_header(ws_of(data), 30) == data


# ---------------------------------------------------------------------------
# The declared base against the header's `drr_fromguid`
# ---------------------------------------------------------------------------

G, H = 0x1111, 0x2222


def header_from(from_guid, kind="filesystem"):
    return parse_stream_header(make_header(kind, from_guid=from_guid))


class TestStreamBaseProblem:
    @pytest.mark.parametrize("base, from_guid, ok", [
        # a declared guid must equal drr_fromguid
        (StreamBase(guid=G), G, True),
        (StreamBase(name="a", guid=G), G, True),            # the guid wins over the name
        (StreamBase(guid=G), H, False),
        (StreamBase(guid=G), 0, False),
        (StreamBase(name="a", guid=G), H, False),           # the name does not rescue a wrong guid
        # a declared name alone must meet a non-zero drr_fromguid
        (StreamBase(name="a"), G, True),
        (StreamBase(name="a"), 0, False),
        # no declared base: a full stream
        (StreamBase(), 0, True),
        (StreamBase(), G, False),
        # the discard list is not the stream's business
        (StreamBase(guid=G, discard=frozenset({1, 2})), G, True),
        (StreamBase(discard=frozenset({1})), 0, True),
        (StreamBase(discard=frozenset({1})), G, False),
    ], ids=lambda v: str(v)[:40])
    def test_the_table(self, base, from_guid, ok):
        problem = stream_base_problem(header_from(from_guid), base)
        assert (problem is None) == ok, problem

    def test_the_messages_say_what_each_side_declared(self):
        wrong = stream_base_problem(header_from(H), StreamBase(guid=G))
        assert f"incremental source guid {G}" in wrong and f"source guid is {H}" in wrong
        full = stream_base_problem(header_from(0), StreamBase(guid=G))
        assert f"incremental source guid {G}" in full and "is a full stream" in full
        undeclared = stream_base_problem(header_from(H), StreamBase())
        assert "declared a full stream" in undeclared and f"source guid {H}" in undeclared
        named = stream_base_problem(header_from(0), StreamBase(name="autosnap_1"))
        assert "'autosnap_1'" in named and "full stream" in named

    @pytest.mark.parametrize("kind", ["filesystem", "volume"])
    def test_the_kind_does_not_matter(self, kind):
        assert stream_base_problem(header_from(G, kind), StreamBase(guid=G)) is None
        assert stream_base_problem(header_from(G, kind), StreamBase()) is not None

    @pytest.mark.parametrize("version", VERSIONS)
    def test_a_real_incremental_capture_against_its_real_base(self, version):
        full = parse_stream_header(capture_header(version, "plain_full"))
        incr = parse_stream_header(capture_header(version, "plain_incr"))
        assert stream_base_problem(incr, StreamBase(guid=full.to_guid)) is None
        assert stream_base_problem(incr, StreamBase()) is not None
        assert stream_base_problem(full, StreamBase()) is None
        assert stream_base_problem(full, StreamBase(guid=full.to_guid)) is not None


# ---------------------------------------------------------------------------
# The forced properties
# ---------------------------------------------------------------------------


class TestForcedProperties:
    def test_the_filesystem_set_is_pinned_in_order(self):
        assert list(RECEIVE_PROPERTIES_FILESYSTEM.items()) == list(FILESYSTEM_FORCED.items())
        assert list(FILESYSTEM_FORCED) == [
            "canmount", "readonly", "volmode", "mountpoint", "sharenfs", "sharesmb", "setuid", "exec", "devices"]

    def test_the_volume_set_is_pinned_in_order(self):
        assert list(RECEIVE_PROPERTIES_VOLUME.items()) == list(VOLUME_FORCED.items())
        assert list(VOLUME_FORCED.items()) == [("readonly", "on"), ("volmode", "none")]

    def test_a_filesystem_is_never_mounted_shared_or_trusted(self):
        assert FILESYSTEM_FORCED["canmount"] == "noauto" and FILESYSTEM_FORCED["mountpoint"] == "none"
        assert FILESYSTEM_FORCED["sharenfs"] == FILESYSTEM_FORCED["sharesmb"] == "off"
        assert (FILESYSTEM_FORCED["setuid"], FILESYSTEM_FORCED["exec"], FILESYSTEM_FORCED["devices"]) == (
            "off", "off", "off")
        assert FILESYSTEM_FORCED["readonly"] == "on"

    def test_a_volume_gets_nothing_a_volume_refuses(self):
        """A volume refuses `-o canmount` ("does not apply to datasets of this type") and the other
        filesystem-only properties, so the set is exactly these two."""
        assert set(VOLUME_FORCED) == {"readonly", "volmode"}
        assert not set(VOLUME_FORCED) & {"canmount", "mountpoint", "sharenfs", "sharesmb", "setuid", "exec",
                                         "devices"}

    def test_both_sets_are_disjoint_from_the_owned_names(self):
        """One name twice in one `zfs receive` is a usage error."""
        assert not set(FILESYSTEM_FORCED) & set(SERVER_OWNED_PROPERTIES)
        assert not set(VOLUME_FORCED) & set(SERVER_OWNED_PROPERTIES)

    def test_forced_for_picks_by_kind(self):
        assert forced_for("filesystem") == FILESYSTEM_FORCED and forced_for("volume") == VOLUME_FORCED


# ---------------------------------------------------------------------------
# What zfs receive is called with
# ---------------------------------------------------------------------------


class TestReceiveCall:
    """Through the harness: `zfs.receive.filesystem`'s arguments."""

    @pytest.mark.parametrize("mode", ["fresh", "resume"])
    @pytest.mark.parametrize("kind, header, forced", [
        ("filesystem", HEADER, FILESYSTEM_FORCED), ("volume", VOLUME_HEADER, VOLUME_FORCED)],
        ids=["filesystem", "volume"])
    def test_the_props_are_the_owned_values_then_the_forced_ones_for_the_streams_type(
        self, stream, mode, kind, header, forced
    ):
        stream.type = kind
        stream.ws.receive.side_effect = stream_script(b"chunk", header=header)
        stream.run(mode)
        assert stream.sent == [{"status": "ok"}]
        (call,) = stream.receive.call_args_list
        assert list(call.kwargs["props"].items()) == list({**OWNED, **forced}.items())

    @pytest.mark.parametrize("mode", ["fresh", "resume"])
    @pytest.mark.parametrize("header", [HEADER, VOLUME_HEADER], ids=["filesystem", "volume"])
    def test_holds_are_not_received_and_nothing_is_reset(self, stream, mode, header):
        stream.type = "volume" if header is VOLUME_HEADER else "filesystem"
        stream.ws.receive.side_effect = stream_script(b"chunk", header=header)
        stream.run(mode)
        kwargs = stream.receive.call_args.kwargs
        assert kwargs["holds"] is False
        assert "reset" not in kwargs                          # no `-x`: it protects nothing here
        assert (kwargs["save"], kwargs["mount"]) == (True, False)

    def test_fresh_and_resume_call_receive_identically_for_each_type(self, mocker):
        for kind, header in (("filesystem", HEADER), ("volume", VOLUME_HEADER)):
            calls = {}
            for mode in ("fresh", "resume"):
                s = Stream(mocker)
                s.type = kind
                s.ws.receive.side_effect = stream_script(b"chunk", header=header)
                s.run(mode)
                (call,) = s.receive.call_args_list
                calls[mode] = (str(call.args[0]), dict(call.kwargs))
            assert calls["fresh"] == calls["resume"], kind

    @pytest.mark.parametrize("header, forced", [(HEADER, FILESYSTEM_FORCED), (VOLUME_HEADER, VOLUME_FORCED)],
                             ids=["filesystem", "volume"])
    def test_the_force_flag_still_follows_decide_receive(self, stream, header, forced):
        """Unchanged from milestone C: a dataset with no snapshots or the base as newest gets -F; a
        new dataset gets none."""
        stream.type = "volume" if header is VOLUME_HEADER else "filesystem"
        stream.ws.receive.side_effect = stream_script(b"chunk", header=header)
        stream.run("resume")
        assert stream.receive.call_args.kwargs["force"] is True

    def test_a_new_dataset_is_received_without_force_whatever_its_type(self, mocker):
        for header in (HEADER, VOLUME_HEADER):
            s = Stream(mocker)
            s.exists.return_value = False
            resp = s.authed.client().post(
                "/backup/client1/pool/data/negotiate", json={"snapshots": [], "config": GOOD_CONFIG})
            assert resp.status_code == 200
            s.create.reset_mock()
            s.ws.receive.side_effect = stream_script(b"chunk", header=header)
            s.run("fresh")
            assert s.sent == [{"status": "ok"}]
            assert s.receive.call_args.kwargs["force"] is False
            assert s.receive.call_args.kwargs["props"] == {
                **OWNED, **forced_for("volume" if header is VOLUME_HEADER else "filesystem")}

    def test_the_type_of_a_dataset_that_does_not_exist_is_not_read(self, mocker):
        """There is nothing to compare: the stream decides what is created."""
        s = Stream(mocker)
        s.exists.return_value = False
        s.authed.client().post("/backup/client1/pool/data/negotiate", json={"snapshots": [], "config": GOOD_CONFIG})
        s.reads.clear()
        s.ws.receive.side_effect = stream_script(b"chunk", header=VOLUME_HEADER)
        s.run("fresh")
        assert "type" not in [prop for prop, _ in s.reads]

    def test_the_type_of_an_existing_dataset_is_read_once_without_a_source_restriction(self, stream):
        stream.run("resume")
        assert [r for r in stream.reads if r[0] == "type"] == [("type", None)]

    def test_the_type_is_read_of_the_server_dataset(self, stream):
        stream.run("resume")
        (call,) = [c for c in stream.get_value.call_args_list if c.args[1] == "type"]
        assert str(call.args[0]) == SERVER_DS

    def test_a_gone_dataset_after_the_header_is_received_without_force(self, stream):
        """The existence asked for `-F` is asked again after the abort/the header (unchanged)."""
        stream.exists.side_effect = [True, True, False]
        stream.run("resume")
        assert stream.receive.call_args.kwargs["force"] is False


class TestReceiveArgv:
    """The real `zfs receive` argv (only `Popen` is mocked): per type, fresh and resume, with and
    without `-F`."""

    procs = ()

    EXISTING = [f"{SERVER_DS}@autosnap_20260101000000\tsnapshot\t100\t5000\n"]

    def run(self, mocker, mock_subprocess, mode, header, *, exists=True):
        mocker.patch("zfsbackup.api.read_zfsbackup_properties", return_value={})
        mocker.patch("zfsbackup.api.zfs.exists", return_value=exists)
        mocker.patch("zfsbackup.api.zfs.create")
        kind = parse_stream_header(header).kind
        from _stream_support import get_value_answers
        mocker.patch("zfsbackup.api.zfs.get.value", side_effect=get_value_answers(
            {PROP_CONFIG: STORED, PROP_CLIENT_ID: "client1", "type": kind}))
        authed = Authed(_remote_config())
        query = {"mode": mode}
        if exists:
            query["from_guid"] = "5000"
        if not exists:
            assert authed.client().post(
                "/backup/client1/pool/data/negotiate", json={"snapshots": [], "config": GOOD_CONFIG}
            ).status_code == 200
            mock_subprocess.setup_multi(("",))                      # the receive
        elif mode == "resume":
            mock_subprocess.setup_multi((self.EXISTING,), ("",))    # snapshots, receive
        else:
            mock_subprocess.setup_multi(
                (self.EXISTING,), ([f"{SERVER_DS}\tfilesystem\t-\n"],), ("",))   # + token
        first = len(mock_subprocess.call_args_list)
        real = mock_subprocess.side_effect
        self.procs = []

        def recording(*args, **kwargs):
            self.procs.append(real(*args, **kwargs))
            return self.procs[-1]

        mock_subprocess.side_effect = recording
        ws = MagicMock()
        ws.receive.side_effect = stream_script(b"chunk", header=header)
        with authed.context(STREAM_PATH, query_string=query):
            authed.app.view_functions["backup_stream"].__wrapped__(ws, "client1", "pool/data")
        assert json.loads(ws.send.call_args[0][0]) == {"status": "ok"}
        argvs = [c[0][0][1:] for c in mock_subprocess.call_args_list[first:]]
        (argv,) = [a for a in argvs if a[0] == "receive" and a[1] != "-A"]
        return argv

    @pytest.mark.parametrize("mode", ["fresh", "resume"])
    def test_a_filesystem_gets_the_nine_forced_values(self, mocker, mock_subprocess, mode):
        argv = self.run(mocker, mock_subprocess, mode, incremental_header(5000))
        assert argv == expected_receive_argv(OWNED, "filesystem", SERVER_DS, force=True)
        assert argv[-4:] == ["-h", "-s", "-u", SERVER_DS]
        assert "-x" not in argv and argv.count("-o") == 3 + 9

    @pytest.mark.parametrize("mode", ["fresh", "resume"])
    def test_a_volume_gets_only_readonly_and_volmode(self, mocker, mock_subprocess, mode):
        argv = self.run(mocker, mock_subprocess, mode, incremental_header(5000, "volume"))
        assert argv == expected_receive_argv(OWNED, "volume", SERVER_DS, force=True)
        assert argv.count("-o") == 3 + 2
        assert argv[-8:] == ["-o", "readonly=on", "-o", "volmode=none", "-F", "-h", "-s", "-u", SERVER_DS][-8:]
        joined = " ".join(argv)
        for refused in ("canmount", "mountpoint", "sharenfs", "sharesmb", "setuid", "exec=", "devices"):
            assert refused not in joined, refused

    @pytest.mark.parametrize("kind", ["filesystem", "volume"])
    def test_a_new_dataset_is_received_without_dash_f(self, mocker, mock_subprocess, kind):
        argv = self.run(mocker, mock_subprocess, "fresh", make_header(kind), exists=False)
        assert argv == expected_receive_argv(OWNED, kind, SERVER_DS, force=False)
        assert "-F" not in argv

    def test_fresh_and_resume_argv_are_identical_for_each_type(self, mocker, mock_subprocess):
        for kind in ("filesystem", "volume"):
            fresh = self.run(mocker, mock_subprocess, "fresh", incremental_header(5000, kind))
            resumed = self.run(mocker, mock_subprocess, "resume", incremental_header(5000, kind))
            assert fresh == resumed, kind

    def test_the_header_is_in_the_stream_the_process_is_given(self, mocker, mock_subprocess):
        """The first bytes written to the real `zfs receive` stdin are the header, unchanged."""
        self.run(mocker, mock_subprocess, "resume", incremental_header(5000))
        receive_process = self.procs[-1]                   # the listing came first, the receive last
        writes = [c.args[0] for c in receive_process.stdin.write.call_args_list]
        assert writes == [incremental_header(5000), b"chunk"]


# ---------------------------------------------------------------------------
# The header bytes are the first input of the receive, unchanged
# ---------------------------------------------------------------------------


class TestHeaderBytesAreFedFirst:
    def test_the_header_is_written_first_then_every_chunk_in_order(self, stream):
        stream.ws.receive.side_effect = stream_script(b"one", b"two", b"three")
        stream.run("resume")
        assert stream.written == [HEADER, b"one", b"two", b"three"]
        assert stream.sent == [{"status": "ok"}]

    def test_bytes_that_arrive_with_the_header_are_not_split_or_reordered(self, stream):
        stream.ws.receive.side_effect = [HEADER + b"tail", b"next", DONE]
        stream.run("resume")
        assert stream.written == [HEADER + b"tail", b"next"]

    def test_a_header_split_across_messages_is_written_as_one_piece(self, stream):
        stream.ws.receive.side_effect = [HEADER[:100], HEADER[100:250], HEADER[250:], b"data", DONE]
        stream.run("resume")
        assert stream.written == [HEADER, b"data"]

    def test_a_header_with_a_payload_length_is_passed_on_unchanged(self, stream):
        """A raw or resumed substream: the payload that follows the record is simply more bytes."""
        header = make_header(payloadlen=96, flags=0x48)
        stream.ws.receive.side_effect = stream_script(b"p" * 96, b"data", header=header)
        stream.run("resume")
        assert stream.written[0] == header
        assert b"".join(stream.written) == header + b"p" * 96 + b"data"

    @pytest.mark.parametrize("version", VERSIONS)
    @pytest.mark.parametrize("name", ["plain_full", "raw_full", "resume_plain_full", "plain_incr"])
    def test_a_real_capture_goes_in_byte_for_byte(self, stream, version, name):
        raw = capture_header(version, name)
        guid = parse_stream_header(raw).from_guid
        stream.ws.receive.side_effect = stream_script(b"rest", header=raw)
        stream.run("resume", **({"from_guid": str(guid)} if guid else {}))
        assert stream.written == [raw, b"rest"]
        assert stream.sent == [{"status": "ok"}]

    def test_the_receive_is_started_before_the_first_write(self, stream):
        stream.writer.write.side_effect = lambda d: stream.order.append("write")
        stream.run("resume")
        assert stream.order == ["get", "receive", "write", "write"]

    def test_the_writer_is_closed_once_after_the_last_chunk(self, stream):
        stream.run("resume")
        stream.writer.close.assert_called_once()

    def test_the_header_reply_follows_the_close(self, stream):
        stream.writer.close.side_effect = lambda *a: stream.order.append("close")
        stream.ws.send.side_effect = lambda m: stream.order.append(("send", json.loads(m)))
        stream.run("resume")
        assert stream.order[-2:] == ["close", ("send", {"status": "ok"})]


# ---------------------------------------------------------------------------
# Refusals
# ---------------------------------------------------------------------------


def assert_refused(stream, reason, *, spawned=False, text=None):
    """A reply with `reason` and `error` and nothing else, nothing spawned or aborted, the claim
    and the gate released."""
    (reply,) = stream.sent
    assert set(reply) == {"error", "reason"}, reply            # no retry: the client cannot fix it by waiting
    assert reply["reason"] == reason
    if text is not None:
        assert text in reply["error"]
    assert stream.receive.called is spawned
    stream.abort.assert_not_called()
    stream.create.assert_not_called()
    stream.set.assert_not_called()
    stream.writer.write.assert_not_called()
    assert stream.gate.active == 0 and stream.claimed == set()
    return reply


BAD_HEADERS = {
    "compound-capture": (capture_header("2.2.2", "plain_p"), "compound streams"),
    "compound-raw-capture": (capture_header("2.4.1", "raw_p_incr"), "compound streams"),
    "compound-synthetic": (make_header(hdrtype=2), "compound streams"),
    "bad-magic": (make_header(magic=0xABCDEF), "not a zfs send stream"),
    "zeros": (b"\0" * 400, "not a zfs send stream"),
    "text": (b"hello, this is not a zfs stream " * 12, "not a zfs send stream"),
    "not-a-begin-record": (make_header(drr_type=1), "begin record"),
    "objset-0": (make_header(objset=0), "object set type (0)"),
    "objset-1": (make_header(objset=1), "object set type (1)"),
    "objset-4": (make_header(objset=4), "object set type (4)"),
    "hdrtype-0": (make_header(hdrtype=0), "header type (0)"),
    "hdrtype-3": (make_header(hdrtype=3), "header type (3)"),
}


class TestHeaderRefusals:
    @pytest.mark.parametrize("mode", ["fresh", "resume"])
    @pytest.mark.parametrize("data, text", list(BAD_HEADERS.values()), ids=list(BAD_HEADERS))
    def test_a_stream_that_does_not_start_with_a_plain_substream_is_refused(self, stream, mode, data, text):
        stream.ws.receive.side_effect = [data, DONE]
        stream.run(mode)
        assert_refused(stream, "stream_header", text=text)
        assert stream.ws.receive.call_count == 1                   # nothing past the header was read

    @pytest.mark.parametrize("data", [b"", b"x" * 100, HEADER[:311]], ids=["nothing", "100", "311"])
    def test_a_stream_that_ends_before_the_header_is_refused_with_the_count(self, stream, data):
        stream.ws.receive.side_effect = ([data] if data else []) + [DONE]
        stream.run("resume")
        assert_refused(stream, "stream_header", text=f"ended after {len(data)} bytes, before its 312-byte header")

    def test_a_refusal_is_one_server_warning_naming_the_reason_and_the_client(self, stream, caplog):
        stream.ws.receive.side_effect = [capture_header("2.2.2", "plain_p")]
        with caplog.at_level(logging.INFO, logger="zfsbackup.api"):
            stream.run("resume")
        warnings = [r.getMessage() for r in caplog.records if r.levelno >= logging.WARNING]
        assert len(warnings) == 1
        assert "refused (stream_header)" in warnings[0] and "client1" in warnings[0]
        assert "compound" in warnings[0]

    def test_the_claim_is_released_once_before_the_reply(self, stream):
        stream.ws.receive.side_effect = [make_header(hdrtype=2)]
        with _spy_releases(stream) as releases:
            stream.run("resume")
        assert releases == [(SERVER_DS, 0)]                     # once, before the first send
        assert stream.claimed == set()

    def test_no_receive_dash_a_and_no_token_lookup_even_when_a_partial_state_exists(self, stream):
        """The header is judged before the fresh-mode abort: a refused stream discards nothing."""
        stream.token(TOKEN)
        stream.ws.receive.side_effect = [make_header(objset=4)]
        stream.run("fresh")
        assert_refused(stream, "stream_header")
        stream.list.assert_not_called()                         # no snapshot listing, no token lookup
        assert stream.order == ["get"]                          # the config read; no abort, no receive

    def test_the_next_stream_is_served_after_a_refusal(self, stream):
        stream.ws.receive.side_effect = [make_header(hdrtype=2)]
        stream.run("resume")
        stream.ws.receive.side_effect = stream_script(b"x")
        stream.run("resume")
        assert stream.sent[0]["reason"] == "stream_header" and stream.sent[1] == {"status": "ok"}

    def test_a_refusal_survives_a_dead_socket(self, stream):
        stream.ws.receive.side_effect = [make_header(hdrtype=2)]
        stream.ws.send.side_effect = Exception("closed")
        stream.run("resume")                                    # must not raise
        stream.receive.assert_not_called()
        assert stream.gate.active == 0 and stream.claimed == set()

    def test_the_header_is_judged_before_the_snapshot_listing_and_the_decision(self, stream):
        """A bad header wins over a decision that would also refuse (B-2 review): nothing is listed."""
        stream.server_has("autosnap_20260101000000", "autosnap_20260102000000")
        stream.ws.receive.side_effect = [make_header(hdrtype=2)]
        stream.run("resume", **{"from": "autosnap_20260101000000", "from_guid": "1"})
        (reply,) = stream.sent
        assert reply["reason"] == "stream_header"
        stream.list.assert_not_called()

    def test_a_decision_refusal_reads_exactly_the_header_first(self, stream):
        stream.server_has("autosnap_20260101000000", "autosnap_20260102000000")
        guid = int(stream.snapshots[0]["guid"].value)
        stream.ws.receive.side_effect = stream_script(b"never read", header=incremental_header(guid))
        stream.run("resume", **{"from": "autosnap_20260101000000", "from_guid": str(guid)})
        (reply,) = stream.sent
        assert reply["reason"] == "server_newer"
        assert stream.ws.receive.call_count == 1
        stream.receive.assert_not_called()

    def test_a_dry_run_reads_no_header_and_checks_no_type(self, mocker):
        s = Stream(mocker, dry_run=True)
        s.ws.receive.side_effect = [make_header(hdrtype=2)]
        s.run("fresh")
        assert s.sent == [{"status": "ok", "dry_run": True}]
        s.ws.receive.assert_not_called()
        assert "type" not in [prop for prop, _ in s.reads]
        s.receive.assert_not_called()

    def test_an_unexpected_failure_while_reading_is_reported_not_raised(self, stream):
        stream.ws.receive.side_effect = RuntimeError("socket exploded")
        stream.run("resume")
        assert stream.sent == [{"error": "socket exploded"}]
        stream.receive.assert_not_called()
        assert stream.gate.active == 0 and stream.claimed == set()


class TestBaseMismatch:
    @pytest.mark.parametrize("query, header, text", [
        # the client declared a full stream, the stream is incremental
        ({}, incremental_header(5000), "declared a full stream"),
        # the client declared an incremental from guid 5000, the stream is full
        ({"from": "autosnap_20260101000000", "from_guid": "5000"}, HEADER, "stream is a full stream"),
        # ... from another guid
        ({"from": "autosnap_20260101000000", "from_guid": "5000"}, incremental_header(6000),
         "source guid is 6000"),
        # a name alone, the stream is full
        ({"from": "autosnap_20260101000000"}, HEADER, "named the incremental source"),
    ], ids=["declared-full", "declared-incremental-sent-full", "other-guid", "name-only-sent-full"])
    @pytest.mark.parametrize("mode", ["fresh", "resume"])
    def test_a_stream_that_is_not_the_declared_one_is_refused(self, stream, mode, query, header, text):
        stream.ws.receive.side_effect = stream_script(b"x", header=header)
        stream.run(mode, **query)
        reply = assert_refused(stream, "base_mismatch", text=text)
        assert reply["error"].endswith("so nothing is received")

    def test_it_is_refused_before_the_abort_even_with_a_partial_state(self, stream):
        stream.token(TOKEN)
        stream.ws.receive.side_effect = stream_script(b"x", header=incremental_header(5000))
        stream.run("fresh")
        assert_refused(stream, "base_mismatch")
        assert stream.order == ["get"]

    def test_the_claim_is_released_once_before_the_reply(self, stream):
        stream.ws.receive.side_effect = stream_script(b"x", header=incremental_header(5000))
        with _spy_releases(stream) as releases:
            stream.run("resume")
        assert releases == [(SERVER_DS, 0)]

    def test_it_is_one_warning(self, stream, caplog):
        stream.ws.receive.side_effect = stream_script(b"x", header=incremental_header(5000))
        with caplog.at_level(logging.INFO, logger="zfsbackup.api"):
            stream.run("resume")
        warnings = [r.getMessage() for r in caplog.records if r.levelno >= logging.WARNING]
        assert len(warnings) == 1 and "refused (base_mismatch)" in warnings[0]

    def test_a_matching_incremental_is_received_for_both_types(self, stream):
        stream.ws.receive.side_effect = stream_script(b"x", header=incremental_header(5000))
        stream.run("resume", **{"from": "autosnap_20260101000000", "from_guid": "5000"})
        assert stream.sent == [{"status": "ok"}]


class TestTypeMismatch:
    @pytest.mark.parametrize("existing, header, incoming", [
        ("filesystem", VOLUME_HEADER, "volume"), ("volume", HEADER, "filesystem")],
        ids=["volume-over-filesystem", "filesystem-over-volume"])
    @pytest.mark.parametrize("mode", ["fresh", "resume"])
    def test_a_stream_of_the_other_type_is_refused_both_ways(self, stream, mode, existing, header, incoming):
        stream.type = existing
        stream.ws.receive.side_effect = stream_script(b"x", header=header)
        stream.run(mode)
        reply = assert_refused(stream, "type_mismatch")
        assert f"is a {existing} on the server and the stream is a {incoming}" in reply["error"]
        assert reply["error"].endswith("so nothing is received") and SERVER_DS in reply["error"]

    def test_the_matching_types_are_received(self, stream):
        for existing, header in (("filesystem", HEADER), ("volume", VOLUME_HEADER)):
            stream.type = existing
            stream.ws.receive.side_effect = stream_script(b"x", header=header)
            stream.run("resume")
        assert stream.sent == [{"status": "ok"}] * 2

    @pytest.mark.parametrize("existing", ["snapshot", "bookmark", "", "FILESYSTEM"])
    def test_any_other_type_the_server_reports_is_a_mismatch(self, stream, existing):
        stream.type = existing
        stream.run("resume")
        assert_refused(stream, "type_mismatch")

    def test_the_refusal_is_before_the_abort_and_the_spawn(self, stream):
        stream.token(TOKEN)
        stream.type = "volume"
        stream.run("fresh")
        assert_refused(stream, "type_mismatch")
        assert stream.order == ["get"]

    def test_the_claim_is_released_once_before_the_reply(self, stream):
        stream.type = "volume"
        with _spy_releases(stream) as releases:
            stream.run("resume")
        assert releases == [(SERVER_DS, 0)]

    def test_it_is_one_warning(self, stream, caplog):
        stream.type = "volume"
        with caplog.at_level(logging.INFO, logger="zfsbackup.api"):
            stream.run("resume")
        warnings = [r.getMessage() for r in caplog.records if r.levelno >= logging.WARNING]
        assert len(warnings) == 1 and "refused (type_mismatch)" in warnings[0]

    def test_an_unreadable_type_is_a_500_style_error_and_nothing_is_spawned(self, stream, caplog):
        stream.type = RuntimeError("cannot open: permission denied for tank/secret")
        with caplog.at_level(logging.ERROR, logger="zfsbackup.api"):
            stream.run("fresh")
        assert stream.sent == [{"error": f"Failed to read the type of {SERVER_DS}"}]
        assert "tank/secret" not in json.dumps(stream.sent) and "tank/secret" in caplog.text
        stream.receive.assert_not_called()
        stream.abort.assert_not_called()
        assert stream.gate.active == 0 and stream.claimed == set()

    def test_a_volume_stream_into_a_new_dataset_is_never_a_mismatch(self, mocker):
        s = Stream(mocker)
        s.exists.return_value = False
        s.authed.client().post("/backup/client1/pool/data/negotiate", json={"snapshots": [], "config": GOOD_CONFIG})
        s.type = "filesystem"                                   # (unread: the dataset does not exist)
        s.ws.receive.side_effect = stream_script(b"x", header=VOLUME_HEADER)
        s.run("fresh")
        assert s.sent == [{"status": "ok"}]


class TestHeaderPhaseEnds:
    """The connection or the data stops while the header is still being read."""

    @pytest.mark.parametrize("before", [[], [b"x" * 100]], ids=["at-once", "after-100-bytes"])
    def test_a_closed_connection_spawns_nothing_and_releases_everything(self, stream, before):
        stream.ws.receive.side_effect = before + [ConnectionClosed(None, None)]
        with _spy_releases(stream):
            stream.run("resume")
        stream.receive.assert_not_called()
        assert stream.sent == []                                # nobody to answer
        assert stream.gate.active == 0 and stream.claimed == set()
        stream.abort.assert_not_called()

    def test_a_closed_connection_before_the_header_leaves_a_partial_state_alone(self, stream):
        stream.token(TOKEN)
        stream.ws.receive.side_effect = ConnectionClosed(None, None)
        stream.run("fresh")
        stream.abort.assert_not_called()
        assert stream.order == ["get"]

    def test_an_idle_timeout_is_reported_as_the_timeout_it_is(self, stream):
        stream.ws.receive.side_effect = [b"x" * 10, None]
        stream.run("resume")
        (reply,) = stream.sent
        assert "no data from the client" in reply["error"] and "reason" not in reply and "retry" not in reply
        stream.receive.assert_not_called()
        assert stream.gate.active == 0 and stream.claimed == set()

    def test_the_next_stream_can_claim_the_name_after_any_header_phase_end(self, stream):
        for first in (ConnectionClosed(None, None), None, DONE):
            stream.ws.receive.side_effect = [first]
            stream.run("resume")
            assert stream.claimed == set()
        stream.ws.receive.side_effect = stream_script(b"x")
        stream.run("resume")
        assert stream.sent[-1] == {"status": "ok"}


# ---------------------------------------------------------------------------
# A base named without its guid (B-2 review)
# ---------------------------------------------------------------------------

A, B = "autosnap_20260101000000", "autosnap_20260102000000"


class TestNameOnlyBase:
    """A client that sends `from=<name>` and no `from_guid`: the server finds the base by NAME, so
    the snapshot it holds under that name must be the one the stream is really incremental from,
    or `-F` would be decided from the wrong snapshot."""

    @pytest.fixture
    def held(self, stream):
        stream.server_has(A, B)
        return stream

    def guid_of(self, stream, name):
        (row,) = [r for r in stream.snapshots if r.short == name]
        return int(row["guid"].value)

    @pytest.mark.parametrize("mode", ["fresh", "resume"])
    def test_a_different_guid_under_the_name_is_a_base_mismatch(self, held, mode):
        held.ws.receive.side_effect = stream_script(b"x", header=incremental_header(0xBAD))
        held.run(mode, **{"from": B})
        reply = assert_refused(held, "base_mismatch")
        guid = self.guid_of(held, B)
        assert f"{B!r}" in reply["error"] and f"guid {guid}" in reply["error"]
        assert f"source guid is {0xBAD}" in reply["error"] and reply["error"].endswith("so nothing is received")
        assert held.ws.receive.call_count == 1                 # the header only

    def test_it_is_judged_before_the_decision_the_abort_and_the_spawn(self, held):
        held.token(TOKEN)
        held.ws.receive.side_effect = stream_script(b"x", header=incremental_header(0xBAD))
        held.run("fresh", **{"from": A, "discard": "1"})
        assert_refused(held, "base_mismatch")
        assert held.order == ["get"]                            # no token lookup, no `receive -A`

    def test_the_claim_is_released_once_before_the_reply(self, held):
        held.ws.receive.side_effect = stream_script(header=incremental_header(0xBAD))
        with _spy_releases(held) as releases:
            held.run("resume", **{"from": B})
        assert releases == [(SERVER_DS, 0)]

    def test_the_matching_guid_goes_on_to_the_decision(self, held):
        guid = self.guid_of(held, B)
        held.ws.receive.side_effect = stream_script(b"x", header=incremental_header(guid))
        held.run("resume", **{"from": B})
        assert held.sent == [{"status": "ok"}] and held.receive.call_args.kwargs["force"] is True

    def test_the_matching_guid_of_an_older_base_still_meets_the_server_newer_decision(self, held):
        guid = self.guid_of(held, A)
        held.ws.receive.side_effect = stream_script(b"x", header=incremental_header(guid))
        held.run("resume", **{"from": A})
        assert_refused(held, "server_newer")

    @pytest.mark.parametrize("mode", ["fresh", "resume"])
    def test_a_name_the_server_does_not_hold_is_base_missing(self, held, mode):
        held.ws.receive.side_effect = stream_script(b"x", header=incremental_header(7))
        held.run(mode, **{"from": "autosnap_29990101000000"})
        reply = assert_refused(held, "base_missing")
        assert "autosnap_29990101000000" in reply["error"]

    def test_a_full_stream_with_a_named_base_is_a_base_mismatch_before_any_listing(self, held):
        held.ws.receive.side_effect = stream_script(b"x", header=HEADER)
        held.run("resume", **{"from": A})
        assert_refused(held, "base_mismatch", text="named the incremental source")
        held.list.assert_not_called()

    def test_a_guid_given_is_not_second_guessed_by_name(self, held):
        """With `from_guid` the name is only for logs: the guid is compared with the header."""
        guid = self.guid_of(held, B)
        held.ws.receive.side_effect = stream_script(b"x", header=incremental_header(guid))
        held.run("resume", **{"from": "a-name-the-server-never-heard-of", "from_guid": str(guid)})
        assert held.sent == [{"status": "ok"}]


# ---------------------------------------------------------------------------
# One WARNING per (server dataset, reason)
# ---------------------------------------------------------------------------


def stream_warnings(caplog):
    return [r for r in caplog.records
            if r.name == "zfsbackup.api" and "refused (" in r.getMessage() and r.levelno >= logging.DEBUG
            and r.getMessage().startswith("stream from client")]


def levels(caplog, reason):
    return [r.levelname for r in stream_warnings(caplog) if f"refused ({reason})" in r.getMessage()]


class TestRefusalIsNotedOnce:
    """A client retries a refused stream every cycle: WARNING the first time, DEBUG after, until a
    stream of that dataset succeeds. The reply is sent every time."""

    BAD = make_header(hdrtype=2)

    def refuse(self, stream, header=None):
        stream.ws.receive.side_effect = [header or self.BAD]
        stream.run("resume")

    def test_warning_then_debug_on_repeat(self, stream, caplog):
        with caplog.at_level(logging.DEBUG, logger="zfsbackup.api"):
            for _ in range(3):
                self.refuse(stream)
        assert levels(caplog, "stream_header") == ["WARNING", "DEBUG", "DEBUG"]
        assert [m["reason"] for m in stream.sent] == ["stream_header"] * 3       # answered every time
        assert stream.claimed == set() and stream.gate.active == 0

    def test_a_success_re_arms_the_warning(self, stream, caplog):
        with caplog.at_level(logging.DEBUG, logger="zfsbackup.api"):
            self.refuse(stream)
            self.refuse(stream)
            stream.ws.receive.side_effect = stream_script(b"x")
            stream.run("resume")
            self.refuse(stream)
        assert stream.sent[2] == {"status": "ok"}
        assert levels(caplog, "stream_header") == ["WARNING", "DEBUG", "WARNING"]

    def test_a_failed_stream_does_not_re_arm_it(self, stream, caplog):
        with caplog.at_level(logging.DEBUG, logger="zfsbackup.api"):
            self.refuse(stream)
            stream.writer.close.side_effect = RuntimeError("cannot receive: bad stream")
            stream.ws.receive.side_effect = stream_script(b"x")
            stream.run("resume")
            self.refuse(stream)
        assert "error" in stream.sent[1]
        assert levels(caplog, "stream_header") == ["WARNING", "DEBUG"]

    def test_each_reason_has_its_own_first_warning(self, stream, caplog):
        with caplog.at_level(logging.DEBUG, logger="zfsbackup.api"):
            self.refuse(stream)
            stream.ws.receive.side_effect = stream_script(b"x", header=incremental_header(5))
            stream.run("resume")                                               # base_mismatch
            stream.type = "volume"
            stream.ws.receive.side_effect = stream_script(b"x")
            stream.run("resume")                                               # type_mismatch
            self.refuse(stream)
        assert levels(caplog, "stream_header") == ["WARNING", "DEBUG"]
        assert levels(caplog, "base_mismatch") == ["WARNING"]
        assert levels(caplog, "type_mismatch") == ["WARNING"]

    def test_each_server_dataset_has_its_own_first_warning(self, stream, caplog):
        with caplog.at_level(logging.DEBUG, logger="zfsbackup.api"):
            self.refuse(stream)
            stream.ws.receive.side_effect = [self.BAD]
            stream.run("resume", dataset="pool/other")
            stream.ws.receive.side_effect = [self.BAD]
            stream.run("resume", dataset="pool/other")
        assert levels(caplog, "stream_header") == ["WARNING", "WARNING", "DEBUG"]

    def test_the_text_is_the_same_at_both_levels(self, stream, caplog):
        with caplog.at_level(logging.DEBUG, logger="zfsbackup.api"):
            self.refuse(stream)
            self.refuse(stream)
        first, second = [r.getMessage() for r in stream_warnings(caplog)]
        assert first == second and "compound" in first and "client1" in first

    def test_another_clients_success_does_not_re_arm_this_dataset(self, stream, caplog):
        """The key is the server dataset, which is under the bound client id."""
        with caplog.at_level(logging.DEBUG, logger="zfsbackup.api"):
            self.refuse(stream)
            stream.ws.receive.side_effect = stream_script(b"x")
            stream.run("resume", client="client2")
            self.refuse(stream)
        assert levels(caplog, "stream_header") == ["WARNING", "DEBUG"]

    def test_the_noted_set_is_bounded(self, stream):
        from _api_support import _find_in_closures
        noted = _find_in_closures(stream.app, "_refused_noted")
        for i in range(1100):
            stream.ws.receive.side_effect = [self.BAD]
            stream.run("resume", dataset=f"pool/d{i}")
        assert len(noted) <= 1025
