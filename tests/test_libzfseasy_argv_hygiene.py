"""Argument hygiene in libzfseasy: property names never reach `zfs` as an option, and a log line
never leaks URL credentials (production readiness plan S5/S6 follow-ups).

* `Validate.attribute` refuses a leading `-` (`-o:x` would parse as an option in `zfs set`, `zfs
  inherit`, `zfs get` ... where a name is a bare argument) and a name longer than `Validate.max_name_len`.
* Every command that puts a caller-supplied property name into argv validates it first, so a leading-dash
  name raises `ValueError` with NO `Popen` call: set, inherit, get, get.value, list(properties=),
  create / snapshot / clone with `-o`.
* `receive` validates the names in `reset` (`-x`) and `props` (`-o`) before it execs; every name zfsbackup
  excludes passes, so the server's receive cannot start raising.
* `_redact` clips what it scans (`_MAX_REDACT`) without leaking a credential cut by the clip.

Nothing here runs `zfs`: `subprocess.Popen` is the `mock_subprocess` fixture, and "no Popen call" is asserted
on it.
"""

import time

import pytest

import libzfseasy as zfs
from libzfseasy.types import Dataset, Filesystem, Snapshot, Validate, Volume
from libzfseasy.zfs import _MAX_REDACT, _redact

pytestmark = pytest.mark.unit


def no_exec(mock_subprocess):
    assert mock_subprocess.call_count == 0, mock_subprocess.call_args_list


# ---------------------------------------------------------------------------
# Validate.attribute
# ---------------------------------------------------------------------------


class TestValidateAttribute:
    @pytest.mark.parametrize("name", [
        "-o:x", "-x:y", "-foo", "-", "--", "-a", "-compression", "-:", "- ",
    ])
    def test_a_leading_dash_is_refused(self, name):
        with pytest.raises(ValueError, match="is not a valid ZFS property name"):
            Validate.attribute(name)

    @pytest.mark.parametrize("name", [
        "a:b", "x-y:z", "org.zfsbackup:config", "org.zfsbackup:anchor.offsite", "MyOrg:Prop",
        "UPPER:CASE", "a:", ":b", "a_b:c-d.e", "compression", "mountpoint", "quota", "com.sun:auto-snapshot",
    ])
    def test_valid_names(self, name):
        Validate.attribute(name)
        Validate.property(name)

    @pytest.mark.parametrize("name", ["nocolon", "not-native", "", "a b:c", "a\n:b", "a:b\n", "é:x", "a:b$"])
    def test_other_invalid_names_are_still_refused(self, name):
        with pytest.raises(ValueError):
            Validate.attribute(name)

    def test_the_length_boundary(self):
        assert Validate.max_name_len == 255
        Validate.attribute("a:" + "x" * (Validate.max_name_len - 2))                      # 255
        Validate.attribute("x" * (Validate.max_name_len - 2) + ":a")
        with pytest.raises(ValueError, match="is not a valid ZFS property name"):
            Validate.attribute("a:" + "x" * (Validate.max_name_len - 1))                  # 256
        with pytest.raises(ValueError):
            Validate.attribute("a:" + "x" * 5000)

    def test_the_255_character_name_has_exactly_that_length(self):
        name = "a:" + "x" * 253
        assert len(name) == 255
        Validate.attribute(name)
        with pytest.raises(ValueError):
            Validate.attribute(name + "x")

    def test_the_error_does_not_echo_a_huge_name_verbatim_into_a_second_message(self):
        with pytest.raises(ValueError) as excinfo:
            Validate.attribute("-" + "x" * 10)
        assert str(excinfo.value).endswith("is not a valid ZFS property name")

    def test_native_attribute_stays_lenient_for_names_zfs_returned(self):
        Validate.native_attribute("somenewnativeprop")                # a property a newer OpenZFS added
        Validate.native_attribute("a:b")
        with pytest.raises(ValueError):
            Validate.native_attribute("-o:x")


# ---------------------------------------------------------------------------
# Commands: a leading dash never reaches zfs
# ---------------------------------------------------------------------------

DASH = ["-o:x", "-x:y", "-foo", "-compression"]
FS = Filesystem("pool/data")


class TestLeadingDashNeverReachesZfs:
    @pytest.mark.parametrize("name", DASH)
    def test_set(self, mock_subprocess, name):
        with pytest.raises(ValueError):
            zfs.set(FS, {name: "v"})
        no_exec(mock_subprocess)

    @pytest.mark.parametrize("name", DASH)
    def test_set_after_a_valid_name_still_execs_nothing(self, mock_subprocess, name):
        with pytest.raises(ValueError):
            zfs.set(FS, {"org:ok": "1", name: "v"})
        no_exec(mock_subprocess)

    @pytest.mark.parametrize("name", DASH)
    def test_inherit_with_a_list(self, mock_subprocess, name):
        with pytest.raises(ValueError):
            zfs.inherit(FS, [name])
        no_exec(mock_subprocess)

    @pytest.mark.parametrize("name", DASH)
    def test_inherit_with_a_string(self, mock_subprocess, name):
        """The bare positional path: a string `props` is wrapped into a one-element list."""
        with pytest.raises(ValueError):
            zfs.inherit(FS, name)
        no_exec(mock_subprocess)

    @pytest.mark.parametrize("name", DASH)
    def test_inherit_mixed(self, mock_subprocess, name):
        with pytest.raises(ValueError):
            zfs.inherit(FS, ["org:ok", name])
        no_exec(mock_subprocess)

    @pytest.mark.parametrize("name", DASH)
    def test_get(self, mock_subprocess, name):
        with pytest.raises(ValueError):
            zfs.get(FS, name)
        no_exec(mock_subprocess)

    @pytest.mark.parametrize("name", DASH)
    def test_get_with_a_list(self, mock_subprocess, name):
        with pytest.raises(ValueError):
            zfs.get(FS, ["org:ok", name])
        no_exec(mock_subprocess)

    @pytest.mark.parametrize("name", DASH)
    def test_get_value(self, mock_subprocess, name):
        with pytest.raises(ValueError):
            zfs.get.value(FS, name)
        no_exec(mock_subprocess)

    @pytest.mark.parametrize("name", DASH)
    def test_get_value_with_sources(self, mock_subprocess, name):
        with pytest.raises(ValueError):
            zfs.get.value(FS, name, sources="local")
        no_exec(mock_subprocess)

    @pytest.mark.parametrize("name", DASH)
    def test_list_properties(self, mock_subprocess, name):
        with pytest.raises(ValueError):
            zfs.list(roots=Dataset("pool/data"), properties=[name])
        no_exec(mock_subprocess)

    @pytest.mark.parametrize("name", DASH)
    def test_list_properties_as_a_string(self, mock_subprocess, name):
        with pytest.raises(ValueError):
            zfs.list(roots=Dataset("pool/data"), properties=name)
        no_exec(mock_subprocess)

    @pytest.mark.parametrize("name", DASH)
    def test_create_filesystem_with_o(self, mock_subprocess, name):
        with pytest.raises(ValueError):
            zfs.create.filesystem("pool/new", properties={name: "v"})
        no_exec(mock_subprocess)

    @pytest.mark.parametrize("name", DASH)
    def test_create_dispatcher_with_o(self, mock_subprocess, name):
        with pytest.raises(ValueError):
            zfs.create("pool/new", properties={name: "v"})
        no_exec(mock_subprocess)

    @pytest.mark.parametrize("name", DASH)
    def test_create_volume_with_o(self, mock_subprocess, name):
        with pytest.raises(ValueError):
            zfs.create.volume("pool/vol", "1G", properties={name: "v"})
        no_exec(mock_subprocess)

    @pytest.mark.parametrize("name", DASH)
    def test_snapshot_with_o(self, mock_subprocess, name):
        with pytest.raises(ValueError):
            zfs.snapshot(FS, "snap", properties={name: "v"})
        no_exec(mock_subprocess)

    @pytest.mark.parametrize("name", DASH)
    def test_recursive_snapshot_with_o_lists_nothing_first(self, mock_subprocess, name):
        with pytest.raises(ValueError):
            zfs.snapshot(FS, "snap", recursive=True, properties={name: "v"})
        no_exec(mock_subprocess)

    @pytest.mark.parametrize("name", DASH)
    def test_clone_with_o(self, mock_subprocess, name):
        with pytest.raises(ValueError):
            zfs.clone(Snapshot(FS, "snap"), "pool/clone", properties={name: "v"})
        no_exec(mock_subprocess)

    def test_the_valid_equivalents_do_exec(self, mock_subprocess):
        """The control: the same calls with a valid name run (so the tests above fail for the name)."""
        mock_subprocess.setup(stdout="")
        zfs.set(FS, {"org:ok": "1"})
        assert mock_subprocess.call_count == 1
        argv = mock_subprocess.call_args_list[0][0][0]
        assert argv[1:] == ["set", "org:ok=1", "pool/data"]


# ---------------------------------------------------------------------------
# receive: -x and -o names are validated before exec
# ---------------------------------------------------------------------------


def _receive_forms(**kwargs):
    """Both the filesystem and the dataset form of `receive`, with the same keyword arguments."""
    return [
        pytest.param(lambda: zfs.receive.filesystem(Filesystem("pool/backup"), **kwargs), id="filesystem"),
        pytest.param(lambda: zfs.receive.dataset(Dataset("pool/backup"), **kwargs), id="dataset"),
    ]


class TestReceiveNamesAreValidated:
    @pytest.mark.parametrize("name", DASH + ["nocolon", "a b:c", "a\n:b", "a:" + "x" * 300])
    @pytest.mark.parametrize("form", ["filesystem", "dataset"])
    @pytest.mark.parametrize("shape", ["list", "string", "tuple"])
    def test_an_invalid_name_in_reset_raises_before_exec(self, mock_subprocess, name, form, shape):
        reset = {"list": [name], "string": name, "tuple": (name,)}[shape]
        target = Filesystem("pool/backup") if form == "filesystem" else Dataset("pool/backup")
        with pytest.raises(ValueError):
            getattr(zfs.receive, form)(target, reset=reset)
        no_exec(mock_subprocess)

    def test_an_empty_name_in_a_list_raises_before_exec(self, mock_subprocess):
        with pytest.raises(ValueError):
            zfs.receive.filesystem(Filesystem("pool/backup"), reset=[""])
        no_exec(mock_subprocess)

    @pytest.mark.parametrize("form", ["filesystem", "dataset"])
    def test_the_comma_string_form_validates_every_element(self, mock_subprocess, form):
        target = Filesystem("pool/backup") if form == "filesystem" else Dataset("pool/backup")
        with pytest.raises(ValueError):
            getattr(zfs.receive, form)(target, reset="mountpoint,-o:x")
        with pytest.raises(ValueError):
            getattr(zfs.receive, form)(target, reset="org:x,-foo,quota")
        no_exec(mock_subprocess)

    @pytest.mark.parametrize("name", DASH + ["nocolon", "a b:c", ""])
    @pytest.mark.parametrize("form", ["filesystem", "dataset"])
    def test_an_invalid_key_in_props_raises_before_exec(self, mock_subprocess, name, form):
        target = Filesystem("pool/backup") if form == "filesystem" else Dataset("pool/backup")
        with pytest.raises(ValueError):
            getattr(zfs.receive, form)(target, props={name: "v"})
        no_exec(mock_subprocess)

    def test_an_invalid_name_after_valid_ones_still_execs_nothing(self, mock_subprocess):
        with pytest.raises(ValueError):
            zfs.receive.filesystem(Filesystem("pool/backup"), reset=["mountpoint", "org:x", "-bad"])
        with pytest.raises(ValueError):
            zfs.receive.filesystem(Filesystem("pool/backup"), props={"org:ok": "1", "-bad": "2"})
        no_exec(mock_subprocess)

    @pytest.mark.parametrize("form", ["filesystem", "dataset"])
    def test_reset_with_valid_names_yields_x_flags_in_order(self, form):
        from libzfseasy.zfs import ReceiveCommand
        cmd = ReceiveCommand._get_options(reset=["mountpoint", "org:x"])
        assert cmd == ["-x", "mountpoint", "-x", "org:x"]

    @pytest.mark.parametrize("form", ["filesystem", "dataset"])
    def test_reset_through_the_real_command_argv(self, mock_subprocess, form):
        target = Filesystem("pool/backup") if form == "filesystem" else Dataset("pool/backup")
        mock_subprocess.setup(stdout="")
        handle = getattr(zfs.receive, form)(target, reset=["mountpoint", "org:x"])
        handle.close()
        argv = mock_subprocess.call_args_list[0][0][0][1:]
        assert argv[0] == "receive"
        assert argv[argv.index("-x"):argv.index("-x") + 4] == ["-x", "mountpoint", "-x", "org:x"]
        assert argv[-1] == "pool/backup"

    def test_a_comma_string_of_valid_names(self):
        from libzfseasy.zfs import ReceiveCommand
        assert ReceiveCommand._get_options(reset="mountpoint,org:x") == ["-x", "mountpoint", "-x", "org:x"]

    def test_every_name_zfsbackup_excludes_passes(self):
        """The server's receive passes these on every stream: validation must never reject them."""
        from zfsbackup.api import RECEIVE_EXCLUDED_PROPERTIES
        from libzfseasy.zfs import ReceiveCommand
        assert len(RECEIVE_EXCLUDED_PROPERTIES) == 3
        for name in RECEIVE_EXCLUDED_PROPERTIES:
            Validate.attribute(name)
        cmd = ReceiveCommand._get_options(reset=list(RECEIVE_EXCLUDED_PROPERTIES), force=True, save=True,
                                          mount=False)
        assert cmd == ["-x", "org.zfsbackup:config", "-x", "org.zfsbackup:client_id",
                       "-x", "org.zfsbackup:source_dataset", "-F", "-s", "-u"]

    def test_the_servers_receive_call_does_not_raise(self, mock_subprocess):
        from zfsbackup.api import RECEIVE_EXCLUDED_PROPERTIES
        mock_subprocess.setup(stdout="")
        handle = zfs.receive.filesystem(Filesystem("pool/backups/c1/data"), reset=list(RECEIVE_EXCLUDED_PROPERTIES),
                                        force=True, save=True, mount=False)
        handle.close()
        assert mock_subprocess.call_count == 1

    def test_origin_is_unaffected(self):
        from libzfseasy.zfs import ReceiveCommand
        origin = Snapshot(Filesystem("pool/src"), "snap")
        assert ReceiveCommand._get_options(origin=origin) == ["-o", "origin=pool/src@snap"]
        assert ReceiveCommand._get_options(origin=origin, reset=["org:x"]) == [
            "-o", "origin=pool/src@snap", "-x", "org:x"]

    def test_valid_props_still_become_o_flags(self):
        from libzfseasy.zfs import ReceiveCommand
        assert ReceiveCommand._get_options(props={"org:a": "1", "compression": "lz4"}) == [
            "-o", "org:a=1", "-o", "compression=lz4"]

    def test_no_reset_no_props_adds_nothing(self):
        from libzfseasy.zfs import ReceiveCommand
        assert ReceiveCommand._get_options() == []
        assert ReceiveCommand._get_options(reset=None, props=None) == []
        assert ReceiveCommand._get_options(reset=[], props={}) == []


# ---------------------------------------------------------------------------
# _redact
# ---------------------------------------------------------------------------


class TestRedact:
    def test_the_bound(self):
        assert _MAX_REDACT == 2048

    def test_credentials_in_a_short_url_are_hidden_as_before(self):
        assert _redact("https://user:pass@host/key") == "https://***@host/key"
        assert _redact("keylocation=https://user:pa:ss@example.com:8443/x") == "keylocation=https://***@example.com:8443/x"
        assert _redact("-o keylocation=ftp://u@h") == "-o keylocation=ftp://***@h"

    def test_several_urls_in_one_argument(self):
        assert _redact("a=https://u:p@h1/x b=http://v:q@h2/y") == "a=https://***@h1/x b=http://***@h2/y"

    def test_text_without_credentials_is_unchanged(self):
        for text in ("pool/data", "https://host/path", "a@b", "x://host", "zfs send -t 1-abc", ""):
            assert _redact(text) == text

    def test_an_ordinary_long_argument_is_unchanged(self):
        text = "x" * 2000 + " https://host/path"
        assert _redact(text[:_MAX_REDACT]) == text[:_MAX_REDACT]

    def test_a_huge_input_returns_promptly_and_clipped(self):
        text = "a" * 10_000_000
        started = time.monotonic()
        out = _redact(text)
        assert time.monotonic() - started < 2.0
        assert len(out) == _MAX_REDACT + 3
        assert out == "a" * _MAX_REDACT + "..."

    def test_a_huge_run_of_scheme_characters_is_not_quadratic(self):
        """The userinfo pattern is quadratic in a run of scheme characters: the clip bounds it."""
        text = "a" * 2_000_000 + "://" + "b" * 100_000
        started = time.monotonic()
        _redact(text)
        assert time.monotonic() - started < 2.0

    def test_a_credential_cut_by_the_clip_does_not_leak(self):
        text = "x" * 2040 + "https://user:pa" + "ssword@host/key"
        out = _redact(text)
        assert "user:pa" not in out and "password" not in out and "user" not in out
        assert out.endswith("https://***...")
        assert len(out) == len("x" * 2040 + "https://***...")

    def test_a_credential_cut_exactly_after_the_scheme(self):
        text = "x" * (_MAX_REDACT - len("https://")) + "https://" + "secret@host"
        out = _redact(text)
        assert "secret" not in out and out.endswith("https://***...")

    def test_a_credential_ending_inside_the_clip_is_hidden_normally(self):
        text = "x" * 2000 + "https://user:pw@host/" + "y" * 100
        out = _redact(text)
        assert "user" not in out and "pw" not in out
        assert "https://***@host/" in out and out.endswith("...")

    def test_exactly_the_limit_is_not_clipped(self):
        text = "x" * _MAX_REDACT
        assert _redact(text) == text and len(_redact(text)) == _MAX_REDACT
        assert not _redact(text).endswith("...")

    def test_one_over_the_limit_is_clipped(self):
        out = _redact("x" * (_MAX_REDACT + 1))
        assert out == "x" * _MAX_REDACT + "..."

    def test_exactly_the_limit_with_a_credential_inside(self):
        text = "https://user:pw@h/" + "z" * (_MAX_REDACT - len("https://user:pw@h/"))
        assert len(text) == _MAX_REDACT
        out = _redact(text)
        assert out == "https://***@h/" + "z" * (_MAX_REDACT - len("https://user:pw@h/"))
        assert not out.endswith("...")

    @pytest.mark.parametrize("arg, expected", [
        (None, "None"), (5, "5"), (1.5, "1.5"), (True, "True"), (["a", "b"], "['a', 'b']"),
        (Filesystem("pool/data"), "pool/data (filesystem)"),
    ])
    def test_a_non_string_argument_is_converted(self, arg, expected):
        out = _redact(arg)
        assert isinstance(out, str)
        assert out == str(arg)

    def test_a_non_string_with_a_credential(self):
        class Url:
            def __str__(self):
                return "https://u:p@h/x"
        assert _redact(Url()) == "https://***@h/x"

    def test_a_huge_non_string_is_clipped_too(self):
        class Big:
            def __str__(self):
                return "q" * 100_000
        assert len(_redact(Big())) == _MAX_REDACT + 3
