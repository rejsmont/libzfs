"""Local destinations (`url: zfs:<dataset>`), plan item 2.1: the config model,
the daemon's pre-start checks and the remote worker's dispatch to the local manager.

* `destination_url_problem` / `destination_problem` / `Destination.is_local` /
  `.target_dataset` and `BackupConfig.from_file` (pure; no subprocess);
* the dataset-name validator is loaded without importing the `libzfseasy`
  package (whose `__init__` resolves a `zfs` binary), so `zfsbackup-config`
  keeps working on a host without ZFS;
* `daemon._remote_destinations` / `_local_destinations`, `_client_tls_problem`,
  `_tls_inputs`, `_candidate_warnings` and `_report_client_tls`;
* `RemoteBackupWorker` handed a `zfs:` destination: it goes to
  `LocalBackupManager` and never to `RemoteBackupManager`; a mixed config sends
  to both in one cycle, each with its own status record.

Nothing here reaches a real `zfs`.
"""

import logging
import subprocess
import sys
import threading
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

from _hermetic import hermetic_env
from _sendloop_support import LoopRig, bound, seed_status
from zfsbackup import daemon as daemon_module
from zfsbackup import health
from zfsbackup.config import (
    BackupConfig,
    DatasetConfig,
    Destination,
    RemoteDatasetConfig,
    destination_problem,
    destination_url_problem,
)
from zfsbackup.config import model as model_module
from zfsbackup.health import StatusWriter, ensure_status_dir, read_worker_status
from zfsbackup.sendmemory import COPY_CONFIRMED

REPO = Path(__file__).resolve().parent.parent
FP = "ab" * 32
FP2 = "cd" * 32


# ---------------------------------------------------------------------------
# destination_url_problem
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestLocalUrlGrammar:
    @pytest.mark.parametrize("url", [
        "zfs:tank/x",
        "zfs:tank",
        "zfs:a/b:c",
        "  zfs:tank/x  ",
        "zfs:tank/a.b/c-d_e",
        "zfs:tank/x\n",             # strip() removes edge whitespace, as for https
    ])
    def test_valid_local_urls_are_accepted(self, url):
        assert destination_url_problem(url) is None

    @pytest.mark.parametrize("url", [
        "zfs:", "zfs:   ", "zfs://x", "zfs:/x", "zfs:a@s", "zfs:a#b", "zfs:a//b",
        "zfs:1pool/x", "zfs: a", "zfs:a/", "zfs:a\nb", "zfs:a\tb", "zfs:\na",
        "zfs:a\x00b", "zfs:a\x7fb", "zfs:" + "x" * 300,
    ])
    def test_invalid_local_urls_are_refused(self, url):
        problem = destination_url_problem(url)
        assert isinstance(problem, str) and problem

    @pytest.mark.parametrize("url", [
        "zfs:a\nb", "zfs:a\tb", "zfs:a\x00b", "zfs:a\x7fb", "zfs:\x1bx", "ZFS:a\nb",
        "zfs:a\rb",
    ])
    def test_no_raw_control_character_reaches_a_message(self, url):
        problem = destination_url_problem(url)
        assert problem is not None
        assert all(ord(c) >= 0x20 and ord(c) != 0x7f for c in problem), repr(problem)

    @pytest.mark.parametrize("url", ["ZFS:tank/x", "Zfs:tank/x", "zFS:tank"])
    def test_a_non_lowercase_prefix_is_refused_with_a_hint(self, url):
        problem = destination_url_problem(url)
        assert problem is not None and "lowercase" in problem and "zfs:" in problem

    def test_the_uppercase_prefix_does_not_become_a_local_destination(self):
        assert Destination(url="ZFS:tank/x").is_local is False

    def test_snapshot_and_bookmark_get_their_own_explanation(self):
        assert "snapshot" in destination_url_problem("zfs:a@s")
        assert "bookmark" in destination_url_problem("zfs:a#b")

    def test_a_dataset_with_a_colon_is_not_url_parsed(self):
        # Everything after the first "zfs:" is a dataset name; "a/b:c" would be
        # a bad URL port if it were parsed as one.
        assert destination_url_problem("zfs:a/b:c") is None
        assert Destination(url="zfs:a/b:c").target_dataset == "a/b:c"

    @pytest.mark.parametrize("url", [None, "", "   ", 5, b"zfs:a"])
    def test_a_non_string_or_empty_url_is_still_refused(self, url):
        assert destination_url_problem(url) == "url must be a non-empty string"


@pytest.mark.unit
class TestHttpsUrlsUnchanged:
    @pytest.mark.parametrize("url", [
        "https://h", "https://h:8443", "HTTPS://H", "https://h/pool", " https://h ",
    ])
    def test_https_is_still_accepted(self, url):
        assert destination_url_problem(url) is None

    @pytest.mark.parametrize("url,fragment", [
        ("http://h", "http"),
        ("https://", "no host"),
        ("https://h:0", "port"),
        ("https://h:", "port"),
        ("https://h:99999", "Port out of range"),
        ("ftp://h", "ftp"),
        ("h/pool", "no scheme"),
    ])
    def test_https_refusals_are_still_refused(self, url, fragment):
        problem = destination_url_problem(url)
        assert problem is not None and fragment in problem

    @pytest.mark.parametrize("url", ["ftp://h", "h/pool", "//h"])
    def test_the_bad_scheme_message_keeps_its_phrase_and_mentions_zfs(self, url):
        problem = destination_url_problem(url)
        assert "destinations must be https:// URLs" in problem
        assert "zfs:" in problem

    def test_a_scheme_merely_containing_zfs_is_not_local(self):
        problem = destination_url_problem("zfsx://h")
        assert problem is not None and "destinations must be https:// URLs" in problem


# ---------------------------------------------------------------------------
# Destination.is_local / target_dataset
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestDestinationKinds:
    def test_a_zfs_url_is_local_with_its_dataset(self):
        d = Destination(url="zfs:bk/backups")
        assert d.is_local is True and d.target_dataset == "bk/backups"

    def test_surrounding_whitespace_is_ignored(self):
        d = Destination(url="  zfs:bk/backups  ")
        assert d.is_local is True and d.target_dataset == "bk/backups"

    def test_an_https_url_is_remote_with_no_target_dataset(self):
        d = Destination(url="https://h:8443/pool")
        assert d.is_local is False and d.target_dataset is None

    def test_the_properties_are_not_dataclass_fields(self):
        import dataclasses

        assert [f.name for f in dataclasses.fields(Destination)] == ["url", "server_fingerprint"]
        assert "is_local" not in vars(Destination("zfs:a"))
        with pytest.raises(AttributeError):
            Destination("zfs:a").is_local = False

    def test_equality_and_positional_construction_are_unchanged(self):
        assert Destination("https://h", FP) == Destination(url="https://h", server_fingerprint=FP)
        assert Destination("zfs:a") == Destination(url="zfs:a", server_fingerprint=None)
        assert Destination("zfs:a") != Destination("zfs:b")
        assert Destination("zfs:a") != Destination("https://h")
        assert repr(Destination("zfs:a")) == "Destination(url='zfs:a', server_fingerprint=None)"

    def test_a_legacy_bad_url_still_constructs(self):
        # The URL policy is the config boundary's, not the constructor's.
        d = Destination(url="ftp://x")
        assert d.is_local is False and d.target_dataset is None

    def test_a_non_string_url_is_not_local(self):
        assert Destination(url=None).is_local is False       # type: ignore[arg-type]

    def test_the_dataset_is_not_validated_by_the_property(self):
        assert Destination(url="zfs:").target_dataset == ""


# ---------------------------------------------------------------------------
# destination_problem
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestDestinationProblem:
    def test_importable_from_the_package_and_in_all(self):
        import zfsbackup.config as config_package

        assert config_package.destination_problem is model_module.destination_problem
        assert "destination_problem" in config_package.__all__

    def test_local_with_a_fingerprint_is_refused(self):
        problem = destination_problem(Destination(url="zfs:bk/x", server_fingerprint=FP))
        assert problem is not None and "server_fingerprint" in problem
        assert "local destination" in problem

    @pytest.mark.parametrize("dest", [
        Destination(url="zfs:bk/x"),
        Destination(url="https://h"),
        Destination(url="https://h", server_fingerprint=FP),
    ])
    def test_acceptable_destinations_pass(self, dest):
        assert destination_problem(dest) is None

    def test_the_url_problem_is_reported_first(self):
        dest = Destination(url="zfs:a@s", server_fingerprint=FP)
        assert destination_problem(dest) == destination_url_problem("zfs:a@s")

    def test_an_https_url_problem_is_the_url_problem(self):
        dest = Destination(url="http://h")
        assert destination_problem(dest) == destination_url_problem("http://h")

    @pytest.mark.parametrize("thing", [None, object(), SimpleNamespace(), SimpleNamespace(url="zfs:a")])
    def test_a_non_destination_is_tolerated(self, thing):
        # No attribute error: a missing url is the URL problem; a url-only
        # object has no fingerprint and no is_local.
        result = destination_problem(thing)
        assert result is None or isinstance(result, str)
        if not hasattr(thing, "url"):
            assert result == "url must be a non-empty string"

    def test_a_url_only_namespace_with_a_valid_url_has_no_problem(self):
        assert destination_problem(SimpleNamespace(url="https://h")) is None


# ---------------------------------------------------------------------------
# BackupConfig.from_file
# ---------------------------------------------------------------------------

DATASET = "datasets:\n  - name: pool/data\n    frequency: 1h\n"


def _load(tmp_path, destinations, extra=""):
    f = tmp_path / "c.yaml"
    f.write_text(DATASET + extra + destinations)
    return BackupConfig.from_file(f)


@pytest.mark.unit
class TestFromFileLocalDestination:
    def test_a_zfs_destination_loads(self, tmp_path):
        cfg = _load(tmp_path, "destinations:\n  here:\n    url: zfs:bk/backups\n")
        d = cfg.destinations["here"]
        assert d.url == "zfs:bk/backups" and d.is_local and d.target_dataset == "bk/backups"
        assert d.server_fingerprint is None

    def test_the_url_is_stripped(self, tmp_path):
        cfg = _load(tmp_path, "destinations:\n  here:\n    url: '  zfs:bk/b  '\n")
        assert cfg.destinations["here"].url == "zfs:bk/b"

    @pytest.mark.parametrize("fp_line", ["server_fingerprint:\n", "server_fingerprint: ''\n",
                                          "server_fingerprint: '   '\n"])
    def test_a_null_or_empty_fingerprint_is_accepted(self, tmp_path, fp_line):
        cfg = _load(tmp_path, f"destinations:\n  here:\n    url: zfs:bk/b\n    {fp_line}")
        assert cfg.destinations["here"].server_fingerprint is None

    def test_a_fingerprint_on_a_local_destination_is_refused_with_the_prefix(self, tmp_path):
        with pytest.raises(ValueError, match=r"^destinations\[here\]: .*server_fingerprint") as ei:
            _load(tmp_path,
                  f"destinations:\n  here:\n    url: zfs:bk/b\n    server_fingerprint: {FP}\n")
        assert "local destination" in str(ei.value)

    @pytest.mark.parametrize("url", ["zfs:", "zfs://x", "zfs:a@s", "ZFS:bk/b", "zfs:1p/x"])
    def test_a_bad_local_url_is_refused_with_the_prefix(self, tmp_path, url):
        with pytest.raises(ValueError, match=r"^destinations\[here\]: "):
            _load(tmp_path, f"destinations:\n  here:\n    url: '{url}'\n")

    def test_a_mixed_https_and_zfs_config_loads(self, tmp_path):
        cfg = _load(
            tmp_path,
            "destinations:\n"
            "  here:\n    url: zfs:bk/b\n"
            f"  offsite:\n    url: https://h:8443\n    server_fingerprint: {FP}\n",
        )
        assert cfg.destinations["here"].is_local and not cfg.destinations["offsite"].is_local
        assert cfg.destinations["offsite"].server_fingerprint == FP

    def test_https_with_a_fingerprint_is_unchanged(self, tmp_path):
        cfg = _load(tmp_path, f"destinations:\n  offsite:\n    url: https://h\n    server_fingerprint: {FP}\n")
        assert cfg.destinations["offsite"].server_fingerprint == FP

    def test_http_is_still_refused(self, tmp_path):
        with pytest.raises(ValueError, match=r"^destinations\[offsite\]: .*plaintext http"):
            _load(tmp_path, "destinations:\n  offsite:\n    url: http://h\n")

    def test_a_bad_scheme_is_refused_and_the_message_mentions_zfs(self, tmp_path):
        with pytest.raises(ValueError, match=r"^destinations\[offsite\]: ") as ei:
            _load(tmp_path, "destinations:\n  offsite:\n    url: ftp://h\n")
        assert "destinations must be https:// URLs" in str(ei.value)
        assert "zfs:" in str(ei.value)

    def test_a_dataset_may_reference_a_local_destination(self, tmp_path):
        f = tmp_path / "c.yaml"
        f.write_text(
            "datasets:\n  - name: pool/data\n    frequency: 1h\n"
            "    remote:\n      - destination: here\n"
            "destinations:\n  here:\n    url: zfs:bk/b\n"
        )
        cfg = BackupConfig.from_file(f)
        assert cfg.datasets[0].remote[0].destination == "here"

    def test_client_cert_and_key_are_validated_as_before(self, tmp_path):
        dest = "destinations:\n  here:\n    url: zfs:bk/b\n"
        with pytest.raises(ValueError, match="client_cert_file and client_key_file must be set together"):
            _load(tmp_path, dest, extra="client_cert_file: /etc/c.crt\n")
        with pytest.raises(ValueError, match="absolute"):
            _load(tmp_path, dest, extra="client_cert_file: c.crt\nclient_key_file: c.key\n")
        cfg = _load(tmp_path, dest,
                    extra="client_cert_file: /etc/c.crt\nclient_key_file: /etc/c.key\n")
        assert cfg.client_cert_file == Path("/etc/c.crt")


# ---------------------------------------------------------------------------
# The dataset validator and the libzfseasy import
# ---------------------------------------------------------------------------


def _isolated(code):
    return subprocess.run(
        [sys.executable, "-I", "-c", "import sys\n"
         f"sys.path[:0] = [{str(REPO)!r}, {str(REPO / 'libs' / 'libzfseasy')!r}]\n" + code],
        capture_output=True, text=True, timeout=120, env=hermetic_env(),
    )


@pytest.mark.unit
class TestDatasetValidatorLoading:
    def test_importing_the_config_package_does_not_import_libzfseasy(self):
        proc = _isolated(
            "import zfsbackup.config\n"
            "assert not [m for m in sys.modules if m == 'libzfseasy' or m.startswith('libzfseasy.')], "
            "sorted(m for m in sys.modules if 'libzfseasy' in m)\n"
            "print('ok')\n"
        )
        assert proc.returncode == 0, proc.stderr
        assert proc.stdout.strip() == "ok"

    def test_validating_a_local_url_works_without_the_libzfseasy_package(self):
        proc = _isolated(
            "from zfsbackup.config import destination_url_problem, Destination, destination_problem\n"
            "assert destination_url_problem('zfs:tank/x') is None\n"
            "assert destination_url_problem('zfs:1pool/x') is not None\n"
            "assert destination_problem(Destination('zfs:tank/x')) is None\n"
            "assert not [m for m in sys.modules if m == 'libzfseasy' or m.startswith('libzfseasy.')], "
            "sorted(m for m in sys.modules if 'libzfseasy' in m)\n"
            "print('ok')\n"
        )
        assert proc.returncode == 0, proc.stderr
        assert proc.stdout.strip() == "ok"

    def test_it_also_works_when_libzfseasy_was_imported_first(self):
        proc = _isolated(
            "import libzfseasy.types\n"
            "from zfsbackup.config import model\n"
            "assert model.destination_url_problem('zfs:tank/x') is None\n"
            "assert model.destination_url_problem('zfs:a//b') is not None\n"
            "assert model._dataset_validator.__module__ == 'libzfseasy.types'\n"
            "print('ok')\n"
        )
        assert proc.returncode == 0, proc.stderr
        assert proc.stdout.strip() == "ok"

    def test_loaded_by_path_in_process_when_the_types_module_is_not_loaded(self, monkeypatch):
        monkeypatch.setattr(model_module, "_dataset_validator", None)
        monkeypatch.delitem(sys.modules, "libzfseasy.types", raising=False)
        assert destination_url_problem("zfs:tank/x") is None
        assert destination_url_problem("zfs:a//b") is not None
        assert model_module._dataset_validator.__module__ == "_zfsbackup_libzfseasy_types"

    def test_the_validator_is_cached(self, monkeypatch):
        monkeypatch.setattr(model_module, "_dataset_validator", None)
        first = model_module._get_dataset_validator()
        assert model_module._get_dataset_validator() is first
        assert model_module._dataset_validator is first


# ---------------------------------------------------------------------------
# daemon helpers
# ---------------------------------------------------------------------------


def config(destinations, *, enabled=True, remote_to=None, **kwargs):
    """A config whose one dataset sends to `remote_to` (default: every name in
    `destinations`)."""
    names = list(destinations) if remote_to is None else list(remote_to)
    ds = DatasetConfig(
        name="pool/data", enabled=enabled,
        remote=[RemoteDatasetConfig(destination=n) for n in names],
    )
    return BackupConfig(datasets=[ds], destinations=dict(destinations), **kwargs)


LOCAL = Destination(url="zfs:bk/backups")
HTTPS = Destination(url="https://h/pool", server_fingerprint=FP)
HTTPS_NEW = Destination(url="https://h/pool")


@pytest.mark.unit
class TestDestinationSplit:
    def test_remote_destinations_are_https_only(self):
        cfg = config({"b": HTTPS, "a": LOCAL, "c": HTTPS_NEW})
        assert daemon_module._remote_destinations(cfg) == ["b", "c"]

    def test_local_destinations_are_local_only(self):
        cfg = config({"b": HTTPS, "a": LOCAL, "c": Destination(url="zfs:bk/other")})
        assert daemon_module._local_destinations(cfg) == ["a", "c"]

    def test_an_undeclared_name_counts_as_remote(self):
        cfg = config({"a": LOCAL}, remote_to=["a", "ghost"])
        assert daemon_module._remote_destinations(cfg) == ["ghost"]
        assert daemon_module._local_destinations(cfg) == ["a"]

    def test_a_destination_no_dataset_sends_to_counts_for_neither(self):
        cfg = config({"a": LOCAL, "b": HTTPS}, remote_to=["b"])
        assert daemon_module._local_destinations(cfg) == []
        assert daemon_module._remote_destinations(cfg) == ["b"]

    def test_a_disabled_dataset_contributes_nothing(self):
        cfg = config({"a": LOCAL, "b": HTTPS}, enabled=False)
        assert daemon_module._remote_destinations(cfg) == []
        assert daemon_module._local_destinations(cfg) == []

    def test_names_are_deduplicated_and_sorted(self):
        cfg = config({"z": LOCAL, "m": LOCAL})
        cfg.datasets.append(DatasetConfig(
            name="pool/two", remote=[RemoteDatasetConfig(destination="m")]))
        assert daemon_module._local_destinations(cfg) == ["m", "z"]


@pytest.fixture
def load_identity(mocker):
    return mocker.patch("zfsbackup.daemon.identity.load_identity")


@pytest.mark.unit
class TestClientTlsProblemWithLocalDestinations:
    LOCAL_WORDS = "local destination(s)"

    def _problems(self, cfg, client_id, tls=None):
        return daemon_module._client_tls_problem(cfg, client_id, tls)

    def test_only_local_with_a_client_id_is_clean_and_loads_no_identity(
        self, tmp_path, load_identity
    ):
        assert self._problems(config({"here": LOCAL}), "host-1", tmp_path / "x.db.tls") == ([], None)
        load_identity.assert_not_called()
        assert list(tmp_path.iterdir()) == []

    def test_only_local_with_no_tls_dir_needs_no_identity_either(self, load_identity):
        assert self._problems(config({"here": LOCAL}), "host-1", None) == ([], None)
        load_identity.assert_not_called()

    def test_only_local_without_a_client_id_gives_exactly_the_local_warning(
        self, tmp_path, load_identity
    ):
        problems, loaded = self._problems(config({"here": LOCAL}), None, tmp_path / "x.db.tls")
        assert loaded is None and len(problems) == 1
        assert self.LOCAL_WORDS in problems[0] and "here" in problems[0]
        assert "client id" in problems[0] and "set-client-id" in problems[0]
        assert "enroll" not in problems[0]
        load_identity.assert_not_called()

    def test_every_local_name_is_listed(self):
        problems, _ = self._problems(
            config({"b": LOCAL, "a": Destination(url="zfs:bk/o")}), None)
        assert "a, b" in problems[0]

    def test_mixed_without_a_client_id_has_the_local_and_the_enroll_message(self, tmp_path):
        problems, _ = self._problems(
            config({"here": LOCAL, "off": HTTPS}), None, tmp_path / "x.db.tls")
        assert any(self.LOCAL_WORDS in p and "here" in p for p in problems)
        assert any("no client id yet" in p and "zfsbackup-config enroll" in p for p in problems)
        assert not any("off" in p and self.LOCAL_WORDS in p for p in problems)

    def test_mixed_with_a_client_id_still_checks_the_remote_identity(self, tmp_path):
        problems, loaded = self._problems(
            config({"here": LOCAL, "off": HTTPS}), "host-1", tmp_path / "x.db.tls")
        assert loaded is None
        assert not any(self.LOCAL_WORDS in p for p in problems)
        assert any("no client TLS identity" in p for p in problems)

    def test_unenrolled_check_only_names_the_https_destinations(self, tmp_path):
        problems, _ = self._problems(
            config({"here": LOCAL, "off": HTTPS_NEW}), "host-1", tmp_path / "x.db.tls")
        unenrolled = [p for p in problems if "not enrolled" in p]
        assert len(unenrolled) == 1 and "off" in unenrolled[0] and "here" not in unenrolled[0]

    def test_https_only_is_unchanged(self, tmp_path):
        problems, loaded = self._problems(config({"off": HTTPS}), None, tmp_path / "x.db.tls")
        assert loaded is None
        assert any("no client id yet" in p for p in problems)
        assert not any(self.LOCAL_WORDS in p for p in problems)

    def test_no_remote_worker_needed_is_silent_even_with_a_local_destination(self, load_identity):
        cfg = config({"here": LOCAL}, enabled=False)
        assert self._problems(cfg, None) == ([], None)
        cfg = config({"here": LOCAL}, remote_to=[])
        assert self._problems(cfg, None) == ([], None)


@pytest.mark.unit
class TestTlsInputsAndWarnings:
    def _warn(self, new, old=None, **kw):
        kw.setdefault("client_id", "a")
        kw.setdefault("previous_client_id", "a")
        return daemon_module._candidate_warnings(new, old, **kw)

    def test_adding_a_local_destination_changes_the_inputs(self):
        https = config({"off": HTTPS})
        both = config({"off": HTTPS, "here": LOCAL})
        assert daemon_module._tls_inputs(https, "a") != daemon_module._tls_inputs(both, "a")

    def test_changing_a_local_destination_changes_the_inputs(self):
        a = config({"here": LOCAL})
        b = config({"there": LOCAL})
        assert daemon_module._tls_inputs(a, "a") != daemon_module._tls_inputs(b, "a")

    def test_an_identical_config_has_equal_inputs(self):
        assert daemon_module._tls_inputs(config({"off": HTTPS, "here": LOCAL}), "a") == \
            daemon_module._tls_inputs(config({"off": HTTPS, "here": LOCAL}), "a")

    def test_a_local_url_change_alone_does_not_change_the_inputs(self):
        # The verdict depends on the NAMES in use, not on the dataset text.
        a = config({"here": LOCAL})
        b = config({"here": Destination(url="zfs:bk/else")})
        assert daemon_module._tls_inputs(a, "a") == daemon_module._tls_inputs(b, "a")

    def test_a_reload_adding_a_local_destination_is_re_evaluated(self, mocker):
        check = mocker.patch("zfsbackup.daemon._client_tls_problem", return_value=(["bad"], None))
        old = config({"off": HTTPS})
        new = config({"off": HTTPS, "here": LOCAL})
        warnings = self._warn(new, old)
        check.assert_called_once()
        assert warnings == ["remote backup: bad; local snapshots and pruning are unaffected"]

    def test_a_reload_changing_which_local_destination_is_used_is_re_evaluated(self, mocker):
        check = mocker.patch("zfsbackup.daemon._client_tls_problem", return_value=([], None))
        old = config({"here": LOCAL, "other": LOCAL}, remote_to=["here"])
        new = config({"here": LOCAL, "other": LOCAL}, remote_to=["other"])
        self._warn(new, old)
        check.assert_called_once()

    def test_an_identical_reload_is_quiet(self, mocker):
        check = mocker.patch("zfsbackup.daemon._client_tls_problem", return_value=(["bad"], None))
        same = config({"off": HTTPS, "here": LOCAL})
        assert self._warn(same, config({"off": HTTPS, "here": LOCAL})) == []
        check.assert_not_called()

    def test_the_real_check_warns_once_for_a_local_destination_without_an_id(self):
        warnings = self._warn(config({"here": LOCAL}), None, client_id=None)
        assert len(warnings) == 1
        assert warnings[0].startswith("local destination(s) here need a client id")
        assert warnings[0].endswith("; local snapshots and pruning are unaffected")

    def test_a_client_id_appearing_clears_the_local_warning_on_reload(self):
        old = config({"here": LOCAL})
        warnings = self._warn(old, old, client_id="a", previous_client_id=None)
        assert warnings == []


@pytest.mark.unit
class TestProblemLabel:
    def test_a_local_problem_has_no_remote_backup_label(self):
        assert daemon_module._problem_label("local destination(s) here need a client id") == ""

    def test_every_other_problem_keeps_the_remote_backup_label(self):
        for problem in ("no client id yet", "no client TLS identity", "destination(s) off not enrolled",
                        "a local destination(s) elsewhere"):
            assert daemon_module._problem_label(problem) == "remote backup: "

    def test_a_remote_problem_is_still_prefixed_in_the_candidate_warning(self, tmp_path):
        warnings = daemon_module._candidate_warnings(
            config({"off": HTTPS}), None, client_id=None, previous_client_id="a",
            tls=tmp_path / "x.db.tls")
        assert warnings and all(w.startswith("remote backup: ") for w in warnings)

    def test_a_remote_problem_is_still_prefixed_in_the_report(self, caplog):
        caplog.set_level(logging.DEBUG, logger="zfsbackup.daemon")
        daemon_module._report_client_tls(config({"off": HTTPS}), None, None, "under test")
        warnings = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
        assert warnings and all(
            w.startswith("configuration generation under test: remote backup: ") for w in warnings)


@pytest.mark.unit
class TestReportClientTls:
    def _run(self, caplog, cfg, client_id, tls=None):
        caplog.set_level(logging.DEBUG, logger="zfsbackup.daemon")
        daemon_module._report_client_tls(cfg, client_id, tls, "under test")
        return [(r.levelno, r.getMessage()) for r in caplog.records]

    def test_the_local_warning_is_logged_at_warning_with_the_generation(self, caplog):
        records = self._run(caplog, config({"here": LOCAL}), None)
        warnings = [m for lvl, m in records if lvl == logging.WARNING]
        assert len(warnings) == 1
        assert warnings[0].startswith("configuration generation under test: local destination(s) here")
        assert warnings[0].endswith("local snapshots and pruning are unaffected")

    def test_only_local_with_an_id_logs_no_warning(self, caplog):
        records = self._run(caplog, config({"here": LOCAL}), "host-1")
        assert not [m for lvl, m in records if lvl >= logging.WARNING]
        assert (logging.INFO, "client id: host-1") in records

    def test_mixed_without_an_id_logs_both_warnings(self, caplog, tmp_path):
        records = self._run(caplog, config({"here": LOCAL, "off": HTTPS}), None, tmp_path / "x.db.tls")
        warnings = [m for lvl, m in records if lvl == logging.WARNING]
        assert any("local destination(s) here" in m for m in warnings)
        assert any("no client id yet" in m for m in warnings)


# ---------------------------------------------------------------------------
# RemoteBackupWorker with a local destination
# ---------------------------------------------------------------------------


@pytest.fixture
def sdir(tmp_path):
    (tmp_path / "config.db").write_bytes(b"")
    directory = tmp_path / "config.db.status"
    ensure_status_dir(directory)
    return directory


class Rig:
    """The REAL `RemoteBackupWorker.run()` (scheduler and sender threads, `_sendloop_support.LoopRig`)
    over a real status directory, a fake `RemoteBackupManager` (`rbm`) and a fake `LocalBackupManager`
    (`lbm`); `order` is every `backup_dataset` call of either, in call order (run with `serial=True`
    -- one sender at a time -- when the order itself is under test)."""

    def __init__(self, monkeypatch, sdir, destinations, names=("pool/a",), uses=None, serial=False):
        self.sdir = sdir
        uses = tuple(destinations) if uses is None else tuple(uses)
        self.order = []
        self.config = BackupConfig(
            datasets=[
                DatasetConfig(name=n, remote=[RemoteDatasetConfig(destination=d) for d in uses])
                for n in names
            ],
            destinations=dict(destinations),
        )
        self.rig = LoopRig(self.config, monkeypatch, status_dir=sdir, caps=(1, 1) if serial else None)
        self.rbm, self.lbm = self.rig.remote, self.rig.local
        self.rbm.last_problem.return_value = None
        self.lbm.last_problem.return_value = None
        self.rbm.backup_dataset.side_effect = self._backup("remote")
        self.lbm.backup_dataset.side_effect = self._backup("local")
        self.manager = self.rig.manager
        self.stop = self.rig.stop

    def _backup(self, kind):
        """An acknowledged send: recorded in `order`, and the copy is confirmed (R-2)."""
        def backup(dsi, rc, **kw):
            self.order.append((kind, dsi.name, rc.destination))
            self.rig.memory.note(COPY_CONFIRMED, (dsi.name, rc.destination))
            return True
        return backup

    def run(self):
        self.rig.run()
        return self.rig.worker

    def records(self):
        st = read_worker_status(self.sdir, "remote-backup-worker")
        return {r.key: r for r in st.records} if st is not None else {}

    @staticmethod
    def _sent(manager):
        return [(c.args[0].name, c.args[1].destination) for c in manager.backup_dataset.call_args_list]

    def sent(self):
        return self._sent(self.rbm)

    def sent_local(self):
        return self._sent(self.lbm)


@pytest.mark.unit
class TestRemoteWorkerSendsToLocalDestinations:
    @pytest.fixture
    def rig(self, monkeypatch, sdir):
        return Rig(monkeypatch, sdir, {"here": LOCAL})

    def test_a_local_destination_goes_to_the_local_manager(self, rig):
        rig.run()
        assert rig.sent_local() == [("pool/a", "here")]

    def test_the_remote_manager_is_never_handed_a_local_destination(self, rig):
        rig.run()
        rig.rbm.backup_dataset.assert_not_called()
        assert [kind for kind, _ in rig.rig.built] == ["local"], "a sender is built over the local class only"

    def test_the_local_manager_gets_the_dataset_info_the_destination_and_should_stop(self, rig):
        rig.run()
        (call,) = rig.lbm.backup_dataset.call_args_list
        dsi, remote_cfg = call.args
        assert dsi.name == "pool/a" and dsi.config is rig.config.datasets[0]
        assert dsi is not rig.manager.datasets[0], "a sender has its own DatasetInfo"
        assert remote_cfg.destination == "here"
        assert callable(call.kwargs["should_stop"])

    def test_the_local_manager_is_built_with_the_client_id_and_the_workers_memory(self, rig):
        rig.run()
        ((kind, kwargs),) = rig.rig.built
        assert kind == "local" and kwargs["client_id"] == "client1" and kwargs["memory"] is rig.rig.memory

    def test_should_stop_follows_the_stop_flag(self, rig):
        seen = []

        def backup(dsi, rc, **kw):
            seen.append(kw["should_stop"]())
            rig.stop.set()
            seen.append(kw["should_stop"]())
            return True

        rig.lbm.backup_dataset.side_effect = backup
        rig.run()
        assert seen == [False, True]

    def test_no_not_implemented_warning_and_no_warned_set_remain(self, rig, caplog):
        caplog.set_level(logging.WARNING)
        worker = rig.run()
        assert not hasattr(worker, "_local_warned")
        assert "not implemented" not in caplog.text

    def test_a_completed_send_is_recorded_for_the_local_destination(self, rig):
        rig.run()
        rec = rig.records()[("send", "pool/a", "here")]
        assert rec.result == "ok" and rec.consecutive_failures == 0

    def test_a_failed_send_is_recorded_with_the_local_managers_account(self, rig):
        rig.lbm.backup_dataset.side_effect = None
        rig.lbm.backup_dataset.return_value = False
        rig.lbm.last_problem.return_value = "the target dataset bk/backups does not exist"
        rig.run()
        rec = rig.records()[("send", "pool/a", "here")]
        assert rec.result == "error"
        assert rec.last_error == "the target dataset bk/backups does not exist"
        rig.lbm.last_problem.assert_called_with("pool/a", "here")
        rig.rbm.last_problem.assert_not_called()

    def test_a_failure_without_an_account_says_so(self, rig):
        rig.lbm.backup_dataset.side_effect = None
        rig.lbm.backup_dataset.return_value = False
        rig.lbm.last_problem.return_value = None
        rig.run()
        assert rig.records()[("send", "pool/a", "here")].last_error == "not completed (see the journal)"

    def test_a_stopped_send_records_neither_success_nor_failure(self, rig):
        def stopped(dsi, rc, **kw):
            rig.stop.set()
            return False

        rig.lbm.backup_dataset.side_effect = stopped
        rig.run()
        assert ("send", "pool/a", "here") not in rig.records()

    def test_a_scheduled_dispatch_to_a_local_destination_is_forced_past_the_interval_check(self, rig):
        """The schedule decides when a pair sends; the anchor is still READ (a future-dated one is announced)."""
        rig.manager.get_own_anchor.return_value = "autosnap_20260101000000"
        young = SimpleNamespace(name="autosnap_20260101000000", age=timedelta(minutes=10))
        rig.manager.list_snapshots.return_value = [young]
        rig.run()
        assert rig.sent_local() == [("pool/a", "here")]
        assert [c.args[1] for c in rig.manager.get_own_anchor.call_args_list] == ["here"]

    def test_the_interval_check_of_the_unforced_rerun_applies_to_a_local_destination_too(self, rig):
        """`force_confirm=False` (the re-run of a slot after a clock step back) keeps the anchor-age check."""
        from zfsbackup.sendpair import PAIR_NOT_DUE, send_pair
        rig.manager.get_own_anchor.return_value = "autosnap_20260101000000"
        young = SimpleNamespace(name="autosnap_20260101000000", age=timedelta(minutes=10))
        rig.manager.list_snapshots.return_value = [young]
        rig.manager.clock_behind.return_value = False
        dsi = SimpleNamespace(name="pool/a", config=SimpleNamespace(frequency=timedelta(hours=1)))
        remote_cfg = SimpleNamespace(destination="here", frequency=None)
        outcome = send_pair(rig.manager, dsi, remote_cfg, rig.lbm, lambda: False, lambda: False)
        assert outcome.kind == PAIR_NOT_DUE
        rig.lbm.backup_dataset.assert_not_called()

    def test_an_anchor_older_than_the_frequency_makes_it_due(self, rig):
        rig.manager.get_own_anchor.return_value = "autosnap_20260101000000"
        old = SimpleNamespace(name="autosnap_20260101000000", age=timedelta(hours=2))
        rig.manager.list_snapshots.return_value = [old]
        rig.run()
        assert rig.sent_local() == [("pool/a", "here")]

    def test_its_status_key_is_retained_and_updated(self, rig):
        seed_status(rig.sdir, ("pool/a", "here", False, "x"))
        rig.run()
        rec = rig.records()[("send", "pool/a", "here")]
        assert rec.result == "ok" and rec.consecutive_failures == 0

    def test_a_stale_key_for_a_destination_no_longer_configured_is_dropped(self, rig):
        seed = StatusWriter(rig.sdir, "remote-backup-worker")
        seed.record("send", "pool/a", "gone", ok=True)
        seed.flush(force=True)
        rig.run()
        assert ("send", "pool/a", "gone") not in rig.records()

    def test_the_schedule_includes_the_local_destination(self, rig):
        rig.run()
        assert rig.rig.loop.schedule.keys() == [("pool/a", "here")]


@pytest.mark.unit
class TestRemoteWorkerMixedDestinations:
    @pytest.fixture
    def rig(self, monkeypatch, sdir):
        return Rig(monkeypatch, sdir, {"here": LOCAL, "off": HTTPS}, serial=True)

    def test_each_destination_goes_to_its_own_manager_in_the_same_run(self, rig):
        rig.run()
        assert rig.sent_local() == [("pool/a", "here")]
        assert rig.sent() == [("pool/a", "off")]

    def test_one_sender_at_a_time_they_are_sent_in_the_configured_order_local_first(self, rig):
        rig.run()
        assert rig.order == [("local", "pool/a", "here"), ("remote", "pool/a", "off")]

    def test_one_sender_at_a_time_they_are_sent_in_the_configured_order_remote_first(self, monkeypatch, sdir):
        rig = Rig(monkeypatch, sdir, {"off": HTTPS, "here": LOCAL}, uses=["off", "here"], serial=True)
        rig.run()
        assert rig.order == [("remote", "pool/a", "off"), ("local", "pool/a", "here")]

    def test_the_two_destinations_of_one_dataset_may_run_at_once(self, monkeypatch, sdir):
        """The same dataset to several destinations is unrelated work: two senders overlap."""
        rig = Rig(monkeypatch, sdir, {"here": LOCAL, "off": HTTPS})
        gate = threading.Barrier(2, timeout=bound())
        rig.lbm.backup_dataset.side_effect = lambda dsi, rc, **kw: gate.wait() >= 0
        rig.rbm.backup_dataset.side_effect = lambda dsi, rc, **kw: gate.wait() >= 0
        rig.rig.on_exit_release(gate.abort)
        rig.run()                                       # both would deadlock if they ran one after the other
        assert rig.sent_local() and rig.sent()

    def test_each_one_records_its_own_status(self, rig):
        rig.run()
        keys = {k for k in rig.records() if k[0] == health.KIND_SEND}
        assert keys == {("send", "pool/a", "off"), ("send", "pool/a", "here")}
        assert all(rig.records()[k].result == "ok" for k in keys)

    def test_a_failed_local_send_is_charged_to_it_alone_and_the_remote_one_still_runs(self, rig):
        rig.lbm.backup_dataset.side_effect = lambda dsi, rc, **kw: (
            rig.order.append(("local", dsi.name, rc.destination)) or False
        )
        rig.lbm.last_problem.return_value = "local boom"
        rig.run()
        assert rig.records()[("send", "pool/a", "here")].result == "error"
        assert rig.records()[("send", "pool/a", "here")].last_error == "local boom"
        assert rig.records()[("send", "pool/a", "off")].result == "ok"
        assert rig.sent() == [("pool/a", "off")]

    def test_a_failed_remote_send_is_charged_to_it_alone(self, rig):
        rig.rbm.backup_dataset.side_effect = None
        rig.rbm.backup_dataset.return_value = False
        rig.rbm.last_problem.return_value = "remote boom"
        rig.run()
        assert rig.records()[("send", "pool/a", "off")].last_error == "remote boom"
        assert rig.records()[("send", "pool/a", "here")].result == "ok"
        rig.lbm.last_problem.assert_not_called()

    def test_the_anchor_is_read_for_both_destinations(self, rig):
        rig.run()
        assert sorted(c.args[1] for c in rig.manager.get_own_anchor.call_args_list) == ["here", "off"]

    def test_a_missing_destination_object_goes_to_the_remote_manager(self, monkeypatch, sdir):
        # An undeclared name is `from_file`'s to refuse; the worker must not
        # treat it as local, and the remote manager says "unknown destination".
        rig = Rig(monkeypatch, sdir, {"off": HTTPS}, uses=["off", "ghost"])
        rig.run()
        assert sorted(d for _, d in rig.sent()) == ["ghost", "off"]
        rig.lbm.backup_dataset.assert_not_called()
