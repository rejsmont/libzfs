"""Phase 2 review fixes of docs/local_destination_plan.md (local destinations are `url: zfs:<dataset>`).

* ``TestDatasetValidatorLoader`` -- `model._get_dataset_validator` raises only `ImportError` when
  `libzfseasy/types.py` is missing, unreadable or broken, and does not cache the failure;
  `destination_url_problem("zfs:...")` then returns a problem string and never raises.
* ``TestMapperValidatorUnavailable`` -- `mapper._dataset_name_problem` turns that `ImportError` into a
  problem, so `validate_config` raises `ValueError` for an enabled dataset / `remote_backup` and warns
  for a disabled one.
* ``TestEndpointRefusals`` -- `remote._endpoint` and `RemoteBackupManager._enrolled_endpoint` for a
  local destination.
* ``TestBeginCycleExpiryCheck`` -- `begin_cycle` reads the client certificate only when an https
  destination is declared (used or not).

Nothing here reaches a real `zfs`, opens a socket or touches the real `libzfseasy/types.py` on disk.
"""

import importlib.util
import logging
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from _remote_support import client_tls_dir, _client_tls_template  # noqa: F401  (fixtures)
from zfsbackup import remote
from zfsbackup.config import (
    BackupConfig,
    DatasetConfig,
    Destination,
    RemoteDatasetConfig,
    RemoteServerConfig,
    destination_url_problem,
)
from zfsbackup.config import model
from zfsbackup.config.store import mapper, validate_config
from zfsbackup.remote import RemoteBackupManager

HTTPS = "https://backup.example.com"
FP = "ab" * 32


# ---------------------------------------------------------------------------
# model._get_dataset_validator
# ---------------------------------------------------------------------------


@pytest.fixture
def cold_loader(monkeypatch):
    """The validator cache empty and `libzfseasy.types` not in `sys.modules`, so the next call goes
    to the by-path loader. Both are restored afterwards."""
    monkeypatch.setattr(model, "_dataset_validator", None)
    monkeypatch.delitem(__import__("sys").modules, "libzfseasy.types", raising=False)


@pytest.fixture
def package_at(monkeypatch, cold_loader, tmp_path):
    """`package_at(directory)` makes the loader find the `libzfseasy` package in `directory`."""
    real_find_spec = importlib.util.find_spec
    where = {"dir": None}

    def find_spec(name, *args, **kwargs):
        if name == "libzfseasy" and where["dir"] is not None:
            return SimpleNamespace(submodule_search_locations=[str(where["dir"])])
        return real_find_spec(name, *args, **kwargs)

    monkeypatch.setattr(importlib.util, "find_spec", find_spec)

    def point(directory):
        where["dir"] = directory

    return point


GOOD_TYPES = (
    "class Validate:\n"
    "    @staticmethod\n"
    "    def dataset(s):\n"
    "        if not s:\n"
    "            raise ValueError(s)\n"
)


@pytest.mark.unit
class TestDatasetValidatorLoader:
    @pytest.mark.parametrize("kind", ["missing", "syntax", "raises_on_import", "no_validate"])
    def test_a_missing_or_broken_types_file_raises_only_import_error(self, package_at, tmp_path, kind):
        directory = tmp_path / "libzfseasy"
        directory.mkdir()
        if kind == "syntax":
            (directory / "types.py").write_text("def broken(:\n")
        elif kind == "raises_on_import":
            (directory / "types.py").write_text("raise RuntimeError('boom')\n")
        elif kind == "no_validate":
            (directory / "types.py").write_text("x = 1\n")
        package_at(directory)
        with pytest.raises(ImportError):
            model._get_dataset_validator()

    def test_a_missing_types_file_is_not_a_raw_file_not_found_error(self, package_at, tmp_path):
        directory = tmp_path / "libzfseasy"
        directory.mkdir()
        package_at(directory)
        with pytest.raises(ImportError) as excinfo:
            model._get_dataset_validator()
        assert not isinstance(excinfo.value, (OSError, SyntaxError))
        assert "types.py" in str(excinfo.value)

    def test_an_unimportable_package_raises_import_error(self, monkeypatch, cold_loader):
        monkeypatch.setattr(importlib.util, "find_spec", lambda name, *a, **k: None)
        with pytest.raises(ImportError):
            model._get_dataset_validator()

    def test_the_failure_is_not_cached(self, package_at, tmp_path):
        directory = tmp_path / "libzfseasy"
        directory.mkdir()
        package_at(directory)
        for _ in range(2):
            with pytest.raises(ImportError):
                model._get_dataset_validator()
        assert model._dataset_validator is None
        # The file appears: the very next call works, with no process restart.
        (directory / "types.py").write_text(GOOD_TYPES)
        validator = model._get_dataset_validator()
        assert model._dataset_validator is validator
        validator("tank/x")
        with pytest.raises(ValueError):
            validator("")

    def test_the_loaded_validator_is_cached_on_success(self, package_at, tmp_path):
        directory = tmp_path / "libzfseasy"
        directory.mkdir()
        (directory / "types.py").write_text(GOOD_TYPES)
        package_at(directory)
        first = model._get_dataset_validator()
        (directory / "types.py").unlink()
        assert model._get_dataset_validator() is first

    @pytest.mark.parametrize("kind", ["missing", "syntax"])
    def test_destination_url_problem_returns_a_string_and_never_raises(self, package_at, tmp_path, kind):
        directory = tmp_path / "libzfseasy"
        directory.mkdir()
        if kind == "syntax":
            (directory / "types.py").write_text("def broken(:\n")
        package_at(directory)
        problem = destination_url_problem("zfs:tank/x")
        assert isinstance(problem, str)
        assert "zfs:tank/x" in problem and "unavailable" in problem
        assert model._dataset_validator is None

    def test_the_cheap_checks_still_run_before_the_loader(self, package_at, tmp_path):
        directory = tmp_path / "libzfseasy"
        directory.mkdir()
        package_at(directory)
        assert "names no dataset" in destination_url_problem("zfs:")
        assert "snapshot" in destination_url_problem("zfs:tank@snap")

    def test_destination_url_problem_recovers_when_the_file_appears(self, package_at, tmp_path):
        directory = tmp_path / "libzfseasy"
        directory.mkdir()
        package_at(directory)
        assert isinstance(destination_url_problem("zfs:tank/x"), str)
        (directory / "types.py").write_text(GOOD_TYPES)
        assert destination_url_problem("zfs:tank/x") is None


# ---------------------------------------------------------------------------
# mapper._dataset_name_problem and validate_config
# ---------------------------------------------------------------------------


def config(*, enabled=True, remote_target=None, remote_enabled=True):
    return BackupConfig(
        datasets=[DatasetConfig(name="tank/data", enabled=enabled)],
        remote_backup=(
            None if remote_target is None
            else RemoteServerConfig(remote_target, enabled=remote_enabled)
        ),
    )


@pytest.fixture
def validator_unavailable(mocker):
    return mocker.patch.object(
        model, "_get_dataset_validator", side_effect=ImportError("types.py is gone")
    )


@pytest.mark.unit
class TestMapperValidatorUnavailable:
    def test_the_import_error_becomes_a_problem_string(self, validator_unavailable):
        problem = mapper._dataset_name_problem("tank/data")
        assert isinstance(problem, str)
        assert "cannot be checked" in problem and "types.py is gone" in problem

    def test_the_real_broken_loader_gives_the_same_kind_of_problem(self, package_at, tmp_path):
        directory = tmp_path / "libzfseasy"
        directory.mkdir()
        package_at(directory)
        problem = mapper._dataset_name_problem("tank/data")
        assert isinstance(problem, str) and "cannot be checked" in problem

    def test_an_invalid_name_is_still_the_grammar_message(self):
        assert "cannot be managed by zfsbackup" in mapper._dataset_name_problem("tank//x")
        assert mapper._dataset_name_problem("tank/x") is None

    def test_an_enabled_dataset_is_refused(self, validator_unavailable):
        with pytest.raises(ValueError) as excinfo:
            validate_config(config())
        assert "datasets[tank/data]" in str(excinfo.value)
        assert "cannot be checked" in str(excinfo.value)

    def test_a_disabled_dataset_warns(self, validator_unavailable):
        warnings = validate_config(config(enabled=False))
        assert any(
            "datasets[tank/data]" in w and "cannot be checked" in w and "disabled" in w
            for w in warnings
        )

    def test_an_enabled_remote_backup_target_is_refused(self, validator_unavailable):
        with pytest.raises(ValueError) as excinfo:
            validate_config(config(enabled=False, remote_target="backup/in"))
        assert "remote_backup.target_dataset" in str(excinfo.value)
        assert "cannot be checked" in str(excinfo.value)

    def test_a_disabled_remote_backup_target_warns(self, validator_unavailable):
        warnings = validate_config(
            config(enabled=False, remote_target="backup/in", remote_enabled=False)
        )
        assert any(
            "remote_backup.target_dataset" in w and "cannot be checked" in w and "disabled" in w
            for w in warnings
        )

    def test_with_the_validator_available_nothing_is_reported(self):
        assert not [w for w in validate_config(config(remote_target="backup/in"))
                    if "cannot be checked" in w]


# ---------------------------------------------------------------------------
# remote._endpoint and _enrolled_endpoint
# ---------------------------------------------------------------------------


def manager(destinations, *, tls_dir=None, client_id="client1"):
    cfg = BackupConfig(
        datasets=[DatasetConfig(
            name="pool/data",
            remote=[RemoteDatasetConfig(destination=n) for n in destinations],
        )],
        destinations=destinations,
    )
    return RemoteBackupManager(cfg, MagicMock(name="local_manager"),
                               client_id=client_id, tls_dir=tls_dir)


@pytest.mark.unit
class TestEndpointRefusals:
    @pytest.mark.parametrize("url", [
        "zfs:x/y", "zfs:tank", "  zfs:tank/x  ",
    ])
    def test_a_local_url_has_no_endpoint(self, url):
        with pytest.raises(ValueError):
            remote._endpoint(url)

    @pytest.mark.parametrize("url", [
        "http://backup.example.com", "ftp://backup.example.com", "https://", "https:///path",
        "backup.example.com", "", "   ",
    ])
    def test_any_non_https_or_hostless_url_is_refused(self, url):
        with pytest.raises(ValueError):
            remote._endpoint(url)

    def test_the_local_refusal_names_the_url(self):
        with pytest.raises(ValueError, match="zfs:x/y"):
            remote._endpoint("zfs:x/y")

    def test_an_https_url_still_parses(self):
        endpoint = remote._endpoint("HTTPS://Backup.Example.com:8443/base/")
        assert (endpoint.host, endpoint.port) == ("backup.example.com", 8443)
        assert endpoint.base_url == "https://backup.example.com:8443/base"
        assert endpoint.ws_base == "wss://backup.example.com:8443/base"

    def test_enrolled_endpoint_is_none_for_a_local_destination_without_parsing(self, mocker):
        endpoint = mocker.patch.object(remote, "_endpoint")
        problem = mocker.patch.object(remote, "destination_url_problem")
        dest = Destination(url="zfs:bk/o")
        mgr = manager({"here": dest})
        assert mgr._enrolled_endpoint("here", dest) is None
        endpoint.assert_not_called()
        problem.assert_not_called()

    def test_it_logs_one_refusal_through_log_refusal_once(self, mocker, caplog):
        dest = Destination(url="zfs:bk/o")
        mgr = manager({"here": dest})
        spy = mocker.spy(mgr, "_log_refusal_once")
        caplog.set_level(logging.DEBUG, logger="zfsbackup.remote")
        for _ in range(3):
            assert mgr._enrolled_endpoint("here", dest) is None
        assert spy.call_count == 3
        key, message = spy.call_args.args
        assert key == (None, "here")
        assert "destination here is a local destination (zfs:bk/o)" in message
        errors = [r for r in caplog.records if r.levelno == logging.ERROR]
        debugs = [r for r in caplog.records if r.levelno == logging.DEBUG]
        assert len(errors) == 1 and errors[0].getMessage() == message
        assert len(debugs) == 2

    def test_an_https_destination_is_not_refused_as_local(self, caplog):
        dest = Destination(url=HTTPS, server_fingerprint=FP)
        mgr = manager({"off": dest})
        endpoint = mgr._enrolled_endpoint("off", dest)
        assert endpoint is not None and endpoint.host == "backup.example.com"
        assert not [r for r in caplog.records if r.levelno >= logging.ERROR]


# ---------------------------------------------------------------------------
# begin_cycle and the client certificate expiry check
# ---------------------------------------------------------------------------


@pytest.fixture
def expiry(mocker):
    """`identity.client_cert_expiry_problem` as `zfsbackup.remote` calls it: never a problem."""
    return mocker.patch.object(remote.ident, "client_cert_expiry_problem", return_value=None)


@pytest.mark.unit
class TestBeginCycleExpiryCheck:
    def test_only_local_destinations_never_look_at_the_certificate(self, expiry, client_tls_dir):
        mgr = manager({"here": Destination(url="zfs:bk/o")}, tls_dir=client_tls_dir)
        mgr.begin_cycle()
        expiry.assert_not_called()
        assert mgr._expiry_checked_at is None

    def test_several_local_destinations_the_same(self, expiry, client_tls_dir):
        mgr = manager({"a": Destination(url="zfs:bk/a"), "b": Destination(url="zfs:bk/b")},
                      tls_dir=client_tls_dir)
        mgr.begin_cycle()
        expiry.assert_not_called()

    def test_no_declared_destination_never_looks_at_the_certificate(self, expiry, client_tls_dir):
        mgr = manager({}, tls_dir=client_tls_dir)
        mgr.begin_cycle()
        expiry.assert_not_called()
        assert mgr._expiry_checked_at is None

    def test_an_https_destination_checks_the_certificate(self, expiry, client_tls_dir):
        mgr = manager({"off": Destination(url=HTTPS, server_fingerprint=FP)}, tls_dir=client_tls_dir)
        mgr.begin_cycle()
        expiry.assert_called_once()
        assert mgr._expiry_checked_at is not None

    def test_a_mixed_configuration_checks(self, expiry, client_tls_dir):
        mgr = manager({"here": Destination(url="zfs:bk/o"),
                       "off": Destination(url=HTTPS, server_fingerprint=FP)},
                      tls_dir=client_tls_dir)
        mgr.begin_cycle()
        expiry.assert_called_once()

    def test_a_declared_but_unused_https_destination_still_counts(self, expiry, client_tls_dir):
        destinations = {"here": Destination(url="zfs:bk/o"),
                        "off": Destination(url=HTTPS, server_fingerprint=FP)}
        cfg = BackupConfig(
            datasets=[DatasetConfig(name="pool/data",
                                    remote=[RemoteDatasetConfig(destination="here")])],
            destinations=destinations,
        )
        mgr = RemoteBackupManager(cfg, MagicMock(), client_id="client1", tls_dir=client_tls_dir)
        mgr.begin_cycle()
        expiry.assert_called_once()
