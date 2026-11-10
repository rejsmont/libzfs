"""`zfsbackup-config` support for local destinations (`url: zfs:<dataset>`;
docs/local_destination_plan.md item 2.3).

A local destination is a dataset on THIS host that receives the backups (no
server, no TLS, no enrollment). Its copies live at `<target>/<client id>/<dataset>`,
so this host's client id matters to it. Covered, each against a real database
built by `import` (the real schema, foreign keys and compare-and-swap):

- ``TestListLocal``            `list destinations` text (KIND, `-` for ENROLLED) and JSON
                               (`kind`, `target_dataset`).
- ``TestSetUrlKind``           `set destinations.N.url zfs:...`: the kind switch, the
                               fingerprint refusal and its way out, the warnings (target
                               change, https <-> zfs:, no-op), invalid values refused with the
                               model's message, `server_fingerprint` on a local destination.
- ``TestEditLocal``            `edit` (whole config and `edit destination`) with `zfs:`.
- ``TestEnrollRefusesLocal``   `enroll` of a local destination: refused before the token is read,
                               before any file or network work; a mixed config still enrolls.
- ``TestRotateLocal``          `identity rotate` with only local destinations / a mix.
- ``TestCheckLocal``           the client-certificate entry of `check`.
- ``TestRenameDatasetLocal``   `rename dataset`: the copy paths and the exact `zfs rename`.
- ``TestRenameDestinationLocal`` `rename destination` of a local destination.
- ``TestSetClientIdLocal``     `identity set-client-id`: the move hint per target.
- ``TestCommitConfigClientId`` `write.commit_config`: the client id generated under one writer
                               lock hold, generation +2, one reload request, conflicts, failure
                               after the id, a caller-held lock, an existing id kept.
- ``TestImportClientId``       `import` of a YAML with a local destination.
- ``TestEditClientId``         `edit` adding the first local destination.

Nothing here reaches a real `zfs` (the `fake_zfs` fixture is autouse).
"""

import json
import re

import pytest

from _cli_support import (
    ANCHOR,
    NEVER_PRUNE_TEXT,
    RICH_YAML,
    buffers_in,
    count_containing,
    db,
    db_state,
    fake_zfs,
    generation,
    host_client_id,
    isolated,
    make_db,
    run,
    scratch_tmp,
    seed_client_id,
    stderr_lines,
    store_key,
    stored,
    stub,
    warnings_in,
    write_yaml,
)
from _enroll_support import (
    client_with,
    do_enroll,
    make_server,
    reloads,
    tls_of,
    token_file,
    zfs_stub,
)
from zfsbackup import daemonlock, identity
from zfsbackup.cli import check as check_view
from zfsbackup.cli import editor
from zfsbackup.cli import main as cli_main
from zfsbackup.cli import write as cli_write
from zfsbackup.config import Destination
from zfsbackup.config.model import destination_url_problem, client_id_problem
from zfsbackup import health
from zfsbackup.config.store import (
    ConfigBusy,
    ConfigConflict,
    dispose_all,
    read_config,
    resolve_config_path,
    write_config,
    writer_lock,
)

# No module-level tier marker: conftest marks every test that declares none as
# `unit`; the ones that run a TLS server are `integration` individually.

CID = "host-1"
FP = "ab" * 32

#: Two local destinations (`loc` -> backup/local, `loc.two` -> other/target) and two https
#: ones (`offsite`, an unused `spare`). `tank/a` sends to a local and a remote destination,
#: `tank/b` to the remote one only, `tank/c` to the local one only, `tank/two` to both locals.
LOCAL_YAML = """\
snapshot_prefix: "autosnap"
check_interval: "5m"
datasets:
  - name: "tank/a"
    frequency: "1h"
    retention:
      "1d": "30d"
    remote:
      - destination: "loc"
      - destination: "offsite"
  - name: "tank/b"
    frequency: "1h"
    retention:
      "1d": "30d"
    remote:
      - destination: "offsite"
  - name: "tank/c"
    frequency: "1h"
    retention:
      "1d": "30d"
    remote:
      - destination: "loc"
  - name: "tank/two"
    frequency: "1h"
    retention:
      "1d": "30d"
    remote:
      - destination: "loc"
      - destination: "loc.two"
  - name: "tank/none"
    frequency: "1h"
    retention:
      "1d": "30d"
destinations:
  loc:
    url: "zfs:backup/local"
  loc.two:
    url: "zfs:other/target"
  offsite:
    url: "https://offsite:8080"
  spare:
    url: "https://spare:8080"
"""

#: The same, with the `offsite` server pinned (enrolled).
PINNED_YAML = LOCAL_YAML.replace(
    '    url: "https://offsite:8080"\n',
    f'    url: "https://offsite:8080"\n    server_fingerprint: "{FP}"\n',
)

#: One local destination, nothing remote, and a never-prune dataset (a standing warning).
ONE_LOCAL_YAML = """\
datasets:
  - name: "tank/forever"
    frequency: "1h"
    retention: forever
    remote:
      - destination: "loc"
destinations:
  loc:
    url: "zfs:backup/local"
"""


@pytest.fixture(autouse=True)
def _never_a_real_zfs(fake_zfs):
    """Nothing in this module may see a real `zfs`: `ZFS_CMD` is always the fake."""
    yield fake_zfs


@pytest.fixture
def mixed(run, db):
    """Local + https destinations, a client id already stored (`CID`)."""
    make_db(run, db, LOCAL_YAML)
    seed_client_id(db, CID)
    return db


@pytest.fixture
def pinned(run, db):
    make_db(run, db, PINNED_YAML)
    seed_client_id(db, CID)
    return db


@pytest.fixture
def rich(run, db):
    """Remote destinations only (`offsite`, `dc.two`, `spare`), no client id."""
    return make_db(run, db, RICH_YAML)


def drop_client_id(database):
    """Remove the `host_identity` row, as on a store that has never had an id
    (a direct delete through the store's engine factory; no generation bump)."""
    from zfsbackup.config.store import db as store_db
    from zfsbackup.config.store import models

    url = store_db.url_for_path(database)
    try:
        with store_db.session_scope(url) as session:
            session.query(models.HostIdentity).delete()
    finally:
        dispose_all()
    assert host_client_id(database) is None


def ok(run, database, *args):
    result = run(*args, "-c", database)
    assert result.exit_code == 0, result.stderr
    return result


def table(result):
    return [re.split(r"\s{2,}", line.strip()) for line in result.stdout.splitlines()]


def snapshot_of(database):
    resolved = resolve_config_path(str(database))
    try:
        return resolved, read_config(resolved)
    finally:
        dispose_all()


GENERATED = "generated this host's client id: "


def generated_id(result):
    """The id announced on stdout, or None."""
    found = [l for l in result.stdout.splitlines() if l.startswith(GENERATED)]
    assert len(found) <= 1, result.stdout
    return found[0][len(GENERATED):] if found else None


# --------------------------------------------------------------------------
# list destinations
# --------------------------------------------------------------------------


class TestListLocal:
    def test_text_has_a_kind_column_and_a_dash_for_a_local_enrolled_cell(self, run, pinned):
        result = run("list", "destinations", "-c", pinned)
        assert result.exit_code == 0, result.stderr
        assert result.stdout.splitlines()[0].split() == ["NAME", "KIND", "URL", "ENROLLED", "DATASETS"]
        assert table(result)[1:] == [
            ["loc", "local", "zfs:backup/local", "-", "tank/a, tank/c, tank/two"],
            ["loc.two", "local", "zfs:other/target", "-", "tank/two"],
            ["offsite", "remote", "https://offsite:8080", FP[:16], "tank/a, tank/b"],
            ["spare", "remote", "https://spare:8080", "not enrolled", "-"],
        ]

    def test_a_local_destination_is_never_called_not_enrolled(self, run, mixed):
        rows = table(run("list", "destinations", "-c", mixed))[1:]
        assert {r[0]: r[3] for r in rows} == {
            "loc": "-", "loc.two": "-", "offsite": "not enrolled", "spare": "not enrolled",
        }

    def test_json_has_kind_and_target_dataset_and_keeps_the_old_keys(self, run, pinned):
        result = run("list", "destinations", "--json", "-c", pinned)
        assert result.exit_code == 0, result.stderr
        data = json.loads(result.stdout)
        assert data == [
            {"name": "loc", "kind": "local", "url": "zfs:backup/local",
             "target_dataset": "backup/local", "server_fingerprint": None,
             "datasets": ["tank/a", "tank/c", "tank/two"]},
            {"name": "loc.two", "kind": "local", "url": "zfs:other/target",
             "target_dataset": "other/target", "server_fingerprint": None,
             "datasets": ["tank/two"]},
            {"name": "offsite", "kind": "remote", "url": "https://offsite:8080",
             "target_dataset": None, "server_fingerprint": FP, "datasets": ["tank/a", "tank/b"]},
            {"name": "spare", "kind": "remote", "url": "https://spare:8080",
             "target_dataset": None, "server_fingerprint": None, "datasets": []},
        ]
        assert result.stderr == ""

    def test_a_remote_entry_has_a_null_target_dataset(self, run, rich):
        data = json.loads(run("list", "destinations", "--json", "-c", rich).stdout)
        assert data and all(d["kind"] == "remote" and d["target_dataset"] is None for d in data)

    def test_enabled_only_hides_a_local_destination_nothing_enabled_sends_to(self, run, db):
        make_db(run, db, LOCAL_YAML.replace(
            '  - name: "tank/c"\n', '  - name: "tank/c"\n    enabled: false\n'
        ).replace(
            '  - name: "tank/two"\n', '  - name: "tank/two"\n    enabled: false\n'
        ))
        rows = table(run("list", "destinations", "--enabled-only", "-c", db))[1:]
        assert [(r[0], r[1], r[4]) for r in rows] == [
            ("loc", "local", "tank/a"), ("offsite", "remote", "tank/a, tank/b"),
        ]


# --------------------------------------------------------------------------
# set destinations.N.url
# --------------------------------------------------------------------------


class TestSetUrlKind:
    def test_an_unenrolled_https_destination_becomes_local_with_the_full_send_warning(
        self, run, mixed
    ):
        gen = generation(mixed)
        result = ok(run, mixed, "set", "destinations.spare.url", "zfs:third/target")
        assert result.stdout.splitlines()[0] == (
            "destinations[spare].url: https://spare:8080 -> zfs:third/target"
        )
        (warned,) = warnings_in(result)
        assert warned.startswith("warning: destinations[spare] changes from the server https://spare:8080 "
                                 "to the local target 'third/target'")
        assert "the next send to it is a full send into third/target/host-1/" in warned
        assert "the copies already on the server are no longer updated" in warned
        spare = stored(mixed).destinations["spare"]
        assert (spare.url, spare.is_local, spare.target_dataset, spare.server_fingerprint) == (
            "zfs:third/target", True, "third/target", None)
        assert generation(mixed) == gen + 1                    # a stored id: no second bump
        assert host_client_id(mixed) == CID and generated_id(result) is None

    def test_without_a_client_id_the_warning_names_the_placeholder_and_an_id_is_generated(
        self, run, rich
    ):
        gen = generation(rich)
        result = ok(run, rich, "set", "destinations.spare.url", "zfs:backup/local")
        (warned,) = warnings_in(result)
        assert "full send into backup/local/<client id>/" in warned
        new_id = generated_id(result)
        assert new_id and client_id_problem(new_id) is None
        assert host_client_id(rich) == new_id
        assert generation(rich) == gen + 2                     # the id, then the configuration
        assert stored(rich).destinations["spare"].url == "zfs:backup/local"

    def test_a_fingerprint_blocks_the_switch_and_names_the_unset_command(self, run, pinned):
        before, gen = db_state(pinned), generation(pinned)
        result = run("set", "destinations.offsite.url", "zfs:third/target", "-c", pinned)
        assert result.exit_code == 1
        assert "destinations[offsite] is enrolled with a server" in result.stderr
        assert "Nothing was changed." in result.stderr
        assert "`zfsbackup-config unset destinations[offsite].server_fingerprint`" in result.stderr
        assert db_state(pinned) == before and generation(pinned) == gen
        assert stored(pinned).destinations["offsite"].url == "https://offsite:8080"

    def test_after_unsetting_the_fingerprint_the_switch_succeeds(self, run, pinned):
        assert run("set", "destinations.offsite.url", "zfs:third/target", "-c", pinned).exit_code == 1
        unset = ok(run, pinned, "unset", "destinations.offsite.server_fingerprint")
        assert any("server_fingerprint removed" in w for w in warnings_in(unset))
        result = ok(run, pinned, "set", "destinations.offsite.url", "zfs:third/target")
        assert any("changes from the server" in w for w in warnings_in(result))
        offsite = stored(pinned).destinations["offsite"]
        assert (offsite.url, offsite.server_fingerprint) == ("zfs:third/target", None)

    def test_a_fingerprint_on_a_dotted_destination_names_the_bracketed_path(self, run, db):
        make_db(run, db, RICH_YAML.replace(
            '    url: "https://dc2:8080"\n', f'    url: "https://dc2:8080"\n    server_fingerprint: "{FP}"\n'))
        result = run("set", "destinations[dc.two].url", "zfs:third/target", "-c", db)
        assert result.exit_code == 1
        assert "`zfsbackup-config unset destinations[dc.two].server_fingerprint`" in result.stderr

    def test_changing_a_local_target_warns_with_the_old_copy_path(self, run, mixed):
        result = ok(run, mixed, "set", "destinations.loc.url", "zfs:backup/new")
        (warned,) = warnings_in(result)
        assert "destinations[loc] changes its local target from 'backup/local' to 'backup/new'" in warned
        assert "the next send to it is a full send" in warned
        assert "existing copies under backup/local/host-1/ are orphaned" in warned
        assert "zfs rename" in warned
        assert stored(mixed).destinations["loc"].url == "zfs:backup/new"

    def test_changing_a_local_target_without_an_id_uses_the_placeholder(self, run, db):
        make_db(run, db, ONE_LOCAL_YAML)
        drop_client_id(db)
        result = ok(run, db, "set", "destinations.loc.url", "zfs:backup/new")
        (warned,) = [w for w in warnings_in(result) if "changes its local target" in w]
        assert "existing copies under backup/local/<client id>/ are orphaned" in warned

    def test_local_to_https_warns_about_orphaned_copies(self, run, mixed):
        result = ok(run, mixed, "set", "destinations.loc.url", "https://elsewhere:9")
        (warned,) = warnings_in(result)
        assert ("destinations[loc] changes from the local target 'backup/local' to the server "
                "https://elsewhere:9") in warned
        assert "existing copies under backup/local/host-1/ are orphaned" in warned
        loc = stored(mixed).destinations["loc"]
        assert (loc.url, loc.is_local, loc.server_fingerprint) == ("https://elsewhere:9", False, None)

    def test_setting_the_same_local_url_is_a_noop_without_a_warning(self, run, mixed):
        before, gen = db_state(mixed), generation(mixed)
        result = ok(run, mixed, "set", "destinations.loc.url", "zfs:backup/local")
        assert result.stdout == "destinations[loc].url is already zfs:backup/local; nothing changed\n"
        assert warnings_in(result) == []
        assert db_state(mixed) == before and generation(mixed) == gen

    def test_an_https_to_https_change_has_no_local_warning(self, run, mixed):
        result = ok(run, mixed, "set", "destinations.spare.url", "https://other:1")
        assert not any("local target" in w for w in warnings_in(result))

    @pytest.mark.parametrize(
        "value",
        [
            '"zfs:"',                       # empty dataset (bare `zfs:` is a YAML mapping)
            "zfs:/backup/local",            # leading slash
            "zfs://backup/local",
            "zfs:backup@snap",              # a snapshot
            "zfs:backup#bookmark",          # a bookmark
            "ZFS:backup/local",             # the prefix is exact lowercase
            "Zfs:backup/local",
            "zfs:backup//local",            # a doubled slash
            "zfs:backup/local/",
        ],
    )
    def test_an_invalid_local_url_is_refused_with_the_models_message(self, run, rich, value):
        text = value.strip('"')
        problem = destination_url_problem(text)
        assert problem, "the model must refuse this"
        before, gen = db_state(rich), generation(rich)
        result = run("set", "destinations.spare.url", value, "-c", rich)
        assert result.exit_code == 1
        assert f"destinations[spare]: {problem}" in result.stderr
        assert "Traceback" not in result.stderr
        assert db_state(rich) == before and generation(rich) == gen
        assert host_client_id(rich) is None                    # no id for a refused value

    def test_get_of_a_local_destinations_fingerprint_says_not_applicable(self, run, mixed):
        for path in ("destinations.loc.server_fingerprint", "destinations[loc.two].server_fingerprint"):
            assert run("get", path, "-c", mixed).stdout == "not applicable (local destination)\n"
        assert run("get", "destinations.spare.server_fingerprint", "-c", mixed).stdout == "not enrolled\n"

    def test_the_destination_block_of_a_local_destination_is_only_its_url(self, run, mixed):
        import yaml

        out = yaml.safe_load(run("get", "destinations.loc", "-c", mixed).stdout)
        assert out == {"url": "zfs:backup/local"}

    def test_setting_a_fingerprint_on_a_local_destination_is_refused(self, run, mixed):
        before, gen = db_state(mixed), generation(mixed)
        result = run("set", "destinations.loc.server_fingerprint", FP, "-c", mixed)
        assert result.exit_code == 1
        assert "destination 'loc' is local (zfs:backup/local)" in result.stderr
        assert "no server_fingerprint" in result.stderr and "Nothing was changed." in result.stderr
        assert db_state(mixed) == before and generation(mixed) == gen
        assert stored(mixed).destinations["loc"].server_fingerprint is None

    def test_unsetting_the_fingerprint_of_a_local_destination_is_a_noop(self, run, mixed):
        before = db_state(mixed)
        result = ok(run, mixed, "unset", "destinations.loc.server_fingerprint")
        assert "nothing changed" in result.stdout and warnings_in(result) == []
        assert db_state(mixed) == before


# --------------------------------------------------------------------------
# edit
# --------------------------------------------------------------------------


def edit(run, database, stub, *args, input=None, **env):
    return run("edit", *args, "-c", database, env=stub.env(database, **env), input=input)


def replace(old, new):
    return {"replace": [old, new]}


@pytest.fixture
def tty(monkeypatch):
    monkeypatch.setattr(editor, "_stdin_is_tty", lambda: True)


class TestEditLocal:
    def test_the_whole_config_buffer_accepts_a_zfs_url_and_warns(self, run, mixed, stub, scratch_tmp):
        stub.plan([replace('url: "https://spare:8080"', 'url: "zfs:third/target"')])
        result = edit(run, mixed, stub, "--no-prompt")
        assert result.exit_code == 0, result.stderr
        (warned,) = [w for w in warnings_in(result) if "changes from the server" in w]
        assert "to the local target 'third/target'" in warned and "third/target/host-1/" in warned
        assert stored(mixed).destinations["spare"].url == "zfs:third/target"
        assert buffers_in(scratch_tmp) == []

    def test_the_whole_config_buffer_warns_about_a_changed_local_target(self, run, mixed, stub, scratch_tmp):
        stub.plan([replace('url: "zfs:backup/local"', 'url: "zfs:backup/new"')])
        result = edit(run, mixed, stub, "--no-prompt")
        assert result.exit_code == 0, result.stderr
        (warned,) = [w for w in warnings_in(result) if "changes its local target" in w]
        assert "'backup/local' to 'backup/new'" in warned and "backup/local/host-1/ are orphaned" in warned

    def test_an_untouched_buffer_with_local_destinations_is_a_noop(self, run, mixed, stub, scratch_tmp):
        stub.plan([])
        gen, state = generation(mixed), db_state(mixed)
        result = edit(run, mixed, stub, "--no-prompt")
        assert result.exit_code == 0, result.stderr
        assert warnings_in(result) == []
        assert generation(mixed) == gen and db_state(mixed) == state

    def test_the_destination_buffer_accepts_a_zfs_url_and_warns(self, run, mixed, stub, scratch_tmp):
        stub.plan([replace("https://spare:8080", "zfs:third/target")])
        result = edit(run, mixed, stub, "destination", "spare", "--no-prompt")
        assert result.exit_code == 0, result.stderr
        assert any("changes from the server https://spare:8080 to the local target" in w
                   for w in warnings_in(result))
        assert stored(mixed).destinations["spare"].url == "zfs:third/target"

    def test_a_local_destination_buffer_is_its_url_and_explains_zfs(self, run, mixed, stub, scratch_tmp):
        stub.plan([])
        edit(run, mixed, stub, "destination", "loc", "--no-prompt")
        body = [l for l in stub.log[0]["before"].splitlines() if l.strip() and not l.startswith("#")]
        assert body == ['url: "zfs:backup/local"']
        assert "zfs:<dataset>" in stub.log[0]["before"]

    def test_a_local_destination_can_be_switched_to_https_in_its_buffer(self, run, mixed, stub, scratch_tmp):
        stub.plan([replace("zfs:backup/local", "https://elsewhere:9")])
        result = edit(run, mixed, stub, "destination", "loc", "--no-prompt")
        assert result.exit_code == 0, result.stderr
        assert any("changes from the local target 'backup/local' to the server" in w
                   for w in warnings_in(result))
        assert stored(mixed).destinations["loc"].url == "https://elsewhere:9"

    def test_a_fingerprint_in_a_zfs_buffer_is_rejected_and_nothing_is_written(
        self, run, mixed, stub, scratch_tmp
    ):
        state = db_state(mixed)
        stub.plan([{"append": f'server_fingerprint: "{FP}"\n'}])
        result = edit(run, mixed, stub, "destination", "loc", "--no-prompt")
        assert result.exit_code == 1
        assert "local destination, which has no server and no server_fingerprint" in result.stderr
        assert db_state(mixed) == state
        assert len(buffers_in(scratch_tmp)) == 1               # kept for the operator

    def test_a_fingerprint_beside_a_zfs_url_in_the_whole_buffer_is_rejected(
        self, run, mixed, stub, scratch_tmp
    ):
        state = db_state(mixed)
        stub.plan([replace('url: "zfs:backup/local"', f'url: "zfs:backup/local"\n    server_fingerprint: "{FP}"')])
        result = edit(run, mixed, stub, "--no-prompt")
        assert result.exit_code == 1
        assert "destinations[loc]" in result.stderr and "no server_fingerprint" in result.stderr
        assert db_state(mixed) == state

    def test_switching_a_pinned_destination_needs_the_pin_removed_in_the_same_buffer(
        self, run, pinned, stub, scratch_tmp
    ):
        state = db_state(pinned)
        stub.plan([replace("https://offsite:8080", "zfs:third/target")])
        assert edit(run, pinned, stub, "destination", "offsite", "--no-prompt").exit_code == 1
        assert db_state(pinned) == state
        stub.plan([replace("https://offsite:8080", "zfs:third/target"),
                   replace(f'server_fingerprint: "{FP}"\n', "")])
        result = edit(run, pinned, stub, "destination", "offsite", "--no-prompt")
        assert result.exit_code == 0, result.stderr
        warned = warnings_in(result)
        assert any("server_fingerprint removed" in w for w in warned)
        assert any("changes from the server https://offsite:8080 to the local target" in w for w in warned)
        offsite = stored(pinned).destinations["offsite"]
        assert (offsite.url, offsite.server_fingerprint) == ("zfs:third/target", None)

    def test_an_invalid_zfs_url_in_a_buffer_is_refused_with_the_models_message(
        self, run, mixed, stub, scratch_tmp
    ):
        state = db_state(mixed)
        stub.plan([replace("https://spare:8080", "zfs:backup@snap")])
        result = edit(run, mixed, stub, "destination", "spare", "--no-prompt")
        assert result.exit_code == 1
        assert destination_url_problem("zfs:backup@snap") in result.stderr
        assert db_state(mixed) == state

    def test_invalid_then_fixed_on_retry_saves(self, run, mixed, stub, scratch_tmp, tty):
        stub.plan(
            [replace("https://spare:8080", "zfs:/bad")],
            [replace("zfs:/bad", "zfs:third/target")],
        )
        result = edit(run, mixed, stub, "destination", "spare", input="r\n")
        assert result.exit_code == 0, result.stderr
        assert stub.runs == 2
        assert stored(mixed).destinations["spare"].url == "zfs:third/target"

    def test_invalid_then_abort_writes_nothing(self, run, mixed, stub, scratch_tmp, tty):
        state = db_state(mixed)
        stub.plan([replace("https://spare:8080", "zfs:/bad")])
        result = edit(run, mixed, stub, "destination", "spare", input="a\n")
        assert result.exit_code == 1
        assert db_state(mixed) == state

    def test_a_dotted_local_destination_name_edits_by_name(self, run, mixed, stub, scratch_tmp):
        stub.plan([replace("zfs:other/target", "zfs:other/moved")])
        result = edit(run, mixed, stub, "destination", "loc.two", "--no-prompt")
        assert result.exit_code == 0, result.stderr
        assert stored(mixed).destinations["loc.two"].url == "zfs:other/moved"
        assert stored(mixed).destinations["loc"].url == "zfs:backup/local"


# --------------------------------------------------------------------------
# enroll
# --------------------------------------------------------------------------

LOCAL_REFUSAL = "local destinations need no enrollment"


class TestEnrollRefusesLocal:
    @pytest.fixture
    def local_db(self, run, db, reloads):
        return client_with(run, db, reloads, {"loc": ("zfs:backup/local", None)})

    def untouched(self, database, before, reloads):
        assert db_state(database) == before
        assert list(reloads) == []
        assert not tls_of(database).exists(), "no TLS file may be created for a local destination"

    @pytest.mark.parametrize(
        "token_args",
        [("--token-file", "NOSUCHFILE"), ("--token", "-")],
        ids=["token-file", "stdin"],
    )
    def test_a_stored_local_destination_is_refused_before_the_token_is_read(
        self, run, local_db, reloads, tmp_path, token_args
    ):
        before = db_state(local_db)
        args = [str(tmp_path / "missing-token") if a == "NOSUCHFILE" else a for a in token_args]
        result = run("enroll", "loc", *args, "-c", local_db, input="")
        assert result.exit_code == 1, result.stderr
        assert "destination 'loc' is local ('zfs:backup/local'); " + LOCAL_REFUSAL in result.stderr
        assert "token" not in result.stderr.replace("local destinations need no enrollment", "")
        self.untouched(local_db, before, reloads)

    def test_a_new_destination_with_a_zfs_url_is_refused_and_not_created(self, run, local_db, reloads, tmp_path):
        before = db_state(local_db)
        path = token_file(tmp_path / "tokens", "zfsb1." + "a" * 64 + "." + "b" * 43)
        result = run("enroll", "fresh", "--url", "zfs:third/target", "--token-file", path, "-c", local_db)
        assert result.exit_code == 1
        assert "destination 'fresh' is local ('zfs:third/target'); " + LOCAL_REFUSAL in result.stderr
        assert "fresh" not in stored(local_db).destinations
        self.untouched(local_db, before, reloads)

    @pytest.mark.integration
    def test_an_https_destination_with_a_zfs_url_is_refused_and_no_server_is_contacted(
        self, run, db, reloads, make_server, tmp_path
    ):
        srv = make_server("a")
        client_with(run, db, reloads, {"offsite": (srv.url, None)})
        before = db_state(db)
        path = token_file(tmp_path / "tokens", srv.token())
        result = run("enroll", "offsite", "--url", "zfs:third/target", "--token-file", path, "-c", db)
        assert result.exit_code == 1
        assert "destination 'offsite' is local ('zfs:third/target'); " + LOCAL_REFUSAL in result.stderr
        assert srv.log == [], "no request may reach the server"
        assert stored(db).destinations["offsite"].url == srv.url
        self.untouched(db, before, reloads)

    @pytest.mark.parametrize("url", ["zfs:third/target", "ZFS:x"])
    def test_the_url_is_judged_by_its_prefix_alone(self, run, local_db, reloads, tmp_path, url):
        # `ZFS:x` is not a local url (the model refuses it), so enroll says what it always said
        # about a bad url rather than "local".
        before = db_state(local_db)
        result = run("enroll", "fresh", "--url", url, "--token-file", tmp_path / "none", "-c", local_db)
        assert result.exit_code == 1
        assert (LOCAL_REFUSAL in result.stderr) == url.startswith("zfs:")
        self.untouched(local_db, before, reloads)

    def test_a_missing_database_keeps_its_usual_error(self, run, db, tmp_path):
        token, _ = identity.make_token(FP)
        path = token_file(tmp_path / "tokens", token)
        result = run("enroll", "loc", "--token-file", path, "-c", db)
        assert result.exit_code == 78
        assert LOCAL_REFUSAL not in result.stderr
        assert not db.parent.exists()

    def test_the_early_probe_does_not_repeat_a_standing_warning(self, run, db, reloads, tmp_path):
        """`enroll` reads the configuration twice (a probe for the local refusal, then its own
        read); the stored configuration's warnings are shown once."""
        make_db(run, db, ONE_LOCAL_YAML.replace("zfs:backup/local", "https://127.0.0.1:1"))
        seed_client_id(db, CID)
        token, _ = identity.make_token(FP)
        path = token_file(tmp_path / "tokens", token)
        result = run("enroll", "loc", "--token-file", path, "-c", db)
        assert result.exit_code != 0                           # nothing listens on port 1
        assert count_containing(stderr_lines(result), NEVER_PRUNE_TEXT) == 1

    def test_a_malformed_token_still_prints_no_warning_about_the_stored_configuration(
        self, run, db, reloads, tmp_path
    ):
        """The probe's read is muted: a command that fails on its token before its own read
        has never shown the stored configuration's warnings, and still does not."""
        make_db(run, db, ONE_LOCAL_YAML.replace("zfs:backup/local", "https://127.0.0.1:1"))
        seed_client_id(db, CID)
        path = token_file(tmp_path / "tokens", "not-a-token")
        result = run("enroll", "loc", "--token-file", path, "-c", db)
        assert result.exit_code == 1
        assert count_containing(stderr_lines(result), NEVER_PRUNE_TEXT) == 0

    @pytest.mark.integration
    def test_a_mixed_config_still_enrolls_its_https_destination(self, run, db, reloads, make_server):
        srv = make_server("a")
        client_with(run, db, reloads, {"loc": ("zfs:backup/local", None), "offsite": (srv.url, None)})
        result = do_enroll(run, db, srv, srv.token(), url=False)
        assert result.exit_code == 0, result.stderr
        assert stored(db).destinations["offsite"].server_fingerprint == srv.fp
        loc = stored(db).destinations["loc"]
        assert (loc.url, loc.server_fingerprint) == ("zfs:backup/local", None)
        assert "/enroll" in [e.path for e in srv.log]


# --------------------------------------------------------------------------
# identity rotate
# --------------------------------------------------------------------------


def rotate(run, database, *extra):
    return run("identity", "rotate", *extra, "-c", database)


class TestRotateLocal:
    def test_with_only_local_destinations_it_is_refused_and_creates_nothing(self, run, db, reloads):
        client_with(run, db, reloads, {"loc": ("zfs:backup/local", None), "loc2": ("zfs:other/t", None)})
        before = db_state(db)
        result = rotate(run, db)
        assert result.exit_code == 1
        assert "every destination is local (zfs:<dataset>)" in result.stderr
        assert "nothing to rotate against" in result.stderr
        assert db_state(db) == before and list(reloads) == []
        assert not tls_of(db).exists()

    @pytest.mark.integration
    def test_with_a_mix_only_the_enrolled_https_destination_is_contacted(
        self, run, db, reloads, make_server
    ):
        srv = make_server("a")
        client_with(run, db, reloads, {"loc": ("zfs:backup/local", None), "offsite": (srv.url, None)})
        assert do_enroll(run, db, srv, srv.token(), url=False).exit_code == 0
        reloads.clear()
        srv.log.clear()
        before = db_state(db)
        old = identity.load_identity(tls_of(db) / "client.crt", tls_of(db) / "client.key",
                                     identity.ROLE_CLIENT)
        result = rotate(run, db)
        assert result.exit_code == 0, result.stderr
        lines = [l.split() for l in result.stdout.splitlines() if l.strip()]
        assert lines[0][:4] == ["DESTINATION", "RESULT", "URL", "DETAIL"]
        rows = []
        for cells in lines[1:]:
            if cells[0] == "client":
                break
            rows.append(cells[:2])
        assert rows == [["offsite", "rotated"]]               # `loc` is not in the table
        assert [(e.method, e.path) for e in srv.log] == [("GET", "/whoami"), ("POST", "/rotate")]
        new = identity.load_identity(tls_of(db) / "client.crt", tls_of(db) / "client.key",
                                     identity.ROLE_CLIENT)
        assert new.fingerprint != old.fingerprint
        assert db_state(db) == before and list(reloads) == []


# --------------------------------------------------------------------------
# check
# --------------------------------------------------------------------------


def config_of(database):
    resolved, snap = snapshot_of(database)
    return resolved, snap.config


def tls_entry(world_db):
    from datetime import datetime, timezone

    resolved, config = config_of(world_db)
    return check_view.client_certificate_entry(health, config, resolved, datetime.now(timezone.utc))


class TestCheckLocal:
    def test_with_only_local_destinations_there_is_no_client_certificate_entry(self, run, db):
        make_db(run, db, ONE_LOCAL_YAML)
        assert tls_entry(db) is None

    def test_with_a_mix_the_entry_is_present(self, run, mixed):
        entry = tls_entry(mixed)
        assert entry is not None
        assert (entry.source, entry.subject) == ("tls", "client_certificate")
        assert entry.state == "WARN"                           # no certificate yet: not enrolled

    def test_with_no_destination_at_all_the_entry_is_still_absent(self, run, db):
        make_db(run, db, "datasets:\n  - name: tank/a\n    frequency: 1h\n")
        assert tls_entry(db) is None

    def test_with_only_https_destinations_the_entry_is_unchanged(self, run, rich):
        assert tls_entry(rich).state == "WARN"

    def entries(self, run, database):
        result = run("-c", database, "check", "--json")
        data = json.loads(result.stdout)
        return {(e["source"], e["subject"]) for e in data["entries"]}

    def test_end_to_end_a_local_only_host_has_no_certificate_check(self, run, db):
        make_db(run, db, ONE_LOCAL_YAML)
        assert ("tls", "client_certificate") not in self.entries(run, db)

    def test_end_to_end_a_mixed_host_checks_the_certificate(self, run, mixed):
        assert ("tls", "client_certificate") in self.entries(run, mixed)

    def test_removing_the_last_https_destination_drops_the_entry(self, run, mixed):
        resolved, snap = snapshot_of(mixed)
        snap.config.destinations.pop("offsite")
        snap.config.destinations.pop("spare")
        for ds in snap.config.datasets:
            ds.remote = [r for r in ds.remote if r.destination not in ("offsite", "spare")]
        write_config(resolved, snap.config, expected_generation=snap.generation)
        dispose_all()
        assert tls_entry(mixed) is None


# --------------------------------------------------------------------------
# rename
# --------------------------------------------------------------------------


def copy_path(target, name, cid=CID):
    return f"{target}/{cid}/{name}"


class TestRenameDatasetLocal:
    def test_a_local_only_dataset_needs_force_and_the_refusal_names_both_paths_and_the_command(
        self, run, mixed
    ):
        before, gen = db_state(mixed), generation(mixed)
        result = run("rename", "dataset", "tank/c", "tank/d", "-c", mixed)
        assert result.exit_code == 1
        err = result.stderr
        assert "'tank/c' sends to loc. Renaming it affects its local backups:" in err
        assert "local destination 'loc' (zfs:backup/local)" in err
        assert copy_path("backup/local", "tank/c") in err and copy_path("backup/local", "tank/d") in err
        assert ("`zfs rename backup/local/host-1/tank/c backup/local/host-1/tank/d`") in err
        assert "<server target>" not in err                    # nothing remote here
        assert "Re-run with --force to rename anyway. Nothing was changed." in err
        assert db_state(mixed) == before and generation(mixed) == gen

    def test_force_renames_and_the_warning_has_the_same_note(self, run, mixed):
        result = run("rename", "dataset", "tank/c", "tank/d", "--force", "-c", mixed)
        assert result.exit_code == 0, result.stderr
        (warned,) = [w for w in warnings_in(result) if w.startswith("warning: --force:")]
        assert copy_path("backup/local", "tank/c") in warned and copy_path("backup/local", "tank/d") in warned
        assert "`zfs rename backup/local/host-1/tank/c backup/local/host-1/tank/d`" in warned
        assert "tank/d" in [d.name for d in stored(mixed).datasets]

    def test_a_same_parent_rename_gets_the_exact_rename_and_relabel_commands(self, run, mixed):
        result = run("rename", "dataset", "tank/c", "tank/d", "-c", mixed)
        assert result.exit_code == 1
        # the copy moves, and its recorded source (a LOCAL property the receive path checks)
        # is re-labelled with the new client dataset name
        assert "`zfs rename backup/local/host-1/tank/c backup/local/host-1/tank/d`" in result.stderr
        assert ("`zfs set org.zfsbackup:source_dataset=tank/d backup/local/host-1/tank/d`"
                in result.stderr)
        assert "zfs rename -p" not in result.stderr
        # a copy left behind is still discovered and pruned (`local_copies`): say so
        err = " ".join(result.stderr.split())
        assert "the pruner still applies its last stamped retention" in err
        assert "prunes it to its rules any more" not in err

    @pytest.mark.parametrize(
        "old, new",
        [("tank/c", "tank/sub/c"), ("tank/c", "pool2/c"), ("tank/c", "pool2/x/c")],
    )
    def test_a_parent_change_gets_no_recipe_only_the_new_path(self, run, db, old, new):
        make_db(run, db, ONE_LOCAL_YAML.replace("tank/forever", old).replace(
            'retention: forever', 'retention:\n      "1d": "30d"'))
        seed_client_id(db, CID)
        result = run("rename", "dataset", old, new, "-c", db)
        assert result.exit_code == 1
        err = result.stderr
        assert f"backup/local/host-1/{old}" in err and f"backup/local/host-1/{new}" in err
        assert "The parent path changes" in err and "orphaned" in err
        assert f"a full send into backup/local/host-1/{new}" in err
        assert "zfs rename" not in err and "zfs set" not in err   # a `-p` container is never adopted
        flat = " ".join(err.split())
        assert "the pruner still applies its last stamped retention" in flat
        assert "move it out of backup/local/host-1/ by hand" in flat

    def test_a_top_level_rename_has_the_same_parent(self, run, db):
        make_db(run, db, ONE_LOCAL_YAML.replace("tank/forever", "tank").replace(
            'retention: forever', 'retention:\n      "1d": "30d"'))
        seed_client_id(db, CID)
        result = run("rename", "dataset", "tank", "pool", "-c", db)
        assert result.exit_code == 1
        assert "`zfs rename backup/local/host-1/tank backup/local/host-1/pool`" in result.stderr

    def test_the_commands_quote_names_the_shell_would_split(self, run, db):
        make_db(run, db, ONE_LOCAL_YAML.replace("tank/forever", "tank/my data").replace(
            'retention: forever', 'retention:\n      "1d": "30d"'))
        seed_client_id(db, CID)
        result = run("rename", "dataset", "tank/my data", "tank/b c", "-c", db)
        assert result.exit_code == 1
        assert ("`zfs rename 'backup/local/host-1/tank/my data' 'backup/local/host-1/tank/b c'`"
                in result.stderr)
        assert ("`zfs set org.zfsbackup:source_dataset='tank/b c' 'backup/local/host-1/tank/b c'`"
                in result.stderr)

    def test_a_mixed_dataset_shows_the_server_note_and_the_local_note(self, run, mixed):
        result = run("rename", "dataset", "tank/a", "tank/z", "-c", mixed)
        assert result.exit_code == 1
        err = result.stderr
        assert "'tank/a' sends to loc, offsite. Renaming it affects its remote backups:" in err
        assert "<server target>/host-1/tank/a" in err and "<server target>/host-1/tank/z" in err
        assert "local destination 'loc' (zfs:backup/local)" in err
        assert err.index("<server target>") < err.index("local destination 'loc'")
        assert err.count("local destination ") == 1
        assert "`zfs rename backup/local/host-1/tank/a backup/local/host-1/tank/z`" in err

    def test_two_local_destinations_get_a_note_each(self, run, mixed):
        result = run("rename", "dataset", "tank/two", "tank/three", "-c", mixed)
        assert result.exit_code == 1
        err = result.stderr
        assert "'tank/two' sends to loc, loc.two. Renaming it affects its local backups:" in err
        assert "local destination 'loc' (zfs:backup/local)" in err
        assert "local destination 'loc.two' (zfs:other/target)" in err
        assert "`zfs rename backup/local/host-1/tank/two backup/local/host-1/tank/three`" in err
        assert "`zfs rename other/target/host-1/tank/two other/target/host-1/tank/three`" in err

    def test_a_remote_only_message_is_unchanged(self, run, mixed):
        result = run("rename", "dataset", "tank/b", "tank/z", "-c", mixed)
        assert result.exit_code == 1
        assert result.stderr == (
            "error: 'tank/b' sends to offsite. Renaming it affects its remote backups: "
            "the server keeps this dataset at <server target>/host-1/tank/b; under the new "
            "name it would be <server target>/host-1/tank/z. The incremental base is "
            "negotiated by snapshot name, so the next backup to each destination is likely "
            "a full send unless the server already has the dataset under the new path; the "
            "existing copy at <server target>/host-1/tank/b stays where it is, orphaned "
            "(this tool never touches a server). "
            "Re-run with --force to rename anyway. Nothing was changed.\n"
        )

    def test_a_remote_only_force_warning_is_unchanged(self, run, mixed):
        result = run("rename", "dataset", "tank/b", "tank/z", "--force", "-c", mixed)
        assert result.exit_code == 0, result.stderr
        (warned,) = [w for w in warnings_in(result) if w.startswith("warning: --force:")]
        assert warned == (
            "warning: --force: the server keeps this dataset at <server target>/host-1/tank/b; "
            "under the new name it would be <server target>/host-1/tank/z. The incremental base "
            "is negotiated by snapshot name, so the next backup to each destination is likely "
            "a full send unless the server already has the dataset under the new path; the "
            "existing copy at <server target>/host-1/tank/b stays where it is, orphaned "
            "(this tool never touches a server)"
        )
        assert "local destination" not in result.stderr

    def test_without_a_client_id_the_placeholder_is_used(self, run, db):
        make_db(run, db, ONE_LOCAL_YAML.replace('retention: forever', 'retention:\n      "1d": "30d"'))
        drop_client_id(db)
        result = run("rename", "dataset", "tank/forever", "tank/other", "-c", db)
        assert result.exit_code == 1
        assert "backup/local/<client-id>/tank/forever" in result.stderr
        assert "backup/local/<client-id>/tank/other" in result.stderr

    def test_a_dataset_that_sends_nowhere_needs_no_note(self, run, mixed):
        result = run("rename", "dataset", "tank/none", "tank/other", "-c", mixed)
        assert result.exit_code == 0, result.stderr
        assert "local destination" not in result.stderr + result.stdout
        assert "tank/other" in [d.name for d in stored(mixed).datasets]


class TestRenameDestinationLocal:
    def test_a_local_destination_is_renamed_without_force_and_anchors_are_copied(
        self, run, mixed, fake_zfs
    ):
        fake_zfs.add("tank/a", {ANCHOR + "loc": "snapA"})
        fake_zfs.add("tank/c", {ANCHOR + "loc": "snapC"})
        fake_zfs.add("tank/two", {ANCHOR + "loc": "snapT", ANCHOR + "loc.two": "snapU"})
        gen = generation(mixed)
        result = run("rename", "destination", "loc", "loc9", "-c", mixed)
        assert result.exit_code == 0, result.stderr
        assert "Renamed destination 'loc' -> 'loc9' in the configuration." in result.stdout
        assert "tank/a: anchor.loc9 set to snapA (copied from anchor.loc)" in result.stdout
        assert fake_zfs.props("tank/a")[ANCHOR + "loc9"] == "snapA"
        assert fake_zfs.props("tank/a")[ANCHOR + "loc"] == "snapA"       # copied, never moved
        assert fake_zfs.props("tank/c")[ANCHOR + "loc9"] == "snapC"
        assert ANCHOR + "loc.two9" not in fake_zfs.props("tank/two")
        assert fake_zfs.props("tank/two")[ANCHOR + "loc.two"] == "snapU"
        config = stored(mixed)
        assert "loc" not in config.destinations
        assert config.destinations["loc9"].url == "zfs:backup/local"
        assert [r.destination for r in {d.name: d for d in config.datasets}["tank/a"].remote] == [
            "loc9", "offsite"]
        assert generation(mixed) == gen + 1
        assert host_client_id(mixed) == CID

    def test_a_local_destination_rename_prints_no_server_path_note(self, run, mixed, fake_zfs):
        fake_zfs.add("tank/c", {})
        result = run("rename", "destination", "loc", "loc9", "--skip-missing", "-c", mixed)
        assert result.exit_code == 0, result.stderr
        assert "--force" not in result.stderr and "<server target>" not in result.stderr


# --------------------------------------------------------------------------
# identity set-client-id
# --------------------------------------------------------------------------


def set_id(run, database, *args):
    return run("identity", "set-client-id", *args, "-c", database)


def hint_lines(result):
    """The example commands in the local-destination warning: one `zfs destroy -r` per target.
    (A `zfs rename` would be wrong here: the copies carry the OLD client id as a local
    property, so a renamed tree is not adopted under the new id.)"""
    return [l.strip() for l in result.stderr.splitlines() if l.strip().startswith("zfs ")]


class TestSetClientIdLocal:
    def test_it_warns_and_prints_one_example_command_per_distinct_target(self, run, mixed):
        result = set_id(run, mixed, "new-1")
        assert result.exit_code == 0, result.stderr
        (warned,) = [w for w in warnings_in(result) if w.startswith("warning: local destination(s)")]
        assert "'loc', 'loc.two'" in warned
        assert "keep their copies under <target>/host-1/" in warned
        assert "FULL send into <target>/new-1/" in warned
        assert hint_lines(result) == [
            "zfs destroy -r backup/local/host-1",
            "zfs destroy -r other/target/host-1",
        ]
        assert "a rename would not adopt them" in warned
        assert "the copies carry the old client id" in warned
        assert "the copies under the old id are orphaned" in warned
        assert "FULL send into <target>/new-1/" in warned
        assert "Optional and DESTRUCTIVE" in warned and "destroy it by hand" in warned
        assert "zfs rename" not in result.stderr
        assert host_client_id(mixed) == "new-1"
        assert result.stdout.splitlines()[0] == "client id: host-1 -> new-1"

    def test_no_hint_when_there_was_no_previous_id(self, run, db):
        make_db(run, db, ONE_LOCAL_YAML)
        drop_client_id(db)
        result = set_id(run, db, "new-1")
        assert result.exit_code == 0, result.stderr
        assert not any("local destination" in w for w in warnings_in(result))
        assert hint_lines(result) == []
        assert host_client_id(db) == "new-1"

    def test_without_local_destinations_the_output_is_unchanged(self, run, rich):
        seed_client_id(rich, CID)
        result = set_id(run, rich, "new-1")
        assert result.exit_code == 0, result.stderr
        assert len(warnings_in(result)) == 1
        assert warnings_in(result)[0].startswith("warning: changing the client id from 'host-1' to 'new-1'")
        assert "local destination" not in result.stderr and "zfs rename" not in result.stderr
        assert result.stdout.splitlines()[0] == "client id: host-1 -> new-1"

    def test_a_same_id_is_a_noop_without_the_hint(self, run, mixed):
        before = db_state(mixed)
        result = set_id(run, mixed, CID)
        assert result.exit_code == 0
        assert result.stdout == f"client id is already {CID}; nothing changed\n"
        assert hint_lines(result) == [] and result.stderr == ""
        assert db_state(mixed) == before

    def test_a_pinned_https_destination_still_blocks_without_force_and_local_ones_do_not(
        self, run, pinned
    ):
        before = db_state(pinned)
        result = set_id(run, pinned, "new-1")
        assert result.exit_code == 1
        assert "destination(s) 'offsite' are enrolled" in result.stderr
        assert "'loc'" not in result.stderr.split("Re-run with --force")[0]
        assert db_state(pinned) == before and host_client_id(pinned) == CID

    def test_force_with_a_mix_warns_about_both(self, run, pinned):
        result = set_id(run, pinned, "new-1", "--force")
        assert result.exit_code == 0, result.stderr
        assert any("stay enrolled under the old client id" in w for w in warnings_in(result))
        assert any(w.startswith("warning: local destination(s)") for w in warnings_in(result))
        assert len(hint_lines(result)) == 2


# --------------------------------------------------------------------------
# commit_config: the client id for a local destination
# --------------------------------------------------------------------------


def with_local(config, name="spare", url="zfs:backup/local"):
    config.destinations[name] = Destination(url=url)
    return config


@pytest.fixture
def lock_spy(monkeypatch):
    """Count `writer_lock` entries made through `cli_write` (the lock `commit_config` takes)."""
    calls = []
    real = cli_write.writer_lock

    import contextlib

    @contextlib.contextmanager
    def spy(*args, **kwargs):
        calls.append(args)
        with real(*args, **kwargs) as held:
            yield held

    monkeypatch.setattr(cli_write, "writer_lock", spy)
    return calls


class TestCommitConfigClientId:
    def snap(self, database):
        resolved = resolve_config_path(str(database))
        return resolved, cli_write.read_snapshot(resolved)

    def test_a_local_destination_without_an_id_stores_a_unique_id_then_the_config(
        self, run, rich, reloads, capsys
    ):
        resolved, snapshot = self.snap(rich)
        assert snapshot.client_id is None
        result = cli_write.commit_config(resolved, snapshot, with_local(snapshot.config))
        out = capsys.readouterr().out
        new_id = host_client_id(rich)
        assert new_id and client_id_problem(new_id) is None
        assert f"generated this host's client id: {new_id}\n" in out
        assert result.generation == snapshot.generation + 2 and generation(rich) == result.generation
        assert stored(rich).destinations["spare"].url == "zfs:backup/local"

    def test_ids_are_unique_per_store(self, run, tmp_path):
        ids = []
        for n in range(2):
            path = tmp_path / f"s{n}" / "config.db"
            make_db(run, path, RICH_YAML, name=f"in{n}.yaml")
            resolved, snapshot = self.snap(path)
            cli_write.commit_config(resolved, snapshot, with_local(snapshot.config), announce=False)
            ids.append(host_client_id(path))
        assert ids[0] and ids[1] and ids[0] != ids[1]

    def test_one_reload_request_on_the_final_generation(self, run, rich, reloads):
        resolved, snapshot = self.snap(rich)
        result = cli_write.commit_config(resolved, snapshot, with_local(snapshot.config))
        assert len(reloads) == 1
        skip, held = reloads[0]
        assert skip == (snapshot.store_id, result.generation) == held == store_key(rich)

    def test_through_set_it_is_one_request_and_the_id_line_is_printed(self, run, rich, reloads):
        gen = generation(rich)
        result = ok(run, rich, "set", "destinations.spare.url", "zfs:backup/local")
        assert generated_id(result) == host_client_id(rich)
        assert generation(rich) == gen + 2
        assert [r[0] for r in reloads] == [store_key(rich)] and len(reloads) == 1

    def test_a_concurrent_write_is_a_conflict_and_no_id_is_stored(self, run, rich, reloads):
        resolved, snapshot = self.snap(rich)
        other = read_config(resolved)
        other.config.api_port = 9999
        write_config(resolved, other.config, expected_generation=other.generation)
        dispose_all()
        with pytest.raises(ConfigConflict):
            cli_write.commit_config(resolved, snapshot, with_local(snapshot.config))
        assert host_client_id(rich) is None
        assert generation(rich) == snapshot.generation + 1     # only the competing write
        assert stored(rich).destinations["spare"].url == "https://spare:8080"
        assert list(reloads) == []

    def test_through_set_a_concurrent_write_is_exit_75_and_no_id(self, run, rich, reloads, monkeypatch):
        real = cli_main.commit_config

        def racing(resolved, snapshot, config, **kwargs):
            current = read_config(resolved)
            current.config.api_port = 9999
            write_config(resolved, current.config, expected_generation=current.generation)
            dispose_all()
            return real(resolved, snapshot, config, **kwargs)

        monkeypatch.setattr(cli_main, "commit_config", racing)
        result = run("set", "destinations.spare.url", "zfs:backup/local", "-c", rich)
        assert result.exit_code == 75
        assert "generated this host's client id" not in result.stdout
        assert host_client_id(rich) is None
        assert stored(rich).destinations["spare"].url == "https://spare:8080"
        assert list(reloads) == []

    def test_a_failed_config_write_after_the_id_still_asks_for_one_reload_and_reraises(
        self, run, rich, reloads, monkeypatch, capsys
    ):
        resolved, snapshot = self.snap(rich)

        def boom(*args, **kwargs):
            raise ConfigBusy("simulated: the database is busy")

        monkeypatch.setattr(cli_write, "write_config", boom)
        with pytest.raises(ConfigBusy):
            cli_write.commit_config(resolved, snapshot, with_local(snapshot.config))
        new_id = host_client_id(rich)
        assert new_id, "the id is committed"
        assert f"generated this host's client id: {new_id}" in capsys.readouterr().out
        assert generation(rich) == snapshot.generation + 1
        assert stored(rich).destinations["spare"].url == "https://spare:8080"
        assert len(reloads) == 1
        assert reloads[0][0] == (snapshot.store_id, snapshot.generation + 1) == reloads[0][1]

    def test_a_failure_after_the_id_carries_the_committed_mark(self, run, rich, reloads, monkeypatch):
        resolved, snapshot = self.snap(rich)

        def boom(*args, **kwargs):
            raise ConfigBusy("simulated: the database is busy")

        monkeypatch.setattr(cli_write, "write_config", boom)
        with pytest.raises(ConfigBusy) as caught:
            cli_write.commit_config(resolved, snapshot, with_local(snapshot.config))
        assert getattr(caught.value, cli_write.ID_COMMITTED_MARK) is True

    def test_a_conflict_before_the_id_has_no_mark(self, run, rich, reloads):
        resolved, snapshot = self.snap(rich)
        other = read_config(resolved)
        other.config.api_port = 9999
        write_config(resolved, other.config, expected_generation=other.generation)
        dispose_all()
        with pytest.raises(ConfigConflict) as caught:
            cli_write.commit_config(resolved, snapshot, with_local(snapshot.config))
        assert not getattr(caught.value, cli_write.ID_COMMITTED_MARK, False)

    def test_an_interrupt_between_the_id_and_the_config_write_reloads_exactly_once(
        self, run, rich, reloads, monkeypatch
    ):
        resolved, snapshot = self.snap(rich)
        calls = []
        real = cli_write.begin_reload

        def counting(*args, **kwargs):
            calls.append((args[1:], kwargs))
            return real(*args, **kwargs)

        def interrupted(*args, **kwargs):
            raise KeyboardInterrupt

        monkeypatch.setattr(cli_write, "begin_reload", counting)
        monkeypatch.setattr(cli_write, "write_config", interrupted)
        with pytest.raises(KeyboardInterrupt) as caught:
            cli_write.commit_config(resolved, snapshot, with_local(snapshot.config))
        assert len(calls) == 1
        (generation_arg, store_arg), kwargs = calls[0]
        assert generation_arg == snapshot.generation + 1 and store_arg == snapshot.store_id
        assert kwargs == {"announce_pending": True}
        assert getattr(caught.value, cli_write.ID_COMMITTED_MARK, False) is True
        assert host_client_id(rich) is not None and len(reloads) == 1

    def test_through_set_a_failed_write_after_the_id_is_exit_75_with_the_id_announced(
        self, run, rich, reloads, monkeypatch
    ):
        def boom(*args, **kwargs):
            raise ConfigBusy("simulated: the database is busy")

        monkeypatch.setattr(cli_write, "write_config", boom)
        result = run("set", "destinations.spare.url", "zfs:backup/local", "-c", rich)
        assert result.exit_code == 75
        new_id = host_client_id(rich)
        assert new_id and generated_id(result) == new_id       # committed, so announced
        assert stored(rich).destinations["spare"].url == "https://spare:8080"
        assert len(reloads) == 1

    def test_a_caller_held_lock_is_reused(self, run, rich, reloads, lock_spy):
        resolved, snapshot = self.snap(rich)
        with writer_lock(resolved) as held:
            lock_spy.clear()
            result = cli_write.commit_config(
                resolved, snapshot, with_local(snapshot.config), announce=False, lock=held)
        assert lock_spy == [], "a second writer_lock was taken"
        assert result.generation == snapshot.generation + 2
        assert host_client_id(rich) is not None
        assert list(reloads) == []                              # announce=False: the caller signals

    def test_a_caller_held_lock_gets_the_line_with_the_callers_own_reload(self, run, rich, reloads, capsys):
        resolved, snapshot = self.snap(rich)
        with writer_lock(resolved) as held:
            result = cli_write.commit_config(
                resolved, snapshot, with_local(snapshot.config), announce=False, lock=held)
        assert "generated this host's client id" not in capsys.readouterr().out   # lock was held
        cli_write.begin_reload(resolved, result.generation, result.store_id)
        assert f"generated this host's client id: {host_client_id(rich)}" in capsys.readouterr().out
        assert len(reloads) == 1

    def test_a_failure_under_a_caller_held_lock_signals_but_prints_nothing_yet(
        self, run, rich, reloads, monkeypatch, capsys
    ):
        resolved, snapshot = self.snap(rich)

        def boom(*args, **kwargs):
            raise ConfigBusy("simulated: the database is busy")

        monkeypatch.setattr(cli_write, "write_config", boom)
        with writer_lock(resolved) as held:
            with pytest.raises(ConfigBusy):
                cli_write.commit_config(
                    resolved, snapshot, with_local(snapshot.config), announce=False, lock=held)
        assert len(reloads) == 1
        assert "generated this host's client id" not in capsys.readouterr().out

    def test_the_line_comes_after_the_reload_request(self, run, rich, monkeypatch):
        events = []
        monkeypatch.setattr(daemonlock, "request_reload",
                            lambda resolved, *, skip_if_applied=None: events.append("reload")
                            or daemonlock.ReloadRequest("no-daemon"))
        real = cli_write._announce_client_id
        monkeypatch.setattr(cli_write, "_announce_client_id",
                            lambda client_id: (events.append("line"), real(client_id)))
        ok(run, rich, "set", "destinations.spare.url", "zfs:backup/local")
        assert events == ["reload", "line"]

    def test_without_a_held_lock_exactly_one_writer_lock_covers_both_writes(
        self, run, rich, reloads, lock_spy
    ):
        resolved, snapshot = self.snap(rich)
        cli_write.commit_config(resolved, snapshot, with_local(snapshot.config), announce=False)
        assert len(lock_spy) == 1

    def test_an_existing_id_is_never_replaced(self, run, mixed, reloads, capsys):
        resolved, snapshot = self.snap(mixed)
        assert snapshot.client_id == CID
        result = cli_write.commit_config(
            resolved, snapshot, with_local(snapshot.config, "spare", "zfs:third/target"))
        assert host_client_id(mixed) == CID
        assert result.generation == snapshot.generation + 1
        assert "generated this host's client id" not in capsys.readouterr().out

    def test_no_local_destination_stores_no_id(self, run, rich, reloads):
        resolved, snapshot = self.snap(rich)
        snapshot.config.api_port = 9999
        result = cli_write.commit_config(resolved, snapshot, snapshot.config)
        assert host_client_id(rich) is None and result.generation == snapshot.generation + 1

    def test_the_lock_is_free_afterwards(self, run, rich, reloads):
        resolved, snapshot = self.snap(rich)
        cli_write.commit_config(resolved, snapshot, with_local(snapshot.config), announce=False)
        with writer_lock(resolved):
            pass                                                 # raises ConfigBusy if still held


# --------------------------------------------------------------------------
# import
# --------------------------------------------------------------------------


class TestImportClientId:
    def importing(self, run, database, text, name="imp.yaml"):
        source = write_yaml(database.parent.parent, text, name)
        return run("import", source, "-c", database)

    def test_a_new_store_with_a_local_destination_gets_an_id(self, run, db, reloads):
        result = self.importing(run, db, ONE_LOCAL_YAML)
        assert result.exit_code == 0, result.stderr
        new_id = generated_id(result)
        assert new_id and client_id_problem(new_id) is None
        assert host_client_id(db) == new_id
        assert len(reloads) == 1
        assert reloads[0][0] == reloads[0][1] == store_key(db)  # the LAST generation
        # the id line comes after the import's own report and before the daemon notice
        lines = result.stdout.splitlines()
        assert lines[0].startswith("Imported ") and lines[1].startswith("Schema revision:")
        assert lines[2] == GENERATED + new_id and lines[3].startswith("config updated (generation ")
        assert f"(generation {store_key(db)[1]})" in lines[3]

    def test_an_existing_store_without_an_id_gets_one_and_the_old_config_is_replaced(
        self, run, db, reloads
    ):
        make_db(run, db, RICH_YAML)
        assert host_client_id(db) is None
        reloads.clear()
        result = self.importing(run, db, ONE_LOCAL_YAML)
        assert result.exit_code == 0, result.stderr
        new_id = generated_id(result)
        assert new_id and host_client_id(db) == new_id
        assert sorted(stored(db).destinations) == ["loc"]
        assert len(reloads) == 1 and reloads[0][0] == store_key(db)

    def test_an_existing_id_is_kept_and_nothing_is_generated(self, run, db, reloads):
        make_db(run, db, RICH_YAML)
        seed_client_id(db, CID)
        reloads.clear()
        result = self.importing(run, db, ONE_LOCAL_YAML)
        assert result.exit_code == 0, result.stderr
        assert generated_id(result) is None and host_client_id(db) == CID
        assert len(reloads) == 1 and reloads[0][0] == store_key(db)

    def test_the_generation_with_an_existing_id_is_the_imports_alone(self, run, db, reloads):
        make_db(run, db, RICH_YAML)
        seed_client_id(db, CID)
        before = generation(db)
        self.importing(run, db, ONE_LOCAL_YAML)
        assert generation(db) == before + 1

    def test_without_a_local_destination_nothing_is_generated(self, run, db, reloads, monkeypatch):
        called = []
        monkeypatch.setattr(cli_main, "ensure_client_id", lambda *a, **k: called.append(a))
        result = self.importing(run, db, RICH_YAML)
        assert result.exit_code == 0, result.stderr
        assert called == [] and generated_id(result) is None
        assert host_client_id(db) is None
        assert len(reloads) == 1

    def test_the_generation_with_a_new_id_is_two_steps_past_the_empty_store(self, run, db, reloads):
        make_db(run, db, RICH_YAML)
        before = generation(db)
        self.importing(run, db, ONE_LOCAL_YAML)
        assert generation(db) == before + 2

    def test_a_failure_after_the_import_still_sends_one_reload_and_keeps_the_import(
        self, run, db, reloads, monkeypatch
    ):
        def busy(*args, **kwargs):
            raise ConfigBusy("simulated: the database is busy")

        monkeypatch.setattr(cli_main, "ensure_client_id", busy)
        result = self.importing(run, db, ONE_LOCAL_YAML)
        assert result.exit_code == 75
        assert "the import itself was committed" in result.stderr
        assert "identity set-client-id" in result.stderr
        assert sorted(stored(db).destinations) == ["loc"]
        assert host_client_id(db) is None
        assert len(reloads) == 1 and reloads[0][0] == store_key(db)

    def test_the_imports_own_warnings_are_printed_exactly_once(self, run, db):
        result = self.importing(run, db, ONE_LOCAL_YAML)
        assert result.exit_code == 0, result.stderr
        assert count_containing(stderr_lines(result), NEVER_PRUNE_TEXT) == 1
        assert generated_id(result) is not None

    def test_the_warnings_are_once_also_when_no_id_is_needed(self, run, db):
        make_db(run, db, ONE_LOCAL_YAML)
        seed_client_id(db, CID)
        result = self.importing(run, db, ONE_LOCAL_YAML)
        assert count_containing(stderr_lines(result), NEVER_PRUNE_TEXT) == 1
        assert generated_id(result) is None

    def rewriting_after_import(self, monkeypatch, path, text):
        """`import_yaml` commits, then the file on disk is replaced by `text`."""
        real = cli_main.import_yaml

        def commit_then_change(*args, **kwargs):
            result = real(*args, **kwargs)
            path.write_text(text)
            return result

        monkeypatch.setattr(cli_main, "import_yaml", commit_then_change)

    def test_the_stored_configuration_decides_not_the_file_read_again(self, run, db, reloads, monkeypatch):
        source = write_yaml(db.parent.parent, ONE_LOCAL_YAML, "imp.yaml")
        self.rewriting_after_import(monkeypatch, source, RICH_YAML)     # changed since the import
        result = run("import", source, "-c", db)
        assert result.exit_code == 0, result.stderr
        new_id = host_client_id(db)
        assert new_id and generated_id(result) == new_id
        assert sorted(stored(db).destinations) == ["loc"]

    def test_a_file_that_gained_a_local_destination_since_generates_nothing(
        self, run, db, reloads, monkeypatch
    ):
        source = write_yaml(db.parent.parent, RICH_YAML, "imp.yaml")
        self.rewriting_after_import(monkeypatch, source, ONE_LOCAL_YAML)
        result = run("import", source, "-c", db)
        assert result.exit_code == 0, result.stderr
        assert generated_id(result) is None and host_client_id(db) is None

    def test_the_file_is_read_once_so_a_fifo_can_be_imported(self, run, db, reloads, tmp_path):
        """A named pipe holds the YAML once: a second read would wait for a writer that never
        comes. (The check is that the import returns, with the id generated from what was
        COMMITTED.)"""
        import os
        import threading

        fifo = tmp_path / "fifo.yaml"
        os.mkfifo(fifo)

        def writer():
            with open(fifo, "w") as handle:
                handle.write(ONE_LOCAL_YAML)

        threading.Thread(target=writer, daemon=True).start()
        outcome = []
        reader = threading.Thread(target=lambda: outcome.append(run("import", fifo, "-c", db)), daemon=True)
        reader.start()
        reader.join(30)
        if reader.is_alive():                         # unblock a second open, then fail
            try:
                with open(fifo, "w") as handle:
                    handle.write(ONE_LOCAL_YAML)
            except OSError:
                pass
            pytest.fail("import hung: the YAML file was read a second time")
        result = outcome[0]
        assert result.exit_code == 0, result.stderr
        new_id = host_client_id(db)
        assert new_id and generated_id(result) == new_id
        assert len(reloads) == 1

    def test_an_id_that_appeared_meanwhile_is_not_announced(self, run, db, reloads, monkeypatch):
        # `ensure_client_id` finds an id stored by someone else between the check and the call:
        # created=False, so nothing was generated here and the key is the import's.
        monkeypatch.setattr(cli_main, "ensure_client_id", lambda resolved, lock=None: ("someone-1", False))
        result = self.importing(run, db, ONE_LOCAL_YAML)
        assert result.exit_code == 0, result.stderr
        assert generated_id(result) is None
        assert len(reloads) == 1 and reloads[0][0] == store_key(db)

    def test_a_read_back_failure_after_the_import_keeps_it_and_asks_for_one_reload(
        self, run, db, reloads, monkeypatch
    ):
        def busy(*args, **kwargs):
            raise ConfigBusy("simulated: the database is busy")

        monkeypatch.setattr(cli_main, "read_snapshot", busy)
        result = self.importing(run, db, ONE_LOCAL_YAML)
        assert result.exit_code == 75
        assert "the import itself was committed, but it could not be read back" in result.stderr
        assert "identity set-client-id" in result.stderr
        assert sorted(stored(db).destinations) == ["loc"] and host_client_id(db) is None
        assert len(reloads) == 1 and reloads[0][0] == store_key(db)

    def test_a_failed_read_after_the_id_still_announces_it_and_reloads_once(
        self, run, db, reloads, monkeypatch
    ):
        real = cli_main.read_snapshot
        calls = []

        def second_read_fails(*args, **kwargs):
            calls.append(1)
            if len(calls) == 2:                      # the read of the key after the id
                raise ConfigBusy("simulated: the database is busy")
            return real(*args, **kwargs)

        monkeypatch.setattr(cli_main, "read_snapshot", second_read_fails)
        result = self.importing(run, db, ONE_LOCAL_YAML)
        assert result.exit_code == 0, result.stderr
        new_id = host_client_id(db)
        assert new_id and generated_id(result) == new_id
        assert len(reloads) == 1 and reloads[0][0] is None     # no key: the poll covers it

    def test_an_invalid_local_url_in_the_yaml_fails_before_anything_is_written(self, run, db, reloads):
        result = self.importing(run, db, ONE_LOCAL_YAML.replace("zfs:backup/local", "zfs:/bad"))
        assert result.exit_code == 1
        assert "destinations[loc]" in result.stderr
        assert not db.exists() and list(reloads) == []


# --------------------------------------------------------------------------
# edit: the first local destination
# --------------------------------------------------------------------------


class TestEditClientId:
    def test_the_whole_config_buffer_adding_the_first_local_destination_generates_the_id(
        self, run, rich, stub, scratch_tmp, reloads
    ):
        gen = generation(rich)
        stub.plan([replace('url: "https://spare:8080"', 'url: "zfs:backup/local"')])
        result = edit(run, rich, stub, "--no-prompt")
        assert result.exit_code == 0, result.stderr
        new_id = generated_id(result)
        assert new_id and host_client_id(rich) == new_id
        assert generation(rich) == gen + 2
        assert stored(rich).destinations["spare"].url == "zfs:backup/local"
        assert len(reloads) == 1 and reloads[0][0] == reloads[0][1] == store_key(rich)
        assert buffers_in(scratch_tmp) == []

    def test_edit_destination_does_the_same(self, run, rich, stub, scratch_tmp, reloads):
        gen = generation(rich)
        stub.plan([replace("https://spare:8080", "zfs:backup/local")])
        result = edit(run, rich, stub, "destination", "spare", "--no-prompt")
        assert result.exit_code == 0, result.stderr
        new_id = host_client_id(rich)
        assert new_id and generated_id(result) == new_id
        assert generation(rich) == gen + 2 and len(reloads) == 1

    def test_with_an_id_already_stored_nothing_is_generated(self, run, mixed, stub, scratch_tmp, reloads):
        gen = generation(mixed)
        stub.plan([replace("https://spare:8080", "zfs:third/target")])
        result = edit(run, mixed, stub, "destination", "spare", "--no-prompt")
        assert result.exit_code == 0, result.stderr
        assert generated_id(result) is None and host_client_id(mixed) == CID
        assert generation(mixed) == gen + 1

    def test_a_failed_write_after_the_id_says_only_the_id_was_stored(
        self, run, rich, stub, scratch_tmp, reloads, monkeypatch
    ):
        def boom(*args, **kwargs):
            raise ConfigBusy("simulated: the database is busy")

        monkeypatch.setattr(cli_write, "write_config", boom)
        stub.plan([replace('url: "https://spare:8080"', 'url: "zfs:backup/local"')])
        result = edit(run, rich, stub, "--no-prompt")
        assert result.exit_code == 75
        assert "only this host's new client id was stored, the configuration itself was not changed" in result.stderr
        assert "nothing was written to the database" not in result.stderr
        assert host_client_id(rich) is not None and generated_id(result) == host_client_id(rich)
        assert stored(rich).destinations["spare"].url == "https://spare:8080"
        assert len(reloads) == 1
        assert len(buffers_in(scratch_tmp)) == 1               # the operator's edit is kept

    def test_a_concurrent_write_during_the_edit_is_75_and_stores_no_id(
        self, run, rich, stub, scratch_tmp, reloads, monkeypatch
    ):
        real = editor.commit_config

        def racing(resolved, snapshot, config, **kwargs):
            current = read_config(resolved)
            current.config.api_port = 9999
            write_config(resolved, current.config, expected_generation=current.generation)
            dispose_all()
            return real(resolved, snapshot, config, **kwargs)

        monkeypatch.setattr(editor, "commit_config", racing)
        stub.plan([replace('url: "https://spare:8080"', 'url: "zfs:backup/local"')])
        result = edit(run, rich, stub, "--no-prompt")
        assert result.exit_code == 75
        assert "nothing was written to the database" in result.stderr
        assert "new client id was stored" not in result.stderr
        assert host_client_id(rich) is None
        assert stored(rich).destinations["spare"].url == "https://spare:8080"
