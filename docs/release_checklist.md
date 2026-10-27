# Release checklist

For a zfsbackup release (the `zfsbackup` distribution, which ships `zfsbackup` and `libzfseasy`). Work through it
on the release branch after the last merge. Every item that touches a VM uses only the `zfsb-*` Multipass VMs.

## 1. Version and notes

- [ ] Bump `version` in `pyproject.toml` (the only place it is written; `zfsbackup.version.installed_version()`
      reads it from the installed metadata).
- [ ] `poetry lock` if dependencies changed; check the pins (`werkzeug <3.2`, `urllib3 <3`, `requests <3`,
      `sqlalchemy <2.2`) still match what CI tested.
- [ ] Release notes say, explicitly:
  - [ ] whether `PROTOCOL_VERSION` (`zfsbackup/protocol.py`) changed: if it did, **client and server must be
        upgraded together** (an unmatched peer gets 426 and remote backups pause);
  - [ ] whether the config schema head (`EXPECTED_HEAD`, `zfsbackup/config/store/migrate.py`) changed: the unit's
        `ExecStartPre` migrates and writes a pre-migration backup to `/var/lib/zfsbackup/backups`;
  - [ ] any change to the frozen send flags (`-L -c`, raw `-w`) or to the `raw` semantics: an existing server copy
        then needs a full re-send after it is renamed aside;
  - [ ] new or removed known limitations (`zfsbackup/README.md`, "Known limitations").

## 2. Automated checks

- [ ] GitHub Actions green on the release commit: the mocked matrix (Python 3.11–3.15, SQLAlchemy 2.0 and 2.1,
      macOS).
- [ ] The latest **real-ZFS** workflow run on `master` (`.github/workflows/real-zfs.yml`) is green, or run it by
      hand (`workflow_dispatch`).
- [ ] The latest **latest-deps** run is green, or its failures are understood (a contract test naming a changed
      internal means a pin must stay or code must change).
- [ ] The mocked suite on `zfsb-ci` with the system Pythons (`~/src/sys3.11`, `sys3.12`, `sys3.13`), `ZFS_CMD` left
      **unset** (conftest stubs it).

## 3. Real ZFS

- [ ] OpenZFS matrix: `scenarios/test_openzfs_matrix.sh --only 21,22,24 --extra tests/test_zfsbackup_real.py`
      (2.1, 2.2 and 2.4). No two matrix runs at once on one VM.
- [ ] If the zfs output formats might have changed: `scenarios/test_openzfs_matrix.sh --captures-only` and diff
      against `tests/fixtures/openzfs/`; commit refreshed fixtures with the reason.
- [ ] Two-VM harness, every scenario: `SRC_REF=<release ref> ./scenarios/test_two_vm_backup.sh` (A–M, L1, R).
      Record the result (pass/fail counts, date, OpenZFS version) in `docs/two_vm_backup_report.md`.
- [ ] `scenarios/test_config_permissions.sh` (the group-operator permission model).
- [ ] Install scenario on fresh Ubuntu 22.04 (systemd 249, the `pre-253.conf` drop-in) and 24.04 (systemd 255):
      `scenarios/test_install.sh`, including the upgrade step from the previous release's wheel.

## 4. Package

- [ ] `poetry build`: the wheel holds `zfsbackup/` and `libzfseasy/` (with
      `zfsbackup/config/store/migrations/versions/*.py`) and no `packaging/` or `tests/`; the sdist holds
      `packaging/` (units, sysusers, tmpfiles).
- [ ] `zfsbackupd --version` and `zfsbackup-config --version` from a clean venv print the new version, the wire
      protocol and the schema head.
- [ ] Tag the release commit and attach the wheel and sdist.

## 5. After the release

- [ ] Upgrade one real host per role following the runbook in `zfsbackup/README.md` ("Upgrading and
      downgrading"): `zfsbackup-config backup DIR` first, then stop, install, start; confirm `zfsbackup-config
      check` is OK on both sides afterwards.
