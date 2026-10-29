# Packaging plan: two wheels and a Debian package

**Status: decided 2026-10-28, in progress on branch `packaging-split`.** Before this, `poetry build` made one
wheel, `zfsbackup`, which shipped both the `zfsbackup` and the `libzfseasy` packages; the install path was the
wheel in a virtualenv at `/opt/zfsbackup/venv` plus the units from the sdist
([zfsbackup/README.md](../zfsbackup/README.md), "Installation").

Decided:
1. **Split into two wheels**, with `libzfseasy` in a self-contained `libs/libzfseasy/` (its own `pyproject.toml`,
   README and licence) so it can later move to a repository of its own.
2. **Debian packages on distro dependencies** (strategy A below) for **Ubuntu 26.04 and Debian 13**. Debian 13
   ships OpenZFS 2.3, so 2.3 joins the tested versions.
3. **`flask-sock` is vendored** (`zfsbackup/_vendor/flask_sock.py`).
4. The `flask` and `websocket-client` floors come down to Debian 13's versions (3.1.1, 1.8.0), checked by running
   the mocked suite against each distribution's own packages.
5. **Release files only**: the `.deb`s are attached to the release like the wheels; no apt repository yet.

## 1. Two wheels

### Why

`libzfseasy` imports only the stdlib, and nothing in it imports `zfsbackup`. Shipped inside the `zfsbackup`
wheel, anyone who wants just the bindings also pulls in Flask, werkzeug, SQLAlchemy, cryptography, … and the
two console scripts. A separate `libzfseasy` wheel (no dependencies) fixes that, and gives a Debian
`python3-libzfseasy` package a dist-info of its own (section 2).

### How (verified with Poetry 2.1.1 in a scratch tree)

Poetry builds one distribution per `pyproject.toml`, and poetry-core refuses a package outside the project
directory (`from = ".."` fails with "is not in the subpath of"). So `libzfseasy` has to move into its own
project directory:

```
pyproject.toml                    # zfsbackup: [project] dependencies = ["libzfseasy==<same version>", …]
zfsbackup/…
libs/libzfseasy/pyproject.toml    # libzfseasy: no dependencies
libs/libzfseasy/libzfseasy/…      # git mv of today's libzfseasy/
```

The root `pyproject.toml` moves to PEP 621 `[project]` metadata and keeps a development-only path source:

```toml
[project]
dependencies = ["libzfseasy==1.0.0rc1", …]

[tool.poetry.dependencies]
libzfseasy = {path = "libs/libzfseasy", develop = true}
```

`poetry check` passes, `poetry lock` records `libzfseasy` as a `directory` source, `poetry install` gets an
editable `libzfseasy`, and the built `zfsbackup` wheel's METADATA says `Requires-Dist: libzfseasy (==1.0.0rc1)`
(the static `[project]` list wins; no `file://` URL leaks into the wheel). `poetry build` at the root still
builds one wheel; the second is `poetry build -C libs/libzfseasy -o ../../dist`, so a `make dist` (or a CI step) runs both.
Import names are unchanged.

### What else has to change

- **Version guard (S-8), the one code change that matters.** `version.code_signature()` stats only the
  `zfsbackup` dist-info `RECORD`. After the split, reinstalling only `libzfseasy` would count as a
  dependency-only upgrade and go undetected. The signature must cover both distributions (both `RECORD`s, each
  falling back to its own package's `__init__.py`); `installed_version()` stays the `zfsbackup` version.
- **Lockstep versions.** Two `version =` lines and the `==` pin must agree. A test should assert that, and the
  release checklist bumps all three. Lockstep with an exact pin keeps the tested combination the only
  installable one; `libzfseasy` can get its own version line later if it gains other users.
- **Builds and tests that assume one wheel.** `tests/test_zfsbackup_version.py` (`TestBuiltPackages` skips when
  it sees other than one wheel), the package job in `.github/workflows/tests.yml`, `scenarios/test_install.sh`
  (installs and upgrades `zfsbackup-*.whl`), the release checklist's "the wheel holds `zfsbackup/` and
  `libzfseasy/`".
- **Paths.** CLAUDE.md, the agent definitions' file ownership, coverage config, and the worktree tip
  (`PYTHONPATH=<wt>:<wt>/libs/libzfseasy`).
- **Runbook.** `pip install libzfseasy-….whl zfsbackup-….whl`.
- **Upgrading an existing single-wheel install (1.0.0rc1) needs `pip uninstall zfsbackup` first.** A plain
  `pip install` of the split wheels over it leaves `libzfseasy` deleted (verified in a scratch venv): pip installs
  the new `libzfseasy` distribution, then uninstalls the old `zfsbackup`, whose `RECORD` still lists the
  `libzfseasy/` files. The `.deb` path is not affected.

Rough size: a day, mostly the guard change, its tests and the install scenario.

## 2. Debian packaging

### What the distributions ship (October 2026)

Read from the `zfsb-ci` (24.04) and `zfsb-client` (26.04) VMs, and Debian's archive (`madison`):

| | pin | Ubuntu 24.04 | Ubuntu 26.04 | Debian 13 | Debian 14 (testing) |
|---|---|---|---|---|---|
| python3 | ≥ 3.11 | 3.12 | 3.14 | 3.13 | |
| flask | ^3.1.3 | 3.0.2 ✗ | 3.1.3 | 3.1.1 ✗ | 3.1.3 |
| flask-sock | ^0.7 | — ✗ | — ✗ | — ✗ | — ✗ |
| simple-websocket | 1.1.x | — ✗ | 1.1.0 | 1.1.0 | 1.1.0 |
| websocket-client | ^1.9 | 1.7.0 ✗ | 1.9.0 | 1.8.0 ✗ | 1.9.0 |
| werkzeug | 3.1.x | 3.0.1 ✗ | 3.1.5 | 3.1.3 | 3.1.9 |
| sqlalchemy | 2.0–2.1 | 1.4.50 ✗ | 2.0.45 | 2.0.40 | 2.0.52 |
| alembic | ^1.13 | 1.13.1 | 1.18.4 | 1.13.2 | 1.18.4 |
| cryptography | ≥ 43 | 41.0.7 ✗ | 46.0.5 | 43.0.0 | 49.0.0 |
| urllib3 / requests / click / pyyaml | | ok | ok | ok | ok |
| zfsutils-linux | 2.1/2.2/2.4 | 2.2.2 | 2.4.1 | 2.3.9 (contrib, best-effort) | 2.4.4 |

Notes:
- `flask-sock` is in no Debian or Ubuntu suite. It is one 102-line MIT module.
- The `flask ^3.1.3` and `websocket-client ^1.9.0` floors came from `poetry add` when the prototype was written
  (May 2026), not from a known need. Lowering them to 3.1.1 / 1.8.0 would make Debian 13 fit, but only after a
  CI job runs the suite (above all `test_zfsbackup_tls_platform.py` and `test_zfsbackup_platform_contracts.py`)
  at those versions.
- Ubuntu 22.04 has Python 3.10, below our floor; no `.deb` for it either way.

### Two ways to build it

**A. Distro dependencies (`Architecture: all`).** pybuild (`pybuild-plugin-pyproject`, poetry-core is in both
archives) installs our code under `/usr/lib/python3/dist-packages`, and `Depends:` names the distro's
`python3-*` packages with our pins as version ranges (dh_python3 drops upper bounds, so `<<` goes in
`debian/control` by hand). Works on Ubuntu 26.04 now and on Debian 13 if the floors come down; not on 24.04.
- For: the distro ships the security fixes for cryptography/OpenSSL, werkzeug, urllib3 and requests, which
  matters for a TLS-facing daemon; one small package for all architectures; builds offline (a Launchpad PPA
  works); could eventually go into Debian (`contrib`, because ZFS is).
- Against: a distro security update can patch the werkzeug/urllib3/simple-websocket internals we override.
  `autopkgtest` running the two platform-contract test files catches that on the distro's side, not ours.
  Dependency updates also arrive under a running daemon (unattended-upgrades), which the version guard does not
  see. On 26.04 `needrestart` is installed and restarts services whose Python modules changed, and a dpkg
  `interest-noawait` trigger on the dependencies' directories (→ `systemctl try-restart zfsbackup`) would make
  that independent of needrestart. A restart is safe: the stop path waits for safe points and sends resume.

**B. Bundled virtualenv (`Architecture: any`).** The `.deb` carries `/opt/zfsbackup/venv` built from
`poetry.lock` with the distro's `python3` (dh-virtualenv 1.2.2 is in both Ubuntu archives, or a plain
`debian/rules` that pip-installs pre-fetched, hash-checked wheels). Works on 24.04, 26.04 and Debian 13 with
exactly the dependency set CI tested, keeps today's paths, and the version guard sees upgrades through `RECORD`.
- Against: we own every security update of about 20 bundled packages, including the OpenSSL statically linked
  into the cryptography wheel; one build per distro release and architecture (the venv is tied to that Python
  minor version); no offline/PPA build without vendoring wheels into the source package; never acceptable for
  the Debian archive.

**Recommendation: A, for Ubuntu 26.04 and later (Debian 13 once the floors are tested), and vendor
`flask-sock`.** Nothing is deployed yet and the test VMs already run 26.04. On 24.04 the wheel-in-a-venv
runbook stays as it is; B is the fallback if a `.deb` for 24.04 turns out to be required. Vendoring
`flask-sock` as `zfsbackup/_vendor/flask_sock.py` (with its licence) removes the only missing dependency for
pip installs too; the alternative is building and hosting a `python3-flask-sock` package next to ours.

### The package, under A

One source package `zfsbackup`, version `1.0.0~rc1-1` (the `~` sorts the candidate before `1.0.0`), with
`debian/` in this repository and an own apt repository (reprepro/aptly) or a PPA:

- **Binary packages.** With the two-wheel split: `python3-libzfseasy` (`all`, depends on `python3` and
  `zfsutils-linux`; plain Debian Python library, stdlib only) and `zfsbackup` (depends on `python3-libzfseasy
  (= ${binary:Version})` and the `python3-*` list). Without the split: one `zfsbackup` package.
  (Ubuntu's unrelated `python3-libzfs` package is a different module; no clash.)
- **Paths.** Console scripts in `/usr/bin`; the units from `packaging/` with `/opt/zfsbackup/venv/bin`
  substituted by `/usr/bin` at build time, so `packaging/` stays the one source.
- **System files.** `dh_installsysusers` and `dh_installtmpfiles` from `packaging/sysusers.d` and
  `packaging/tmpfiles.d`; on Ubuntu < 25.04 the `pre-253.conf` drop-in is not needed (A does not target 24.04).
- **Service handling.** `dh_installsystemd --no-enable --no-restart-after-upgrade`:
  - first install: not enabled, so not started (`deb-systemd-invoke` skips disabled units, checked on 26.04).
    The operator imports the config, enrolls, then `systemctl enable --now zfsbackup`, as the runbook says today.
    The unit's `AssertPathExists=` stays.
  - upgrade: the new package's `preinst` stops the daemon, the files are replaced, `postinst` starts it again if
    it is enabled. `python3-libzfseasy` `Breaks: zfsbackup (<< ${binary:Version})`, so apt unpacks zfsbackup
    first; a library unpacked alone deconfigures the old zfsbackup, whose `prerm` stops the units on
    `deconfigure`. Either way the library never changes under a running daemon.
    That is the runbook's "stop, install, start", the unit's `ExecStartPre` migrates the schema (with a backup),
    and the version guard never has to drain.
  - `zfsbackup-check.timer`: same flags, enabled by the operator. The packaged `zfsbackup-check.service` writes
    to `/var/lib/prometheus/node-exporter/zfsbackup.prom` (`debian/rules` rewrites `packaging/`'s upstream
    node_exporter path to the directory Debian's `prometheus-node-exporter` reads; `Suggests:` it).
- **Purge leaves `/var/lib/zfsbackup` alone** (config DB, TLS keys, trust state, migration backups) and says so.
  Deleting a backup host's keys on `apt purge` is the wrong default.
- **Tests.** The mocked suite at build time is heavy (real child processes, forkserver); run `-m unit` there and
  honour `nocheck`. `debian/tests/` (autopkgtest): the two platform-contract files, `zfsbackupd --version`,
  `zfsbackup-config --version`, a `--test-config` against an imported example.
- **CI and scenarios.** `.github/workflows/deb.yml` builds the `.deb`s in `debian:13` and `ubuntu:26.04`
  containers, runs `lintian`, installs them and runs the autopkgtests (weekly too, so a distribution update that
  moves an overridden internal is caught). `scenarios/test_install_deb.sh` installs them on fresh Ubuntu 26.04
  and Debian 13 VMs, upgrades to a `+1` build under a running daemon, fires the dependency trigger, purges
  (state kept) and downgrades.
