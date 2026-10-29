# zfsbackup

Automated ZFS snapshots, retention and remote backup, and the Python ZFS bindings they are built on.

This repository builds two Python distributions, released together with the same version:

- **`libzfseasy`** ([libs/libzfseasy/](libs/libzfseasy/README.md)): a Python interface to ZFS that wraps the `zfs`
  and `zpool` command line utilities as Python objects (filesystems, volumes, snapshots, bookmarks, properties). Its
  `-H` output is parsed fail-closed, and a command can be bounded with `libzfseasy.timeout(seconds)`. Standard
  library only.
- **`zfsbackup`**: a snapshot, retention and remote-backup daemon (`zfsbackupd`) and its configuration and operations
  CLI (`zfsbackup-config`). Configuration lives in a SQLite database; remote backups go over HTTPS with mutual TLS.
  Install, configuration, upgrade, restore and monitoring are in [zfsbackup/README.md](zfsbackup/README.md).

OpenZFS 2.1, 2.2, 2.3 and 2.4 are tested, on Python 3.11 to 3.15. Production use is Linux only; macOS is supported
for development. Releases carry the wheels and Debian packages for Debian 13 and Ubuntu 26.04 (`debian/`), which
depend on the distribution's own Python libraries.

## libzfseasy in brief

The package exports one object per supported `zfs` subcommand (`zfs.list`, `zfs.create`, `zfs.snapshot`, `zfs.get`,
`zfs.send`, ...):

```python
import libzfseasy as zfs

fs = zfs.create.filesystem('tank/data', properties={'compression': 'lz4'})
zfs.snapshot(fs, 'before-upgrade')
with zfs.timeout(30):                                   # each command: ZFSTimeoutError after 30 s
    for snap in zfs.list(roots=fs, types='snapshot'):
        print(snap.name)                                # tank/data@before-upgrade
print(zfs.get.value(fs, 'compression'))                 # lz4
```

`ZFS_CMD` and `ZPOOL_CMD` override the paths of the `zfs` and `zpool` binaries.

## Development

```bash
poetry install --with dev                       # zfsbackup, with libs/libzfseasy installed editable
poetry build                                    # dist/zfsbackup-*.whl and sdist
poetry build -C libs/libzfseasy -o ../../dist   # the same dist/ (-o is relative to -C)
```

The `zfsbackupd` and `zfsbackup-config` entry points are then in the Poetry environment; `python -m zfsbackup.daemon`
and `python -m zfsbackup.cli` work as well.

## Testing

There are three tiers:

1. **Unit tests** (`-m unit`) and
2. **integration tests** (`-m integration`): mocked, no pool needed; a bare `pytest` runs both. The suite points
   `zfs`/`zpool` at a stub, so it never reaches the host's real ZFS.
3. **Real ZFS tests** (`-m real_zfs`): real `zfs` commands. This tier is opt-in: without `ZFSB_REAL_ZFS=1`, every
   test in it is skipped.

```bash
pytest                               # the mocked tiers (real_zfs is deselected)
pytest -m unit                       # or: pytest -m integration
ZFSB_REAL_ZFS=1 pytest -m real_zfs   # the real tier
```

The real tier creates its own throwaway file-backed pool (`zfsbackup_pytest_<pid>`) with `sudo -n zpool create`, so
it needs ZFS and passwordless `sudo` for `zpool`/`zfs` (or `MULTIPASS_VM=<vm>` to put the pool in a Multipass VM). It
destroys the pool at the end. Set `ZFSB_REAL_ZFS=1` for each command; never export it in a shared shell profile.

See [tests/README.md](tests/README.md) for the details. The shell scenarios under [scenarios/](scenarios/README.md)
drive real pools and disposable Multipass VMs (end-to-end two-host backups, the OpenZFS version matrix, the systemd
install).

## Documentation

- [zfsbackup/README.md](zfsbackup/README.md): the zfsbackup runbook
- [CHANGELOG.md](CHANGELOG.md): release notes
- [tests/README.md](tests/README.md) and [scenarios/README.md](scenarios/README.md): testing
- [docs/](docs/): design plans (config store and CLI, TLS), the production-readiness plan and report, the release
  checklist and the two-VM test report
- [CLAUDE.md](CLAUDE.md): architecture notes for contributors
