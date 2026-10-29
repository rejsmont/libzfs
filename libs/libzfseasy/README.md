# libzfseasy

A Python interface to ZFS that wraps the `zfs` and `zpool` command line utilities as Python objects
(filesystems, volumes, snapshots, bookmarks, properties). Standard library only. OpenZFS 2.1, 2.2, 2.3 and 2.4
on Linux; Python 3.11 or later.

The package exports one object per supported `zfs` subcommand (`zfs.list`, `zfs.create`, `zfs.snapshot`,
`zfs.get`, `zfs.send`, ...):

```python
import libzfseasy as zfs

fs = zfs.create.filesystem('tank/data', properties={'compression': 'lz4'})
zfs.snapshot(fs, 'before-upgrade')
with zfs.timeout(30):                                   # each command: ZFSTimeoutError after 30 s
    for snap in zfs.list(roots=fs, types='snapshot'):
        print(snap.name)                                # tank/data@before-upgrade
print(zfs.get.value(fs, 'compression'))                 # lz4
```

`-H` output is parsed fail-closed: rows that cannot be trusted are skipped with a WARNING on the `libzfseasy`
logger, or raise with `strict=True`. `ZFS_CMD` and `ZPOOL_CMD` override the paths of the `zfs` and `zpool`
binaries (they are resolved when the module is imported).

libzfseasy is developed in the [zfsbackup](../../README.md) repository, which also holds its tests, and is
released in lockstep with it.
