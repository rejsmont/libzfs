"""The `zfsbackup-config` command-line application.

`main.py` is the Click application and its entry point, `render.py` turns a
`BackupConfig` into YAML that `BackupConfig.from_file` reads back, and
`errors.py` maps every failure to a message and an exit code. Run it as the
`zfsbackup-config` console script or as `python -m zfsbackup.cli`.

Nothing is imported here: importing the package must stay free of side
effects so that `python -m zfsbackup.cli` pays for one import of `main`.
"""
