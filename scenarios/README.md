# scenarios/

Shell-based **real-world** test scenarios for `libzfseasy` + `zfsbackup`. These drive real ZFS
pools and, in one case, real VMs — they are slow, side-effectful, and require ZFS (and Multipass
for the two-VM harness). Run them from the **repository root**.

The mocked/`real_zfs` **pytest** suites live under [`tests/`](../tests/), not here.

## Scripts

| Script | What it does |
|---|---|
| [`setup_test_pool.sh`](setup_test_pool.sh) | Create a file-backed test pool and delegate `zfs`/`zpool` permissions to the current user (env: `TEST_ZFS_POOL`, `ZFS_TEST_DISK`, `ZFS_TEST_SIZE`). |
| [`cleanup_test_pool.sh`](cleanup_test_pool.sh) | Destroy the pool created by `setup_test_pool.sh` and remove its backing image. |
| [`create_test_pool.sh`](create_test_pool.sh) | Create a temp-file pool with nested datasets (`mountpoint=none`) matching [`../zfsbackup/config.example.yaml`](../zfsbackup/config.example.yaml). |
| [`test_pool.sh`](test_pool.sh) | Ad-hoc scratch pool with a nested layout for manual experimentation. |
| [`test_two_vm_backup.sh`](test_two_vm_backup.sh) | End-to-end two-VM (Multipass) harness exercising the real network transfer path — scenarios A (full) / B (incremental) / C (server-down resilience) / D (client-restart persistence). Flags: `--keep`, `--keep-pools`, `--purge`. |
| [`test_config_permissions.sh`](test_config_permissions.sh) | One Multipass VM, users `zfsbackup` (daemon) + `alice`/`bob` (group operators): proves the SQLite config store's group-operator permission model on real Linux — root's first import (dir `2770 root:zfsbackup`, DB `0660`), operator re-imports while the daemon runs as `zfsbackup`, SIGHUP-killed first opener leaving sidecars in the service group, non-default `0600` `-c` staying `0600`, and the clear preflight error when the setgid bit is missing. Also covers item 15.2: the supervisor loads the config once (logs `Configuration generation N`, writes `<db>.daemon.pid` = `pid\ngeneration`), workers/respawns hold no `config.db` fd and keep the startup generation after a re-import, the CLI notice `running daemon pid P is on generation N and will not apply this until restarted`, a second daemon exits 1 naming the first pid, and `.daemon.pid`/`.writer.lock` are `0660` group `zfsbackup`. `ZFSB_SRC=worktree` ships the uncommitted working tree to the VM instead of `git archive HEAD`. Uses a stub `zfs`/`zpool` and `--dry-run` (the point is DB access, not ZFS ops). Flags: `--keep`, `--purge`. |

## Usage

```bash
# From the repo root
./scenarios/setup_test_pool.sh
ZFSB_REAL_ZFS=1 pytest -m real_zfs   # without the opt-in every real test is skipped
./scenarios/cleanup_test_pool.sh

# Two-VM end-to-end harness (needs Multipass on the host)
./scenarios/test_two_vm_backup.sh
```

## macOS / Multipass caveats

These bit us repeatedly; the two-VM harness already works around them, but keep them in mind when
editing these scripts:

- **Never redirect a `multipass exec`'s *host* stdout to `/dev/null`** — the exec wedges and becomes
  unkillable. Discard output *inside the guest* instead (e.g. `... -o /dev/null`, or
  `sh -c '... >/dev/null 2>&1'`).
- **Deliver config via `multipass transfer`, not heredoc-piped stdin** (`multipass exec ... tee`) —
  piping on host stdin hangs/spins.
- **A guest command that exits with status 4 hangs `multipass exec` forever** (verified: `exit 4`
  wedges, 2/3/104 do not). It bites `systemctl is-active` on a unit that no longer exists (exit 4 on
  current systemd). `test_config_permissions.sh` runs every guest command in an inner shell that remaps
  4 to 104; do the same in any new Multipass code that probes unit state.
- `multipass purge` is global (it destroys every deleted VM, not just yours); prefer
  `multipass delete --purge <vm>` for a single VM.
