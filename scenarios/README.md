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

## Usage

```bash
# From the repo root
./scenarios/setup_test_pool.sh
pytest -m real_zfs
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
