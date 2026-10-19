# ZFS Backup Daemon

An automated ZFS snapshot management daemon with configurable frequency and retention policies.

## Features

- 🕐 **Automated Snapshots**: Take snapshots at configurable intervals (minutes, hours, days)
- 📋 **Retention Policies**: Define tiered retention rules based on snapshot age
- 🔄 **Recursive Snapshots**: Support for recursive dataset snapshots
- 🗑️ **Automatic Cleanup**: Automatically purge expired snapshots based on retention rules
- 🔍 **Dry Run Mode**: Test configuration without making changes
- 📊 **Detailed Logging**: Comprehensive logging of all operations
- ⚙️ **SQLite Config Store**: Config lives in a SQLite database; write it as YAML and import it

## Installation

The daemon requires:
- Python 3.11+
- ZFS utilities (zfs, zpool)
- The `libzfseasy` library (included in this project)

Install dependencies:
```bash
poetry install
```

## Configuration

The daemon reads its configuration from a SQLite database (default `/var/lib/zfsbackup/config.db`,
overridable with `-c` or `$ZFSBACKUP_CONFIG`). It does not read YAML and never creates the database.
Write the configuration as YAML:

```yaml
snapshot_prefix: "autosnap"
check_interval: "5m"
dry_run: false

datasets:
  - name: "tank/data"
    enabled: true
    frequency: "1h"
    recursive: false
    retention:
      "1d": "1M"    # Keep daily snapshots for 1 month
      "1w": "3M"    # Keep weekly snapshots for 3 months
      "1M": "1y"    # Keep monthly snapshots for 1 year
```

See [config.example.yaml](config.example.yaml) for more examples.

Then import it. Import is a **full replace** of whatever the database already holds:

```bash
zfsbackup-config import /path/to/config.yaml
```

The outgoing configuration is saved next to the database as `config.db.pre-import-<UTC>.yaml` first
(`--no-backup` skips it). `zfsbackup-config export` prints the stored configuration as YAML, and
`zfsbackup-config migrate` upgrades the database schema after a package upgrade.

**A running daemon picks up configuration changes on its own.** After every committed change,
`zfsbackup-config` asks the daemon to reload. The daemon also checks the database every 10 seconds, so a
change always arrives within about 10 s even when no signal is sent. What happens next:
- **Reload is checked first.** The daemon reads the new configuration, validates it and builds its dataset
  list before switching. If anything is wrong, it **rejects** that configuration, logs why, and keeps
  running the previous one.
- **Nothing is interrupted.** Each worker switches at its next safe point, between datasets or between
  destinations. A transfer in progress always finishes first. The daemon logs a warning if a switch is
  still waiting after a minute.
- **`--dry-run` stays in force** across reloads.

After a change, `zfsbackup-config` reports what the daemon did. It waits up to 3 s for an answer, which
you can change with the global option `--reload-wait SECONDS`; it goes before the subcommand, and 0
means don't wait. The answer is one of:
- the daemon is now on the new generation;
- it accepted it and is switching its workers over;
- it rejected it (a warning, with a pointer to the daemon log);
- it deferred it: not applied yet, with the reason (the new API address is in use). The daemon
  re-checks it every poll (10 s) and applies it as soon as it passes, so this is not a rejection and the
  command still exits 0;
- no daemon is running.

`zfsbackup-config status` shows the stored generation and the generation the daemon runs, accepted,
rejected or deferred (with the reason). `status --wait SECONDS` is meant for scripts. It exits:
- 0 once the daemon is in sync, or when no daemon is running;
- 1 if the daemon rejected the stored configuration, or will never catch up;
- 75 on timeout, including a configuration that is still deferred at the deadline (never 1: a deferred
  configuration is re-checked every poll and may still be applied);
- 78 if the database is unusable or its schema is out of date.

**When the CLI does not signal.** It only signals a process it can prove is the daemon:
- that process holds the daemon lock on that exact file;
- it is the process that wrote the pid file, checked by pid and start time, and it is not a zombie;
- the pid file is owned by root, by you, or by the daemon's own user.

That proof needs Linux's `/proc/locks`, so on macOS and other systems the CLI never signals. It also skips
the signal when you are not allowed to signal the daemon, for example a group operator and a daemon
running as another user. In every one of these cases the 10 s check applies the change, and the CLI says
so. You can still send `SIGHUP` yourself (`systemctl reload zfsbackup`, see below).

**Restoring the database.** To go back to an earlier configuration while a daemon or an editor is running,
use `zfsbackup-config import` (the `pre-import-*.yaml` backups are made for this). Don't copy an old
database file into place: a restored copy can carry the same identity and generation as a later edit, and
then the change can go unnoticed until the next write.

Besides the database (and its `-wal`/`-shm` files while it is open) you will see three more files. All
are tiny, all are safe to leave in place, and none is configuration state:
- `config.db.writer.lock` serialises `zfsbackup-config` writers.
- `config.db.daemon.pid` marks a running daemon and records the generation it has applied, is applying,
  rejected (final) or deferred (refused for now, re-checked every poll, with a one-line reason). One left behind by a crash is harmless.
- `config.db.workers.lock` is held by every worker. A newly started daemon waits, with a warning, while
  workers of a previous daemon are still finishing.

If `zfsbackup-config` exits with status **75**, another write was in progress or the configuration
changed (or the database was replaced) underneath it: just run the command again.

### Changing the configuration

```bash
zfsbackup-config list datasets            # or: list destinations; --json for scripts
zfsbackup-config get  'datasets[tank/data].retention'
zfsbackup-config set  'datasets[tank/data].frequency' 30m
zfsbackup-config set  'datasets[tank/data].retention.1w' 3M     # add/replace one rule
zfsbackup-config unset prune_interval                           # back to "derived from check_interval"
zfsbackup-config edit                                           # whole config in $EDITOR
zfsbackup-config edit dataset tank/data                         # one dataset
zfsbackup-config rename dataset tank/data tank/projects
zfsbackup-config rename destination offsite dr
```

Values are read exactly as in the YAML file (`30m`, `true`, `forever`). Dataset names may contain dots;
the bracket form `datasets[tank/vm.01].frequency` always works. A rejected change writes nothing.

`edit` never saves a buffer it can't validate; it offers to retry, and keeps your buffer (and prints its
path) if you abort, the editor fails, or another change got there first (exit 75).

**Renaming.** Renaming a dataset in the configuration does **not** rename the ZFS dataset. If the dataset
sends backups to a remote, `rename dataset` needs `--force`: the server stores it under a path derived
from the name, so the next send is likely a full one. `--clear-stale-props` removes the old dataset's
`org.zfsbackup:*` properties. It is refused up front when the new dataset doesn't exist in ZFS yet (if
you are about to `zfs rename`, those properties follow it). If a daemon is running, the rename itself is
still saved but the properties are left alone: the command prints the `zfs inherit` commands to run
after stopping the daemon and exits 1. To rename the ZFS dataset and the configuration together, use
`zfsbackup-config rename dataset` (it handles the properties). If you renamed the ZFS dataset by hand with
`zfs rename`, run `systemctl restart zfsbackup`: the `org.zfsbackup:config` property is synced only when
the snapshot worker starts or the configuration changes, so `systemctl reload` with an unchanged
configuration does not re-sync it.

`rename destination` and `--clear-stale-props` change ZFS user properties, so they need ZFS write
rights on the datasets involved: run them as root, or grant them with
`zfs allow <user> userprop <dataset>`. Without those rights:
- `rename dataset --clear-stale-props` still saves the rename (the configuration is written first),
  then reports that clearing the old properties failed and prints the `zfs inherit` commands to run
  with the right privileges;
- `rename destination` fails on the first dataset it cannot write and saves nothing; anchors already
  copied to earlier datasets stay, and the exact `zfs set`/`zfs inherit` commands to undo them are
  printed.

`rename destination` copies each dataset's `org.zfsbackup:anchor.<old>` to `anchor.<new>` before saving.
If `anchor.<new>` already exists with a different value (left over from an earlier destination of that
name), it is overwritten and both values are printed; the snapshot it used to name is no longer pinned
by it. The daemon protects the snapshot named by *every* local or received anchor property, so keep the
old anchor until the first send to the new name has succeeded, then clear it with the `zfs inherit`
commands the CLI prints. The running daemon reloads after a rename like after any other change. Both
renames support `--dry-run`.

## Usage

### Start the Daemon

```bash
# Using the default database location
python -m zfsbackup.daemon

# With a custom database
python -m zfsbackup.daemon -c /path/to/config.db

# Dry run mode (no changes made)
python -m zfsbackup.daemon --dry-run

# Verbose logging
python -m zfsbackup.daemon -v
```

### Test Configuration

```bash
python -m zfsbackup.daemon --test-config -c /path/to/config.db
```

### Command Line Options

- `-c, --config PATH`: Path to the config database (default: `$ZFSBACKUP_CONFIG`, then `/var/lib/zfsbackup/config.db`)
- `-d, --dry-run`: Dry run mode - do not create or destroy snapshots
- `-v, --verbose`: Enable verbose (debug) logging
- `--test-config`: Test configuration and exit

## Configuration Reference

### Global Settings

- `snapshot_prefix`: Prefix for all managed snapshots (default: "autosnap")
- `check_interval`: How often to check if snapshots are needed (e.g., "5m", "1h")
- `prune_interval`: How often the pruning worker applies retention policy. **Omit to follow
  `check_interval`** — the value is derived when the daemon reads its config, not frozen when the
  config is written, so a later change to `check_interval` carries over.
- `client_id_file`: Where this client's generated identity is stored. **Omit to derive**
  `~/.config/zfsbackup/client_id` — resolved in the daemon's own `$HOME`, not that of whoever wrote
  the config. Set it explicitly whenever the daemon and the config-managing user differ (systemd
  units, `sudo`), because a wrong path makes the daemon generate a *new* identity and lose track of
  its existing server-side backups.
- `dry_run`: Set to `true` to test without making changes (default: false)

### Dataset Configuration

Each dataset can have:

- `name`: ZFS dataset path (required)
- `enabled`: Whether this dataset should be backed up (default: true)
- `frequency`: How often to take snapshots (default: "1h")
- `recursive`: Whether to snapshot child datasets (default: false)
- `retention`: Dictionary of retention rules

### Retention Rules

Retention rules define how long to keep snapshots based on their age:

```yaml
retention:
  "1d": "1M"    # Snapshots 1 day old: keep for 1 month
  "1w": "3M"    # Snapshots 1 week old: keep for 3 months
  "1M": "10y"   # Snapshots 1 month old: keep for 10 years
```

**How it works:**
- The daemon evaluates each snapshot's age
- Applies the appropriate retention rule
- Deletes snapshots when: `snapshot_age + keep_duration > now`

### Time Format

- `h` = hours (e.g., "1h", "12h")
- `d` = days (e.g., "1d", "7d")
- `w` = weeks (e.g., "1w", "2w")
- `M` = months (e.g., "1M", "6M") - approximate as 30 days
- `y` = years (e.g., "1y", "10y") - approximate as 365 days

## Snapshot Naming

Snapshots are created with the format:
```
{prefix}_{YYYYMMDD}_{HHMMSS}
```

Example: `autosnap_20231215_143022`

This naming convention allows the daemon to:
- Identify managed snapshots
- Parse creation timestamps
- Calculate snapshot age for retention policies

## Example Use Cases

### 1. Frequent Snapshots with Tiered Retention

For important data that changes frequently:

```yaml
- name: "tank/database"
  frequency: "15m"
  retention:
    "1h": "1d"     # Hourly granularity for 1 day
    "1d": "1w"     # Daily granularity for 1 week
    "1w": "1M"     # Weekly granularity for 1 month
    "1M": "1y"     # Monthly granularity for 1 year
```

### 2. Simple Daily Backups

For less critical data:

```yaml
- name: "tank/documents"
  frequency: "1d"
  retention:
    "1w": "1M"     # Keep for 1 month
    "1M": "1y"     # Then keep monthly for 1 year
```

### 3. Long-Term Archives

For data requiring long retention:

```yaml
- name: "tank/archives"
  frequency: "1w"
  retention:
    "1M": "10y"    # Keep monthly snapshots for 10 years
```

## Running as a Service

### systemd Service

Create `/etc/systemd/system/zfsbackup.service`:

```ini
[Unit]
Description=ZFS Backup Daemon
After=zfs-import.target

[Service]
# systemd >= 253: the supervisor reports READY after its workers are forked and
# a reload as RELOADING -> READY. systemd sends the reload SIGHUP itself, so
# there is NO ExecReload= line (with one, `systemctl reload` would signal twice).
# systemd < 253 does not know notify-reload: see the variant below.
Type=notify-reload
# Only the supervisor talks to systemd. Workers never see $NOTIFY_SOCKET (the
# supervisor removes it from its environment before the first fork), so "main"
# is exact and anything else is ignored.
NotifyAccess=main
ExecStart=/usr/bin/python3 -m zfsbackup.daemon -c /var/lib/zfsbackup/config.db
# On `systemctl stop`, SIGTERM only the supervisor. It raises its stop flag;
# workers leave at their next safe point, and the API worker gives in-flight
# receives up to 20 s. The default (control-group) would SIGTERM every worker
# directly and cut a `zfs send` mid-stream.
KillMode=mixed
# The supervisor waits up to 30 s for workers, then escalates; a realistic stop
# takes well under a minute. 90 s leaves room for a stop that lands during a
# reload (a database read, at most 5 s). The supervisor itself makes no `zfs`
# call, so a hung pool cannot stall the stop; a worker stuck in `zfs` is
# escalated (SIGTERM, then SIGKILL) inside the same 30 s budget.
TimeoutStopSec=90
Restart=on-failure
# A daemon killed by a stray SIGHUP should be restarted, not treated as a clean exit.
RestartForceExitStatus=SIGHUP
RestartSec=60

[Install]
WantedBy=multi-user.target
```

**systemd older than 253** (check with `systemctl --version`): replace the `Type=` line and add the
reload command; everything else stays.

```ini
Type=notify
ExecReload=/bin/kill -HUP $MAINPID
```

What the daemon tells systemd (`zfsbackup/sdnotify.py`, stdlib only; with no `$NOTIFY_SOCKET`, e.g. a
`Type=simple` unit or a shell, all of it is a no-op):

- `READY=1` once the workers have been forked. The workers' own first act, the dataset check, is not
  waited for, so a bad pool shows up in the log, not as a failed start. `STATUS=` carries the running
  configuration generation (`systemctl status`).
- On `systemctl reload`: `RELOADING=1` (with `MONOTONIC_USEC=`), then `READY=1` on every outcome. READY
  means the reload was **decided** (accepted, rejected, deferred, or nothing changed), not that every worker has
  switched over: a worker finishing a long transfer is never interrupted for a reload, and waiting for it
  would hang `systemctl reload`. A rejected or deferred configuration is therefore still a "successful" reload
  as far as systemd is concerned; read the log, or `zfsbackup-config status`, for the outcome (`STATUS=` also
  names a rejected generation, or "generation N deferred"). A configuration change picked up by the 10 s poll only updates `STATUS=`.
- `STOPPING=1` as soon as a stop begins.
- `EXTEND_TIMEOUT_USEC=` (30 s, every second) while a start is waiting out the workers of a previous
  daemon, so `TimeoutStartSec` does not kill a start that is legitimately waiting for a transfer. A hung
  start that stops sending still times out 30 s after its last message. Each wait extends for at most
  one hour: a leftover worker stuck in uninterruptible sleep (a hung `zfs`) holds
  `<db>.workers.lock` for ever, and would otherwise keep the unit "activating" indefinitely. After the hour
  the daemon logs one ERROR ("no longer extending the systemd start timeout") and keeps waiting, but
  sends no more extensions, so systemd fails the start about 30 s after the last one. Find the stuck process with
  `fuser <db>.workers.lock`.

Watchdog (`WatchdogSec=`) is not implemented; do not set it.

**If the supervisor crashes** (OOM, `kill -9`), its workers finish their current dataset or transfer and
exit on their own, and a newly started daemon waits for them (the `workers.lock`). With
`KillMode=mixed`, though, systemd kills the remaining workers as soon as the supervisor is gone, so a
transfer in progress is cut. It resumes from where it stopped (`zfs receive -s`) and nothing is lost, but
the progress since the last resume point has to be sent again. If long transfers matter more to you than
a clean cgroup, use `KillMode=process` instead: survivors then finish their transfer, at the cost of
systemd not reaping them for you.

Enable and start:
```bash
sudo systemctl enable zfsbackup
sudo systemctl start zfsbackup
sudo systemctl status zfsbackup
sudo systemctl reload zfsbackup   # re-read the configuration now (it also does so every 10 s)
```

## Logging

The daemon logs to stdout by default. Key log messages include:
- Snapshot creation and deletion
- Retention policy application
- Errors and warnings
- Daemon lifecycle events

Use `-v` flag for debug-level logging.

## Safety Features

- **Dry Run Mode**: Test configuration without making changes
- **Prefix Filtering**: Only manages snapshots with the configured prefix
- **Graceful Shutdown**: Handles SIGINT/SIGTERM signals; SIGHUP reloads the configuration
- **Error Handling**: Continues operation even if individual datasets fail
- **Validation**: Validates configuration on startup and before every reload (a rejected reload keeps the previous configuration). An API address that is *in use* at reload (`EADDRINUSE`) is *deferred*, not rejected: the daemon keeps running the previous configuration, publishes `deferred=` and `deferred_reason=` in the pid file (`zfsbackup-config status` says `deferred`), re-checks it at every 10 s poll and applies it as soon as it passes, without a new write. An address that cannot be bound for any other reason (not an address of this host, a privileged port, a name that does not resolve, a malformed host) is rejected for good, because retrying would only repeat the failure (and re-resolve a dead name on every poll). A client id problem is only a warning (the file is read with a size cap, and a FIFO, device or directory is reported as unusable, never read); local snapshots and pruning keep running. At startup, however, an unbindable API address is still a hard refusal (exit 1), so restarting the daemon while the port is held keeps it down until the port is free

## Troubleshooting

### Snapshots not being created

1. Check dataset exists: `zfs list tank/data`
2. Check permissions: Run with appropriate ZFS permissions
3. Enable verbose logging: `python -m zfsbackup.daemon -v`
4. Test config: `python -m zfsbackup.daemon --test-config`

### Snapshots not being deleted

1. Check retention rules are correctly configured
2. Verify snapshot ages with: `zfs list -t snapshot -o name,creation`
3. Run in dry-run mode to see what would be deleted
4. Check logs for errors

### Configuration errors

```bash
# Test configuration
python -m zfsbackup.daemon --test-config -c /path/to/config.db
```

## License

See the main project LICENSE file.

## Contributing

Contributions are welcome! Please ensure:
- Code follows existing style
- Add tests for new features
- Update documentation
