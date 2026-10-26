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
- Python 3.11 or newer (3.11 to 3.14 are supported)
- OpenZFS utilities (`zfs`, `zpool`); 2.1, 2.2 and 2.4 are the tested versions
- systemd 249 or newer, if you run it as a service (below 253 needs a drop-in, see below)
- a clock kept right by NTP on every client and server (see [Clocks](#clocks))

zfsbackup is one Python distribution, `zfsbackup`, that ships two packages: the daemon and CLI
(`zfsbackup`) and the ZFS bindings (`libzfseasy`). Install one wheel per host. It provides two commands:
`zfsbackupd` (the daemon) and `zfsbackup-config` (the configuration, enrollment and operations CLI).

### Install from the wheel and the sdist

Build the two artifacts with `poetry build` (or take them from a release): the **wheel** is the program, the
**sdist** also carries the `packaging/` directory (systemd units, `sysusers.d`, `tmpfiles.d`), which is never
installed by pip.

```bash
# 1. The program, in its own virtualenv. The units expect exactly this path.
sudo apt install python3-venv                       # Debian/Ubuntu
sudo python3 -m venv /opt/zfsbackup/venv
sudo /opt/zfsbackup/venv/bin/pip install ./zfsbackup-0.1.0-py3-none-any.whl
sudo ln -s /opt/zfsbackup/venv/bin/zfsbackup-config /usr/local/bin/zfsbackup-config   # both commands on PATH:
sudo ln -s /opt/zfsbackup/venv/bin/zfsbackupd /usr/local/bin/zfsbackupd               # the README uses them bare

# 2. The system files, from the sdist.
tar xzf zfsbackup-0.1.0.tar.gz && cd zfsbackup-0.1.0
sudo install -D -m 0644 packaging/sysusers.d/zfsbackup.conf /etc/sysusers.d/zfsbackup.conf
sudo install -D -m 0644 packaging/tmpfiles.d/zfsbackup.conf /etc/tmpfiles.d/zfsbackup.conf
sudo install -D -m 0644 packaging/systemd/zfsbackup.service /etc/systemd/system/zfsbackup.service
sudo systemd-sysusers                               # creates the group `zfsbackup` (first!)
sudo systemd-tmpfiles --create zfsbackup.conf       # /var/lib/zfsbackup (2770) and its backups/ (0700 root)
sudo systemctl daemon-reload
```

Notes:
- **Python on older distributions.** Ubuntu 22.04's `python3` is 3.10, which is too old. Use a newer
  interpreter for the venv, either `deadsnakes` (`apt install python3.12 python3.12-venv`, then
  `python3.12 -m venv …`) or [uv](https://docs.astral.sh/uv/):
  `UV_PYTHON_INSTALL_DIR=/opt/zfsbackup/python uv venv --python 3.12 /opt/zfsbackup/venv` and
  `uv pip install --python /opt/zfsbackup/venv/bin/python ./zfsbackup-….whl` (a `uv venv` has no `pip`; use
  `uv pip`, or run `python -m ensurepip` in it first, for this and for upgrades). Keep the interpreter where
  root can reach it at boot (not under a user's home directory).
- **Upgrading an old venv.** A venv from before the distribution was renamed holds a `libzfseasy` distribution;
  installing `zfsbackup` into it leaves both, and `pip uninstall libzfseasy` would delete files that
  `zfsbackup` now owns. Make a fresh venv instead (a new install is not affected).
- **systemd older than 253** (`systemctl --version`; Ubuntu 22.04 has 249) does not know
  `Type=notify-reload`. Also install the drop-in, and do **not** install it on newer systemd:
  ```bash
  sudo install -D -m 0644 packaging/systemd/zfsbackup.service.d/pre-253.conf /etc/systemd/system/zfsbackup.service.d/pre-253.conf
  sudo systemctl daemon-reload
  ```
  The unit then logs "Failed to parse service type, ignoring: notify-reload" at load, which is expected: the
  drop-in sets the type that works. It changes nothing else (`RestartForceExitStatus=` stays).
- **The group.** `sysusers` creates the group `zfsbackup` and no user: the daemon runs as root. People who
  run `zfsbackup-config` without being root join the group (`sudo usermod -aG zfsbackup alice`); they can edit
  the configuration but cannot read the TLS private keys. `/var/lib/zfsbackup` is `2770 root:zfsbackup`
  (setgid, so the database's `-wal`/`-shm` files get the group whoever creates them).
- **Development install.** From a checkout, `poetry install` and run `poetry run zfsbackup-config …` /
  `poetry run zfsbackupd …`. `python -m zfsbackup.daemon` also works.

### First run

The order matters, because the unit refuses to start without a database (`AssertPathExists=`):

1. **Write the configuration and import it** (see [Configuration](#configuration)):
   `sudo zfsbackup-config import /root/config.yaml`. This creates `/var/lib/zfsbackup/config.db`.
2. **Check it** without starting anything: `sudo /opt/zfsbackup/venv/bin/zfsbackupd --test-config`.
3. **Remote backups, if you use them** (see [TLS and enrollment](#tls-and-enrollment)): on the server
   `trust token create`; on each client `enroll DESTINATION --url … --token-file …`.
4. **Start it and keep it enabled:** `sudo systemctl enable --now zfsbackup`, then `systemctl status zfsbackup`
   and `journalctl -u zfsbackup`.
5. **Look at the health report** (see [Monitoring](#monitoring)): `zfsbackup-config check`.

On the **server**, also set a quota per client once its first backup has created `<target>/<client_id>`
(`zfs set quota=2T tank/backups/<client_id>`): neither the daemon nor the API limits how much a client may
send, and sends are not bandwidth limited either.

### Clocks

Snapshot names carry UTC time and retention, lag and the "clock behind" check all read it, so every host
needs NTP (`timedatectl` should say "System clock synchronized: yes"). A snapshot dated more than
`max(5 minutes, check_interval)` in the future is treated as a clock problem: it is logged once at ERROR,
the dataset is treated as due, and `check` reports CRIT. TLS certificates are checked against the clock too:
a server refusing a client certificate as expired or not yet valid logs one WARNING per client address per
hour (`API: TLS client … refused: …`), and a client whose own certificate expires within 30 days, or has
expired, logs a WARNING (an ERROR once expired) at startup, on every reload and once a day. A wrong clock on
either side shows up as these messages; fix the clock before rotating or re-enrolling.

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
`zfsbackup-config migrate` upgrades the database schema after a package upgrade (the service does it
for you, see [Upgrading](#upgrading-and-downgrading)).

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
rejected or deferred (with the reason). Once the workers have written their status files it also lists,
per worker, when it last updated, how many of its records are failing and the worst failure's last error
(`--json`: a `workers` key; absent when there is no data; it never changes the verdict or the exit code).
`status --wait SECONDS` is meant for scripts. It exits:
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
database file into place while the daemon runs: a restored copy can carry the same identity and generation
as a later edit, and then the change can go unnoticed until the next write. To restore the whole host
(database, identity and trust) from `zfsbackup-config backup`, with the daemon stopped, see
[Restoring a configuration backup](#restoring-a-configuration-backup).

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

## TLS and enrollment

Remote backup is HTTPS only, with mutual authentication. Both sides pin each other's certificate by its
SHA-256 fingerprint; there is no certificate authority, no host name check and no plaintext mode. The
server accepts TLS 1.3 only, and every endpoint except `/health` and `/enroll` requires a client
certificate the server has pinned. A client is known to the server by its **client id**, and the server
binds the id to the fingerprints of the certificates enrolled for it; it never takes the id from a
request. A request for another id's datasets is refused with 403.

**Where the identities live.** `<db>.tls/` (next to the config database; mode 0750, the database file's
group) holds `server.crt`/`server.key`, `client.crt`/`client.key`, a pending `client.next.crt`/
`client.next.key` during a rotation or `enroll --new-key`, and `identity.lock`. Keys are mode 0600 and
never stored in the database; certificates are 0640. The certificates are generated (ECDSA P-256, valid ten
years). Run `enroll`, `identity rotate` and `trust token create` as root, or as the user that owns the
directory, and run the daemon as the same user so it can read the keys. A group operator cannot read a
key. Back up `<db>.tls/` with the database, with `zfsbackup-config backup` (see
[Backing up the host](#backing-up-the-host)): replacing the database file keeps the TLS identity but loses
the pins **and the client id**, so servers would see a new client and do a full send (restore the id with
`identity set-client-id` before enrolling again).

### Server: create a token

The server needs `remote_backup` enabled (with its `target_dataset`) and an `api_host` its clients can
reach. The daemon creates the server identity at startup if there is none; `trust token create` creates it
too, so you can enroll a client before the first start. `--dry-run` creates no TLS files: the API serves
with a throwaway identity that no client can pin.

```bash
zfsbackup-config trust token create                 # prints a token on stdout
zfsbackup-config trust token create --ttl 30m       # default 1h, at most 7d
```

The token is single use, contains the server's certificate fingerprint (which is how the client learns
what to pin) and is shown only once; only its hash is stored. A new token replaces the previous one.
`trust token show` reports its state (never the secret) and `trust token revoke` deletes it. Hand the token
to the client's operator over a channel you trust: until it is used or expires, whoever holds it can
enroll one client.

### Client: enroll

On the client host, write the token to a file readable only by root, enroll, and delete the file:

```bash
umask 077; printf '%s\n' 'zfsb1.…' > /root/offsite.token
zfsbackup-config enroll offsite --url https://backup.example.com:8080 --token-file /root/offsite.token
rm /root/offsite.token
```

`--token -` reads the token from stdin and a hidden prompt is the fallback; it is never accepted on the
command line. `--url` is required for a new destination and must be `https://`; a destination URL with `http://`, port 0 or an empty port is refused when the configuration is validated (on `import` as well). `enroll`
generates this host's client id and client certificate if it has none, connects with the server
fingerprint from the token pinned (a different server presents a different fingerprint and the token is
never sent), proves possession of the client key, and stores the destination's `url` and
`server_fingerprint` in the configuration. Running it again for an enrolled destination is safe: the
server is asked first and the token is not used. A destination without a `server_fingerprint` is not
enrolled and is skipped.

If the server answers `409 client_id_taken` (it already knows this id, or already holds a dataset for it),
the token is not spent: ask the server's operator for one made with `trust token create --client-id ID`
(see below).

### Verify

```bash
zfsbackup-config identity show          # client: the id, fingerprints, expiry; and the server certificate
zfsbackup-config list destinations      # the ENROLLED column; "not enrolled" until enroll succeeds
zfsbackup-config trust list             # on the server: client ids, label, active/revoked, fingerprints
```

`identity show` is read-only and generates nothing. Compare the client fingerprint on both sides.

### Revocation

```bash
zfsbackup-config trust revoke FINGERPRINT   # one certificate; a unique prefix of 8+ hex characters works
zfsbackup-config trust revoke --client ID   # every certificate of that client id
```

- **Permanent.** A revoked fingerprint is recorded and refused for ever on enroll and rotate, so a stolen
  key cannot be enrolled again with a fresh token. Revoking a fingerprint also revokes every certificate
  that descends from it by rotation, including successors that have already been used and retired their
  predecessor; if the fingerprint was itself retired by a rotation, `trust revoke` says so and revokes the
  successor chain. A rotation predecessor that has been retired is refused too.
- **New connections only.** The API picks a trust change up within about 2 s, with no restart or reload.
  A stream already in flight finishes; the client's data on the server is left in place.
- Trust changes never signal the daemon or bump the configuration generation.

### Rotation

`zfsbackup-config identity rotate` replaces this host's client certificate without re-enrolling: it
creates `client.next.*`, registers it with every enrolled destination (authenticated with the current
certificate), and swaps the files only if every destination accepted it. A failure on any destination
swaps nothing; fix or remove that destination and re-run (servers that already accepted answer
`already`). A server retires the old certificate on the successor's first use (or when the successor itself
rotates), and a retired certificate is refused for ever. If a destination refuses the successor, the advice
names the servers that already accepted it. With `client_cert_file`/`client_key_file` configured, give the new pair with
`--cert`/`--key`. The daemon picks up new generated files by itself. Exit status 75 means a destination
was unreachable or busy: wait and re-run. A server that has seen five failed enrollments from your address
answers HTTP 429 with `Retry-After`; the message says how long to wait (15 minutes right after the
lockout), and re-running sooner cannot succeed. A server with other enrollments in flight answers
"busy" (wait about 2 seconds).

### A lost, stolen or revoked key

1. On the server: `zfsbackup-config trust revoke FINGERPRINT` (or `--client ID`; take the id from
   `trust list`).
2. On the server: `zfsbackup-config trust token create --client-id ID --replace`. A token scoped to a known
   id can only enroll that id; `--replace` makes the new certificate the only one for it.
3. On the client: `zfsbackup-config enroll offsite --new-key --token-file FILE`. The revoked certificate
   can never be enrolled again, so `--new-key` generates a fresh key as `client.next`, enrolls it, and once
   the server accepted it makes it this host's client certificate. A pending `client.next` is reused only
   if the server at this destination already trusts it; otherwise it is replaced (a pin mismatch while
   asking aborts and discards nothing). (With `client_cert_file`/`client_key_file`
   configured, supply the new pair with `--cert`/`--key`.)

With **several destinations**, run `--new-key` **once**, against the first server. Every other server
still trusts only the old certificate and refuses this host (its destination is skipped before any send)
until it is enrolled again: its operator creates a `trust token create --client-id ID --replace` token, and
you run a plain `enroll DESTINATION --token-file FILE` for it. A `--replace` token redeemed with a
certificate the server already pins for that id also performs the replace, so a host whose key might be
stolen can drop the other certificates without waiting for a revocation.

### Changing the client id

`zfsbackup-config identity set-client-id ID` (letters, digits, `.`, `_`, `-`; at most 64; starting with a
letter or digit) changes it. Every server then sees a new client and the next remote backup is a full
send. It is refused while any destination is enrolled, because each server has this host's certificate
bound to the old id; `--force` overrides that. The better way to move to a new id is to re-enroll with a
`--client-id NEW` token. A change to the id bumps the configuration generation, so a running daemon
reloads and hands the new id to its remote worker.

### Firewall and the port

The API listens on `api_host:api_port` (default `127.0.0.1:8080`; set `api_host` to an address clients can
reach). Allow inbound TCP to that port from your clients' addresses only: `/health` and `/enroll` answer
anyone who can connect. Each source address (an IPv6 /64) is rate limited and capped at 8 connections until it
has presented a pinned client certificate, and 5 failed enrollments in 15 minutes lock it out of `/enroll`
(429); a wrong token is refused and nothing is enrolled without a valid one. Clients pin the server's own certificate and the server reads the client
certificate in the TLS handshake, so a TLS-terminating proxy in front of the API will not work. The server
is served by a pinned version of werkzeug's WSGI server, hardened against slow and hostile peers; see
[Known limitations](#known-limitations) and
[docs/production_readiness_report.md](../docs/production_readiness_report.md) for the remaining caveats.

## Usage

### Start the Daemon

`zfsbackupd` is the daemon (`python -m zfsbackup.daemon` is the same program). Under systemd you do not
start it by hand; these are for trying it out.

```bash
# Using the default database location
zfsbackupd

# With a custom database
zfsbackupd -c /path/to/config.db

# Dry run mode (no changes made)
zfsbackupd --dry-run

# Verbose logging
zfsbackupd -v

# What version is this, and which wire protocol and config schema does it speak?
zfsbackupd --version
```

### Test Configuration

```bash
zfsbackupd --test-config -c /path/to/config.db
```

### Command Line Options

- `-c, --config PATH`: Path to the config database (default: `$ZFSBACKUP_CONFIG`, then `/var/lib/zfsbackup/config.db`)
- `-d, --dry-run`: Dry run mode - do not create or destroy snapshots
- `-v, --verbose`: Enable verbose (debug) logging; same as `--log-level debug`
- `--log-level debug|info|warning|error`: Log level (default `info`; with `-v`, the finer wins)
- `--version`: Print the version, wire protocol and config schema, and exit
- `--test-config`: Test configuration and exit

## Configuration Reference

### Global Settings

- `snapshot_prefix`: Prefix for all managed snapshots (default: "autosnap")
- `check_interval`: How often to check if snapshots are needed (e.g., "5m", "1h")
- `prune_interval`: How often the pruning worker applies retention policy. **Omit to follow
  `check_interval`** — the value is derived when the daemon reads its config, not frozen when the
  config is written, so a later change to `check_interval` carries over.
- `client_cert_file`, `client_key_file`: optional absolute paths to your own client certificate and key,
  used instead of the generated pair in `<db>.tls/` (see [TLS and enrollment](#tls-and-enrollment)). Give
  both or neither.
- The **client id** is not a setting. It lives in the config database, is generated by
  `zfsbackup-config enroll`, and is shown by `zfsbackup-config identity show`. `import` and `export` never
  touch it, and the old `client_id_file` key is refused.
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

- `m` = minutes (e.g., "30m")
- `h` = hours (e.g., "1h", "12h")
- `d` = days (e.g., "1d", "7d")
- `w` = weeks (e.g., "1w", "2w")
- `M` = months (e.g., "1M", "6M") - approximate as 30 days
- `y` = years (e.g., "1y", "10y") - approximate as 365 days

## Snapshot Naming

Snapshots are created with the format:
```
{prefix}_{YYYYMMDD}{HHMMSS}
```

Example: `autosnap_20231215143022` (UTC)

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

The unit files are in the sdist's `packaging/` directory (see [Installation](#installation)); install
`packaging/systemd/zfsbackup.service` as `/etc/systemd/system/zfsbackup.service`. Read it: every setting
has a comment saying why. In short:

- **`Type=notify-reload` + `NotifyAccess=main`** (systemd 253 and later): the supervisor reports READY after
  its workers are forked and a reload as RELOADING, then READY. systemd sends the reload SIGHUP itself, so
  the unit has **no** `ExecReload=` line (with one, `systemctl reload` would signal twice). Older systemd
  uses the `pre-253.conf` drop-in (`Type=notify` plus `ExecReload=/bin/kill -HUP $MAINPID`).
- **`ExecStartPre=… migrate --backup-dir /var/lib/zfsbackup/backups`** upgrades the database schema before
  the daemon starts, keeping a copy first, and only when the schema is actually behind (see
  [Upgrading](#upgrading-and-downgrading)).
- **`After=`/`Wants=` network-online, zfs-mount and zfs-import, `RequiresMountsFor=/var/lib/zfsbackup`,
  `AssertPathExists=…/config.db`**: start after the pool is imported and mounted, and fail with a clear
  message, rather than loop, on a host that has not imported a configuration yet.
- **`KillMode=mixed`, `TimeoutStopSec=90`**: on `systemctl stop`, SIGTERM goes to the supervisor only; it
  raises its stop flag, workers leave at their next safe point, and the API worker gives in-flight receives
  up to 20 s. The default control-group kill would cut a `zfs send` mid-stream. The supervisor itself makes
  no `zfs` call, so a hung pool cannot stall the stop; a stuck worker is escalated (SIGTERM, then SIGKILL)
  inside a 30 s budget.
- **`Restart=on-failure`, `RestartForceExitStatus=SIGHUP 76`, `RestartSec=60`**: a daemon killed by a stray
  SIGHUP in the few milliseconds before it ignores the signal is restarted, not treated as a clean exit.
  Exit status 76 means "the installed zfsbackup (or its Python interpreter) changed under me, restart me":
  the daemon drains, finishing the work in hand without cutting a transfer, then exits 76 and systemd starts it
  once, after `RestartSec` (see [Upgrading](#upgrading-and-downgrading)). Nothing else exits 76.
- **No `ProtectSystem=`, `ProtectHome=`, `PrivateTmp=`** or any other sandboxing that gives the service a
  private mount namespace: the daemon runs `zfs create` and `zfs receive`, and the datasets they mount must
  appear on the host, not only inside the service's namespace. Do not add them without testing a real receive.

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

## Monitoring

`zfsbackup-config check` answers "are my backups happening?". It reads the stored configuration and ZFS
(read-only: it writes nothing, takes no lock and never signals the daemon) and grades **lag** against how
often something is expected to happen. A lag is WARN from **2x** and CRIT from **4x** the expected interval
(`--warn-factor` and `--crit-factor` change the factors):

| What | Measured | Expected interval |
|---|---|---|
| `snapshot` (client) | the newest managed snapshot of each enabled dataset | the dataset's `frequency`, but at least `check_interval` (the snapshot worker looks no more often) |
| `anchor` (client) | the snapshot last sent to each destination | the larger of the dataset's interval (as above) and the destination's `frequency` |
| `received` (server) | the newest snapshot of each received client dataset | the largest of that received dataset's own `frequency` and its remote frequencies (as the client sent them) |

Also: a dataset with **no managed snapshot** is CRIT; a destination that has **not been sent to yet** is WARN
until the dataset's oldest snapshot is itself older than the CRIT threshold, then CRIT; a snapshot dated
in the future beyond the tolerance is CRIT ("clock behind newest snapshot"); `dry_run` is a WARN; a dataset
`zfs` could not list is CRIT, and so is a server whose `target_dataset` cannot be listed or does not exist. A
received dataset with no managed snapshot stays WARN (there is no time reference to turn it into CRIT). A send in progress is noted, not graded separately. A raw/plain mismatch with
the server's copy shows only as lag (the client logs the reason once).

`check` adds entries of its own:

- **`daemon/process`**: CRIT when no daemon is running or the supervisor is gone (its workers are leftovers);
  otherwise OK. WARN if the pid file cannot be read at all, because then it cannot tell.
- **`daemon/config_applied`**: OK when the daemon runs the stored configuration (or is switching its workers
  to it); WARN when it is behind, has **deferred** it, **rejected** it or cannot be read. A rejected
  configuration is only a WARN because the old configuration keeps running; `zfsbackup-config status` says why.
- **`tls/client_certificate`**, only when destinations are configured: WARN when the certificate is missing
  (not enrolled), unreadable or would not be accepted, or expires within 30 days; CRIT once it has expired
  (`identity rotate`); OK otherwise.
- **`worker_snapshot`, `worker_prune`, `worker_send`** (subject the dataset, peer the destination for a send):
  the outcome of each worker's last attempts, read from the status files the daemon writes next to the
  database (`<db>.status/<worker>.json`). A failed attempt counts like one missed interval: attempts failing
  **in a row** below `--warn-factor` (default 2) are OK (a single failure is named in the reason but is not a
  verdict), from `--warn-factor` WARN, from `--crit-factor` (default 4) CRIT; a last attempt that succeeded is
  OK whatever came before. **No status files is "no data", never a failure**: a daemon that predates them, a
  worker that has nothing to report yet, or a `--dry-run` daemon (which writes none) adds no entry. A status
  file that is refused (a symlink, group- or world-writable, owned by a user the CLI does not trust, too big,
  not JSON, or a newer schema than this CLI reads) is one `worker_status` WARN naming the file. Owners trusted
  are root, the user running the CLI, the database file's owner and the owner of `<db>.daemon.pid` (the user
  the daemon runs as). These entries sit beside the lag entries: the lag says that something is late, the
  outcome says why (the last error).
  A status file that cannot be taken as current is **stale**: no daemon is running (a stopped daemon, or a
  worker a live-to-`--dry-run` reload replaced, leaves its last file behind), the writing process is gone, or
  it was not updated for `max(4 x check_interval, 24 h)` (a backstop; one long transfer can keep a worker from
  writing for hours). A stale file is one WARN `worker_status` entry ("no recent data") and its outcomes are
  not graded: never OK, never CRIT for staleness alone. `status` marks it `STALE` and uses 24 h.

```bash
zfsbackup-config check                       # text: one line per entry, then "overall: …"
zfsbackup-config check --json                # machine readable; "schema": 1, only ever extended
zfsbackup-config check --nagios              # one plugin line; exit 0 OK, 1 WARN, 2 CRIT, 3 UNKNOWN
zfsbackup-config check --prometheus          # Prometheus text format on stdout
zfsbackup-config check --prometheus --output /var/lib/node_exporter/textfile_collector/zfsbackup.prom
```

- **Exit status.** 0 when every entry is OK, 1 when any is WARN or CRIT, 78 when the configuration database is
  unusable (and the CLI's usual 2 for a usage error, 75 for busy). With `--output FILE` the exit status is 0
  as soon as the file is written, whatever the verdict (it is in the file); an unwritable path or an
  unusable database still exits non-zero.
- **`--nagios`** prints one line, `ZFSBACKUP <STATE> - <the first three entries that are not OK> | perfdata`
  (an age per lag entry, with its warn and crit thresholds), and exits 0/1/2 for OK/WARN/CRIT. **Anything that
  stops the check from running is UNKNOWN (3), never CRIT**: an unusable database, and bad arguments when
  `--nagios` is on the command line (printed as `ZFSBACKUP UNKNOWN - …`). A dataset or pool that `zfs`
  cannot read is different: it is a CRIT entry about the backups (exit 2), not UNKNOWN. It cannot be combined
  with `--json`, `--prometheus` or `--output`.
- **`--output FILE`** (or `-o`) writes the text, JSON or Prometheus form to FILE instead of stdout, and prints
  nothing. The write is atomic (a temporary file in the same directory, then a rename; mode 0644), so
  node_exporter's textfile collector never reads half a file. A check that fails to run **leaves the previous
  file in place**: alert on `zfsbackup_check_timestamp_seconds` to notice a stale one. `--output -` is stdout.
- The metric names are prefixed by what they measure and will only be added to: `zfsbackup_snapshot_*`,
  `zfsbackup_anchor_*` and `zfsbackup_received_*`, each with `_age_seconds`, `_threshold_seconds{level="warn|crit"}`
  and `_check_state` (0 OK, 1 WARN, 2 CRIT), labelled `dataset` and, where it applies, `destination` or
  `client`; other checks appear as `zfsbackup_check_state{source,subject}`; then `zfsbackup_check_overall_state`,
  `zfsbackup_check_success` (1 only when nothing is WARN or CRIT), `zfsbackup_check_timestamp_seconds` and
  `zfsbackup_build_info{version}`. The worker-outcome entries above are
  `zfsbackup_check_state{source="worker_send",subject="tank/data",peer="offsite"}` and so on, with
  `zfsbackup_worker_consecutive_failures{kind,dataset[,destination]}` (0 = the last attempt succeeded; JSON
  entries carry the same number as `consecutive_failures`, with `kind`, `last_attempt` and `last_success`;
  `--nagios` adds one perfdata item per record: failures with the two thresholds). The daemon and certificate entries above are
  `zfsbackup_check_state{source="daemon",subject="process"}` and the like. Label values are escaped as the exposition format requires. There is no
  HTTP `/metrics` endpoint.
- **A scheduled check.** The sdist ships an optional `packaging/systemd/zfsbackup-check.service` and
  `.timer` (disabled; nothing enables them). Install both, set `ZFSBACKUP_CHECK_OUTPUT` in the service (or
  `systemctl edit zfsbackup-check.service`) to your node_exporter textfile directory, and
  `systemctl enable --now zfsbackup-check.timer`. It runs every 5 minutes. With `--output`, `check` exits 0
  once the file is written, whatever the verdict, because the verdict is in the metrics; a failed unit means the
  check could not run or could not write the file (database unusable, an unwritable path, a crash), and the
  previous file stays until the next successful run.

## Upgrading and downgrading

**Recommended: stop it first** (step 3). An in-place `pip install --upgrade` under a running daemon is
detected and handled, but it is the fallback, not the plan: workers import parts of the program lazily from
disk, so for a few seconds a worker could load new code beside old.

**What happens on an in-place install.** The daemon records the installed version, the identity of the
package's `RECORD` file and the Python interpreter's binary when the process starts, and compares them every
10 s (and before it starts a worker). A change must be read twice, 10 s apart, before the daemon acts on it, so
a transient read error cannot restart a healthy daemon. It then logs one WARNING (old -> new), shows
`STATUS=` in `systemctl status` and `draining=` in the pid file, reloads no configuration (a `zfsbackup-config`
write is answered as deferred, to be applied after the restart, not re-checked) and **drains**: the remote
worker, then the API worker, which load parts of the program from disk on demand, finish what they are doing
at their next safe point (a `zfs send` or an inbound receive runs to its end, however long that takes, not cut
after the usual 30 s stop budget) and are not restarted; the API worker keeps serving until the remote worker
has left. Snapshots and pruning go on meanwhile (if one of them is found not running, the drain is cut short
with the ordinary stop). When those two have left, the daemon stops the rest the usual way and exits
with status 76; systemd restarts it **once**, `RestartSec` (60 s) later, on the new code, and `ExecStartPre`
migrates the database then. In that gap `zfsbackup-config check` reports the daemon absent. A `systemctl
stop` or SIGTERM during the drain is an ordinary stop (30 s budget, normal exit status, no restart). A daemon
that starts while the install is half done (the package files present, their metadata missing) exits 76
straight away and is retried after `RestartSec`.

**Not covered:** upgrading a dependency on its own (werkzeug, cryptography, SQLAlchemy, ...) with the
package untouched, and source edits of an editable (`pip install -e`) install: only a reinstall is seen. After
those run `systemctl restart zfsbackup`. A daemon run outside systemd exits 76 and stays down.

1. **Read the release notes** for a protocol or schema bump. `zfsbackupd --version` and
   `zfsbackup-config --version` print `<version> (wire protocol N, config schema <revision>)`; compare them
   between the client and the server.
2. **Back up**: `sudo zfsbackup-config backup DIR` (see [Backing up the host](#backing-up-the-host)). It does
   not signal or lock anything and is safe while the daemon runs. A group operator must pass a `DIR` of
   their own and `--no-keys`: `/var/lib/zfsbackup/backups` belongs to root.
3. **Stop, install, start:**
   ```bash
   sudo systemctl stop zfsbackup
   sudo /opt/zfsbackup/venv/bin/pip install --upgrade ./zfsbackup-NEW.whl
   # a venv made by uv has no pip:
   #   sudo uv pip install --python /opt/zfsbackup/venv/bin/python --upgrade ./zfsbackup-NEW.whl
   # if the release changed packaging/ (unit files): install them from the new sdist, then
   sudo systemctl daemon-reload
   sudo systemctl start zfsbackup
   ```
4. **The start migrates the database.** `ExecStartPre` runs `zfsbackup-config migrate --backup-dir
   /var/lib/zfsbackup/backups`. When (and only when) the schema is behind, it first writes
   `config.db.pre-<revision>-<UTC time>` there and migrates only if that succeeded; on a plain restart it
   writes nothing, and a restart loop writes at most one backup per distinct database state (schema
   revision, store id and generation), so it cannot fill the disk. When the schema is behind, `migrate` needs the
   daemon **stopped**: it refuses with **exit 75** while the supervisor is running (the presence lock) or any
   worker holds the workers lock, including the running daemon's own and a previous daemon's survivors (a
   stuck `zfs`, or `KillMode=process`); the start then fails and is retried after `RestartSec`. A plain `zfsbackup-config migrate` refuses in the same way, with or without `--backup-dir`;
   when the schema is current neither checks or writes anything. `migrate` does not copy the TLS keys (a
   migration does not touch them).
5. **Check**: `systemctl status zfsbackup`, `zfsbackup-config status`, `zfsbackup-config check`.

**Downgrading** is the old wheel plus the pre-migration backup: an old release refuses a database whose
schema is newer than it knows. Stop the daemon (and the check timer), install the old
wheel, put the matching `config.db.pre-<revision>-…` in place **exactly as in [Restoring a configuration
backup](#restoring-a-configuration-backup)** (stage it under a temporary name, remove `-wal`/`-shm` first, `mv` it in
last; never `cp` over the existing file), start it. The pre-migration backup is a WAL-mode database: opening it
where it lies would create `-wal`/`-shm` files in the root-only `backups/` directory, so copy it elsewhere
before you look inside it, or open it read-only (`sqlite3 'file:/path/config.db.pre-…?mode=ro'`). Read that section first: it **loses** everything the
trust tables recorded since that backup.

**Upgrade the client and the server together.** The two sides check a wire protocol number. A mismatch is
answered 426 and the client logs it: remote backups pause (local snapshots and pruning go on) until both sides
match, and `check` shows the lag growing.

**Changing how a destination is sent** (the send flags are fixed per destination, or `raw:` set to a
different value) cannot be applied to a server copy that already exists: the client refuses, logs one ERROR
and sends nothing. The server copy has to be renamed aside (below); the next cycle sends in full.

## Operating the backup server

- **The target is a dedicated dataset.** `remote_backup.target_dataset` holds only what zfsbackup places under
  it: the daemon enumerates everything below it and treats it as received client data, and prunes it. A
  dataset of this host's own configuration at, below, or (when it is `recursive`) above the target is refused
  when the configuration is imported or reloaded, because its snapshots and pruning would act on received
  data. Put nothing else there. Create it before the first client connects (`zfs create tank/backups`): the
  server does not create it, and `check` reports a target that does not exist as CRIT.
- **Server copies are inert.** Every receive sets, as local values, `canmount=noauto`, `readonly=on`,
  `volmode=none`, `mountpoint=none`, `sharenfs=off`, `sharesmb=off`, `setuid=off`, `exec=off` and
  `devices=off` on a filesystem, and `readonly=on`, `volmode=none` on a volume, and takes no holds (`-h`). A
  received dataset is never mounted, shared or exposed as a device, whatever the client's properties say;
  the containers the server creates for a client (`<target>/<client_id>` and parents) are `canmount=noauto`.
  Read data through a clone outside the tree ([Getting files back](#getting-files-back-file-level)).
- **Only plain streams are received.** The server reads each stream's header before `zfs receive` runs and
  refuses property or replication streams (`zfs send -p`, `-R`, `-I`), a stream whose base is not the one the
  client declared, and a volume stream onto a filesystem copy (or the reverse). The client never sends those;
  a refusal means a client/server mismatch or a bug, and is logged on both sides.
- **The server only writes into datasets it created.** A dataset under `<target>/<client_id>` that the server
  did not create (it has no local `org.zfsbackup:client_id`), including one you created or moved there by
  hand, is refused with 409 and the hint `zfs set org.zfsbackup:client_id=<id> <ds>`: backups of that dataset,
  and of new client datasets below it, stop (one ERROR on the client) while the rest carry on. Run the hint only
  to adopt that dataset into the client's backups on purpose (and, on a filesystem, set `canmount=noauto`).
  A server copy renamed aside and back keeps its stamp.
- **A client that shortens its retention** is logged on the server at WARNING ("the retention horizon of …
  shrinks from 30d to 7d"): the server applies the client's retention as sent, so older snapshots go at the
  next pruning cycle.
- **Layout.** A client's dataset `tank/data` is stored as `<target>/<client_id>/data`: the server drops the
  client's **pool** name and decides the rest; the client never chooses a path. A client's pool root `tank`
  is `<target>/<client_id>` itself. Two client datasets that map to the same server name (`tank/data` and
  `rpool/data`) collide: the second is refused with 409.
- **Retention is the client's, run by the server.** The server prunes each received dataset with the
  retention rules that the client sent (`org.zfsbackup:config`), using the **server's** `snapshot_prefix`. A
  client with another prefix is never pruned on the server (it keeps everything), so use the same prefix.
- **Never edit the `org.zfsbackup:*` properties** `config`, `client_id` and `source_dataset` of a received
  dataset, and **never `zfs inherit -S`** them. The server sets them as local values, but a hidden *received*
  copy of each sits underneath (it came with the stream); `inherit -S` would revert to the client's copy.
- **Parents are sent first.** The remote worker sends a dataset's ancestors before its descendants. If a
  child was backed up first (its parent was added to the configuration afterwards), the server holds a
  placeholder for the parent and a full stream of the parent is refused. Rename the placeholder aside
  (below); that also moves the children's server copies, so they are sent in full again.
- **Encrypted datasets go raw by default** (`zfs send -w`), and the server never holds the key. The server
  therefore cannot mount, read or restore the contents of an encrypted dataset; keep a copy of the key off
  the client. Per destination, `raw: true` requires a raw send and `raw: false` forbids it. An unencrypted
  server copy cannot be switched to raw or the other way round without a full re-send.
- **Renaming a server copy aside.** For a server copy that can no longer be continued from the client (a
  different kind of stream, a placeholder parent, no common snapshot), move it **out of the target tree**
  on the server, then let the next cycle send in full:
  ```bash
  sudo zfs create -p tank/backups-aside
  sudo zfs rename tank/backups/<client_id>/data tank/backups-aside/<client_id>-data-$(date +%F)
  ```
  Inside the tree it would still be pruned, and its `source_dataset` would get the new send refused with
  409. Destroy the aside copy when you no longer need it.
  **Under an encrypted target**, a plain copy inherits the target's key, and ZFS refuses to rename a dataset
  out of the encryption root it inherits from. Put the encryption root *above* the target (for example
  `tank/enc` as the root, `tank/enc/backups` as the target, `tank/enc/aside` for aside copies; the aside
  dataset must stay under that root), or first make the copy its own encryption root with
  `zfs change-key -o keyformat=… -o keylocation=file:///… <copy>` (both options are required; a `prompt`
  location waits for a passphrase on a terminal). A raw copy is always its own encryption root and renames
  anywhere.
- **Sizing.** Nothing limits a client's space or bandwidth (`zfs set quota=… <target>/<client_id>` does; see
  [First run](#first-run)); a host's remote sends run one after another.

## Backing up the host

`zfsbackup-config export` is **not** a backup: it omits the trust tables, the client id and the keys.

```bash
sudo zfsbackup-config backup /srv/zfsbackup-backups            # root: includes the TLS private keys
zfsbackup-config backup --no-keys ~/zfsbackup-backups          # a group operator: no keys
```

It writes `DIR/zfsbackup-<host>-<UTC time>/` (mode 0700) with `config.db` (a consistent copy of the
configuration, the host identity **and the trust tables**), `tls/` (the certificates and, unless `--no-keys`, the
**private keys**) and a `MANIFEST`, written last: a directory without one is incomplete. It never overwrites,
and a failure or ^C removes what it wrote. A backup that includes keys is as sensitive as the keys. A group
operator cannot read the root-owned keys; `backup` then refuses (exit 1) unless `--no-keys` is given, and a
backup without keys cannot restore this host's TLS identity (the host would have to enroll again). Keep backups
off the host.

### Restoring a configuration backup

Restore with **everything that can open the database stopped**: the daemon, the check timer, and no
`zfsbackup-config` running. The database is replaced by a new file, never overwritten in place, and the old
`-wal`/`-shm` files go **before** it: a connection that opened the old file in between would replay the old
write-ahead log onto the restored one.

```bash
DB=/var/lib/zfsbackup/config.db
sudo zfsbackup-config trust list > ~/trust-before-restore.txt   # the LIVE trust state, before you replace anything
sudo systemctl stop zfsbackup zfsbackup-check.timer          # and let no zfsbackup-config run
sudo install -m 0660 -g zfsbackup BACKUP/config.db $DB.restore   # staged next to the target
sudo rm -f $DB-wal $DB-shm                                   # before the file is in place
sudo mv $DB.restore $DB                                      # last: a new inode, atomically
# The TLS directory (a backup made with keys): move the existing one aside, never copy into it.
sudo mv $DB.tls $DB.tls.old-$(date +%Y%m%dT%H%M%S)          # if it exists; a time, so a second restore never moves it INTO an old aside
sudo cp -a BACKUP/tls $DB.tls
sudo chgrp -R zfsbackup $DB.tls && sudo chmod 0750 $DB.tls   # operators need the group for `identity show`, `check`
# Fix the trust state BEFORE the daemon starts (see the box below): compare ~/trust-before-restore.txt with
sudo zfsbackup-config trust list                             # the restored state
# ... re-apply revocations, revoke used tokens, create --replace tokens ...
sudo systemctl start zfsbackup zfsbackup-check.timer         # only now
```

- **`cp -a BACKUP/tls $DB.tls` onto an existing `$DB.tls` nests `tls/` inside it.** The directory exists as soon
  as a server has started or a client has enrolled, so move it aside first as shown. On a rebuilt **server**
  this matters: if the old `server.key`/`server.crt` are not restored, the server presents a new
  certificate and **every client's pin fails**. Restore the keys, or have every client enroll again.
- **A backup made with `--no-keys` has no usable TLS identity.** Restore `config.db` only and leave the
  existing `.tls` directory alone (or enroll again).
- **`backup` does not copy a `client_cert_file`/`client_key_file` that you configured outside `<db>.tls`.** Keep
  those files in your own backup.
- A client restored from a backup **with keys** keeps its client id and its pinned certificate and does not
  enroll again, **unless it rotated its certificate after the backup**: its servers retired the old one and
  refuse it for ever. Such a client must `enroll DESTINATION --new-key --token-file FILE` with a server-side
  `trust token create --client-id ID --replace` token. Without keys, or on a new host:
  `zfsbackup-config identity set-client-id ID`, then that same token and `enroll`. A lost **server**
  `.tls` directory means every client has to enroll again (the server certificate is pinned by its
  fingerprint and cannot be re-keyed).

> **A restore brings back old trust.** The backup's trust tables are as they were when it was made. A restore
> therefore **un-revokes** every fingerprint revoked since, brings back **consumed** enrollment tokens and the
> certificates a rotation has retired, and **loses** clients the API enrolled or rotated since. So, while the
> daemon is still **stopped** (`trust` commands work without it), before `systemctl start`:
> 1. compare the restored state (`zfsbackup-config trust list`) with the live one you saved before the restore,
>    and re-apply every revocation made since the backup (`trust revoke FINGERPRINT`, `trust revoke --client
>    ID`);
> 2. revoke any enrollment token that was already used or revoked (`trust token revoke`), and create a fresh
>    one where you need it;
> 3. for each client that **rotated or enrolled since the backup**, its current certificate is unknown to the
>    restored server and the old, retired one is pinned again: create `trust token create --client-id ID
>    --replace` and have the client run `enroll DESTINATION --token-file FILE`, which drops the resurrected pin.
>
> Or restore only onto a host whose trust state has not changed. The same holds for the pre-migration
> backups: trust writes made by the API between that backup and the upgrade are not in it.

## Restoring data

Backups on a server are ZFS datasets, so restoring is ZFS work: this README gives the supported routes;
there is no restore command and no restore over the API. **Rule one: never mount, `zfs set`, `zfs
inherit` or otherwise modify a received dataset.** Work from clones and sends, which change nothing the daemon relies on.

> **Rule two: rolling a backed-up client dataset back pauses its backup.** After a `zfs rollback`, or a
> restore of an older snapshot in place, the server still holds snapshots the client no longer has; it never
> destroys them for the client, so that dataset's sends stop (one ERROR) until you decide on the server.
> Restore from the server's newest snapshot, or into a new name, to avoid that. Details under
> [Rejoining](#rejoining-seed-and-continue).

### Getting files back (file-level)

On the **server**, clone a snapshot of the received dataset to a place **outside the target tree** and read
it there:

```bash
sudo zfs create -p tank/restore
sudo zfs clone -o readonly=on -o canmount=on -o mountpoint=/mnt/restore/data \
     tank/backups/<client_id>/data@autosnap_20261021120000 tank/restore/data
# … copy the files out of /mnt/restore/data …
sudo zfs destroy tank/restore/data
```

- **Outside the target tree**, because a clone takes its properties from its new parent, not from the origin:
  it does not pick up the server copy's `canmount`, `readonly`, `mountpoint`, `volmode` or `org.zfsbackup:*`
  values, so it can be mounted where you choose.
- **A volume** is cloned with `-o volmode=dev` (and without `canmount`/`mountpoint`); read it at
  `/dev/zvol/tank/restore/data`.
- **Destroy the clone when you are done.** While it exists its origin snapshot cannot be pruned (the pruner
  skips just that snapshot and says so in the log).
- `.zfs/snapshot` browsing on the received dataset itself is not a supported route.
- An **encrypted** (raw) copy cannot be read on the server: `zfs clone` succeeds without the key, but the clone
  cannot be mounted, because the server has no key. Restore it to the client (below).

### Restoring a whole dataset to the client

On the **server**, send the snapshot to the client and receive it unmounted. Send the server's **newest**
snapshot, or receive into a new dataset name ([Rejoining](#rejoining-seed-and-continue)). **Do not use `-p` or `-R`**:
with them the server copy's own values travel with the stream: its `org.zfsbackup:*` values and the inert
settings above (`readonly=on`, `canmount=noauto`, `mountpoint=none`, `exec=off` and the rest). Use the flags the client itself sends with: `-L -c` for a plain copy, `-w` for a raw one. A
dataset that **still exists on the client must be renamed aside first** (`zfs rename tank/data
tank/data.old`): a full receive onto an existing dataset fails.

```bash
# on the server (plain copy; the client dataset tank/data is stored as <target>/<client_id>/data)
sudo zfs send -L -c tank/backups/<client_id>/data@autosnap_20261021120000 \
  | ssh client sudo zfs receive -u tank/data
# an encrypted dataset: raw, then load the key with the client's original key
sudo zfs send -w tank/backups/<client_id>/secret@… | ssh client sudo zfs receive -u tank/secret
ssh client sudo zfs load-key tank/secret
# then, on the client, after checking it:
sudo zfs mount tank/data
```

If a `-p` stream was used anyway, look at what arrived with `zfs get -s received all tank/data`, and undo the
*received* values with plain `zfs inherit` (`readonly`, `mountpoint`, `volmode`, `sharenfs`, `sharesmb`,
`setuid`, `exec`, `devices`) and `zfs set canmount=on`. (Receiving with `-x` for each of those, `-x canmount`, and for
`org.zfsbackup:config`, `org.zfsbackup:client_id` and `org.zfsbackup:source_dataset` avoids it.) **Keep a copy of the encryption key off
the client**: the server has none, so a lost key means a lost raw backup.

### Rejoining: seed and continue

> **The server never rolls its history back for a client.** An incremental receive with `zfs receive -F` would
> roll the server copy back to the stream's base and destroy every server snapshot newer than it (OpenZFS 2.1,
> 2.2 and 2.4 alike). So the server passes `-F` only when the base is its newest snapshot (or when the one
> newer snapshot is one the client itself sent and then lost, see below), and refuses any other stream whose
> base is older than its newest snapshot. A client that rolls back (`zfs rollback -r`), or restores an older
> snapshot in place under the same name, finds the server ahead of it: it logs one ERROR naming the server copy
> and the snapshots involved, and sends nothing for that dataset until you act. `check` shows the remote lag
> growing meanwhile. Nothing is lost while it waits.
>
> Then, on the server, either **keep** the newer snapshots by renaming the server copy aside (out of the
> target tree, see [Renaming a server copy aside](#operating-the-backup-server)); the next cycle sends a full
> stream into a fresh server dataset. Or, if they are expendable, **destroy** just them with the
> `zfs destroy <server copy>@<first>%<last>` the ERROR prints (a `%` range is inclusive, by creation order; over
> 64 newer snapshots it prints the open-ended `<first>%`, which runs to the newest);
> the next cycle continues incrementally.
>
> The one case the client lets the server roll back by itself: a snapshot it had just sent was deleted on the
> client before the client could record the send. The server then holds exactly one snapshot the client never
> recorded, newer than the client's recorded base, and the next cycle replaces it (the server logs a WARNING
> naming it).

Safe ways to bring a dataset back on the client:

1. **Restore the server's NEWEST snapshot in place.** The next send is incremental from it and loses nothing.
   Negotiation finds the common snapshot by name, and the server checks the stream's base by guid; check the
   next cycle in the log or with `zfsbackup-config check`.
2. **Restore into a new dataset name** (and configure that name). The server stores it as a separate
   dataset, sent in full, and the old server copy stays untouched until you decide to destroy it.
3. **Rename the server copy aside first** (above), then do whatever you need on the client. The next cycle
   sends a full stream into a fresh server dataset, and the old history is kept under the aside name.

It also pays to stop the sending while you work: disable the dataset (`zfsbackup-config set
'datasets[tank/data].enabled' false`) or stop the daemon, and enable it again when the client is as you want it.

When there is simply **no common snapshot** (the dataset on the client was replaced by an unrelated one, or
you restored a snapshot the server does not have), the server never replaces or wipes its copy: it refuses a
full stream onto a dataset that has snapshots (so would ZFS), encrypted or not. The client logs one ERROR and
sends nothing for that dataset, which shows as remote lag turning CRIT in `check`, until you rename the server
copy aside (above). The next cycle then sends a full stream into a fresh dataset. Keep the aside copy until you
are satisfied.

### Recovering a whole host

Restore the host's `zfsbackup-config backup` (above), then its datasets with the full restore. Restoring
the **trust** tables has consequences; read the warning in
[Restoring a configuration backup](#restoring-a-configuration-backup).

## Logging

The daemon logs to standard output. Under systemd that is the journal:

```bash
journalctl -u zfsbackup                  # everything
journalctl -u zfsbackup -p warning       # warnings and worse
journalctl -u zfsbackup -p err           # errors only
```

- **Priorities.** When standard output is the journal's own stream (the daemon checks `$JOURNAL_STREAM` against
  the file it is writing to, not just that the variable is set), each line carries its syslog priority
  (debug, info, warning, err, crit), on every line of a multi-line record such as a traceback, so
  `journalctl -p err` finds the errors. Lines then have no timestamp of their own (the journal has one). Anywhere else
  (a terminal, a file) the format is `timestamp - logger - LEVEL - message`.
- **Level.** `--log-level debug|info|warning|error` (default `info`); `-v` is the same as `--log-level
  debug`. To change it for the service: `systemctl edit zfsbackup`, add `[Service]`, an empty `ExecStart=` and
  your own `ExecStart=… zfsbackupd -c … --log-level warning`. The setting applies to the workers as well.
- **The API's access lines** (one per request) are logged by status class: 1xx to 3xx at DEBUG, 4xx
  (including 401 and 403) at INFO, 5xx at WARNING.
- Key messages: snapshot creation and deletion, retention decisions, remote transfers and their refusals,
  reloads (accepted, rejected, deferred), TLS problems, and lifecycle events. A reload that is rejected is
  logged at WARNING with the reason.
- Logs are plain text; they never contain an enrollment token or a private key.

## Safety Features

- **Dry Run Mode**: Test configuration without making changes
- **Prefix Filtering**: Only manages snapshots with the configured prefix
- **Graceful Shutdown**: Handles SIGINT/SIGTERM signals; SIGHUP reloads the configuration
- **Error Handling**: Continues operation even if individual datasets fail
- **Validation**: Validates configuration on startup and before every reload (a rejected reload keeps the previous configuration). An API address that is *in use* at reload (`EADDRINUSE`) is *deferred*, not rejected: the daemon keeps running the previous configuration, publishes `deferred=` and `deferred_reason=` in the pid file (`zfsbackup-config status` says `deferred`), re-checks it at every 10 s poll and applies it as soon as it passes, without a new write. An address that cannot be bound for any other reason (not an address of this host, a privileged port, a name that does not resolve, a malformed host) is rejected for good, because retrying would only repeat the failure (and re-resolve a dead name on every poll). Client TLS problems (no client id yet, an unreadable, expired or soon-to-expire client certificate, a destination that is not enrolled) are only warnings; local snapshots and pruning keep running and the remote worker skips what it cannot use. At startup, however, an unbindable API address is still a hard refusal (exit 1), so restarting the daemon while the port is held keeps it down until the port is free

## Known limitations

What to know before you rely on it. None of these blocks a deployment on a network you control. The full
list with background is in [docs/production_readiness_report.md](../docs/production_readiness_report.md).

**Remote backup and the API**
- **A rollback on the client pauses that dataset's backup.** After a `zfs rollback` of a dataset that is
  being sent, or restoring an older snapshot over it under the same name, the server holds snapshots the
  client no longer has. It keeps them and refuses the client's sends (one ERROR on the client) until the
  operator renames the server copy aside or destroys those snapshots (see
  [Rejoining](#rejoining-seed-and-continue)). There is no command that accepts the loss for you.
- **The HTTP server** is werkzeug's WSGI server (the version is pinned to one minor release, and its
  internals are covered by tests). It has no production-grade alternative that fits the design, so the
  protections around it are zfsbackup's own: a per-connection TLS handshake with a timeout, at most 64
  connections (16 without a client certificate), an absolute deadline for request headers, and a bounded
  drain. Restrict the port by firewall.
- **Revocation cuts running streams** within about 2 seconds (the partial receive is kept for a resume); a key
  rotation does not.
- **Flood control is per address, with constants, no configuration**: a rate limit and 8 connections per
  address (32 and a reserved share of the slots once it has presented a pinned client certificate), and a
  15-minute `/enroll` lockout after 5 failures. Clients behind one NAT share an address. A flood spread over
  many addresses is still bounded only by the connection caps; restrict the port by firewall.
- **A trusted client can postpone a reload** on the server by streaming without pause: the API worker waits
  for in-flight receives and never cuts them. The websocket idle timeout is 600 s.
- **Server certificate re-keying is unsupported.** The certificate is self-signed for ten years and clients
  ignore its expiry by design; a compromised server key means every client enrolls again.
- **The daemon runs as root.** Only the group `zfsbackup` is created for operators.
- **No metrics endpoint**; use `check --prometheus` with the textfile collector. **No bandwidth limit** and no
  automatic per-client quota (set `zfs set quota`). A host's remote sends run one after another.
- **No bookmarks.** A destination that was removed from the configuration leaves its `anchor.*` property
  pinning one snapshot per dataset until you clear it (the pruner protects every local anchor).
- **Received `anchor.*`/`inflight.*` properties** can pin snapshots on a server (disk growth only).
  The hidden *received* copies of the properties the server sets with `-o` are inert, and cannot be
  cleared by a receive flag.
- **A client with a different `snapshot_prefix`** is never pruned on the server (it keeps everything).
- **No common snapshot means a refusal, not an overwrite.** If a client and its server copy have no snapshot
  in common (a restore of a snapshot the server does not have, a replaced dataset), the server refuses the
  full stream because its copy has snapshots, and the server copy is left untouched. The client logs one
  ERROR and sends nothing for that dataset, and `check` shows the remote lag, until the operator renames the
  server copy aside; then a full send goes out.
- **Do not take snapshots under the backup target by hand** (for example a `zfs snapshot -r` of the whole
  pool) while receives may be running. The server decides at the start of a receive whether it may roll the
  copy back to the stream's base; a snapshot that appears on that copy during the receive can still be
  destroyed by it. The target is dedicated to zfsbackup; snapshot the received data through the client's
  retention, or clone it outside the target.
- **A parent added to the configuration after its child** has been backed up meets the placeholder refusal
  and needs the operator's rename (see [Operating the backup server](#operating-the-backup-server)).
- **The server enumerates every dataset under the target** on each pruning cycle (two or three `zfs get`
  executions each); very large servers are slow to cycle.
- **The pending-stream map on the server is shared by all clients** (4096 entries, least recently used
  first). An enrolled client that negotiates thousands of names can evict other clients' entries, whose next
  stream is refused with a retryable "negotiate again".
- **There is no restore over the API.** Restoring is ZFS work done by an operator (see [Restoring data](#restoring-data)).

**Operation**
- **Stop the daemon to upgrade** (see [Upgrading](#upgrading-and-downgrading)). An in-place install is
  detected and the daemon drains and exits 76 for systemd to restart it, but a dependency-only upgrade and
  edits to an editable install are not detected (`systemctl restart zfsbackup`).
- **`--test-config` does not check that datasets exist.** The snapshot worker checks them at start and logs
  an error per missing dataset.
- **On macOS the CLI never signals the daemon**, because it cannot prove which process holds the lock; the
  10 s poll applies the change. (The daemon is meant for Linux.)

**ZFS bindings (`libzfseasy`)**
- **Pool names** containing `:` or inner spaces, which ZFS allows, are refused by the name validation, so
  datasets in such a pool cannot be configured.
- **Names longer than 255 bytes** (`longname`, OpenZFS 2.3 and later) are skipped with a warning.
- **`zpool` must be installed**, though it is never used: both binaries are resolved when the program starts
  (`ZFS_CMD` and `ZPOOL_CMD` override the paths).
- **A user property whose value is exactly `-`** reads back as unset.
- **Ctrl-C in an interactive CLI also reaches the `zfs` child processes**, which share the terminal's process
  group.
- **Not implemented:** hold, release, rollback, promote and diff. zfsbackup protects in-flight snapshots with
  `org.zfsbackup:inflight.*` properties, not holds.

## Troubleshooting

### Snapshots not being created

1. Check dataset exists: `zfs list tank/data`
2. Check permissions: Run with appropriate ZFS permissions
3. Enable verbose logging: `zfsbackupd -v` (or `--log-level debug` in the unit)
4. Test config: `zfsbackupd --test-config`
5. Run `zfsbackup-config check` to see which datasets are behind and by how much

### Snapshots not being deleted

1. Check retention rules are correctly configured
2. Verify snapshot ages with: `zfs list -t snapshot -o name,creation`
3. Run in dry-run mode to see what would be deleted
4. Check logs for errors

### Configuration errors

```bash
# Test configuration
zfsbackupd --test-config -c /path/to/config.db
```

### The service does not start

- **"Assertion failed":** there is no `/var/lib/zfsbackup/config.db` yet: run
  `zfsbackup-config import` first.
- **`ExecStartPre` exits 75:** workers of a previous daemon are still running (`fuser /var/lib/zfsbackup/config.db.workers.lock`);
  the start is retried after `RestartSec`.
- **Schema errors (the database is newer or older than the program):** see [Upgrading](#upgrading-and-downgrading).
- **The API address is in use** at startup: the daemon refuses to start until the port is free.

### Remote backups are not happening

1. `zfsbackup-config check` (and `list destinations`): is the destination enrolled, and which side lags?
2. `journalctl -u zfsbackup`: a 426 means client and server run different protocol versions; a
   refusal that names the server copy needs it renamed aside (see [Operating the backup server](#operating-the-backup-server)).
3. `zfsbackup-config identity show`: is the client certificate valid and the id the one the server knows?

## License

See the main project LICENSE file.

## Contributing

Contributions are welcome! Please ensure:
- Code follows existing style
- Add tests for new features
- Update documentation
