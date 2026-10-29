#!/usr/bin/env bash
#
# test_config_permissions.sh — prove the SQLite config store's group-operator permission model
# on REAL Linux (the pytest suite can only mock it on macOS).
#
# One Multipass VM, three unprivileged identities:
#   zfsbackup  service user  (the de-rooted daemon runs as this)
#   alice, bob operators      (primary group = their own; supplementary group = zfsbackup)
# and root, who makes the first import.
#
# Checks:
#   (a) root's first import of the DEFAULT path /var/lib/zfsbackup/config.db  ->  directory
#       2770 root:zfsbackup (Q7 chgrp), database 660 group zfsbackup.
#   (b) the daemon runs as zfsbackup while alice (then bob) re-import: the import succeeds
#       (no EPERM), the daemon keeps running. Item 15.2: the supervisor reads the config DB ONCE
#       (logs "Configuration generation N, schema revision R" once, records the pid file v2 --
#       "pid\ngeneration\nstart=..\napplied=<store>:<gen>" -- in <db>.daemon.pid) and hands it to
#       every worker; workers hold no config.db fd. Phase 3 (live reload): the operators' imports
#       cannot SIGHUP a daemon of another uid, so the CLI says "runs as another user and was not
#       signalled; it picks up the change within 10s" (never "restart"), the daemon's generation poll
#       accepts them (trigger: poll), every worker turns over (supervisor pid unchanged),
#       `status --wait` reports "in sync", a kill -9'd-and-respawned worker is handed the supervisor's
#       CURRENT config (no second load logged, pid file applied=<DB generation>, no config.db fd),
#       a second daemon on the same DB exits 1 naming the first pid, and <db>.writer.lock /
#       <db>.daemon.pid are 0660 group zfsbackup.
#   (c) alice's process is the FIRST opener of the DB and is killed with SIGHUP while holding
#       it: the leftover -wal/-shm sit in group zfsbackup (setgid directory) and the daemon
#       user can still open the DB.
#   (d) a non-default, deliberately 0600 `-c` DB stays 0600 across a re-import (as owner and
#       as root).
#   (e) with the setgid bit removed from the directory, an operator-created sidecar lands in
#       alice's PRIMARY group; the daemon preflight reports that as a clear
#       ConfigDbPermissionError (exit non-zero, names the sidecar and the fix) instead of the
#       opaque "unable to open database file". (e2) the full daemon refuses to start over the
#       same state and starts no workers.
#   (f) live config reload (Phase 3), the daemon running as zfsbackup:
#       f1 root `set` -> verified and SIGHUPed ("accepted"/"now on", log "trigger: SIGHUP" within 5s,
#          snapshot/pruning/api pids change, supervisor's does not, pid file applied=<store>:<gen>,
#          `status` in sync, `status --wait`); an unchanged SIGHUP logs "unchanged", restarts nothing;
#       f2 group operator alice `set` -> EPERM path: "not signalled; it picks up the change within 10s",
#          the poll accepts it ("trigger: poll") within 15s;
#       f3 the service user's own `set` -> signalled (same uid);
#       f5 a raw sqlite3 UPDATE that makes the stored config invalid and bumps the generation, then
#          SIGHUP -> "rejected", pids unchanged, pid file rejected=, unit active, status reports it
#          (`--wait` exits 1), and a good re-import is accepted and clears rejected=;
#       f4 forgery: a holder (alice) locks the pid file and names a root-owned decoy `sleep 600` with
#          its correct start token (root-owned pid file -> lock-table proof refuses; alice-owned ->
#          owner check refuses); root `set` never signals the decoy;
#       f6 a CLI write during daemon startup: first deterministically (a stand-in surviving worker
#          holds <db>.workers.lock, so the new daemon sits in its startup wait while the CLI writes and
#          SIGHUPs it), then three times racing the CLI against a cold start. The daemon stays up and
#          ends up on the new generation.
#       The start method on the VM is recorded (forkserver on Python >= 3.14).
#   (g) a reload to a BUSY api_port (another process listens there) is DEFERRED, not rejected: the CLI says the
#       daemon "has not applied generation N yet ... re-checks every poll", the log carries one WARNING,
#       `deferred=`/`deferred_reason=` appear in the pid file (never `rejected=`), `status` reports the
#       verdict `deferred` (`--json` too), `status --wait` exits 75, the workers and the old port are
#       untouched; releasing the port makes the 10s poll apply the SAME stored generation (trigger: poll, no new
#       write), `deferred=` clears, the new port answers and the old one does not.
#   (h) the daemon as a real systemd unit, `Type=notify-reload` (systemd >= 253; `Type=notify` + `ExecReload=` below that),
#       as documented in zfsbackup/README.md: `systemctl start` returns after READY (workers already forked),
#       `systemctl reload` returns after the reload DECISION (unchanged / accepted / even rejected), logs exactly one
#       SIGHUP decision, `systemctl show -p StatusText` follows "generation N" (and names a rejected generation),
#       `systemctl stop` is clean (Result=success, no process left, pid file removed).
#   (i) the TLS identity directory <db>.tls (milestone 3): created 0750 in the DB file's group; keys 0600, certificates 0640,
#       identity.lock 0640 (all in the DB file's group). i1: the service user creates the server identity (`trust token
#       create`, as the daemon does at startup -- this scenario's daemon runs --dry-run, which creates nothing); the group
#       operator alice can read server.crt (so she can issue tokens) but not server.key. i2: root creates the client
#       identity (what `enroll` does): the directory is root-owned, the files still carry the DB's group.
#
# Daemon choice: the point is DB ACCESS, not ZFS operations, so the daemon runs `--dry-run`
# against a STUB `zfs`/`zpool` (the same trick .github/workflows/tests.yml uses) — no pool, no
# `zfs allow` delegation, no ZFS install. Workers still start, still load the config from the
# DB as user zfsbackup, and are still respawned — that is exactly the access path under test.
# (Since 15.2(b) only the supervisor reads the DB; workers receive its config, respawns included.)
#
# Seeding: the fixtures are derived from zfsbackup/config.example.yaml (source of truth) and
# imported with the config CLI, `python -m zfsbackup.cli import <yaml> [-c <db>]` (the module
# form of `zfsbackup-config`: the venv does not pip-install the repo, so no console script).
# Re-imports run WITH the default pre-import backup on purpose: the backup file is written by
# the operator into the shared directory, so its owner/group/mode (0660, group zfsbackup via the
# setgid directory) are asserted in (b) and (d); one `--no-backup` import in (d) proves the flag.
# Exit codes: 0 ok, 1 invalid/refused, 75 busy, 78 config DB unusable (asserted in (e3)). Files reach the VM with
# `multipass transfer`; all output discarding happens inside the guest.
#
# Source shipped to the VM: `git archive HEAD` by default. `ZFSB_SRC=worktree` packs the WORKING
# TREE instead (tracked + untracked-not-ignored files that exist on disk, via `git ls-files -co
# --exclude-standard`), so uncommitted code can be exercised. Nothing is committed or stashed.
#
# Usage:
#   ./scenarios/test_config_permissions.sh           # run all checks; VM reused, state reset
#   ZFSB_SRC=worktree ./scenarios/test_config_permissions.sh   # ship uncommitted code
#   ./scenarios/test_config_permissions.sh --keep    # leave daemon + DB state for inspection
#   ./scenarios/test_config_permissions.sh --purge   # delete the VM at the end
#
# Requires: multipass on the host. No sudo on the host. Run from the repo root.

set -uo pipefail

# ─────────────────────────────── Configuration ──────────────────────────────
PERM_VM="${PERM_VM:-zfsb-perm}"

SRC_DIR="/opt/zfsbackup-src"
VENV="/opt/zfsbvenv"
PY="${VENV}/bin/python"
PIP="${VENV}/bin/pip"
FIX_DIR="/opt/zfsb-perm"                 # world-readable fixtures + helper scripts
ENV_WRAP="${FIX_DIR}/zfsb-env"           # exports PYTHONPATH, stub ZFS_CMD...; then exec "$@"
CFG_DIR="/var/lib/zfsbackup"
DB="${CFG_DIR}/config.db"                # the DEFAULT path (no -c / env needed)
LOG="/var/log/zfsbackup-perm.log"
UNIT="zfsperm"
HOLD_UNIT="zfsperm-holder"
SVC_USER="zfsbackup"; SVC_GROUP="zfsbackup"
OPERATORS=(alice bob)

VM_CPUS="${VM_CPUS:-1}"
VM_MEM="${VM_MEM:-1G}"
VM_DISK="${VM_DISK:-5G}"

DAEMON_TIMEOUT=60        # seconds to wait for daemon/worker events

PURGE=0; KEEP=0
for arg in "$@"; do
    case "$arg" in
        --purge) PURGE=1 ;;
        --keep)  KEEP=1 ;;
        -h|--help) grep '^#' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *) echo "Unknown option: $arg" >&2; exit 2 ;;
    esac
done

# ─────────────────────────────── Output helpers ─────────────────────────────
if [ -t 1 ]; then
    RED=$'\033[0;31m'; GREEN=$'\033[0;32m'; YELLOW=$'\033[0;33m'; BLUE=$'\033[0;34m'; BOLD=$'\033[1m'; NC=$'\033[0m'
else
    RED=; GREEN=; YELLOW=; BLUE=; BOLD=; NC=
fi

PASS_COUNT=0; FAIL_COUNT=0
declare -a RESULTS=()

info()  { echo "${BLUE}==>${NC} $*"; }
step()  { echo "${BOLD}${BLUE}::${NC} ${BOLD}$*${NC}"; }
warn()  { echo "${YELLOW}WARN:${NC} $*" >&2; }
die()   { echo "${RED}FATAL:${NC} $*" >&2; exit 1; }

pass()  { PASS_COUNT=$((PASS_COUNT+1)); RESULTS+=("${GREEN}PASS${NC}  $1"); echo "  ${GREEN}✓ PASS${NC} $1"; }
fail()  { FAIL_COUNT=$((FAIL_COUNT+1)); RESULTS+=("${RED}FAIL${NC}  $1"); echo "  ${RED}✗ FAIL${NC} $1"; }
check() { # check "<description>" <expected> <actual>
    if [ "$2" = "$3" ]; then pass "$1 ($3)"; else fail "$1 (expected='$2' actual='$3')"; fi
}
# check_re "<description>" "<ERE>" "<text>"  — pass when the text matches
check_re() {
    if grep -qE -- "$2" <<<"$3"; then pass "$1"; else
        fail "$1 (no match for /$2/ in: $(printf '%s' "$3" | tr '\n' '|' | cut -c1-300))"
    fi
}
check_no_re() { # check_no_re "<description>" "<ERE>" "<text>"  — pass when the text does NOT match
    if grep -qE -- "$2" <<<"$3"; then
        fail "$1 (unexpected /$2/ in: $(printf '%s' "$3" | tr '\n' '|' | cut -c1-300))"
    else pass "$1"; fi
}

# ─────────────────────────────── Multipass helpers ──────────────────────────
# CRITICAL multipass-on-macOS quirk: `multipass exec` busy-spins FOREVER when the remote
# command writes to stdout AND the host redirects that stdout to /dev/null. Host PIPES and
# `$(...)` capture are fine; redirecting INSIDE the guest is fine. Never send an exec's HOST
# stdout to /dev/null — discard output in the guest (`-o /dev/null`, `sh -c '... >/dev/null'`).
mexec() { local vm="$1"; shift; multipass exec "$vm" -- "$@"; }
# SECOND wedge (verified): a guest command that exits with status 4 makes `multipass exec` hang
# forever (e.g. `systemctl is-active` on a unit that no longer exists exits 4). So the command
# runs in an inner shell and status 4 is remapped to 104 before it reaches multipass.
REMAP='bash -c "$1"; rc=$?; [ "$rc" -eq 4 ] && rc=104; exit "$rc"'
msh()   { multipass exec "$PERM_VM" -- sudo bash -c "$REMAP" _ "$*"; }
# asu <user> <command...> — run a command in the guest as <user> inside the fixed environment
# (PYTHONPATH, stub ZFS). Real initgroups: supplementary groups apply, like a real login.
asu()   { local u="$1"; shift; multipass exec "$PERM_VM" -- sudo -u "$u" -H "$ENV_WRAP" bash -c "$REMAP" _ "$*"; }

vm_exists()  { multipass info "$1" >/dev/null 2>&1; }
vm_running() { multipass info "$1" 2>/dev/null | awk -F': *' '/^State/{print $2}' | grep -qi running; }

ensure_vm() {
    if ! vm_exists "$PERM_VM"; then
        step "Launching VM $PERM_VM"
        multipass launch --name "$PERM_VM" --cpus "$VM_CPUS" --memory "$VM_MEM" --disk "$VM_DISK" \
            || die "failed to launch $PERM_VM"
    elif ! vm_running "$PERM_VM"; then
        step "Starting existing VM $PERM_VM"
        multipass start "$PERM_VM" || die "failed to start $PERM_VM"
    else
        info "VM $PERM_VM already running"
    fi
}

# put_file <host-file> <guest-path> <mode>  — via `multipass transfer` (never piped stdin)
put_file() {
    local host="$1" guest="$2" mode="$3" base
    base="/tmp/zfsb-perm-xfer.$$.$RANDOM"
    multipass transfer "$host" "$PERM_VM:$base" || die "transfer to $guest failed"
    msh "install -D -m '$mode' '$base' '$guest' && rm -f '$base'" || die "install of $guest failed"
}

# ─────────────────────────────── Provisioning ───────────────────────────────
install_stack() {
    step "Provisioning $PERM_VM (Python + zfsbackup; no ZFS needed — stub binaries)"
    msh '
        set -e
        export DEBIAN_FRONTEND=noninteractive
        need=""
        dpkg -s python3-venv >/dev/null 2>&1 || need="$need python3-venv"
        dpkg -s python3-pip  >/dev/null 2>&1 || need="$need python3-pip"
        if [ -n "$need" ]; then apt-get update -qq && apt-get install -y -qq $need; fi
    ' || die "package install failed"

    # Ship the committed HEAD (default) or the working tree (ZFSB_SRC=worktree). `git archive`
    # run from scenarios/ would archive ONLY that subtree, so anchor at the repo top level.
    step "Copying source into $PERM_VM:$SRC_DIR (ZFSB_SRC=${ZFSB_SRC:-head})"
    local tar top list n f; tar="$(mktemp)"
    top="$(git -C "$(dirname "$0")" rev-parse --show-toplevel)" || die "not in a git repo"
    case "${ZFSB_SRC:-head}" in
        head)
            git -C "$top" archive --format=tar HEAD > "$tar" || die "git archive failed"
            ;;
        worktree)
            # -c lists tracked files (including ones deleted on disk: skipped below), -o the
            # untracked-but-not-ignored ones. NUL-separated; no stash, no commit, no index change.
            list="$(mktemp)"; n=0
            while IFS= read -r -d '' f; do
                if [ -f "$top/$f" ]; then printf '%s\0' "$f" >> "$list"; n=$((n+1)); fi
            done < <(git -C "$top" ls-files -z -co --exclude-standard)
            [ "$n" -gt 0 ] || die "worktree packaging found no files"
            info "packing $n working-tree files"
            COPYFILE_DISABLE=1 tar -cf "$tar" -C "$top" --null -T "$list" || die "tar of working tree failed"
            rm -f "$list"
            ;;
        *) die "ZFSB_SRC must be 'head' or 'worktree' (got '${ZFSB_SRC}')" ;;
    esac
    multipass transfer "$tar" "$PERM_VM:/tmp/zfsbackup-src.tar" || die "source transfer failed"
    rm -f "$tar"
    msh "rm -rf '$SRC_DIR' && mkdir -p '$SRC_DIR' && tar -m -xf /tmp/zfsbackup-src.tar -C '$SRC_DIR' && rm -f /tmp/zfsbackup-src.tar" \
        || die "source extract failed"

    msh "
        set -e
        test -x '$PY' || python3 -m venv '$VENV'
        '$PIP' install -q --upgrade pip
        '$PIP' install -q pyyaml 'flask>=3' flask-sock websocket-client requests 'cryptography>=43' \
            'sqlalchemy>=2.0' 'alembic>=1.13' 'click>=8.1'
    " || die "pip install failed"
}

make_identities() {
    step "Creating group/user $SVC_GROUP and operators ${OPERATORS[*]}"
    # NB: the group MUST exist before root's first import, or Q7's chgrp only warns.
    msh "
        set -e
        getent group '$SVC_GROUP' >/dev/null || groupadd --system '$SVC_GROUP'
        id -u '$SVC_USER' >/dev/null 2>&1 || useradd --system --gid '$SVC_GROUP' --no-create-home \
            --home-dir /nonexistent --shell /usr/sbin/nologin '$SVC_USER'
        for u in ${OPERATORS[*]}; do
            id -u \$u >/dev/null 2>&1 || useradd --create-home --shell /bin/bash \$u   # primary group = \$u
            usermod -aG '$SVC_GROUP' \$u
        done
        id '$SVC_USER'; for u in ${OPERATORS[*]}; do id \$u; done
    " || die "user/group creation failed"
}

install_fixtures() {
    step "Installing stub zfs, env wrapper, fixtures and helper scripts"
    local t; t="$(mktemp -d)"

    # Stub zfs/zpool: libzfseasy resolves the binaries at import time and refuses to load without
    # them; this scenario never needs them to do anything.
    printf '#!/bin/sh\nexit 0\n' > "$t/zfs"
    put_file "$t/zfs" "$FIX_DIR/zfs" 755

    cat > "$t/zfsb-env" <<EOF
#!/bin/sh
export PYTHONPATH='$SRC_DIR:$SRC_DIR/libs/libzfseasy' PYTHONDONTWRITEBYTECODE=1
export ZFS_CMD='$FIX_DIR/zfs' ZPOOL_CMD='$FIX_DIR/zfs'
unset ZFSBACKUP_CONFIG
exec "\$@"
EOF
    put_file "$t/zfsb-env" "$ENV_WRAP" 755

    # Derive a fixture from the example config: keep the first N datasets, set a prefix and
    # short intervals. usage: mkfixture.py <src.yaml> <dst.yaml> <ndatasets|all> <prefix>
    cat > "$t/mkfixture.py" <<'EOF'
import sys, yaml
src, dst, n, prefix = sys.argv[1:5]
with open(src) as f:
    cfg = yaml.safe_load(f)
if n != "all":
    cfg["datasets"] = cfg["datasets"][: int(n)]
cfg["snapshot_prefix"] = prefix
cfg["check_interval"] = "10s"
cfg["prune_interval"] = "10s"
with open(dst, "w") as f:
    yaml.safe_dump(cfg, f, sort_keys=False)
EOF
    put_file "$t/mkfixture.py" "$FIX_DIR/mkfixture.py" 644

    # Hold the config DB open (first opener, with a read snapshot and the writer lock) until
    # killed. Prints READY once the connection is open and sidecars exist. Deliberately has
    # NO signal handler: SIGHUP's default action terminates without closing the connection,
    # which is what leaves the sidecars behind. usage: holder.py [db-path]
    cat > "$t/holder.py" <<'EOF'
import sys, time
from zfsbackup.config.store.paths import resolve_config_path, open_config_connection
r = resolve_config_path(sys.argv[1] if len(sys.argv) > 1 else None)
with open_config_connection(r) as conn:
    conn.exec_driver_sql("SELECT count(*) FROM sqlite_master").fetchall()
    try:
        conn.exec_driver_sql("BEGIN IMMEDIATE")      # take the writer lock too
    except Exception as exc:                          # harmless if the driver already began
        print("note: BEGIN IMMEDIATE:", exc, flush=True)
    print("READY", flush=True)
    time.sleep(3600)
EOF
    put_file "$t/holder.py" "$FIX_DIR/holder.py" 644

    # Read-only helpers for the item 15.2 checks (run as $SVC_USER / alice, never root, so no
    # root-owned sidecar can appear). All use the DEFAULT database path.
    # dbgen.py: print the stored config generation.
    cat > "$t/dbgen.py" <<'EOF'
from zfsbackup.config.store import dispose_all, read_config, resolve_config_path
print(read_config(resolve_config_path(None)).generation)
dispose_all()
EOF
    put_file "$t/dbgen.py" "$FIX_DIR/dbgen.py" 644
    # fdprobe.py: POSITIVE CONTROL for the "no config.db fd" check - open the DB, then print
    # this process's own /proc/self/fd link targets.
    cat > "$t/fdprobe.py" <<'EOF'
import os
from zfsbackup.config.store import open_config_connection, resolve_config_path
with open_config_connection(resolve_config_path(None), readonly=True) as conn:
    conn.exec_driver_sql("SELECT count(*) FROM sqlite_master").fetchall()
    for fd in os.listdir("/proc/self/fd"):
        try:
            print(os.readlink("/proc/self/fd/" + fd))
        except OSError:
            pass
EOF
    put_file "$t/fdprobe.py" "$FIX_DIR/fdprobe.py" 644
    # probe.py: the daemon-presence probe the CLI runs after a write, on its own.
    cat > "$t/probe.py" <<'EOF'
from zfsbackup import daemonlock
from zfsbackup.config.store import resolve_config_path
info = daemonlock.probe(resolve_config_path(None))
print("none" if info is None else "pid=%s generation=%s" % (info.pid, info.generation))
EOF
    put_file "$t/probe.py" "$FIX_DIR/probe.py" 644
    # forger.py: group (f4). Take LOCK_EX on a pid file and write a FORGED pid file naming some other
    # process (pid + its correct start token), then hold the lock until killed. Prints READY.
    # usage: forger.py <pidfile> <pid> <start-token> <store-id> <generation>
    cat > "$t/forger.py" <<'EOF'
import fcntl, os, sys, time
path, pid, token, store, gen = sys.argv[1:6]
fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o660)
fcntl.flock(fd, fcntl.LOCK_EX)
text = "%s\n%s\nstart=%s\napplied=%s:%s\n" % (pid, gen, token, store, gen)
os.pwrite(fd, text.encode(), 0)
os.ftruncate(fd, len(text))
print("READY", flush=True)
time.sleep(600)
EOF
    put_file "$t/forger.py" "$FIX_DIR/forger.py" 644
    # reloadreason.py: ask request_reload() what it would do and why (outcome + reason_code), exactly as the
    # CLI does after a write. It SIGHUPs only a verified daemon; every forgery below must be refused.
    cat > "$t/reloadreason.py" <<'EOF'
from zfsbackup import daemonlock
from zfsbackup.config.store import resolve_config_path
r = daemonlock.request_reload(resolve_config_path(None))
print("outcome=%s reason=%s pid=%s" % (r.outcome, r.reason_code, r.pid))
EOF
    put_file "$t/reloadreason.py" "$FIX_DIR/reloadreason.py" 644
    # forger2.py: group (f4c), the recycled-pid shape. Take LOCK_EX on the pid file, fork a child that
    # inherits the descriptor (so the lock outlives the parent's fd), write a pid file naming the PARENT
    # with its real start token, then CLOSE the parent's descriptor and stay alive. The lock table still
    # names the parent as the locker (step 7 passes) but the parent no longer has the file open (step 8b).
    # usage: forger2.py <pidfile> <store-id> <generation>
    cat > "$t/forger2.py" <<'EOF'
import fcntl, os, sys, time
from zfsbackup import procident
path, store, gen = sys.argv[1:4]
fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o660)
fcntl.flock(fd, fcntl.LOCK_EX)
if os.fork() == 0:
    time.sleep(600)
    os._exit(0)
pid = os.getpid()
text = "%s\n%s\nstart=%s\napplied=%s:%s\n" % (pid, gen, procident.start_token(pid), store, gen)
os.pwrite(fd, text.encode(), 0)
os.ftruncate(fd, len(text))
os.close(fd)
print("READY", flush=True)
time.sleep(600)
EOF
    put_file "$t/forger2.py" "$FIX_DIR/forger2.py" 644
    # wlock.py: group (f6b). Hold a SHARED lock on <db>.workers.lock, as a surviving worker of a previous
    # daemon would. usage: wlock.py <workers-lock-path>
    cat > "$t/wlock.py" <<'EOF'
import fcntl, os, sys, time
fd = os.open(sys.argv[1], os.O_RDONLY)
fcntl.flock(fd, fcntl.LOCK_SH)
print("READY", flush=True)
time.sleep(600)
EOF
    put_file "$t/wlock.py" "$FIX_DIR/wlock.py" 644
    # ensure_client.py: group (i2). What `enroll` does first: create this host's client TLS identity in <db>.tls (default path).
    cat > "$t/ensure_client.py" <<'EOF'
from zfsbackup import identity
from zfsbackup.config.store import resolve_config_path
r = resolve_config_path(None)
print(identity.ensure_identity(identity.tls_dir(getattr(r, "resolved_path", r)), identity.ROLE_CLIENT).fingerprint)
EOF
    put_file "$t/ensure_client.py" "$FIX_DIR/ensure_client.py" 644
    # startmethod.py: the multiprocessing start method the daemon gets on this VM.
    cat > "$t/startmethod.py" <<'EOF'
import multiprocessing as m
print(m.get_start_method())
EOF
    put_file "$t/startmethod.py" "$FIX_DIR/startmethod.py" 644
    # breakconfig.py: group (f5). Make the stored config invalid (api_port out of range) with a RAW
    # sqlite3 UPDATE that also bumps the generation - what a hand-edit or a buggy tool would do.
    cat > "$t/breakconfig.py" <<'EOF'
import sqlite3, sys
con = sqlite3.connect(sys.argv[1], timeout=20)
con.execute("UPDATE global_settings SET api_port = 70000, generation = generation + 1")
con.commit()
print("generation", con.execute("SELECT generation FROM global_settings").fetchone()[0])
con.close()
EOF
    put_file "$t/breakconfig.py" "$FIX_DIR/breakconfig.py" 644
    # httpget.py: group (g). GET https://127.0.0.1:<port>/health (the API is HTTPS only; /health needs no client certificate
    # and the certificate is not verified here -- this probe is about the PORT) and print the status code, or "ERR".
    cat > "$t/httpget.py" <<'EOF'
import ssl, sys, urllib.request
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE
try:
    print(urllib.request.urlopen("https://127.0.0.1:%s/health" % sys.argv[1], timeout=3, context=ctx).status)
except Exception:
    print("ERR")
EOF
    put_file "$t/httpget.py" "$FIX_DIR/httpget.py" 644
    # portholder.py: group (g). Listen on 127.0.0.1:<port> until killed, so a bind there fails with EADDRINUSE.
    cat > "$t/portholder.py" <<'EOF'
import socket, sys, time
s = socket.socket()
s.bind(("127.0.0.1", int(sys.argv[1])))
s.listen(1)
print("READY", flush=True)
time.sleep(3600)
EOF
    put_file "$t/portholder.py" "$FIX_DIR/portholder.py" 644
    # The Type=notify-reload unit for group (h), as zfsbackup/README.md documents it (systemd >= 253; the
    # variant for older systemd is written by the scenario). Runs the daemon --dry-run against the stub zfs.
    cat > "$t/zfsperm-notify.service" <<EOF
[Unit]
Description=zfsbackup scenario unit (Type=notify-reload)

[Service]
Type=__SDTYPE__
NotifyAccess=main
User=$SVC_USER
Group=$SVC_GROUP
Environment=PYTHONPATH=$SRC_DIR:$SRC_DIR/libs/libzfseasy PYTHONDONTWRITEBYTECODE=1 ZFS_CMD=$FIX_DIR/zfs ZPOOL_CMD=$FIX_DIR/zfs
ExecStart=$PY -m zfsbackup.daemon --dry-run -v
__EXECRELOAD__
KillMode=mixed
TimeoutStopSec=90
StandardOutput=append:$LOG
StandardError=append:$LOG
EOF
    put_file "$t/zfsperm-notify.service" "$FIX_DIR/zfsperm-notify.service.in" 644
    rm -rf "$t"

    # fixtures from the committed example
    msh "
        set -e
        cd '$FIX_DIR'
        '$PY' mkfixture.py '$SRC_DIR/zfsbackup/config.example.yaml' root.yaml  all  rootsnap
        '$PY' mkfixture.py '$SRC_DIR/zfsbackup/config.example.yaml' alice.yaml 2    alicesnap
        '$PY' mkfixture.py '$SRC_DIR/zfsbackup/config.example.yaml' bob.yaml   3    bobsnap
        chmod 644 *.yaml
        ls -l '$FIX_DIR'
    " || die "fixture generation failed"
}

# ─────────────────────────────── State helpers ──────────────────────────────
stat_of()  { msh "stat -c '%a %U %G' '$1' 2>/dev/null"; }     # "<octal> <owner> <group>"
exists()   { msh "test -e '$1'" 2>/dev/null; }

stop_daemon() {
    # All suppression happens in the guest; host stdout is never sent to /dev/null.
    msh "{ systemctl stop $UNIT; systemctl reset-failed $UNIT; systemctl stop $HOLD_UNIT; systemctl reset-failed $HOLD_UNIT; systemctl stop $FORGE_UNIT $DECOY_UNIT $WLOCK_UNIT $PORT_UNIT $NOTIFY_UNIT; systemctl reset-failed $FORGE_UNIT $DECOY_UNIT $WLOCK_UNIT $PORT_UNIT $NOTIFY_UNIT; } >/dev/null 2>&1; true" || true
}

# Wipe the default config state: units, DB + WAL sidecars (all three — a stale -wal next to a
# fresh DB is a confusing failure) and, when asked, the directory itself so root re-creates it.
reset_default_state() { # reset_default_state [keep-dir]
    stop_daemon
    local rmdir="rm -rf '$CFG_DIR'"; [ -n "${1:-}" ] && rmdir=":"
    msh "{ rm -f '$DB' '$DB-wal' '$DB-shm' '$DB.daemon.pid' '$DB.writer.lock' '$DB.workers.lock'; rm -rf '$DB.tls'; $rmdir; } >/dev/null 2>&1; : >'$LOG'; chown '$SVC_USER' '$LOG'; true" || true
}

start_daemon() { # start_daemon [extra daemon args...]  — runs as $SVC_USER on the default DB
    msh "
        systemctl reset-failed $UNIT 2>/dev/null || true
        systemd-run --unit=$UNIT --collect -p User=$SVC_USER -p Group=$SVC_GROUP \
            -p StandardOutput=append:'$LOG' -p StandardError=append:'$LOG' \
            '$ENV_WRAP' '$PY' -m zfsbackup.daemon --dry-run -v $*
    " || die "could not start daemon"
}
daemon_active() { msh "systemctl is-active --quiet $UNIT" 2>/dev/null; }
daemon_pid()    { msh "systemctl show -p MainPID --value $UNIT" | tr -d '[:space:]'; }
# Worker pids come from the supervisor's own "Started <name> worker (pid=N)" log lines (the
# last one wins, so respawns are followed). `pgrep -P` is NOT used: the supervisor's startup
# also spawns short-lived `zfs` children that it would pick up.
worker_pid() { # worker_pid <snapshot|pruning|api>   (a live reload logs "Reloaded <name> worker")
    msh "grep -oE '(Started|Restarted|Reloaded) $1 worker \\(pid=[0-9]+\\)' '$LOG' | tail -n1 | grep -oE '[0-9]+'" 2>/dev/null | tr -d '[:space:]'
}
wait_workers_started() { # all three workers spawned (api is spawned last)
    local waited=0
    until [ -n "$(worker_pid api)" ] && [ -n "$(worker_pid snapshot)" ] && [ -n "$(worker_pid pruning)" ]; do
        [ "$waited" -ge "$DAEMON_TIMEOUT" ] && return 1; sleep 2; waited=$((waited+2))
    done
    sleep 3                              # let each worker finish its first config load
}
# open_db_fds <pid> — readlink targets of that process's fds that ARE the config DB or a WAL
# sidecar. (Exact match: a forked worker legitimately inherits the <db>.daemon.pid lock fd, which
# a substring match on "config.db" would wrongly flag.) Root reads /proc/<pid>/fd.
open_db_fds() {
    msh "for f in /proc/$1/fd/*; do readlink \"\$f\"; done 2>/dev/null | grep -xE '$DB(-wal|-shm)?( \\(deleted\\))?'; true"
}
# "<mode> <group>" of a path (owner dropped)
mode_group() { stat_of "$1" | awk '{print $1, $3}'; }
PIDFILE="${DB}.daemon.pid"
WLOCK="${DB}.writer.lock"
WORKERS_LOCK="${DB}.workers.lock"
NOTIFY_UNIT="zfsperm-notify"
PORT_UNIT="zfsperm-portholder"
FORGE_UNIT="zfsperm-forger"
WLOCK_UNIT="zfsperm-wlock"
DECOY_UNIT="zfsperm-decoy"
# Pid file v2: lines 1-2 are "pid" and "generation"; from line 3 on, key=value lines
# (start=, applied=<store>:<gen>, target=, rejected=).
pf2()    { msh "head -n2 '$PIDFILE' | paste -sd,"; }                    # "<pid>,<generation>"
pf_val() { msh "sed -n 's/^$1=//p' '$PIDFILE' 2>/dev/null | head -n1" | tr -d '[:space:]'; }
# cli_as <who> <zfsbackup-config args...>  — who is root (via msh) or a user (via asu)
cli_as() {
    local who="$1"; shift
    if [ "$who" = root ]; then msh "'$ENV_WRAP' '$PY' -m zfsbackup.cli $* 2>&1"; else asu "$who" "'$PY' -m zfsbackup.cli $* 2>&1"; fi
}
log_lines()  { msh "wc -l < '$LOG'" | tr -d '[:space:]'; }
log_since()  { msh "tail -n +$(( $1 + 1 )) '$LOG'"; }

wait_log() { # wait_log <from-line> <ERE> [timeout]  — wait for a pattern after line N
    local from="$1" re="$2" timeout="${3:-$DAEMON_TIMEOUT}" waited=0
    while :; do
        log_since "$from" | grep -qE -- "$re" && return 0
        [ "$waited" -ge "$timeout" ] && return 1
        sleep 2; waited=$((waited+2))
    done
}
# wait_workers_changed "<snapshot pid> <pruning pid> <api pid>" [timeout] — every worker pid differs from the old one
wait_workers_changed() {
    local timeout="${2:-45}" waited=0 b_snap b_prune b_api
    read -r b_snap b_prune b_api <<<"$1"
    while :; do
        [ "$(worker_pid snapshot)" != "$b_snap" ] && [ "$(worker_pid pruning)" != "$b_prune" ] \
            && [ "$(worker_pid api)" != "$b_api" ] && return 0
        [ "$waited" -ge "$timeout" ] && return 1
        sleep 2; waited=$((waited+2))
    done
}
# wait_pf_re <key> <ERE> [timeout] — wait until the pid file's key=value line matches
wait_pf_re() {
    local key="$1" re="$2" timeout="${3:-45}" waited=0
    while :; do
        pf_val "$key" | grep -qE -- "$re" && return 0
        [ "$waited" -ge "$timeout" ] && return 1
        sleep 2; waited=$((waited+2))
    done
}
wait_unit_inactive() { # wait_unit_inactive [timeout]
    local timeout="${1:-$DAEMON_TIMEOUT}" waited=0
    while daemon_active; do
        [ "$waited" -ge "$timeout" ] && return 1
        sleep 2; waited=$((waited+2))
    done
}

# Start the SIGHUP-able holder as <user>; wait for READY; echo nothing. Returns 1 on failure.
start_holder() { # start_holder <user> [db-path]
    local user="$1" db="${2:-}"
    msh "
        systemctl reset-failed $HOLD_UNIT 2>/dev/null || true
        : > /tmp/holder.out; chmod 666 /tmp/holder.out
        systemd-run --unit=$HOLD_UNIT --collect -p User=$user \
            -p StandardOutput=append:/tmp/holder.out -p StandardError=append:/tmp/holder.out \
            '$ENV_WRAP' '$PY' '$FIX_DIR/holder.py' $db
    " || return 1
    local waited=0
    while ! msh "grep -q READY /tmp/holder.out" 2>/dev/null; do
        [ "$waited" -ge 30 ] && { warn "holder never printed READY: $(msh 'cat /tmp/holder.out')"; return 1; }
        sleep 1; waited=$((waited+1))
    done
}
kill_holder_hup() {
    msh "systemctl kill -s HUP --kill-whom=main $HOLD_UNIT" || return 1
    local waited=0
    while msh "systemctl is-active --quiet $HOLD_UNIT" 2>/dev/null; do
        [ "$waited" -ge 20 ] && return 1
        sleep 1; waited=$((waited+1))
    done
}

# ═════════════════════════════════ Main ═════════════════════════════════════
command -v multipass >/dev/null 2>&1 || die "multipass not found on host — install it first"
command -v git >/dev/null 2>&1 || die "git not found on host"

echo "${BOLD}=================================================================${NC}"
echo "${BOLD} zfsbackup config-DB group-operator permission scenarios${NC}"
echo "${BOLD}=================================================================${NC}"

ensure_vm
install_stack
make_identities
install_fixtures

# ───────────────────────────────── (a) ──────────────────────────────────────
echo
step "CHECK (a) — root's first import of the default path"
reset_default_state                     # also removes the directory: root must CREATE it
out="$(msh "'$ENV_WRAP' '$PY' -m zfsbackup.cli import '$FIX_DIR/root.yaml' 2>&1")"; rc=$?
check "root import exits 0" "0" "$rc"
check_re "CLI reports a new database" "into new database $DB" "$out"
check "first import writes no pre-import backup (nothing to back up)" "" "$(msh "ls $CFG_DIR | grep pre-import")"
check_no_re "no 'group does not exist' warning (group was provisioned first)" "does not exist" "$out"
check "config directory is setgid 2770 root:zfsbackup (Q7 chgrp)" "2770 root $SVC_GROUP" "$(stat_of "$CFG_DIR")"
check "config database is 660 root:zfsbackup" "660 root $SVC_GROUP" "$(stat_of "$DB")"
leftover="$(msh "ls $DB-wal $DB-shm 2>/dev/null")"
check "no root-owned sidecars left behind after the import" "" "$leftover"

# ───────────────────────────────── (b) ──────────────────────────────────────
echo
step "CHECK (b) — daemon runs as $SVC_USER while operators re-import"
start_daemon
waited=0
wait_workers_started || warn "workers did not all start"
if daemon_active; then pass "daemon started as $SVC_USER on the default DB"; else
    fail "daemon not active: $(msh "tail -n 15 '$LOG'")"
fi
check "daemon main process runs as $SVC_USER" "$SVC_USER" "$(msh "ps -o user= -p $(daemon_pid)" | tr -d '[:space:]')"
pid_before="$(daemon_pid)"; workers_before="snapshot=$(worker_pid snapshot) pruning=$(worker_pid pruning) api=$(worker_pid api)"
mark_imports="$(log_lines)"
info "supervisor pid=$pid_before workers=[$workers_before]"
check_re "daemon loaded the original config as $SVC_USER" "Loading configuration from" "$(msh "cat '$LOG'")"

# Item 15.2: one startup load, its generation logged and recorded in <db>.daemon.pid.
gen0="$(msh "grep -oE 'Configuration generation [0-9]+' '$LOG' | head -n1 | grep -oE '[0-9]+\$'" | tr -d '[:space:]')"
check_re "daemon logs 'Configuration generation N, schema revision R' at startup" \
    "Configuration generation [0-9]+, schema revision [0-9a-f]+" "$(msh "cat '$LOG'")"
check "startup load logged exactly once" "1" "$(msh "grep -c 'Configuration generation' '$LOG'" | tr -d '[:space:]')"
check "pid file starts '<supervisor pid>,<startup generation>' ($pid_before,$gen0)" "$pid_before,$gen0" "$(pf2)"
check_re "pid file is v2: it carries a start= token (reload-capable)" "^linux:[0-9]+\$" "$(pf_val start)"
check_re "pid file records applied=<store>:$gen0" "^[0-9]+:$gen0\$" "$(pf_val applied)"
check "pid file is 660 group $SVC_GROUP" "660 $SVC_GROUP" "$(mode_group "$PIDFILE")"
check "writer lock file exists and is 660 group $SVC_GROUP" "660 $SVC_GROUP" "$(mode_group "$WLOCK")"
check "supervisor has no config.db fd open (it disposed its reader)" "" "$(open_db_fds "$pid_before")"
for w in snapshot pruning api; do
    check "$w worker (pid $(worker_pid $w)) holds no config.db fd (config handed over, not read)" "" "$(open_db_fds "$(worker_pid $w)")"
done
check_re "positive control: a process that DOES open the DB shows it in /proc/<pid>/fd" "^$DB\$" \
    "$(asu "$SVC_USER" "'$PY' '$FIX_DIR/fdprobe.py' 2>&1")"

# A group operator can see the daemon: the probe the CLI runs works read-only for alice.
pf_before="$(msh "cksum '$PIDFILE'; stat -c %Y '$PIDFILE'")"
check "alice's probe sees the running daemon (pid, startup generation)" "pid=$pid_before generation=$gen0" \
    "$(asu alice "'$PY' '$FIX_DIR/probe.py' 2>&1")"
check "the probe wrote nothing (pid file checksum and mtime unchanged)" "$pf_before" "$(msh "cksum '$PIDFILE'; stat -c %Y '$PIDFILE'")"

# A second daemon on the same DB, as the service user, must refuse and name the first pid.
out="$(asu "$SVC_USER" "'$PY' -m zfsbackup.daemon --dry-run 2>&1")"; rc=$?
check "second daemon on the same DB exits 1" "1" "$rc"
check_re "second daemon names the first daemon's pid ($pid_before)" "daemon \(pid $pid_before\) is already running" "$out"
check_no_re "second daemon started no workers" "Started .* worker" "$out"
daemon_active && pass "first daemon unaffected by the refused second one" || fail "first daemon died"
check "pid file untouched by the refused second daemon" "$pid_before,$gen0" "$(pf2)"

for op in alice bob; do
    out="$(asu "$op" "'$PY' -m zfsbackup.cli import '$FIX_DIR/$op.yaml' 2>&1")"; rc=$?
    check "$op's re-import (daemon running) exits 0" "0" "$rc"
    check_re "$op's re-import reports success" "Imported .*$op.yaml into $DB" "$out"
    check_re "$op's re-import warns it replaced the stored configuration" "replaced the stored configuration in $DB" "$out"
    bk="$(printf '%s' "$out" | grep -oE "$CFG_DIR/config\.db\.pre-import-[0-9TZ-]+\.yaml" | head -n1)"
    if [ -n "$bk" ]; then
        check "$op's pre-import backup is 660 $op:$SVC_GROUP (setgid dir inheritance)" "660 $op $SVC_GROUP" "$(stat_of "$bk")"
        check_re "$op's pre-import backup is a readable YAML config" "^datasets:" "$(msh "cat '$bk'")"
    else
        fail "$op's re-import reported no pre-import backup path: $(printf '%s' "$out" | tr '\n' '|' | cut -c1-300)"
    fi
    check_no_re "$op's re-import: no EPERM / permission / open failure" "EPERM|Operation not permitted|Permission denied|PermissionError|unable to open" "$out"
    # Phase 3: the CLI cannot signal a daemon of another uid (operators are not the service user), so it
    # says so and promises the poll will pick the write up. It never says "restart".
    check_re "$op's CLI says daemon pid $pid_before runs as another user, was not signalled, and picks the import up within 10s" \
        "daemon pid $pid_before runs as another user and was not signalled; it picks up the change within 10s" "$out"
    check_no_re "$op's CLI no longer tells the operator to restart the daemon" "until restarted|restart it" "$out"
done
check "database still 660 group $SVC_GROUP after operators' imports" "660 root $SVC_GROUP" "$(stat_of "$DB")"
check "directory still 2770 root:$SVC_GROUP after operators' imports" "2770 root $SVC_GROUP" "$(stat_of "$CFG_DIR")"

daemon_active && pass "daemon still running after the operators' imports" || fail "daemon died during operators' imports"
check "supervisor pid unchanged" "$pid_before" "$(daemon_pid)"
# Phase 3 (live reload): nothing signalled the daemon, so its 10s generation poll picks the imports up,
# and every worker turns over at a safe point - the pids change, the supervisor's does not.
if wait_log "$mark_imports" "accepted \\(was [0-9]+; trigger: poll\\)" 25; then
    pass "daemon picked the operators' imports up by its generation poll (trigger: poll)"
else
    fail "daemon never accepted the operators' imports by poll: $(log_since "$mark_imports" | tail -n 8 | tr '\n' '|')"
fi
asu bob "'$PY' -m zfsbackup.cli status --wait 40 >/dev/null 2>&1" ; rc=$?
check "bob's 'status --wait 40' reports the daemon in sync with the imports (exit 0)" "0" "$rc"
check_no_re "no worker crash logged while applying the imports" "exited with code [1-9]|crash" "$(log_since "$mark_imports")"
if [ "$(worker_pid snapshot)" != "$(printf '%s' "$workers_before" | sed -n 's/.*snapshot=\([0-9]*\).*/\1/p')" ]; then
    pass "workers were turned over onto the imported configuration (new pids, same supervisor)"
else
    fail "snapshot worker pid unchanged after the imports were applied"
fi

# Item 15.2(b): a respawned worker is handed the supervisor's CURRENT config; it does not read the DB.
# (Phase 3: "current" now follows the DB through the supervisor's reloads, but only the supervisor reads.)
# Bump the DB generation with one more operator re-import, let the daemon apply it, THEN kill -9 a worker.
out="$(asu bob "'$PY' -m zfsbackup.cli import --no-backup '$FIX_DIR/bob.yaml' 2>&1")"; rc=$?
check "bob's re-import before the kill exits 0" "0" "$rc"
check_re "CLI notice on the re-import: daemon pid $pid_before was not signalled, picks it up within 10s" \
    "daemon pid $pid_before runs as another user and was not signalled; it picks up the change within 10s" "$out"
gen_cli="$(printf '%s' "$out" | sed -n 's/.*config updated (generation \([0-9]*\)).*/\1/p' | head -n1)"
gen_db="$(asu "$SVC_USER" "'$PY' '$FIX_DIR/dbgen.py' 2>&1" | tr -d '[:space:]')"
check "DB generation matches the generation the CLI reported" "$gen_cli" "$gen_db"
if [ -n "$gen_db" ] && [ "$gen_db" -gt "$gen0" ] 2>/dev/null; then pass "DB generation ($gen_db) is ahead of the daemon's startup generation ($gen0)"; else
    fail "DB generation ($gen_db) did not advance past the daemon's ($gen0)"
fi
asu bob "'$PY' -m zfsbackup.cli status --wait 40 >/dev/null 2>&1" ; rc=$?
check "daemon applies bob's last import by poll ('status --wait 40' exits 0)" "0" "$rc"
check "pid file now records applied=<store>:$gen_db and line 2 = $gen_db" "$pid_before,$gen_db" "$(pf2)"
check_re "pid file applied= is the DB generation $gen_db" "^[0-9]+:$gen_db\$" "$(pf_val applied)"
check "supervisor pid still unchanged after the poll-applied reloads" "$pid_before" "$(daemon_pid)"
for w in snapshot pruning api; do
    check "$w worker (pid $(worker_pid $w)) still holds no config.db fd after the reload (config handed over, not read)" "" "$(open_db_fds "$(worker_pid $w)")"
done
check "supervisor still holds no config.db fd after the reloads" "" "$(open_db_fds "$pid_before")"

victim="$(worker_pid snapshot)"
mark="$(log_lines)"
msh "kill -9 $victim" || true
if wait_log "$mark" "Restarted snapshot worker" 90; then pass "supervisor respawned the killed snapshot worker"; else
    fail "no respawn logged after killing worker $victim"
fi
sleep 5                                  # give a (wrongly) re-reading worker time to show it
after="$(log_since "$mark")"
newpid="$(worker_pid snapshot)"
if [ -n "$newpid" ] && [ "$newpid" != "$victim" ] && msh "test -d /proc/$newpid"; then pass "respawned worker is a new live process (pid $victim -> $newpid)"; else
    fail "respawned worker pid not new/alive (victim=$victim new=$newpid)"
fi
check_no_re "respawned worker raised no config/permission error" "ConfigDb|EX_CONFIG|unable to open|Failed to load|ConfigPathError|SchemaError" "$after"
check_no_re "respawn logged no second configuration load" "Loading configuration from|Configuration generation" "$after"
check "daemon log has only the startup 'Configuration generation' line (N=$gen0): reloads do not re-log it" "Configuration generation $gen0" \
    "$(msh "grep -oE 'Configuration generation [0-9]+' '$LOG'" | tr -d '\r')"
check "a worker respawn leaves the pid file on the applied generation $gen_db" "$pid_before,$gen_db" "$(pf2)"
check "respawned worker (pid $newpid) holds no config.db fd (handed the config, did not read the DB)" "" "$(open_db_fds "$newpid")"
for w in pruning api; do
    check "$w worker still holds no config.db fd after the respawn" "" "$(open_db_fds "$(worker_pid $w)")"
done
daemon_active && pass "daemon alive after respawn" || fail "daemon died after worker respawn"

out="$(asu "$SVC_USER" "'$PY' -m zfsbackup.daemon --test-config 2>&1")"; rc=$?
check "$SVC_USER --test-config (concurrent with the running daemon) exits 0" "0" "$rc"
check_re "$SVC_USER sees bob's last import (3 datasets, not root's 5)" "Datasets: 3" "$out"
stop_daemon
check "clean stop removed the pid file" "gone" "$(exists "$PIDFILE" && echo present || echo gone)"

# ───────────────────────────────── (c) ──────────────────────────────────────
echo
step "CHECK (c) — alice is the first opener and is killed by SIGHUP while holding the DB"
sleep 2
check "precondition: no sidecars exist (alice will be the first opener)" "" "$(msh "ls $DB-wal $DB-shm 2>/dev/null")"
if start_holder alice; then
    pass "alice holds the DB open (first opener)"
    check_re "sidecars exist while alice holds the DB" "config.db-wal" "$(msh "ls $DB-wal $DB-shm 2>&1")"
    if kill_holder_hup; then
        pass "alice's process terminated by SIGHUP"
    else
        fail "alice's holder did not terminate on SIGHUP"
    fi
    for sc in "$DB-wal" "$DB-shm"; do
        if exists "$sc"; then
            check "leftover $(basename "$sc") belongs to alice, group $SVC_GROUP (setgid dir inheritance)" "alice $SVC_GROUP" "$(stat_of "$sc" | cut -d' ' -f2-)"
            m="$(stat_of "$sc" | cut -d' ' -f1)"
            check_re "leftover $(basename "$sc") is group-writable (mode $m)" "^[0-7]*[2367][0-7]$" "$m"
        else
            fail "sidecar $(basename "$sc") not left behind (SIGHUP cleaned up?) — check not exercised"
        fi
    done
    out="$(asu "$SVC_USER" "'$PY' -m zfsbackup.daemon --test-config 2>&1")"; rc=$?
    check "$SVC_USER --test-config opens the DB over alice's leftover sidecars (exit 0)" "0" "$rc"
    check_re "daemon reports the config as valid" "Configuration is valid" "$out"
    check_no_re "no preflight/permission error" "ConfigDb|unable to open|Permission denied" "$out"
    # and the full daemon, to be sure workers (not just --test-config) get through too
    start_daemon
    sleep 12
    daemon_active && pass "full daemon runs as $SVC_USER over alice's leftover sidecars" || fail "daemon failed: $(msh "tail -n 15 '$LOG'")"
    check_no_re "daemon log shows no config/permission error" "ConfigDb|EX_CONFIG|unable to open|Failed to load" "$(msh "cat '$LOG'")"
    stop_daemon
else
    fail "could not start alice's holder"
fi

# ───────────────────────────────── (d) ──────────────────────────────────────
echo
step "CHECK (d) — a non-default 0600 -c file stays 0600 across re-import"
msh "
    rm -rf /home/alice/cfg /var/lib/zfsb-perm-root
    install -d -o alice -g alice -m 755 /home/alice/cfg
    install -d -m 755 /var/lib/zfsb-perm-root
" || die "could not create (d) directories"
# import_as <who> <yaml> <db> [extra import flags]  — who is alice (via sudo -u) or root
import_as() {
    local who="$1" yml="$2" dbp="$3" flags="${4:-}"
    local cmd="'$PY' -m zfsbackup.cli -c '$dbp' import $flags '$FIX_DIR/$yml' 2>&1"
    if [ "$who" = root ]; then msh "'$ENV_WRAP' $cmd"; else asu "$who" "$cmd"; fi
}
for who in alice root; do
    if [ "$who" = alice ]; then dbp=/home/alice/cfg/custom.db; else dbp=/var/lib/zfsb-perm-root/custom.db; fi
    yml="$who.yaml"; [ "$who" = root ] && yml=root.yaml
    out="$(import_as "$who" "$yml" "$dbp")"; rc=$?
    check "$who: first import to non-default $dbp exits 0" "0" "$rc"
    msh "chmod 0600 '$dbp'"
    check "$who: precondition — database is 0600" "600" "$(stat_of "$dbp" | cut -d' ' -f1)"
    out="$(import_as "$who" "$yml" "$dbp")"; rc=$?
    check "$who: re-import over the 0600 file exits 0" "0" "$rc"
    check_re "$who: re-import took the existing-database branch" "replaced the stored configuration in $dbp" "$out"
    check "$who: database is STILL 0600 after re-import" "600" "$(stat_of "$dbp" | cut -d' ' -f1)"
    bk="$(printf '%s' "$out" | grep -oE "$dbp\.pre-import-[0-9TZ-]+\.yaml" | head -n1)"
    if [ -n "$bk" ]; then
        check "$who: pre-import backup is 660 (independent of the 0600 database)" "660" "$(stat_of "$bk" | cut -d' ' -f1)"
    else
        fail "$who: re-import reported no pre-import backup path"
    fi
    nb_before="$(msh "ls $(dirname "$dbp") | grep -c pre-import")"
    out="$(import_as "$who" "$yml" "$dbp" --no-backup)"; rc=$?
    check "$who: --no-backup re-import exits 0" "0" "$rc"
    check_re "$who: --no-backup re-import says nothing was backed up" "NOT backed up" "$out"
    check "$who: --no-backup wrote no new backup file" "$nb_before" "$(msh "ls $(dirname "$dbp") | grep -c pre-import")"
    check "$who: database is STILL 0600 after --no-backup re-import" "600" "$(stat_of "$dbp" | cut -d' ' -f1)"
done

# ───────────────────────────────── (e) ──────────────────────────────────────
echo
step "CHECK (e) — setgid removed: operator sidecar in alice's primary group -> clear preflight error"
reset_default_state keep-dir            # fresh DB (and no stale -wal/-shm) in the existing dir
out="$(msh "'$ENV_WRAP' '$PY' -m zfsbackup.cli import '$FIX_DIR/root.yaml' 2>&1")"; rc=$?
check "re-seed the default DB as root" "0" "$rc"
msh "chgrp $SVC_GROUP '$CFG_DIR' '$DB'; chmod 2770 '$CFG_DIR'; chmod 660 '$DB'"   # reset_default_state kept the dir
msh "chmod g-s '$CFG_DIR'" || die "could not remove the setgid bit"
check "setgid bit removed from the directory (mode 770)" "770 root $SVC_GROUP" "$(stat_of "$CFG_DIR")"
if start_holder alice; then
    kill_holder_hup || warn "holder did not exit on SIGHUP"
    for sc in "$DB-wal" "$DB-shm"; do
        if exists "$sc"; then
            check "leftover $(basename "$sc") is in alice's PRIMARY group (no setgid)" "alice alice" "$(stat_of "$sc" | cut -d' ' -f2-)"
        else
            fail "sidecar $(basename "$sc") not left behind — check not exercised"
        fi
    done
    out="$(asu "$SVC_USER" "'$PY' -m zfsbackup.daemon --test-config 2>&1")"; rc=$?
    check "$SVC_USER --test-config refuses (non-zero exit)" "1" "$rc"
    check_re "refusal is a ConfigDbPermissionError-class diagnosis naming the WAL sidecar" "WAL sidecar .*config.db-(wal|shm)" "$out"
    check_re "message names the owner/group of the sidecar and the fix" "owner=alice group=alice.*chmod 0660.*chgrp $SVC_GROUP" "$out"
    check_no_re "NOT the opaque error (no raw sqlite3.OperationalError / SQLAlchemy traceback)" "OperationalError|sqlalche\\.me|Traceback" "$out"

    # (e2) the FULL daemon over the same state must refuse at startup, loudly, and start no
    # workers. (Why not "alice leaves a bad sidecar beside a RUNNING daemon"? A running daemon's
    # workers hold pooled connections for their whole life, so THEY are the sidecars' owner and
    # alice can never be the first opener while it runs. The worker-level EX_CONFIG (78) path is
    # only reachable by racing a sidecar in between the supervisor's load and a worker's load —
    # not deterministic, so not asserted here.)
    echo
    step "CHECK (e2) — the full daemon refuses to start over the unusable sidecar"
    mark="$(log_lines)"
    start_daemon
    if wait_unit_inactive 60; then pass "daemon unit exited by itself (refused to run)"; else
        fail "daemon is still running over an unusable sidecar"
    fi
    tail_log="$(log_since "$mark")"
    check_re "daemon log names the unusable sidecar and the fix" "WAL sidecar .*chgrp $SVC_GROUP" "$tail_log"
    check_no_re "daemon started no workers" "Started .* worker" "$tail_log"
    check_no_re "daemon log has no raw sqlite3.OperationalError / traceback (diagnosed, not opaque)" "OperationalError|sqlalche\\.me|Traceback" "$tail_log"

    # (e3) the config CLI over the same state: must surface the same diagnosis, not the opaque
    # sqlite error, and use the "config database unusable" exit code (78). The CLI runs as the
    # service user (a group member, so it passes the file-permission checks and reaches the
    # sidecar problem); --no-backup keeps the failure about the sidecar alone.
    echo
    step "CHECK (e3) — the config CLI reports the unusable sidecar the same way"
    out="$(asu "$SVC_USER" "'$PY' -m zfsbackup.cli import --no-backup '$FIX_DIR/alice.yaml' 2>&1")"; rc=$?
    check "CLI import over the unusable sidecar exits 78 (config database unusable)" "78" "$rc"
    check_re "CLI names the WAL sidecar" "WAL sidecar .*config.db-(wal|shm)" "$out"
    check_re "CLI names the sidecar's owner/group and the fix" "owner=alice group=alice.*chmod 0660.*chgrp $SVC_GROUP" "$out"
    check_no_re "CLI error is not the raw sqlite3.OperationalError / traceback" "OperationalError|sqlalche\\.me|Traceback" "$out"
else
    fail "could not start alice's holder (e)"
fi
stop_daemon

# ───────────────────────────────── (f) ──────────────────────────────────────
# Phase 3 (live reload). The daemon runs as $SVC_USER under its transient unit; the CLI writers are root,
# a group operator (alice: a different uid, so SIGHUP is EPERM) and $SVC_USER itself (same uid).
echo
step "CHECK (f) — live config reload (SIGHUP and the 10s generation poll)"
START_METHOD="$(msh "'$ENV_WRAP' '$PY' '$FIX_DIR/startmethod.py'" | tr -d '[:space:]')"
info "multiprocessing start method on $PERM_VM: ${START_METHOD:-unknown} (fork on Linux<=3.13, forkserver on >=3.14)"
check_re "the daemon's start method is a known platform default" "^(fork|forkserver|spawn)\$" "$START_METHOD"

reset_default_state                      # also removes <db>.workers.lock; root re-creates the directory
out="$(msh "'$ENV_WRAP' '$PY' -m zfsbackup.cli import '$FIX_DIR/root.yaml' 2>&1")"; rc=$?
check "(f) seed: root imports the 5-dataset fixture" "0" "$rc"
start_daemon
wait_workers_started || warn "workers did not all start"
daemon_active && pass "(f) daemon started as $SVC_USER" || fail "(f) daemon not active: $(msh "tail -n 15 '$LOG'")"
sup="$(daemon_pid)"
g0="$(pf_val applied | cut -d: -f2)"
check_re "(f) daemon is reload-capable: pid file has start= and applied=<store>:<gen>" "^[0-9]+\$" "$g0"
check "(f) workers lock file exists next to the DB, 660 group $SVC_GROUP" "660 $SVC_GROUP" "$(mode_group "$WORKERS_LOCK")"
info "supervisor pid=$sup applied generation=$g0"

# --- (f1) root writes: same-uid-or-root signalling path ------------------------------------------
echo
step "CHECK (f1) — root 'set': the CLI SIGHUPs the verified daemon"
w_before="$(worker_pid snapshot) $(worker_pid pruning) $(worker_pid api)"
mark="$(log_lines)"
out="$(cli_as root set check_interval 2m)"; rc=$?
check "(f1) root 'set check_interval 2m' exits 0" "0" "$rc"
g1="$(printf '%s' "$out" | sed -n 's/.*config updated (generation \([0-9]*\)).*/\1/p' | head -n1)"
check_re "(f1) CLI reports the reload request to daemon pid $sup" "reload requested from daemon pid $sup" "$out"
check_re "(f1) CLI reports the daemon accepted it ('accepted' or 'now on')" "accepted generation $g1|now on generation $g1" "$out"
check_no_re "(f1) CLI does not tell the operator to restart" "restart" "$out"
if wait_log "$mark" "configuration generation $g1 accepted \\(was $g0; trigger: SIGHUP\\)" 6; then
    pass "(f1) daemon logs 'generation $g1 accepted (was $g0; trigger: SIGHUP)' within 5s"
else
    fail "(f1) no 'accepted ... trigger: SIGHUP' for generation $g1 within 5s: $(log_since "$mark" | tail -n 8 | tr '\n' '|')"
fi
if wait_workers_changed "$w_before" 45; then
    pass "(f1) snapshot, pruning and api workers all got new pids (turnover)"
else
    fail "(f1) not every worker turned over: before=[$w_before] now=[$(worker_pid snapshot) $(worker_pid pruning) $(worker_pid api)]"
fi
check "(f1) supervisor pid unchanged" "$sup" "$(daemon_pid)"
if wait_pf_re applied "^[0-9]+:$g1\$" 45; then pass "(f1) pid file shows applied=<store>:$g1"; else
    fail "(f1) pid file never showed applied=...:$g1 (applied='$(pf_val applied)' target='$(pf_val target)')"
fi
check "(f1) pid file line 2 is the applied generation $g1" "$sup,$g1" "$(pf2)"
check "(f1) pid file has no target= once the reload completed" "" "$(pf_val target)"
check_re "(f1) daemon logs 'generation $g1 applied by all workers'" "generation $g1 applied by all workers" "$(log_since "$mark")"
check_re "(f1) the log names the new worker generation ('Reloaded <name> worker ... on generation $g1')" \
    "Reloaded (snapshot|pruning|api) worker \\(pid=[0-9]+\\) on generation $g1" "$(log_since "$mark")"
check_re "(f1) 'Reloading:' summary lists the changed setting" "Reloading: generation $g0 -> $g1 .*settings changed: check_interval" "$(log_since "$mark")"
for w in snapshot pruning api; do
    check "(f1) reloaded $w worker (pid $(worker_pid $w)) holds no config.db fd" "" "$(open_db_fds "$(worker_pid $w)")"
done
out="$(cli_as root status)"; rc=$?
check "(f1) root 'status' exits 0" "0" "$rc"
check_re "(f1) status: verdict 'in sync'" "^status: +in sync" "$out"
check_re "(f1) status: names the daemon pid and reload-capable: yes" "daemon: +pid $sup \\(reload-capable: yes\\)" "$out"
check_re "(f1) status: applied is generation $g1" "applied: +store [0-9]+, generation $g1" "$out"
out="$(cli_as root status --wait 15)"; rc=$?
check "(f1) root 'status --wait 15' exits 0 (already in sync)" "0" "$rc"
# SIGHUP with an unchanged database: "unchanged", and no worker restarts.
w_before="snapshot=$(worker_pid snapshot) pruning=$(worker_pid pruning) api=$(worker_pid api)"
mark="$(log_lines)"
msh "systemctl kill -s HUP --kill-whom=main $UNIT" || warn "systemctl kill failed"
if wait_log "$mark" "SIGHUP: configuration unchanged \\(generation $g1\\)" 6; then
    pass "(f1) SIGHUP with an unchanged DB logs 'configuration unchanged (generation $g1)'"
else
    fail "(f1) no 'unchanged' line after SIGHUP: $(log_since "$mark" | tail -n 5 | tr '\n' '|')"
fi
sleep 6
check "(f1) an unchanged SIGHUP restarts no worker" "$w_before" "snapshot=$(worker_pid snapshot) pruning=$(worker_pid pruning) api=$(worker_pid api)"
daemon_active && pass "(f1) daemon still running" || fail "(f1) daemon died"

# --- (f2) group operator: EPERM -> poll fallback ------------------------------------------------
echo
step "CHECK (f2) — group operator alice 'set': not signalled; the 10s poll applies it"
w_before="$(worker_pid snapshot) $(worker_pid pruning) $(worker_pid api)"
g_prev="$(pf_val applied | cut -d: -f2)"
mark="$(log_lines)"
out="$(cli_as alice set check_interval 3m)"; rc=$?
check "(f2) alice 'set check_interval 3m' exits 0" "0" "$rc"
g2="$(printf '%s' "$out" | sed -n 's/.*config updated (generation \([0-9]*\)).*/\1/p' | head -n1)"
check_re "(f2) CLI: 'runs as another user and was not signalled; it picks up the change within 10s'" \
    "daemon pid $sup runs as another user and was not signalled; it picks up the change within 10s" "$out"
check_no_re "(f2) CLI does not claim to have requested a reload" "reload requested" "$out"
out="$(cli_as alice status)"
check_re "(f2) alice's 'status' right after the write: behind or applying, never a restart hint" "daemon on|applying|in sync" "$out"
check_no_re "(f2) status never says restart for a reload-capable daemon" "restart" "$out"
out="$(cli_as alice status --wait 15)"; rc=$?
check "(f2) alice 'status --wait 15' exits 0 once the poll applied generation $g2" "0" "$rc"
check_re "(f2) status: 'in sync'" "^status: +in sync" "$out"
if wait_log "$mark" "configuration generation $g2 accepted \\(was $g_prev; trigger: poll\\)" 15; then
    pass "(f2) daemon logs 'generation $g2 accepted (was $g_prev; trigger: poll)' within 15s"
else
    fail "(f2) no 'accepted ... trigger: poll' for generation $g2: $(log_since "$mark" | tail -n 8 | tr '\n' '|')"
fi
check_no_re "(f2) it was NOT a SIGHUP reload (alice could not signal)" "generation $g2 accepted .*trigger: SIGHUP" "$(log_since "$mark")"
if wait_workers_changed "$w_before" 45; then pass "(f2) all three worker pids changed"; else
    fail "(f2) not every worker turned over: before=[$w_before]"
fi
check "(f2) supervisor pid unchanged" "$sup" "$(daemon_pid)"
wait_pf_re applied "^[0-9]+:$g2\$" 45 && pass "(f2) pid file shows applied=<store>:$g2" || fail "(f2) pid file applied='$(pf_val applied)'"

# --- (f3) the service user itself: same uid -> signalled -----------------------------------------
echo
step "CHECK (f3) — $SVC_USER 'set': same uid, so it is signalled"
w_before="$(worker_pid snapshot) $(worker_pid pruning) $(worker_pid api)"
mark="$(log_lines)"
out="$(cli_as "$SVC_USER" set check_interval 4m)"; rc=$?
check "(f3) $SVC_USER 'set' exits 0" "0" "$rc"
g3="$(printf '%s' "$out" | sed -n 's/.*config updated (generation \([0-9]*\)).*/\1/p' | head -n1)"
check_re "(f3) CLI: reload requested from daemon pid $sup (signalled, same uid)" "reload requested from daemon pid $sup" "$out"
check_no_re "(f3) CLI does not say it was not signalled" "not signalled" "$out"
if wait_log "$mark" "configuration generation $g3 accepted \\(was $g2; trigger: SIGHUP\\)" 6; then
    pass "(f3) daemon logs 'accepted ... trigger: SIGHUP' within 5s"
else
    fail "(f3) no SIGHUP reload logged for generation $g3: $(log_since "$mark" | tail -n 8 | tr '\n' '|')"
fi
wait_workers_changed "$w_before" 45 && pass "(f3) worker pids changed" || fail "(f3) workers did not turn over"
wait_pf_re applied "^[0-9]+:$g3\$" 45 && pass "(f3) pid file shows applied=<store>:$g3" || fail "(f3) pid file applied='$(pf_val applied)'"

# --- (f5) a stored config the daemon must refuse (run before f4 stops the daemon) ----------------
echo
step "CHECK (f5) — an invalid stored config bumped by a raw sqlite3 UPDATE is rejected; the daemon keeps running"
w_before="snapshot=$(worker_pid snapshot) pruning=$(worker_pid pruning) api=$(worker_pid api)"
mark="$(log_lines)"
good_applied="$(pf_val applied)"
out="$(asu "$SVC_USER" "'$PY' '$FIX_DIR/breakconfig.py' '$DB' 2>&1")"; rc=$?
check "(f5) raw UPDATE (api_port=70000, generation+1) succeeds" "0" "$rc"
g5="$(printf '%s' "$out" | sed -n 's/^generation \([0-9]*\).*/\1/p' | head -n1)"
msh "systemctl kill -s HUP --kill-whom=main $UNIT" || warn "systemctl kill failed"
if wait_log "$mark" "configuration generation $g5 rejected \\(trigger: SIGHUP\\); still running generation $g3" 8; then
    pass "(f5) daemon logs 'generation $g5 rejected (trigger: SIGHUP); still running generation $g3'"
else
    fail "(f5) no rejection logged: $(log_since "$mark" | tail -n 8 | tr '\n' '|')"
fi
check_re "(f5) the rejection names the offending setting (api_port)" "api_port" "$(log_since "$mark")"
check_no_re "(f5) nothing was accepted" "generation $g5 accepted" "$(log_since "$mark")"
sleep 12                                 # two polls: a rejected key is not retried
check "(f5) worker pids unchanged" "$w_before" "snapshot=$(worker_pid snapshot) pruning=$(worker_pid pruning) api=$(worker_pid api)"
check_re "(f5) pid file has rejected=<store>:$g5" "^[0-9]+:$g5\$" "$(pf_val rejected)"
check "(f5) pid file still has the good applied= key" "$good_applied" "$(pf_val applied)"
check "(f5) the unit is still active" "active" "$(daemon_active && echo active || echo not-active)"
check "(f5) supervisor pid unchanged" "$sup" "$(daemon_pid)"
check_re "(f5) a rejected key is logged once (not every poll)" "^1\$" "$(log_since "$mark" | grep -c "generation $g5 rejected")"
out="$(cli_as root status)"
check_re "(f5) status: 'daemon rejected generation $g5'" "daemon rejected generation $g5" "$out"
out="$(cli_as root status --wait 5)"; rc=$?
check "(f5) status --wait exits 1 for a rejected key" "1" "$rc"
# repair: re-import good config -> daemon accepts it and clears rejected=
mark="$(log_lines)"
out="$(cli_as root import --no-backup "$FIX_DIR/root.yaml")"; rc=$?
check "(f5) root re-imports a good config (exit 0)" "0" "$rc"
g6="$(printf '%s' "$out" | sed -n 's/.*config updated (generation \([0-9]*\)).*/\1/p' | head -n1)"
if wait_log "$mark" "configuration generation $g6 accepted" 10; then pass "(f5) daemon accepts the repaired generation $g6"; else
    fail "(f5) repaired generation not accepted: $(log_since "$mark" | tail -n 6 | tr '\n' '|')"
fi
wait_pf_re applied "^[0-9]+:$g6\$" 45 && pass "(f5) pid file shows applied=<store>:$g6" || fail "(f5) pid file applied='$(pf_val applied)'"
check "(f5) rejected= cleared by the next acceptance" "" "$(pf_val rejected)"

# --- (f4) forgery: a pid file naming a decoy must never get the decoy signalled -------------------
echo
step "CHECK (f4) — forged pid file: the CLI signals only a proven lock holder"
store_id="$(pf_val applied | cut -d: -f1)"; gen_f4="$(pf_val applied | cut -d: -f2)"
stop_daemon
msh "systemd-run --unit=$DECOY_UNIT --collect sleep 600" || warn "could not start the decoy"
sleep 1
decoy="$(msh "systemctl show -p MainPID --value $DECOY_UNIT" | tr -d '[:space:]')"
token="$(msh "'$ENV_WRAP' '$PY' -c 'from zfsbackup import procident; print(procident.start_token($decoy))'" | tr -d '[:space:]')"
info "decoy: root-owned 'sleep 600' pid=$decoy start token=$token"
check_re "(f4) the decoy is alive and has a start token" "^linux:[0-9]+\$" "$token"
# start_forger <owner-precreated:yes|no>  — alice holds LOCK_EX on the pid file and writes the forged v2 content
start_forger() {
    msh "
        systemctl reset-failed $FORGE_UNIT 2>/dev/null || true
        : > /tmp/forger.out; chmod 666 /tmp/forger.out
        systemd-run --unit=$FORGE_UNIT --collect -p User=alice -p StandardOutput=append:/tmp/forger.out \
            -p StandardError=append:/tmp/forger.out \
            '$ENV_WRAP' '$PY' '$FIX_DIR/forger.py' '$PIDFILE' '$decoy' '$token' '$store_id' '$gen_f4'
    " || return 1
    local waited=0
    while ! msh "grep -q READY /tmp/forger.out" 2>/dev/null; do
        [ "$waited" -ge 30 ] && { warn "forger never printed READY: $(msh 'cat /tmp/forger.out')"; return 1; }
        sleep 1; waited=$((waited+1))
    done
}
# f4a: pid file root-owned (the owner check passes), so the LOCK-TABLE proof is what must refuse.
msh "rm -f '$PIDFILE'; install -m 660 -o root -g $SVC_GROUP /dev/null '$PIDFILE'"
if start_forger; then
    pass "(f4a) alice holds LOCK_EX on the root-owned pid file, which names the decoy (pid $decoy) with its correct start token"
    check "(f4a) the pid file really names the decoy" "$decoy" "$(msh "head -n1 '$PIDFILE'" | tr -d '[:space:]')"
    check_re "(f4a) request_reload: refused by step 7 (the decoy is not in the lock table)" "outcome=supervisor-gone reason=not-lock-holder" \
        "$(msh "'$ENV_WRAP' '$PY' '$FIX_DIR/reloadreason.py' 2>&1")"
    out="$(cli_as root set check_interval 5m)"; rc=$?
    check "(f4a) root 'set' still exits 0 (the write is durable)" "0" "$rc"
    check_no_re "(f4a) the CLI did NOT report a reload request" "reload requested from daemon pid" "$out"
    info "(f4a) CLI said: $(printf '%s' "$out" | grep 'config updated' | tail -n1)"
    check_re "(f4a) the notice says the supervisor is gone / not signalled / unverified" \
        "is gone|not signalled|was not signalled|could not verify|does not hold" "$out"
    sleep 1
    msh "kill -0 $decoy" && pass "(f4a) the decoy is still alive (never signalled)" || fail "(f4a) the decoy died - it was SIGHUPed"
    check "(f4a) the decoy's unit is still active" "active" "$(msh "systemctl is-active $DECOY_UNIT" | tr -d '[:space:]')"
    msh "systemctl stop $FORGE_UNIT" >/dev/null 2>&1 || true
else
    fail "(f4a) could not start the forger"
fi
# f4b: pid file owned by alice (a group operator may create it in the setgid directory); root must refuse it.
msh "rm -f '$PIDFILE'" || true
if start_forger; then
    check "(f4b) the forged pid file is owned by alice" "alice" "$(stat_of "$PIDFILE" | awk '{print $2}')"
    out="$(cli_as root set check_interval 6m)"; rc=$?
    check "(f4b) root 'set' exits 0" "0" "$rc"
    check_no_re "(f4b) the CLI did NOT report a reload request" "reload requested from daemon pid" "$out"
    info "(f4b) CLI said: $(printf '%s' "$out" | grep 'config updated' | tail -n1)"
    check_re "(f4b) the notice says it was not signalled / unverified / gone" "is gone|not signalled|could not verify|does not hold" "$out"
    sleep 1
    msh "kill -0 $decoy" && pass "(f4b) the decoy is still alive" || fail "(f4b) the decoy died - it was SIGHUPed"
    out="$(cli_as alice status)"
    msh "kill -0 $decoy" && pass "(f4b) the decoy survived 'status' too" || fail "(f4b) the decoy died during status"
else
    fail "(f4b) could not start the forger"
fi
# f4c: recycled-pid shape. The lock table names a LIVE process with a matching start token, but that
# process no longer has the pid file open (a forked child holds the lock) - step 8b must refuse it.
msh "systemctl stop $FORGE_UNIT" >/dev/null 2>&1 || true
msh "rm -f '$PIDFILE'; install -m 660 -o root -g $SVC_GROUP /dev/null '$PIDFILE'"
msh "
    systemctl reset-failed $FORGE_UNIT 2>/dev/null || true
    : > /tmp/forger.out; chmod 666 /tmp/forger.out
    systemd-run --unit=$FORGE_UNIT --collect -p User=alice -p StandardOutput=append:/tmp/forger.out \
        -p StandardError=append:/tmp/forger.out \
        '$ENV_WRAP' '$PY' '$FIX_DIR/forger2.py' '$PIDFILE' '$store_id' '$gen_f4'
" || warn "could not start forger2"
waited=0; until msh "grep -q READY /tmp/forger.out" 2>/dev/null; do [ "$waited" -ge 30 ] && break; sleep 1; waited=$((waited+1)); done
fpid="$(msh "head -n1 '$PIDFILE'" | tr -d '[:space:]')"
if msh "grep -q READY /tmp/forger.out" 2>/dev/null && [ -n "$fpid" ]; then
    pass "(f4c) alice's process $fpid holds the lock through a forked child, names itself with its real start token, and has closed the file"
    check "(f4c) the pid file's pid has the file closed (no /proc/$fpid/fd entry for it)" "0" \
        "$(msh "ls -l /proc/$fpid/fd | grep -c 'daemon.pid'")"
    check_re "(f4c) the lock table still names pid $fpid as the locker (so step 7 alone would accept it)" "FLOCK +ADVISORY +WRITE $fpid " \
        "$(msh "cat /proc/locks")"
    check_re "(f4c) request_reload: refused by step 8b (reason file-not-open), not by the lock table" "outcome=supervisor-gone reason=file-not-open" \
        "$(msh "'$ENV_WRAP' '$PY' '$FIX_DIR/reloadreason.py' 2>&1")"
    out="$(cli_as root set check_interval 8m)"; rc=$?
    check "(f4c) root 'set' exits 0" "0" "$rc"
    info "(f4c) CLI said: $(printf '%s' "$out" | grep 'config updated' | tail -n1)"
    check_no_re "(f4c) the CLI did NOT report a reload request" "reload requested from daemon pid" "$out"
    check_re "(f4c) the notice says it was not signalled (does not have the lock file open)" "does not have the daemon lock file open|not signalled|was not signalled" "$out"
    sleep 1
    msh "kill -0 $fpid" && pass "(f4c) the process named in the pid file is still alive (never SIGHUPed)" || fail "(f4c) it was SIGHUPed"
else
    fail "(f4c) could not start forger2: $(msh 'cat /tmp/forger.out')"
fi
stop_daemon                              # stops the forger and the decoy as well
msh "rm -f '$PIDFILE'" || true

# --- (f6) a CLI write while the daemon is still starting --------------------------------------
echo
step "CHECK (f6) — a CLI write during daemon startup: the daemon stays up and applies it"
# (f6.0) deterministic window: a stand-in for a previous daemon's surviving worker holds the workers lock,
# so the new daemon sits in its startup wait (presence lock taken, config NOT yet read) while root writes
# and the CLI SIGHUPs it. Releasing the holder lets the daemon go on and read the new generation.
stop_daemon
msh "[ -e '$WORKERS_LOCK' ] || install -m 660 -o $SVC_USER -g $SVC_GROUP /dev/null '$WORKERS_LOCK'; : >'$LOG'; rm -f '$PIDFILE'" || true
msh "
    : > /tmp/wlock.out; chmod 666 /tmp/wlock.out
    systemd-run --unit=$WLOCK_UNIT --collect -p User=$SVC_USER -p StandardOutput=append:/tmp/wlock.out \
        -p StandardError=append:/tmp/wlock.out '$ENV_WRAP' '$PY' '$FIX_DIR/wlock.py' '$WORKERS_LOCK'
" || warn "could not start the workers-lock holder"
waited=0; until msh "grep -q READY /tmp/wlock.out" 2>/dev/null; do [ "$waited" -ge 30 ] && break; sleep 1; waited=$((waited+1)); done
check_re "(f6.0) a stand-in for a previous daemon's worker holds the workers lock" "READY" "$(msh 'cat /tmp/wlock.out')"
start_daemon
if wait_log 0 "workers of a previous daemon still hold .*waiting for them to finish" 30; then
    pass "(f6.0) the new daemon logs that it is waiting for the previous daemon's workers"
else
    fail "(f6.0) no 'waiting' line: $(msh "tail -n 8 '$LOG'" | tr '\n' '|')"
fi
sup6="$(daemon_pid)"
check_no_re "(f6.0) the daemon has not started a worker yet" "Started .* worker" "$(msh "cat '$LOG'")"
out="$(cli_as root set check_interval 7m)"; rc=$?
check "(f6.0) root 'set' while the daemon waits to start exits 0" "0" "$rc"
g_wait="$(printf '%s' "$out" | sed -n 's/.*config updated (generation \([0-9]*\)).*/\1/p' | head -n1)"
info "(f6.0) CLI said: $(printf '%s' "$out" | grep 'config updated' | tail -n1)"
sleep 3
daemon_active && pass "(f6.0) the daemon survived the write (and any SIGHUP) during its startup wait" || fail "(f6.0) daemon died during its startup wait: $(msh "tail -n 12 '$LOG'" | tr '\n' '|')"
check "(f6.0) same supervisor pid" "$sup6" "$(daemon_pid)"
msh "systemctl stop $WLOCK_UNIT" >/dev/null 2>&1 || true
wait_workers_started || warn "(f6.0) workers did not start after the holder went away"
check_re "(f6.0) once released, the daemon starts its workers" "Started api worker" "$(msh "cat '$LOG'")"
out="$(cli_as root status --wait 40)"; rc=$?
check "(f6.0) 'status --wait 40' exits 0: the daemon runs generation $g_wait" "0" "$rc"
daemon_active && pass "(f6.0) daemon running after the wait" || fail "(f6.0) daemon not running: $(msh "tail -n 12 '$LOG'" | tr '\n' '|')"
check_re "(f6.0) pid file applied=<store>:$g_wait" "^[0-9]+:$g_wait\$" "$(pf_val applied)"
check_no_re "(f6.0) no crash or traceback" "Traceback|crash loop|exited with code [1-9]" "$(msh "cat '$LOG'")"
for trial in 1 2 3; do
    stop_daemon
    msh ": >'$LOG'" || true
    mark=0
    # Start the daemon and, the moment its pid file appears (the presence lock is taken before the
    # config is read), write through the CLI as root. All in ONE guest shell so nothing is lost to
    # multipass latency. The value alternates so every trial is a real generation bump.
    val=$((trial + 1))
    out="$(msh "
        rm -f '$PIDFILE'
        systemctl reset-failed $UNIT 2>/dev/null || true
        systemd-run --unit=$UNIT --collect -p User=$SVC_USER -p Group=$SVC_GROUP \
            -p StandardOutput=append:'$LOG' -p StandardError=append:'$LOG' \
            '$ENV_WRAP' '$PY' -m zfsbackup.daemon --dry-run -v
        i=0; until [ -s '$PIDFILE' ] || [ \$i -gt 400 ]; do sleep 0.05; i=\$((i+1)); done
        '$ENV_WRAP' '$PY' -m zfsbackup.cli set check_interval ${val}m 2>&1
    " 2>&1)"; rc=$?
    check "(f6.$trial) root 'set' during startup exits 0" "0" "$rc"
    g_race="$(printf '%s' "$out" | sed -n 's/.*config updated (generation \([0-9]*\)).*/\1/p' | head -n1)"
    info "(f6.$trial) CLI said: $(printf '%s' "$out" | grep 'config updated' | tail -n1)"
    wait_workers_started || warn "(f6.$trial) workers did not all start"
    out="$(cli_as root status --wait 40)"; rc=$?
    check "(f6.$trial) 'status --wait 40' exits 0: the daemon applied generation $g_race" "0" "$rc"
    daemon_active && pass "(f6.$trial) daemon stayed up" || fail "(f6.$trial) daemon died: $(log_since "$mark" | tail -n 12 | tr '\n' '|')"
    check_no_re "(f6.$trial) no crash, traceback or restart-limit exit in the log" "Traceback|crash loop|exited with code [1-9]|refus" "$(log_since "$mark")"
    check_re "(f6.$trial) pid file applied=<store>:$g_race" "^[0-9]+:$g_race\$" "$(pf_val applied)"
done

stop_daemon
# ───────────────────────────────── (g) ──────────────────────────────────────
# Follow-ups round 2026-10-19 (B3): a candidate whose api_host:api_port is merely IN USE is DEFERRED, not rejected:
# `deferred=` in the pid file, status verdict `deferred`, `status --wait` exits 75 (never 1), the old API keeps
# answering, and the poll applies it once the port is free - with no new write.
echo
step "CHECK (g) — a reload to a BUSY api_port is deferred; releasing the port applies it by poll"
BUSY_PORT=18099; OLD_PORT=8080
reset_default_state
out="$(msh "'$ENV_WRAP' '$PY' -m zfsbackup.cli import '$FIX_DIR/root.yaml' 2>&1")"; rc=$?
check "(g) seed: root imports the 5-dataset fixture" "0" "$rc"
start_daemon
wait_workers_started || warn "(g) workers did not all start"
daemon_active && pass "(g) daemon started as $SVC_USER" || fail "(g) daemon not active: $(msh "tail -n 15 '$LOG'")"
sup_g="$(daemon_pid)"
good_applied="$(pf_val applied)"
check "(g) the API answers /health on the configured port $OLD_PORT" "200" "$(asu alice "'$PY' '$FIX_DIR/httpget.py' $OLD_PORT")"
msh "
    systemctl reset-failed $PORT_UNIT 2>/dev/null || true
    : > /tmp/portholder.out; chmod 666 /tmp/portholder.out
    systemd-run --unit=$PORT_UNIT --collect -p User=alice -p StandardOutput=append:/tmp/portholder.out \
        -p StandardError=append:/tmp/portholder.out '$ENV_WRAP' '$PY' '$FIX_DIR/portholder.py' $BUSY_PORT
" || warn "(g) could not start the port holder"
waited=0; until msh "grep -q READY /tmp/portholder.out" 2>/dev/null; do [ "$waited" -ge 30 ] && break; sleep 1; waited=$((waited+1)); done
check_re "(g) alice's process holds 127.0.0.1:$BUSY_PORT" "READY" "$(msh 'cat /tmp/portholder.out')"
w_before="snapshot=$(worker_pid snapshot) pruning=$(worker_pid pruning) api=$(worker_pid api)"
mark="$(log_lines)"
out="$(cli_as root set api_port $BUSY_PORT)"; rc=$?
check "(g) root 'set api_port $BUSY_PORT' exits 0 (the write is durable)" "0" "$rc"
gD="$(printf '%s' "$out" | sed -n 's/.*config updated (generation \([0-9]*\)).*/\1/p' | head -n1)"
info "(g) CLI said: $(printf '%s' "$out" | tr '\n' '|' | cut -c1-400)"
check_re "(g) CLI: the daemon has not applied generation $gD yet (api_port in use), re-checks every poll" \
    "has not applied generation $gD yet: .*$BUSY_PORT is in use.*re-checks every poll" "$out"
check_no_re "(g) CLI does not call it a rejection" "rejected generation" "$out"
if wait_log "$mark" "configuration generation $gD deferred \\(trigger: SIGHUP\\)" 8; then
    pass "(g) daemon logs 'generation $gD deferred (trigger: SIGHUP)'"
else
    fail "(g) no 'deferred' line for generation $gD: $(log_since "$mark" | tail -n 8 | tr '\n' '|')"
fi
check_re "(g) the deferral is a WARNING, not an ERROR, and names the busy address" "WARNING.*generation $gD deferred.*127\\.0\\.0\\.1:$BUSY_PORT is in use" "$(log_since "$mark")"
check_no_re "(g) nothing was logged as rejected or accepted for generation $gD" "generation $gD (rejected|accepted)" "$(log_since "$mark")"
sleep 12                                 # two polls: a deferred key is re-evaluated, but logged once
check_re "(g) pid file has deferred=<store>:$gD" "^[0-9]+:$gD\$" "$(pf_val deferred)"
check "(g) pid file has NO rejected= (deferral is not a rejection)" "" "$(pf_val rejected)"
check "(g) pid file applied= is still the running generation" "$good_applied" "$(pf_val applied)"
check_re "(g) pid file names the reason (deferred_reason=)" "$BUSY_PORT" "$(msh "sed -n 's/^deferred_reason=//p' '$PIDFILE'")"
# (-v logs the repeats at DEBUG, so count only the WARNING/ERROR level line)
check "(g) the deferral is WARNING-logged once across the polls (repeats are DEBUG)" "1" "$(log_since "$mark" | grep -cE "(WARNING|ERROR).*generation $gD deferred" | tr -d '[:space:]')"
check "(g) worker pids unchanged while deferred" "$w_before" "snapshot=$(worker_pid snapshot) pruning=$(worker_pid pruning) api=$(worker_pid api)"
check "(g) the OLD port $OLD_PORT still answers /health" "200" "$(asu alice "'$PY' '$FIX_DIR/httpget.py' $OLD_PORT")"
out="$(cli_as root status --json)"
check_re "(g) status --json: verdict 'deferred'" "\"verdict\": *\"deferred\"" "$out"
check_re "(g) status --json: deferred key is generation $gD" "\"deferred\": *\\{[^}]*\"generation\": *$gD" "$(printf '%s' "$out" | tr -d '\n')"
out="$(cli_as root status)"; rc=$?
check_re "(g) status text: 'has not applied generation $gD yet'" "has not applied generation $gD yet" "$out"
check_re "(g) status text shows the deferred row" "^deferred: +store [0-9]+, generation $gD" "$out"
out="$(cli_as root status --wait 4)"; rc=$?
check "(g) 'status --wait 4' exits 75 while deferred (never 1)" "75" "$rc"
daemon_active && pass "(g) daemon still running (the old API keeps serving)" || fail "(g) daemon died"
check "(g) supervisor pid unchanged" "$sup_g" "$(daemon_pid)"
# release the port: NO new write; the poll must apply the stored generation by itself
db_gen_before="$(asu "$SVC_USER" "'$PY' '$FIX_DIR/dbgen.py' 2>&1" | tr -d '[:space:]')"
msh "systemctl stop $PORT_UNIT" >/dev/null 2>&1 || true
mark="$(log_lines)"
if wait_log "$mark" "configuration generation $gD accepted \\(was [0-9]+; trigger: poll\\)" 20; then
    pass "(g) the port was released: the poll accepts generation $gD within 20s (no new write)"
else
    fail "(g) generation $gD not accepted by poll after the release: $(log_since "$mark" | tail -n 8 | tr '\n' '|')"
fi
wait_pf_re applied "^[0-9]+:$gD\$" 45 && pass "(g) pid file shows applied=<store>:$gD" || fail "(g) pid file applied='$(pf_val applied)'"
check "(g) deferred= cleared by the acceptance" "" "$(pf_val deferred)"
check "(g) no write happened meanwhile: the stored generation is still $db_gen_before" "$db_gen_before" "$(asu "$SVC_USER" "'$PY' '$FIX_DIR/dbgen.py' 2>&1" | tr -d '[:space:]')"
waited=0; until [ "$(asu alice "'$PY' '$FIX_DIR/httpget.py' $BUSY_PORT")" = "200" ] || [ "$waited" -ge 30 ]; do sleep 2; waited=$((waited+2)); done
check "(g) the API now answers /health on the new port $BUSY_PORT" "200" "$(asu alice "'$PY' '$FIX_DIR/httpget.py' $BUSY_PORT")"
check "(g) the old port $OLD_PORT no longer answers (API worker replaced)" "ERR" "$(asu alice "'$PY' '$FIX_DIR/httpget.py' $OLD_PORT")"
out="$(cli_as root status --wait 20)"; rc=$?
check "(g) 'status --wait 20' exits 0" "0" "$rc"
check_re "(g) status: 'in sync'" "in sync" "$out"
check "(g) supervisor pid unchanged through the whole deferral" "$sup_g" "$(daemon_pid)"
stop_daemon

# ───────────────────────────────── (h) ──────────────────────────────────────
# Follow-ups round 2026-10-19 (D): sd_notify. `systemctl start` returns after READY (workers forked),
# `systemctl reload` returns after the reload DECISION, StatusText carries "generation N", stop is clean.
echo
step "CHECK (h) — a systemd Type=notify-reload unit (documented in zfsbackup/README.md)"
SD_VER="$(msh "systemctl --version | head -n1 | awk '{print \$2}'" | tr -d '[:space:]')"
info "systemd $SD_VER on $PERM_VM"
if [ "${SD_VER:-0}" -ge 253 ] 2>/dev/null; then
    SD_TYPE="notify-reload"; SD_EXECRELOAD=""
else
    SD_TYPE="notify"; SD_EXECRELOAD='ExecReload=/bin/kill -HUP $MAINPID'    # the documented < 253 variant
fi
msh "sed -e 's|__SDTYPE__|$SD_TYPE|' -e 's|__EXECRELOAD__|$SD_EXECRELOAD|' '$FIX_DIR/zfsperm-notify.service.in' > /etc/systemd/system/$NOTIFY_UNIT.service && systemctl daemon-reload" \
    || die "could not install the $NOTIFY_UNIT unit"
sd_prop() { msh "systemctl show -p $1 --value $NOTIFY_UNIT" | tr -d '\r'; }
reset_default_state
out="$(msh "'$ENV_WRAP' '$PY' -m zfsbackup.cli import '$FIX_DIR/root.yaml' 2>&1")"; rc=$?
check "(h) seed: root imports the 5-dataset fixture" "0" "$rc"
check "(h) the unit's Type is $SD_TYPE" "$SD_TYPE" "$(sd_prop Type)"
msh "systemctl start $NOTIFY_UNIT"; rc=$?
check "(h) 'systemctl start' returns 0 (after READY=1)" "0" "$rc"
check "(h) the unit is active (not 'activating')" "active" "$(sd_prop ActiveState)"
check_re "(h) the workers had already been forked when start returned" "Started api worker" "$(msh "cat '$LOG'")"
sup_h="$(sd_prop MainPID)"
gh0="$(pf_val applied | cut -d: -f2)"
check "(h) StatusText says 'generation $gh0'" "generation $gh0" "$(sd_prop StatusText)"
check "(h) daemon runs as $SVC_USER" "$SVC_USER" "$(msh "ps -o user= -p $sup_h" | tr -d '[:space:]')"
# reload with nothing changed: one SIGHUP (systemd's own; no ExecReload= double signal), decided, READY
mark="$(log_lines)"
msh "systemctl reload $NOTIFY_UNIT"; rc=$?
check "(h) 'systemctl reload' (nothing changed) returns 0" "0" "$rc"
check "(h) the SIGHUP was decided before reload returned: 'configuration unchanged (generation $gh0)' logged once" "1" \
    "$(log_since "$mark" | grep -c "SIGHUP: configuration unchanged (generation $gh0)" | tr -d '[:space:]')"
check "(h) StatusText unchanged" "generation $gh0" "$(sd_prop StatusText)"
check "(h) unit still active, same supervisor" "active $sup_h" "$(sd_prop ActiveState) $(sd_prop MainPID)"
# an operator write the daemon was NOT signalled about (alice is another uid): `systemctl reload` picks it up
mark="$(log_lines)"
out="$(cli_as alice set check_interval 3m)"; rc=$?
check "(h) alice 'set check_interval 3m' exits 0" "0" "$rc"
gh1="$(printf '%s' "$out" | sed -n 's/.*config updated (generation \([0-9]*\)).*/\1/p' | head -n1)"
msh "systemctl reload $NOTIFY_UNIT"; rc=$?
check "(h) 'systemctl reload' returns 0" "0" "$rc"
check_re "(h) the decision was logged before reload returned: generation $gh1 accepted (SIGHUP, or the poll won the race)" \
    "configuration generation $gh1 accepted \\(was $gh0; trigger: (SIGHUP|poll)\\)" "$(log_since "$mark")"
check "(h) StatusText says 'generation $gh1' as soon as reload returned" "generation $gh1" "$(sd_prop StatusText)"
wait_pf_re applied "^[0-9]+:$gh1\$" 45 && pass "(h) pid file shows applied=<store>:$gh1" || fail "(h) pid file applied='$(pf_val applied)'"
# a root write: the CLI signals the daemon; systemd's StatusText follows
out="$(cli_as root set check_interval 4m)"; rc=$?
check "(h) root 'set check_interval 4m' exits 0" "0" "$rc"
gh2="$(printf '%s' "$out" | sed -n 's/.*config updated (generation \([0-9]*\)).*/\1/p' | head -n1)"
check_re "(h) the CLI SIGHUPed the unit's supervisor (pid $sup_h)" "reload requested from daemon pid $sup_h" "$out"
waited=0; until [ "$(sd_prop StatusText)" = "generation $gh2" ] || [ "$waited" -ge 20 ]; do sleep 2; waited=$((waited+2)); done
check "(h) StatusText follows to 'generation $gh2'" "generation $gh2" "$(sd_prop StatusText)"
# a stored config the daemon refuses: reload still SUCCEEDS (READY on every outcome) and StatusText names it
wait_pf_re applied "^[0-9]+:$gh2\$" 45 || warn "(h) generation $gh2 not applied yet"
out="$(asu "$SVC_USER" "'$PY' '$FIX_DIR/breakconfig.py' '$DB' 2>&1")"
gh3="$(printf '%s' "$out" | sed -n 's/^generation \([0-9]*\).*/\1/p' | head -n1)"
mark="$(log_lines)"
msh "systemctl reload $NOTIFY_UNIT"; rc=$?
check "(h) 'systemctl reload' returns 0 even though the stored config is rejected" "0" "$rc"
check_re "(h) the rejection was logged" "configuration generation $gh3 rejected" "$(log_since "$mark")"
check "(h) StatusText names the rejected generation" "generation $gh2; generation $gh3 rejected" "$(sd_prop StatusText)"
check "(h) the unit is still active" "active" "$(sd_prop ActiveState)"
out="$(cli_as root import --no-backup "$FIX_DIR/root.yaml")"; rc=$?
check "(h) root re-imports a good config (exit 0)" "0" "$rc"
gh4="$(printf '%s' "$out" | sed -n 's/.*config updated (generation \([0-9]*\)).*/\1/p' | head -n1)"
waited=0; until [ "$(sd_prop StatusText)" = "generation $gh4" ] || [ "$waited" -ge 20 ]; do sleep 2; waited=$((waited+2)); done
check "(h) StatusText back to 'generation $gh4' once the repaired config is applied" "generation $gh4" "$(sd_prop StatusText)"
# clean stop
t0="$(date +%s)"
msh "systemctl stop $NOTIFY_UNIT"; rc=$?
t1="$(date +%s)"
check "(h) 'systemctl stop' returns 0" "0" "$rc"
if [ $((t1 - t0)) -lt 40 ]; then pass "(h) the stop took $((t1 - t0))s (well inside TimeoutStopSec=90)"; else fail "(h) the stop took $((t1 - t0))s"; fi
check "(h) the unit is inactive" "inactive" "$(sd_prop ActiveState)"
check "(h) the unit ended with Result=success (no SIGKILL, no timeout)" "success" "$(sd_prop Result)"
check "(h) no daemon or worker process is left" "0" "$(msh "pgrep -fc '[z]fsbackup\\.daemon' || true" | tr -d '[:space:]')"
check "(h) a clean stop removed the pid file" "gone" "$(exists "$PIDFILE" && echo present || echo gone)"
check_no_re "(h) no traceback or crash in the log" "Traceback|crash loop" "$(msh "cat '$LOG'")"
msh "{ rm -f /etc/systemd/system/$NOTIFY_UNIT.service; systemctl daemon-reload; } >/dev/null 2>&1; true" || true

# ───────────────────────────────── (i) ──────────────────────────────────────
# Milestone 3 (TLS/mTLS): <db>.tls/ holds the server and client identities. Directory 0750, keys 0600, certificates 0640,
# identity.lock 0640, all in the DB file's group (a group operator can read the certificate -- enough to issue a token --
# but never the key). The scenario's daemon is --dry-run, which creates no server identity, so the identity is created the
# way the CLI does it (the daemon's own startup uses the same function).
echo
step "CHECK (i) — the TLS identity directory <db>.tls: modes and group"
TLS="${DB}.tls"
reset_default_state
out="$(msh "'$ENV_WRAP' '$PY' -m zfsbackup.cli import '$FIX_DIR/root.yaml' 2>&1")"; rc=$?
check "(i) seed: root imports the fixture" "0" "$rc"
# i1: the service user creates the server identity
out="$(cli_as "$SVC_USER" trust token create --json)"; rc=$?
check "(i1) $SVC_USER's 'trust token create' exits 0 (it creates the server identity)" "0" "$rc"
check "(i1) $TLS is 750 and in the DB file's group $SVC_GROUP" "750 $SVC_GROUP" "$(mode_group "$TLS")"
check "(i1) server.key is 600 in group $SVC_GROUP" "600 $SVC_GROUP" "$(mode_group "$TLS/server.key")"
check "(i1) server.crt is 640 in group $SVC_GROUP" "640 $SVC_GROUP" "$(mode_group "$TLS/server.crt")"
check "(i1) identity.lock is 640 in group $SVC_GROUP" "640 $SVC_GROUP" "$(mode_group "$TLS/identity.lock")"
check "(i1) nothing else is in the directory (no stale temporaries)" "identity.lock server.crt server.key" "$(msh "ls -A '$TLS' | tr '\n' ' '" | sed 's/ *$//')"
asu alice "cat '$TLS/server.crt' >/dev/null"; rc=$?
check "(i1) group operator alice can read server.crt" "0" "$rc"
asu alice "cat '$TLS/server.key' >/dev/null 2>&1"; rc=$?
if [ "$rc" -ne 0 ]; then pass "(i1) alice cannot read server.key (exit $rc)"; else fail "(i1) alice can read the server's private key"; fi
out="$(cli_as alice trust token create --json)"; rc=$?
check "(i1) alice can still issue a token (it needs only the certificate)" "0" "$rc"
check_no_re "(i1) alice's token command did not try to generate a new identity" "generated a new server TLS identity" "$out"
# i2: root creates the client identity in a fresh state (what `enroll` does first)
reset_default_state
out="$(msh "'$ENV_WRAP' '$PY' -m zfsbackup.cli import '$FIX_DIR/root.yaml' 2>&1")"; rc=$?
check "(i2) seed: root imports the fixture" "0" "$rc"
out="$(msh "'$ENV_WRAP' '$PY' '$FIX_DIR/ensure_client.py' 2>&1")"; rc=$?
check "(i2) root creates the client identity (exit 0)" "0" "$rc"
check "(i2) $TLS is 750, owned by root, in the DB file's group" "750 root $SVC_GROUP" "$(stat_of "$TLS")"
check "(i2) client.key is 600 in group $SVC_GROUP" "600 $SVC_GROUP" "$(mode_group "$TLS/client.key")"
check "(i2) client.crt is 640 in group $SVC_GROUP" "640 $SVC_GROUP" "$(mode_group "$TLS/client.crt")"
check "(i2) identity.lock is 640 in group $SVC_GROUP" "640 $SVC_GROUP" "$(mode_group "$TLS/identity.lock")"
stop_daemon


# ─────────────────────────────── Report ─────────────────────────────────────
echo
echo "${BOLD}=================================================================${NC}"
echo "${BOLD} Results${NC}"
echo "${BOLD}=================================================================${NC}"
for r in "${RESULTS[@]}"; do echo "  $r"; done
echo "-----------------------------------------------------------------"
echo "  ${GREEN}${PASS_COUNT} passed${NC}, ${RED}${FAIL_COUNT} failed${NC}"

# ─────────────────────────────── Teardown ───────────────────────────────────
echo
if [ "$KEEP" -eq 1 ]; then
    step "Leaving state in place (--keep). Inspect with:"
    echo "    multipass exec $PERM_VM -- sudo ls -la $CFG_DIR"
    echo "    multipass exec $PERM_VM -- sudo tail -n 40 $LOG"
else
    step "Tearing down"
    stop_daemon
    # DB + both WAL sidecars, the whole config dir, the (d) fixtures; users stay (idempotent).
    msh "{ rm -f '$DB' '$DB-wal' '$DB-shm' '$DB.daemon.pid' '$DB.writer.lock'; rm -rf '$CFG_DIR' /home/alice/cfg /var/lib/zfsb-perm-root; } >/dev/null 2>&1; true" || true
    if [ "$PURGE" -eq 1 ]; then
        info "deleting VM $PERM_VM (--purge; this VM only, never a global purge)"
        multipass delete --purge "$PERM_VM"
    else
        info "VM left running for reuse (use --purge to delete it)"
    fi
fi

[ "$FAIL_COUNT" -eq 0 ] && exit 0 || exit 1
