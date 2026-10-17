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
#       (no EPERM), the daemon keeps running, a respawned worker re-reads the new config.
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
#
# Daemon choice: the point is DB ACCESS, not ZFS operations, so the daemon runs `--dry-run`
# against a STUB `zfs`/`zpool` (the same trick .github/workflows/tests.yml uses) — no pool, no
# `zfs allow` delegation, no ZFS install. Workers still start, still load the config from the
# DB as user zfsbackup, and are still respawned (which re-reads the DB) — that is exactly the
# access path under test.
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
# Usage:
#   ./scenarios/test_config_permissions.sh           # run all checks; VM reused, state reset
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
    if printf '%s' "$3" | grep -qE -- "$2"; then pass "$1"; else
        fail "$1 (no match for /$2/ in: $(printf '%s' "$3" | tr '\n' '|' | cut -c1-300))"
    fi
}
check_no_re() { # check_no_re "<description>" "<ERE>" "<text>"  — pass when the text does NOT match
    if printf '%s' "$3" | grep -qE -- "$2"; then
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

    # Ship the committed HEAD. `git archive` run from scenarios/ would archive ONLY that
    # subtree, so anchor it at the repo top level.
    step "Copying source into $PERM_VM:$SRC_DIR"
    local tar top; tar="$(mktemp)"
    top="$(git -C "$(dirname "$0")" rev-parse --show-toplevel)" || die "not in a git repo"
    git -C "$top" archive --format=tar HEAD > "$tar" || die "git archive failed"
    multipass transfer "$tar" "$PERM_VM:/tmp/zfsbackup-src.tar" || die "source transfer failed"
    rm -f "$tar"
    msh "rm -rf '$SRC_DIR' && mkdir -p '$SRC_DIR' && tar -m -xf /tmp/zfsbackup-src.tar -C '$SRC_DIR' && rm -f /tmp/zfsbackup-src.tar" \
        || die "source extract failed"

    msh "
        set -e
        test -x '$PY' || python3 -m venv '$VENV'
        '$PIP' install -q --upgrade pip
        '$PIP' install -q pyyaml 'flask>=3' flask-sock websocket-client requests \
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
export PYTHONPATH='$SRC_DIR' PYTHONDONTWRITEBYTECODE=1
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
    msh "{ systemctl stop $UNIT; systemctl reset-failed $UNIT; systemctl stop $HOLD_UNIT; systemctl reset-failed $HOLD_UNIT; } >/dev/null 2>&1; true" || true
}

# Wipe the default config state: units, DB + WAL sidecars (all three — a stale -wal next to a
# fresh DB is a confusing failure) and, when asked, the directory itself so root re-creates it.
reset_default_state() { # reset_default_state [keep-dir]
    stop_daemon
    local rmdir="rm -rf '$CFG_DIR'"; [ -n "${1:-}" ] && rmdir=":"
    msh "{ rm -f '$DB' '$DB-wal' '$DB-shm'; $rmdir; } >/dev/null 2>&1; : >'$LOG'; chown '$SVC_USER' '$LOG'; true" || true
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
worker_pid() { # worker_pid <snapshot|pruning|api>
    msh "grep -oE '(Started|Restarted) $1 worker \\(pid=[0-9]+\\)' '$LOG' | tail -n1 | grep -oE '[0-9]+'" 2>/dev/null | tr -d '[:space:]'
}
wait_workers_started() { # all three workers spawned (api is spawned last)
    local waited=0
    until [ -n "$(worker_pid api)" ] && [ -n "$(worker_pid snapshot)" ] && [ -n "$(worker_pid pruning)" ]; do
        [ "$waited" -ge "$DAEMON_TIMEOUT" ] && return 1; sleep 2; waited=$((waited+2))
    done
    sleep 3                              # let each worker finish its first config load
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
done
check "database still 660 group $SVC_GROUP after operators' imports" "660 root $SVC_GROUP" "$(stat_of "$DB")"
check "directory still 2770 root:$SVC_GROUP after operators' imports" "2770 root $SVC_GROUP" "$(stat_of "$CFG_DIR")"

daemon_active && pass "daemon still running after the operators' imports" || fail "daemon died during operators' imports"
check "supervisor pid unchanged" "$pid_before" "$(daemon_pid)"
check "no worker was restarted by the imports" "$workers_before" "snapshot=$(worker_pid snapshot) pruning=$(worker_pid pruning) api=$(worker_pid api)"
check_no_re "no worker exit logged during the imports" "exited with" "$(log_since "$mark_imports")"

# A respawned worker re-reads the DB as $SVC_USER: kill one, expect a restart + a clean load.
victim="$(worker_pid snapshot)"
mark="$(log_lines)"
msh "kill -9 $victim" || true
if wait_log "$mark" "Restarted snapshot worker" 90; then pass "supervisor respawned the killed snapshot worker"; else
    fail "no respawn logged after killing worker $victim"
fi
sleep 5                                  # the new worker reads the DB at startup
after="$(log_since "$mark")"
check_no_re "respawned worker read the re-imported DB without a config/permission error" "ConfigDb|EX_CONFIG|unable to open|Failed to load|ConfigPathError|SchemaError" "$after"
daemon_active && pass "daemon alive after respawn" || fail "daemon died after worker respawn"

out="$(asu "$SVC_USER" "'$PY' -m zfsbackup.daemon --test-config 2>&1")"; rc=$?
check "$SVC_USER --test-config (concurrent with the running daemon) exits 0" "0" "$rc"
check_re "$SVC_USER sees bob's last import (3 datasets, not root's 5)" "Datasets: 3" "$out"
stop_daemon

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
    msh "{ rm -f '$DB' '$DB-wal' '$DB-shm'; rm -rf '$CFG_DIR' /home/alice/cfg /var/lib/zfsb-perm-root; } >/dev/null 2>&1; true" || true
    if [ "$PURGE" -eq 1 ]; then
        info "deleting VM $PERM_VM (--purge; this VM only, never a global purge)"
        multipass delete --purge "$PERM_VM"
    else
        info "VM left running for reuse (use --purge to delete it)"
    fi
fi

[ "$FAIL_COUNT" -eq 0 ] && exit 0 || exit 1
