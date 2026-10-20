#!/usr/bin/env bash
#
# test_two_vm_backup.sh — End-to-end zfsbackup transfer reliability harness.
#
# Spins up TWO real Multipass VMs (a backup server and a client), installs ZFS and
# the zfsbackup daemon in each, creates file-backed ZFS pools, and drives the daemon
# through a set of reliability scenarios that exercise the REAL network transfer path
# (real IP, 0.0.0.0 bind, WebSocket over the Multipass bridge) — not the single-VM
# zfs-CLI routing used by the pytest suite.
#
# Same daemon binary on both VMs; the role is decided purely by config:
#   - server: remote_backup.enabled + target_dataset, api_host 0.0.0.0
#   - client: datasets[].remote + destinations pointing at the server
#
# Schedule is deliberately short and human-followable (seconds/minutes only):
#   client: check_interval 15s, snapshot frequency 30s, remote push 30s, prune 1m
#   server: check_interval 1m, prune 5m
#
# Scenarios: A full initial transfer, B incremental, C server-down resilience + catch-up, D client
# restart persistence, and (Phase 3, live config reload; skip with --no-big):
#   E  reload the CLIENT daemon during a large (2 GiB, throttled) send: the transfer completes, the
#      remote worker is only replaced after it, and the 60s deferral WARNING appears
#   F  reload the SERVER daemon during an inbound receive: the receive completes, the API worker is
#      only replaced after it
#   G  `kill -9` the client supervisor mid-send and start a new daemon at once: the new daemon waits
#      ("waiting for them to ...") for the surviving worker to finish, then starts
#   H  `rename dataset OLD NEW --clear-stale-props` while the client daemon RUNS (Linux, verified daemon): the
#      CLI signals it, waits for it to apply the rename, then clears OLD's org.zfsbackup:* properties
#   I  received datasets are pruned NON-recursively: the client dataset is `recursive: true`, the server's copy
#      has a child with its own long retention, and the server's pruner removes the parent's old snapshots
#      without ever taking the child's same-named ones (a recursive destroy would)
#   J  interrupted-transfer RESUME (needs a tree that has `zfs.send.resume_info` and `/stream?mode=`; skipped with
#      a warning otherwise). Runs on fresh pools after I; the client's snapshot frequency is 1d so every snapshot
#      is one the scenario takes itself:
#        J1  a throttled 400 MiB send is killed mid-stream (kill -9 of the client unit): the server keeps a
#            receive_resume_token and the client its `inflight.<dest>` pin; after a newer snapshot and a restart the
#            log shows "Resumed <ds>@<token's snap> (from ...)" and then a follow-up send to the latest snapshot in
#            the same cycle; token cleared, anchor == latest, no pin left
#        J2  stale token: interrupt again, destroy the token's TARGET snapshot, restart: WARNING "Not resuming ...
#            sending fresh", the server discards the partial state (`zfs receive -A`), the incremental succeeds
#        J3  the in-flight pin protects: with the server down and the pin set, the client's pruner (20s) takes an
#            expired control snapshot but never the pinned one; after the server returns the resume completes
#        J4  D7: a second client dataset (own pool) whose server path collides is refused with 409 at negotiate;
#            the first dataset's server copy is untouched
# H, I and J need a real pool but little space (J: ~1.2 GiB per VM); they run after E-G, and also with --no-big.
# E-G throttle the client's egress with `tc` (tbf) so a send lasts long enough to act on; the
# throttle is always removed again (also on error). They write ~3 GiB per VM: the VMs need ~10G disk
# (`multipass set local.<vm>.disk=10G` while stopped; VM_DISK=10G for a new VM).
#
# Usage:
#   ./scenarios/test_two_vm_backup.sh            # run all scenarios; reuse VMs; recreate pools
#   ./scenarios/test_two_vm_backup.sh --keep     # leave daemons + pools running for inspection
#   ./scenarios/test_two_vm_backup.sh --keep-pools
#   ./scenarios/test_two_vm_backup.sh --no-big   # scenarios A-D only (skip the slow E-G)
#   ./scenarios/test_two_vm_backup.sh --only-j   # provision, then only scenario J (fresh pools of its own)
#   ./scenarios/test_two_vm_backup.sh --purge    # delete the two VMs (and only those) at the end
#   SRC_REF=<commit-ish> ./scenarios/test_two_vm_backup.sh   # ship that tree instead of HEAD; for uncommitted work use
#                                                # SRC_REF=$(git stash create) (a commit of the working tree; the tree
#                                                # and the stash list are untouched; empty when the tree is clean)
#
# Requires: multipass on the host (macOS/Linux). No sudo needed on the host.
#
# NOTE ON READINESS: this harness deliberately works around two known defects so it can
# run at all — it (1) installs `requests` explicitly (it is imported by the client but
# missing from pyproject dependencies) and (2) binds the server API to 0.0.0.0 despite
# there being NO authentication/TLS on that API. Do not treat a green run here as a
# security sign-off.

set -uo pipefail

# ─────────────────────────────── Configuration ──────────────────────────────
SERVER_VM="${SERVER_VM:-zfsb-server}"
CLIENT_VM="${CLIENT_VM:-zfsb-client}"

SERVER_POOL="${SERVER_POOL:-zbserver}"
CLIENT_POOL="${CLIENT_POOL:-zbclient}"
TARGET_DS="${SERVER_POOL}/backups"       # server receiver root
SOURCE_DS="${CLIENT_POOL}/data"          # client dataset being backed up
SOURCE_REL="data"                        # SOURCE_DS with the pool stripped

API_PORT="${API_PORT:-8080}"
DEST_NAME="server1"                      # destination key in client config

SRC_DIR="/opt/zfsbackup-src"             # extracted repo inside each VM
VENV="/opt/zfsbvenv"
PY="${VENV}/bin/python"
PIP="${VENV}/bin/pip"
CFG="/etc/zfsbackup/config.yaml"         # YAML import input (the daemon no longer reads YAML)
DB="/var/lib/zfsbackup/config.db"        # SQLite config DB the daemon actually reads
CLIENT_ID_FILE="/var/lib/zfsbackup/client_id"
LOG="/var/log/zfsbackup.log"
IMG_DIR="${IMG_DIR:-/var/tmp}"            # pool backing files; NOT /tmp (a tmpfs, i.e. RAM, on these VMs)
DISK_SIZE="${DISK_SIZE:-5g}"                # sparse backing files; E-G write up to ~3 GiB

SRC_REF="${SRC_REF:-HEAD}"                # git ref (commit-ish) whose tree is shipped to the VMs

# Knobs the J scenarios turn (the defaults reproduce the A-I configs exactly)
SERVER_PRUNE="5m"                          # server prune_interval
CLIENT_SNAP_FREQ="30s"                     # dataset-level frequency of $SOURCE_DS (snapshot worker)
CLIENT_PRUNE="1m"                          # client prune_interval
CLIENT_RETENTION_YAML=$'      "1m": "10m"\n      "5m": "1h"'   # retention items of $SOURCE_DS (6-space indented)

VM_CPUS="${VM_CPUS:-1}"
VM_MEM="${VM_MEM:-1G}"
VM_DISK="${VM_DISK:-10G}"

# Timeouts (seconds)
HEALTH_TIMEOUT=60
SNAP_TIMEOUT=120         # ~4 snapshot periods

# Flags
PURGE=0; KEEP_POOLS=0; KEEP=0; BIG=1; ONLY_J=0
for arg in "$@"; do
    case "$arg" in
        --purge)      PURGE=1 ;;
        --keep-pools) KEEP_POOLS=1 ;;
        --no-big)     BIG=0 ;;
        --only-j)     ONLY_J=1 ;;     # provision + scenario J only (A-I skipped): for iterating on J
        --keep)       KEEP=1; KEEP_POOLS=1 ;;
        -h|--help)    grep '^#' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
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
    if [ "$2" = "$3" ]; then pass "$1"; else fail "$1 (expected='$2' actual='$3')"; fi
}

# ─────────────────────────────── Multipass helpers ──────────────────────────
# CRITICAL multipass-on-macOS quirk (verified 12/12 reproducible): `multipass exec`
# busy-spins a host CPU core FOREVER when the remote command writes to stdout AND the
# host redirects that stdout to /dev/null (e.g. `multipass exec vm -- cmd >/dev/null`).
# Host PIPES (`| tr`, `| sed`) and `$(...)` capture are fine; redirecting INSIDE the
# guest is fine. Rule: never send an exec's HOST stdout to /dev/null — discard output
# in the guest instead (`-o /dev/null`, `sh -c '... >/dev/null 2>&1'`).
mexec() { local vm="$1"; shift; multipass exec "$vm" -- "$@"; }
msh()   { local vm="$1"; shift; multipass exec "$vm" -- sudo bash -c "$*"; }

vm_exists()  { multipass info "$1" >/dev/null 2>&1; }
vm_running() { multipass info "$1" 2>/dev/null | awk -F': *' '/^State/{print $2}' | grep -qi running; }
vm_ip()      { multipass info "$1" 2>/dev/null | awk -F': *' '/^IPv4/{print $2; exit}'; }

ensure_vm() {
    local vm="$1"
    if ! vm_exists "$vm"; then
        step "Launching VM $vm"
        multipass launch --name "$vm" --cpus "$VM_CPUS" --memory "$VM_MEM" --disk "$VM_DISK" \
            || die "failed to launch $vm"
    elif ! vm_running "$vm"; then
        step "Starting existing VM $vm"
        multipass start "$vm" || die "failed to start $vm"
    else
        info "VM $vm already running"
    fi
}

install_stack() {
    local vm="$1"
    step "Provisioning $vm (ZFS + Python + zfsbackup)"

    # Idempotent apt install
    msh "$vm" '
        set -e
        export DEBIAN_FRONTEND=noninteractive
        need=""
        command -v zfs   >/dev/null 2>&1 || need="$need zfsutils-linux"
        command -v curl  >/dev/null 2>&1 || need="$need curl"
        command -v tc    >/dev/null 2>&1 || need="$need iproute2"
        dpkg -s python3-venv >/dev/null 2>&1 || need="$need python3-venv"
        dpkg -s python3-pip  >/dev/null 2>&1 || need="$need python3-pip"
        if [ -n "$need" ]; then apt-get update -qq && apt-get install -y -qq $need; fi
    ' || die "package install failed on $vm"

    # Ship the tree of $SRC_REF (default: committed HEAD) into the VM and extract. Write the archive to
    # a host temp file first, then transfer the file (avoids proxying host stdin through exec).
    step "Copying source into $vm:$SRC_DIR"
    local tar; tar="$(mktemp)"
    # `git archive` run inside scenarios/ would archive ONLY that subtree, so anchor it at the
    # repo top level.
    local top; top="$(git -C "$(dirname "$0")" rev-parse --show-toplevel)" || die "not in a git repo"
    git -C "$top" archive --format=tar "$SRC_REF" > "$tar" || die "git archive of '$SRC_REF' failed"
    multipass transfer "$tar" "$vm:/tmp/zfsbackup-src.tar" || die "source transfer failed for $vm"
    rm -f "$tar"
    msh "$vm" "rm -rf '$SRC_DIR' && mkdir -p '$SRC_DIR' && tar -xf /tmp/zfsbackup-src.tar -C '$SRC_DIR' && rm -f /tmp/zfsbackup-src.tar"

    # venv with runtime deps. We run from source via PYTHONPATH (pyproject only packages
    # libzfseasy, not zfsbackup), and install `requests` explicitly — see readiness note.
    msh "$vm" "
        set -e
        test -x '$PY' || python3 -m venv '$VENV'
        '$PIP' install -q --upgrade pip
        '$PIP' install -q pyyaml 'flask>=3' flask-sock websocket-client requests \
            'sqlalchemy>=2.0' 'alembic>=1.13' 'click>=8.1'
    " || die "pip install failed on $vm"

    msh "$vm" "mkdir -p /etc/zfsbackup /var/lib/zfsbackup"
}

# ─────────────────────────────── ZFS pool helpers ───────────────────────────
recreate_pool() {
    local vm="$1" pool="$2"
    step "Recreating ZFS pool $pool on $vm"
    msh "$vm" "
        set -e
        if zpool list '$pool' >/dev/null 2>&1; then zpool destroy -f '$pool'; fi
        img=${IMG_DIR}/${pool}.img
        rm -f \$img /tmp/${pool}.img          # (older runs kept the image in /tmp, which is a tmpfs = RAM)
        truncate -s '$DISK_SIZE' \$img
        zpool create '$pool' \$img
    " || die "pool creation failed on $vm ($pool)"
}

destroy_pool() {
    local vm="$1" pool="$2"
    # Suppress output in the guest (never redirect exec's host stdout to /dev/null).
    msh "$vm" "{ zpool destroy -f '$pool'; rm -f '${IMG_DIR}/${pool}.img' /tmp/${pool}.img; } >/dev/null 2>&1; true" || true
}

# ─────────────────────────────── Daemon control ─────────────────────────────
# The daemon is launched as a transient systemd unit: systemd-run returns immediately
# (so the exec call completes) and reparents the daemon to systemd. Its "Running as
# unit" message goes to stderr; stdout is empty, so no host redirect is needed.
# start_daemon <vm> [unit] [KillMode]  — the optional KillMode is for E-G: `process` leaves a worker
# running after its supervisor dies (the default `control-group` would kill it with the unit), and
# `mixed` is what zfsbackup/README.md recommends for a production unit.
start_daemon() {
    local vm="$1" unit="${2:-zfsbackup}" killmode="${3:-}" kmprop=""
    [ -n "$killmode" ] && kmprop="-p KillMode=$killmode"
    step "Starting daemon on $vm (unit $unit${killmode:+, KillMode=$killmode})"
    msh "$vm" "
        systemctl reset-failed $unit 2>/dev/null || true
        systemd-run --unit=$unit --collect $kmprop \
            -p StandardOutput=append:'$LOG' -p StandardError=append:'$LOG' \
            env PYTHONPATH='$SRC_DIR' '$PY' -m zfsbackup.daemon -c '$DB' -v
    " || die "could not start daemon on $vm"
}

stop_daemon() {
    local vm="$1"
    # All suppression happens in the guest; host stdout is never sent to /dev/null.
    msh "$vm" "{ systemctl stop zfsbackup zfsbackup-victim; systemctl reset-failed zfsbackup zfsbackup-victim; pkill -f 'zfsbackup.daemon'; } >/dev/null 2>&1; true" || true
}

daemon_running() { msh "$1" "pgrep -f 'zfsbackup.daemon' >/dev/null" 2>/dev/null; }

# ─────────────────────────────── Config writers ─────────────────────────────
# NOTE: config is delivered via `multipass transfer` (reliable) rather than piping a
# heredoc into `multipass exec ... tee` — the latter hangs/spins on host stdin.
# The daemon reads only the SQLite DB, so the YAML is imported (destructive FULL REPLACE;
# creates the DB if missing) and the daemon must be (re)started afterwards — no live reload.
# Import goes through the config CLI (`python -m zfsbackup.cli`: the venv does not pip-install the
# repo, so the `zfsbackup-config` console script does not exist; the module form is identical).
# --no-backup: the harness re-seeds the same VMs over and over and wants a plain full replace,
# not a pile of <db>.pre-import-<UTC>.yaml files next to the DB.
put_config() { # put_config <vm> ; reads config text on stdin, installs it to $CFG, imports into $DB
    local vm="$1" tmp
    tmp="$(mktemp)"
    cat > "$tmp"
    multipass transfer "$tmp" "$vm:/tmp/zfsbackup-config.yaml" || die "config transfer failed for $vm"
    msh "$vm" "install -D -m600 /tmp/zfsbackup-config.yaml '$CFG' && rm -f /tmp/zfsbackup-config.yaml" \
        || die "config install failed on $vm"
    rm -f "$tmp"
    msh "$vm" "env PYTHONPATH='$SRC_DIR' '$PY' -m zfsbackup.cli -c '$DB' import --no-backup '$CFG'" \
        || die "config import failed on $vm"
}

# Remove the YAML, the config DB with its WAL sidecars (a stale -wal beside a fresh DB is a
# confusing failure), and the lock files next to it: <db>.daemon.pid (daemon presence lock; a leftover
# one from a crash would name a dead supervisor and a stale generation), <db>.writer.lock (CLI writer
# lock) and <db>.workers.lock (held by workers; waited on by a restarted daemon). Daemons must be
# stopped first. Output suppressed in the guest.
remove_config() {
    local vm="$1"
    msh "$vm" "{ rm -f '$CFG' '$DB' '$DB-wal' '$DB-shm' '$DB.daemon.pid' '$DB.writer.lock' '$DB.workers.lock'; } >/dev/null 2>&1; true" || true
}

write_server_config() {
    step "Writing server config on $SERVER_VM"
    put_config "$SERVER_VM" <<EOF
# zfsbackup SERVER config (auto-generated by test_two_vm_backup.sh)
snapshot_prefix: "autosnap"
check_interval: "1m"
prune_interval: "${SERVER_PRUNE}"
api_host: "0.0.0.0"          # required so the client VM can reach it (no auth on this API!)
api_port: ${API_PORT}
remote_backup:
  enabled: true
  target_dataset: "${TARGET_DS}"
# datasets list must be non-empty; a disabled placeholder satisfies the loader.
datasets:
  - name: "${TARGET_DS}"
    enabled: false
    frequency: "1h"
    retention:
      "1h": "1d"
EOF
}

write_client_config() { # write_client_config <server-ip> [extra datasets-list YAML items, 2-space indented]
    local server_ip="$1" extra="${2:-}"
    step "Writing client config on $CLIENT_VM (server=${server_ip}:${API_PORT})"
    put_config "$CLIENT_VM" <<EOF
# zfsbackup CLIENT config (auto-generated by test_two_vm_backup.sh)
snapshot_prefix: "autosnap"
check_interval: "15s"
prune_interval: "${CLIENT_PRUNE}"
client_id_file: "${CLIENT_ID_FILE}"
destinations:
  ${DEST_NAME}:
    url: "http://${server_ip}:${API_PORT}"
datasets:
  - name: "${SOURCE_DS}"
    enabled: true
    frequency: "${CLIENT_SNAP_FREQ}"
    recursive: false
    retention:
${CLIENT_RETENTION_YAML}
    remote:
      - destination: ${DEST_NAME}
        frequency: "30s"
${extra}
EOF
}

# ─────────────────────────────── Poll / assert helpers ──────────────────────
server_ds() { echo "${TARGET_DS}/${CLIENT_ID}/${SOURCE_REL}"; }

server_snaps() { # short snapshot names present on the server for this client
    msh "$SERVER_VM" "zfs list -H -t snapshot -r -o name '$(server_ds)' 2>/dev/null" \
        | sed 's/.*@//' | grep -v '^$' || true
}
client_snaps() {
    msh "$CLIENT_VM" "zfs list -H -t snapshot -r -o name '$SOURCE_DS' 2>/dev/null" \
        | sed 's/.*@//' | grep -v '^$' || true
}
count() { grep -c . ; }

wait_for_server_count() { # wait_for_server_count <n> <timeout>
    local want="$1" timeout="$2" waited=0 have
    while :; do
        have="$(server_snaps | count)"
        [ "$have" -ge "$want" ] && return 0
        [ "$waited" -ge "$timeout" ] && return 1
        sleep 3; waited=$((waited+3))
    done
}

wait_for_file() { # wait_for_file <vm> <path> <timeout>
    local vm="$1" path="$2" timeout="$3" waited=0
    while :; do
        msh "$vm" "test -s '$path'" 2>/dev/null && return 0
        [ "$waited" -ge "$timeout" ] && return 1
        sleep 2; waited=$((waited+2))
    done
}

guid() { # guid <vm> <dataset@snap>
    msh "$1" "zfs get -H -o value guid '$2' 2>/dev/null" | tr -d '[:space:]'
}
anchor() { # current anchor snapshot name on the client
    msh "$CLIENT_VM" "zfs get -H -o value org.zfsbackup:anchor.${DEST_NAME} '$SOURCE_DS' 2>/dev/null" \
        | tr -d '[:space:]'
}
latest_client_snap() { client_snaps | sort | tail -n1; }
latest_server_snap() { server_snaps | sort | tail -n1; }

# Race-free assertion: the last-transferred anchor must be present on the server. (Do NOT
# compare the anchor to the client's *latest* snapshot — the client mints a new one every
# `frequency` seconds, so "latest" legitimately runs ahead of the just-transferred anchor.)
anchor_on_server() { # anchor_on_server "<description>"
    local a; a="$(anchor)"
    if [ -n "$a" ] && server_snaps | grep -qx "$a"; then
        pass "$1 (anchor=$a present on server)"
    else
        fail "$1 (anchor='$a' not found on server: [$(server_snaps | tr '\n' ' ')])"
    fi
}

# Integrity check on the ANCHOR snapshot (guaranteed present on the server and protected
# from pruning on the client), so it can't flake on the aggressive retention schedule.
guid_match_anchor() { # guid_match_anchor "<description>"
    local a cg sg; a="$(anchor)"
    cg="$(guid "$CLIENT_VM" "${SOURCE_DS}@${a}")"
    sg="$(guid "$SERVER_VM" "$(server_ds)@${a}")"
    if [ -n "$cg" ] && [ "$cg" = "$sg" ]; then
        pass "$1 (guid $cg matches for anchor $a)"
    else
        fail "$1 (anchor=$a client_guid='$cg' server_guid='$sg')"
    fi
}


# ─────────────────────── Live-reload helpers (scenarios E-G) ─────────────────
# The zfsbackup-config CLI on <vm>, against that VM's DB. The daemon runs as root there, so root's CLI
# is verified (kernel lock table + start token) and SIGHUPs it. stderr is merged: the notice is on stdout.
cli_vm() { local vm="$1"; shift; msh "$vm" "env PYTHONPATH='$SRC_DIR' '$PY' -m zfsbackup.cli -c '$DB' $* 2>&1"; }
# Last logged pid of worker <name> (a live reload logs "Reloaded <name> worker (pid=N) on generation G").
wpid() { # wpid <vm> <snapshot|pruning|remote|api>
    msh "$1" "grep -oE '(Started|Restarted|Reloaded) $2 worker \\(pid=[0-9]+\\)' '$LOG' | tail -n1 | grep -oE '[0-9]+'" | tr -d '[:space:]'
}
loglines()   { msh "$1" "wc -l < '$LOG'" | tr -d '[:space:]'; }
log_since_vm() { msh "$1" "tail -n +$(( $2 + 1 )) '$LOG'"; }
# line number (relative to the mark) of the first line matching an ERE; empty if none
log_lineno() { msh "$1" "tail -n +$(( $2 + 1 )) '$LOG' | grep -nE '$3' | head -n1 | cut -d: -f1" | tr -d '[:space:]'; }
wait_log_vm() { # wait_log_vm <vm> <mark> <ERE> <timeout>
    local waited=0
    while :; do
        log_since_vm "$1" "$2" | grep -qE -- "$3" && return 0
        [ "$waited" -ge "$4" ] && return 1
        sleep 2; waited=$((waited+2))
    done
}
# NB: the bracket trick keeps pgrep from matching its own `bash -c` command line.
send_running() { msh "$CLIENT_VM" "pgrep -f '[z]fs send' >/dev/null" 2>/dev/null; }
recv_running() { msh "$SERVER_VM" "pgrep -f '[z]fs (receive|recv)' >/dev/null" 2>/dev/null; }
wait_for_proc() { # wait_for_proc <send_running|recv_running> <timeout>
    local waited=0
    until "$1"; do [ "$waited" -ge "$2" ] && return 1; sleep 2; waited=$((waited+2)); done
}
append_random() { # append_random <MiB> <name>  — new incompressible file in the client dataset
    msh "$CLIENT_VM" "dd if=/dev/urandom of=\$(zfs get -H -o value mountpoint '$SOURCE_DS')/$2 bs=1M count=$1 status=none"
}
# Rate-limit the CLIENT's egress to the server (token bucket) so a send lasts long enough to act on.
# Always undone: scenarios call throttle_off, and an EXIT trap covers an early abort.
THROTTLE_DEV=""
throttle_on() { # throttle_on <rate e.g. 200mbit>
    THROTTLE_DEV="$(msh "$CLIENT_VM" "ip -o route get '$SERVER_IP' | sed -n 's/.* dev \\([^ ]*\\).*/\\1/p' | head -n1" | tr -d '[:space:]')"
    [ -n "$THROTTLE_DEV" ] || die "could not find the client's interface towards $SERVER_IP"
    msh "$CLIENT_VM" "tc qdisc del dev $THROTTLE_DEV root >/dev/null 2>&1; tc qdisc add dev $THROTTLE_DEV root tbf rate $1 burst 256kb latency 400ms" \
        || die "tc qdisc add failed on $CLIENT_VM (is iproute2 installed?)"
    info "client egress on $THROTTLE_DEV limited to $1"
}
throttle_off() {
    [ -n "$THROTTLE_DEV" ] || return 0
    msh "$CLIENT_VM" "tc qdisc del dev $THROTTLE_DEV root >/dev/null 2>&1; true" || true
    THROTTLE_DEV=""
}
trap throttle_off EXIT
# Enough room for the E-G data (about 3 GiB per VM plus ZFS overhead)?
big_space_ok() {
    local vm kb
    for vm in "$SERVER_VM" "$CLIENT_VM"; do
        kb="$(msh "$vm" "df -Pk '$IMG_DIR' | awk 'NR==2{print \$4}'" | tr -d '[:space:]')"
        if [ -z "$kb" ] || [ "$kb" -lt 4500000 ]; then
            warn "$vm has $((${kb:-0}/1024)) MiB free on $IMG_DIR (< 4.4 GiB): skipping scenarios E-G (grow it: multipass set local.$vm.disk=10G while stopped)"
            return 1
        fi
    done
}

# Value of a ZFS user property on <vm>:<dataset> when set locally or received; empty if unset/inherited.
zprop() { msh "$1" "zfs get -H -o value -s local,received '$3' '$2' 2>/dev/null" | tr -d '[:space:]'; }

# ═════════════════════════════════ Main ═════════════════════════════════════
command -v multipass >/dev/null 2>&1 || die "multipass not found on host — install it first"
command -v git >/dev/null 2>&1 || die "git not found on host"

echo "${BOLD}=================================================================${NC}"
echo "${BOLD} zfsbackup two-VM transfer reliability harness${NC}"
echo "${BOLD}=================================================================${NC}"

# ── Phase 1: provision VMs ──
ensure_vm "$SERVER_VM"
ensure_vm "$CLIENT_VM"
install_stack "$SERVER_VM"
install_stack "$CLIENT_VM"

# Which tree did the VMs get, and does it carry the resume support scenario J needs?
step "Shipped source: ref '$SRC_REF' ($(git -C "$(dirname "$0")" rev-parse --short "$SRC_REF" 2>/dev/null || echo '?'))"
J_READY=1
for vm in "$SERVER_VM" "$CLIENT_VM"; do
    if msh "$vm" "grep -q 'resume_info' '$SRC_DIR/libzfseasy/zfs.py' && grep -q 'mode=' '$SRC_DIR/zfsbackup/api.py'"; then
        info "$vm: shipped tree has zfs.send.resume_info (libzfseasy/zfs.py) and /stream mode= (zfsbackup/api.py)"
    else
        warn "$vm: shipped tree ($SRC_REF) lacks resume support: scenario J will be skipped"
        J_READY=0
    fi
done

SERVER_IP="$(vm_ip "$SERVER_VM")"; [ -n "$SERVER_IP" ] || die "could not determine $SERVER_VM IP"

if [ "$ONLY_J" -eq 0 ]; then    # (--only-j skips everything up to the Scenario J banner)
# ── Phase 2: fresh pools ──
stop_daemon "$SERVER_VM"; stop_daemon "$CLIENT_VM"
remove_config "$SERVER_VM"; remove_config "$CLIENT_VM"   # fresh DB (and no stale -wal/-shm)
recreate_pool "$SERVER_VM" "$SERVER_POOL"
recreate_pool "$CLIENT_VM" "$CLIENT_POOL"
msh "$SERVER_VM" "zfs create -p '$TARGET_DS'" || die "failed to create $TARGET_DS"
msh "$CLIENT_VM" "zfs create '$SOURCE_DS'"    || die "failed to create $SOURCE_DS"
msh "$CLIENT_VM" "rm -f '$CLIENT_ID_FILE'"    # fresh identity for a clean run

# Seed data + record source checksum
step "Seeding client data"
msh "$CLIENT_VM" "head -c 2000000 /dev/urandom > \$(zfs get -H -o value mountpoint '$SOURCE_DS')/file1.bin"
SEED_MD5="$(msh "$CLIENT_VM" "md5sum \$(zfs get -H -o value mountpoint '$SOURCE_DS')/file1.bin | cut -d' ' -f1" | tr -d '[:space:]')"
info "seed md5=$SEED_MD5"

# ── Phase 3: configs + daemons ──
write_server_config
write_client_config "$SERVER_IP"
start_daemon "$SERVER_VM"

step "Waiting for server API health at ${SERVER_IP}:${API_PORT}"
waited=0
until mexec "$CLIENT_VM" sh -c "curl -fsS --connect-timeout 5 --max-time 10 -o /dev/null 'http://${SERVER_IP}:${API_PORT}/health' >/dev/null 2>&1"; do
    [ "$waited" -ge "$HEALTH_TIMEOUT" ] && die "server API never became healthy (check: multipass exec $SERVER_VM -- sudo tail $LOG)"
    sleep 2; waited=$((waited+2))
done
info "server API healthy"

start_daemon "$CLIENT_VM"

# Discover the client id the daemon generated
step "Waiting for client identity"
wait_for_file "$CLIENT_VM" "$CLIENT_ID_FILE" 40 || die "client_id file never created (client daemon may have crashed: multipass exec $CLIENT_VM -- sudo tail $LOG)"
CLIENT_ID="$(msh "$CLIENT_VM" "cat '$CLIENT_ID_FILE'" | tr -d '[:space:]')"
info "client_id=$CLIENT_ID  →  server dataset $(server_ds)"

echo
step "SCENARIO A — full initial transfer"
if wait_for_server_count 1 "$SNAP_TIMEOUT"; then
    s1="$(latest_server_snap)"
    pass "snapshot transferred to server ($s1)"
    guid_match_anchor "content integrity: transferred snapshot byte-identical on server"
    anchor_on_server "client anchor points to a snapshot confirmed on the server"
else
    fail "no snapshot reached the server within ${SNAP_TIMEOUT}s"
fi

echo
step "SCENARIO B — incremental transfer"
before_b="$(server_snaps | count)"
msh "$CLIENT_VM" "head -c 1000000 /dev/urandom >> \$(zfs get -H -o value mountpoint '$SOURCE_DS')/file1.bin"
if wait_for_server_count "$((before_b+1))" "$SNAP_TIMEOUT"; then
    s2="$(latest_server_snap)"
    pass "second (incremental) snapshot transferred ($s2)"
    guid_match_anchor "content integrity: incremental snapshot byte-identical on server"
    anchor_on_server "client anchor advanced past the initial backup"
else
    fail "incremental snapshot did not reach the server within ${SNAP_TIMEOUT}s"
fi

echo
step "SCENARIO C — server-down resilience + catch-up"
anchor_before_c="$(anchor)"
server_count_before_c="$(server_snaps | count)"
stop_daemon "$SERVER_VM"
info "server daemon stopped; mutating client data while server is down"
msh "$CLIENT_VM" "head -c 500000 /dev/urandom >> \$(zfs get -H -o value mountpoint '$SOURCE_DS')/file1.bin"
sleep 45   # allow a client snapshot + a failed push attempt

client_snaps_during="$(client_snaps | count)"
if [ "$client_snaps_during" -ge 3 ]; then
    pass "client kept taking local snapshots while server was down ($client_snaps_during total)"
else
    fail "client did not continue local snapshots while server down (have $client_snaps_during)"
fi
check "anchor did NOT advance while server unreachable" "$anchor_before_c" "$(anchor)"
check "no new snapshot appeared on server while down" "$server_count_before_c" "$(server_snaps | count)"
if msh "$CLIENT_VM" "grep -qiE 'registration failed|transfer failed|connection' '$LOG'" 2>/dev/null; then
    pass "client logged the transfer failure gracefully (no crash)"
else
    warn "did not find an explicit failure log line (non-fatal)"
fi
daemon_running "$CLIENT_VM" && pass "client daemon still alive after server outage" || fail "client daemon died during server outage"

info "restarting server daemon; expecting client to catch up"
start_daemon "$SERVER_VM"
waited=0
until mexec "$CLIENT_VM" sh -c "curl -fsS --connect-timeout 5 --max-time 10 -o /dev/null 'http://${SERVER_IP}:${API_PORT}/health' >/dev/null 2>&1"; do
    [ "$waited" -ge "$HEALTH_TIMEOUT" ] && break; sleep 2; waited=$((waited+2))
done
if wait_for_server_count "$((server_count_before_c+1))" "$SNAP_TIMEOUT"; then
    pass "client caught up after server returned"
    anchor_on_server "anchor advanced to a snapshot confirmed on the server after catch-up"
else
    fail "client did not catch up within ${SNAP_TIMEOUT}s after server returned"
fi

echo
step "SCENARIO D — client restart persistence (stable identity, incremental resume)"
id_before="$CLIENT_ID"
count_before_d="$(server_snaps | count)"
stop_daemon "$CLIENT_VM"
sleep 2
start_daemon "$CLIENT_VM"
id_after="$(msh "$CLIENT_VM" "cat '$CLIENT_ID_FILE'" | tr -d '[:space:]')"
check "client_id stable across restart" "$id_before" "$id_after"
msh "$CLIENT_VM" "head -c 300000 /dev/urandom >> \$(zfs get -H -o value mountpoint '$SOURCE_DS')/file1.bin"
if wait_for_server_count "$((count_before_d+1))" "$SNAP_TIMEOUT"; then
    grew="$(server_snaps | count)"
    if [ "$grew" -le "$((count_before_d+3))" ]; then
        pass "post-restart transfer was incremental (server grew by $((grew-count_before_d)), no full re-send)"
    else
        fail "server snapshot count jumped unexpectedly after restart (before=$count_before_d after=$grew)"
    fi
else
    fail "no transfer after client restart within ${SNAP_TIMEOUT}s"
fi


# ═════════════════════ Phase 3: live reload under transfer load ═════════════════════
if [ "$BIG" -eq 1 ] && big_space_ok; then

echo
step "SCENARIO E — reload the CLIENT daemon during a large send (2 GiB, throttled to 200 Mbit)"
stop_daemon "$CLIENT_VM"
info "writing 2 GiB of random data on the client while its daemon is stopped"
append_random 2048 big_e.bin || fail "could not write the 2 GiB file"
throttle_on 200mbit
start_daemon "$CLIENT_VM"
if wait_for_proc send_running 240; then
    pass "a zfs send is running on the client"
    mark_e="$(loglines "$CLIENT_VM")"
    rpid_e="$(wpid "$CLIENT_VM" remote)"; spid_e="$(wpid "$CLIENT_VM" snapshot)"
    sup_e="$(msh "$CLIENT_VM" "systemctl show -p MainPID --value zfsbackup" | tr -d '[:space:]')"
    info "client supervisor=$sup_e remote worker=$rpid_e snapshot worker=$spid_e"
    out="$(cli_vm "$CLIENT_VM" set check_interval 20s)"; t_rel="$(date +%s)"
    g_e="$(printf '%s' "$out" | sed -n 's/.*config updated (generation \([0-9]*\)).*/\1/p' | head -n1)"
    echo "    CLI: $(printf '%s' "$out" | grep 'config updated' | tail -n1)"
    case "$out" in *"reload requested from daemon pid $sup_e"*) pass "CLI SIGHUPed the client daemon (pid $sup_e)" ;; *) fail "CLI did not report a reload request: $out" ;; esac
    if wait_log_vm "$CLIENT_VM" "$mark_e" "configuration generation $g_e accepted .*trigger: SIGHUP" 8; then
        pass "client daemon accepted generation $g_e (trigger: SIGHUP)"
    else fail "no 'accepted' line for generation $g_e"; fi
    # the snapshot worker turns over at once, while the send is still running
    waited=0; until [ "$(wpid "$CLIENT_VM" snapshot)" != "$spid_e" ] || [ "$waited" -ge 40 ]; do sleep 2; waited=$((waited+2)); done
    if [ "$(wpid "$CLIENT_VM" snapshot)" != "$spid_e" ]; then pass "the snapshot worker switched to the new generation"; else fail "snapshot worker never switched"; fi
    # while the send runs the remote worker must stay put
    mid_switch=0; saw_mid=0
    while send_running; do
        saw_mid=1
        if [ "$(wpid "$CLIENT_VM" remote)" != "$rpid_e" ] && send_running; then mid_switch=1; fi
        sleep 4
    done
    t_done="$(date +%s)"; dur=$((t_done - t_rel))
    if [ "$saw_mid" -eq 1 ]; then pass "the send was still running after the reload was accepted"; else fail "the send finished before it could be observed (too fast?)"; fi
    check "the remote worker was NOT replaced while its send was running" "0" "$mid_switch"
    if wait_log_vm "$CLIENT_VM" "$mark_e" "Backup complete: ${SOURCE_DS}@" 90; then pass "the transfer completed ('Backup complete')"; else fail "no 'Backup complete' after the reload"; fi
    waited=0; until [ "$(wpid "$CLIENT_VM" remote)" != "$rpid_e" ] || [ "$waited" -ge 90 ]; do sleep 2; waited=$((waited+2)); done
    if [ "$(wpid "$CLIENT_VM" remote)" != "$rpid_e" ]; then pass "the remote worker was replaced after the transfer (pid $rpid_e -> $(wpid "$CLIENT_VM" remote))"; else fail "remote worker never switched"; fi
    l_done="$(log_lineno "$CLIENT_VM" "$mark_e" 'Backup complete')"; l_swap="$(log_lineno "$CLIENT_VM" "$mark_e" 'Reloaded remote worker')"
    if [ -n "$l_done" ] && [ -n "$l_swap" ] && [ "$l_done" -lt "$l_swap" ]; then pass "log order: 'Backup complete' (line $l_done) comes before 'Reloaded remote worker' (line $l_swap)"; else
        fail "log order wrong: Backup complete at '$l_done', Reloaded remote worker at '$l_swap'"; fi
    if [ "$dur" -ge 65 ]; then
        if log_since_vm "$CLIENT_VM" "$mark_e" | grep -qE "remote worker \(pid=$rpid_e\) is still finishing work begun under generation"; then
            pass "send outlived the reload by ${dur}s: the deferral WARNING was logged"
        else fail "send outlived the reload by ${dur}s but no deferral WARNING was logged"; fi
    else
        info "the send finished ${dur}s after the reload (< 60s): the deferral WARNING is not expected"
    fi
    check "no transfer failure, crash or traceback logged around the reload" "0" \
        "$(log_since_vm "$CLIENT_VM" "$mark_e" | grep -cE 'Transfer failed|crash|Traceback')"
    guid_match_anchor "reloaded-during-send: anchor snapshot byte-identical on the server"
    anchor_on_server "reloaded-during-send: anchor confirmed on the server"
else
    fail "no zfs send observed on the client within 240s"
fi
throttle_off

echo
step "SCENARIO F — reload the SERVER daemon during an inbound receive (512 MiB, throttled to 100 Mbit)"
cnt_f="$(server_snaps | count)"
append_random 512 big_f.bin || fail "could not write the 512 MiB file"
throttle_on 100mbit
if wait_for_proc recv_running 240; then
    pass "a zfs receive is running on the server"
    mark_f="$(loglines "$SERVER_VM")"; mark_fc="$(loglines "$CLIENT_VM")"
    api_f="$(wpid "$SERVER_VM" api)"
    sup_f="$(msh "$SERVER_VM" "systemctl show -p MainPID --value zfsbackup" | tr -d '[:space:]')"
    out="$(cli_vm "$SERVER_VM" set check_interval 2m)"
    g_f="$(printf '%s' "$out" | sed -n 's/.*config updated (generation \([0-9]*\)).*/\1/p' | head -n1)"
    echo "    CLI: $(printf '%s' "$out" | grep 'config updated' | tail -n1)"
    case "$out" in *"reload requested from daemon pid $sup_f"*) pass "CLI SIGHUPed the server daemon (pid $sup_f)" ;; *) fail "CLI did not report a reload request: $out" ;; esac
    if wait_log_vm "$SERVER_VM" "$mark_f" "configuration generation $g_f accepted .*trigger: SIGHUP" 8; then pass "server daemon accepted generation $g_f"; else fail "server never accepted generation $g_f"; fi
    mid_switch=0; saw_mid=0
    while recv_running; do
        saw_mid=1
        if [ "$(wpid "$SERVER_VM" api)" != "$api_f" ] && recv_running; then mid_switch=1; fi
        sleep 3
    done
    if [ "$saw_mid" -eq 1 ]; then pass "the receive was still running after the reload was accepted"; else fail "the receive finished before it could be observed"; fi
    check "the API worker was NOT replaced while the receive was running" "0" "$mid_switch"
    if wait_for_server_count "$((cnt_f+1))" 90; then pass "the receive completed: a new snapshot is on the server"; else fail "no new snapshot reached the server"; fi
    if wait_log_vm "$CLIENT_VM" "$mark_fc" "Backup complete: ${SOURCE_DS}@" 90; then pass "the client logged 'Backup complete' for the transfer that outlived the server reload"; else fail "client never logged 'Backup complete'"; fi
    waited=0; until [ "$(wpid "$SERVER_VM" api)" != "$api_f" ] || [ "$waited" -ge 90 ]; do sleep 2; waited=$((waited+2)); done
    if [ "$(wpid "$SERVER_VM" api)" != "$api_f" ]; then pass "the API worker was replaced after the receive (pid $api_f -> $(wpid "$SERVER_VM" api))"; else fail "API worker never switched"; fi
    check "no 'Transfer failed' on the client around the server reload" "0" "$(log_since_vm "$CLIENT_VM" "$mark_fc" | grep -c 'Transfer failed')"
    guid_match_anchor "server-reloaded-during-receive: anchor snapshot byte-identical on the server"
    anchor_on_server "server-reloaded-during-receive: anchor confirmed on the server"
else
    fail "no zfs receive observed on the server within 240s"
fi
throttle_off

echo
step "SCENARIO G — kill -9 the client supervisor mid-send, start a new daemon at once (512 MiB, 100 Mbit)"
stop_daemon "$CLIENT_VM"
append_random 512 big_g.bin || fail "could not write the 512 MiB file"
throttle_on 100mbit
cnt_g="$(server_snaps | count)"
start_daemon "$CLIENT_VM" zfsbackup-victim process      # KillMode=process: a worker outlives its supervisor
if wait_for_proc send_running 240; then
    pass "a zfs send is running on the client"
    sup_g="$(msh "$CLIENT_VM" "systemctl show -p MainPID --value zfsbackup-victim" | tr -d '[:space:]')"
    rpid_g="$(wpid "$CLIENT_VM" remote)"
    mark_g="$(loglines "$CLIENT_VM")"
    info "killing supervisor $sup_g (remote worker $rpid_g is mid-send)"
    msh "$CLIENT_VM" "kill -9 $sup_g" || true
    start_daemon "$CLIENT_VM" zfsbackup mixed
    if wait_log_vm "$CLIENT_VM" "$mark_g" "waiting for them to (exit|finish)" 40; then
        pass "the new daemon logs that it is waiting for the survivor(s) of the killed daemon"
        echo "    $(log_since_vm "$CLIENT_VM" "$mark_g" | grep -E 'waiting for them to' | head -n1 | cut -c1-220)"
    else
        fail "no 'waiting for them to ...' line from the new daemon: $(log_since_vm "$CLIENT_VM" "$mark_g" | tail -n 6 | tr '\n' '|')"
    fi
    if msh "$CLIENT_VM" "kill -0 $rpid_g"; then pass "the surviving remote worker (pid $rpid_g) is still alive"; else fail "the surviving worker died with its supervisor"; fi
    if send_running; then pass "its zfs send is still running"; else warn "the send had already finished"; fi
    # until the survivor is done, the new daemon must not have started any worker
    started_early=0
    while send_running; do
        [ "$(log_since_vm "$CLIENT_VM" "$mark_g" | grep -cE 'Started .* worker')" -gt 0 ] && started_early=1
        sleep 3
    done
    check "the new daemon started no worker while the survivor was still sending" "0" "$started_early"
    if wait_log_vm "$CLIENT_VM" "$mark_g" "Backup complete: ${SOURCE_DS}@" 60; then pass "the survivor completed the transfer ('Backup complete')"; else fail "the survivor never completed the transfer"; fi
    if wait_log_vm "$CLIENT_VM" "$mark_g" "Started remote worker" 120; then pass "the new daemon started its workers after the survivor finished"; else
        fail "the new daemon never started its workers: $(log_since_vm "$CLIENT_VM" "$mark_g" | tail -n 6 | tr '\n' '|')"; fi
    l_done="$(log_lineno "$CLIENT_VM" "$mark_g" 'Backup complete')"; l_start="$(log_lineno "$CLIENT_VM" "$mark_g" 'Started .* worker')"
    if [ -n "$l_done" ] && [ -n "$l_start" ] && [ "$l_done" -lt "$l_start" ]; then pass "log order: 'Backup complete' (line $l_done) precedes the new daemon's first 'Started ... worker' (line $l_start)"; else
        fail "log order wrong: Backup complete at '$l_done', first Started at '$l_start'"; fi
    if daemon_running "$CLIENT_VM"; then pass "the new client daemon is running"; else fail "the new client daemon is not running"; fi
    if wait_for_server_count "$((cnt_g+1))" 60; then pass "the killed daemon's transfer reached the server"; else fail "no new snapshot on the server"; fi
    guid_match_anchor "kill -9 mid-send: anchor snapshot byte-identical on the server"
else
    fail "no zfs send observed on the client within 240s"
fi
throttle_off
fi


# ═════════════ Follow-ups round 2026-10-19: rename --clear-stale-props, non-recursive pruning ═════════════
# cfgprop.py: decode/rewrite an org.zfsbackup:config value (base64 JSON) inside a VM.
put_guest_file() { # put_guest_file <vm> <guest-path>   (content on stdin; via multipass transfer, never piped exec)
    local vm="$1" dst="$2" tmp; tmp="$(mktemp)"; cat > "$tmp"
    multipass transfer "$tmp" "$vm:/tmp/zfsb-xfer.$$" || die "transfer to $vm:$dst failed"
    msh "$vm" "install -D -m644 /tmp/zfsb-xfer.$$ '$dst' && rm -f /tmp/zfsb-xfer.$$" || die "install of $dst failed on $vm"
    rm -f "$tmp"
}
CFGPROP_PY="/opt/zfsb-cfgprop.py"
put_guest_file "$SERVER_VM" "$CFGPROP_PY" <<'PYEOF'
import base64, json, sys
# cfgprop.py recursive <value>        -> prints true/false (the "recursive" field)
# cfgprop.py longret <value> <name>   -> prints a copy renamed <name>, non-recursive, one rule keeping hourly for 10y
mode, value = sys.argv[1], sys.argv[2]
data = json.loads(base64.b64decode(value))
if mode == "recursive":
    print(str(data["recursive"]).lower())
else:
    data["name"] = sys.argv[3]
    data["recursive"] = False
    data["retention_rules"] = [{"age": 3600.0, "keep_for": 10 * 365 * 86400.0}]
    print(base64.b64encode(json.dumps(data, separators=(",", ":")).encode()).decode())
PYEOF

echo
step "SCENARIO H — rename dataset --clear-stale-props while the client daemon is RUNNING (verified daemon, Linux)"
REN_OLD="${CLIENT_POOL}/ren_old"; REN_NEW="${CLIENT_POOL}/ren_new"
msh "$CLIENT_VM" "zfs create '$REN_OLD' && zfs create '$REN_NEW'" || fail "could not create $REN_OLD / $REN_NEW"
# Configure ren_old next to the real dataset (import = full replace; the CLI signals the running daemon).
write_client_config "$SERVER_IP" "  - name: \"${REN_OLD}\"
    enabled: true
    frequency: \"30s\"
    recursive: false
    retention:
      \"1m\": \"10m\""
waited=0; until [ -n "$(zprop "$CLIENT_VM" "$REN_OLD" org.zfsbackup:config)" ] || [ "$waited" -ge 120 ]; do sleep 3; waited=$((waited+3)); done
if [ -n "$(zprop "$CLIENT_VM" "$REN_OLD" org.zfsbackup:config)" ]; then pass "the running daemon stamped org.zfsbackup:config on $REN_OLD"; else
    fail "the daemon never set org.zfsbackup:config on $REN_OLD"; fi
msh "$CLIENT_VM" "zfs set org.zfsbackup:anchor.stale=autosnap_19990101000000 '$REN_OLD'" || fail "could not set a stale anchor"
sup_h="$(msh "$CLIENT_VM" "systemctl show -p MainPID --value zfsbackup" | tr -d '[:space:]')"
mark_h="$(loglines "$CLIENT_VM")"
out="$(cli_vm "$CLIENT_VM" --reload-wait 40 rename dataset "$REN_OLD" "$REN_NEW" --clear-stale-props)"; rc=$?
echo "$out" | sed 's/^/    CLI: /'
check "(H) rename --clear-stale-props with a running daemon exits 0" "0" "$rc"
case "$out" in *"reload requested from daemon pid $sup_h"*) pass "(H) the CLI verified and signalled the running client daemon (pid $sup_h)" ;; *) fail "(H) no reload request reported for pid $sup_h" ;; esac
case "$out" in *"Cleared on '$REN_OLD': "*) pass "(H) the CLI reports it cleared the stale properties" ;; *) fail "(H) no 'Cleared on' line" ;; esac
if printf '%s' "$out" | grep -q "Cleared on" && ! printf '%s' "$out" | grep -qE "inherit "; then pass "(H) no 'zfs inherit' fallback commands were printed"; else fail "(H) the CLI fell back to printing zfs inherit commands"; fi
check "(H) no org.zfsbackup:* property is left on $REN_OLD" "0" \
    "$(msh "$CLIENT_VM" "zfs get -H -o property -s local,received all '$REN_OLD' | grep -c '^org.zfsbackup:'" | tr -d '[:space:]')"
wait_log_vm "$CLIENT_VM" "$mark_h" "accepted .*trigger: SIGHUP" 10 && pass "(H) the daemon accepted the rename by SIGHUP" || fail "(H) no 'accepted ... SIGHUP' line for the rename"
waited=0; until [ -n "$(zprop "$CLIENT_VM" "$REN_NEW" org.zfsbackup:config)" ] || [ "$waited" -ge 120 ]; do sleep 3; waited=$((waited+3)); done
if [ -n "$(zprop "$CLIENT_VM" "$REN_NEW" org.zfsbackup:config)" ]; then pass "(H) the daemon now manages $REN_NEW (org.zfsbackup:config set there)"; else fail "(H) $REN_NEW never got org.zfsbackup:config"; fi
sleep 40   # a worker still on the OLD name would re-stamp it within a snapshot cycle
check "(H) $REN_OLD was not re-stamped by a lingering worker" "" "$(zprop "$CLIENT_VM" "$REN_OLD" org.zfsbackup:config)"
msh "$CLIENT_VM" "zfs list -H -o name '$REN_OLD' >/dev/null 2>&1" && pass "(H) the old ZFS dataset itself is untouched" || fail "(H) $REN_OLD vanished"

echo
step "SCENARIO I — received datasets are pruned NON-recursively (a received parent never takes its child's history)"
# 1. the client dataset becomes recursive: true, which the server's copy receives with the next transfer
cli_vm "$CLIENT_VM" set "datasets[${SOURCE_DS}].recursive" true | sed 's/^/    CLI: /'
cnt_i="$(server_snaps | count)"
if wait_for_server_count "$((cnt_i+1))" "$SNAP_TIMEOUT"; then pass "(I) a transfer ran after the client dataset became recursive"; else fail "(I) no transfer after the recursive change"; fi
RCV="$(server_ds)"; RCV_CHILD="${RCV}/child"
rec_prop="$(msh "$SERVER_VM" "'$PY' '$CFGPROP_PY' recursive \"\$(zfs get -H -o value org.zfsbackup:config '$RCV')\"" | tr -d '[:space:]')"
check "(I) the server's received copy of the dataset carries recursive=true (the client's setting)" "true" "$rec_prop"
# 2. quiesce the client (no transfer may meet the planted snapshots), then give the received parent a child with
#    its OWN long retention, and plant old same-named snapshots on both. A recursive destroy of the parent's
#    old snapshots would also take the child's; retention for the child says keep them (hourly, 10y).
stop_daemon "$CLIENT_VM"
msh "$SERVER_VM" "
    set -e
    zfs create '$RCV_CHILD'
    zfs set org.zfsbackup:config=\"\$('$PY' '$CFGPROP_PY' longret \"\$(zfs get -H -o value org.zfsbackup:config '$RCV')\" '$RCV_CHILD')\" '$RCV_CHILD'
    for d in '$RCV' '$RCV_CHILD'; do zfs snapshot \$d@autosnap_20200101000000; zfs snapshot \$d@autosnap_20210101000000; done
" || fail "(I) could not set up the child and the planted snapshots on the server"
planted() { msh "$SERVER_VM" "zfs list -H -t snapshot -o name '$1' 2>/dev/null | grep -c 'autosnap_202[01]0101000000'" | tr -d '[:space:]'; }
check "(I) precondition: 2 planted snapshots on the parent and 2 on the child" "2 2" "$(planted "$RCV") $(planted "$RCV_CHILD")"
# 3. speed the server's pruner up (live reload through the CLI), then wait for the parent's old snapshots to go
cli_vm "$SERVER_VM" set prune_interval 20s | sed 's/^/    CLI: /'
waited=0; until [ "$(planted "$RCV")" = "0" ] || [ "$waited" -ge 150 ]; do sleep 5; waited=$((waited+5)); done
check "(I) the server pruned the parent's old snapshots" "0" "$(planted "$RCV")"
check "(I) the child's same-named snapshots survived (pruning the parent is not recursive)" "2" "$(planted "$RCV_CHILD")"
check "(I) the child dataset still exists" "yes" "$(msh "$SERVER_VM" "zfs list -H -o name '$RCV_CHILD' >/dev/null 2>&1 && echo yes || echo no" | tr -d '[:space:]')"
check "(I) no 'Failed' / traceback in the server log around the prune" "0" "$(msh "$SERVER_VM" "tail -n 200 '$LOG' | grep -cE 'Traceback|Failed to'" | tr -d '[:space:]')"
fi    # ONLY_J == 0

# ═════════════ Scenario J: resuming an interrupted transfer (resume token, in-flight pin, D7) ═════════════
# Needs a tree with the resume work (J_READY, see the source check after install_stack). Starts from FRESH pools
# (E-I leave several GiB, a recursive dataset, a child and a 20s server pruner behind) and a config in which the
# snapshot worker never fires (dataset frequency 1d): every snapshot is one this scenario takes, so "latest" is
# exactly what it says. Server pruner 1h (it would take T once N exists), client pruner 20s.
J_FILE_MIB="${J_FILE_MIB:-400}"       # per interrupted send; ~1.2 GiB per VM over J1-J3
J_RATE="${J_RATE:-80mbit}"            # 400 MiB at 80 Mbit is ~42s: long enough to kill it ~10s in
J_PIN="org.zfsbackup:inflight.${DEST_NAME}"
J_SNAP=""                             # the snapshot j_interrupt sent (and killed)

# 'receive_resume_token' of the server's copy: '-' when there is none
server_token() {
    local t; t="$(msh "$SERVER_VM" "zfs get -H -o value receive_resume_token '$(server_ds)' 2>/dev/null" | tr -d '[:space:]')"
    echo "${t:--}"
}
client_has_snap() { client_snaps | grep -qx "$1"; }
wait_server_snap() { # wait_server_snap <short name> <timeout>
    local waited=0
    until server_snaps | grep -qx "$1"; do [ "$waited" -ge "$2" ] && return 1; sleep 3; waited=$((waited+3)); done
}
# New snapshot <SOURCE_DS>@autosnap_<UTC now> on the client (the 2s sleep keeps names unique); prints the short name.
snap_now() {
    sleep 2
    msh "$CLIENT_VM" "n=autosnap_\$(date -u +%Y%m%d%H%M%S); zfs snapshot '${SOURCE_DS}'@\$n && echo \$n" | tr -d '[:space:]'
}
# SIGKILL the whole client unit: supervisor, workers and their `zfs send`, at the same instant. (A graceful stop
# would let the worker finish its transfer first.) The brackets keep pkill from matching this shell's own command line.
kill_client_hard() {
    msh "$CLIENT_VM" "{ systemctl kill -s SIGKILL zfsbackup; pkill -9 -f '[z]fsbackup.daemon'; pkill -9 -f '[z]fs send'; sleep 1; systemctl stop zfsbackup; systemctl reset-failed zfsbackup; } >/dev/null 2>&1; true" || true
}
# j_interrupt <file> <MiB>: with the client daemon stopped, append <MiB> of random data, take the snapshot J_SNAP, start
# the daemon under a throttle and kill -9 the client unit ~10s into the send. Returns 0 only if a send was running when
# it was killed and the server then held a resume token. The client's expected pin afterwards is "<anchor>,<J_SNAP>".
j_interrupt() {
    local waited=0
    stop_daemon "$CLIENT_VM"
    append_random "$2" "$1" || { fail "could not write the $2 MiB file"; return 1; }
    J_SNAP="$(snap_now)"
    throttle_on "$J_RATE"
    start_daemon "$CLIENT_VM"
    if ! wait_for_proc send_running 120; then
        fail "no zfs send observed on the client within 120s"; throttle_off; return 1
    fi
    sleep 10
    if ! send_running; then
        fail "the send of $J_SNAP finished before it could be interrupted (raise J_FILE_MIB or lower J_RATE)"; throttle_off; return 1
    fi
    kill_client_hard
    throttle_off
    while [ "$(server_token)" = "-" ]; do
        [ "$waited" -ge 60 ] && { fail "the server holds no receive_resume_token 60s after the client was killed"; return 1; }
        sleep 2; waited=$((waited+2))
    done
    pass "a send of $J_SNAP was killed mid-stream; the server holds a resume token"
}
show_log() { # show_log <vm> <mark> <ERE>: the matching log lines since <mark>, indented, truncated
    log_since_vm "$1" "$2" | grep -E -- "$3" | cut -c1-230 | sed 's/^/    log: /'
}
# j_synced <label> <snapshot>: the end state of a successful transfer of <snapshot>
j_synced() {
    check "($1) the server's resume token is cleared" "-" "$(server_token)"
    check "($1) the client's anchor is $2" "$2" "$(anchor)"
    check "($1) no org.zfsbackup:inflight.* property is left on the client" "" "$(zprop "$CLIENT_VM" "$SOURCE_DS" "$J_PIN")"
    local cg sg; cg="$(guid "$CLIENT_VM" "${SOURCE_DS}@$2")"; sg="$(guid "$SERVER_VM" "$(server_ds)@$2")"
    if [ -n "$cg" ] && [ "$cg" = "$sg" ]; then pass "($1) $2 is byte-identical on the server (guid $cg)"; else fail "($1) guid of $2: client='$cg' server='$sg'"; fi
}

# NB: when the newest snapshot is already on the server (latest == common) the client still starts `zfs send -i X X`, which
# zfs refuses ("not an earlier snapshot"); that logs "Transfer failed" every remote cycle and leaves an `inflight` pin
# X,X behind. Scenario J therefore stops the client daemon as soon as a transfer has completed (the anchor, the pin and
# the log line are final by then) and asserts afterwards.

echo
step "SCENARIO J — resume of an interrupted transfer (token validation, in-flight pin, fresh fallback, D7)"
if [ "$J_READY" -ne 1 ]; then
    warn "scenario J skipped: the shipped tree has no resume support"
else
    # fresh pools, fresh DBs, J's config knobs
    stop_daemon "$SERVER_VM"; stop_daemon "$CLIENT_VM"
    remove_config "$SERVER_VM"; remove_config "$CLIENT_VM"
    recreate_pool "$SERVER_VM" "$SERVER_POOL"; recreate_pool "$CLIENT_VM" "$CLIENT_POOL"
    msh "$SERVER_VM" "zfs create -p '$TARGET_DS'" || die "failed to create $TARGET_DS"
    msh "$CLIENT_VM" "zfs create '$SOURCE_DS'"    || die "failed to create $SOURCE_DS"
    msh "$CLIENT_VM" "head -c 2000000 /dev/urandom > \$(zfs get -H -o value mountpoint '$SOURCE_DS')/file1.bin"
    SERVER_PRUNE="1h"; CLIENT_SNAP_FREQ="1d"; CLIENT_PRUNE="20s"
    CLIENT_RETENTION_YAML=$'      "1h": "1d"'
    write_server_config
    write_client_config "$SERVER_IP"
    start_daemon "$SERVER_VM"
    waited=0
    until mexec "$CLIENT_VM" sh -c "curl -fsS --connect-timeout 5 --max-time 10 -o /dev/null 'http://${SERVER_IP}:${API_PORT}/health' >/dev/null 2>&1"; do
        [ "$waited" -ge "$HEALTH_TIMEOUT" ] && die "(J) server API never became healthy"
        sleep 2; waited=$((waited+2))
    done

    # J0: a baseline both sides agree on (and a full send through the new pin/mode=fresh path)
    S0="$(snap_now)"
    mark_c="$(loglines "$CLIENT_VM")"
    start_daemon "$CLIENT_VM"
    wait_for_file "$CLIENT_VM" "$CLIENT_ID_FILE" 40 || die "(J) client_id file never created"
    CLIENT_ID="$(msh "$CLIENT_VM" "cat '$CLIENT_ID_FILE'" | tr -d '[:space:]')"
    wait_log_vm "$CLIENT_VM" "$mark_c" "Backup complete: ${SOURCE_DS}@${S0} \(full\)" 150 || fail "(J0) no 'Backup complete: ${SOURCE_DS}@${S0} (full)'"
    stop_daemon "$CLIENT_VM"
    if wait_server_snap "$S0" 30; then pass "(J0) baseline snapshot $S0 reached the server (full send, mode=fresh)"; else fail "(J0) $S0 never reached the server"; fi
    check "(J0) anchor is the baseline snapshot" "$S0" "$(anchor)"
    check "(J0) the in-flight pin is gone after a normal send" "" "$(zprop "$CLIENT_VM" "$SOURCE_DS" "$J_PIN")"

    echo
    step "SCENARIO J1 — kill a send mid-stream, take a newer snapshot, restart: the token is resumed, then the cycle continues to latest"
    if j_interrupt big_j1.bin "$J_FILE_MIB"; then
        T="$J_SNAP"
        check "(J1) the in-flight pin survived the kill: <anchor>,<token's snapshot>" "${S0},${T}" "$(zprop "$CLIENT_VM" "$SOURCE_DS" "$J_PIN")"
        check "(J1) the anchor did not move while the send was incomplete" "$S0" "$(anchor)"
        msh "$CLIENT_VM" "head -c 1000000 /dev/urandom >> \$(zfs get -H -o value mountpoint '$SOURCE_DS')/file1.bin"
        N="$(snap_now)"
        info "token snapshot T=$T, newer snapshot N=$N, base S0=$S0"
        mark_c="$(loglines "$CLIENT_VM")"; mark_s="$(loglines "$SERVER_VM")"
        start_daemon "$CLIENT_VM"
        if wait_log_vm "$CLIENT_VM" "$mark_c" "Resumed ${SOURCE_DS}@${T} \(from ${S0}\) to ${DEST_NAME}" 120; then
            pass "(J1) log: \"Resumed ${SOURCE_DS}@${T} (from ${S0})\""
        else fail "(J1) no 'Resumed ${SOURCE_DS}@${T} (from ${S0})' line"; fi
        if wait_log_vm "$CLIENT_VM" "$mark_c" "Backup complete: ${SOURCE_DS}@${N} \(incremental from ${T}\)" 90; then
            pass "(J1) the same cycle continued with an incremental ${T} -> ${N} (latest)"
        else fail "(J1) no follow-up 'Backup complete: ${SOURCE_DS}@${N} (incremental from ${T})'"; fi
        stop_daemon "$CLIENT_VM"
        show_log "$CLIENT_VM" "$mark_c" 'Resum|Not resuming|Sending (incremental|full)|Backup complete|Transfer failed'
        l_res="$(log_lineno "$CLIENT_VM" "$mark_c" "Resumed ${SOURCE_DS}@${T}")"
        l_fol="$(log_lineno "$CLIENT_VM" "$mark_c" "Sending incremental ${SOURCE_DS}: ${T} -> ${N}")"
        l_fin="$(log_lineno "$CLIENT_VM" "$mark_c" "Backup complete: ${SOURCE_DS}@${N}")"
        if [ -n "$l_res" ] && [ -n "$l_fol" ] && [ -n "$l_fin" ] && [ "$l_res" -lt "$l_fol" ] && [ "$l_fol" -lt "$l_fin" ]; then
            pass "(J1) log order: Resumed (line $l_res) < follow-up send (line $l_fol) < Backup complete (line $l_fin)"
        else fail "(J1) log order wrong: resumed='$l_res' follow-up='$l_fol' complete='$l_fin'"; fi
        if wait_server_snap "$N" 30 && server_snaps | grep -qx "$T"; then pass "(J1) the server has both the resumed $T and the follow-up $N"; else
            fail "(J1) server snapshots: [$(server_snaps | tr '\n' ' ')] (wanted $T and $N)"; fi
        j_synced "J1" "$N"
        check "(J1) the resumed snapshot $T is byte-identical too" "$(guid "$CLIENT_VM" "${SOURCE_DS}@${T}")" "$(guid "$SERVER_VM" "$(server_ds)@${T}")"
        log_since_vm "$SERVER_VM" "$mark_s" | grep -q 'mode=resume' && pass "(J1) server log: the first stream was mode=resume" || fail "(J1) no 'mode=resume' in the server log"
        check "(J1) the server discarded no partial state (resume keeps it)" "0" "$(log_since_vm "$SERVER_VM" "$mark_s" | grep -c 'discarding the partial receive state')"
        check "(J1) no traceback in the client log" "0" "$(log_since_vm "$CLIENT_VM" "$mark_c" | grep -c Traceback)"
    fi

    echo
    step "SCENARIO J2 — stale token: the token's target snapshot is destroyed; the client sends fresh and the server discards the partial state"
    base_j2="$(anchor)"
    if j_interrupt big_j2.bin "$J_FILE_MIB"; then
        T2="$J_SNAP"
        check "(J2) the in-flight pin is <anchor>,<token's snapshot>" "${base_j2},${T2}" "$(zprop "$CLIENT_VM" "$SOURCE_DS" "$J_PIN")"
        msh "$CLIENT_VM" "zfs destroy '${SOURCE_DS}@${T2}'" && info "destroyed the token's target ${SOURCE_DS}@${T2}" || fail "(J2) could not destroy ${SOURCE_DS}@${T2}"
        msh "$CLIENT_VM" "head -c 1000000 /dev/urandom >> \$(zfs get -H -o value mountpoint '$SOURCE_DS')/file1.bin"
        T3="$(snap_now)"
        mark_c="$(loglines "$CLIENT_VM")"; mark_s="$(loglines "$SERVER_VM")"
        start_daemon "$CLIENT_VM"
        if wait_log_vm "$CLIENT_VM" "$mark_c" "WARNING.*Not resuming ${SOURCE_DS}: .*sending fresh" 120; then
            pass "(J2) WARNING \"Not resuming ... sending fresh\""
        else fail "(J2) no 'Not resuming ... sending fresh' WARNING"; fi
        if wait_log_vm "$CLIENT_VM" "$mark_c" "Backup complete: ${SOURCE_DS}@${T3} \(incremental from ${base_j2}\)" 120; then
            pass "(J2) the fresh incremental ${base_j2} -> ${T3} succeeded"
        else fail "(J2) no 'Backup complete: ${SOURCE_DS}@${T3} (incremental from ${base_j2})'"; fi
        stop_daemon "$CLIENT_VM"
        show_log "$CLIENT_VM" "$mark_c" 'Resum|Not resuming|Sending (incremental|full)|Backup complete|Transfer failed'
        show_log "$SERVER_VM" "$mark_s" 'discarding the partial|stream\?mode='
        log_since_vm "$SERVER_VM" "$mark_s" | grep -q 'mode=fresh' && pass "(J2) server log: the stream was mode=fresh" || fail "(J2) no 'mode=fresh' in the server log"
        log_since_vm "$SERVER_VM" "$mark_s" | grep -qE 'WARNING.*discarding the partial receive state of' && pass "(J2) server log: it discarded the partial receive state" || fail "(J2) no 'discarding the partial receive state' WARNING on the server"
        check "(J2) the client did not log a resume" "0" "$(log_since_vm "$CLIENT_VM" "$mark_c" | grep -c 'Resumed ')"
        j_synced "J2" "$T3"
        if server_snaps | grep -qx "$T2"; then fail "(J2) the destroyed snapshot $T2 exists on the server"; else pass "(J2) the destroyed snapshot $T2 never reached the server"; fi
    fi

    echo
    step "SCENARIO J3 — the in-flight pin protects the interrupted pair from the pruner (client pruner 20s, retention 10s:30s)"
    stop_daemon "$CLIENT_VM"
    CLIENT_RETENTION_YAML=$'      "10s": "30s"'
    write_client_config "$SERVER_IP"
    base_j3="$(anchor)"
    if j_interrupt big_j3.bin "$J_FILE_MIB"; then
        X1="$J_SNAP"
        check "(J3) the in-flight pin is <anchor>,<token's snapshot>" "${base_j3},${X1}" "$(zprop "$CLIENT_VM" "$SOURCE_DS" "$J_PIN")"
        # Keep the client from resuming: take the server down (its token stays on disk), then add two newer snapshots so that
        # X1 is neither the newest nor the anchor. Retention keeps nothing older than 30s apart from those, and X2 (not pinned
        # by anything) is the control that shows the pruner really runs.
        stop_daemon "$SERVER_VM"
        X2="$(snap_now)"; X3="$(snap_now)"
        info "pinned X1=$X1, control X2=$X2, newest X3=$X3, anchor=$base_j3"
        start_daemon "$CLIENT_VM"
        x1_lost=0; waited=0
        while client_has_snap "$X2" && [ "$waited" -lt 150 ]; do
            client_has_snap "$X1" || x1_lost=1
            sleep 5; waited=$((waited+5))
        done
        client_has_snap "$X1" || x1_lost=1
        if client_has_snap "$X2"; then fail "(J3) the pruner never took the control snapshot $X2 within 150s (inconclusive)"; else pass "(J3) the pruner ran and took the unpinned control snapshot $X2 after ${waited}s"; fi
        check "(J3) the pinned snapshot $X1 survived (it is neither newest nor anchored, and long expired)" "0" "$x1_lost"
        check "(J3) the pin is still set" "${base_j3},${X1}" "$(zprop "$CLIENT_VM" "$SOURCE_DS" "$J_PIN")"
        client_has_snap "$base_j3" && pass "(J3) the anchor $base_j3 survived" || fail "(J3) the anchor $base_j3 is gone"
        client_has_snap "$X3" && pass "(J3) the newest snapshot $X3 survived" || fail "(J3) the newest snapshot $X3 is gone"
        mark_c="$(loglines "$CLIENT_VM")"
        start_daemon "$SERVER_VM"
        if wait_log_vm "$CLIENT_VM" "$mark_c" "Resumed ${SOURCE_DS}@${X1} \(from ${base_j3}\)" 150; then
            pass "(J3) with the server back, the interrupted $X1 was resumed (its pinned base and target had survived)"
        else fail "(J3) no 'Resumed ${SOURCE_DS}@${X1} (from ${base_j3})' after the server returned"; fi
        if wait_log_vm "$CLIENT_VM" "$mark_c" "Backup complete: ${SOURCE_DS}@${X3} \(incremental from ${X1}\)" 90; then pass "(J3) and continued to the latest snapshot $X3"; else fail "(J3) no follow-up to $X3"; fi
        stop_daemon "$CLIENT_VM"
        j_synced "J3" "$X3"
        start_daemon "$CLIENT_VM"      # the pruner's first pass runs at start
        waited=0; while client_has_snap "$X1" && [ "$waited" -lt 90 ]; do sleep 5; waited=$((waited+5)); done
        stop_daemon "$CLIENT_VM"
        if client_has_snap "$X1"; then fail "(J3) once the pin was released the pruner still has not taken $X1 (90s)"; else pass "(J3) with the pin released, the pruner took $X1 (the pin was the only thing keeping it)"; fi
    fi

    echo
    step "SCENARIO J4 — D7: a second client dataset mapping to the same server dataset is refused (409)"
    stop_daemon "$CLIENT_VM"
    CLIENT2_POOL="zbclient2"; CLIENT2_DS="${CLIENT2_POOL}/data"
    msh "$CLIENT_VM" "
        set -e
        if zpool list '$CLIENT2_POOL' >/dev/null 2>&1; then zpool destroy -f '$CLIENT2_POOL'; fi
        rm -f '${IMG_DIR}/${CLIENT2_POOL}.img'; truncate -s 300M '${IMG_DIR}/${CLIENT2_POOL}.img'
        zpool create '$CLIENT2_POOL' '${IMG_DIR}/${CLIENT2_POOL}.img'
        zfs create '$CLIENT2_DS'
        head -c 1000000 /dev/urandom > \$(zfs get -H -o value mountpoint '$CLIENT2_DS')/f.bin
    " || fail "(J4) could not create the second client pool $CLIENT2_POOL"
    CLIENT_RETENTION_YAML=$'      "1m": "10m"'
    write_client_config "$SERVER_IP" "  - name: \"${CLIENT2_DS}\"
    enabled: true
    frequency: \"30s\"
    recursive: false
    retention:
      \"1m\": \"10m\"
    remote:
      - destination: ${DEST_NAME}
        frequency: \"30s\""
    server_before="$(msh "$SERVER_VM" "zfs list -H -t snapshot -o name,guid -r '$(server_ds)' | sort" | tr '\n' ' ')"
    src_before="$(zprop "$SERVER_VM" "$(server_ds)" org.zfsbackup:source_dataset)"
    check "(J4) precondition: the server's copy records ${SOURCE_DS} as its source" "$SOURCE_DS" "$src_before"
    mark_s="$(loglines "$SERVER_VM")"; mark_c="$(loglines "$CLIENT_VM")"
    start_daemon "$CLIENT_VM"
    wait_log_vm "$SERVER_VM" "$mark_s" "POST /backup/[^ ]*${CLIENT2_DS}/negotiate HTTP/1\.1[^\"]*\" 409" 150
    if log_since_vm "$SERVER_VM" "$mark_s" | grep -qE "POST /backup/[^ ]*${CLIENT2_DS}/negotiate HTTP/1\.1[^\"]*\" 409"; then
        pass "(J4) server log: negotiate for ${CLIENT2_DS} was refused with 409"
    else fail "(J4) no 409 for the negotiate of ${CLIENT2_DS} in the server log within 150s"; fi
    show_log "$SERVER_VM" "$mark_s" "negotiate HTTP/1\.1[^\"]*\" 409"
    show_log "$CLIENT_VM" "$mark_c" "${CLIENT2_DS}.*(Negotiation|refus|409|Transfer failed|Backup complete)"
    check "(J4) the refused dataset has no anchor (nothing was transferred)" "" "$(zprop "$CLIENT_VM" "$CLIENT2_DS" "org.zfsbackup:anchor.${DEST_NAME}")"
    check "(J4) no stream for ${CLIENT2_DS} reached the server" "0" "$(log_since_vm "$SERVER_VM" "$mark_s" | grep -cE "${CLIENT2_DS}/stream")"
    check "(J4) the server's copy still records ${SOURCE_DS} as its source" "$SOURCE_DS" "$(zprop "$SERVER_VM" "$(server_ds)" org.zfsbackup:source_dataset)"
    check "(J4) the first dataset's server snapshots (names and guids) are untouched" "$server_before" \
        "$(msh "$SERVER_VM" "zfs list -H -t snapshot -o name,guid -r '$(server_ds)' | sort" | tr '\n' ' ')"
    code="$(msh "$CLIENT_VM" "curl -s -o /tmp/zfsb-409.json -w '%{http_code}' -X POST -H 'Content-Type: application/json' --data '{\"snapshots\": []}' 'http://${SERVER_IP}:${API_PORT}/backup/${CLIENT_ID}/${CLIENT2_DS}/negotiate'" | tr -d '[:space:]')"
    check "(J4) a direct negotiate for ${CLIENT2_DS} answers HTTP 409" "409" "$code"
    msh "$CLIENT_VM" "grep -q 'refusing to mix in' /tmp/zfsb-409.json" && pass "(J4) the 409 body names the conflict: $(msh "$CLIENT_VM" "cut -c1-200 /tmp/zfsb-409.json" | tr -d '\n')" || fail "(J4) the 409 body does not say 'refusing to mix in'"
    msh "$CLIENT_VM" "rm -f /tmp/zfsb-409.json" || true
    daemon_running "$CLIENT_VM" && pass "(J4) the client daemon is alive (the refusal skips the dataset, not the daemon)" || fail "(J4) the client daemon died"
    stop_daemon "$CLIENT_VM"
    destroy_pool "$CLIENT_VM" "$CLIENT2_POOL"
fi

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
    step "Leaving daemons + pools running (--keep). Inspect with:"
    echo "    multipass exec $SERVER_VM -- sudo zfs list -t snapshot -r $TARGET_DS"
    echo "    multipass exec $CLIENT_VM -- sudo tail -f $LOG"
else
    step "Tearing down"
    stop_daemon "$SERVER_VM"; stop_daemon "$CLIENT_VM"
    remove_config "$SERVER_VM"; remove_config "$CLIENT_VM"
    if [ "$KEEP_POOLS" -eq 0 ]; then
        destroy_pool "$SERVER_VM" "$SERVER_POOL"
        destroy_pool "$CLIENT_VM" "$CLIENT_POOL"
        destroy_pool "$CLIENT_VM" "zbclient2"      # scenario J4's extra pool (already gone unless J4 aborted)
        info "pools destroyed"
    else
        info "pools left in place (--keep-pools)"
    fi
    if [ "$PURGE" -eq 1 ]; then
        info "deleting VMs (--purge; these two only, never a global purge)"
        multipass delete --purge "$SERVER_VM" && multipass delete --purge "$CLIENT_VM"
    else
        info "VMs left running for reuse (use --purge to delete them)"
    fi
fi

[ "$FAIL_COUNT" -eq 0 ] && exit 0 || exit 1
