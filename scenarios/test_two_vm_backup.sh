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
# Usage:
#   ./scenarios/test_two_vm_backup.sh            # run all scenarios; reuse VMs; recreate pools
#   ./scenarios/test_two_vm_backup.sh --keep     # leave daemons + pools running for inspection
#   ./scenarios/test_two_vm_backup.sh --keep-pools
#   ./scenarios/test_two_vm_backup.sh --purge    # delete the VMs entirely at the end
#
# Requires: multipass on the host (macOS/Linux). No sudo needed on the host.
#
# NOTE ON READINESS: this harness deliberately works around two known defects so it can
# run at all — it (1) installs `requests` explicitly (it is imported by the client but
# missing from pyproject dependencies) and (2) binds the server API to 0.0.0.0 despite
# there being NO authentication/TLS on that API. Do not treat a green run here as a
# security sign-off. Snapshot resume is NOT tested because it is unfinished server-side.

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
CFG="/etc/zfsbackup/config.yaml"
CLIENT_ID_FILE="/var/lib/zfsbackup/client_id"
LOG="/var/log/zfsbackup.log"
DISK_SIZE="512m"

VM_CPUS="${VM_CPUS:-1}"
VM_MEM="${VM_MEM:-1G}"
VM_DISK="${VM_DISK:-5G}"

# Timeouts (seconds)
HEALTH_TIMEOUT=60
SNAP_TIMEOUT=120         # ~4 snapshot periods

# Flags
PURGE=0; KEEP_POOLS=0; KEEP=0
for arg in "$@"; do
    case "$arg" in
        --purge)      PURGE=1 ;;
        --keep-pools) KEEP_POOLS=1 ;;
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
        dpkg -s python3-venv >/dev/null 2>&1 || need="$need python3-venv"
        dpkg -s python3-pip  >/dev/null 2>&1 || need="$need python3-pip"
        if [ -n "$need" ]; then apt-get update -qq && apt-get install -y -qq $need; fi
    ' || die "package install failed on $vm"

    # Ship the current repo (committed HEAD) into the VM and extract. Write the archive to
    # a host temp file first, then transfer the file (avoids proxying host stdin through exec).
    step "Copying source into $vm:$SRC_DIR"
    local tar; tar="$(mktemp)"
    ( cd "$(dirname "$0")" && git archive --format=tar HEAD ) > "$tar" || die "git archive failed"
    multipass transfer "$tar" "$vm:/tmp/zfsbackup-src.tar" || die "source transfer failed for $vm"
    rm -f "$tar"
    msh "$vm" "rm -rf '$SRC_DIR' && mkdir -p '$SRC_DIR' && tar -xf /tmp/zfsbackup-src.tar -C '$SRC_DIR' && rm -f /tmp/zfsbackup-src.tar"

    # venv with runtime deps. We run from source via PYTHONPATH (pyproject only packages
    # libzfseasy, not zfsbackup), and install `requests` explicitly — see readiness note.
    msh "$vm" "
        set -e
        test -x '$PY' || python3 -m venv '$VENV'
        '$PIP' install -q --upgrade pip
        '$PIP' install -q pyyaml 'flask>=3' flask-sock websocket-client requests
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
        img=/tmp/${pool}.img
        rm -f \$img
        truncate -s '$DISK_SIZE' \$img
        zpool create '$pool' \$img
    " || die "pool creation failed on $vm ($pool)"
}

destroy_pool() {
    local vm="$1" pool="$2"
    # Suppress output in the guest (never redirect exec's host stdout to /dev/null).
    msh "$vm" "{ zpool destroy -f '$pool'; rm -f /tmp/${pool}.img; } >/dev/null 2>&1; true" || true
}

# ─────────────────────────────── Daemon control ─────────────────────────────
# The daemon is launched as a transient systemd unit: systemd-run returns immediately
# (so the exec call completes) and reparents the daemon to systemd. Its "Running as
# unit" message goes to stderr; stdout is empty, so no host redirect is needed.
start_daemon() {
    local vm="$1"
    step "Starting daemon on $vm"
    msh "$vm" "
        systemctl reset-failed zfsbackup 2>/dev/null || true
        systemd-run --unit=zfsbackup --collect \
            -p StandardOutput=append:'$LOG' -p StandardError=append:'$LOG' \
            env PYTHONPATH='$SRC_DIR' '$PY' -m zfsbackup.daemon -c '$CFG' -v
    " || die "could not start daemon on $vm"
}

stop_daemon() {
    local vm="$1"
    # All suppression happens in the guest; host stdout is never sent to /dev/null.
    msh "$vm" "{ systemctl stop zfsbackup; systemctl reset-failed zfsbackup; pkill -f 'zfsbackup.daemon'; } >/dev/null 2>&1; true" || true
}

daemon_running() { msh "$1" "pgrep -f 'zfsbackup.daemon' >/dev/null" 2>/dev/null; }

# ─────────────────────────────── Config writers ─────────────────────────────
# NOTE: config is delivered via `multipass transfer` (reliable) rather than piping a
# heredoc into `multipass exec ... tee` — the latter hangs/spins on host stdin.
put_config() { # put_config <vm> ; reads config text on stdin, installs it to $CFG
    local vm="$1" tmp
    tmp="$(mktemp)"
    cat > "$tmp"
    multipass transfer "$tmp" "$vm:/tmp/zfsbackup-config.yaml" || die "config transfer failed for $vm"
    msh "$vm" "install -D -m600 /tmp/zfsbackup-config.yaml '$CFG' && rm -f /tmp/zfsbackup-config.yaml" \
        || die "config install failed on $vm"
    rm -f "$tmp"
}

write_server_config() {
    step "Writing server config on $SERVER_VM"
    put_config "$SERVER_VM" <<EOF
# zfsbackup SERVER config (auto-generated by test_two_vm_backup.sh)
snapshot_prefix: "autosnap"
check_interval: "1m"
prune_interval: "5m"
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

write_client_config() {
    local server_ip="$1"
    step "Writing client config on $CLIENT_VM (server=${server_ip}:${API_PORT})"
    put_config "$CLIENT_VM" <<EOF
# zfsbackup CLIENT config (auto-generated by test_two_vm_backup.sh)
snapshot_prefix: "autosnap"
check_interval: "15s"
prune_interval: "1m"
client_id_file: "${CLIENT_ID_FILE}"
destinations:
  ${DEST_NAME}:
    url: "http://${server_ip}:${API_PORT}"
datasets:
  - name: "${SOURCE_DS}"
    enabled: true
    frequency: "30s"
    recursive: false
    retention:
      "1m": "10m"
      "5m": "1h"
    remote:
      - destination: ${DEST_NAME}
        frequency: "30s"
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

# ── Phase 2: fresh pools ──
stop_daemon "$SERVER_VM"; stop_daemon "$CLIENT_VM"
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
SERVER_IP="$(vm_ip "$SERVER_VM")"; [ -n "$SERVER_IP" ] || die "could not determine $SERVER_VM IP"
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

echo
echo "${YELLOW}NOTE:${NC} snapshot-resume (interrupted transfer) is NOT tested — the server never"
echo "      reads the client's ?from= token (dead code) and the feature is unfinished."

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
    if [ "$KEEP_POOLS" -eq 0 ]; then
        destroy_pool "$SERVER_VM" "$SERVER_POOL"
        destroy_pool "$CLIENT_VM" "$CLIENT_POOL"
        info "pools destroyed"
    else
        info "pools left in place (--keep-pools)"
    fi
    if [ "$PURGE" -eq 1 ]; then
        info "deleting VMs (--purge)"
        multipass delete "$SERVER_VM" "$CLIENT_VM" && multipass purge
    else
        info "VMs left running for reuse (use --purge to delete them)"
    fi
fi

[ "$FAIL_COUNT" -eq 0 ] && exit 0 || exit 1
