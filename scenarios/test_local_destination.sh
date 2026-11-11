#!/usr/bin/env bash
#
# test_local_destination.sh -- the LOCAL destination (`url: zfs:<dataset>`) on real ZFS, in one Multipass VM
# (plan docs/local_destination_plan.md, item 3.4).
#
# One disposable VM, `zfsb-local` (Ubuntu 24.04, OpenZFS 2.2 by default), two file-backed pools:
#   zblsrc  the SOURCE pool:  zblsrc/plain (unencrypted) and zblsrc/enc (passphrase-encrypted, key loaded -> raw: auto)
#   zbbak   the BACKUP pool:  zbbak/zfsbackup is the target (made by the operator, `zfs create -o canmount=off`)
# (scenario 7 adds the tree zblsrc/rec/... to the source pool)
# The daemon is the real `python -m zfsbackup.daemon` under a transient systemd unit, started from the shipped tree
# with a SQLite config DB that `zfsbackup-config import` filled; the destination is
#     destinations: { local1: { url: "zfs:zbbak/zfsbackup" } }
# and the copies must appear at <target>/<client id>/<pool>/<path>, e.g. zbbak/zfsbackup/<id>/zblsrc/plain. There is no
# HTTP, TLS or enrollment: the daemon pipes `zfs send` into `zfs receive` itself. Everything is judged by ZFS state
# (datasets, snapshots, guids, properties, anchors), not by log wording; log lines are printed as evidence only.
#
# Schedule (short): snapshot frequency 30s, remote push 20s, check_interval 10s, prune_interval 20s.
#
# Scenarios (numbers follow the plan's item 3.4 list):
#   1  install; both pools; `import` of a config with the two datasets and the local destination prints a GENERATED
#      client id (the host had none); `list destinations` shows kind `local` (text and --json); `enroll` refuses a local
#      destination and creates no client identity; `--test-config`; the daemon starts
#   2  the FIRST full send, then a CHAIN of >= 3 incremental steps (the daemon is stopped, four snapshots with data in
#      between are taken for each dataset, the daemon is started: the backlog goes out in order). Copies exist at the exact
#      path with the forced properties (readonly=on, canmount=noauto, mountpoint=none, volmode=none, setuid/exec/devices/
#      share* off; the owned org.zfsbackup:client_id / source_dataset / config set LOCALLY and nothing received), the
#      containers <target>/<id> and <target>/<id>/<pool> are owned, the encrypted copy is RAW (encryption set, keystatus
#      unavailable, encryptionroot = the copy, not mounted), the copy's (snapshot, guid) list is exactly the source's from
#      the first sent snapshot on, and the source's org.zfsbackup:anchor.local1 advances and names a snapshot on the copy
#   3  `kill -9` the REMOTE WORKER while a 1 GiB incremental is being sent (a guest-side catcher polls every 50 ms for the
#      `zfs send ...@<snapshot>` and kills the worker that owns it; the unit is CPU-capped so the stream is long): the
#      old zfs send/receive children vanish (no strays), the copy keeps a receive_resume_token, the supervisor replaces
#      the worker, the copy converges (token cleared, guid of the big snapshot equal) and a read-only clone of it hashes
#      like the source file
#   4  `zpool export` of the backup pool while the daemon runs: the supervisor stays up, snapshots go on being taken on
#      the source, the anchors do not move, `check` grades the anchor lag WARN/CRIT; `zpool import` -> the destination
#      recovers and the whole backlog goes out as a chain
#   5  PRUNING the copies: (a) retention 1m:2m for the destination -> the old copy snapshots go (the stamped
#      org.zfsbackup:config carries the new rule), the source is untouched, a hand-made CHILD of the copy keeps its
#      same-named snapshots (never recursive), planted sibling trees <target>/zfsb-other/... and <target>/<id>x/... (other
#      client ids, one of them sharing our id as a prefix, each with a config that WOULD prune it) are byte-identical
#      before and after; (b) retention 1s:5s with the snapshot worker quiet (frequency 1d) -> every copy shrinks to
#      exactly ONE snapshot, the newest, and stays there for 3 prune cycles; the next snapshot still arrives incrementally
#   6  `zfsbackup-config check` grades both dataset x local1 anchors OK once caught up (JSON; exit 0); `list destinations`
#      again
#   7  a RECURSIVE tree (sender rework, Track A; plan docs/sender_rework_plan.md, item A6b): zblsrc/rec (`recursive: true`, an
#      unencrypted parent) with an ENCRYPTED child (its own encryption root: raw), a grandchild inheriting that encryption, an
#      unencrypted child and a zvol, with the destination retention 1s:120s stamped on every copy. (a) every descendant gets its
#      own copy at <target>/<id>/<pool>/<path> (parent first), plain or raw per child, with the forced properties and a copy
#      config naming the CHILD (recursive: false), and its own LOCAL anchor (the inherited-anchor regression: a child never
#      reads its parent's); (b) a backlog of three recursive snapshots goes out as a chain for every child; (c) children
#      CREATED mid-run (`late`, `gone`) are discovered and sent (a full, then incrementals); `gone` is then DESTROYED on the
#      source: its copy stays and the host's pruner thins it to its newest snapshot by the retention stamped on it; (d)
#      `check --json` grades one anchor per child x destination (OK once caught up; `gone` is no longer listed); (e) a BACKUP COPY
#      planted below the recursive dataset (it carries org.zfsbackup:client_id / source_dataset) is never snapshotted (the
#      worker takes ONE atomic `zfs snapshot` over the source datasets instead of `-r`: one createtxg), never sent, never
#      pruned (a snapshot NAME it holds is kept across the whole tree, a name it lacks is pruned from the source children),
#      and `check` shows a `copies` WARN naming it
#   8  clean exit: the daemon is stopped (no zfsbackup process, no zfs send/receive left), no Traceback in its log, the
#      pools are destroyed, the VM is deleted (unless a keep flag is given)
#
# Usage (from the repository root):
#   ./scenarios/test_local_destination.sh                # run everything, then delete the daemon, pools and the VM
#   ./scenarios/test_local_destination.sh --keep         # leave the daemon, pools and VM in place (to inspect)
#   ./scenarios/test_local_destination.sh --keep-pools   # remove the daemon and config, keep the pools and the VM
#   ./scenarios/test_local_destination.sh --keep-vm      # clean the VM up (daemon, config, pools) but do not delete it
#   ./scenarios/test_local_destination.sh --only-rec     # scenarios 1, 7, 8 only (setup, the recursive tree, clean exit): ~12 minutes
#   ./scenarios/test_local_destination.sh --purge        # accepted: deleting the VM (multipass delete --purge: only this
#                                                        # VM) is already the default
#   SRC_REF=<commit-ish> ...    # the tree shipped to the VM (default HEAD); SRC_REF=WORKTREE ships the working tree as it
#                               # is, untracked files included (a commit object built from a throwaway index: the
#                               # repository's index, stash list and refs are untouched)
#
# Env: LOCAL_VM (default zfsb-local; must start with `zfsb-`), LOCAL_IMAGE (default 24.04), VM_CPUS / VM_MEM / VM_DISK
#      (default 2 / 2G / 10G), IMG_DIR (pool images, default /var/tmp: /tmp is a tmpfs), DISK_SIZE (default 4g, sparse),
#      L_BIG_MIB (the big incremental of scenario 3, default 1024), L_CPU_QUOTA (CPUQuota of the unit during that send,
#      default 30%; empty = none), L_CATCH_DELAY (seconds the stream runs before the kill, default 1),
#      L_RESUME_TIMEOUT (seconds to converge after the kill, default 400), LOCAL_LOG_DIR (host copy of the daemon log and
#      the final ZFS listing; default a mktemp directory, printed at the end).
#
# Exit status: 0 when every assertion passed, 1 otherwise or when the run could not complete, 2 on usage errors.
# Needs multipass and git on the host. Never touches ZFS on the host and never any VM but the one named above. About
# 30 minutes (the VM install, ~6 minutes of waiting for prune ages, the 130 s outage of scenario 4 and the recursive tree of
# scenario 7 included).

set -uo pipefail

# ─────────────────────────────── Configuration ──────────────────────────────
LOCAL_VM="${LOCAL_VM:-zfsb-local}"
LOCAL_IMAGE="${LOCAL_IMAGE:-24.04}"
VM_CPUS="${VM_CPUS:-2}"
VM_MEM="${VM_MEM:-2G}"
VM_DISK="${VM_DISK:-10G}"
IMG_DIR="${IMG_DIR:-/var/tmp}"
DISK_SIZE="${DISK_SIZE:-4g}"
SRC_REF="${SRC_REF:-HEAD}"
L_BIG_MIB="${L_BIG_MIB:-1024}"
L_CPU_QUOTA="${L_CPU_QUOTA-30%}"
L_CATCH_DELAY="${L_CATCH_DELAY:-1}"
L_RESUME_TIMEOUT="${L_RESUME_TIMEOUT:-400}"

SRC_POOL="zblsrc"
BAK_POOL="zbbak"
SRC_PLAIN="${SRC_POOL}/plain"
SRC_ENC="${SRC_POOL}/enc"
R_TOP="${SRC_POOL}/rec"                                # scenario 7: the recursive tree
R_ENC="${R_TOP}/enc"; R_INNER="${R_ENC}/inner"; R_PLAIN="${R_TOP}/plain"; R_VOL="${R_TOP}/vol"
R_LATE="${R_TOP}/late"; R_GONE="${R_TOP}/gone"; R_REPL="${R_TOP}/replica"
R_RET="120"                                            # seconds: the destination retention (1s:<R_RET>s) stamped on the copies
TARGET="${BAK_POOL}/zfsbackup"
DEST="local1"
BIGFILE="big.bin"

SRC_DIR="/opt/zfsbackup-src"
SRC_PP="${SRC_DIR}:${SRC_DIR}/libs/libzfseasy"       # trees from before the packaging split keep libzfseasy at the root
VENV="/opt/zfsbvenv"
PY="${VENV}/bin/python"
PIP="${VENV}/bin/pip"
CFG="/etc/zfsbackup/config.yaml"
DB="/var/lib/zfsbackup/config.db"
TLS_DIR="${DB}.tls"
LOG="/var/log/zfsbackup-local.log"
GDIR="/opt/zfsb-local"                                # helper scripts in the guest
JGET_PY="${GDIR}/jget.py"
KEYFILE="/root/zfsb-local.key"
UNIT="zfsbackup"

# Flags
KEEP=0; KEEP_POOLS=0; KEEP_VM=0; ONLY_REC=0
for arg in "$@"; do
    case "$arg" in
        --keep)       KEEP=1; KEEP_POOLS=1; KEEP_VM=1 ;;
        --keep-pools) KEEP_POOLS=1; KEEP_VM=1 ;;
        --keep-vm)    KEEP_VM=1 ;;
        --purge)      ;;     # the default
        --only-rec)   ONLY_REC=1 ;;  # scenarios 1, 7 and 8 only (the recursive tree), for iterating on 7
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
declare -a FINDINGS=()      # things the scenario noticed that are not assertions (wording, timing); listed after the results

info()  { echo "${BLUE}==>${NC} $*"; }
step()  { echo "${BOLD}${BLUE}::${NC} ${BOLD}$*${NC}"; }
warn()  { echo "${YELLOW}WARN:${NC} $*" >&2; }
die()   { echo "${RED}FATAL:${NC} $*" >&2; exit 1; }
finding() { FINDINGS+=("$1"); echo "  ${YELLOW}! FINDING${NC} $1"; }
pass()  { PASS_COUNT=$((PASS_COUNT+1)); RESULTS+=("${GREEN}PASS${NC}  $1"); echo "  ${GREEN}✓ PASS${NC} $1"; }
fail()  { FAIL_COUNT=$((FAIL_COUNT+1)); RESULTS+=("${RED}FAIL${NC}  $1"); echo "  ${RED}✗ FAIL${NC} $1"; }
check() { # check "<description>" <expected> <actual>
    if [ "$2" = "$3" ]; then pass "$1"; else fail "$1 (expected='$2' actual='$3')"; fi
}
check_has() { # check_has "<description>" <ERE> <text>
    if printf '%s\n' "$3" | grep -E -- "$2" >/dev/null; then pass "$1"; else fail "$1 (no match for /$2/ in: $(printf '%s' "$3" | head -c 300 | tr '\n' '|'))"; fi
}
check_lacks() { # check_lacks "<description>" <ERE> <text>
    if printf '%s\n' "$3" | grep -E -- "$2" >/dev/null; then fail "$1 (found /$2/: $(printf '%s\n' "$3" | grep -E -- "$2" | head -n2 | tr '\n' '|'))"; else pass "$1"; fi
}
gq() { grep "$@" >/dev/null; }

case "$LOCAL_VM" in
    zfsb-*) ;;
    *) die "LOCAL_VM must start with 'zfsb-' (got '$LOCAL_VM'): this script never touches any other VM" ;;
esac
command -v multipass >/dev/null 2>&1 || die "multipass not found on host"
command -v git >/dev/null 2>&1 || die "git not found on host"

# ─────────────────────────────── Multipass helpers ──────────────────────────
# `multipass exec` busy-spins FOREVER when the guest command writes to stdout and the HOST sends that stdout to /dev/null:
# never do that, discard inside the guest. A guest command that exits 4 hangs it too, so the command runs in an inner shell
# and status 4 becomes 104. Config and scripts go in with `multipass transfer`, never through piped stdin.
REMAP='bash -c "$1"; rc=$?; [ "$rc" -eq 4 ] && rc=104; exit "$rc"'
msh()  { multipass exec "$LOCAL_VM" -- sudo bash -c "$REMAP" _ "$*"; }
vm_exists()  { multipass list 2>/dev/null | awk -v n="$LOCAL_VM" 'NR>1 && $1==n {f=1} END{exit !f}'; }
vm_running() { multipass list 2>/dev/null | awk -v n="$LOCAL_VM" 'NR>1 && $1==n && tolower($2)=="running" {f=1} END{exit !f}'; }

VM_TOUCHED=0
TORN=0
WORK="$(mktemp -d "${TMPDIR:-/tmp}/zfsb-local.XXXXXX")" || die "mktemp failed"
LOG_DIR="${LOCAL_LOG_DIR:-$(mktemp -d "${TMPDIR:-/tmp}/zfsb-local-logs.XXXXXX")}"
mkdir -p "$LOG_DIR" || die "cannot create $LOG_DIR"

ensure_vm() {
    if ! vm_exists; then
        step "Launching VM $LOCAL_VM (Ubuntu $LOCAL_IMAGE, $VM_CPUS cpus, $VM_MEM, $VM_DISK)"
        VM_TOUCHED=1
        multipass launch "$LOCAL_IMAGE" --name "$LOCAL_VM" --cpus "$VM_CPUS" --memory "$VM_MEM" --disk "$VM_DISK" \
            || die "failed to launch $LOCAL_VM"
    elif ! vm_running; then
        VM_TOUCHED=1
        step "Starting existing VM $LOCAL_VM"
        multipass start "$LOCAL_VM" || die "failed to start $LOCAL_VM"
    else
        VM_TOUCHED=1
        info "VM $LOCAL_VM already running"
    fi
}

put_file() { # put_file <host-file> <guest-path> <mode>
    local base="/tmp/zblocal-xfer.$$.$RANDOM"
    multipass transfer "$1" "$LOCAL_VM:$base" || die "transfer of $1 failed"
    msh "install -D -m '$3' '$base' '$2' && rm -f '$base'" || die "install of $2 failed"
}

# ─────────────────────────────── Guest helper scripts ───────────────────────
write_guest_scripts() {
    cat > "$WORK/jget.py" <<'EOF_JGET'
import json, sys
d = json.load(sys.stdin)
v = eval(sys.argv[1], {"d": d})
print("" if v is None else v)
EOF_JGET

    # catch.sh <snapshot> <delay> <timeout> <copy dataset> <supervisor pid>
    # Waits for the `zfs send ... @<snapshot>` of the remote worker, lets it run <delay> seconds, then kill -9s the WORKER
    # that owns it (never the zfs children), and reports: the old send/receive pids still alive after up to 30 s (strays),
    # and whether the copy holds a receive_resume_token. Polls every 50 ms: the window is the stream's length.
    cat > "$WORK/catch.sh" <<'EOF_CATCH'
#!/bin/bash
SNAP="$1"; DELAY="$2"; TMO="$3"; COPY="$4"; MAIN="$5"
alive() { # a live, non-zombie process
    [ -r "/proc/$1/stat" ] || return 1
    [ "$(sed 's/.*) //' "/proc/$1/stat" | cut -c1)" != "Z" ]
}
end=$(( $(date +%s) + TMO ))
send=""
while [ "$(date +%s)" -lt "$end" ]; do
    send="$(pgrep -f "[z]fs send .*@${SNAP}( |\$)" | head -n1)"
    [ -n "$send" ] && break
    sleep 0.05
done
[ -n "$send" ] || { echo "NOSEND"; exit 0; }
worker="$(ps -o ppid= -p "$send" | tr -d ' ')"
children="$(pgrep -P "$MAIN" | wc -l | tr -d ' ')"
sleep "$DELAY"
alive "$send" || { echo "TOOLATE (the send ended before the kill)"; exit 0; }
if [ -z "$worker" ] || ! tr '\0' ' ' < "/proc/$worker/cmdline" 2>/dev/null | grep zfsbackup >/dev/null; then
    echo "NOWORKER ppid=${worker:-?}"; exit 0
fi
recv="$(pgrep -P "$worker" -f '[z]fs (receive|recv) ' | head -n1)"
kill -9 "$worker"
echo "KILLED worker=$worker send=$send recv=${recv:-none} children=$children"
left=""
for i in $(seq 1 150); do
    left=""
    for p in $send $recv; do alive "$p" && left="$left $p"; done
    [ -z "$left" ] && break
    sleep 0.2
done
echo "STRAYS=${left:-none}"
tok="-"
for i in $(seq 1 100); do
    tok="$(zfs get -H -o value receive_resume_token "$COPY" 2>/dev/null)"
    [ -n "$tok" ] && [ "$tok" != "-" ] && break
    sleep 0.2
done
if [ -n "$tok" ] && [ "$tok" != "-" ]; then echo "TOKEN=yes"; else echo "TOKEN=no"; fi
EOF_CATCH

    # pruned_ok.sh <copy dataset> <min age s> <names file>: of the snapshot names in the file, those older than <min age>
    # (by the UTC stamp in the name) must be gone from the copy: prints EXPECT=<how many are that old> LEFT=<still there>
    cat > "$WORK/pruned_ok.sh" <<'EOF_PRUNED'
#!/bin/bash
copy="$1"; minage="$2"; file="$3"
now="$(date -u +%s)"; e=0; l=0
while read -r n; do
    [ -n "$n" ] || continue
    t="${n#*_}"
    ts="$(date -u -d "${t:0:4}-${t:4:2}-${t:6:2} ${t:8:2}:${t:10:2}:${t:12:2}" +%s 2>/dev/null)" || continue
    [ $((now - ts)) -gt "$minage" ] || continue
    e=$((e + 1))
    zfs list -H -t snapshot -o name "$copy@$n" >/dev/null 2>&1 && l=$((l + 1))
done < "$file"
echo "EXPECT=$e LEFT=$l"
EOF_PRUNED

    # plant.sh <target> <other client id> <source dataset> <config value file>: a complete foreign copy
    # <target>/<id>/<pool>/<path> with that client id, source_dataset and config as LOCAL properties and four snapshots
    cat > "$WORK/plant.sh" <<'EOF_PLANT'
#!/bin/bash
set -e
target="$1"; oid="$2"; src="$3"; cfg="$(cat "$4")"
pool="${src%%/*}"
zfs create -u -o canmount=noauto -o "org.zfsbackup:client_id=$oid" "$target/$oid"
zfs create -u -o canmount=noauto -o "org.zfsbackup:client_id=$oid" "$target/$oid/$pool"
zfs create -u -o canmount=noauto -o mountpoint=none -o readonly=on -o "org.zfsbackup:client_id=$oid" \
    -o "org.zfsbackup:source_dataset=$src" -o "org.zfsbackup:config=$cfg" "$target/$oid/$src"
for s in 20200101000001 20200101000002 20200101000003 "$(date -u +%Y%m%d%H%M%S)"; do
    zfs snapshot "$target/$oid/$src@autosnap_$s"
done
EOF_PLANT

    # sibcfg.py <base64 config>: the same config with a retention that would prune everything but the newest
    cat > "$WORK/sibcfg.py" <<'EOF_SIB'
import base64, json, sys
d = json.loads(base64.b64decode(sys.argv[1]))
d["retention_rules"] = [{"age": 1.0, "keep_for": 5.0}]
print(base64.b64encode(json.dumps(d, separators=(",", ":")).encode()).decode())
EOF_SIB
}

put_guest_scripts() {
    write_guest_scripts
    msh "mkdir -p '$GDIR'"
    put_file "$WORK/jget.py"      "$JGET_PY"             644
    put_file "$WORK/catch.sh"     "$GDIR/catch.sh"       755
    put_file "$WORK/pruned_ok.sh" "$GDIR/pruned_ok.sh"   755
    put_file "$WORK/plant.sh"     "$GDIR/plant.sh"       755
    put_file "$WORK/sibcfg.py"    "$GDIR/sibcfg.py"      644
}

# ─────────────────────────────── Install ────────────────────────────────────
ship_tree() {
    local ref="$SRC_REF" top tarfile
    top="$(git -C "$(dirname "$0")" rev-parse --show-toplevel)" || die "not in a git repo"
    if [ "$ref" = "WORKTREE" ]; then
        local idx tree
        idx="$(mktemp -u "${TMPDIR:-/tmp}/zfsb-wt-index.XXXXXX")"
        tree="$(cd "$top" && GIT_INDEX_FILE="$idx" git read-tree HEAD && GIT_INDEX_FILE="$idx" git add -A && GIT_INDEX_FILE="$idx" git write-tree)" \
            || { rm -f "$idx"; die "could not snapshot the working tree"; }
        rm -f "$idx"
        ref="$(git -C "$top" commit-tree "$tree" -p HEAD -m 'local-destination scenario: working-tree snapshot')" \
            || die "could not snapshot the working tree"
        info "SRC_REF=WORKTREE -> $ref (an unreferenced commit of the working tree)"
    fi
    step "Shipping the tree ($(git -C "$top" rev-parse --short "$ref" 2>/dev/null || echo "$ref")) to $LOCAL_VM:$SRC_DIR"
    tarfile="$WORK/src.tar"
    git -C "$top" archive --format=tar "$ref" > "$tarfile" || die "git archive of '$ref' failed"
    multipass transfer "$tarfile" "$LOCAL_VM:/tmp/zfsbackup-src.tar" || die "source transfer failed"
    msh "rm -rf '$SRC_DIR' && mkdir -p '$SRC_DIR' && tar -m -xf /tmp/zfsbackup-src.tar -C '$SRC_DIR' && rm -f /tmp/zfsbackup-src.tar" \
        || die "could not extract the tree"
    rm -f "$tarfile"
}

install_stack() {
    step "Provisioning $LOCAL_VM (ZFS, Python venv, zfsbackup dependencies)"
    msh '
        set -e
        export DEBIAN_FRONTEND=noninteractive
        cloud-init status --wait >/dev/null 2>&1 || true
        need=""
        command -v zfs >/dev/null 2>&1 || need="$need zfsutils-linux"
        dpkg -s python3-venv >/dev/null 2>&1 || need="$need python3-venv"
        dpkg -s python3-pip  >/dev/null 2>&1 || need="$need python3-pip"
        if [ -n "$need" ]; then apt-get update -qq && apt-get install -y -qq $need; fi
        modprobe zfs
        zfs version
    ' || die "package install failed"
    ship_tree
    msh "
        set -e
        test -x '$PY' || python3 -m venv '$VENV'
        '$PIP' install -q --upgrade pip
        '$PIP' install -q pyyaml 'flask>=3' flask-sock websocket-client requests 'cryptography>=43' \
            'sqlalchemy>=2.0' 'alembic>=1.13' 'click>=8.1'
        mkdir -p /etc/zfsbackup /var/lib/zfsbackup
    " || die "pip install failed"
    put_guest_scripts
}

# ─────────────────────────────── Pools ──────────────────────────────────────
destroy_pools() {
    msh "{ zpool destroy -f '$SRC_POOL'; zpool destroy -f '$BAK_POOL'; zpool import -d '$IMG_DIR/$BAK_POOL.img' '$BAK_POOL' && zpool destroy -f '$BAK_POOL'; rm -f '$IMG_DIR/$SRC_POOL.img' '$IMG_DIR/$BAK_POOL.img' '$KEYFILE'; } >/dev/null 2>&1; true" || true
}

create_pools() {
    step "Creating the pools $SRC_POOL (source) and $BAK_POOL (backup) on file-backed images in $IMG_DIR"
    destroy_pools
    msh "
        set -e
        for p in '$SRC_POOL' '$BAK_POOL'; do
            truncate -s '$DISK_SIZE' '$IMG_DIR/'\$p.img
            zpool create -f \$p '$IMG_DIR/'\$p.img
        done
        printf '%s' 'correct-horse-battery-staple' > '$KEYFILE'; chmod 600 '$KEYFILE'
        zfs create '$SRC_PLAIN'
        zfs create -o encryption=on -o keyformat=passphrase -o keylocation=file://$KEYFILE '$SRC_ENC'
        zfs create -o canmount=off '$TARGET'
        for d in '$SRC_PLAIN' '$SRC_ENC'; do
            dd if=/dev/urandom of=\$(zfs get -H -o value mountpoint \$d)/seed.bin bs=1M count=8 status=none
        done
        zpool list -H -o name,size
    " || die "pool creation failed"
}

# ─────────────────────────────── Daemon and CLI ─────────────────────────────
# The daemon runs as a transient systemd unit (systemd-run returns at once and reparents it). Extra systemd-run properties
# may be passed (the CPU cap of scenario 3).
start_daemon() {
    step "Starting the daemon (unit $UNIT${1:+, $*})"
    msh "
        systemctl reset-failed $UNIT >/dev/null 2>&1 || true
        systemd-run --unit=$UNIT --collect $* \
            -p StandardOutput=append:'$LOG' -p StandardError=append:'$LOG' \
            env PYTHONPATH='$SRC_PP' '$PY' -m zfsbackup.daemon -c '$DB' -v
    " || die "could not start the daemon"
}
stop_daemon() {
    msh "{ systemctl stop $UNIT; systemctl reset-failed $UNIT; pkill -f '[z]fsbackup.daemon'; } >/dev/null 2>&1; true" || true
}
daemon_active() { [ "$(msh "systemctl is-active $UNIT 2>&1 | head -n1" | tr -d '[:space:]')" = "active" ]; }
mainpid() { msh "systemctl show -p MainPID --value $UNIT 2>/dev/null" | tr -d '[:space:]'; }
daemon_procs() { msh "pgrep -fc '[z]fsbackup.daemon' || true" | tr -d '[:space:]'; }
xfer_procs() { msh "pgrep -fc '[z]fs (send|receive|recv) ' || true" | tr -d '[:space:]'; }
no_xfer() { [ "$(xfer_procs)" = "0" ]; }

cli() { msh "env PYTHONPATH='$SRC_PP' '$PY' -m zfsbackup.cli -c '$DB' $* 2>&1"; }
cli_to_file() { local f="$1"; shift; msh "env PYTHONPATH='$SRC_PP' '$PY' -m zfsbackup.cli -c '$DB' $* >'$f' 2>/dev/null"; }
jfile() { msh "'$PY' '$JGET_PY' '$2' < '$1'"; }      # jfile <guest json file> '<python expr over d>'
ck_run()  { msh "env PYTHONPATH='$SRC_PP' '$PY' -m zfsbackup.cli -c '$DB' check $* >/tmp/zfsb-ck.out 2>/tmp/zfsb-ck.err"; CK_RC=$?; }
ck_expr() { jfile /tmp/zfsb-ck.out "$1"; }
ck_anchor_states() { ck_expr '",".join(sorted(e["subject"] + ":" + e.get("peer", "") + "=" + e["state"] for e in d["entries"] if e["source"] == "anchor"))' | tr -d '[:space:]'; }

loglines() { msh "wc -l < '$LOG'" | tr -d '[:space:]'; }
log_since() { msh "tail -n +$(( $1 + 1 )) '$LOG'"; }

# config: write_config <snapshot frequency> [<destination retention items, e.g. '"1m": "2m"'>]
# The destination's retention overrides the dataset's for the COPIES (the config stamped on them); empty = inherit.
write_config() {
    local freq="$1" ret="${2:-}" rl=""
    [ -n "$ret" ] && rl=$'        retention:\n          '"$ret"
    {
        cat <<EOF
# zfsbackup config of test_local_destination.sh
snapshot_prefix: "autosnap"
check_interval: "10s"
prune_interval: "20s"
destinations:
  ${DEST}:
    url: "zfs:${TARGET}"
datasets:
EOF
        local d
        for d in "$SRC_PLAIN" "$SRC_ENC"; do
            cat <<EOF
  - name: "${d}"
    enabled: true
    frequency: "${freq}"
    recursive: false
    retention:
      "1s": "1d"
    remote:
      - destination: ${DEST}
        frequency: "20s"
${rl}
EOF
        done
    } > "$WORK/config.yaml"
    put_file "$WORK/config.yaml" "$CFG" 600
}
import_config() { cli "import --no-backup '$CFG'"; }

# ─────────────────────────────── ZFS helpers ────────────────────────────────
CID=""
copy_of() { printf '%s' "${TARGET}/${CID}/$1"; }               # <target>/<client id>/<pool>/<path>
guid_of() { msh "zfs get -H -o value guid '$1' 2>/dev/null" | tr -d '[:space:]'; }
zv()      { msh "zfs get -H -o value $1 '$2' 2>/dev/null" | tr -d '[:space:]'; }                 # zv <prop> <dataset>
zvs()     { msh "zfs get -H -o value,source $1 '$2' 2>/dev/null" | tr '\t' ' ' | tr -s ' ' | sed 's/ *$//'; }   # "value source"
exists()  { msh "zfs list -H -o name '$1' >/dev/null 2>&1 && echo yes || echo no" | tr -d '[:space:]'; }
snaps()   { msh "zfs list -H -d 1 -t snapshot -s createtxg -o name '$1' 2>/dev/null" | sed 's/.*@//' | grep -v '^$' || true; }
snapfp()  { msh "zfs list -H -d 1 -t snapshot -s createtxg -o name,guid '$1' 2>/dev/null" | sed 's/^[^@]*@//' | tr '\t' ':' | grep -v '^$' || true; }
count()   { grep -c . || true; }
anchor_of() { local a; a="$(zv "org.zfsbackup:anchor.${DEST}" "$1")"; [ "$a" = "-" ] && a=""; printf '%s' "$a"; }
idx_in()  { snaps "$1" | grep -nx "$2" | cut -d: -f1 | head -n1; }      # idx_in <dataset> <snapshot>: 1-based, oldest first
wait_for() { # wait_for <timeout> <predicate> [args...]
    local timeout="$1" waited=0; shift
    until "$@"; do
        [ "$waited" -ge "$timeout" ] && return 1
        sleep 3; waited=$((waited+3))
    done
}

# a snapshot named like the daemon's, taken now (the 2 s sleep keeps names unique); prints the short name
snap_now() {
    local n="" i
    for i in 1 2 3; do
        sleep 2
        n="$(msh "n=autosnap_\$(date -u +%Y%m%d%H%M%S); zfs snapshot '$1'@\$n && echo \$n" | tr -d '[:space:]')"
        [ -n "$n" ] && break
    done
    printf '%s' "$n"
}
write_mib() { msh "dd if=/dev/urandom of=\$(zfs get -H -o value mountpoint '$1')/$2 bs=1M count=$3 status=none"; }   # write_mib <ds> <file> <MiB>

copy_has() { # copy_has <source ds> <snapshot>: the copy has it, with the source's guid
    local cg sg; cg="$(guid_of "$(copy_of "$1")@$2")"; sg="$(guid_of "$1@$2")"
    [ -n "$cg" ] && [ "$cg" = "$sg" ]
}
copies_have() { copy_has "$SRC_PLAIN" "$1" && copy_has "$SRC_ENC" "$2"; }
# the copy's (snapshot:guid) list equals the source's from the copy's first snapshot on, same length: nothing missing,
# nothing extra, same order (a chain sends EVERY managed snapshot; pruning has not started)
prefix_ok() { # prefix_ok <source ds>
    local c s first n
    c="$(snapfp "$(copy_of "$1")")"
    [ -n "$c" ] || return 1
    first="$(printf '%s\n' "$c" | head -n1 | cut -d: -f1)"
    n="$(printf '%s\n' "$c" | wc -l | tr -d '[:space:]')"
    s="$(snapfp "$1" | sed -n "/^${first}:/,\$p" | head -n "$n")"
    [ "$s" = "$c" ]
}
both_prefix_ok() { prefix_ok "$SRC_PLAIN" && prefix_ok "$SRC_ENC"; }
# every (snapshot:guid) of the copy is the source's (pruning may have thinned the copy, never altered it)
subset_ok() { # subset_ok <source ds>
    local extra; extra="$(comm -23 <(snapfp "$(copy_of "$1")" | sort) <(snapfp "$1" | sort) 2>/dev/null)"
    [ -z "$extra" ] && [ -n "$(snaps "$(copy_of "$1")")" ]
}
copy_token() { local t; t="$(zv receive_resume_token "$(copy_of "$1")")"; printf '%s' "${t:--}"; }
token_clear() { [ "$(copy_token "$1")" = "-" ]; }
# scenario 7: the dataset's LOCAL anchor.<dest> (empty when unset or only inherited/received: a child never reads its parent's)
local_anchor_of() { local v; v="$(zvs "org.zfsbackup:anchor.${DEST}" "$1")"; case "$v" in *" local") printf '%s' "${v% local}" ;; esac; }
rec_all=("$R_TOP" "$R_ENC" "$R_INNER" "$R_PLAIN" "$R_VOL")      # the datasets of the tree from the start (late/gone join later)
# a recursive snapshot of the whole tree, taken now (the 2 s sleep keeps names unique); prints the short name
snap_rec() {
    local n="" i
    for i in 1 2 3; do
        sleep 2
        n="$(msh "n=autosnap_\$(date -u +%Y%m%d%H%M%S); zfs snapshot -r '$R_TOP'@\$n && echo \$n" | tr -d '[:space:]')"
        [ -n "$n" ] && break
    done
    printf '%s' "$n"
}
rec_first_done() { local d; for d in "${rec_all[@]}"; do [ -n "$(snaps "$(copy_of "$d")")" ] && [ -n "$(local_anchor_of "$d")" ] || return 1; done; }
rec_copies_have() { local d; for d in "$@"; do copy_has "$d" "$REC_N" || return 1; done; }     # every dataset in "$@" has $REC_N with the source's guid
rec_copy_cfg() { msh "zfs get -H -o value org.zfsbackup:config '$(copy_of "$1")' | base64 -d 2>/dev/null | '$PY' '$JGET_PY' '$2'" | tr -d '[:space:]'; }   # rec_copy_cfg <source ds> '<expr over d>'

# the forced and owned properties of one copy
check_copy_props() { # check_copy_props <label> <source ds>
    local lbl="$1" src="$2" cp p want
    cp="$(copy_of "$src")"
    check "($lbl) the copy $cp exists" "yes" "$(exists "$cp")"
    for p in readonly=on canmount=noauto mountpoint=none volmode=none sharenfs=off sharesmb=off setuid=off exec=off devices=off; do
        want="${p#*=}"
        check "($lbl) ${p%%=*} is $want, set LOCALLY on the copy" "$want local" "$(zvs "${p%%=*}" "$cp")"
    done
    check "($lbl) the copy is not mounted" "no" "$(zv mounted "$cp")"
    check "($lbl) org.zfsbackup:client_id is $CID (local)" "$CID local" "$(zvs org.zfsbackup:client_id "$cp")"
    check "($lbl) org.zfsbackup:source_dataset is $src (local)" "$src local" "$(zvs org.zfsbackup:source_dataset "$cp")"
    check "($lbl) org.zfsbackup:config is set locally" "local" "$(zvs org.zfsbackup:config "$cp" | cut -d' ' -f2)"
    check "($lbl) the config names $src" "$src" \
        "$(msh "zfs get -H -o value org.zfsbackup:config '$cp' | base64 -d 2>/dev/null | '$PY' '$JGET_PY' 'd[\"name\"]'" | tr -d '[:space:]')"
    check "($lbl) no org.zfsbackup:* property was received (the stream carries none)" "0" \
        "$(msh "zfs get -H -s received -o property all '$cp' 2>/dev/null | grep -c '^org.zfsbackup:' || true" | tr -d '[:space:]')"
}

# ─────────────────────────────── Teardown ───────────────────────────────────
collect_artifacts() {
    msh "{ echo '--- zpool list'; zpool list; echo '--- zfs list -t all (target)'; zfs list -r -t all -o name,guid,readonly,canmount,mountpoint,org.zfsbackup:client_id '$TARGET'; echo '--- zfs list -t snapshot (source)'; zfs list -r -t snapshot -o name,guid '$SRC_POOL'; echo '--- anchors'; zfs get -r -H -o name,value org.zfsbackup:anchor.$DEST '$SRC_POOL'; } >/tmp/zfsb-local-final.txt 2>&1; cp '$LOG' /tmp/zfsb-local-daemon.log; chmod 644 /tmp/zfsb-local-final.txt /tmp/zfsb-local-daemon.log; true" || true
    multipass transfer "$LOCAL_VM:/tmp/zfsb-local-daemon.log" "$LOG_DIR/daemon.log" >/dev/null 2>&1 || true
    multipass transfer "$LOCAL_VM:/tmp/zfsb-local-final.txt" "$LOG_DIR/final-zfs.txt" >/dev/null 2>&1 || true
}

teardown() {
    [ "$TORN" -eq 1 ] && return 0
    TORN=1
    [ "$VM_TOUCHED" -eq 1 ] || { rm -rf "$WORK"; return 0; }
    vm_exists || { rm -rf "$WORK"; return 0; }
    echo
    if vm_running; then collect_artifacts; fi
    if [ "$KEEP" -eq 1 ]; then
        step "Leaving the daemon, pools and VM in place (--keep). Inspect with:"
        echo "    multipass exec $LOCAL_VM -- sudo zfs list -r -t all $TARGET"
        echo "    multipass exec $LOCAL_VM -- sudo tail -f $LOG"
        rm -rf "$WORK"; return 0
    fi
    step "Tearing down"
    if vm_running; then
        stop_daemon
        msh "{ rm -f '$CFG' '$DB' '$DB-wal' '$DB-shm' '$DB.daemon.pid' '$DB.writer.lock' '$DB.workers.lock'; rm -rf '$TLS_DIR' '$DB.status' '$GDIR' /mnt/zfsb-verify; } >/dev/null 2>&1; true" || true
        if [ "$KEEP_POOLS" -eq 0 ]; then destroy_pools; info "pools destroyed"; else info "pools left in place"; fi
    fi
    if [ "$KEEP_VM" -eq 0 ]; then
        info "deleting VM $LOCAL_VM (multipass delete --purge: only this VM)"
        multipass delete --purge "$LOCAL_VM" || warn "could not delete $LOCAL_VM"
    else
        info "VM $LOCAL_VM left in place (delete it with: multipass delete --purge $LOCAL_VM)"
    fi
    rm -rf "$WORK"
}
trap teardown EXIT
trap 'exit 130' INT TERM

# ═══════════════════════════════ Scenarios ══════════════════════════════════
echo "${BOLD}=================================================================${NC}"
echo "${BOLD} zfsbackup LOCAL destination (zfs:<dataset>) on real ZFS${NC}"
echo "${BOLD}=================================================================${NC}"
ensure_vm
install_stack
# a VM reused from an earlier run: no daemon, no config
stop_daemon
msh "{ rm -f '$CFG' '$DB' '$DB-wal' '$DB-shm' '$DB.daemon.pid' '$DB.writer.lock' '$DB.workers.lock' '$LOG'; rm -rf '$TLS_DIR' '$DB.status'; } >/dev/null 2>&1; true" || true
create_pools

# ───────────────────────────────── 1 ────────────────────────────────────────
echo
step "1 — import (a generated client id), list, enroll refusal, --test-config, daemon start"
write_config "30s"
out="$(import_config)"; rc=$?
printf '%s\n' "$out" | sed 's/^/    CLI: /'
check "(1) import exits 0" "0" "$rc"
CID="$(printf '%s\n' "$out" | sed -n "s/^generated this host's client id: //p" | tr -d '[:space:]')"
re_id='^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$'
if [[ "$CID" =~ $re_id ]]; then pass "(1) import printed a generated client id: $CID"; else fail "(1) import printed no valid generated client id (got '$CID')"; fi
cli_to_file /tmp/zfsb-id.json identity show --json
check "(1) identity show --json agrees: the id is stored" "$CID" "$(jfile /tmp/zfsb-id.json 'd["client_id"]' | tr -d '[:space:]')"
out="$(cli list destinations)"
printf '%s\n' "$out" | sed 's/^/    CLI: /'
check_has "(1) list destinations: $DEST is kind local with url zfs:$TARGET" "${DEST} +local +zfs:${TARGET}" "$out"
cli_to_file /tmp/zfsb-ld.json list destinations --json
check "(1) list destinations --json: kind local, target_dataset $TARGET" "local $TARGET" \
    "$(jfile /tmp/zfsb-ld.json '"%s %s" % (d[0]["kind"], d[0]["target_dataset"])' | tr -d '\r')"
out="$(cli enroll "$DEST" --token-file /dev/null)"; rc=$?
printf '%s\n' "$out" | sed 's/^/    CLI: /'
check "(1) enroll of a local destination is refused (exit non-zero)" "refused" "$([ "$rc" -ne 0 ] && echo refused || echo "exit $rc")"
check "(1) ... and it created no client identity" "no" "$(msh "test -e '$TLS_DIR/client.crt' && echo yes || echo no" | tr -d '[:space:]')"
out="$(msh "env PYTHONPATH='$SRC_PP' '$PY' -m zfsbackup.daemon --test-config -c '$DB' 2>&1")"; rc=$?
printf '%s\n' "$out" | tail -n 5 | sed 's/^/    daemon: /'
check "(1) daemon --test-config exits 0" "0" "$rc"

COPY_PLAIN="$(copy_of "$SRC_PLAIN")"; COPY_ENC="$(copy_of "$SRC_ENC")"
start_daemon
wait_for 60 daemon_active || fail "(1) the daemon unit is not active"
check "(1) the daemon is running" "yes" "$(daemon_active && echo yes || echo no)"

if [ "$ONLY_REC" -eq 0 ]; then    # (--only-rec skips scenarios 2-6)
# ───────────────────────────────── 2 ────────────────────────────────────────
echo
step "2 — first full send, then a chain of incremental steps; the copies' path, properties and guids"
copies_have_any() { [ -n "$(snaps "$COPY_PLAIN")" ] && [ -n "$(snaps "$COPY_ENC")" ] && [ -n "$(anchor_of "$SRC_PLAIN")" ] && [ -n "$(anchor_of "$SRC_ENC")" ]; }
if wait_for 240 copies_have_any; then pass "(2) both copies received their first snapshot"; else fail "(2) a copy has no snapshot after 240 s (plain: [$(snaps "$COPY_PLAIN" | tr '\n' ' ')] enc: [$(snaps "$COPY_ENC" | tr '\n' ' ')])"; fi
check "(2) the copy of the plain dataset is exactly at <target>/<client id>/<pool>/<path>" "yes" "$(exists "$COPY_PLAIN")"
check "(2) the copy of the encrypted dataset is exactly at <target>/<client id>/<pool>/<path>" "yes" "$(exists "$COPY_ENC")"
for c in "${TARGET}/${CID}" "${TARGET}/${CID}/${SRC_POOL}"; do
    check "(2) the container $c is owned (client_id local)" "$CID local" "$(zvs org.zfsbackup:client_id "$c")"
    check "(2) ... and canmount=noauto" "noauto" "$(zv canmount "$c")"
done
check_copy_props "2 plain" "$SRC_PLAIN"
check_copy_props "2 enc" "$SRC_ENC"
check "(2) the plain copy is not encrypted" "off" "$(zv encryption "$COPY_PLAIN")"
check "(2) the encrypted copy is RAW: encryption is set" "yes" "$([ "$(zv encryption "$COPY_ENC")" != "off" ] && echo yes || echo no)"
check "(2) ... keystatus is unavailable (no key was ever loaded)" "unavailable" "$(zv keystatus "$COPY_ENC")"
check "(2) ... and it is its own encryption root" "$COPY_ENC" "$(zv encryptionroot "$COPY_ENC")"
for src in "$SRC_PLAIN" "$SRC_ENC"; do
    first="$(snaps "$(copy_of "$src")" | head -n1)"
    check "(2) the first send put a source snapshot on the copy of $src with its guid ($first)" "yes" "$(copy_has "$src" "$first" && echo yes || echo no)"
done
A1_PLAIN="$(anchor_of "$SRC_PLAIN")"; A1_ENC="$(anchor_of "$SRC_ENC")"
info "anchors after the first send: plain=$A1_PLAIN enc=$A1_ENC"
check "(2) the plain anchor $A1_PLAIN is on the copy with the source's guid" "yes" "$(copy_has "$SRC_PLAIN" "$A1_PLAIN" && echo yes || echo no)"
check "(2) the encrypted anchor $A1_ENC is on the copy with the source's guid" "yes" "$(copy_has "$SRC_ENC" "$A1_ENC" && echo yes || echo no)"
N1_PLAIN="$(snaps "$COPY_PLAIN" | count)"; N1_ENC="$(snaps "$COPY_ENC" | count)"

# the backlog: with the daemon stopped, four snapshots per dataset with new data between them
stop_daemon
sleep 3
declare -a CH_PLAIN=() CH_ENC=()
for i in 1 2 3 4; do
    write_mib "$SRC_PLAIN" "chain$i.bin" 2; CH_PLAIN+=("$(snap_now "$SRC_PLAIN")")
    write_mib "$SRC_ENC" "chain$i.bin" 2;   CH_ENC+=("$(snap_now "$SRC_ENC")")
done
info "backlog: plain ${CH_PLAIN[*]}  enc ${CH_ENC[*]}"
MARK2="$(loglines)"
start_daemon
if wait_for 240 copies_have "${CH_PLAIN[3]}" "${CH_ENC[3]}"; then pass "(2) the backlog arrived: ${CH_PLAIN[3]} / ${CH_ENC[3]} are on the copies with the source's guids"
else fail "(2) the newest backlog snapshots did not arrive within 240 s"; fi
for i in 0 1 2; do
    check "(2) chain step ${CH_PLAIN[$i]} (plain) is on the copy with the source's guid" "yes" "$(copy_has "$SRC_PLAIN" "${CH_PLAIN[$i]}" && echo yes || echo no)"
    check "(2) chain step ${CH_ENC[$i]} (enc) is on the copy with the source's guid" "yes" "$(copy_has "$SRC_ENC" "${CH_ENC[$i]}" && echo yes || echo no)"
done
wait_for 60 both_prefix_ok
check "(2) plain: the copy's (snapshot, guid) list is the source's from the first sent snapshot on (every step, in order)" "yes" "$(prefix_ok "$SRC_PLAIN" && echo yes || echo no)"
check "(2) enc: the copy's (snapshot, guid) list is the source's from the first sent snapshot on (every step, in order)" "yes" "$(prefix_ok "$SRC_ENC" && echo yes || echo no)"
N2_PLAIN="$(snaps "$COPY_PLAIN" | count)"; N2_ENC="$(snaps "$COPY_ENC" | count)"
check "(2) plain: at least 4 incremental steps were added ($N1_PLAIN -> $N2_PLAIN)" "yes" "$([ "$N2_PLAIN" -ge $((N1_PLAIN + 4)) ] && echo yes || echo no)"
check "(2) enc: at least 4 incremental steps were added ($N1_ENC -> $N2_ENC)" "yes" "$([ "$N2_ENC" -ge $((N1_ENC + 4)) ] && echo yes || echo no)"
A2_PLAIN="$(anchor_of "$SRC_PLAIN")"; A2_ENC="$(anchor_of "$SRC_ENC")"
check "(2) the plain anchor advanced past the first one ($A1_PLAIN -> $A2_PLAIN, by position in the source's order)" "yes" \
    "$([ "$(idx_in "$SRC_PLAIN" "$A2_PLAIN")" -ge "$(idx_in "$SRC_PLAIN" "${CH_PLAIN[3]}")" ] 2>/dev/null && echo yes || echo no)"
check "(2) the encrypted anchor advanced past the first one ($A1_ENC -> $A2_ENC)" "yes" \
    "$([ "$(idx_in "$SRC_ENC" "$A2_ENC")" -ge "$(idx_in "$SRC_ENC" "${CH_ENC[3]}")" ] 2>/dev/null && echo yes || echo no)"
check "(2) the newest plain anchor snapshot is on the copy with the source's guid" "yes" "$(copy_has "$SRC_PLAIN" "$A2_PLAIN" && echo yes || echo no)"
check "(2) the newest encrypted anchor snapshot is on the copy with the source's guid" "yes" "$(copy_has "$SRC_ENC" "$A2_ENC" && echo yes || echo no)"
check "(2) the copies are still read-only, noauto, mountpoint none, encrypted copy still locked" "on noauto none unavailable" \
    "$(zv readonly "$COPY_PLAIN") $(zv canmount "$COPY_PLAIN") $(zv mountpoint "$COPY_PLAIN") $(zv keystatus "$COPY_ENC")"
steps_logged="$(log_since "$MARK2" | grep -cE "Sending incremental ${SRC_PLAIN}:" || true)"
info "evidence: the log shows $steps_logged 'Sending incremental ${SRC_PLAIN}: ...' lines for the backlog"
[ "${steps_logged:-0}" -ge 3 ] || finding "(2) fewer than 3 'Sending incremental ${SRC_PLAIN}:' log lines for a 4-step backlog (the local manager may word it differently)"

# ───────────────────────────────── 3 ────────────────────────────────────────
echo
step "3 — kill -9 the remote worker mid-stream (a ${L_BIG_MIB} MiB incremental): no strays, resume or fresh, convergence"
stop_daemon
sleep 3
write_mib "$SRC_PLAIN" "$BIGFILE" "$L_BIG_MIB"
BIG="$(snap_now "$SRC_PLAIN")"
info "big snapshot: $SRC_PLAIN@$BIG"
MARK3="$(loglines)"
if [ -n "$L_CPU_QUOTA" ]; then start_daemon "-p CPUQuota=$L_CPU_QUOTA"; else start_daemon; fi
wait_for 60 daemon_active
MAIN3="$(mainpid)"
catch_out="$(msh "bash '$GDIR/catch.sh' '$BIG' '$L_CATCH_DELAY' 240 '$COPY_PLAIN' '$MAIN3'")"
printf '%s\n' "$catch_out" | sed 's/^/    catcher: /'
msh "systemctl set-property --runtime $UNIT CPUQuota= >/dev/null 2>&1; true" || true
KILLED_WORKER=""; CHILDREN3=0
case "$catch_out" in
    *KILLED*)
        pass "(3) the remote worker was killed (kill -9) while 'zfs send ...@$BIG' was running"
        KILLED_WORKER="$(printf '%s\n' "$catch_out" | sed -n 's/.*KILLED worker=\([0-9]*\).*/\1/p' | head -n1)"
        CHILDREN3="$(printf '%s\n' "$catch_out" | sed -n 's/.*children=\([0-9]*\).*/\1/p' | head -n1)"
        check "(3) the old zfs send/receive children are gone within 30 s (no strays)" "none" "$(printf '%s\n' "$catch_out" | sed -n 's/^STRAYS=//p')"
        check "(3) the partial receive was kept: the copy holds a receive_resume_token" "TOKEN=yes" "$(printf '%s\n' "$catch_out" | grep '^TOKEN=')"
        ;;
    *TOOLATE*) fail "(3) the send of $BIG ended before it could be interrupted (raise L_BIG_MIB or lower L_CPU_QUOTA)" ;;
    *NOSEND*)  fail "(3) no 'zfs send ...@$BIG' was seen within 240 s" ;;
    *)         fail "(3) the worker could not be identified or killed: $catch_out" ;;
esac
if [ -n "$KILLED_WORKER" ]; then
    workers_back() { [ "$(msh "pgrep -P '$MAIN3' | wc -l" | tr -d '[:space:]')" -ge "$CHILDREN3" ] && [ "$(msh "kill -0 $KILLED_WORKER 2>/dev/null && echo alive || echo dead" | tr -d '[:space:]')" = "dead" ]; }
    if wait_for 90 workers_back; then pass "(3) the supervisor ($MAIN3) is still the main pid and replaced the killed worker (children back to $CHILDREN3)"; else fail "(3) the supervisor did not restore its workers within 90 s"; fi
    check "(3) the daemon stayed up" "$MAIN3" "$(mainpid)"
fi
big_converged() { copy_has "$SRC_PLAIN" "$BIG" && token_clear "$SRC_PLAIN"; }
if wait_for "$L_RESUME_TIMEOUT" big_converged; then pass "(3) the copy converged: $BIG is on it with the source's guid and the resume token is cleared"
else fail "(3) the copy did not converge within ${L_RESUME_TIMEOUT}s (token=$(copy_token "$SRC_PLAIN"), has=$(copy_has "$SRC_PLAIN" "$BIG" && echo yes || echo no))"; fi
wait_for 120 both_prefix_ok
check "(3) plain: the copy's (snapshot, guid) list is still exactly the source's from the first sent snapshot on" "yes" "$(prefix_ok "$SRC_PLAIN" && echo yes || echo no)"
check "(3) enc: ... and so is the encrypted copy's" "yes" "$(prefix_ok "$SRC_ENC" && echo yes || echo no)"
big_used="$(msh "zfs get -Hp -o value referenced '$COPY_PLAIN@$BIG' 2>/dev/null" | tr -d '[:space:]')"
check "(3) the copy's $BIG holds the data (referenced >= 90% of ${L_BIG_MIB} MiB)" "yes" "$([ "${big_used:-0}" -ge $((L_BIG_MIB * 1024 * 1024 * 9 / 10)) ] && echo yes || echo no)"
hashes="$(msh "zfs clone -o canmount=on -o readonly=on -o mountpoint=/mnt/zfsb-verify '$COPY_PLAIN@$BIG' '$BAK_POOL/verify' >/dev/null 2>&1 && a=\$(sha256sum /mnt/zfsb-verify/$BIGFILE | cut -d' ' -f1); zfs destroy -f '$BAK_POOL/verify' >/dev/null 2>&1; b=\$(sha256sum \$(zfs get -H -o value mountpoint '$SRC_PLAIN')/$BIGFILE | cut -d' ' -f1); echo \"\${a:-none} \$b\"" | tr -d '\r')"
read -r h_copy h_src <<< "$hashes"
check "(3) a read-only clone of the copy's $BIG hashes like the source file (the resumed data is intact)" "equal" "$([ -n "${h_copy:-}" ] && [ "${h_copy:-}" != "none" ] && [ "${h_copy:-}" = "${h_src:-x}" ] && echo equal || echo "differ: $hashes")"
check "(3) the clone is gone again (it would pin the snapshot)" "no" "$(exists "$BAK_POOL/verify")"
no_xfer_wait() { no_xfer; }
wait_for 60 no_xfer_wait
check "(3) no zfs send/receive process is left" "0" "$(xfer_procs)"
echo "    evidence (the wording of the local manager is not asserted):"
log_since "$MARK3" | grep -E "Resumed|Not resuming|resum|receive -A|fresh" | cut -c1-200 | head -n 8 | sed 's/^/      log: /'
if ! log_since "$MARK3" | grep -iE "resum|fresh|receive -A" >/dev/null; then
    finding "(3) the log shows no resume / fresh-send line after the kill (the copy converged anyway)"
fi

# ───────────────────────────────── 4 ────────────────────────────────────────
echo
step "4 — zpool export of the backup pool under the running daemon: skipped, daemon up; import: recovery"
idle() { no_xfer; }
wait_for 90 idle
MAIN4="$(mainpid)"
A4_PLAIN="$(anchor_of "$SRC_PLAIN")"; A4_ENC="$(anchor_of "$SRC_ENC")"
MARK4="$(loglines)"
export_ok=0
for i in 1 2 3 4 5 6; do
    if msh "zpool export '$BAK_POOL' 2>&1"; then export_ok=1; break; fi
    sleep 3
done
[ "$export_ok" -eq 1 ] || { msh "zpool export -f '$BAK_POOL' 2>&1" && export_ok=1; }
check "(4) zpool export $BAK_POOL succeeded" "1" "$export_ok"
check "(4) the pool is gone from the host" "absent" "$(msh "zpool list -H -o name '$BAK_POOL' >/dev/null 2>&1 && echo present || echo absent" | tr -d '[:space:]')"
X_PLAIN="$(snap_now "$SRC_PLAIN")"; X_ENC="$(snap_now "$SRC_ENC")"
SC0="$(snaps "$SRC_PLAIN" | count)"
sleep 70
check "(4) after 70 s the daemon is still up (same supervisor pid)" "$MAIN4 yes" "$(mainpid) $(daemon_active && echo yes || echo no)"
SC1="$(snaps "$SRC_PLAIN" | count)"
check "(4) snapshots went on being taken on the source while the target was away ($SC0 -> $SC1)" "yes" "$([ "$SC1" -ge $((SC0 + 2)) ] && echo yes || echo no)"
sleep 65
check "(4) after 135 s the daemon is still up" "$MAIN4 yes" "$(mainpid) $(daemon_active && echo yes || echo no)"
check "(4) the anchors did not move (plain)" "$A4_PLAIN" "$(anchor_of "$SRC_PLAIN")"
check "(4) the anchors did not move (enc)" "$A4_ENC" "$(anchor_of "$SRC_ENC")"
check "(4) no zfs send/receive is running against the missing pool" "0" "$(xfer_procs)"
ck_run --json
st="$(ck_anchor_states)"
info "check while the target is away: $st"
case "$st" in
    *=OK*) fail "(4) check still grades an anchor OK 135 s after the last send: $st" ;;
    *:${DEST}=*) pass "(4) check grades the stale anchors WARN/CRIT: $st" ;;
    *) fail "(4) check reports no anchor entry for $DEST: '$st'" ;;
esac
check_lacks "(4) no Traceback in the daemon log during the outage" "Traceback" "$(log_since "$MARK4")"
echo "    evidence (not asserted):"
log_since "$MARK4" | grep -E " (WARNING|ERROR) " | cut -c1-200 | sort | uniq -c | sort -rn | head -n 5 | sed 's/^/      log: /'
msh "zpool import -d '$IMG_DIR/$BAK_POOL.img' '$BAK_POOL' 2>&1" || fail "(4) zpool import $BAK_POOL failed"
check "(4) the pool is back" "present" "$(msh "zpool list -H -o name '$BAK_POOL' >/dev/null 2>&1 && echo present || echo absent" | tr -d '[:space:]')"
recovered() { copies_have "$X_PLAIN" "$X_ENC"; }
if wait_for 300 recovered; then pass "(4) the destination recovered: $X_PLAIN / $X_ENC (taken during the outage) reached the copies"; else fail "(4) the destination did not recover within 300 s"; fi
wait_for 120 both_prefix_ok
check "(4) plain: the whole backlog went out in order (the copy's list equals the source's from the first sent snapshot)" "yes" "$(prefix_ok "$SRC_PLAIN" && echo yes || echo no)"
check "(4) enc: ... and so did the encrypted one" "yes" "$(prefix_ok "$SRC_ENC" && echo yes || echo no)"
A4B_PLAIN="$(anchor_of "$SRC_PLAIN")"; A4B_ENC="$(anchor_of "$SRC_ENC")"
check "(4) the plain anchor moved on after the import ($A4_PLAIN -> $A4B_PLAIN)" "yes" "$([ "$A4B_PLAIN" != "$A4_PLAIN" ] && [ "$(idx_in "$SRC_PLAIN" "$A4B_PLAIN")" -ge "$(idx_in "$SRC_PLAIN" "$X_PLAIN")" ] 2>/dev/null && echo yes || echo no)"
check "(4) the encrypted anchor moved on after the import ($A4_ENC -> $A4B_ENC)" "yes" "$([ "$A4B_ENC" != "$A4_ENC" ] && [ "$(idx_in "$SRC_ENC" "$A4B_ENC")" -ge "$(idx_in "$SRC_ENC" "$X_ENC")" ] 2>/dev/null && echo yes || echo no)"
check "(4) the token is clear on both copies" "- -" "$(copy_token "$SRC_PLAIN") $(copy_token "$SRC_ENC")"
check "(4) the encrypted copy is still locked" "unavailable" "$(zv keystatus "$COPY_ENC")"
check "(4) the daemon never restarted (same supervisor pid)" "$MAIN4" "$(mainpid)"
check_copy_props "4 plain" "$SRC_PLAIN"

# ───────────────────────────────── 5 ────────────────────────────────────────
echo
step "5 — pruning the copies: only <target>/<own id>, never recursive, never the newest, siblings untouched"
SRC_BEFORE_PLAIN="$(snaps "$SRC_PLAIN")"
P0_PLAIN="$(snaps "$COPY_PLAIN")"; P0_ENC="$(snaps "$COPY_ENC")"
KID="${COPY_PLAIN}/kid"
msh "zfs create -o canmount=off '$KID'" || fail "(5) could not create the hand-made child $KID"
for n in $(printf '%s\n' "$P0_PLAIN" | head -n3); do msh "zfs snapshot '$KID@$n'" || fail "(5) could not snapshot $KID@$n"; done
KID_BEFORE="$(snaps "$KID" | tr '\n' ' ')"
info "hand-made child $KID with the same-named snapshots: $KID_BEFORE"
# the sibling trees: other client ids whose config WOULD prune everything but the newest (retention 1s:5s)
msh "python3 '$GDIR/sibcfg.py' \"\$(zfs get -H -o value org.zfsbackup:config '$COPY_PLAIN')\" > '$GDIR/sibcfg.txt'" || fail "(5) could not build the sibling config"
OTHER="zfsb-other"; PREFIXED="${CID}x"
for oid in "$OTHER" "$PREFIXED"; do
    msh "bash '$GDIR/plant.sh' '$TARGET' '$oid' '$SRC_PLAIN' '$GDIR/sibcfg.txt'" || fail "(5) could not plant the sibling tree of $oid"
done
sib_fp() { msh "{ zfs list -H -t all -r -o name,guid '$TARGET/$1'; zfs get -H -o name,property,value,source org.zfsbackup:client_id,org.zfsbackup:config,readonly,canmount '$TARGET/$1/$SRC_PLAIN'; } 2>&1 | sort | md5sum | cut -d' ' -f1" | tr -d '[:space:]'; }
SIB1_BEFORE="$(sib_fp "$OTHER")"; SIB2_BEFORE="$(sib_fp "$PREFIXED")"
check "(5) the planted sibling trees exist ($OTHER, $PREFIXED), 4 snapshots each" "4 4" "$(snaps "$TARGET/$OTHER/$SRC_PLAIN" | count) $(snaps "$TARGET/$PREFIXED/$SRC_PLAIN" | count)"

# (a) retention 1m:2m for the destination
printf '%s\n' "$P0_PLAIN" | head -n 200 > "$WORK/p0-plain.txt"; printf '%s\n' "$P0_ENC" | head -n 200 > "$WORK/p0-enc.txt"
put_file "$WORK/p0-plain.txt" "$GDIR/p0-plain.txt" 644; put_file "$WORK/p0-enc.txt" "$GDIR/p0-enc.txt" 644
stop_daemon
sleep 3
write_config "30s" '"1m": "2m"'
out="$(import_config)"; rc=$?
printf '%s\n' "$out" | sed 's/^/    CLI: /'
check "(5a) the import with the destination retention 1m:2m exits 0" "0" "$rc"
check "(5a) ... and keeps the client id" "$CID" "$(cli_to_file /tmp/zfsb-id.json identity show --json; jfile /tmp/zfsb-id.json 'd["client_id"]' | tr -d '[:space:]')"
start_daemon
Y_PLAIN="$(snap_now "$SRC_PLAIN")"; Y_ENC="$(snap_now "$SRC_ENC")"
retention_stamped() { # the copy's stamped config carries the destination's rule 60 s / 120 s
    [ "$(msh "zfs get -H -o value org.zfsbackup:config '$COPY_PLAIN' | base64 -d 2>/dev/null | '$PY' '$JGET_PY' '\"yes\" if [(r[\"age\"], r[\"keep_for\"]) for r in d[\"retention_rules\"]] == [(60.0, 120.0)] else \"no\"'" | tr -d '[:space:]')" = "yes" ]
}
if wait_for 120 retention_stamped; then pass "(5a) the copy's org.zfsbackup:config carries the destination's retention (60 s / 120 s)"
else fail "(5a) the copy's config never showed the destination retention: $(msh "zfs get -H -o value org.zfsbackup:config '$COPY_PLAIN' | base64 -d 2>/dev/null | head -c 300")"; fi
pruned_plain() {
    local o; o="$(msh "bash '$GDIR/pruned_ok.sh' '$COPY_PLAIN' 150 '$GDIR/p0-plain.txt'" | tr -d '\r')"; PR_PLAIN="$o"
    [ "${o%% *}" != "EXPECT=0" ] && [ "${o##* LEFT=}" = "0" ]
}
pruned_enc() {
    local o; o="$(msh "bash '$GDIR/pruned_ok.sh' '$COPY_ENC' 150 '$GDIR/p0-enc.txt'" | tr -d '\r')"; PR_ENC="$o"
    [ "${o%% *}" != "EXPECT=0" ] && [ "${o##* LEFT=}" = "0" ]
}
PR_PLAIN=""; PR_ENC=""
if wait_for 420 pruned_plain; then pass "(5a) plain: every original copy snapshot older than 150 s was pruned ($PR_PLAIN)"; else fail "(5a) plain: old copy snapshots are still there after 420 s ($PR_PLAIN)"; fi
if wait_for 120 pruned_enc;   then pass "(5a) enc: every original copy snapshot older than 150 s was pruned, with no key loaded ($PR_ENC)"; else fail "(5a) enc: old copy snapshots are still there ($PR_ENC)"; fi
C5_PLAIN="$(snaps "$COPY_PLAIN" | count)"; C5_ENC="$(snaps "$COPY_ENC" | count)"
check "(5a) plain: the copy has fewer snapshots than before ($(printf '%s\n' "$P0_PLAIN" | count) -> $C5_PLAIN) but at least one" "yes" "$([ "$C5_PLAIN" -ge 1 ] && [ "$C5_PLAIN" -lt "$(printf '%s\n' "$P0_PLAIN" | count)" ] && echo yes || echo no)"
check "(5a) plain: what is left on the copy is the source's own (names and guids)" "yes" "$(subset_ok "$SRC_PLAIN" && echo yes || echo no)"
check "(5a) enc: what is left on the copy is the source's own (names and guids)" "yes" "$(subset_ok "$SRC_ENC" && echo yes || echo no)"
newest_plain="$(snaps "$COPY_PLAIN" | tail -n1)"; newest_enc="$(snaps "$COPY_ENC" | tail -n1)"
check "(5a) the newest copy snapshots ($newest_plain / $newest_enc) are the source's, with its guids" "yes" "$(copy_has "$SRC_PLAIN" "$newest_plain" && copy_has "$SRC_ENC" "$newest_enc" && echo yes || echo no)"
SRC_AFTER_PLAIN="$(snaps "$SRC_PLAIN")"
check "(5a) the SOURCE lost nothing (every earlier source snapshot is still there)" "0" \
    "$(comm -23 <(printf '%s\n' "$SRC_BEFORE_PLAIN" | sort) <(printf '%s\n' "$SRC_AFTER_PLAIN" | sort) | count)"
check "(5a) never recursive: the child $KID kept its same-named snapshots" "$KID_BEFORE" "$(snaps "$KID" | tr '\n' ' ')"
check "(5a) the sibling tree of $OTHER is byte-identical (names, guids, owner, config)" "$SIB1_BEFORE" "$(sib_fp "$OTHER")"
check "(5a) the sibling tree of ${PREFIXED} (our id as a prefix) is byte-identical" "$SIB2_BEFORE" "$(sib_fp "$PREFIXED")"
check "(5a) the copy is still received incrementally: $Y_PLAIN / $Y_ENC arrive" "yes" "$(wait_for 120 copies_have "$Y_PLAIN" "$Y_ENC" && echo yes || echo no)"

# (b) retention 1s:5s with the snapshot worker quiet: shrink to exactly the newest, keep it, stay incremental
stop_daemon
sleep 3
write_config "1d" '"1s": "5s"'
out="$(import_config)"; rc=$?
check "(5b) the import with retention 1s:5s and snapshot frequency 1d exits 0" "0" "$rc"
start_daemon
only_newest() { [ "$(snaps "$COPY_PLAIN" | count)" = "1" ] && [ "$(snaps "$COPY_ENC" | count)" = "1" ]; }
zero_seen=0
waited=0
until only_newest; do
    for c in "$COPY_PLAIN" "$COPY_ENC"; do [ "$(snaps "$c" | count)" = "0" ] && zero_seen=1; done
    [ "$waited" -ge 240 ] && break
    sleep 3; waited=$((waited+3))
done
check "(5b) each copy shrank to exactly ONE snapshot" "1 1" "$(snaps "$COPY_PLAIN" | count) $(snaps "$COPY_ENC" | count)"
LAST_PLAIN="$(snaps "$COPY_PLAIN" | tail -n1)"; LAST_ENC="$(snaps "$COPY_ENC" | tail -n1)"
check "(5b) ... and it is the source's newest sent snapshot, the anchor ($LAST_PLAIN / $LAST_ENC)" "$LAST_PLAIN $LAST_ENC" "$(anchor_of "$SRC_PLAIN") $(anchor_of "$SRC_ENC")"
sleep 65
check "(5b) after 3 more prune cycles the newest snapshots are still there, alone" "$LAST_PLAIN $LAST_ENC 1 1" \
    "$(snaps "$COPY_PLAIN" | tail -n1) $(snaps "$COPY_ENC" | tail -n1) $(snaps "$COPY_PLAIN" | count) $(snaps "$COPY_ENC" | count)"
check "(5b) the copies were never seen empty" "0" "$zero_seen"
Z_PLAIN="$(snap_now "$SRC_PLAIN")"; Z_ENC="$(snap_now "$SRC_ENC")"
if wait_for 180 copies_have "$Z_PLAIN" "$Z_ENC"; then pass "(5b) the next snapshots ($Z_PLAIN / $Z_ENC) arrived incrementally on the pruned copies (the base survived)"; else fail "(5b) the next snapshots did not arrive after the copies were pruned to their newest"; fi
settled() { [ "$(snaps "$COPY_PLAIN" | tail -n1)" = "$Z_PLAIN" ] && only_newest; }
if wait_for 120 settled; then pass "(5b) the old newest was pruned in turn: each copy holds exactly its new newest"; else fail "(5b) the copies did not settle on the new newest (plain: [$(snaps "$COPY_PLAIN" | tr '\n' ' ')])"; fi
check "(5b) the child $KID still has its snapshots" "$KID_BEFORE" "$(snaps "$KID" | tr '\n' ' ')"
check "(5b) the sibling tree of $OTHER is still byte-identical" "$SIB1_BEFORE" "$(sib_fp "$OTHER")"
check "(5b) the sibling tree of ${PREFIXED} is still byte-identical" "$SIB2_BEFORE" "$(sib_fp "$PREFIXED")"
check "(5b) the sibling's old snapshots are all there" "4 4" "$(snaps "$TARGET/$OTHER/$SRC_PLAIN" | count) $(snaps "$TARGET/$PREFIXED/$SRC_PLAIN" | count)"
check "(5b) the source still has every snapshot (its retention 1s:1d prunes nothing)" "0" \
    "$(comm -23 <(printf '%s\n' "$SRC_BEFORE_PLAIN" | sort) <(snaps "$SRC_PLAIN" | sort) | count)"
echo "    evidence (not asserted): prune lines"
log_since "$MARK4" | grep -iE "prun" | cut -c1-200 | tail -n 6 | sed 's/^/      log: /'

# ───────────────────────────────── 6 ────────────────────────────────────────
echo
step "6 — check grades the local destination; list destinations"
W_PLAIN="$(snap_now "$SRC_PLAIN")"; W_ENC="$(snap_now "$SRC_ENC")"
wait_for 180 copies_have "$W_PLAIN" "$W_ENC" || fail "(6) the last snapshots did not reach the copies"
ck_run --json
st="$(ck_anchor_states)"
echo "    entries: $(ck_expr '"; ".join("%s:%s %s=%s" % (e["source"], e["subject"], e.get("peer", ""), e["state"]) for e in d["entries"])')"
check "(6) check: both dataset x $DEST anchor entries are OK once caught up" "${SRC_ENC}:${DEST}=OK,${SRC_PLAIN}:${DEST}=OK" "$st"
check "(6) check: overall state OK, exit 0" "0 OK" "$CK_RC $(ck_expr 'd["state"]' | tr -d '[:space:]')"
ck_run --nagios
check "(6) check --nagios exits 0" "0" "$CK_RC"
out="$(cli list destinations)"
check_has "(6) list destinations still shows $DEST as local" "${DEST} +local +zfs:${TARGET}" "$out"
out="$(cli list datasets)"
check_has "(6) list datasets shows both datasets sending to $DEST" "${SRC_PLAIN}.*${DEST}" "$out"

fi    # ONLY_REC
# ───────────────────────────────── 7 ────────────────────────────────────────
echo
step "7 — a recursive tree: every descendant is its own copy (raw where encrypted), children come and go, a planted backup copy is left alone"
stop_daemon
sleep 3
msh "
    set -e
    zfs create '$R_TOP'
    zfs create -o encryption=on -o keyformat=passphrase -o keylocation=file://$KEYFILE '$R_ENC'
    zfs create '$R_INNER'
    zfs create '$R_PLAIN'
    zfs create -V 8M '$R_VOL'
    for d in '$R_TOP' '$R_ENC' '$R_INNER' '$R_PLAIN'; do
        dd if=/dev/urandom of=\$(zfs get -H -o value mountpoint \$d)/seed.bin bs=1M count=2 status=none
    done
    udevadm settle || true
    for i in 1 2 3 4 5 6 7 8 9 10; do [ -b /dev/zvol/$R_VOL ] && break; sleep 1; done
    if [ -b /dev/zvol/$R_VOL ]; then dd if=/dev/urandom of=/dev/zvol/$R_VOL bs=1M count=4 conv=fsync status=none; else echo 'no zvol node: the volume stays empty'; fi
    zfs list -H -r -o name,type,encryption,keystatus '$R_TOP'
" || fail "(7) could not create the tree under $R_TOP"
check "(7) precondition: the encrypted child and the grandchild are encrypted with a loaded key, the plain child is not" "yes/available yes/available off" \
    "$([ "$(zv encryption "$R_ENC")" != off ] && echo yes)/$(zv keystatus "$R_ENC") $([ "$(zv encryption "$R_INNER")" != off ] && echo yes)/$(zv keystatus "$R_INNER") $(zv encryption "$R_PLAIN")"
check "(7) precondition: the grandchild inherits the child's encryption root" "$R_ENC" "$(zv encryptionroot "$R_INNER")"

# the whole config is replaced by the recursive dataset alone: the earlier plain/enc datasets stop being sources (their copies stay)
{
    cat <<EOF
# zfsbackup config of test_local_destination.sh (scenario 7: a recursive dataset)
snapshot_prefix: "autosnap"
check_interval: "10s"
prune_interval: "20s"
destinations:
  ${DEST}:
    url: "zfs:${TARGET}"
datasets:
  - name: "${R_TOP}"
    enabled: true
    frequency: "30s"
    recursive: true
    retention:
      "1s": "1d"
    remote:
      - destination: ${DEST}
        frequency: "20s"
        retention:
          "1s": "${R_RET}s"
EOF
} > "$WORK/config-rec.yaml"
put_file "$WORK/config-rec.yaml" "$CFG" 600
out="$(import_config)"; rc=$?
printf '%s\n' "$out" | sed 's/^/    CLI: /'
check "(7) the import of the recursive config exits 0 (no copy lies below the tree: the CLI has nothing to refuse)" "0" "$rc"
check "(7) ... and keeps the client id" "$CID" "$(cli_to_file /tmp/zfsb-id.json identity show --json; jfile /tmp/zfsb-id.json 'd["client_id"]' | tr -d '[:space:]')"
MARK7="$(loglines)"
start_daemon
wait_for 60 daemon_active
MAIN7="$(mainpid)"

# 7a: every descendant is sent as a dataset of its own
if wait_for 360 rec_first_done; then pass "(7a) all five datasets of the tree (parent, enc, enc/inner, plain, vol) got a copy with a snapshot and a local anchor"
else fail "(7a) not every dataset of the tree reached its copy within 360 s: $(for d in "${rec_all[@]}"; do printf '%s=[%s] ' "${d#$R_TOP}" "$(snaps "$(copy_of "$d")" | count)"; done)"; fi
for d in "${rec_all[@]}"; do
    check "(7a) $d: the copy is exactly at <target>/<id>/<pool>/<path>" "yes" "$(exists "$(copy_of "$d")")"
done
check "(7a) the plain children and the parent are PLAIN copies (not encrypted)" "off off off" "$(zv encryption "$(copy_of "$R_TOP")") $(zv encryption "$(copy_of "$R_PLAIN")") $(zv encryption "$(copy_of "$R_VOL")")"
check "(7a) the encrypted child's copy is RAW: encrypted, keystatus unavailable, its own encryption root, not mounted" "yes unavailable $(copy_of "$R_ENC") no" \
    "$([ "$(zv encryption "$(copy_of "$R_ENC")")" != off ] && echo yes) $(zv keystatus "$(copy_of "$R_ENC")") $(zv encryptionroot "$(copy_of "$R_ENC")") $(zv mounted "$(copy_of "$R_ENC")")"
check "(7a) the grandchild's copy is RAW too (encrypted, key unavailable)" "yes unavailable" \
    "$([ "$(zv encryption "$(copy_of "$R_INNER")")" != off ] && echo yes) $(zv keystatus "$(copy_of "$R_INNER")")"
info "evidence: the grandchild's copy encryptionroot is '$(zv encryptionroot "$(copy_of "$R_INNER")")' (the source's is $R_ENC)"
check "(7a) the zvol's copy is a volume with readonly=on and volmode=none" "volume on none" "$(zv type "$(copy_of "$R_VOL")") $(zv readonly "$(copy_of "$R_VOL")") $(zv volmode "$(copy_of "$R_VOL")")"
for d in "$R_TOP" "$R_ENC" "$R_INNER" "$R_PLAIN"; do check_copy_props "7a ${d#$SRC_POOL/}" "$d"; done
for d in "$R_ENC" "$R_INNER" "$R_PLAIN" "$R_VOL"; do
    check "(7a) $d: the copy's stamped config names the CHILD and says recursive=false" "$d|False" "$(rec_copy_cfg "$d" '"%s|%s" % (d["name"], d["recursive"])')"
    check "(7a) $d: ... and carries the destination's retention (1 s / $R_RET s)" "yes" \
        "$(rec_copy_cfg "$d" '"yes" if [(r["age"], r["keep_for"]) for r in d["retention_rules"]] == [(1.0, '"$R_RET"'.0)] else "no"')"
done
check "(7a) the parent's own copy config keeps recursive=true (it is the parent's config)" "True" "$(rec_copy_cfg "$R_TOP" 'd["recursive"]')"
# the inherited-anchor regression: every child has its OWN local anchor, naming a snapshot on its copy with the source's guid
for d in "${rec_all[@]}"; do
    a="$(local_anchor_of "$d")"
    check "(7a) $d has its own LOCAL anchor.$DEST ($a), and that snapshot is on its copy with the source's guid" "yes" "$([ -n "$a" ] && copy_has "$d" "$a" && echo yes || echo no)"
done
rec_log="$(log_since "$MARK7")"
check_has "(7a) the log shows the parent's full send before the first child's" "Backup complete: ${R_TOP}@" "$rec_log"
first_parent="$(printf '%s\n' "$rec_log" | grep -nE "Backup complete: ${R_TOP}@" | head -n1 | cut -d: -f1)"
first_child="$(printf '%s\n' "$rec_log" | grep -nE "Backup complete: ${R_TOP}/[a-z]+(/[a-z]+)?@" | head -n1 | cut -d: -f1)"
check "(7a) ... and the parent's copy was completed before any child's (parents first)" "yes" "$([ -n "$first_parent" ] && [ -n "$first_child" ] && [ "$first_parent" -lt "$first_child" ] && echo yes || echo no)"

# 7b: a backlog of three recursive snapshots: a chain for every child
stop_daemon
sleep 3
declare -a RC=()
for i in 1 2 3; do
    for d in "$R_TOP" "$R_ENC" "$R_INNER" "$R_PLAIN"; do write_mib "$d" "chain$i.bin" 1; done
    RC+=("$(snap_rec)")
done
info "backlog: ${RC[*]}"
MARK7B="$(loglines)"
start_daemon
wait_for 60 daemon_active
MAIN7="$(mainpid)"          # the supervisor of 7b-7e (7a's was stopped for the backlog)
REC_N="${RC[2]}"
if wait_for 300 rec_copies_have "${rec_all[@]}"; then pass "(7b) the newest backlog snapshot ${RC[2]} is on all five copies with the source's guids"
else fail "(7b) ${RC[2]} did not reach every copy within 300 s: $(for d in "${rec_all[@]}"; do printf '%s=%s ' "${d#$R_TOP}" "$(copy_has "$d" "$REC_N" && echo yes || echo no)"; done)"; fi
for d in "${rec_all[@]}"; do
    a="$(local_anchor_of "$d")"
    check "(7b) $d: the local anchor is on the copy (guid equal) and at or past the newest backlog snapshot by position" "yes" \
        "$([ -n "$a" ] && copy_has "$d" "$a" && [ "$(idx_in "$d" "$a")" -ge "$(idx_in "$d" "$REC_N")" ] 2>/dev/null && echo yes || echo no)"
    check "(7b) $d: everything on its copy is the source's own (names and guids)" "yes" "$(subset_ok "$d" && echo yes || echo no)"
done
steps_enc="$(log_since "$MARK7B" | grep -cE "Sending incremental ${R_ENC}:" || true)"
check "(7b) the encrypted child's backlog went out as >= 3 incremental steps ($steps_enc logged)" "yes" "$([ "${steps_enc:-0}" -ge 3 ] && echo yes || echo no)"
check "(7b) the raw copies are still locked" "unavailable unavailable" "$(zv keystatus "$(copy_of "$R_ENC")") $(zv keystatus "$(copy_of "$R_INNER")")"

# 7c: children created mid-run; one is destroyed again
msh "
    set -e
    zfs create '$R_LATE'; zfs create '$R_GONE'
    for d in '$R_LATE' '$R_GONE'; do dd if=/dev/urandom of=\$(zfs get -H -o value mountpoint \$d)/seed.bin bs=1M count=2 status=none; done
" || fail "(7c) could not create the mid-run children"
MARK7C="$(loglines)"
late_gone_have_all() { [ -n "$(local_anchor_of "$R_LATE")" ] && [ -n "$(local_anchor_of "$R_GONE")" ] && [ "$(snaps "$(copy_of "$R_GONE")" | count)" -ge 3 ]; }
if wait_for 420 late_gone_have_all; then pass "(7c) the children created mid-run were discovered and sent: copies with a local anchor, and >= 3 snapshots on gone's copy"
else fail "(7c) late/gone did not get copies in 420 s (late: [$(snaps "$(copy_of "$R_LATE")" | count)] anchor '$(local_anchor_of "$R_LATE")', gone: [$(snaps "$(copy_of "$R_GONE")" | count)] anchor '$(local_anchor_of "$R_GONE")')"; fi
check "(7c) late's copy is a plain copy at <target>/<id>/<pool>/<path>, parent first" "yes off" "$(exists "$(copy_of "$R_LATE")") $(zv encryption "$(copy_of "$R_LATE")")"
check_has "(7c) the first send of gone was a FULL one" "Backup complete: ${R_GONE}@[^ ]+ \\(full\\)" "$(log_since "$MARK7C")"
check "(7c) gone: its copy's config names it, recursive=false, retention ${R_RET}s" "$R_GONE|False" "$(rec_copy_cfg "$R_GONE" '"%s|%s" % (d["name"], d["recursive"])')"
G_COPY="$(copy_of "$R_GONE")"
destroyed=0
for i in 1 2 3 4 5; do
    if msh "zfs destroy -r '$R_GONE' 2>&1"; then destroyed=1; break; fi
    sleep 4
done
check "(7c) the source child $R_GONE was destroyed (a child gone mid-run)" "yes" "$([ "$destroyed" -eq 1 ] && [ "$(exists "$R_GONE")" = no ] && echo yes || echo no)"
wait_for 60 no_xfer          # a send of gone that was already running (the destroy is refused while it runs) has ended
G_NEWEST="$(snaps "$G_COPY" | tail -n1)"
G_COPY="$(copy_of "$R_GONE")"; G_COPY_GUID="$(guid_of "$G_COPY")"
G_COUNT0="$(snaps "$G_COPY" | count)"
info "gone's copy right after the destroy: $G_COUNT0 snapshots, newest $G_NEWEST"
only_newest_gone() { [ "$(snaps "$G_COPY" | count)" = "1" ]; }
if wait_for 420 only_newest_gone; then pass "(7c) the destroyed child's copy was thinned to ONE snapshot by the retention stamped on it ($G_COUNT0 -> 1)"
else fail "(7c) the copy of the destroyed child still has $(snaps "$G_COPY" | count) snapshots after 420 s: [$(snaps "$G_COPY" | tr '\n' ' ')]"; fi
check "(7c) ... the one left is its newest, $G_NEWEST (a pruned copy keeps its newest)" "$G_NEWEST" "$(snaps "$G_COPY" | tail -n1)"
check "(7c) the copy itself still exists (same guid): the daemon never destroys a copy" "$G_COPY_GUID" "$(guid_of "$G_COPY")"
sleep 45
check "(7c) after 2 more prune cycles it still holds exactly its newest" "1 $G_NEWEST" "$(snaps "$G_COPY" | count) $(snaps "$G_COPY" | tail -n1)"
check "(7c) the daemon never restarted (same supervisor pid)" "$MAIN7" "$(mainpid)"
check_lacks "(7c) no Traceback in the daemon log" "Traceback" "$(log_since "$MARK7")"

# 7d: check grades one anchor per child x destination
rec_all2=("${rec_all[@]}" "$R_LATE")
W_N="$(snap_rec)"; REC_N="$W_N"
rec_wait_all() { rec_copies_have "${rec_all2[@]}"; }
wait_for 240 rec_wait_all || fail "(7d) the last recursive snapshot $W_N did not reach every copy"
ck_run --json
st="$(ck_anchor_states)"
echo "    entries: $(ck_expr '"; ".join("%s:%s %s=%s" % (e["source"], e["subject"], e.get("peer", ""), e["state"]) for e in d["entries"])')"
want="$(for d in "${rec_all2[@]}"; do printf '%s:%s=OK\n' "$d" "$DEST"; done | LC_ALL=C sort | paste -sd, -)"
check "(7d) check --json: one anchor entry per dataset x $DEST (parent, five children), every one OK once caught up" "$want" "$st"
check "(7d) ... the destroyed child $R_GONE has no entry any more, and its copy is not a check subject" "0" "$(printf '%s\n' "$st" | grep -c "$R_GONE" || true)"
check "(7d) check: overall state OK, exit 0" "0 OK" "$CK_RC $(ck_expr 'd["state"]' | tr -d '[:space:]')"
ck_run --nagios
check "(7d) check --nagios exits 0" "0" "$CK_RC"

# 7e: a backup copy planted below the recursive dataset
step "7e — a backup copy below the recursive dataset is never snapshotted, sent or pruned"
stop_daemon
sleep 35          # the next snapshot is then due the moment the daemon is back
X_NAME="autosnap_20200101000001"; Y_NAME="autosnap_20200101000002"
msh "
    set -e
    zfs create -o canmount=noauto -o mountpoint=none -o org.zfsbackup:client_id=zfsb-foreign -o org.zfsbackup:source_dataset=otherpool/data '$R_REPL'
    zfs snapshot -r '$R_TOP'@$X_NAME
    zfs snapshot \$(zfs list -H -o name -r -t filesystem,volume '$R_TOP' | grep -vE '^${R_REPL}\$' | sed 's/\$/@$Y_NAME/' | tr '\\n' ' ')
" || fail "(7e) could not plant the copy $R_REPL and the old snapshots"
REPL_GUID="$(guid_of "$R_REPL")"; REPL_SNAP_GUID="$(guid_of "$R_REPL@$X_NAME")"
check "(7e) precondition: $R_REPL holds the stray name $X_NAME (from the recursive snapshot) and not $Y_NAME, which the other datasets have" "$X_NAME 1" \
    "$(snaps "$R_REPL" | tr '\n' ' ' | sed 's/ *$//') $(snaps "$R_REPL" | count)"
check "(7e) precondition: $X_NAME and $Y_NAME exist on the parent and the children" "yes yes yes" \
    "$([ "$(exists "$R_TOP@$X_NAME")" = yes ] && echo yes) $([ "$(exists "$R_ENC@$Y_NAME")" = yes ] && echo yes) $([ "$(exists "$R_PLAIN@$X_NAME")" = yes ] && echo yes)"
MARK7E="$(loglines)"
start_daemon
wait_for 60 daemon_active
replica_logged() { log_since "$MARK7E" | gq -E " ERROR .*${R_REPL}"; }
wait_for 150 replica_logged
check_has "(7e) the daemon logs an ERROR naming the copy $R_REPL (and how to move it out)" "ERROR.*${R_REPL}" "$(log_since "$MARK7E")"
newest_top() { snaps "$R_TOP" | tail -n1; }
rec_new_snap() { local n; n="$(newest_top)"; [ -n "$n" ] && [ "$n" != "$W_N" ] && [ "$n" != "$X_NAME" ] && [ "$n" != "$Y_NAME" ]; }
wait_for 150 rec_new_snap
N7E="$(newest_top)"
info "the snapshot taken after the plant: $N7E"
check "(7e) the worker took a new snapshot of the tree after the copy appeared ($N7E)" "yes" "$([ "$N7E" != "$W_N" ] && [ "$N7E" != "$X_NAME" ] && [ "$N7E" != "$Y_NAME" ] && echo yes || echo no)"
check "(7e) the new snapshot is on the parent and on every source child" "yes" \
    "$(ok=yes; for d in "$R_TOP" "$R_ENC" "$R_INNER" "$R_PLAIN" "$R_VOL" "$R_LATE"; do [ "$(exists "$d@$N7E")" = yes ] || ok=no; done; echo $ok)"
check "(7e) ... all of them in ONE transaction (a single atomic zfs snapshot: the same createtxg everywhere)" "1" \
    "$(msh "for d in '$R_TOP' '$R_ENC' '$R_INNER' '$R_PLAIN' '$R_VOL' '$R_LATE'; do zfs get -H -o value createtxg \"\$d@$N7E\"; done | sort -u | wc -l" | tr -d '[:space:]')"
check "(7e) NEVER SNAPSHOTTED: the copy $R_REPL has no snapshot but the stray $X_NAME" "$X_NAME" "$(snaps "$R_REPL" | tr '\n' ' ' | sed 's/ *$//')"
# never pruned, and the rest of the tree is pruned: the old names are far past the 1 d retention
y_pruned() { [ "$(exists "$R_ENC@$Y_NAME")" = no ] && [ "$(exists "$R_TOP@$Y_NAME")" = no ] && [ "$(exists "$R_PLAIN@$Y_NAME")" = no ]; }
if wait_for 150 y_pruned; then pass "(7e) $Y_NAME, a name the copy does not hold, was pruned from the parent and the children (the pruner is alive)"
else fail "(7e) $Y_NAME is still on the source tree after 150 s"; fi
sleep 65
check "(7e) NEVER PRUNED: $X_NAME, a name the copy holds, is still on the copy" "yes" "$(exists "$R_REPL@$X_NAME")"
check "(7e) ... and so is the same name on the parent and the children (a recursive destroy would have taken the copy's too)" "yes yes yes" \
    "$(exists "$R_TOP@$X_NAME") $(exists "$R_ENC@$X_NAME") $(exists "$R_PLAIN@$X_NAME")"
check "(7e) the copy's guid, its snapshot's guid and its markers are unchanged" "$REPL_GUID $REPL_SNAP_GUID zfsb-foreign local otherpool/data" \
    "$(guid_of "$R_REPL") $(guid_of "$R_REPL@$X_NAME") $(zvs org.zfsbackup:client_id "$R_REPL" | tr -d '\n') $(zv org.zfsbackup:source_dataset "$R_REPL")"
check "(7e) NEVER SENT: no copy of $R_REPL exists under the target" "0" "$(msh "zfs list -H -r -o name '$TARGET' | grep -c '/replica\$' || true" | tr -d '[:space:]')"
check "(7e) ... nor any dataset under the target whose source_dataset is it" "0" \
    "$(msh "zfs get -H -r -o value org.zfsbackup:source_dataset '$TARGET' | grep -c '^${R_REPL}\$' || true" | tr -d '[:space:]')"
check "(7e) the daemon kept sending the other children meanwhile: the newest anchor of the encrypted child is on its copy" "yes" \
    "$(a="$(local_anchor_of "$R_ENC")"; [ -n "$a" ] && copy_has "$R_ENC" "$a" && echo yes || echo no)"
ck_run --json
echo "    entries: $(ck_expr '"; ".join("%s:%s %s=%s" % (e["source"], e["subject"], e.get("peer", ""), e["state"]) for e in d["entries"])')"
check "(7e) check --json has a WARN 'copies' entry naming $R_REPL" "WARN" \
    "$(ck_expr '",".join(sorted(set(e["state"] for e in d["entries"] if e["source"] == "copies" and e["subject"] == "'"$R_REPL"'")))' | tr -d '[:space:]')"
check "(7e) ... the copy is not graded as a child (no anchor entry for it), and the overall exit is WARN (1)" "0 1" \
    "$(ck_expr 'sum(1 for e in d["entries"] if e["source"] == "anchor" and e["subject"] == "'"$R_REPL"'")' | tr -d '[:space:]') $CK_RC"
check_lacks "(7e) no Traceback in the daemon log" "Traceback" "$(log_since "$MARK7")"
echo "    evidence (not asserted): the daemon's lines about the copy"
log_since "$MARK7E" | grep -E "${R_REPL}" | grep -v " DEBUG " | cut -c1-230 | sort | uniq -c | sort -rn | head -n 5 | sed 's/^/      log: /'
stop_daemon
msh "zfs destroy -r '$R_REPL' >/dev/null 2>&1; true" || true

# ───────────────────────────────── 8 ────────────────────────────────────────
echo
step "8 — clean exit"
stop_daemon
sleep 3
check "(8) no zfsbackup daemon/worker process is left" "0" "$(daemon_procs)"
check "(8) no zfs send/receive process is left" "0" "$(xfer_procs)"
check "(8) the unit is gone or inactive" "yes" "$(daemon_active && echo no || echo yes)"
dlog="$(msh "cat '$LOG'")"
check_lacks "(8) no Traceback in the daemon log" "Traceback" "$dlog"
echo "    evidence (not asserted): distinct ERROR/WARNING lines of the whole run, by count"
printf '%s\n' "$dlog" | grep -E " (ERROR|WARNING) " | sed -E 's/^[0-9: ,.-]+//' | cut -c1-170 | sort | uniq -c | sort -rn | head -n 12 | sed 's/^/      /'

# ─────────────────────────────── Report ─────────────────────────────────────
echo
echo "${BOLD}=================================================================${NC}"
echo "${BOLD} Results${NC}"
echo "${BOLD}=================================================================${NC}"
for r in ${RESULTS[@]+"${RESULTS[@]}"}; do echo "  $r"; done
echo "-----------------------------------------------------------------"
echo "  ${GREEN}${PASS_COUNT} passed${NC}, ${RED}${FAIL_COUNT} failed${NC}"
if [ "${#FINDINGS[@]}" -gt 0 ]; then
    echo; echo "${BOLD}Findings (not counted as failures)${NC}"
    for f in "${FINDINGS[@]}"; do echo "  - $f"; done
fi
echo "  daemon log and final ZFS listing: $LOG_DIR (daemon.log, final-zfs.txt)"

teardown
[ "$FAIL_COUNT" -eq 0 ] && exit 0 || exit 1
