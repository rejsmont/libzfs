#!/usr/bin/env bash
#
# test_openzfs_matrix.sh — run the real-ZFS pytest tier on every supported OpenZFS version (plan items T2 / B4).
#
# Supported matrix (docs/production_readiness_plan.md, decision Q5/D4); 2.3 is best-effort and not in it:
#   zfsb-zfs21   Ubuntu 22.04 jammy     OpenZFS 2.1.x
#   zfsb-zfs22   Ubuntu 24.04 noble     OpenZFS 2.2.x
#   zfsb-zfs24   Ubuntu 26.04 resolute  OpenZFS 2.4.x   (2.4.1 verified)
#
# For each VM (created if missing, reused otherwise) the harness:
#   1. installs zfsutils-linux from the distro, plus a Python >= 3.11 (the floor in pyproject.toml): the distro
#      python3 is used when it is new enough; otherwise (jammy: 3.10) python3.11 comes from the deadsnakes PPA, or (Launchpad 503s) uv;
#   2. ships `git archive $SRC_REF` (default HEAD) and builds a venv with the test dependencies;
#   3. logs `zfs version` / `zpool version`;
#   4. runs `ZFSB_REAL_ZFS=1 pytest -m real_zfs tests/test_real_zfs.py [extra files]` INSIDE the VM, as the
#      unprivileged login user (the `auto_zfs_pool` fixture creates a 512 MiB file-backed pool with
#      `sudo -n zpool create` and delegates with `zfs allow`; TMPDIR/--basetemp point the image at /var/tmp,
#      because /tmp is a tmpfs). The MULTIPASS_VM wrapper is deliberately NOT used: `multipass exec` adds
#      buffering, and stdin=DEVNULL through it hits the known wedge;
#   5. captures the zfs output formats zfsbackup parses (B4) with a tiny guest script (see --no-formats);
#   6. with --captures / --captures-only: runs the SP spike (plan items M1-M7, Z1-Z8): ~250 recorded zfs
#      commands on a throwaway file-backed pool (keyfiles, never prompts), once per locale (default login
#      environment, then LC_ALL=C LANG=C), each record kept as exact-byte files for the fixtures (see CAPTURES);
#   7. prints a per-version summary (pass/fail/skip/error counts and the failing test ids).
#
# Usage:
#   ./scenarios/test_openzfs_matrix.sh                      # all three versions, tests/test_real_zfs.py only
#   ./scenarios/test_openzfs_matrix.sh --only 21,24         # a subset (21 22 24)
#   ./scenarios/test_openzfs_matrix.sh --extra tests/test_zfsbackup_real.py   # more test files (repeatable)
#   ./scenarios/test_openzfs_matrix.sh --formats-only       # provision + capture formats, no pytest
#   ./scenarios/test_openzfs_matrix.sh --no-formats         # pytest only
#   ./scenarios/test_openzfs_matrix.sh --keep-pools         # leave the format-capture pool in place
#   ./scenarios/test_openzfs_matrix.sh --purge              # delete the matrix VMs (only these three) at the end
#   ./scenarios/test_openzfs_matrix.sh --captures-only      # SP spike only: ship the scripts, run the captures, no pytest/formats
#   ./scenarios/test_openzfs_matrix.sh --captures           # ... in addition to pytest and the formats
#   ./scenarios/test_openzfs_matrix.sh --captures-only --sections M3,Z4   # a subset (M1 M2 M3 M4 EP M5 M6 M7 Z2 Z3 Z4 Z5 Z6 Z7)
#   ./scenarios/test_openzfs_matrix.sh --captures-only --locales default,C,en_US.utf8   # passes (default: default,C)
#   SRC_REF=<commit-ish> ...     # ship that tree instead of HEAD; for uncommitted work: SRC_REF=$(git stash create)
#   MATRIX_LOG_DIR=<dir> ...     # where the per-VM logs go (default: a fresh mktemp dir, printed at the end)
#   (uncommitted work does not need shipping for --captures-only: the capture scripts are embedded in this file)
#
# CAPTURES (--captures): per VM, under MATRIX_LOG_DIR (give it a persistent path):
#   <vm>.captures/<locale>/<SECTION>.txt        a section's records, in order, with fixed markers: "=== CASE <sec>/<id>",
#                                               "--- cmd|rc|pipestatus|stdout|stderr|send.stderr", "=== END", "#note:" lines
#   <vm>.captures/<locale>/<SECTION>/<id>.{cmd,rc,stdout,stderr[,pipestatus,send.stderr,token]}   the exact bytes, one
#                                               file per stream (M7: these are what the fixtures are built from)
#   <vm>.captures/<locale>/{ENV.txt,M1-SUMMARY.tsv,SETUP-ERRORS.log}   versions + locale env; one parsed line per M1 token
#   <vm>.captures.log   the run's progress;  <vm>.locale-diff.txt  default vs LC_ALL=C, ids/sizes/pool names normalised
#   Sections: M1 token flags, M2 stale tokens, M3_Z8 destroy failures, M4 listing, EP encryption properties,
#   M5_Z1 raw receive and -F, M6 -L mismatch, M7 -F/rollback/modified-head semantics (B-2b U1 U2 U4 U5 U6), Z2 ownership -o on raw, Z3 partial new dataset, Z4 key unloaded,
#   Z5 change-key, Z6 encryption/encryptionroot, Z7 raw copy without a key.
#
# Exit status: 0 when every selected version ran and no test failed or errored, 1 otherwise, 2 on usage errors.
# Requires: multipass on the host. Never touches ZFS on the host, and never any VM but the three above.

set -uo pipefail

# ─────────────────────────────── Configuration ──────────────────────────────
# name:image:label
ALL_TARGETS=("zfsb-zfs21:22.04:2.1" "zfsb-zfs22:24.04:2.2" "zfsb-zfs24:26.04:2.4")

VM_CPUS="${VM_CPUS:-2}"
VM_MEM="${VM_MEM:-2G}"
VM_DISK="${VM_DISK:-10G}"

SRC_REF="${SRC_REF:-HEAD}"
WORK="/var/tmp/zfsb-matrix"          # guest work dir (owned by the login user): tree, venv, pool images, scratch
GUEST_RUN="$WORK/run.sh"
GUEST_FORMATS="$WORK/formats.sh"
GUEST_CAPTURES="$WORK/captures.sh"

PURGE=0; KEEP_POOLS=0; RUN_TESTS=1; RUN_FORMATS=1; RUN_CAPTURES=0; CAPTURES_ONLY=0
ONLY=""; SECTIONS=""; LOCALES="default,C"
declare -a EXTRA_TESTS=()
while [ $# -gt 0 ]; do
    case "$1" in
        --purge)        PURGE=1 ;;
        --keep-pools)   KEEP_POOLS=1 ;;
        --no-formats)   RUN_FORMATS=0 ;;
        --formats-only) RUN_TESTS=0 ;;
        --captures)      RUN_CAPTURES=1 ;;
        --captures-only) RUN_CAPTURES=1; RUN_TESTS=0; RUN_FORMATS=0; CAPTURES_ONLY=1 ;;
        --sections)      shift; SECTIONS="${1:-}" ;;
        --sections=*)    SECTIONS="${1#--sections=}" ;;
        --locales)       shift; LOCALES="${1:-}" ;;
        --locales=*)     LOCALES="${1#--locales=}" ;;
        --only)         shift; ONLY="${1:-}" ;;
        --only=*)       ONLY="${1#--only=}" ;;
        --extra)        shift; EXTRA_TESTS+=("${1:-}") ;;
        --extra=*)      EXTRA_TESTS+=("${1#--extra=}") ;;
        -h|--help)      grep '^#' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *) echo "Unknown option: $1" >&2; exit 2 ;;
    esac
    shift
done

declare -a TARGETS=()
for t in "${ALL_TARGETS[@]}"; do
    label="${t##*:}"; short="${label/./}"
    if [ -z "$ONLY" ] || [[ ",${ONLY}," == *",${short},"* ]] || [[ ",${ONLY}," == *",${label},"* ]]; then
        TARGETS+=("$t")
    fi
done
[ "${#TARGETS[@]}" -gt 0 ] || { echo "--only '$ONLY' selects nothing (use 21,22,24)" >&2; exit 2; }
for f in ${EXTRA_TESTS[@]+"${EXTRA_TESTS[@]}"}; do
    case "$f" in tests/*.py) ;; *) echo "--extra takes a repo-relative tests/*.py path, got '$f'" >&2; exit 2 ;; esac
done
case "$SECTIONS" in *[!A-Za-z0-9,]*) echo "--sections takes a comma list like M3,Z4 (M1 M2 M3 M4 EP M5 M6 M7 Z2 Z3 Z4 Z5 Z6 Z7)" >&2; exit 2 ;; esac
case "$LOCALES" in ""|*[!A-Za-z0-9,._@-]*) echo "--locales takes a comma list like default,C" >&2; exit 2 ;; esac

# ─────────────────────────────── Output helpers ─────────────────────────────
if [ -t 1 ]; then
    RED=$'\033[0;31m'; GREEN=$'\033[0;32m'; YELLOW=$'\033[0;33m'; BLUE=$'\033[0;34m'; BOLD=$'\033[1m'; NC=$'\033[0m'
else
    RED=; GREEN=; YELLOW=; BLUE=; BOLD=; NC=
fi
info() { echo "${BLUE}==>${NC} $*"; }
step() { echo "${BOLD}${BLUE}::${NC} ${BOLD}$*${NC}"; }
warn() { echo "${YELLOW}WARN:${NC} $*" >&2; }
die()  { echo "${RED}FATAL:${NC} $*" >&2; exit 1; }

# ─────────────────────────────── Multipass helpers ──────────────────────────
# Never send an exec's HOST stdout to /dev/null (the exec spins forever): discard in the guest, or pipe.
# A guest command that exits with status 4 hangs `multipass exec` forever (pytest's usage-error code is 4!):
# the guest runner remaps 4 -> 104 itself.
vm_exists()  { multipass info "$1" >/dev/null 2>&1; }
vm_running() { multipass info "$1" 2>/dev/null | awk -F': *' '/^State/{print $2}' | grep -qi running; }

ensure_vm() {
    local vm="$1" image="$2"
    if ! vm_exists "$vm"; then
        step "Launching VM $vm (Ubuntu $image)"
        multipass launch "$image" --name "$vm" --cpus "$VM_CPUS" --memory "$VM_MEM" --disk "$VM_DISK" \
            || die "failed to launch $vm"
    elif ! vm_running "$vm"; then
        step "Starting existing VM $vm"
        multipass start "$vm" || die "failed to start $vm"
    else
        info "VM $vm already running"
    fi
}

# put_file <host-file> <vm> <guest-path>: via `multipass transfer` (never piped stdin)
put_file() { multipass transfer "$1" "$2:$3" || die "transfer of $1 to $2:$3 failed"; }

# ─────────────────────────────── Guest scripts ──────────────────────────────
TMP_HOST="$(mktemp -d)"
trap 'rm -rf "$TMP_HOST"' EXIT
LOG_DIR="${MATRIX_LOG_DIR:-$(mktemp -d "${TMPDIR:-/tmp}/zfsb-matrix-logs.XXXXXX")}"
mkdir -p "$LOG_DIR" || die "cannot create $LOG_DIR"

# run.sh: executed as the login user (sudo inside). Modes: provision | pytest | formats | cleanup
cat > "$TMP_HOST/run.sh" <<'EOF_RUN'
#!/bin/bash
WORK=/var/tmp/zfsb-matrix
SRC=$WORK/src
VENV=$WORK/venv
mode="${1:-}"; shift || true

py_ok() { "$1" -c 'import sys; sys.exit(0 if sys.version_info >= (3, 11) else 1)' 2>/dev/null; }

pick_python() {
    local c
    for c in python3.13 python3.12 python3.11 python3; do
        if command -v "$c" >/dev/null 2>&1 && py_ok "$c"; then command -v "$c"; return 0; fi
    done
    return 1
}

provision() {
    set -e
    export DEBIAN_FRONTEND=noninteractive
    sudo mkdir -p "$WORK" && sudo chown "$(id -un)":"$(id -gn)" "$WORK"
    need=""
    command -v zfs  >/dev/null 2>&1 || need="$need zfsutils-linux"
    command -v curl >/dev/null 2>&1 || need="$need curl"
    dpkg -s python3-venv >/dev/null 2>&1 || need="$need python3-venv"
    dpkg -s python3-pip  >/dev/null 2>&1 || need="$need python3-pip"
    if [ -n "$need" ]; then
        sudo apt-get update -qq
        sudo apt-get install -y -qq $need
    fi
    PY="$(pick_python || true)"
    if [ -z "$PY" ]; then
        echo "distro python3 is below 3.11: trying python3.11 from the deadsnakes PPA (Launchpad is flaky: retried)"
        for attempt in 1 2 3; do
            if sudo apt-get install -y -qq software-properties-common \
               && sudo add-apt-repository -y ppa:deadsnakes/ppa \
               && sudo apt-get update -qq \
               && sudo apt-get install -y -qq -o Acquire::Retries=5 python3.11 python3.11-venv; then
                PY=python3.11; break
            fi
            echo "deadsnakes attempt $attempt failed"; sleep 10
        done
    fi
    if [ -z "$PY" ]; then
        echo "deadsnakes unavailable: falling back to a uv-managed python 3.11 (python-build-standalone)"
        python3 -m venv "$WORK/uvenv" && "$WORK/uvenv/bin/pip" install -q uv \
            && "$WORK/uvenv/bin/uv" python install 3.11 \
            && PY="$("$WORK/uvenv/bin/uv" python find 3.11)"
    fi
    [ -n "$PY" ] || { echo "no Python >= 3.11 could be installed" >&2; exit 1; }
    echo "python: $PY ($($PY --version 2>&1))"
    if [ -x "$VENV/bin/python" ] && ! py_ok "$VENV/bin/python"; then rm -rf "$VENV"; fi
    test -x "$VENV/bin/python" || "$PY" -m venv "$VENV"
    "$VENV/bin/pip" install -q --upgrade pip
    "$VENV/bin/pip" install -q pyyaml 'flask>=3' flask-sock 'simple-websocket>=1.1,<1.2' websocket-client \
        requests 'cryptography>=43' 'sqlalchemy>=2.0' 'alembic>=1.13' 'click>=8.1' \
        pytest pytest-mock pytest-cov pytest-xdist requests-mock
    rm -rf "$SRC" && mkdir -p "$SRC"
    tar -m -xf "$WORK/src.tar" -C "$SRC"
    rm -f "$WORK/src.tar"
    echo "provisioned: $("$VENV/bin/python" --version 2>&1), zfs $(zfs version 2>&1 | head -n1)"
}

run_pytest() {
    export LC_ALL=C.UTF-8
    echo "=== zfs version"; zfs version 2>&1; echo "=== zpool version"; zpool version 2>&1 || true
    echo "=== python"; "$VENV/bin/python" --version 2>&1
    echo "=== kernel"; uname -r
    # leftovers of an earlier, killed run
    for p in $(zpool list -H -o name 2>/dev/null | grep '^zfsbackup_pytest_'); do sudo zpool destroy -f "$p" 2>&1 || true; done
    rm -rf "$WORK/tmp"; mkdir -p "$WORK/tmp"
    cd "$SRC" || exit 1
    # addopts is cleared: pytest.ini's --cov/-m "not real_zfs" would otherwise be stacked on ours
    TMPDIR="$WORK/tmp" ZFSB_REAL_ZFS=1 PYTHONPATH="$SRC" "$VENV/bin/python" -m pytest \
        -o addopts= -p no:cacheprovider --basetemp="$WORK/tmp/pytest" \
        -m real_zfs -v --tb=short -rfEsx "$@"
    rc=$?
    echo "=== pytest exit code: $rc"
    for p in $(zpool list -H -o name 2>/dev/null | grep '^zfsbackup_pytest_'); do
        echo "leaked pool $p: destroying"; sudo zpool destroy -f "$p" 2>&1 || true
    done
    rm -rf "$WORK/tmp"
    return $rc
}

mode_formats() {
    sudo env FORMATS_WORK="$WORK" FORMATS_KEEP="${FORMATS_KEEP:-0}" bash "$WORK/formats.sh"
}

# captures: one pass per locale (CAPTURE_LOCALES, comma list; "default" = the login environment untouched), each on its
# own throwaway pool zfsbcap<d|c|...>; then one tarball (captures.tgz) for the host to fetch
mode_captures() {
    local tags="${CAPTURE_LOCALES:-default,C}" tag pool
    sudo rm -rf "$WORK/captures" "$WORK/captures.tgz"; mkdir -p "$WORK/captures"
    echo "=== zfs version"; zfs version 2>&1
    echo "=== login environment: LANG=${LANG:-<unset>} LC_ALL=${LC_ALL:-<unset>}"
    for tag in ${tags//,/ }; do
        case "$tag" in default) pool=zfsbcapd ;; C) pool=zfsbcapc ;; *) pool="zfsbcap_${tag//[^A-Za-z0-9]/}" ;; esac
        sudo zpool destroy -f "$pool" >/dev/null 2>&1 || true
        sudo env ${LANG:+LANG="$LANG"} ${LC_ALL:+LC_ALL="$LC_ALL"} CAP_WORK="$WORK" CAP_TAG="$tag" \
            CAP_OUT="$WORK/captures/$tag" CAP_POOL="$pool" CAP_SECTIONS="${CAPTURE_SECTIONS:-}" \
            CAP_KEEP="${FORMATS_KEEP:-0}" bash "$WORK/captures.sh"
    done
    sudo tar -C "$WORK" -czf "$WORK/captures.tgz" captures && sudo chown "$(id -u):$(id -g)" "$WORK/captures.tgz" \
        && echo "=== captures.tgz: $(du -h "$WORK/captures.tgz" | cut -f1)"
}

mode_cleanup() {
    sudo zpool destroy -f zfsbfmt >/dev/null 2>&1 || true
    sudo rm -f "$WORK/zfsbfmt.img"
    for p in $(zpool list -H -o name 2>/dev/null | grep '^zfsbcap'); do sudo zpool destroy -f "$p" >/dev/null 2>&1 || true; done
    sudo rm -f "$WORK"/zfsbcap*.img "$WORK/cap.key1" "$WORK/cap.key2"
    for p in $(zpool list -H -o name 2>/dev/null | grep '^zfsbackup_pytest_'); do sudo zpool destroy -f "$p" >/dev/null 2>&1 || true; done
}

case "$mode" in
    provision) provision; rc=$? ;;
    pytest)    run_pytest "$@"; rc=$? ;;
    formats)   mode_formats; rc=$? ;;
    captures)  mode_captures; rc=$? ;;
    cleanup)   mode_cleanup; rc=0 ;;
    *) echo "run.sh: unknown mode '$mode'" >&2; rc=2 ;;
esac
# `multipass exec` hangs forever on a guest exit status of 4 (pytest's usage error): remap it.
[ "$rc" = 4 ] && rc=104
exit "$rc"
EOF_RUN

# formats.sh: executed as root. Captures the real output of every zfs command whose text zfsbackup parses.
cat > "$TMP_HOST/formats.sh" <<'EOF_FORMATS'
#!/bin/bash
# Captures zfs output formats that libzfseasy / zfsbackup parse (plan item B4). Plain text on stdout.
export LC_ALL=C LANG=C
W="${FORMATS_WORK:-/var/tmp/zfsb-matrix}"; P=zfsbfmt; IMG="$W/$P.img"
O="$W/fmt.out"; E="$W/fmt.err"

# run <args...>: show the command, rc, and stdout / stderr SEPARATELY (cat -A: tab = ^I, end of line = $)
run() {
    echo "\$ $*"
    "$@" >"$O" 2>"$E"; local rc=$?
    echo "[rc=$rc]"
    echo "-- stdout"; cat -A "$O" | head -n "${MAXL:-60}"
    echo "-- stderr"; cat -A "$E" | head -n "${MAXL:-60}"
}
# runp <shell-pipeline>: same for a pipeline (rc of the whole `bash -c`)
runp() {
    echo "\$ $1"
    bash -c "$1" >"$O" 2>"$E"; local rc=$?
    echo "[rc=$rc]"
    echo "-- stdout"; cat -A "$O" | head -n "${MAXL:-60}"
    echo "-- stderr"; cat -A "$E" | head -n "${MAXL:-60}"
}
sect() { echo; echo "=================== $*"; }

zpool destroy -f $P >/dev/null 2>&1; rm -f "$IMG"
truncate -s 512m "$IMG" && zpool create -m "/$P" $P "$IMG" || { echo "cannot create pool $P"; exit 1; }

sect "versions"
run zfs version
run zpool version

# ---- fixture: src (incompressible data, a user property), snapshots a b c
zfs create -o compression=off $P/src
dd if=/dev/urandom of=/$P/src/f1 bs=1M count=24 status=none; zfs snapshot $P/src@a
dd if=/dev/urandom of=/$P/src/f2 bs=1M count=8 status=none;  zfs snapshot $P/src@b
dd if=/dev/urandom of=/$P/src/f3 bs=1M count=8 status=none;  zfs snapshot $P/src@c
zfs set zfsb:note=hello $P/src

sect "zfs list -j / zfs get -j (JSON output; OpenZFS 2.3+)"
MAXL=6 run zfs list -j $P/src      # (an unknown option prints the whole usage text: only the head is kept)
MAXL=12 run zfs get -j compression $P/src

sect "interrupted 'receive -s' -> receive_resume_token (stream truncated after 6 MB)"
runp "zfs send $P/src@a | head -c 6000000 | zfs receive -s -u $P/dst"
run zfs get -H -o property,value,source receive_resume_token $P/dst
TOKEN="$(zfs get -H -o value receive_resume_token $P/dst)"
echo "token length: ${#TOKEN}"

sect "zfs send -nv -t <token>   (the resume_info parse: which stream carries the contents block)"
run zfs send -nv -t "$TOKEN"

sect "zfs send -nv -t <token> after the token's target snapshot was destroyed (stale token)"
# a token for a snapshot we then remove: send a snapshot that exists only briefly
zfs snapshot $P/src@gone
runp "zfs send $P/src@gone | head -c 3000000 | zfs receive -s -u $P/dst2"
TOKEN2="$(zfs get -H -o value receive_resume_token $P/dst2)"
zfs destroy $P/src@gone
run zfs send -nv -t "$TOKEN2"

sect "zfs send -nv -t <corrupt token>"
run zfs send -nv -t "1-deadbeef-notatoken"

sect "zfs receive -A : with resumable state, then already clean, then no such dataset"
run zfs receive -A $P/dst
run zfs receive -A $P/dst
run zfs receive -A $P/nosuchdataset
run zfs receive -A $P/dst2
# the real daemon flow: the server dataset EXISTS (a full receive completed), then an incremental is interrupted.
# (The abort above removed dst/dst2 altogether: a dataset created by the interrupted receive itself is destroyed.)
runp "zfs send $P/src@a | zfs receive -u $P/dst3"
runp "zfs send -i @a $P/src@b | head -c 3000000 | zfs receive -s -u $P/dst3"
run zfs get -H -o property,value,source receive_resume_token $P/dst3
run zfs receive -A $P/dst3
run zfs receive -A $P/dst3
run zfs get -H -o property,value,source receive_resume_token $P/dst3
run zfs receive -A $P/src

sect "zfs destroy -v -p  (destroy / reclaim lines): dry run (-n) and real"
zfs create $P/dd
for s in x y z; do dd if=/dev/urandom of=/$P/dd/$s bs=1M count=1 status=none; zfs snapshot $P/dd@$s; done
run zfs destroy -n -v -p $P/dd@x
run zfs destroy -n -v -p $P/dd@x%y
run zfs destroy -n -v -p $P/dd@x,y
run zfs destroy -n -v -p $P/dd@x%y,z
run zfs destroy -v -p $P/dd@x,y
run zfs destroy -n -v -p -r $P/dd
run zfs destroy -v -p -r $P/dd
run zfs destroy -n -v -p $P/dd@nosuch

sect "'received' column of zfs get (a property that arrived with 'send -p' | receive)"
zfs snapshot $P/src@p1
zfs create -p $P/recvroot
runp "zfs send -p $P/src@p1 | zfs receive -u $P/recvroot/copy"
run zfs get -H -o name,property,value,received,source zfsb:note,compression $P/recvroot/copy
run zfs get -H -o name,property,value,received,source -s local,received zfsb:note $P/recvroot/copy
run zfs set zfsb:note=overridden $P/recvroot/copy
run zfs get -H -o name,property,value,received,source zfsb:note $P/recvroot/copy
run zfs get -H -o name,property,value,received,source zfsb:note $P/src
run zfs get -H -o name,property,value,received,source receive_resume_token $P/src

if [ "${FORMATS_KEEP:-0}" != 1 ]; then
    zpool destroy -f $P >/dev/null 2>&1; rm -f "$IMG" "$O" "$E"
fi
EOF_FORMATS
# captures.sh: executed as root. The SP spike (plan items M1-M6, Z1-Z8); see the CAPTURES note in the header.
cat > "$TMP_HOST/captures.sh" <<'EOF_CAPTURES'
#!/bin/bash
# captures.sh: executed as root INSIDE a matrix VM (plan item SP: captures M1-M6 and Z1-Z8).
#
# Every capture is a RECORD: <section dir>/<id>.{cmd,rc,stdout,stderr[,pipestatus,send.stderr]} holds the exact
# bytes (mechanically extractable, M7), and <OUT>/<section>.txt concatenates a section's records in order with
# fixed markers:
#     === CASE <section>/<id>
#     --- cmd / --- rc / --- pipestatus / --- stdout / --- stderr / --- send.stderr
#     === END
# plus "#note:" lines for remarks. One pass runs under ONE locale tag: CAP_TAG=default (the login environment,
# untouched) or CAP_TAG=C (LC_ALL=C LANG=C). The pool is throwaway and file-backed (CAP_POOL, unique per pass).
#
# Env: CAP_OUT (required), CAP_TAG, CAP_POOL, CAP_WORK, CAP_SECTIONS (comma list; empty = all), CAP_KEEP=1.
# Everything uses keyfiles (keylocation=file://); nothing prompts (stdin is /dev/null).

export PATH="/usr/sbin:/sbin:$PATH"
W="${CAP_WORK:-/var/tmp/zfsb-matrix}"
TAG="${CAP_TAG:-default}"
OUT="${CAP_OUT:?CAP_OUT is required}"
P="${CAP_POOL:-zfsbcapd}"
IMG="$W/$P.img"
KEY1="$W/cap.key1"; KEY2="$W/cap.key2"
WANT="${CAP_SECTIONS:-}"
CAP_T="${CAP_TIMEOUT:-240}"
case "$TAG" in
    default) ;;                                   # the login environment, untouched
    C)       export LC_ALL=C LANG=C ;;
    *)       export LC_ALL="$TAG" LANG="$TAG" ;;  # any other locale name (e.g. --locales default,C,en_US.utf8)
esac

log() { echo "[$(date +%H:%M:%S)] [$TAG] $*"; }
mkdir -p "$OUT"; : > "$OUT/SETUP-ERRORS.log"

# ---------------------------------------------------------------- recording helpers
SEC=""; SD=""; SECDESC=""; R=""; NOTEN=0; LAST=""

# st <cmd...>: an UNRECORDED setup command; a failure is logged to SETUP-ERRORS.log (and returned)
st() {
    "$@" >"$W/st.out" 2>"$W/st.err" </dev/null && return 0
    echo "SETUP FAILED [$SEC]: $* :: $(tr '\n' ' ' <"$W/st.err")" >> "$OUT/SETUP-ERRORS.log"
    return 1
}

# cap <id> <cmd...>: run and record stdout, stderr and rc separately
cap() {
    local id="$1"; shift
    LAST="$SD/$id"
    printf '%s\n' "$*" > "$LAST.cmd"
    timeout "$CAP_T" "$@" </dev/null >"$LAST.stdout" 2>"$LAST.stderr"
    echo $? > "$LAST.rc"
    echo "$id" >> "$SD/INDEX"
}

# capsh <id> '<shell snippet>': same for a snippet that needs shell syntax (pipefail: the rc is the first failing stage's)
capsh() {
    local id="$1"; shift
    LAST="$SD/$id"
    printf '%s\n' "$1" > "$LAST.cmd"
    timeout "$CAP_T" bash -c "set -o pipefail; $1" </dev/null >"$LAST.stdout" 2>"$LAST.stderr"
    echo $? > "$LAST.rc"
    echo "$id" >> "$SD/INDEX"
}

# cappipe <id> <limit-bytes|0> '<sender>' '<receiver>': sender | [head -c limit |] receiver.
# stdout/stderr/rc are the RECEIVER's; the sender's stderr goes to <id>.send.stderr; <id>.pipestatus lists every stage.
cappipe() {
    local id="$1" lim="$2" snd="$3" rcv="$4" mid=" | "
    LAST="$SD/$id"
    [ "$lim" -gt 0 ] && mid=" | head -c $lim | "
    printf '%s%s%s\n' "$snd" "$mid" "$rcv" > "$LAST.cmd"
    timeout "$CAP_T" bash -c '
        snd=$1; rcv=$2; lim=$3; base=$4
        if [ "$lim" -gt 0 ]; then
            bash -c "$snd" 2>"$base.send.stderr" </dev/null | head -c "$lim" | bash -c "$rcv" >"$base.stdout" 2>"$base.stderr"
        else
            bash -c "$snd" 2>"$base.send.stderr" </dev/null | bash -c "$rcv" >"$base.stdout" 2>"$base.stderr"
        fi
        echo "${PIPESTATUS[*]}" >"$base.pipestatus"' _ "$snd" "$rcv" "$lim" "$LAST"
    awk '{print $NF}' "$LAST.pipestatus" > "$LAST.rc" 2>/dev/null || echo "?" > "$LAST.rc"
    echo "$id" >> "$SD/INDEX"
}

note() { NOTEN=$((NOTEN+1)); local f; f=$(printf 'NOTE.%03d' "$NOTEN"); printf '%s\n' "$*" > "$SD/$f"; echo "$f" >> "$SD/INDEX"; }

out() { cat "${LAST}.stdout" 2>/dev/null; }                 # the last record's stdout
tok() { zfs get -H -o value receive_resume_token "$1" 2>/dev/null | head -n1; }

section() {
    SEC="$1"; SECDESC="$2"; SD="$OUT/$1"; NOTEN=0
    rm -rf "$SD"; mkdir -p "$SD"; : > "$SD/INDEX"
    R="$P/$SEC"
    log "section $SEC: $SECDESC"
    st zfs create "$R"
}

end_section() {
    local f="$OUT/$SEC.txt" id
    {
        echo "##### SECTION $SEC ($SECDESC)  tag=$TAG  $(zfs version 2>&1 | head -n1)"
        while read -r id; do
            case "$id" in
                NOTE.*) echo "#note: $(cat "$SD/$id")"; continue ;;
            esac
            echo "=== CASE $SEC/$id"
            echo "--- cmd";    cat "$SD/$id.cmd"
            echo "--- rc";     cat "$SD/$id.rc"
            [ -f "$SD/$id.pipestatus" ] && { echo "--- pipestatus"; cat "$SD/$id.pipestatus"; }
            echo "--- stdout"; cat "$SD/$id.stdout"
            echo "--- stderr"; cat "$SD/$id.stderr"
            [ -s "$SD/$id.send.stderr" ] && { echo "--- send.stderr"; cat "$SD/$id.send.stderr"; }
            echo "=== END"
        done < "$SD/INDEX"
    } > "$f"
    rm -f "$SD/INDEX.sorted"
}

want() { [ -z "$WANT" ] || [[ ",$WANT," == *",$1,"* ]]; }

# ---------------------------------------------------------------- fixtures
rnd() { dd if=/dev/urandom of="$1" bs=1M count="$2" status=none; }

# mk_src <ds> <recordsize> <compression> <enc 0|1> <small|large>: snapshots @a (full) and @b (incremental data)
mk_src() {
    local ds="$1" rs="$2" comp="$3" enc="$4" sz="$5" o m
    o="-o recordsize=$rs -o compression=$comp"
    [ "$enc" = 1 ] && o="$o -o encryption=aes-256-gcm -o keyformat=passphrase -o keylocation=file://$KEY1"
    st zfs create -p $o "$ds" || return 1
    m="/$ds"
    if [ "$sz" = large ]; then rnd "$m/f1" 8; seq 1 1000000 > "$m/f2"; else rnd "$m/f1" 2; seq 1 300000 > "$m/f2"; fi
    echo "tiny file" > "$m/tiny"            # a candidate for an embedded block under lz4
    st zfs snapshot "$ds@a"
    if [ "$sz" = large ]; then rnd "$m/f3" 6; else rnd "$m/f3" 2; fi
    echo "tiny two" > "$m/tiny2"
    st zfs snapshot "$ds@b"
}

# mk_enc <ds> [keyfile]: an encrypted (passphrase, keyfile) fs with 3 x 2 MB of random data, snapshots @a @b @c
mk_enc() {
    local ds="$1" key="${2:-$KEY1}" i
    st zfs create -p -o recordsize=128K -o compression=lz4 -o encryption=aes-256-gcm -o keyformat=passphrase \
        -o keylocation=file://$key "$ds" || return 1
    for i in a b c; do rnd "/$ds/f_$i" 2; st zfs snapshot "$ds@$i"; done
}
mk_plain() {
    local ds="$1" i
    st zfs create -p -o recordsize=128K -o compression=lz4 "$ds" || return 1
    for i in a b c; do rnd "/$ds/f_$i" 2; st zfs snapshot "$ds@$i"; done
}
# mk_srv: under $R, srv (unencrypted target) and esrv (ENCRYPTED target, key loaded), both mountpoint=none
mk_srv() {
    st zfs create -o mountpoint=none "$R/srv"
    st zfs create -o mountpoint=none -o encryption=aes-256-gcm -o keyformat=passphrase -o keylocation=file://$KEY2 "$R/esrv"
}
unload() { zfs unmount "$1" >/dev/null 2>&1; zfs unload-key "$1" >/dev/null 2>&1; }

OWNCFG="eyJ2IjoxLCJkYXRhc2V0IjoicG9vbC9kYXRhIiwiZnJlcXVlbmN5IjoiMWgifQ=="
OWN="-o org.zfsbackup:config=$OWNCFG -o org.zfsbackup:client_id=client-1 -o org.zfsbackup:source_dataset=pool/data"
OWN2="-o org.zfsbackup:config=ZGlmZmVyZW50 -o org.zfsbackup:client_id=client-2 -o org.zfsbackup:source_dataset=pool/other"
PROPS_OWN="org.zfsbackup:config,org.zfsbackup:client_id,org.zfsbackup:source_dataset"
ENCP="encryption,encryptionroot,keystatus"
RF="-F -s -u"                              # the flags api.py's receive uses today
# props <id> <ds> [extra props]: user/ownership props + encryption view + resume token, with the received column
props() { cap "$1" zfs get -H -o name,property,value,received,source "$PROPS_OWN,$ENCP,canmount,readonly,volmode,receive_resume_token${3:+,$3}" "$2"; }
snaps() { cap "$1" zfs list -H -t snapshot -o name,guid,createtxg -r "$2"; }

# ---------------------------------------------------------------- pool
zpool destroy -f "$P" >/dev/null 2>&1; rm -f "$IMG"
printf 'zfsb-cap-passphrase-one' > "$KEY1"; printf 'zfsb-cap-passphrase-two' > "$KEY2"; chmod 600 "$KEY1" "$KEY2"
truncate -s "${CAP_POOL_SIZE:-3g}" "$IMG" && zpool create -f -m "/$P" "$P" "$IMG" || { echo "cannot create pool $P"; exit 1; }
cleanup_pool() {
    pkill -x sleep >/dev/null 2>&1
    rm -f "$W/st.out" "$W/st.err"
    if [ "${CAP_KEEP:-0}" != 1 ]; then zpool destroy -f "$P" >/dev/null 2>&1; rm -f "$IMG" "$KEY1" "$KEY2"; fi
}
trap cleanup_pool EXIT

{
    echo "tag: $TAG"; echo "date: $(date -u +%FT%TZ)"; echo "uname: $(uname -r)"
    echo "--- zfs version"; zfs version 2>&1
    echo "--- zpool version"; zpool version 2>&1
    echo "--- environment (LANG/LC_*/LANGUAGE)"; env | grep -E '^(LANG|LC_|LANGUAGE)' | sort
    echo "--- locale"; locale 2>&1
    echo "--- zfs message catalogues installed"; ls /usr/share/locale/*/LC_MESSAGES/zfs* /usr/share/locale/*/LC_MESSAGES/zpool* 2>&1 | head -n 5
    echo "--- pool features at creation"; zpool get -H -o property,value all "$P" | grep -E 'feature@(large_blocks|embedded_data|encryption|zstd_compress|extensible_dataset|bookmark_v2|bookmark_written|livelist|device_rebuild)'
} > "$OUT/ENV.txt" 2>&1

# ================================================================ M1: resume-token flags per send-flag set
sec_M1() {
    section M1 "resume-token flags per send flag set (recordsize 128K/1M, lz4/off, full + incremental)"
    local FN=(none L c e Lc w) FA=("" "-L" "-c" "-e" "-L -c" "-w")
    local rs comp enc sz i src fl nm id tk rf ri step
    st zfs create "$P/M1r"
    m1_case() { # <src> <flags> <id>
        local src="$1" fl="$2" id="$3" r tk
        local rf="$P/M1r/${id//./_}_f" ri="$P/M1r/${id//./_}_i"
        log "M1 $id (send flags: '${fl}')"
        # ---- full stream interrupted
        cappipe "$id.full.1_recv_cut" 1000000 "zfs send $fl $src@a" "zfs receive -s -u $rf"
        cap "$id.full.2_token_prop" zfs get -H -o name,property,value,source receive_resume_token "$rf"
        tk="$(tok "$rf")"
        if [ -n "$tk" ] && [ "$tk" != "-" ]; then
            echo "$tk" > "$SD/$id.full.token"
            cap "$id.full.3_send_nv_t" zfs send -nv -t "$tk"
            cappipe "$id.full.4_resume" 0 "zfs send -t $tk" "zfs receive -s -u $rf"
            cap "$id.full.5_after" zfs get -H -o name,property,value receive_resume_token "$rf"
            snaps "$id.full.6_snaps" "$rf"
        else
            note "$id full: NO resume token after the cut (token property='$tk')"
        fi
        # ---- incremental stream interrupted (base received completely with the same flags)
        cappipe "$id.incr.0_base" 0 "zfs send $fl $src@a" "zfs receive -u $ri"
        cappipe "$id.incr.1_recv_cut" 500000 "zfs send $fl -i $src@a $src@b" "zfs receive -s -u $ri"
        cap "$id.incr.2_token_prop" zfs get -H -o name,property,value,source receive_resume_token "$ri"
        tk="$(tok "$ri")"
        if [ -n "$tk" ] && [ "$tk" != "-" ]; then
            echo "$tk" > "$SD/$id.incr.token"
            cap "$id.incr.3_send_nv_t" zfs send -nv -t "$tk"
            cappipe "$id.incr.4_resume" 0 "zfs send -t $tk" "zfs receive -s -u $ri"
            cap "$id.incr.5_after" zfs get -H -o name,property,value receive_resume_token "$ri"
            snaps "$id.incr.6_snaps" "$ri"
        else
            note "$id incr: NO resume token after the cut (token property='$tk')"
        fi
        st zfs destroy -r -f "$rf"; st zfs destroy -r -f "$ri"
    }
    # the 128K datasets first: the pool's large_blocks feature is still 'enabled', not 'active'
    for step in 128K:small 1M:large; do
        rs="${step%%:*}"; sz="${step##*:}"
        capsh "pool_features_before_$rs" "zpool get -H -o property,value feature@large_blocks,feature@embedded_data,feature@encryption $P"
        for comp in lz4 off; do
            src="$P/M1/s_${rs}_${comp}_plain"
            mk_src "$src" "$rs" "$comp" 0 "$sz"
            snaps "src_${rs}_${comp}_plain.snaps" "$src"
            cap "src_${rs}_${comp}_plain.props" zfs get -H -o name,property,value recordsize,compression "$src"
            for i in 0 1 2 3 4 5; do
                m1_case "$src" "${FA[$i]}" "${rs}_${comp}_plain_${FN[$i]}"
            done
            # encrypted source: -w (the raw set), plus the frozen plain pair for the 2 extreme combos
            src="$P/M1/s_${rs}_${comp}_enc"
            mk_src "$src" "$rs" "$comp" 1 "$sz"
            snaps "src_${rs}_${comp}_enc.snaps" "$src"
            m1_case "$src" "-w" "${rs}_${comp}_enc_w"
            if [ "${rs}_${comp}" = 128K_off ] || [ "${rs}_${comp}" = 1M_lz4 ]; then
                m1_case "$src" "" "${rs}_${comp}_enc_none"
                m1_case "$src" "-L -c" "${rs}_${comp}_enc_Lc"
            fi
            st zfs destroy -r -f "$P/M1/s_${rs}_${comp}_plain"; st zfs destroy -r -f "$P/M1/s_${rs}_${comp}_enc"
        done
        capsh "pool_features_after_$rs" "zpool get -H -o property,value feature@large_blocks,feature@embedded_data,feature@encryption $P"
    done
    end_section
}

# ================================================================ M2: stale tokens
sec_M2() {
    section M2 "stale resume tokens (incremental source destroyed, target destroyed/recreated, sender renamed)"
    st zfs create "$R/s"; st zfs create "$R/r"
    local SRC T TK kind
    m2_mk() { # <name> <full|incr>  -> SRC T TK
        SRC="$R/s/$1"; T="$R/r/$1"; kind="$2"
        mk_src "$SRC" 128K lz4 0 small
        if [ "$kind" = full ]; then
            cappipe "$1.0_recv_cut" 1000000 "zfs send -L -c $SRC@a" "zfs receive -s -u $T"
        else
            cappipe "$1.0a_base" 0 "zfs send -L -c $SRC@a" "zfs receive -u $T"
            cappipe "$1.0_recv_cut" 500000 "zfs send -L -c -i $SRC@a $SRC@b" "zfs receive -s -u $T"
        fi
        TK="$(tok "$T")"
        cap "$1.1_nv_t_control" zfs send -nv -t "$TK"
    }
    m2_try() { # <name>: what the client/server would do next with the stale token
        cap "$1.8_nv_t" zfs send -nv -t "$TK"
        cappipe "$1.9_resume_try" 0 "zfs send -t $TK" "zfs receive -s -u $T"
        cap "$1.9a_token_after" zfs get -H -o name,property,value receive_resume_token "$T"
        cap "$1.9b_abort" zfs receive -A "$T"
    }
    # --- incremental token
    m2_mk i_from_destroyed incr;   cap i_from_destroyed.2_destroy_sender_from zfs destroy "$SRC@a"; m2_try i_from_destroyed
    m2_mk i_to_destroyed incr;     cap i_to_destroyed.2_destroy_sender_to zfs destroy "$SRC@b"; m2_try i_to_destroyed
    m2_mk i_to_recreated incr
    cap i_to_recreated.2_destroy_sender_to zfs destroy "$SRC@b"
    echo "new content" > "/$SRC/f_new"; rnd "/$SRC/f_new2" 1
    cap i_to_recreated.3_recreate_sender_to zfs snapshot "$SRC@b"
    snaps i_to_recreated.4_sender_snaps "$SRC"
    m2_try i_to_recreated
    m2_mk i_from_recreated incr
    cap i_from_recreated.2_destroy_sender_from zfs destroy "$SRC@a"
    cap i_from_recreated.3_recreate_sender_from zfs snapshot "$SRC@a"
    snaps i_from_recreated.4_sender_snaps "$SRC"
    m2_try i_from_recreated
    m2_mk i_sender_renamed incr;   cap i_sender_renamed.2_rename_sender zfs rename "$SRC" "${SRC}_renamed"; m2_try i_sender_renamed
    m2_mk i_sender_moved incr;     st zfs create "$R/moved"; cap i_sender_moved.2_rename_sender_elsewhere zfs rename "$SRC" "$R/moved/x"; m2_try i_sender_moved
    m2_mk i_receiver_base_gone incr; cap i_receiver_base_gone.2_destroy_receiver_base zfs destroy "$T@a"; m2_try i_receiver_base_gone
    m2_mk i_receiver_replaced incr
    cap i_receiver_replaced.2_destroy_receiver zfs destroy -r "$T"
    cappipe i_receiver_replaced.3_recreate_receiver_full 0 "zfs send -L -c $SRC@a" "zfs receive -u $T"
    cap i_receiver_replaced.3a_token_prop zfs get -H -o name,property,value receive_resume_token "$T"
    m2_try i_receiver_replaced
    # --- full token
    m2_mk f_to_destroyed full;     cap f_to_destroyed.2_destroy_sender_to zfs destroy "$SRC@a"; m2_try f_to_destroyed
    m2_mk f_to_recreated full
    cap f_to_recreated.2_destroy_sender_to zfs destroy "$SRC@a"
    cap f_to_recreated.2a_destroy_sender_b zfs destroy "$SRC@b"
    rnd "/$SRC/f_new2" 1
    cap f_to_recreated.3_recreate_sender_to zfs snapshot "$SRC@a"
    snaps f_to_recreated.4_sender_snaps "$SRC"
    m2_try f_to_recreated
    m2_mk f_sender_renamed full;   cap f_sender_renamed.2_rename_sender zfs rename "$SRC" "${SRC}_renamed"; m2_try f_sender_renamed
    m2_mk f_receiver_replaced full
    cap f_receiver_replaced.2_abort zfs receive -A "$T"
    cappipe f_receiver_replaced.3_recreate_receiver_full 0 "zfs send -L -c $SRC@a" "zfs receive -u $T"
    m2_try f_receiver_replaced
    end_section
}

# ================================================================ M3 / Z8: destroy failures
sec_M3() {
    section M3_Z8 "destroy failures: held, cloned, busy (send in progress), recursive child hold; batch vs single; dry runs"
    local H C B RP
    mk_snaps() { # <ds> <n> <MB per snapshot>
        local i; st zfs create "$1" || return 1
        for i in $(seq 1 "$2"); do rnd "/$1/f$i" "$3"; st zfs snapshot "$1@s$i"; done
    }
    listing() { cap "$1" zfs list -H -t all -o name,userrefs,clones -r "$2"; }
    # ---- held snapshot
    H="$R/held"; mk_snaps "$H" 4 2; st zfs hold keep "$H@s2"
    cap held.00_holds zfs holds -H "$H@s2"
    listing held.01_list_start "$H"
    cap held.10_dry_batch  zfs destroy -n -v -p "$H@s1,s2,s3"
    cap held.11_dry_single zfs destroy -n -v -p "$H@s2"
    cap held.12_dry_range  zfs destroy -n -v -p "$H@s1%s3"
    cap held.20_batch      zfs destroy -v -p "$H@s1,s2,s3"
    listing held.21_list_after_batch "$H"
    cap held.30_single     zfs destroy -v -p "$H@s2"
    listing held.31_list_after_single "$H"
    cap held.40_range      zfs destroy -v -p "$H@s1%s3"
    listing held.41_list_after_range "$H"
    cap held.50_control_batch_without_held zfs destroy -v -p "$H@s1,s3"
    listing held.51_list_after_control "$H"
    cap held.60_dry_control zfs destroy -n -v -p "$H@s4"
    # ---- cloned snapshot
    C="$R/cloned"; mk_snaps "$C" 4 2; st zfs clone "$C@s2" "$R/clone1"
    listing cloned.01_list_start "$C"
    cap cloned.10_dry_batch  zfs destroy -n -v -p "$C@s1,s2,s3"
    cap cloned.11_dry_single zfs destroy -n -v -p "$C@s2"
    cap cloned.12_dry_range  zfs destroy -n -v -p "$C@s1%s3"
    cap cloned.20_batch      zfs destroy -v -p "$C@s1,s2,s3"
    listing cloned.21_list_after_batch "$C"
    cap cloned.30_single     zfs destroy -v -p "$C@s2"
    listing cloned.31_list_after_single "$C"
    cap cloned.40_range      zfs destroy -v -p "$C@s1%s3"
    listing cloned.41_list_after_range "$C"
    cap cloned.50_control_batch_without_clone zfs destroy -v -p "$C@s1,s3"
    listing cloned.51_list_after_control "$C"
    st zfs clone "$C@s4" "$R/clone2"; st zfs clone "$C@s4" "$R/clone3"
    cap cloned.60_two_clones_single zfs destroy -v -p "$C@s4"
    cap cloned.61_two_clones_dry    zfs destroy -n -v -p "$C@s4"
    # ---- busy snapshot (a zfs send in progress)
    B="$R/busy"; mk_snaps "$B" 4 3
    ( zfs send "$B@s4" | sleep 60 ) </dev/null >/dev/null 2>"$SD/busy.sender.stderr" &
    sleep 3
    capsh busy.00_sender_running 'pgrep -a -x zfs; pgrep -a -x sleep'
    listing busy.01_list_start "$B"
    cap busy.10_dry_batch  zfs destroy -n -v -p "$B@s3,s4"
    cap busy.11_dry_single zfs destroy -n -v -p "$B@s4"
    cap busy.20_batch      zfs destroy -v -p "$B@s3,s4"
    listing busy.21_list_after_batch "$B"
    cap busy.30_single     zfs destroy -v -p "$B@s4"
    listing busy.31_list_after_single "$B"
    cap busy.40_range      zfs destroy -v -p "$B@s3%s4"
    # an idle sibling is NOT blocked by the send of s4
    cap busy.50_control_other_snapshot zfs destroy -v -p "$B@s1"
    pkill -x sleep >/dev/null 2>&1; wait; sleep 1
    capsh busy.60_sender_gone 'pgrep -a -x zfs; pgrep -a -x sleep; true'
    cap busy.61_batch_after_sender_gone zfs destroy -v -p "$B@s3,s4"
    listing busy.62_list_after "$B"
    # ---- recursive destroy with a hold on a CHILD's snapshot
    RP="$R/rec"; st zfs create "$RP"; st zfs create "$RP/c"; rnd "/$RP/f" 1; rnd "/$RP/c/f" 1
    st zfs snapshot -r "$RP@t1"; rnd "/$RP/g" 1; st zfs snapshot -r "$RP@t2"; rnd "/$RP/h" 1; st zfs snapshot -r "$RP@t3"
    st zfs hold keep "$RP/c@t1"
    listing rec.01_list_start "$RP"
    cap rec.10_dry_r        zfs destroy -n -v -p -r "$RP@t1"
    cap rec.11_dry_r_batch  zfs destroy -n -v -p -r "$RP@t1,t2"
    cap rec.20_r            zfs destroy -v -p -r "$RP@t1"
    listing rec.21_list_after_r "$RP"
    cap rec.30_r_batch      zfs destroy -v -p -r "$RP@t1,t2"
    listing rec.31_list_after_r_batch "$RP"
    cap rec.40_child_single zfs destroy -v -p "$RP/c@t1"
    cap rec.50_parent_only_nonrecursive zfs destroy -v -p "$RP@t1"
    listing rec.51_list_after_parent_only "$RP"
    cap rec.60_control_r_no_hold zfs destroy -v -p -r "$RP@t3"
    listing rec.61_list_after_control "$RP"
    end_section
}

# ================================================================ M4: the listing with createtxg, guid, userrefs, clones
sec_M4() {
    section M4 "zfs list -H -r -t snapshot -o name,type,createtxg,guid,userrefs,clones (0/1/2 clones, holds, equal txg, -s createtxg)"
    local L="$R/l" i
    local LC=(-H -r -t snapshot -o name,type,createtxg,guid,userrefs,clones)
    st zfs create "$L"; st zfs create "$L/child"
    for i in 1 2 3; do rnd "/$L/f$i" 1; rnd "/$L/child/f$i" 1; st zfs snapshot -r "$L@s$i"; done
    cap 00_noclones           zfs list "${LC[@]}" "$L"
    cap 01_noclones_sorted    zfs list "${LC[@]}" -s createtxg "$L"
    cap 02_noclones_sorted_rev zfs list "${LC[@]}" -S createtxg "$L"
    cap 03_all_types          zfs list -H -r -t all -o name,type,createtxg,guid,userrefs,clones "$L"
    cap 04_noclones_parseable zfs list -H -p -r -t snapshot -o name,type,createtxg,guid,userrefs,clones "$L"
    st zfs clone "$L@s2" "$R/c1"
    cap 10_one_clone          zfs list "${LC[@]}" "$L"
    st zfs clone "$L@s2" "$R/c2"
    cap 20_two_clones_same_snapshot zfs list "${LC[@]}" "$L"
    st zfs clone "$L@s3" "$R/c3"
    cap 30_three_clones_two_snapshots zfs list "${LC[@]}" -s createtxg "$L"
    cap 31_clone_origin zfs list -H -o name,origin "$R/c1" "$R/c2" "$R/c3"
    st zfs hold a "$L@s1"; st zfs hold b "$L@s1"
    cap 40_two_holds          zfs list "${LC[@]}" "$L"
    cap 41_holds              zfs holds -H "$L@s1"
    # equal txg: several snapshots from ONE command
    cap 49_two_snapshots_one_fs_one_command zfs snapshot "$L@e1" "$L@e2"
    st zfs snapshot "$L@e1" "$L/child@e1"
    cap 50_equal_txg_parent_child zfs list "${LC[@]}" -s createtxg "$L"
    st zfs create "$R/l2"; rnd "/$R/l2/f" 1
    st zfs snapshot "$L@x1" "$R/l2@x1"
    cap 51_equal_txg_two_datasets zfs list -H -t snapshot -o name,createtxg -s createtxg "$L@x1" "$R/l2@x1"
    cap 60_filesystem_rows    zfs list -H -t filesystem -o name,type,createtxg,guid,userrefs,clones "$L" "$R/c1"
    cap 61_sorted_by_name_for_comparison zfs list "${LC[@]}" -s name "$L"
    end_section
}

# ================================================================ EP: encryption properties on fs and snapshot
sec_EP() {
    section EP "encryption, keystatus, encryptionroot, keyformat, keylocation on fs and snapshot, before/after unload-key"
    local E="$R/e" Pl="$R/p" cols="encryption,keystatus,encryptionroot,keyformat,keylocation"
    mk_enc "$E"; mk_plain "$Pl"
    cap 00_fs_loaded        zfs get -H -o name,property,value,source "$cols" "$E"
    cap 01_snap_loaded      zfs get -H -o name,property,value,source "$cols" "$E@a"
    cap 02_list_loaded      zfs list -H -t all -o name,encryption,keystatus,encryptionroot,keyformat,keylocation -r "$E"
    cap 10_plain_fs         zfs get -H -o name,property,value,source "$cols" "$Pl"
    cap 11_plain_snap       zfs get -H -o name,property,value,source "$cols" "$Pl@a"
    cap 12_plain_keystatus_value zfs get -H -o value keystatus "$Pl"
    cap 13_plain_list       zfs list -H -t all -o name,encryption,keystatus,encryptionroot,keyformat,keylocation -r "$Pl"
    cap 20_unload_mounted   zfs unload-key "$E"
    cap 21_unmount          zfs unmount "$E"
    cap 22_unload           zfs unload-key "$E"
    cap 23_unload_again     zfs unload-key "$E"
    cap 30_fs_unloaded      zfs get -H -o name,property,value,source "$cols" "$E"
    cap 31_snap_unloaded    zfs get -H -o name,property,value,source "$cols" "$E@a"
    cap 32_list_unloaded    zfs list -H -t all -o name,encryption,keystatus,encryptionroot,keyformat,keylocation -r "$E"
    cap 33_keystatus_value  zfs get -H -o value keystatus "$E"
    cap 40_load_again       zfs load-key "$E"
    cap 41_fs_loaded_again  zfs get -H -o name,property,value,source keystatus "$E"
    cap 42_pbkdf2           zfs get -H -o name,property,value,source pbkdf2iters,pbkdf2salt "$E"
    end_section
}

# ================================================================ M5 / Z1: raw receive, -F onto pre-created datasets, encrypted targets
sec_M5() {
    section M5_Z1 "raw receive: new dataset, -o props, full -F onto pre-created (unencrypted / under encrypted target), incremental, resume, -o encryption"
    local ES="$R/cs/enc" PS="$R/cs/plain" tk
    st zfs create "$R/cs"; mk_srv
    mk_enc "$ES"; mk_plain "$PS"
    st zfs set org.x:z=srcvalue "$ES"; st zfs set org.x:y=srcy "$ES"; st zfs snapshot "$ES@p"
    cap 00_src_props zfs get -H -o name,property,value,source "$ENCP,keylocation" "$ES" "$PS"
    # ---- raw full into a NON-existent leaf under an unencrypted parent
    cappipe 10_raw_new_plain_opts 0 "zfs send -w $ES@a" "zfs receive -s -u $R/srv/n0"
    props 10a_props "$R/srv/n0"
    cappipe 11_raw_new_o_user 0 "zfs send -w $ES@a" "zfs receive -s -u -o org.x:y=v $R/srv/n1"
    props 11a_props "$R/srv/n1" org.x:y
    cappipe 12_raw_new_o_canmount 0 "zfs send -w $ES@a" "zfs receive -s -u -o canmount=noauto $R/srv/n2"
    props 12a_props "$R/srv/n2"
    cappipe 13_raw_new_o_readonly 0 "zfs send -w $ES@a" "zfs receive -s -u -o readonly=on $R/srv/n3"
    props 13a_props "$R/srv/n3"
    cappipe 14_raw_new_all_three 0 "zfs send -w $ES@a" "zfs receive -s -u -o org.x:y=v -o canmount=noauto -o readonly=on $R/srv/n4"
    props 14a_props "$R/srv/n4" org.x:y
    cappipe 15_raw_p_stream_with_x 0 "zfs send -w -p $ES@p" "zfs receive -s -u -x org.x:z $R/srv/n5"
    props 15a_props "$R/srv/n5" org.x:y,org.x:z
    cappipe 16_raw_p_stream_with_o_and_x 0 "zfs send -w -p $ES@p" "zfs receive -s -u -o org.x:y=v -x org.x:z $R/srv/n6"
    props 16a_props "$R/srv/n6" org.x:y,org.x:z
    cappipe 17_raw_p_stream_without_x 0 "zfs send -w -p $ES@p" "zfs receive -s -u $R/srv/n7"
    props 17a_props "$R/srv/n7" org.x:y,org.x:z
    cappipe 18_raw_new_daemon_flags 0 "zfs send -w $ES@a" "zfs receive $RF $OWN $R/srv/n8"
    props 18a_props "$R/srv/n8"
    cappipe 19_raw_new_two_levels_below_parent 0 "zfs send -w $ES@a" "zfs receive -s -u $R/srv/missing/n9"
    cappipe 19b_raw_new_two_levels_with_p 0 "zfs send -w $ES@a" "zfs receive -s -u -p $R/srv/missing2/n9"
    cap 19c_list_srv zfs list -H -t all -o name,type -r "$R/srv"
    # ---- Z1: a full raw with -F onto a pre-created UNENCRYPTED dataset
    st zfs create "$R/srv/pre1"
    cappipe 20_raw_F_onto_precreated_unenc 0 "zfs send -w $ES@a" "zfs receive $RF $R/srv/pre1"
    props 20a_props "$R/srv/pre1"
    cap 20b_list zfs list -H -t all -o name,type,encryption -r "$R/srv/pre1"
    st zfs create "$R/srv/pre2"
    cappipe 21_raw_noF_onto_precreated_unenc 0 "zfs send -w $ES@a" "zfs receive -s -u $R/srv/pre2"
    st zfs create "$R/srv/pre3"
    cappipe 22_raw_F_onto_precreated_unenc_with_own_props 0 "zfs send -w $ES@a" "zfs receive $RF $OWN $R/srv/pre3"
    props 22a_props "$R/srv/pre3"
    st zfs create -o org.zfsbackup:config=$OWNCFG -o org.zfsbackup:client_id=client-1 "$R/srv/pre4"
    cappipe 23_raw_F_onto_precreated_with_local_props 0 "zfs send -w $ES@a" "zfs receive -F -s -u $R/srv/pre4"
    props 23a_props "$R/srv/pre4"
    st zfs create "$R/srv/pre5"
    cappipe 24_raw_F_without_s_onto_precreated_unenc 0 "zfs send -w $ES@a" "zfs receive -F -u $R/srv/pre5"
    # ---- Z1: a PLAIN full with -F onto a pre-created dataset under an ENCRYPTED target (and the controls)
    st zfs create "$R/esrv/pre1"
    cappipe 30_plain_F_onto_precreated_under_enc 0 "zfs send $PS@a" "zfs receive $RF $R/esrv/pre1"
    props 30a_props "$R/esrv/pre1"
    st zfs create "$R/esrv/pre2"
    cappipe 31_plain_noF_onto_precreated_under_enc 0 "zfs send $PS@a" "zfs receive -s -u $R/esrv/pre2"
    st zfs create "$R/srv/pre6"
    cappipe 32_plain_F_onto_precreated_under_unenc 0 "zfs send $PS@a" "zfs receive $RF $R/srv/pre6"
    props 32a_props "$R/srv/pre6"
    cappipe 33_plain_new_leaf_under_enc 0 "zfs send $PS@a" "zfs receive $RF $OWN $R/esrv/new1"
    props 33a_props "$R/esrv/new1"
    cappipe 34_plain_new_leaf_under_unenc 0 "zfs send $PS@a" "zfs receive $RF $OWN $R/srv/new1"
    props 34a_props "$R/srv/new1"
    st zfs create "$R/esrv/pre3"
    cappipe 35_plain_F_L_c_onto_precreated_under_enc 0 "zfs send -L -c $PS@a" "zfs receive $RF $R/esrv/pre3"
    # ---- Z1: a raw full into a non-existent leaf under an ENCRYPTED parent
    cappipe 40_raw_new_leaf_under_enc 0 "zfs send -w $ES@a" "zfs receive $RF $OWN $R/esrv/rawleaf"
    props 40a_props "$R/esrv/rawleaf"
    st zfs create "$R/esrv/id"
    cappipe 41_raw_new_leaf_under_id_under_enc 0 "zfs send -w $ES@a" "zfs receive $RF $OWN $R/esrv/id/rawleaf"
    props 41a_props "$R/esrv/id/rawleaf"
    cap 41b_id_props zfs get -H -o name,property,value $ENCP "$R/esrv/id"
    st zfs create "$R/srv/id"
    cappipe 42_raw_new_leaf_under_id_under_unenc 0 "zfs send -w $ES@a" "zfs receive $RF $OWN $R/srv/id/rawleaf"
    props 42a_props "$R/srv/id/rawleaf"
    st zfs create "$R/esrv/pre4"
    cappipe 43_raw_F_onto_precreated_under_enc 0 "zfs send -w $ES@a" "zfs receive $RF $R/esrv/pre4"
    props 43a_props "$R/esrv/pre4"
    # ---- raw incremental
    cappipe 50_raw_incr 0 "zfs send -w -i $ES@a $ES@b" "zfs receive -s -u $R/srv/n0"
    snaps 50a_snaps "$R/srv/n0"
    cappipe 51_raw_incr_with_o_user 0 "zfs send -w -i $ES@a $ES@b" "zfs receive -s -u -o org.x:y=incr $R/srv/n1"
    props 51a_props "$R/srv/n1" org.x:y
    cappipe 52_raw_incr_daemon_flags 0 "zfs send -w -i $ES@a $ES@b" "zfs receive $RF $OWN $R/srv/n8"
    props 52a_props "$R/srv/n8"
    cappipe 53_raw_incr_onto_wrong_base 0 "zfs send -w -i $ES@b $ES@c" "zfs receive -s -u $R/srv/n2"
    cappipe 54_raw_incr_F 0 "zfs send -w -i $ES@b $ES@c" "zfs receive -F -s -u $R/srv/n0"
    # ---- raw resume with the keys unloaded on BOTH sides (full into a new dataset, then incremental)
    cappipe 60_raw_full_cut 1000000 "zfs send -w $ES@a" "zfs receive -s -u $R/srv/rr"
    tk="$(tok "$R/srv/rr")"; echo "$tk" > "$SD/60.token"
    cap 60a_unload_source_key bash -c "zfs unmount $ES; zfs unload-key $ES; zfs get -H -o name,property,value keystatus $ES $R/srv/rr"
    cap 61_send_nv_t_keys_unloaded zfs send -nv -t "$tk"
    cappipe 62_raw_resume_keys_unloaded 0 "zfs send -t $tk" "zfs receive -s -u $R/srv/rr"
    props 62a_props "$R/srv/rr"
    cappipe 63_raw_incr_cut_keys_unloaded 500000 "zfs send -w -i $ES@a $ES@b" "zfs receive -s -u $R/srv/rr"
    tk="$(tok "$R/srv/rr")"; echo "$tk" > "$SD/63.token"
    cap 64_send_nv_t_keys_unloaded zfs send -nv -t "$tk"
    cappipe 65_raw_incr_resume_keys_unloaded 0 "zfs send -t $tk" "zfs receive -s -u $R/srv/rr"
    snaps 65a_snaps "$R/srv/rr"
    cap 66_load_source_key zfs load-key "$ES"
    # ---- -o / -x encryption on a raw receive (expected to be refused) and, for contrast, on a plain receive
    cappipe 70_raw_o_encryption 0 "zfs send -w $ES@a" "zfs receive -s -u -o encryption=aes-256-gcm $R/srv/e1"
    cappipe 71_raw_o_encryption_off 0 "zfs send -w $ES@a" "zfs receive -s -u -o encryption=off $R/srv/e2"
    cappipe 72_raw_o_keyformat 0 "zfs send -w $ES@a" "zfs receive -s -u -o keyformat=passphrase $R/srv/e3"
    cappipe 73_raw_o_keylocation 0 "zfs send -w $ES@a" "zfs receive -s -u -o keylocation=prompt $R/srv/e4"
    props 73a_props "$R/srv/e4" keylocation
    cappipe 74_raw_x_encryption 0 "zfs send -w $ES@a" "zfs receive -s -u -x encryption $R/srv/e5"
    cappipe 75_raw_x_keylocation 0 "zfs send -w $ES@a" "zfs receive -s -u -x keylocation $R/srv/e6"
    cappipe 76_plain_o_encryption 0 "zfs send $PS@a" "zfs receive -s -u -o encryption=aes-256-gcm -o keyformat=passphrase -o keylocation=file://$KEY2 $R/srv/e7"
    props 76a_props "$R/srv/e7" keylocation
    cappipe 77_plain_o_encryption_under_enc_target 0 "zfs send $PS@a" "zfs receive -s -u -o encryption=off $R/esrv/e8"
    cap 90_list_all zfs list -H -t all -o name,type,encryption,keystatus,encryptionroot -r "$R/srv" "$R/esrv"
    end_section
}

# ================================================================ M6: -L mismatch in both directions
sec_M6() {
    section M6 "-L (and -c) mismatch between a full receive and a later incremental, both directions"
    local big="$R/cs/big" small="$R/cs/small" src nm pair f i fl il id t
    st zfs create "$R/cs"; st zfs create -o mountpoint=none "$R/srv"
    for nm in big small; do
        src="$R/cs/$nm"
        st zfs create -o recordsize="$([ $nm = big ] && echo 1M || echo 128K)" -o compression=off "$src"
        rnd "/$src/f_a" 6; st zfs snapshot "$src@a"
        rnd "/$src/f_b" 4; st zfs snapshot "$src@b"
        rnd "/$src/f_c" 4; st zfs snapshot "$src@c"
        cap "$nm.00_src_props" zfs get -H -o name,property,value recordsize "$src"
        # "full-flags:incr-flags"  (a '.' stands for no flags)
        for pair in .:. L:. .:L Lc:. .:Lc c:. .:c L:c c:L L:Lc Lc:L Lc:Lc; do
            f="${pair%%:*}"; i="${pair##*:}"
            fl="$([ "$f" = . ] && echo '' || echo "$f" | sed -e 's/L/-L /' -e 's/c/-c/')"
            il="$([ "$i" = . ] && echo '' || echo "$i" | sed -e 's/L/-L /' -e 's/c/-c/')"
            id="$nm.${f/./none}_then_${i/./none}"
            t="$R/srv/${nm}_${f/./n}_${i/./n}"
            log "M6 $id"
            cappipe "$id.1_full" 0 "zfs send $fl $src@a" "zfs receive -u $t"
            cappipe "$id.2_incr" 0 "zfs send $il -i $src@a $src@b" "zfs receive -u $t"
            cappipe "$id.3_incr_again" 0 "zfs send $il -i $src@b $src@c" "zfs receive -u $t"
            cap "$id.4_snaps" zfs list -H -t snapshot -o name -r "$t"
            cappipe "$id.5_F_full" 0 "zfs send $fl $src@a" "zfs receive -u ${t}_F"
            cappipe "$id.6_F_incr" 0 "zfs send $il -i $src@a $src@b" "zfs receive -F -u ${t}_F"
        done
    done
    # chains: does the -L state of the LAST received incremental decide (full, incr b, incr c)
    fl_of() { case "$1" in none) echo "" ;; L) echo "-L" ;; Lc) echo "-L -c" ;; c) echo "-c" ;; esac; }
    src="$R/cs/big"
    for pair in none:none:L none:L:none none:L:L L:L:none Lc:Lc:c; do
        IFS=: read -r f i j <<<"$pair"
        id="chain.${f}_${i}_${j}"; t="$R/srv/chain_${f}_${i}_${j}"
        cappipe "$id.1_full" 0 "zfs send $(fl_of $f) $src@a" "zfs receive -u $t"
        cappipe "$id.2_incr_b" 0 "zfs send $(fl_of $i) -i $src@a $src@b" "zfs receive -u $t"
        cappipe "$id.3_incr_c" 0 "zfs send $(fl_of $j) -i $src@b $src@c" "zfs receive -u $t"
    done
    # the same mismatch on a RESUMED receive: the token carries the flags of the interrupted stream
    src="$R/cs/big"; t="$R/srv/resumed_mismatch"
    cappipe resumed.1_full 0 "zfs send -L $src@a" "zfs receive -u $t"
    cappipe resumed.2_incr_cut_without_L 400000 "zfs send -i $src@a $src@b" "zfs receive -s -u $t"
    cap resumed.3_token zfs get -H -o name,property,value receive_resume_token "$t"
    end_section
}

# ================================================================ M7: -F / rollback / modified-head semantics (plan B-2b: U1 U2 U4 U5 U6)
# helpers (plain commands only; everything the daemon's receiver would do is spelled out)
# m7_src <ds> <enc 0|1> <snap>...: a dataset with one fresh random file per snapshot (M7MB MiB, default 3), snapshots in order
m7_src() {
    local ds="$1" enc="$2" s o=""; shift 2
    [ "$enc" = 1 ] && o="-o encryption=aes-256-gcm -o keyformat=passphrase -o keylocation=file://$KEY1"
    st zfs create -o compression=off $o "$ds" || return 1
    for s in "$@"; do rnd "/$ds/f_$s" "${M7MB:-3}"; st zfs snapshot "$ds@$s"; done
}
# m7_xfer '<sender>' '<receiver>': an UNRECORDED setup transfer
m7_xfer() { st bash -c "set -o pipefail; $1 | $2"; }
# m7_hist <src> <target> <w>: the receiver's history is old, new1 (a full stream of old, then an incremental to new1); w = -w for raw
m7_hist() {
    m7_xfer "zfs send $3 $1@old" "zfs receive -u $2"
    m7_xfer "zfs send $3 -i $1@old $1@new1" "zfs receive -u $2"
}
# m7_ab <src> <target> <w>: the receiver's newest snapshot is b (full a, incremental a -> b)
m7_ab() {
    m7_xfer "zfs send $3 $1@a" "zfs receive -u $2"
    m7_xfer "zfs send $3 -i $1@a $1@b" "zfs receive -u $2"
}
# m7_modify <ds> write|read|mount: change the head of a received dataset (mounts it, then unmounts it again)
m7_modify() {
    local ds="$1" how="$2" mp
    case "$(zfs get -H -o value encryption "$ds" 2>/dev/null)" in off|"") ;; *) st zfs load-key -L "file://$KEY1" "$ds" ;; esac
    st zfs mount "$ds"
    mp="$(zfs get -H -o value mountpoint "$ds" 2>/dev/null)"
    case "$how" in
        write) st bash -c "echo head-change > '$mp/head_change'" ;;
        read)  st bash -c "cat '$mp'/f_* >/dev/null" ;;
        mount) ;;
    esac
    st zpool sync "$P"
    st zfs unmount "$ds"
}
m7_written() { cap "$1" zfs get -H -o name,property,value written "$2"; }

sec_M7() {
    section M7 "-F on an incremental whose receiver is ahead (U1/U2), rollback -r vs local user properties (U4), modified head (U5), full stream onto a copy with snapshots (U6)"
    local src t tk n how w sfx otherp
    # ------------------------------------------------------------ U1
    st zfs create "$R/u1"; src="$R/u1src"
    m7_src "$src" 0 old new1 new2
    note "U1: sender $src has old,new1,new2. Each receiver t_* is built with a full stream of @old and an incremental to @new1 (its history is AHEAD of the base @old). Then: zfs send -i @old @new2 | zfs receive [-F] -s -u"
    snaps U1.00_sender_snaps "$src"
    for n in a b c d; do m7_hist "$src" "$R/u1/t_$n" ""; done
    st zfs hold keep "$R/u1/t_d@new1"
    snaps U1.a1_before "$R/u1/t_a"
    cappipe U1.a2_F_incr "0" "zfs send -i $src@old $src@new2" "zfs receive -F -s -u $R/u1/t_a"
    snaps U1.a3_after "$R/u1/t_a"
    cap U1.a4_token zfs get -H -o name,property,value receive_resume_token "$R/u1/t_a"
    snaps U1.b1_before "$R/u1/t_b"
    cappipe U1.b2_noF_incr "0" "zfs send -i $src@old $src@new2" "zfs receive -s -u $R/u1/t_b"
    snaps U1.b3_after "$R/u1/t_b"
    cap U1.b4_token zfs get -H -o name,property,value receive_resume_token "$R/u1/t_b"
    note "U1 c/d: the sender no longer has @new1 (it was destroyed there after the receiver got it: the rollback/restore situation)"
    st zfs destroy "$src@new1"
    snaps U1.c0_sender_snaps "$src"
    snaps U1.c1_before "$R/u1/t_c"
    cappipe U1.c2_F_incr_sender_lacks_new1 "0" "zfs send -i $src@old $src@new2" "zfs receive -F -s -u $R/u1/t_c"
    snaps U1.c3_after "$R/u1/t_c"
    cap U1.d0_holds zfs holds -H "$R/u1/t_d@new1"
    snaps U1.d1_before "$R/u1/t_d"
    cappipe U1.d2_F_incr_newer_snapshot_held "0" "zfs send -i $src@old $src@new2" "zfs receive -F -s -u $R/u1/t_d"
    snaps U1.d3_after "$R/u1/t_d"
    # ------------------------------------------------------------ U2
    st zfs create "$R/u2"; src="$R/u2src"
    m7_src "$src" 0 old new1 new2
    note "U2: the same -F -s incremental cut after 1000000 bytes; are the newer snapshots still there while the partial state exists; then resume"
    snaps U2.00_sender_snaps "$src"
    for n in a b c; do m7_hist "$src" "$R/u2/t_$n" ""; done
    snaps U2.a1_before "$R/u2/t_a"
    cappipe U2.a2_F_incr_cut 1000000 "zfs send -i $src@old $src@new2" "zfs receive -F -s -u $R/u2/t_a"
    snaps U2.a3_while_partial "$R/u2/t_a"
    cap U2.a3b_list_all zfs list -H -t all -o name,type -r "$R/u2/t_a"
    cap U2.a4_token_prop zfs get -H -o name,property,value receive_resume_token "$R/u2/t_a"
    tk="$(tok "$R/u2/t_a")"; echo "$tk" > "$SD/U2.a.token"
    cap U2.a5_send_nv_t zfs send -nv -t "$tk"
    cappipe U2.a6_resume_F 0 "zfs send -t $tk" "zfs receive -F -s -u $R/u2/t_a"
    snaps U2.a7_after_resume "$R/u2/t_a"
    cap U2.a8_token_prop zfs get -H -o name,property,value receive_resume_token "$R/u2/t_a"
    note "U2 b: the same cut, then the resume WITHOUT -F"
    cappipe U2.b1_F_incr_cut 1000000 "zfs send -i $src@old $src@new2" "zfs receive -F -s -u $R/u2/t_b"
    snaps U2.b2_while_partial "$R/u2/t_b"
    tk="$(tok "$R/u2/t_b")"; echo "$tk" > "$SD/U2.b.token"
    cappipe U2.b3_resume_noF 0 "zfs send -t $tk" "zfs receive -s -u $R/u2/t_b"
    snaps U2.b4_after_resume "$R/u2/t_b"
    cap U2.b5_token_prop zfs get -H -o name,property,value receive_resume_token "$R/u2/t_b"
    note "U2 c: the same cut, then zfs receive -A (the server's fresh-mode abort)"
    cappipe U2.c1_F_incr_cut 1000000 "zfs send -i $src@old $src@new2" "zfs receive -F -s -u $R/u2/t_c"
    snaps U2.c2_while_partial "$R/u2/t_c"
    cap U2.c3_abort zfs receive -A "$R/u2/t_c"
    snaps U2.c4_after_abort "$R/u2/t_c"
    cap U2.c5_token_prop zfs get -H -o name,property,value receive_resume_token "$R/u2/t_c"
    # ------------------------------------------------------------ U4
    st zfs create "$R/u4"; src="$R/u4/src"
    st zfs create "$src"
    st zfs set org.zfsbackup:anchor.x=snapA "$src"
    rnd "/$src/f_a" 1; st zfs snapshot "$src@A"
    st zfs set org.zfsbackup:anchor.y=afterA "$src"
    rnd "/$src/f_b" 1; st zfs snapshot "$src@B"
    rnd "/$src/f_c" 1; st zfs snapshot "$src@C"
    note "U4: local user properties on the sender survive zfs rollback -r @A?"
    cap U4.a1_props_before zfs get -H -o name,property,value,source org.zfsbackup:anchor.x,org.zfsbackup:anchor.y "$src"
    snaps U4.a2_snaps_before "$src"
    cap U4.a3_rollback_without_r zfs rollback "$src@A"
    cap U4.a4_rollback_r zfs rollback -r "$src@A"
    snaps U4.a5_snaps_after "$src"
    cap U4.a6_props_after zfs get -H -o name,property,value,source org.zfsbackup:anchor.x,org.zfsbackup:anchor.y "$src"
    capsh U4.a7_local_all "zfs get -H -s local -o property,value,source all $src | grep -E '^org\\.'"
    note "U4 b: the anchors arrive as RECEIVED properties (send -p); rollback -r on the receiving copy; plus a local override"
    m7_xfer "zfs send -p $src@A" "zfs receive -u $R/u4/rcv"
    st zfs set org.zfsbackup:anchor.w=snapW "$src"; st zfs snapshot "$src@W"
    m7_xfer "zfs send -p -i $src@A $src@W" "zfs receive -u $R/u4/rcv"
    rnd "/$src/f_x" 1; st zfs snapshot "$src@X"
    m7_xfer "zfs send -p -i $src@W $src@X" "zfs receive -u $R/u4/rcv"
    st zfs set org.zfsbackup:anchor.z=localz "$R/u4/rcv"
    st zfs set org.zfsbackup:anchor.x=localx "$R/u4/rcv"
    snaps U4.b1_snaps_before "$R/u4/rcv"
    cap U4.b2_props_before zfs get -H -o name,property,value,received,source org.zfsbackup:anchor.x,org.zfsbackup:anchor.y,org.zfsbackup:anchor.w,org.zfsbackup:anchor.z "$R/u4/rcv"
    cap U4.b3_rollback_r zfs rollback -r "$R/u4/rcv@A"
    snaps U4.b4_snaps_after "$R/u4/rcv"
    cap U4.b5_props_after zfs get -H -o name,property,value,received,source org.zfsbackup:anchor.x,org.zfsbackup:anchor.y,org.zfsbackup:anchor.w,org.zfsbackup:anchor.z "$R/u4/rcv"
    # ------------------------------------------------------------ U5
    st zfs create "$R/u5"
    for w in plain raw; do
        src="$R/u5/src_$w"
        if [ "$w" = raw ]; then m7_src "$src" 1 a b c; sfx="-w"; else m7_src "$src" 0 a b c; sfx=""; fi
        note "U5 $w: receiver newest snapshot == base @b, then its head is modified; zfs send $sfx -i @b @c, without -F and then with -F ($RF)"
        # c0: control, head untouched
        t="$R/u5/${w}_c0"; m7_ab "$src" "$t" "$sfx"
        m7_written "U5.$w.c0.1_written" "$t"
        cappipe "U5.$w.c0.2_noF_untouched" 0 "zfs send $sfx -i $src@b $src@c" "zfs receive -s -u $t"
        snaps "U5.$w.c0.3_snaps" "$t"
        for how in mount write read; do
            t="$R/u5/${w}_$how"; m7_ab "$src" "$t" "$sfx"
            m7_written "U5.$w.$how.1_written_before" "$t"
            m7_modify "$t" "$how"
            m7_written "U5.$w.$how.2_written_after" "$t"
            cappipe "U5.$w.$how.3_noF" 0 "zfs send $sfx -i $src@b $src@c" "zfs receive -s -u $t"
            snaps "U5.$w.$how.4_snaps_after_noF" "$t"
            cappipe "U5.$w.$how.5_F" 0 "zfs send $sfx -i $src@b $src@c" "zfs receive -F -s -u $t"
            snaps "U5.$w.$how.6_snaps_after_F" "$t"
            cap "U5.$w.$how.7_diff_ok" zfs get -H -o name,property,value receive_resume_token,written "$t"
        done
        if [ "$w" = raw ]; then
            # the server's case: modified while the key was loaded, then the key is gone again
            t="$R/u5/raw_unloaded"; m7_ab "$src" "$t" "$sfx"
            m7_modify "$t" write
            st zfs unload-key "$t"
            cap U5.raw.unloaded.1_keystatus zfs get -H -o name,property,value keystatus,written "$t"
            cappipe U5.raw.unloaded.2_noF 0 "zfs send $sfx -i $src@b $src@c" "zfs receive -s -u $t"
            cappipe U5.raw.unloaded.3_F 0 "zfs send $sfx -i $src@b $src@c" "zfs receive -F -s -u $t"
            snaps U5.raw.unloaded.4_snaps_after "$t"
            # load-key and unload-key only (no mount): does the key load alone modify the head?
            t="$R/u5/raw_keyload"; m7_ab "$src" "$t" "$sfx"
            st zfs load-key -L "file://$KEY1" "$t"; st zpool sync "$P"; st zfs unload-key "$t"
            cap U5.raw.keyload.1_written zfs get -H -o name,property,value keystatus,written "$t"
            cappipe U5.raw.keyload.2_noF 0 "zfs send $sfx -i $src@b $src@c" "zfs receive -s -u $t"
            snaps U5.raw.keyload.3_snaps_after "$t"
        fi
    done
    # ------------------------------------------------------------ U6
    st zfs create "$R/u6"; src="$R/u6/src"; otherp="$R/u6/other"
    m7_src "$src" 0 a b c
    m7_src "$otherp" 0 a z
    m7_src "$R/u6/esrc" 1 a b c
    note "U6: a FULL stream onto an existing copy; the copy (src a,b) has snapshots a,b. Per case: without -F (zfs receive -s -u), then with -F ($RF), each on a fresh copy"
    for n in same_newer:"$src@c" same_existing_name:"$src@a" other_same_name:"$otherp@a" other_new_name:"$otherp@z"; do
        for how in noF F; do
            t="$R/u6/c_${n%%:*}_$how"; m7_ab "$src" "$t" ""
            snaps "U6.${n%%:*}.$how.1_before" "$t"
            cappipe "U6.${n%%:*}.$how.2_recv" 0 "zfs send ${n#*:}" "zfs receive $([ $how = F ] && echo -F) -s -u $t"
            snaps "U6.${n%%:*}.$how.3_after" "$t"
            cap "U6.${n%%:*}.$how.4_props" zfs get -H -o name,property,value,source receive_resume_token "$t"
        done
    done
    note "U6 empty: the existing dataset has NO snapshots (placeholder / cut first receive)"
    for how in noF F; do
        t="$R/u6/empty_$how"; st zfs create "$t"
        cappipe "U6.empty.$how.1_recv" 0 "zfs send $src@c" "zfs receive $([ $how = F ] && echo -F) -s -u $t"
        snaps "U6.empty.$how.2_after" "$t"
    done
    note "U6 raw: a raw full stream of the encrypted dataset onto a raw copy that has snapshots a,b"
    for how in noF F; do
        t="$R/u6/raw_$how"; m7_ab "$R/u6/esrc" "$t" "-w"
        snaps "U6.raw.$how.1_before" "$t"
        cappipe "U6.raw.$how.2_recv" 0 "zfs send -w $R/u6/esrc@c" "zfs receive $([ $how = F ] && echo -F) -s -u $t"
        snaps "U6.raw.$how.3_after" "$t"
    done
    end_section
}

# ================================================================ Z2: -o org.zfsbackup:* (and ownership props) on raw receives
sec_Z2() {
    section Z2 "-o org.zfsbackup:{config,client_id,source_dataset} on raw full / incremental / resumed full / resumed incremental; -o readonly/canmount/volmode on raw"
    local ES="$R/cs/enc" v nm args tk T
    st zfs create "$R/cs"; mk_srv; mk_enc "$ES"
    st zfs set org.zfsbackup:config=FORGED "$ES"; st zfs set org.zfsbackup:client_id=forged-id "$ES"
    st zfs set org.zfsbackup:source_dataset=forged/ds "$ES"; st zfs snapshot "$ES@p"
    # ---- raw full into a non-existent leaf (the R0 flow: the receive creates the dataset)
    cappipe 10_raw_full 0 "zfs send -w $ES@a" "zfs receive $RF $OWN $R/srv/full"
    props 10a_props "$R/srv/full"
    # ---- raw incremental: changed values / same values / no -o
    cappipe 20_incr_base 0 "zfs send -w $ES@a" "zfs receive $RF $OWN $R/srv/incr_diff"
    cappipe 20a_incr_diff 0 "zfs send -w -i $ES@a $ES@b" "zfs receive $RF $OWN2 $R/srv/incr_diff"
    props 20b_props "$R/srv/incr_diff"
    cappipe 21_incr_base 0 "zfs send -w $ES@a" "zfs receive $RF $OWN $R/srv/incr_same"
    cappipe 21a_incr_same 0 "zfs send -w -i $ES@a $ES@b" "zfs receive $RF $OWN $R/srv/incr_same"
    props 21b_props "$R/srv/incr_same"
    cappipe 22_incr_base 0 "zfs send -w $ES@a" "zfs receive $RF $OWN $R/srv/incr_none"
    cappipe 22a_incr_none 0 "zfs send -w -i $ES@a $ES@b" "zfs receive $RF $R/srv/incr_none"
    props 22b_props "$R/srv/incr_none"
    # ---- resumed raw full: resume with the same -o / no -o / different -o
    for v in same:"$OWN" none: diff:"$OWN2"; do
        nm="${v%%:*}"; args="${v#*:}"; T="$R/srv/rfull_$nm"
        cappipe "30_$nm.1_cut" 1000000 "zfs send -w $ES@a" "zfs receive $RF $OWN $T"
        tk="$(tok "$T")"; echo "$tk" > "$SD/30_$nm.token"
        props "30_$nm.2_props_partial" "$T"
        cappipe "30_$nm.3_resume" 0 "zfs send -t $tk" "zfs receive $RF $args $T"
        props "30_$nm.4_props_final" "$T"
        snaps "30_$nm.5_snaps" "$T"
    done
    # ---- resumed raw incremental
    for v in same:"$OWN" none: diff:"$OWN2"; do
        nm="${v%%:*}"; args="${v#*:}"; T="$R/srv/rincr_$nm"
        cappipe "31_$nm.0_base" 0 "zfs send -w $ES@a" "zfs receive $RF $OWN $T"
        cappipe "31_$nm.1_cut" 500000 "zfs send -w -i $ES@a $ES@b" "zfs receive $RF $OWN $T"
        tk="$(tok "$T")"; echo "$tk" > "$SD/31_$nm.token"
        props "31_$nm.2_props_partial" "$T"
        cappipe "31_$nm.3_resume" 0 "zfs send -t $tk" "zfs receive $RF $args $T"
        props "31_$nm.4_props_final" "$T"
        snaps "31_$nm.5_snaps" "$T"
    done
    # ---- B-2's later -o set on raw: readonly / canmount / volmode (accepted? what is the result?)
    for v in readonly=on canmount=noauto volmode=none "readonly=on -o canmount=noauto -o volmode=none"; do
        nm="${v%%=*}"; [ "$v" != "${v#* }" ] && nm=all3
        cappipe "40_$nm.1_raw_full" 0 "zfs send -w $ES@a" "zfs receive $RF $OWN -o $v $R/srv/ro_$nm"
        props "40_$nm.2_props" "$R/srv/ro_$nm"
        cappipe "40_$nm.3_raw_incr" 0 "zfs send -w -i $ES@a $ES@b" "zfs receive $RF $OWN -o $v $R/srv/ro_$nm"
        props "40_$nm.4_props" "$R/srv/ro_$nm"
    done
    # readonly=on is in force: a later incremental still receives? (zfs receive writes below the readonly check)
    cappipe 41_raw_incr_onto_readonly_without_F 0 "zfs send -w -i $ES@b $ES@c" "zfs receive -s -u $R/srv/ro_readonly"
    # resumed raw full / incremental with B-2's props
    T="$R/srv/ro_resume_full"
    cappipe 42_resume_full.1_cut 1000000 "zfs send -w $ES@a" "zfs receive $RF $OWN -o readonly=on -o canmount=noauto -o volmode=none $T"
    tk="$(tok "$T")"
    cappipe 42_resume_full.2_resume 0 "zfs send -t $tk" "zfs receive $RF $OWN -o readonly=on -o canmount=noauto -o volmode=none $T"
    props 42_resume_full.3_props "$T"
    T="$R/srv/ro_resume_incr"
    cappipe 43_resume_incr.0_base 0 "zfs send -w $ES@a" "zfs receive $RF $OWN -o readonly=on -o canmount=noauto -o volmode=none $T"
    cappipe 43_resume_incr.1_cut 500000 "zfs send -w -i $ES@a $ES@b" "zfs receive $RF $OWN -o readonly=on -o canmount=noauto -o volmode=none $T"
    tk="$(tok "$T")"
    cappipe 43_resume_incr.2_resume 0 "zfs send -t $tk" "zfs receive $RF $OWN -o readonly=on -o canmount=noauto -o volmode=none $T"
    props 43_resume_incr.3_props "$T"
    # ---- the forged-property check on raw: a -p raw stream carrying LOCAL config on the sender, received with -o
    cappipe 50_raw_p_stream_with_o 0 "zfs send -w -p $ES@p" "zfs receive $RF $OWN $R/srv/forge_o"
    props 50a_props "$R/srv/forge_o"
    cappipe 51_raw_p_stream_without_o 0 "zfs send -w -p $ES@p" "zfs receive $RF $R/srv/forge_none"
    props 51a_props "$R/srv/forge_none"
    cappipe 52_raw_p_stream_with_x_not_o 0 "zfs send -w -p $ES@p" "zfs receive $RF -x org.zfsbackup:config -x org.zfsbackup:client_id -x org.zfsbackup:source_dataset $R/srv/forge_x"
    props 52a_props "$R/srv/forge_x"
    cappipe 53_raw_o_and_x_same_property 0 "zfs send -w -p $ES@p" "zfs receive $RF -o org.zfsbackup:config=a -x org.zfsbackup:config $R/srv/forge_ox"
    end_section
}

# ================================================================ Z3: a partial receive of a NEW dataset with -s
sec_Z3() {
    section Z3 "partial new-dataset receive with -s: existence, zfs list, token holder, -o values while partial, receive -A residue, retries, destroy, resume"
    local ES="$R/cs/enc" PS="$R/cs/plain" mode snd L tk
    st zfs create "$R/cs"; mk_srv; mk_enc "$ES"; mk_plain "$PS"
    z3_cut() { # <mode> <name>: leaves a partial NEW dataset $R/srv/<mode>_<name>; sets L SND TK
        mode="$1"
        if [ "$mode" = raw ]; then snd="zfs send -w $ES@a"; else snd="zfs send $PS@a"; fi
        L="$R/srv/${mode}_$2"
        cappipe "$mode.$2.0_cut" 1000000 "$snd" "zfs receive $RF $OWN -o readonly=on $L"
        tk="$(tok "$L")"; echo "$tk" > "$SD/$mode.$2.token"
    }
    for mode in raw plain; do
        # ---- a: inspect the partial dataset, retry, abort
        z3_cut $mode a
        cap "$mode.a.1_list_srv"        zfs list -H -t all -o name,type -r "$R/srv"
        cap "$mode.a.2_list_L"          zfs list -H -t all -o name,type,mounted,canmount,readonly,used -r "$L"
        cap "$mode.a.3_list_L_exact"    zfs list -H -o name,type "$L"
        props "$mode.a.4_props_partial" "$L"
        cap "$mode.a.5_token_recursive" zfs get -H -r -o name,property,value,source receive_resume_token "$R/srv"
        cap "$mode.a.6_percent_recv"    zfs get -H -o name,property,value receive_resume_token "$L/%recv"
        cap "$mode.a.7_list_percent_recv" zfs list -H -t all "$L/%recv"
        capsh "$mode.a.8_zdb"           "zdb -d $P 2>&1 | grep -E 'recv|${mode}_a'"
        cap "$mode.a.9_get_all_local"   zfs get -H -s local,received -o name,property,value,source all "$L"
        cappipe "$mode.a.10_retry_fresh_full_F" 0 "$snd" "zfs receive $RF $OWN $L"
        cappipe "$mode.a.11_retry_fresh_full_noF" 0 "$snd" "zfs receive -s -u $L"
        cap "$mode.a.12_abort"          zfs receive -A "$L"
        cap "$mode.a.13_list_after_abort" zfs list -H -t all -o name,type -r "$R/srv"
        cap "$mode.a.14_get_after_abort" zfs get -H -o name,property,value,source "$PROPS_OWN,receive_resume_token" "$L"
        capsh "$mode.a.15_zdb_after"    "zdb -d $P 2>&1 | grep -E 'recv|${mode}_a'; true"
        cap "$mode.a.16_abort_again"    zfs receive -A "$L"
        cappipe "$mode.a.17_fresh_full_after_abort" 0 "$snd" "zfs receive $RF $OWN $L"
        snaps "$mode.a.18_snaps" "$L"
        # ---- b: destroy the partial dataset
        z3_cut $mode b
        cap "$mode.b.1_destroy"         zfs destroy "$L"
        cap "$mode.b.2_destroy_r"       zfs destroy -r "$L"
        cap "$mode.b.3_list"            zfs list -H -t all -o name,type -r "$R/srv"
        # ---- c: resume with the same -o, and complete
        z3_cut $mode c
        cappipe "$mode.c.1_resume" 0 "zfs send -t $tk" "zfs receive $RF $OWN -o readonly=on $L"
        props "$mode.c.2_props_final" "$L"
        snaps "$mode.c.3_snaps" "$L"
        # ---- d: resume with no -o at all
        z3_cut $mode d
        cappipe "$mode.d.1_resume_no_o" 0 "zfs send -t $tk" "zfs receive $RF $L"
        props "$mode.d.2_props_final" "$L"
        # ---- e: operate on the partial dataset
        z3_cut $mode e
        cap "$mode.e.1_set_user_prop"   zfs set org.zfsbackup:anchor.x=1 "$L"
        cap "$mode.e.2_snapshot"        zfs snapshot "$L@local"
        cap "$mode.e.3_rename"          zfs rename "$L" "${L}_renamed"
        cap "$mode.e.4_list_after"      zfs list -H -t all -o name,type -r "$R/srv"
        cap "$mode.e.5_token_after_rename" zfs get -H -r -o name,property,value receive_resume_token "$R/srv"
        cap "$mode.e.6_resume_after_rename" bash -c "zfs send -t $tk | zfs receive -s -u ${L}_renamed"
        # ---- f: the token property of the partial dataset itself
        z3_cut $mode f
        cap "$mode.f.1_token_prop_value" zfs get -H -o value receive_resume_token "$L"
        # ---- g: a partial INCREMENTAL on an existing dataset: where is the token, is there a hidden %recv, what does -A leave
        if [ "$mode" = raw ]; then snd="zfs send -w"; src="$ES"; else snd="zfs send"; src="$PS"; fi
        L="$R/srv/${mode}_g"
        cappipe "$mode.g.0_base" 0 "$snd $src@a" "zfs receive $RF $OWN $L"
        cappipe "$mode.g.1_incr_cut" 500000 "$snd -i $src@a $src@b" "zfs receive $RF $OWN2 $L"
        cap "$mode.g.2_list" zfs list -H -t all -o name,type -r "$L"
        props "$mode.g.3_props_partial" "$L"
        cap "$mode.g.4_token_recursive" zfs get -H -r -o name,property,value receive_resume_token "$L"
        capsh "$mode.g.5_zdb" "zdb -d $P 2>&1 | grep -E 'recv|${mode}_g'"
        cap "$mode.g.6_destroy_base_snapshot" zfs destroy "$L@a"
        cap "$mode.g.7_abort" zfs receive -A "$L"
        cap "$mode.g.8_list_after_abort" zfs list -H -t all -o name,type -r "$L"
        props "$mode.g.9_props_after_abort" "$L"
        capsh "$mode.g.10_zdb_after" "zdb -d $P 2>&1 | grep -E 'recv|${mode}_g'; true"
    done
    end_section
}

# ================================================================ Z4: sends with the key unloaded
sec_Z4() {
    section Z4 "zfs snapshot and send with the key unloaded (raw ok, plain error text)"
    local ES="$R/cs/enc" tk
    st zfs create "$R/cs"; mk_srv; mk_enc "$ES"
    # a plain-send token made while the key is still loaded (resume with the key unloaded comes later)
    cappipe 00_plain_cut_key_loaded 1000000 "zfs send $ES@a" "zfs receive -s -u $R/srv/plaintok"
    tk="$(tok "$R/srv/plaintok")"; echo "$tk" > "$SD/00.plain.token"
    cappipe 01_raw_cut_key_loaded 1000000 "zfs send -w $ES@a" "zfs receive -s -u $R/srv/rawtok"
    local tkw; tkw="$(tok "$R/srv/rawtok")"; echo "$tkw" > "$SD/01.raw.token"
    capsh 02_control_plain_wc_key_loaded "zfs send $ES@a | wc -c"
    cap 10_unmount zfs unmount "$ES"
    cap 11_unload_key zfs unload-key "$ES"
    cap 12_keystatus zfs get -H -o name,property,value keystatus "$ES" "$ES@a"
    cap 20_snapshot_key_unloaded zfs snapshot "$ES@k1"
    cap 21_snapshot_recursive_r_key_unloaded zfs snapshot -r "$ES@k2"
    cap 22_snapshots zfs list -H -t snapshot -o name,keystatus,encryption,createtxg -r "$ES"
    capsh 30_send_w_full_wc "zfs send -w $ES@a | wc -c"
    capsh 31_send_w_incr_wc "zfs send -w -i $ES@a $ES@b | wc -c"
    cap   32_send_w_nv zfs send -w -nv "$ES@a"
    capsh 33_send_w_snapshot_made_while_unloaded_wc "zfs send -w -i $ES@c $ES@k1 | wc -c"
    capsh 34_send_w_p_wc "zfs send -w -p $ES@a | wc -c"
    cap   35_send_w_nv_incr zfs send -w -nv -i "$ES@a" "$ES@b"
    capsh 40_send_plain_full_wc "zfs send $ES@a | wc -c"
    capsh 41_send_plain_incr_wc "zfs send -i $ES@a $ES@b | wc -c"
    cap   42_send_plain_nv zfs send -nv "$ES@a"
    cap   43_send_plain_nv_incr zfs send -nv -i "$ES@a" "$ES@b"
    capsh 44_send_plain_L_c_wc "zfs send -L -c $ES@a | wc -c"
    cap   45_send_plain_L_c_nv zfs send -L -c -nv "$ES@a"
    capsh 46_send_plain_e_wc "zfs send -e $ES@a | wc -c"
    cap   47_send_plain_p_nv zfs send -p -nv "$ES@a"
    cap   48_send_plain_snapshot_made_while_unloaded zfs send -nv "$ES@k1"
    cap   50_send_nv_t_plain_token_key_unloaded zfs send -nv -t "$tk"
    cappipe 51_resume_plain_token_key_unloaded 0 "zfs send -t $tk" "zfs receive -s -u $R/srv/plaintok"
    cap   52_send_nv_t_raw_token_key_unloaded zfs send -nv -t "$tkw"
    cappipe 53_resume_raw_token_key_unloaded 0 "zfs send -t $tkw" "zfs receive -s -u $R/srv/rawtok"
    snaps 54_snaps "$R/srv/rawtok"
    cap   60_hold_key_unloaded zfs hold keep "$ES@a"
    cap   61_destroy_snapshot_key_unloaded zfs destroy -v -p "$ES@k2"
    cap   62_list_after zfs list -H -t snapshot -o name,keystatus,userrefs -r "$ES"
    cap   63_bookmark_key_unloaded zfs bookmark "$ES@a" "$ES#bm"
    # the same plain/raw view from a server-side raw copy that has never had its key
    cappipe 70_server_raw_full 0 "zfs send -w $ES@a" "zfs receive $RF $OWN $R/srv/copy"
    cap   71_server_snapshot_no_key zfs snapshot "$R/srv/copy@srv1"
    capsh 72_server_send_w_wc "zfs send -w $R/srv/copy@a | wc -c"
    capsh 73_server_send_plain_wc "zfs send $R/srv/copy@a | wc -c"
    end_section
}

# ================================================================ Z5: raw incremental after change-key / change-key -i
sec_Z5() {
    section Z5 "raw incremental after zfs change-key, and after change-key -i"
    local EA="$R/cs/ea" EB="$R/cs/eb" EC="$R/cs/eb/c" k
    st zfs create "$R/cs"; mk_srv
    keyinfo() { cap "$1" zfs get -H -o name,property,value keyformat,keylocation,pbkdf2iters,pbkdf2salt,encryptionroot,keystatus "${@:2}"; }
    tryload() { # <id> <dataset>: which passphrase opens it (key 1, key 2); always unloads again
        cap "$1.k1" zfs load-key -L "file://$KEY1" "$2"; zfs unload-key "$2" >/dev/null 2>&1
        cap "$1.k2" zfs load-key -L "file://$KEY2" "$2"; zfs unload-key "$2" >/dev/null 2>&1
    }
    # ---- A: change-key on the encryption root itself
    mk_enc "$EA"
    cappipe A.00_raw_full 0 "zfs send -w $EA@a" "zfs receive -s -u $R/srv/ea"
    keyinfo A.01_src_keyinfo "$EA"; keyinfo A.02_srv_keyinfo "$R/srv/ea"
    cap A.10_change_key zfs change-key -o keyformat=passphrase -o keylocation=file://$KEY2 "$EA"
    keyinfo A.11_src_keyinfo_after "$EA"
    rnd "/$EA/f_d" 2; st zfs snapshot "$EA@d"
    cappipe A.20_raw_incr_pre_change_snapshot 0 "zfs send -w -i $EA@a $EA@b" "zfs receive -s -u $R/srv/ea"
    keyinfo A.21_srv_keyinfo "$R/srv/ea"
    tryload A.22_srv_load "$R/srv/ea"
    cappipe A.23_raw_incr_post_change_snapshot 0 "zfs send -w -i $EA@b $EA@d" "zfs receive -s -u $R/srv/ea"
    keyinfo A.24_srv_keyinfo "$R/srv/ea"
    tryload A.25_srv_load "$R/srv/ea"
    snaps A.26_snaps "$R/srv/ea"
    cappipe A.30_raw_full_new_leaf_after_change 0 "zfs send -w $EA@d" "zfs receive -s -u $R/srv/ea_new"
    keyinfo A.31_new_keyinfo "$R/srv/ea_new"
    tryload A.32_new_load "$R/srv/ea_new"
    cappipe A.40_raw_full_old_snapshot_new_leaf 0 "zfs send -w $EA@a" "zfs receive -s -u $R/srv/ea_old"
    tryload A.41_old_snapshot_load "$R/srv/ea_old"
    # a second change-key, then incremental with the daemon's flags
    cap A.50_change_key_back zfs change-key -o keyformat=passphrase -o keylocation=file://$KEY1 "$EA"
    st zfs snapshot "$EA@e"
    cappipe A.51_raw_incr_daemon_flags 0 "zfs send -w -i $EA@d $EA@e" "zfs receive $RF $OWN $R/srv/ea"
    keyinfo A.52_srv_keyinfo "$R/srv/ea"
    tryload A.53_srv_load "$R/srv/ea"
    # ---- B: change-key -i (the child stops being its own encryption root and inherits the parent's)
    mk_enc "$EB"
    st zfs create -o encryption=aes-256-gcm -o keyformat=passphrase -o keylocation=file://$KEY2 "$EC"
    rnd "/$EC/f_a" 2; st zfs snapshot "$EC@a"; rnd "/$EC/f_b" 2; st zfs snapshot "$EC@b"
    cap B.00_encryptionroot_before zfs get -H -o name,property,value encryptionroot,keystatus "$EB" "$EC" "$EC@a"
    cappipe B.10_raw_full_child 0 "zfs send -w $EC@a" "zfs receive -s -u $R/srv/ebc"
    keyinfo B.11_srv_keyinfo "$R/srv/ebc"
    cap B.20_change_key_i zfs change-key -i "$EC"
    cap B.21_encryptionroot_after zfs get -H -o name,property,value encryptionroot,keystatus,keylocation "$EB" "$EC" "$EC@a" "$EC@b"
    rnd "/$EC/f_c" 2; st zfs snapshot "$EC@c"
    cappipe B.30_raw_incr_pre_snapshot 0 "zfs send -w -i $EC@a $EC@b" "zfs receive -s -u $R/srv/ebc"
    keyinfo B.31_srv_keyinfo "$R/srv/ebc"
    tryload B.32_srv_load "$R/srv/ebc"
    cappipe B.40_raw_incr_post_snapshot 0 "zfs send -w -i $EC@b $EC@c" "zfs receive -s -u $R/srv/ebc"
    keyinfo B.41_srv_keyinfo "$R/srv/ebc"
    tryload B.42_srv_load "$R/srv/ebc"
    snaps B.43_snaps "$R/srv/ebc"
    cappipe B.50_raw_full_post_snapshot_new_leaf 0 "zfs send -w $EC@c" "zfs receive -s -u $R/srv/ebc_new"
    keyinfo B.51_new_keyinfo "$R/srv/ebc_new"
    tryload B.52_new_load "$R/srv/ebc_new"
    # ---- C: change-key -i on a child whose parent is also in the server tree
    cappipe C.00_raw_parent 0 "zfs send -w $EB@a" "zfs receive -s -u $R/srv/eb"
    cappipe C.01_raw_child_under_raw_parent 0 "zfs send -w $EC@c" "zfs receive -s -u $R/srv/eb/c"
    keyinfo C.02_srv_keyinfo "$R/srv/eb" "$R/srv/eb/c"
    end_section
}

# ================================================================ Z6: encryption / encryptionroot of received datasets
sec_Z6() {
    section Z6 "encryption and encryptionroot of a raw copy, a raw child under a raw parent, plain under an encrypted target, plain under an unencrypted target"
    local EP="$R/cs/ep" EC="$R/cs/ep/ec" PL="$R/cs/pl" g
    st zfs create "$R/cs"; mk_srv; mk_enc "$EP"; st zfs create "$EC"; rnd "/$EC/f" 2; st zfs snapshot "$EC@a"
    mk_plain "$PL"
    g() { cap "$1" zfs get -H -o name,property,value $ENCP,keylocation,keyformat "${@:2}"; }
    g 00_sources "$EP" "$EC" "$PL"
    # raw parent, then raw child under it
    cappipe 10_raw_parent 0 "zfs send -w $EP@a" "zfs receive $RF $OWN $R/srv/ep"
    g 11_props "$R/srv/ep"
    cappipe 12_raw_child_under_raw_parent 0 "zfs send -w $EC@a" "zfs receive $RF $OWN $R/srv/ep/ec"
    g 13_props "$R/srv/ep/ec"
    # raw child under a PLAIN parent
    st zfs create -o mountpoint=none "$R/srv/q"
    cappipe 14_raw_child_under_plain_parent 0 "zfs send -w $EC@a" "zfs receive $RF $OWN $R/srv/q/ec"
    g 15_props "$R/srv/q/ec"
    # a replication stream of the tree
    cappipe 16_raw_R_tree 0 "zfs send -w -R $EP@a" "zfs receive -s -u $R/srv/tree"
    g 17_props "$R/srv/tree" "$R/srv/tree/ec"
    # plain under an unencrypted target, plain under an encrypted target
    cappipe 20_plain_under_unenc 0 "zfs send $PL@a" "zfs receive $RF $OWN $R/srv/pl"
    g 21_props "$R/srv/pl"
    cappipe 22_plain_under_enc 0 "zfs send $PL@a" "zfs receive $RF $OWN $R/esrv/pl"
    g 23_props "$R/esrv/pl" "$R/esrv"
    # an encrypted source sent PLAIN (decrypted on the wire): into an unencrypted and into an encrypted target
    cappipe 24_enc_source_plain_under_unenc 0 "zfs send $EP@a" "zfs receive $RF $OWN $R/srv/decrypted"
    g 25_props "$R/srv/decrypted"
    cappipe 26_enc_source_plain_under_enc 0 "zfs send $EP@a" "zfs receive $RF $OWN $R/esrv/decrypted"
    g 27_props "$R/esrv/decrypted"
    # the real layout: <target>/<client_id>/<dataset>
    st zfs create "$R/esrv/id1"; st zfs create "$R/srv/id1"
    cappipe 30_raw_leaf_under_id_under_enc 0 "zfs send -w $EP@a" "zfs receive $RF $OWN $R/esrv/id1/ep"
    cappipe 31_raw_child_under_raw_parent_under_id 0 "zfs send -w $EC@a" "zfs receive $RF $OWN $R/esrv/id1/ep/ec"
    cappipe 32_plain_leaf_under_id_under_enc 0 "zfs send $PL@a" "zfs receive $RF $OWN $R/esrv/id1/pl"
    cappipe 33_raw_leaf_under_id_under_unenc 0 "zfs send -w $EP@a" "zfs receive $RF $OWN $R/srv/id1/ep"
    cappipe 34_plain_leaf_under_id_under_unenc 0 "zfs send $PL@a" "zfs receive $RF $OWN $R/srv/id1/pl"
    g 35_props_id_enc "$R/esrv/id1" "$R/esrv/id1/ep" "$R/esrv/id1/ep/ec" "$R/esrv/id1/pl"
    g 36_props_id_unenc "$R/srv/id1" "$R/srv/id1/ep" "$R/srv/id1/pl"
    cap 40_everything zfs get -H -r -o name,property,value encryption,encryptionroot,keystatus "$R/srv" "$R/esrv"
    cap 41_snapshot_view zfs list -H -t snapshot -o name,encryption,keystatus,encryptionroot -r "$R/srv" "$R/esrv"
    end_section
}

# ================================================================ Z7: a raw-received dataset with no key: properties and destroy
sec_Z7() {
    section Z7 "server-side zfs get -s local org.zfsbackup:* and zfs destroy of a snapshot on a raw copy that has no key"
    local ES="$R/cs/enc" X
    st zfs create "$R/cs"; mk_srv; mk_enc "$ES"; rnd "/$ES/f_d" 2; st zfs snapshot "$ES@d"
    X="$R/srv/rx"
    cappipe 00_raw_full 0 "zfs send -w $ES@a" "zfs receive $RF $OWN $X"
    cappipe 01_raw_incr_b 0 "zfs send -w -i $ES@a $ES@b" "zfs receive $RF $OWN $X"
    cappipe 02_raw_incr_c 0 "zfs send -w -i $ES@b $ES@c" "zfs receive $RF $OWN $X"
    st zfs set org.zfsbackup:anchor.d1=snap_20260101000000 "$X"
    cap 10_keystatus zfs get -H -o name,property,value keystatus,encryptionroot "$X"
    cap 11_list_cols zfs list -H -t snapshot -o name,createtxg,guid,userrefs,clones,encryption,keystatus -r "$X"
    cap 20_get_local_all zfs get -H -s local all "$X"
    cap 21_get_local_org zfs get -H -s local -o name,property,value,source "$PROPS_OWN,org.zfsbackup:anchor.d1" "$X"
    cap 22_get_local_recursive zfs get -H -r -s local -o name,property,value "$PROPS_OWN" "$R/srv"
    cap 23_get_local_wildcard zfs get -H -s local 'org.zfsbackup:*' "$X"
    cap 24_get_local_received_all zfs get -H -s local,received -o name,property,value,source all "$X"
    cap 25_get_value zfs get -H -o value org.zfsbackup:config "$X"
    cap 30_set_user_prop_keyless zfs set org.zfsbackup:anchor.d2=x "$X"
    cap 31_inherit_user_prop_keyless zfs inherit org.zfsbackup:anchor.d2 "$X"
    cap 40_dry_destroy_single zfs destroy -n -v -p "$X@a"
    cap 41_dry_destroy_batch zfs destroy -n -v -p "$X@a,b"
    cap 42_destroy_single zfs destroy -v -p "$X@a"
    cap 43_list zfs list -H -t snapshot -o name,keystatus -r "$X"
    cap 44_destroy_batch zfs destroy -v -p "$X@b"
    cap 45_list zfs list -H -t snapshot -o name -r "$X"
    cappipe 50_raw_incr_after_destroys 0 "zfs send -w -i $ES@c $ES@d" "zfs receive $RF $OWN $X"
    cap 51_list zfs list -H -t snapshot -o name -r "$X"
    cap 60_snapshot_keyless zfs snapshot "$X@srv1"
    cap 61_hold_keyless zfs hold keep "$X@srv1"
    cap 62_destroy_held_keyless zfs destroy -v -p "$X@srv1"
    cap 63_release zfs release keep "$X@srv1"
    cap 64_destroy_keyless_local_snapshot zfs destroy -v -p "$X@srv1"
    cap 65_list_final zfs list -H -t snapshot -o name,userrefs,keystatus -r "$X"
    end_section
}


# ================================================================ M1 summary: one mechanically parsed line per token
summarize_m1() {
    [ -d "$OUT/M1" ] || return 0
    local f id kind flags rc3 rc4 after
    {
        printf '# case\tkind\tsend-nv-t rc\ttoken *ok flags\tresume rc\ttoken property after resume\n'
        for f in "$OUT"/M1/*.3_send_nv_t.stdout; do
            [ -f "$f" ] || continue
            id="$(basename "$f" .3_send_nv_t.stdout)"; kind="${id##*.}"; id="${id%.*}"
            flags="$(awk '/^[ \t]+[a-z]+ok = 1/{print $1}' "$f" | sort | tr '\n' ',' | sed 's/,$//')"
            rc3="$(cat "$OUT/M1/$id.$kind.3_send_nv_t.rc")"; rc4="$(cat "$OUT/M1/$id.$kind.4_resume.rc" 2>/dev/null)"
            after="$(awk '{print $3}' "$OUT/M1/$id.$kind.5_after.stdout" 2>/dev/null | head -n1)"
            printf '%s\t%s\t%s\t%s\t%s\t%s\n' "$id" "$kind" "$rc3" "${flags:--}" "$rc4" "$after"
        done
    } > "$OUT/M1-SUMMARY.tsv"
}

# ================================================================ main
ZVER="$(zfs version 2>&1 | head -n1)"
want M1    && sec_M1
want M2    && sec_M2
want M3    && sec_M3
want M4    && sec_M4
want EP    && sec_EP
want M5    && sec_M5
want M6    && sec_M6
want M7    && sec_M7
want Z2    && sec_Z2
want Z3    && sec_Z3
want Z4    && sec_Z4
want Z5    && sec_Z5
want Z6    && sec_Z6
want Z7    && sec_Z7
summarize_m1
zpool get -H -o property,value feature@large_blocks,feature@embedded_data,feature@encryption "$P" > "$OUT/POOL-FEATURES-END.txt" 2>&1
log "done: $OUT"
echo done > "$OUT/DONE"
exit 0
EOF_CAPTURES
chmod +x "$TMP_HOST/run.sh" "$TMP_HOST/formats.sh" "$TMP_HOST/captures.sh"

# ─────────────────────────────── Per-VM work ────────────────────────────────
declare -a SUMMARY=()      # one printable block per VM
OVERALL=0

provision_vm() { # provision_vm <vm> <image>
    local vm="$1" image="$2"
    ensure_vm "$vm" "$image"
    step "Provisioning $vm"
    local top; top="$(git -C "$(dirname "$0")" rev-parse --show-toplevel)" || die "not in a git repo"
    local tar="$TMP_HOST/src.tar"
    git -C "$top" archive --format=tar "$SRC_REF" > "$tar" || die "git archive of '$SRC_REF' failed"
    # the work dir may not exist yet: create it (world-writable for the transfer; run.sh chowns it)
    multipass exec "$vm" -- sudo bash -c "mkdir -p '$WORK' && chown \$(id -u ubuntu):\$(id -g ubuntu) '$WORK'" \
        || die "cannot create $WORK on $vm"
    put_file "$tar" "$vm" "$WORK/src.tar"
    put_file "$TMP_HOST/run.sh" "$vm" "$GUEST_RUN"
    put_file "$TMP_HOST/formats.sh" "$vm" "$GUEST_FORMATS"
    put_file "$TMP_HOST/captures.sh" "$vm" "$GUEST_CAPTURES"
    multipass exec "$vm" -- bash "$GUEST_RUN" provision 2>&1 | tee "$LOG_DIR/$vm.provision.log"
    [ "${PIPESTATUS[0]}" -eq 0 ] || return 1
}

ship_vm() { # ship_vm <vm> <image>: --captures-only. The VMs are normally provisioned already: ship the scripts only.
    local vm="$1" image="$2"
    ensure_vm "$vm" "$image"
    step "Shipping the scripts to $vm"
    multipass exec "$vm" -- sudo bash -c "mkdir -p '$WORK' && chown \$(id -u ubuntu):\$(id -g ubuntu) '$WORK'" \
        || die "cannot create $WORK on $vm"
    put_file "$TMP_HOST/run.sh" "$vm" "$GUEST_RUN"
    put_file "$TMP_HOST/formats.sh" "$vm" "$GUEST_FORMATS"
    put_file "$TMP_HOST/captures.sh" "$vm" "$GUEST_CAPTURES"
    if ! multipass exec "$vm" -- bash -c 'command -v zfs >/dev/null 2>&1 && command -v zpool >/dev/null 2>&1; r=$?; [ $r = 4 ] && r=104; exit $r'; then
        warn "$vm has no zfs yet: running the full provisioning"
        provision_vm "$vm" "$image" || return 1
    fi
}

# normalise what legitimately differs between two passes (pool names, guids/txgs/sizes, tokens, dates, pids)
norm_capture() {
    sed -E -e 's/zfsbcap[A-Za-z0-9_]*/POOL/g' -e 's/1-[0-9a-f]+-[0-9a-f]+-[0-9a-f]+(-[0-9a-f]+)*/TOKEN/g' \
        -e 's/0x[0-9a-f]+/0xH/g' -e 's/[0-9a-f]{32,}/HEX/g' \
        -e 's/[A-Z][a-z]{2} [A-Z][a-z]{2} +[0-9]+ [0-9:]+ [0-9]{4}/DATE/g' -e 's/[0-9]+(\.[0-9]+)?[KMGT]($|[^A-Za-z0-9])/SZ\2/g' \
        -e 's/[0-9]{2,}/N/g' -e 's/^N (zfs|sleep)/PID \1/' "$1" 2>/dev/null
}

captures_vm() { # captures_vm <vm>: run the passes in the guest, fetch the tarball, diff the locales
    local vm="$1" log="$LOG_DIR/$vm.captures.log" dir="$LOG_DIR/$vm.captures"
    step "SP captures on $vm (locales: $LOCALES${SECTIONS:+, sections: $SECTIONS})"
    multipass exec "$vm" -- env CAPTURE_LOCALES="$LOCALES" CAPTURE_SECTIONS="$SECTIONS" FORMATS_KEEP="$KEEP_POOLS" \
        bash "$GUEST_RUN" captures 2>&1 | tee "$log"
    [ "${PIPESTATUS[0]}" -eq 0 ] || { warn "capture run on $vm exited non-zero (see $log)"; return 1; }
    rm -rf "$dir" "$LOG_DIR/$vm.captures.tgz"; mkdir -p "$dir"
    multipass transfer "$vm:$WORK/captures.tgz" "$LOG_DIR/$vm.captures.tgz" || { warn "cannot fetch captures.tgz from $vm"; return 1; }
    tar -xzf "$LOG_DIR/$vm.captures.tgz" -C "$dir" --strip-components=1 || { warn "cannot unpack the captures of $vm"; return 1; }
    info "captures of $vm: $dir  ($(find "$dir" -type f | wc -l | tr -d ' ') files)"
    [ -s "$dir/default/SETUP-ERRORS.log" ] && warn "setup commands failed on $vm: see $dir/default/SETUP-ERRORS.log"
    # default vs LC_ALL=C
    if [ -d "$dir/default" ] && [ -d "$dir/C" ]; then
        local out="$LOG_DIR/$vm.locale-diff.txt" f rel n=0 d=0
        : > "$out"
        while IFS= read -r f; do
            rel="${f#"$dir/default/"}"
            case "$rel" in *.token|ENV.txt|SETUP-ERRORS.log|*.txt) continue ;; esac
            n=$((n+1))
            if [ ! -f "$dir/C/$rel" ]; then echo "ONLY IN default: $rel" >> "$out"; d=$((d+1)); continue; fi
            if ! diff -q <(norm_capture "$f") <(norm_capture "$dir/C/$rel") >/dev/null; then
                d=$((d+1)); { echo "=== DIFF $rel"; diff <(norm_capture "$f") <(norm_capture "$dir/C/$rel") | head -n 12; } >> "$out"
            fi
        done < <(find "$dir/default" -type f | sort)
        echo "compared $n record files (default vs C, normalised); $d differ" | tee -a "$out"
        info "locale diff: $out"
    fi
}

pytest_vm() { # pytest_vm <vm>   -> sets P_PASSED P_FAILED P_SKIPPED P_ERRORS P_XFAILED P_RC P_FAILS
    local vm="$1" log="$LOG_DIR/$vm.pytest.log"
    local -a files=("tests/test_real_zfs.py")
    for f in ${EXTRA_TESTS[@]+"${EXTRA_TESTS[@]}"}; do files+=("$f"); done
    step "pytest -m real_zfs ${files[*]} inside $vm"
    multipass exec "$vm" -- bash "$GUEST_RUN" pytest "${files[@]}" 2>&1 | tee "$log"
    local summary; summary="$(grep -E '^=+ .*(passed|failed|skipped|error|no tests ran).* in [0-9.]+s' "$log" | tail -n1)"
    P_PASSED="$(grep -oE '[0-9]+ passed' <<<"$summary" | grep -oE '[0-9]+' || true)"; P_PASSED="${P_PASSED:-0}"
    P_FAILED="$(grep -oE '[0-9]+ failed' <<<"$summary" | grep -oE '[0-9]+' || true)"; P_FAILED="${P_FAILED:-0}"
    P_SKIPPED="$(grep -oE '[0-9]+ skipped' <<<"$summary" | grep -oE '[0-9]+' || true)"; P_SKIPPED="${P_SKIPPED:-0}"
    P_ERRORS="$(grep -oE '[0-9]+ errors?' <<<"$summary" | grep -oE '[0-9]+' || true)"; P_ERRORS="${P_ERRORS:-0}"
    P_XFAILED="$(grep -oE '[0-9]+ xfailed' <<<"$summary" | grep -oE '[0-9]+' || true)"; P_XFAILED="${P_XFAILED:-0}"
    P_RC="$(grep -oE '^=== pytest exit code: [0-9]+' "$log" | tail -n1 | grep -oE '[0-9]+$' || true)"; P_RC="${P_RC:-?}"
    P_SUMMARY_LINE="${summary:-<no pytest summary line: see $log>}"
    P_FAILS="$(grep -E '^(FAILED|ERROR) ' "$log" | sort -u || true)"
}

formats_vm() { # formats_vm <vm>
    local vm="$1" log="$LOG_DIR/$vm.formats.log"
    step "Capturing zfs output formats on $vm"
    multipass exec "$vm" -- env FORMATS_KEEP="$KEEP_POOLS" bash "$GUEST_RUN" formats 2>&1 | tee "$log"
    [ "${PIPESTATUS[0]}" -eq 0 ] || warn "format capture on $vm exited non-zero (see $log)"
}

# ─────────────────────────────── Main ───────────────────────────────────────
echo "${BOLD} OpenZFS version matrix — ${#TARGETS[@]} VM(s), tree $SRC_REF${NC}"
echo " logs: $LOG_DIR"

for t in "${TARGETS[@]}"; do
    vm="${t%%:*}"; rest="${t#*:}"; image="${rest%%:*}"; label="${rest##*:}"
    echo
    step "=== $vm (Ubuntu $image, OpenZFS $label.x) ==="
    if [ "$CAPTURES_ONLY" = 1 ]; then prov=ship_vm; else prov=provision_vm; fi
    if ! "$prov" "$vm" "$image"; then
        SUMMARY+=("${RED}$vm  OpenZFS $label  PROVISIONING FAILED${NC} (see $LOG_DIR/$vm.provision.log)")
        OVERALL=1
        continue
    fi
    zver="$(multipass exec "$vm" -- bash -c 'zfs version 2>&1 | head -n1' | tr -d '\r')"
    if [ "$RUN_TESTS" = 1 ]; then
        pytest_vm "$vm"
        if [ "$P_FAILED" -gt 0 ] || [ "$P_ERRORS" -gt 0 ] || [ "$P_RC" != 0 ]; then
            verdict="${RED}FAIL${NC}"; OVERALL=1
        else
            verdict="${GREEN}PASS${NC}"
        fi
        block="$verdict  $vm  [$zver]  passed=$P_PASSED failed=$P_FAILED errors=$P_ERRORS skipped=$P_SKIPPED xfailed=$P_XFAILED (pytest rc=$P_RC)"
        [ -n "$P_FAILS" ] && block+=$'\n'"$(sed 's/^/        /' <<<"$P_FAILS")"
        SUMMARY+=("$block")
    else
        SUMMARY+=("----  $vm  [$zver]  (pytest not run: --formats-only / --captures-only)")
    fi
    if [ "$RUN_FORMATS" = 1 ]; then formats_vm "$vm"; fi
    if [ "$RUN_CAPTURES" = 1 ]; then captures_vm "$vm" || OVERALL=1; fi
    if [ "$KEEP_POOLS" = 0 ]; then
        multipass exec "$vm" -- bash "$GUEST_RUN" cleanup 2>&1 | tee "$LOG_DIR/$vm.cleanup.log" >/dev/null
    fi
done

echo
echo "${BOLD}════════════ OpenZFS matrix summary ════════════${NC}"
for b in "${SUMMARY[@]}"; do echo "$b"; done
echo "logs: $LOG_DIR  (<vm>.pytest.log, <vm>.formats.log, <vm>.captures/, <vm>.captures.log, <vm>.locale-diff.txt)"

if [ "$PURGE" = 1 ]; then
    info "deleting the matrix VMs (--purge; these only, never a global purge)"
    for t in "${TARGETS[@]}"; do
        vm="${t%%:*}"
        multipass delete --purge "$vm" || warn "could not delete $vm"
    done
else
    info "VMs left in place for reuse (use --purge to delete them)"
fi

exit "$OVERALL"
