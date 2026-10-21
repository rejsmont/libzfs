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
#   6. prints a per-version summary (pass/fail/skip/error counts and the failing test ids).
#
# Usage:
#   ./scenarios/test_openzfs_matrix.sh                      # all three versions, tests/test_real_zfs.py only
#   ./scenarios/test_openzfs_matrix.sh --only 21,24         # a subset (21 22 24)
#   ./scenarios/test_openzfs_matrix.sh --extra tests/test_zfsbackup_real.py   # more test files (repeatable)
#   ./scenarios/test_openzfs_matrix.sh --formats-only       # provision + capture formats, no pytest
#   ./scenarios/test_openzfs_matrix.sh --no-formats         # pytest only
#   ./scenarios/test_openzfs_matrix.sh --keep-pools         # leave the format-capture pool in place
#   ./scenarios/test_openzfs_matrix.sh --purge              # delete the matrix VMs (only these three) at the end
#   SRC_REF=<commit-ish> ...     # ship that tree instead of HEAD; for uncommitted work: SRC_REF=$(git stash create)
#   MATRIX_LOG_DIR=<dir> ...     # where the per-VM logs go (default: a fresh mktemp dir, printed at the end)
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

PURGE=0; KEEP_POOLS=0; RUN_TESTS=1; RUN_FORMATS=1
ONLY=""
declare -a EXTRA_TESTS=()
while [ $# -gt 0 ]; do
    case "$1" in
        --purge)        PURGE=1 ;;
        --keep-pools)   KEEP_POOLS=1 ;;
        --no-formats)   RUN_FORMATS=0 ;;
        --formats-only) RUN_TESTS=0 ;;
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

mode_cleanup() {
    sudo zpool destroy -f zfsbfmt >/dev/null 2>&1 || true
    sudo rm -f "$WORK/zfsbfmt.img"
    for p in $(zpool list -H -o name 2>/dev/null | grep '^zfsbackup_pytest_'); do sudo zpool destroy -f "$p" >/dev/null 2>&1 || true; done
}

case "$mode" in
    provision) provision; rc=$? ;;
    pytest)    run_pytest "$@"; rc=$? ;;
    formats)   mode_formats; rc=$? ;;
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
chmod +x "$TMP_HOST/run.sh" "$TMP_HOST/formats.sh"

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
    multipass exec "$vm" -- bash "$GUEST_RUN" provision 2>&1 | tee "$LOG_DIR/$vm.provision.log"
    [ "${PIPESTATUS[0]}" -eq 0 ] || return 1
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
    if ! provision_vm "$vm" "$image"; then
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
        SUMMARY+=("----  $vm  [$zver]  (pytest not run: --formats-only)")
    fi
    if [ "$RUN_FORMATS" = 1 ]; then formats_vm "$vm"; fi
    if [ "$KEEP_POOLS" = 0 ]; then
        multipass exec "$vm" -- bash "$GUEST_RUN" cleanup 2>&1 | tee "$LOG_DIR/$vm.cleanup.log" >/dev/null
    fi
done

echo
echo "${BOLD}════════════ OpenZFS matrix summary ════════════${NC}"
for b in "${SUMMARY[@]}"; do echo "$b"; done
echo "logs: $LOG_DIR  (<vm>.pytest.log, <vm>.formats.log)"

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
