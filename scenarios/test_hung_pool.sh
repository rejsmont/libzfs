#!/usr/bin/env bash
#
# test_hung_pool.sh -- libzfseasy against a REALLY hung pool: the slow-command watchdog and `timeout()` (plan D6 / D7).
#
# One disposable Multipass VM, `zfsb-hang` (Ubuntu 24.04, OpenZFS 2.2, 2 GiB RAM), is created if missing and reused
# otherwise. In it the harness:
#   1. installs zfsutils-linux, dmsetup and python3, and ships `git archive $SRC_REF` (default HEAD;
#      SRC_REF=WORKTREE ships the working tree, untracked files included);
#   2. builds a file-backed pool on a device-mapper target (loop device -> dm `linear` -> `zpool create`), so that the
#      pool's I/O can be frozen at will with `dmsetup suspend`;
#   3. runs a small Python driver in the guest (as root) that hangs the pool and checks libzfseasy against it.
#
# HOW THE POOL IS HUNG. Freezing the device alone is not enough: `zfs list` and `zfs get` are answered from the ARC
# (a pool's metadata is read in full when it is imported), so they do not need the disk and keep working. What does
# hang them for real, and in the way production hangs look, is `spa_namespace_lock`: `zpool export` on a frozen
# device holds it while it waits for the final txg to reach the disk, and every later `zfs list`/`zfs get`/`zpool
# status` (any pool) then sleeps in `spa_open_common` in uninterruptible sleep (state D). SIGKILL does not end such a
# process; only `dmsetup resume` does (the export then completes, the pool is gone, the waiting commands return).
#
# WHAT IS CHECKED (the driver prints one PASS/FAIL line per check):
#   P0  baseline: on the healthy pool a timed `zfs.list` works and logs no warning
#   P1  the pool is hung (the export is in D state, an external `zfs list` is in D state, SIGKILL does not end it);
#       `slow_command_warning = 2`: a `with libzfseasy.timeout(3): zfs.list(...)` raises ZFSTimeoutError no sooner
#       than 3 s and within about 3 + 4 s (SIGTERM, then SIGKILL, 2 s each); the watchdog WARNING "is still running
#       after N s" fires at the configured interval (2.0 s, then 4.0 s, ...); the child survives SIGKILL (state D), one
#       WARNING says so, and the message names the pid
#   P2  the next TIMED call (list, and a different command, get) raises ZFSTimeoutError at once ("not started"), without
#       spawning a zfs process
#   P3  an UNTIMED call is not refused: run in a daemon thread it starts a new zfs child, which blocks (the thread
#       stays alive, the watchdog warns for the new pid); the thread is then abandoned. A timed call is still refused
#   P4  `dmsetup resume`: both stuck zfs children and the export finish; after the pool is imported again a timed call
#       works, quickly, and is not refused (the registry released the stuck child)
#
# Usage:
#   ./scenarios/test_hung_pool.sh            # provision (first time), ship the tree, run the driver
#   ./scenarios/test_hung_pool.sh --purge    # ... and delete the VM at the end (multipass delete --purge zfsb-hang)
#   SRC_REF=<commit-ish>|WORKTREE ./scenarios/test_hung_pool.sh
#   HANG_LOG_DIR=<dir>                       # where the host-side copy of the guest log goes (default: a mktemp dir)
#
# Exit status: 0 when every check passed, 1 when any failed or the run could not complete, 2 on usage errors.
# Requires: multipass on the host. Never touches ZFS on the host and never any VM but `zfsb-hang`. The VM may wedge
# (that is what it is for): it is disposable.

set -uo pipefail

HANG_VM="zfsb-hang"
IMAGE="${HANG_IMAGE:-24.04}"
VM_CPUS="${VM_CPUS:-2}"
VM_MEM="${VM_MEM:-2G}"
VM_DISK="${VM_DISK:-6G}"
SRC_REF="${SRC_REF:-HEAD}"
GUEST_DIR="/var/tmp/zfsb-hang"

PURGE=0
for arg in "$@"; do
    case "$arg" in
        --purge)   PURGE=1 ;;
        -h|--help) grep '^#' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *) echo "Unknown option: $arg" >&2; exit 2 ;;
    esac
done

if [ -t 1 ]; then
    RED=$'\033[0;31m'; GREEN=$'\033[0;32m'; YELLOW=$'\033[0;33m'; BLUE=$'\033[0;34m'; BOLD=$'\033[1m'; NC=$'\033[0m'
else
    RED=; GREEN=; YELLOW=; BLUE=; BOLD=; NC=
fi
info() { echo "${BLUE}==>${NC} $*"; }
step() { echo "${BOLD}${BLUE}::${NC} ${BOLD}$*${NC}"; }
warn() { echo "${YELLOW}WARN:${NC} $*" >&2; }
die()  { echo "${RED}FATAL:${NC} $*" >&2; exit 1; }

command -v multipass >/dev/null 2>&1 || die "multipass not found on host"
command -v git >/dev/null 2>&1 || die "git not found on host"

# ─────────────────────────────── Multipass helpers ──────────────────────────
# Never send an exec's HOST stdout to /dev/null (it spins forever): discard in the guest, or pipe on the host.
# A guest command that exits with status 4 hangs `multipass exec` forever: the guest runner remaps 4 -> 104.
mexec() { multipass exec "$HANG_VM" -- "$@"; }
msh()   { multipass exec "$HANG_VM" -- sudo bash -c "$*"; }
vm_exists()  { multipass list 2>/dev/null | awk -v n="$HANG_VM" 'NR>1 && $1==n {f=1} END{exit !f}'; }
vm_running() { multipass list 2>/dev/null | awk -v n="$HANG_VM" 'NR>1 && $1==n && tolower($2)=="running" {f=1} END{exit !f}'; }

ensure_vm() {
    if ! vm_exists; then
        step "Launching VM $HANG_VM (Ubuntu $IMAGE, $VM_CPUS cpus, $VM_MEM, $VM_DISK)"
        multipass launch "$IMAGE" --name "$HANG_VM" --cpus "$VM_CPUS" --memory "$VM_MEM" --disk "$VM_DISK" \
            || die "failed to launch $HANG_VM"
    elif ! vm_running; then
        step "Starting existing VM $HANG_VM"
        multipass start "$HANG_VM" || die "failed to start $HANG_VM"
    else
        info "VM $HANG_VM already running"
    fi
}

purge_vm() {
    step "Deleting VM $HANG_VM (multipass delete --purge: only this VM)"
    multipass delete --purge "$HANG_VM" || warn "could not delete $HANG_VM"
}

# ─────────────────────────────── The tree to ship ───────────────────────────
if [ "$SRC_REF" = "WORKTREE" ]; then
    wt_top="$(git -C "$(dirname "$0")" rev-parse --show-toplevel)" || die "not in a git repo"
    wt_idx="$(mktemp -u "${TMPDIR:-/tmp}/zfsb-wt-index.XXXXXX")"
    wt_tree="$(cd "$wt_top" && GIT_INDEX_FILE="$wt_idx" git read-tree HEAD && GIT_INDEX_FILE="$wt_idx" git add -A && GIT_INDEX_FILE="$wt_idx" git write-tree)" \
        || { rm -f "$wt_idx"; die "could not snapshot the working tree"; }
    rm -f "$wt_idx"
    SRC_REF="$(git -C "$wt_top" commit-tree "$wt_tree" -p HEAD -m 'hung-pool scenario: working-tree snapshot')" \
        || die "could not snapshot the working tree"
    info "SRC_REF=WORKTREE -> $SRC_REF (an unreferenced commit of the working tree)"
fi

TMP_HOST="$(mktemp -d)"
trap 'rm -rf "$TMP_HOST"' EXIT
LOG_DIR="${HANG_LOG_DIR:-$(mktemp -d "${TMPDIR:-/tmp}/zfsb-hang-logs.XXXXXX")}"
mkdir -p "$LOG_DIR" || die "cannot create $LOG_DIR"
LOG="$LOG_DIR/$HANG_VM.log"

# ─────────────────────────────── Guest scripts ──────────────────────────────
# provision.sh: as root, idempotent. cloud-init may still hold the apt lock on a fresh VM.
cat > "$TMP_HOST/provision.sh" <<'EOF_PROV'
#!/bin/bash
set -e
export DEBIAN_FRONTEND=noninteractive
cloud-init status --wait >/dev/null 2>&1 || true
need=""
command -v zfs      >/dev/null 2>&1 || need="$need zfsutils-linux"
command -v dmsetup  >/dev/null 2>&1 || need="$need dmsetup"
command -v python3  >/dev/null 2>&1 || need="$need python3"
command -v losetup  >/dev/null 2>&1 || need="$need mount"
if [ -n "$need" ]; then
    apt-get update -qq
    apt-get install -y -qq $need
fi
modprobe zfs
modprobe dm_mod 2>/dev/null || true
mkdir -p /var/tmp/zfsb-hang
zfs version
python3 --version
EOF_PROV

# run.sh: as root. Builds the dm-backed pool, runs the driver, ALWAYS resumes the device and tears everything down.
cat > "$TMP_HOST/run.sh" <<'EOF_RUN'
#!/bin/bash
WORK=/var/tmp/zfsb-hang
DEV=zfsb-hangdev
POOL=zfsb-hangpool
LOOP=""

teardown() {
    # the device must never be left suspended: every stuck process of the driver waits on it
    dmsetup resume "$DEV" >/dev/null 2>&1
    for i in $(seq 1 30); do
        pgrep -x 'zfs|zpool' >/dev/null 2>&1 || break
        sleep 1
    done
    zpool destroy -f "$POOL" >/dev/null 2>&1
    zpool export -f "$POOL" >/dev/null 2>&1
    dmsetup remove -f "$DEV" >/dev/null 2>&1
    [ -n "$LOOP" ] && losetup -d "$LOOP" >/dev/null 2>&1
    rm -f "$WORK/pool.img"
}
trap teardown EXIT
trap 'exit 130' INT TERM

# leftovers of an earlier run
dmsetup resume "$DEV" >/dev/null 2>&1
zpool destroy -f "$POOL" >/dev/null 2>&1; zpool export -f "$POOL" >/dev/null 2>&1
dmsetup remove -f "$DEV" >/dev/null 2>&1

set -e
rm -f "$WORK/pool.img"; truncate -s 512M "$WORK/pool.img"
LOOP="$(losetup -f --show "$WORK/pool.img")"
echo "0 $(blockdev --getsz "$LOOP") linear $LOOP 0" | dmsetup create "$DEV"
zpool create -f -o failmode=wait -O mountpoint=none -O compression=off "$POOL" "/dev/mapper/$DEV"
zfs create "$POOL/data"
zfs create "$POOL/data/child"
echo "pool $POOL on /dev/mapper/$DEV (loop $LOOP):"; zpool status "$POOL" | sed -n 1,10p
zfs version
set +e

PYTHONPATH="$WORK/src" timeout -k 15 300 python3 -u "$WORK/driver.py" "$POOL" "$DEV"
rc=$?
echo "driver exit status: $rc"
[ "$rc" -eq 4 ] && rc=104      # a guest exit status of 4 hangs `multipass exec`
exit "$rc"
EOF_RUN

# driver.py: the checks. Runs as root in the guest.
cat > "$TMP_HOST/driver.py" <<'EOF_DRIVER'
"""Hang a real pool and check libzfseasy's slow-command watchdog and timeout() against it."""
import logging
import os
import subprocess
import sys
import threading
import time

import libzfseasy as zfs
from libzfseasy.types import Filesystem

POOL, DEV = sys.argv[1], sys.argv[2]
ROOT = Filesystem(POOL)
PASS = FAIL = 0


def check(desc, ok, detail=''):
    global PASS, FAIL
    if ok:
        PASS += 1
        print(f'  PASS {desc}', flush=True)
    else:
        FAIL += 1
        print(f'  FAIL {desc}' + (f' ({detail})' if detail else ''), flush=True)


def step(text):
    print(f'\n:: {text}', flush=True)


def info(text):
    print(f'  .. {text}', flush=True)


def run(*argv, check_rc=True):
    return subprocess.run(argv, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                          text=True, check=check_rc).stdout


# ---- processes ---------------------------------------------------------------------------------------------------
def pids_of(*names):
    """pids of every process whose comm is one of `names`."""
    found = set()
    for entry in os.listdir('/proc'):
        if entry.isdigit():
            try:
                with open(f'/proc/{entry}/comm') as f:
                    if f.read().strip() in names and state_of(int(entry)) is not None:   # (not a zombie)
                        found.add(int(entry))
            except OSError:
                pass
    return found


def state_of(pid):
    """The one-letter state of `pid` ('D' = uninterruptible sleep), or None when it is gone (a zombie counts as gone)."""
    try:
        with open(f'/proc/{pid}/stat') as f:
            state = f.read().rsplit(')', 1)[1].split()[0]
    except (OSError, IndexError):
        return None
    return None if state in ('Z', 'X') else state


def wait_for(cond, seconds, step_s=0.2):
    end = time.monotonic() + seconds
    while time.monotonic() < end:
        if cond():
            return True
        time.sleep(step_s)
    return cond()


def wchan_of(pid):
    try:
        with open(f'/proc/{pid}/wchan') as f:
            return f.read().strip()
    except OSError:
        return ''


# ---- log capture ---------------------------------------------------------------------------------------------------
class Capture(logging.Handler):
    def __init__(self):
        super().__init__(logging.DEBUG)
        self.records = []

    def emit(self, record):
        self.records.append((time.monotonic(), record.levelno, record.getMessage()))

    def since(self, t0, needle):
        return [(t - t0, m) for t, lvl, m in self.records if t >= t0 and lvl >= logging.WARNING and needle in m]


cap = Capture()
lz_logger = logging.getLogger('libzfseasy')
lz_logger.addHandler(cap)
lz_logger.setLevel(logging.DEBUG)
lz_logger.propagate = False
STILL = 'is still running after'


def names_of(rows):
    return [getattr(d, 'name', str(d)) for d in rows]


export_proc = None
export_out = open('/var/tmp/zfsb-hang/export.out', 'w')


def hang_on():
    """Freeze the device, then start the `zpool export` that wedges spa_namespace_lock. Returns once an unrelated
    `zfs list` provably sleeps in D state behind it."""
    global export_proc
    run('dmsetup', 'suspend', DEV)
    export_proc = subprocess.Popen(['zpool', 'export', POOL], stdin=subprocess.DEVNULL, stdout=export_out,
                                   stderr=subprocess.STDOUT)
    ok = wait_for(lambda: state_of(export_proc.pid) == 'D', 15)
    check('the frozen device holds `zpool export` in uninterruptible sleep (state D)', ok,
          f'state={state_of(export_proc.pid)} wchan={wchan_of(export_proc.pid)}')
    return ok


def hang_off():
    run('dmsetup', 'resume', DEV, check_rc=False)


# ---- P0 ------------------------------------------------------------------------------------------------------------
step('P0 - baseline: a timed call on the healthy pool')
zfs.slow_command_warning = 2
t0 = time.monotonic()
with zfs.timeout(10):
    rows = names_of(zfs.list(ROOT))
dt = time.monotonic() - t0
check('timed zfs.list(%s) returns the pool' % POOL, POOL in rows, f'rows={rows}')
check('... quickly (< 2 s)', dt < 2, f'{dt:.2f}s')
check('... and logs no warning', not [m for _, lvl, m in cap.records if lvl >= logging.WARNING], str(cap.records))
check('no zfs/zpool process is running before the hang', not pids_of('zfs', 'zpool'), str(pids_of('zfs', 'zpool')))

# ---- P1 ------------------------------------------------------------------------------------------------------------
step('P1 - hung pool: watchdog at 2 s, timeout(3) -> ZFSTimeoutError within ~3 + 4 s')
if not hang_on():
    print('\ncould not hang the pool; the remaining checks would be meaningless', flush=True)
    print(f'SUMMARY pass={PASS} fail={FAIL + 1}', flush=True)
    sys.exit(1)
# an unrelated `zfs list` (not libzfseasy's) must sleep in D state behind the export: this proves the hang is real
probe = subprocess.Popen(['zfs', 'list', '-H', '-o', 'name', POOL], stdin=subprocess.DEVNULL,
                         stdout=subprocess.PIPE, stderr=subprocess.PIPE)
check('an external `zfs list` blocks in D state (spa_open_common)',
      wait_for(lambda: state_of(probe.pid) == 'D', 10), f'state={state_of(probe.pid)} wchan={wchan_of(probe.pid)}')
probe.kill()
time.sleep(1)
check('... and SIGKILL does not end it (uninterruptible)', state_of(probe.pid) == 'D', f'state={state_of(probe.pid)}')
probe_pid = probe.pid

before = pids_of('zfs')
t_start = time.monotonic()
raised = None
try:
    with zfs.timeout(3):
        list(zfs.list(ROOT))
except zfs.ZFSTimeoutError as e:
    raised = e
except BaseException as e:
    raised = e
elapsed = time.monotonic() - t_start
check('timeout(3) around zfs.list raises ZFSTimeoutError', isinstance(raised, zfs.ZFSTimeoutError), repr(raised))
check('... no sooner than 3 s', elapsed >= 3.0, f'{elapsed:.2f}s')
check('... and within about 3 + 4 s (SIGTERM, SIGKILL, 2 s each)', elapsed <= 8.5, f'{elapsed:.2f}s')
info(f'raised after {elapsed:.2f}s: {raised}')
check('the message says it timed out after 3s and names the pid that survived SIGKILL',
      'timed out after 3s' in str(raised) and 'did not exit after SIGKILL' in str(raised), str(raised))
warnings = cap.since(t_start, STILL)
info('watchdog WARNINGs: ' + '; '.join(f'+{t:.1f}s' for t, _ in warnings))
check('the watchdog fired while the call was running', len(warnings) >= 1, str(warnings))
if warnings:
    first_t, first_m = warnings[0]
    check('... the first at the configured 2 s', 1.8 <= first_t <= 3.0, f'+{first_t:.2f}s')
    check('... and its text reports "N s" of 2.0 s', 'after 2.0 s' in first_m or 'after 2.1 s' in first_m, first_m)
if len(warnings) >= 2:
    check('... then again at the next multiple (4 s)', 3.8 <= warnings[1][0] <= 5.0, f'+{warnings[1][0]:.2f}s')
new = pids_of('zfs') - before
check('exactly one new zfs child was started for the call', len(new) == 1, str(new))
stuck_pid = next(iter(new)) if new else None
check('the child survived SIGTERM and SIGKILL (state D)', stuck_pid is not None and state_of(stuck_pid) == 'D',
      f'pid={stuck_pid} state={state_of(stuck_pid) if stuck_pid else None}')
sk = cap.since(t_start, 'did not exit after SIGTERM and SIGKILL')
check('one WARNING says the child did not exit after SIGTERM and SIGKILL (and names its pid)',
      len(sk) == 1 and stuck_pid is not None and f'pid {stuck_pid}' in sk[0][1], str(sk))

# ---- P2 ------------------------------------------------------------------------------------------------------------
step('P2 - the next TIMED call fails fast, without spawning a child')
pids_before = pids_of('zfs', 'zpool')
for label, call in (('zfs.list', lambda: list(zfs.list(ROOT))),
                    ('zfs.get', lambda: list(zfs.get(ROOT, properties='used')))):
    t0 = time.monotonic()
    err = None
    try:
        with zfs.timeout(3):
            call()
    except BaseException as e:
        err = e
    dt = time.monotonic() - t0
    check(f'timed {label} raises ZFSTimeoutError', isinstance(err, zfs.ZFSTimeoutError), repr(err))
    check(f'... at once (< 0.5 s, not 3 s)', dt < 0.5, f'{dt:.2f}s')
    check(f'... saying it was not started and which pid is still running',
          err is not None and 'not started' in str(err) and str(stuck_pid) in str(err), str(err))
check('... and no zfs/zpool process was spawned by those calls', pids_of('zfs', 'zpool') == pids_before,
      f'{sorted(pids_before)} -> {sorted(pids_of("zfs", "zpool"))}')

# ---- P3 ------------------------------------------------------------------------------------------------------------
step('P3 - an UNTIMED call is not refused (it starts a child and blocks); the thread is abandoned')
box = {}


def untimed():
    try:
        box['rows'] = names_of(zfs.list(ROOT))
    except BaseException as e:      # noqa: BLE001 - recorded, checked after the resume
        box['error'] = e
    box['done'] = time.monotonic()


pids_before = pids_of('zfs')
t_untimed = time.monotonic()
thread = threading.Thread(target=untimed, name='untimed-list', daemon=True)
thread.start()
time.sleep(3.5)
new = pids_of('zfs') - pids_before
check('an untimed zfs.list started a new zfs child', len(new) == 1, str(new))
untimed_pid = next(iter(new)) if new else None
check('... the call is blocked (the thread is alive, nothing was raised)',
      thread.is_alive() and 'error' not in box and 'rows' not in box, str(box))
check('... its child sleeps in D state', untimed_pid is not None and state_of(untimed_pid) == 'D',
      f'state={state_of(untimed_pid) if untimed_pid else None}')
w = cap.since(t_untimed, STILL)
check('... and the watchdog warned for its pid at 2 s',
      any(untimed_pid is not None and f'pid {untimed_pid})' in m for _, m in w), str(w))
t0 = time.monotonic()
err = None
try:
    with zfs.timeout(3):
        list(zfs.list(ROOT))
except BaseException as e:
    err = e
check('a timed call is still refused at once while the untimed one runs',
      isinstance(err, zfs.ZFSTimeoutError) and time.monotonic() - t0 < 0.5, repr(err))
# nested timeout(None) lifts the limit: also not refused (it blocks like any untimed call)
box2 = {}


def lifted():
    try:
        with zfs.timeout(3):
            with zfs.timeout(None):
                box2['rows'] = names_of(zfs.list(ROOT))
    except BaseException as e:      # noqa: BLE001
        box2['error'] = e


pids_before = pids_of('zfs')
thread2 = threading.Thread(target=lifted, name='lifted-list', daemon=True)
thread2.start()
time.sleep(2)
check('timeout(None) inside timeout(3) lifts the limit: the call is not refused and blocks',
      thread2.is_alive() and 'error' not in box2 and len(pids_of('zfs') - pids_before) == 1, f'{box2}')

# ---- P4 ------------------------------------------------------------------------------------------------------------
step('P4 - dmsetup resume: the stuck children exit, timed calls work again')
t_resume = time.monotonic()
hang_off()
gone = wait_for(lambda: all(state_of(p) is None for p in (probe_pid, stuck_pid, untimed_pid) if p), 30)
check('the stuck zfs children (external probe, timed-out call, untimed call) exited after the resume', gone,
      str({p: state_of(p) for p in (probe_pid, stuck_pid, untimed_pid) if p}))
probe.wait()      # reap the external probe (killed above, it could only exit now)
check('the blocked `zpool export` completed', wait_for(lambda: export_proc.poll() is not None, 30),
      f'rc={export_proc.poll()}')
check('the abandoned thread returned (it was never killed)', wait_for(lambda: not thread.is_alive(), 20),
      str(box))
info(f'untimed call ended with: {box.get("error") or box.get("rows")}')
wait_for(lambda: not thread2.is_alive(), 20)
# the export removed the pool; bring it back so there is something to list
out = run('zpool', 'import', '-d', '/dev/mapper', POOL, check_rc=False)
check('the pool imports again', POOL in run('zpool', 'list', '-H', '-o', 'name', check_rc=False), out)
t0 = time.monotonic()
err = None
rows = []
try:
    with zfs.timeout(5):
        rows = names_of(zfs.list(ROOT))
except BaseException as e:
    err = e
dt = time.monotonic() - t0
check('a timed call works again (the registry released the stuck child)', err is None and POOL in rows,
      f'{err!r} rows={rows}')
check('... quickly (< 2 s)', dt < 2, f'{dt:.2f}s')
with zfs.timeout(5):
    check('another timed command works too (zfs.get)', len(list(zfs.get(ROOT, properties='used'))) >= 1)
check('no zfs/zpool process is left behind', wait_for(lambda: not pids_of('zfs', 'zpool'), 10),
      str(pids_of('zfs', 'zpool')))

print(f'\nSUMMARY pass={PASS} fail={FAIL}', flush=True)
sys.exit(1 if FAIL else 0)
EOF_DRIVER

# ─────────────────────────────── Main ────────────────────────────────────────
echo "${BOLD}=================================================================${NC}"
echo "${BOLD} libzfseasy against a hung pool (slow-command watchdog, timeout())${NC}"
echo "${BOLD}=================================================================${NC}"

ensure_vm

step "Provisioning $HANG_VM (ZFS, dmsetup, python3)"
multipass transfer "$TMP_HOST/provision.sh" "$HANG_VM:/tmp/zfsb-hang-provision.sh" || die "transfer of provision.sh failed"
msh "bash /tmp/zfsb-hang-provision.sh" || die "provisioning failed"

step "Shipping the tree ($(git rev-parse --short "$SRC_REF" 2>/dev/null || echo "$SRC_REF"))"
git archive --format=tar.gz -o "$TMP_HOST/src.tar.gz" "$SRC_REF" || die "git archive $SRC_REF failed"
multipass transfer "$TMP_HOST/src.tar.gz" "$HANG_VM:/tmp/zfsb-hang-src.tar.gz" || die "transfer of the tree failed"
multipass transfer "$TMP_HOST/run.sh" "$HANG_VM:/tmp/zfsb-hang-run.sh"       || die "transfer of run.sh failed"
multipass transfer "$TMP_HOST/driver.py" "$HANG_VM:/tmp/zfsb-hang-driver.py" || die "transfer of driver.py failed"
msh "mkdir -p '$GUEST_DIR' && rm -rf '$GUEST_DIR/src' && mkdir '$GUEST_DIR/src' && tar -xzf /tmp/zfsb-hang-src.tar.gz -C '$GUEST_DIR/src' \
     && install -m755 /tmp/zfsb-hang-run.sh '$GUEST_DIR/run.sh' && install -m644 /tmp/zfsb-hang-driver.py '$GUEST_DIR/driver.py' \
     && rm -f /tmp/zfsb-hang-src.tar.gz /tmp/zfsb-hang-run.sh /tmp/zfsb-hang-driver.py" || die "could not install the tree and scripts"

step "Running the driver in $HANG_VM (the pool is hung on purpose; log: $LOG)"
msh "bash '$GUEST_DIR/run.sh'" 2>&1 | tee "$LOG"
guest_rc="${PIPESTATUS[0]}"

# The guest trap already resumed the device. A last, independent resume costs nothing and a wedged VM is disposable.
msh "dmsetup resume zfsb-hangdev >/dev/null 2>&1; true" || true

pass_count="$(grep -c '^  PASS ' "$LOG" || true)"
fail_count="$(grep -c '^  FAIL ' "$LOG" || true)"
summary="$(grep '^SUMMARY ' "$LOG" | tail -n1 || true)"
echo
echo "${BOLD}=================================================================${NC}"
echo "${BOLD} Results${NC}"
echo "${BOLD}=================================================================${NC}"
grep -E '^  FAIL ' "$LOG" | sed "s/^  FAIL /  ${RED}FAIL${NC} /" || true
echo "  ${summary:-no SUMMARY line: the driver did not finish} (host count: ${pass_count} passed, ${fail_count} failed)"
echo "  guest exit status: $guest_rc   log: $LOG"

if [ "$PURGE" -eq 1 ]; then
    purge_vm
else
    info "VM $HANG_VM left in place for reuse (it is disposable: --purge, or multipass delete --purge $HANG_VM)"
fi

if [ "$guest_rc" -eq 0 ] && [ "$fail_count" -eq 0 ] && [ -n "$summary" ]; then
    echo "${GREEN}${BOLD}All checks passed.${NC}"
    exit 0
fi
echo "${RED}${BOLD}FAILED${NC}"
exit 1
