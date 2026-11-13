#!/usr/bin/env bash
#
# test_parallel_senders.sh -- the remote worker's PARALLEL SENDER THREADS on real ZFS and a real network (sender rework,
# Track B; plan docs/sender_rework_plan.md item B4; report docs/parallel_senders_report.md).
#
# The remote worker's main thread is a scheduler (zfsbackup/sendloop.py); every due (dataset, destination) pair runs in its
# own short-lived sender thread, at most 4 in all and 2 per destination (`sendschedule.MAX_SENDERS` /
# `MAX_SENDERS_PER_DESTINATION`, constants). This harness drives the real daemon (a transient systemd unit, KillMode=mixed
# and TimeoutStopSec=90 like the shipped unit) on TWO existing Multipass VMs, never launches or deletes one:
#   zfsb-local   the SENDER (Ubuntu 24.04, OpenZFS 2.2, 2 vCPUs: the only VM a pool may be hung on). Pools: zpsrc (source),
#                zptgt (local targets zptgt/a, zptgt/b), zphang (scenario 4 only: a dm-linear target that is frozen)
#   zfsb-server  the SERVER (Ubuntu 26.04, 1 vCPU): zpsrv, target zpsrv/backups, the daemon with `remote_backup.enabled`
# The sender is enrolled the operator's way (`trust token create` -> token FILE -> `enroll`, mutual TLS, pinned server). Its
# datasets go to up to two destinations at once: `remote1` (https://<server>:8080) and `local1`/`local2`/`hung1` (`zfs:<ds>`).
# Everything is judged by ZFS state (guids, anchors, resume tokens) and by SAMPLED PROCESS TABLES (a guest-side sampler records
# every change of the live `zfs send`/`zfs receive` processes every 0.1 s: that is how overlap and the caps are shown);
# log lines are evidence, never the verdict. To put several pairs on the scheduler's wake-up at the same instant (the per-pair
# stagger would otherwise spread them over seconds) the harness picks DATASET NAMES whose hash fractions
# (`sendschedule.pair_fraction`) are within a tiny window: it changes nothing in the daemon, only which names get used.
#
# Scenarios (numbers follow the task list):
#   1  OVERLAP: six datasets, each to remote1 AND local1 (12 pairs); the unit is CPU-capped so sends last seconds. The sampler
#      shows >= 3 sends alive at once and never more than 4 in all, 2 local receives, 2 receives on the server; all 12 copies
#      are complete (guids equal, anchors on the newest snapshot), no ERROR, no Traceback, the status file has 12 ok records; after the
#      sends the daemon is watched for 60 s: every pair sent exactly once and attempted no more often than its period allows
#   2  RELOAD DURING A LONG SEND: two datasets x two destinations with a three-step incremental backlog (about 100 MiB a step,
#      CPU-capped); once the steps are in flight `zfsbackup-config set prune_interval ...` reloads: every in-flight step is
#      FINISHED (logged "Backup complete" before the worker's exit), the remote worker exits and is replaced, no stream is torn
#      (no receive_resume_token, no `Resumed`), nothing is sent from scratch (no `Sending full` after the reload), the chain
#      continues from the anchor, `applied=` reaches `target=`, and the copies end with every snapshot.
#   2b (same flag) the same with a destination frequency (5 min) LONGER than the dataset's (30 s): the reload lands between chain steps at
#      the pairs' 5-minute slot; the status file shows `incomplete: true` written by the old worker, and the successor continues the cut
#      chain at start (inside the catch-up delay, not at the next 300 s slot): no step twice, no full send, no torn stream
#   3  CONCURRENT LOCAL SENDS: siblings and cousins whose containers (<target>/<id>/<pool>/<path>) are all MISSING are sent to
#      one local target at the same instant (start-aligned names, R3_ROUNDS = 18 rounds on fresh trees; the negotiate claim and the re-read of an unowned-looking container): all
#      copies end present and equal, NO ownership ERROR ("not recorded as a backup": fails the round), no Traceback, and the last status
#      record of every pair is ok (no "busy" left). A zfs-only probe first shows that a freshly created container is visible to
#      `zfs list` before its client_id is readable; the busy path (INFO) and the reads inside that window are counted
#   4  HUNG TARGET POOL (zfsb-local only): the local target pool (a file-backed vdev on a dm-linear device, failmode=wait) is
#      FROZEN with `dmsetup suspend` while a send to it runs: the receive blocks in the kernel, the first timed zfs call on the pool
#      runs out its 60 s budget and its child is abandoned (it survives SIGKILL), the daemon keeps sending to the other
#      destination (the backlog and two later snapshots reach the server during the hang), the local destination is skipped with
#      ONE WARNING per pair and episode (the repeats are DEBUG), at most 4 sends and 2 receives into the frozen pool ever, a
#      second, HEALTHY local destination (local1) is skipped as well (the stuck-child registry is per process: the documented
#      trade-off), `systemctl stop` is followed by the supervisor's exit inside its budget (the remote worker is terminated after
#      the 30 s shutdown budget; inside TimeoutStopSec=90) with no daemon process left, and after `dmsetup resume` the strays
#      exit, the pool is healthy and a restarted daemon completes every copy (a cut receive is RESUMED). About 8 minutes
#   5  THROUGHPUT at 1, 2 and 4 concurrent senders (1, 2 pairs to local1; 2 pairs to local1+local2; and the same through remote1,
#      remote1+local1), MiB/s per configuration next to a raw `zfs send | zfs receive` (`| nc |` for the network leg) run of the same
#      data with the same concurrency, each THRU_REPS times, with the daemon's own CPU (Python) per MiB against its children's: is the
#      aggregate GIL-bound? Containers are pre-created (the first sends would otherwise queue on the claim); the daemon runs without -v
#
# Usage (from the repository root):
#   ./scenarios/test_parallel_senders.sh                 # everything, then remove the daemons, tc, pools (VMs are never deleted)
#   ./scenarios/test_parallel_senders.sh --only-4        # one scenario (several --only-N combine); --no-N skips one
#   ./scenarios/test_parallel_senders.sh --keep          # leave daemons and pools in place
#   ./scenarios/test_parallel_senders.sh --reuse         # reuse an environment a previous --keep left (pools, enrollment)
#   SRC_REF=<commit-ish>|WORKTREE ...                    # the tree shipped to both VMs (default HEAD)
#
# Env: SENDER_VM (zfsb-local) / SERVER_VM (zfsb-server) (must start with zfsb-; zfsb-zfs* and foam are refused),
#      API_PORT (8080), DISK_SIZE (5g, sparse), IMG_DIR (/var/tmp), PS_LOG_DIR (host copy of logs/samples/analyses),
#      S1_MIB (192: size of each scenario 1 dataset), S2_MIB (100: a scenario 2 step), S4_MIB (256: scenario 4 backlog),
#      THRU_MIB (512: scenario 5 dataset for local destinations), THRU_REMOTE_MIB (160: when remote1 is a destination: the 1 GiB server
#      is OOM-killed by two concurrent receives of 600 MiB), THRU_REPS (2), CPU_QUOTA (40%: unit CPUQuota of scenarios 1, 2 and 4;
#      scenario 5 and 3 run uncapped).
#
# Exit status: 0 when every assertion passed, 1 otherwise or when the run could not complete, 2 on usage errors.
# macOS/Multipass: never redirect a `multipass exec`'s host stdout to /dev/null (discard in the guest); config goes in with
# `multipass transfer`; a guest status 4 is remapped to 104; long pipelines are filtered IN the guest.

set -uo pipefail

# ─────────────────────────────── Configuration ──────────────────────────────
SND="${SENDER_VM:-zfsb-local}"
SRV="${SERVER_VM:-zfsb-server}"
API_PORT="${API_PORT:-8080}"
SRC_REF="${SRC_REF:-HEAD}"
IMG_DIR="${IMG_DIR:-/var/tmp}"
DISK_SIZE="${DISK_SIZE:-5g}"
S1_MIB="${S1_MIB:-192}"
S2_MIB="${S2_MIB:-100}"
S4_MIB="${S4_MIB:-256}"
THRU_MIB="${THRU_MIB:-512}"
THRU_REMOTE_MIB="${THRU_REMOTE_MIB:-160}"
THRU_REPS="${THRU_REPS:-2}"
CPU_QUOTA="${CPU_QUOTA-40%}"

SRC_POOL="zpsrc"
TGT_POOL="zptgt"; TGT_A="zptgt/a"; TGT_B="zptgt/b"
HANG_POOL="zphang"; HANG_T="zphang/zfsbackup"; HANG_DEV="zfsb-psdev"; HANG_IMG="${IMG_DIR}/zphang.img"
SRV_POOL="zpsrv"; SRV_TARGET="zpsrv/backups"
CID="psend1"

SRC_DIR="/opt/zfsbackup-src"
SRC_PP="${SRC_DIR}:${SRC_DIR}/libs/libzfseasy"
VENV="/opt/zfsbvenv"; PY="${VENV}/bin/python"; PIP="${VENV}/bin/pip"
CFG="/etc/zfsbackup/config.yaml"
DB="/var/lib/zfsbackup/config.db"
TLS_DIR="${DB}.tls"
LOG="/var/log/zfsbackup-ps.log"
GDIR="/opt/zfsb-ps"                 # helper scripts in the guests
WDIR="/var/tmp/zfsb-ps"             # samples and analyses in the guests
JGET_PY="${GDIR}/jget.py"
UNIT="zfsbackup"
DAEMON_V="-v"                       # the daemon's verbosity flag; scenario 5 runs it without (the shipped unit does not use -v)

# Flags
KEEP=0; KEEP_POOLS=0; REUSE=0
declare -a ONLY=() NOT=()
for arg in "$@"; do
    case "$arg" in
        --keep)       KEEP=1; KEEP_POOLS=1 ;;
        --keep-pools) KEEP_POOLS=1 ;;
        --reuse)      REUSE=1 ;;
        --purge)      ;;     # accepted for symmetry with the other harnesses: this one never deletes a VM
        --only-[1-5]) ONLY+=("${arg#--only-}") ;;
        --no-[1-5])   NOT+=("${arg#--no-}") ;;
        -h|--help)    grep '^#' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *) echo "Unknown option: $arg" >&2; exit 2 ;;
    esac
done
scn() { # scn <n>: does scenario <n> run?
    local x
    if [ "${#ONLY[@]}" -gt 0 ]; then for x in "${ONLY[@]}"; do [ "$x" = "$1" ] && return 0; done; return 1; fi
    if [ "${#NOT[@]}" -gt 0 ]; then for x in "${NOT[@]}"; do [ "$x" = "$1" ] && return 1; done; fi
    return 0
}

# ─────────────────────────────── Output helpers ─────────────────────────────
if [ -t 1 ]; then
    RED=$'\033[0;31m'; GREEN=$'\033[0;32m'; YELLOW=$'\033[0;33m'; BLUE=$'\033[0;34m'; BOLD=$'\033[1m'; NC=$'\033[0m'
else
    RED=; GREEN=; YELLOW=; BLUE=; BOLD=; NC=
fi
PASS_COUNT=0; FAIL_COUNT=0
declare -a RESULTS=()
declare -a FINDINGS=()
info()  { echo "${BLUE}==>${NC} $*"; }
step()  { echo "${BOLD}${BLUE}::${NC} ${BOLD}$*${NC}"; }
warn()  { echo "${YELLOW}WARN:${NC} $*" >&2; }
die()   { echo "${RED}FATAL:${NC} $*" >&2; exit 1; }
finding() { FINDINGS+=("$1"); echo "  ${YELLOW}! FINDING${NC} $1"; }
pass()  { PASS_COUNT=$((PASS_COUNT+1)); RESULTS+=("${GREEN}PASS${NC}  $1"); echo "  ${GREEN}✓ PASS${NC} $1"; }
fail()  { FAIL_COUNT=$((FAIL_COUNT+1)); RESULTS+=("${RED}FAIL${NC}  $1"); echo "  ${RED}✗ FAIL${NC} $1"; }
check() { if [ "$2" = "$3" ]; then pass "$1"; else fail "$1 (expected='$2' actual='$3')"; fi; }
ge()    { # ge "<desc>" <min> <actual>   (integers or decimals)
    if awk -v a="$3" -v m="$2" 'BEGIN{exit !(a+0 >= m+0)}'; then pass "$1 (>= $2: $3)"; else fail "$1 (expected >= $2, actual='$3')"; fi
}
le()    { if awk -v a="$3" -v m="$2" 'BEGIN{exit !(a+0 <= m+0)}'; then pass "$1 (<= $2: $3)"; else fail "$1 (expected <= $2, actual='$3')"; fi; }
gq()    { grep "$@" >/dev/null; }
check_has() { if printf '%s\n' "$3" | grep -E -- "$2" >/dev/null; then pass "$1"; else fail "$1 (no match for /$2/ in: $(printf '%s' "$3" | head -c 300 | tr '\n' '|'))"; fi; }
check_lacks() { if printf '%s\n' "$3" | grep -E -- "$2" >/dev/null; then fail "$1 (found /$2/: $(printf '%s\n' "$3" | grep -E -- "$2" | head -n2 | tr '\n' '|'))"; else pass "$1"; fi; }

for _vm in "$SND" "$SRV"; do
    case "$_vm" in
        zfsb-zfs*) die "$_vm belongs to another harness (the OpenZFS matrix): refused" ;;
        zfsb-*) ;;
        *) die "VM names must start with 'zfsb-' (got '$_vm'): this script never touches any other VM" ;;
    esac
done
command -v multipass >/dev/null 2>&1 || die "multipass not found on host"
command -v git >/dev/null 2>&1 || die "git not found on host"

# ─────────────────────────────── Multipass helpers ──────────────────────────
# `multipass exec` busy-spins FOREVER when the guest command writes to stdout and the HOST sends that stdout to /dev/null:
# never do that, discard inside the guest. A guest command that exits 4 hangs it too (status 4 becomes 104). Config and
# scripts go in with `multipass transfer`, never through piped stdin. Long pipelines (log | grep | cut | tail) are filtered IN
# the guest too: they wedge the exec as well.
REMAP='bash -c "$1"; rc=$?; [ "$rc" -eq 4 ] && rc=104; exit "$rc"'
msh()  { local vm="$1"; shift; multipass exec "$vm" -- sudo bash -c "$REMAP" _ "$*"; }
snd()  { msh "$SND" "$@"; }
srv()  { msh "$SRV" "$@"; }
vm_exists()  { multipass list 2>/dev/null | awk -v n="$1" 'NR>1 && $1==n {f=1} END{exit !f}'; }
vm_running() { multipass list 2>/dev/null | awk -v n="$1" 'NR>1 && $1==n && tolower($2)=="running" {f=1} END{exit !f}'; }
vm_ip()      { multipass list 2>/dev/null | awk -v n="$1" 'NR>1 && $1==n {print $3; exit}'; }
trim() { tr -d '[:space:]'; }

WORK="$(mktemp -d "${TMPDIR:-/tmp}/zfsb-ps.XXXXXX")" || die "mktemp failed"
LOG_DIR="${PS_LOG_DIR:-$(mktemp -d "${TMPDIR:-/tmp}/zfsb-ps-logs.XXXXXX")}"
mkdir -p "$LOG_DIR" || die "cannot create $LOG_DIR"

ensure_vm() {
    vm_exists "$1" || die "VM $1 does not exist; this harness never launches a VM"
    if ! vm_running "$1"; then step "Starting existing VM $1"; multipass start "$1" || die "failed to start $1"; fi
}
put_file() { # put_file <vm> <host-file> <guest-path> <mode>
    local base="/tmp/zbps-xfer.$$.$RANDOM"
    multipass transfer "$2" "$1:$base" || die "transfer of $2 failed"
    msh "$1" "install -D -m '$4' '$base' '$3' && rm -f '$base'" || die "install of $3 failed"
}
get_file() { # get_file <vm> <guest-path> <host-file>: copy a guest file out through a world-readable temp copy
    local base="/tmp/zbps-get.$$.$RANDOM"
    msh "$1" "cp '$2' '$base' && chmod 644 '$base'" || return 1
    multipass transfer "$1:$base" "$3" >/dev/null 2>&1; local rc=$?
    msh "$1" "rm -f '$base'" || true
    return $rc
}

# ─────────────────────────────── Guest helper scripts ───────────────────────
write_guest_scripts() {
    cat > "$WORK/jget.py" <<'EOF_JGET'
import json, sys
d = json.load(sys.stdin)
v = eval(sys.argv[1], {"d": d})
print("" if v is None else v)
EOF_JGET

    # sampler.py <out> [interval]: appends a line whenever the set of live `zfs send` / `zfs receive` processes (or the number of
    # them in uninterruptible sleep) changes: ts <TAB> sends <TAB> receives <TAB> D-state <TAB> S:pid:state:name;R:pid:state:name
    # (`zfs send -n...`, the resume-token probe, is ignored). Stops cleanly on SIGTERM with an END line.
    cat > "$WORK/sampler.py" <<'EOF_SAMPLER'
import os, signal, sys, time
out, interval = sys.argv[1], (float(sys.argv[2]) if len(sys.argv) > 2 else 0.1)
fh = open(out, 'a', buffering=1)
def procs():
    items = []
    for e in os.listdir('/proc'):
        if not e.isdigit():
            continue
        try:
            with open('/proc/%s/cmdline' % e, 'rb') as f:
                raw = f.read()
            if not raw:
                continue
            a = raw.rstrip(b'\0').split(b'\0')
            if len(a) < 2 or os.path.basename(a[0]) != b'zfs' or a[1] not in (b'send', b'receive', b'recv'):
                continue
            if a[1] == b'send' and any(x.startswith(b'-') and not x.startswith(b'--') and b'n' in x[1:] for x in a[2:]):
                continue
            with open('/proc/%s/stat' % e) as f:
                st = f.read().rsplit(')', 1)[1].split()[0]
            if st in ('Z', 'X'):
                continue
        except (OSError, IndexError):
            continue
        items.append(('S' if a[1] == b'send' else 'R', int(e), st, a[-1].decode('utf-8', 'replace')))
    return sorted(items)
def stop(*_):
    fh.write('%.3f\tEND\n' % time.time()); fh.close(); sys.exit(0)
signal.signal(signal.SIGTERM, stop); signal.signal(signal.SIGINT, stop)
last = None
while True:
    it = procs()
    sig = [(k, p, s == 'D') for k, p, s, _ in it]
    if sig != last:
        last = sig
        fh.write('%.3f\t%d\t%d\t%d\t%s\n' % (time.time(), sum(1 for k, *_ in it if k == 'S'), sum(1 for k, *_ in it if k == 'R'),
                 sum(1 for _, _, s, _ in it if s == 'D'), ';'.join('%s:%d:%s:%s' % i for i in it)))
    time.sleep(interval)
EOF_SAMPLER

    # analyze.py <samples> [t0 [t1]]: key=value lines. max_send / max_recv / max_d, seconds with >= 1..4 sends alive, the first
    # send's start, the last one's end, and one `send <pid> <start> <end> <name>` line per send process.
    cat > "$WORK/analyze.py" <<'EOF_ANALYZE'
import sys
path = sys.argv[1]
t0 = float(sys.argv[2]) if len(sys.argv) > 2 and sys.argv[2] != '-' else None
t1 = float(sys.argv[3]) if len(sys.argv) > 3 and sys.argv[3] != '-' else None
pfx = sys.argv[4] if len(sys.argv) > 4 else None       # max_recv_pfx: the most receives at once whose target starts with this
rows = []
for line in open(path):
    f = line.rstrip('\n').split('\t')
    if len(f) < 5:
        if len(f) == 2 and f[1] == 'END':
            rows.append((float(f[0]), None))
        continue
    items = []
    for s in filter(None, f[4].split(';')):
        k, pid, st, name = s.split(':', 3)
        items.append((k, int(pid), st, name))
    rows.append((float(f[0]), items))
if not rows:
    print('max_send=0\nmax_recv=0\nmax_d=0\nnsends=0'); sys.exit(0)
end = t1 if t1 is not None else rows[-1][0]
mx_s = mx_r = mx_d = mx_p = 0
sec = [0.0] * 5
life = {}
for i, (ts, items) in enumerate(rows):
    nxt = rows[i + 1][0] if i + 1 < len(rows) else end
    a, b = max(ts, t0 if t0 is not None else ts), min(nxt, end)
    if items is None or b <= a:
        continue
    ns = sum(1 for k, *_ in items if k == 'S'); nr = sum(1 for k, *_ in items if k == 'R')
    nd = sum(1 for _, _, s, _ in items if s == 'D')
    mx_s, mx_r, mx_d = max(mx_s, ns), max(mx_r, nr), max(mx_d, nd)
    if pfx:
        mx_p = max(mx_p, sum(1 for k, _, _, name in items if k == 'R' and name.startswith(pfx)))
    for n in range(1, 5):
        if ns >= n:
            sec[n] += b - a
    for k, pid, st, name in items:
        if k == 'S':
            e = life.setdefault(pid, [a, b, name]); e[1] = b
print('max_send=%d\nmax_recv=%d\nmax_d=%d\nmax_recv_pfx=%d' % (mx_s, mx_r, mx_d, mx_p))
for n in range(1, 5):
    print('sec_ge%d=%.2f' % (n, sec[n]))
if life:
    print('first_send=%.3f\nlast_send_end=%.3f\nwindow=%.2f\nnsends=%d' % (
        min(v[0] for v in life.values()), max(v[1] for v in life.values()),
        max(v[1] for v in life.values()) - min(v[0] for v in life.values()), len(life)))
    for pid, (a, b, name) in sorted(life.items(), key=lambda kv: kv[1][0]):
        print('send %d %.3f %.3f %s' % (pid, a, b, name))
else:
    print('nsends=0')
EOF_ANALYZE

    # logoverlap.py < filtered daemon log lines: sends as the DAEMON LOG shows them. A send runs from its "Sending full snapshot
    # DS@SNAP" / "Sending incremental DS: A -> SNAP" line to the matching "Backup complete: DS@SNAP" line (both logged by the sender
    # thread, so the interval lies inside the thread's life); prints the peak number alive at once in all, per transport, and the
    # seconds with >= 2/3/4 alive
    cat > "$WORK/logoverlap.py" <<'EOF_LOGOV'
import re, sys, datetime
rx = re.compile(r'(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d),(\d{3}) - zfsbackup\.(remote|local) - INFO - (.*)')
starts, ivs = {}, []
for line in sys.stdin:
    m = rx.match(line)
    if not m:
        continue
    t = datetime.datetime.strptime(m.group(1), '%Y-%m-%d %H:%M:%S').timestamp() + int(m.group(2)) / 1000.0
    lg, msg = m.group(3), m.group(4)
    a = re.match(r'Sending full snapshot (\S+)@(\S+)$', msg) or re.match(r'Sending incremental (\S+): \S+ -> (\S+)$', msg)
    if a:
        starts[(lg, a.group(1), a.group(2))] = t
        continue
    c = re.match(r'Backup complete: (\S+)@(\S+) ', msg)
    if c and (lg, c.group(1), c.group(2)) in starts:
        ivs.append((starts.pop((lg, c.group(1), c.group(2))), t, lg))
ev = sorted([(a, 1, lg) for a, b, lg in ivs] + [(b, -1, lg) for a, b, lg in ivs])
cur = {'remote': 0, 'local': 0}; mx = {'total': 0, 'remote': 0, 'local': 0}; sec = [0.0] * 5; last = None
for t, d, lg in ev:
    if last is not None:
        tot = cur['remote'] + cur['local']
        for n in range(1, 5):
            if tot >= n:
                sec[n] += t - last
    cur[lg] += d; last = t
    mx['total'] = max(mx['total'], cur['remote'] + cur['local']); mx[lg] = max(mx[lg], cur[lg])
print('log_sends=%d\nlog_max_total=%d\nlog_max_remote=%d\nlog_max_local=%d' % (len(ivs), mx['total'], mx['remote'], mx['local']))
for n in range(2, 5):
    print('log_sec_ge%d=%.2f' % (n, sec[n]))
EOF_LOGOV

    # pickalign.py <client id> <dests comma> <window> <template>...: dataset names (template with {n}) whose pair fractions
    # (sendschedule.pair_fraction) for EVERY destination lie within one window of <window>, so the scheduler finds all those pairs
    # due at (nearly) the same instant: due = start + frac * min(W, 300 s). Prints one name per template.
    cat > "$WORK/pickalign.py" <<'EOF_PICK'
import bisect, sys
from zfsbackup.sendschedule import pair_fraction
cid, dests, w, templates = sys.argv[1], sys.argv[2].split(','), float(sys.argv[3]), sys.argv[4:]
N = 120000
cand = []
for t in templates:
    rows = []
    for n in range(N):
        name = t.replace('{n}', str(n))
        fr = [pair_fraction(cid, name, d) for d in dests]
        if max(fr) - min(fr) <= w:
            rows.append((min(fr), max(fr), name))
    rows.sort()
    cand.append(rows)
for lo0, hi0, name0 in sorted(cand[0], key=lambda r: r[1] - r[0]):
    picks, ok = [name0], True
    for rows in cand[1:]:
        i = bisect.bisect_left(rows, (lo0 - w, -1, ''))
        hit = None
        while i < len(rows) and rows[i][0] <= lo0 + w:
            if rows[i][0] >= min(lo0, rows[i][0]) and max(hi0, rows[i][1]) - min(lo0, rows[i][0]) <= w:
                hit = rows[i][2]; break
            i += 1
        if hit is None:
            ok = False; break
        picks.append(hit)
    if ok:
        print('\n'.join(picks)); sys.exit(0)
sys.exit(1)
EOF_PICK

    # mkdata.sh <dataset> <MiB> [file]: create the dataset if missing and append <MiB> MiB of incompressible data to <file>
    cat > "$WORK/mkdata.sh" <<'EOF_MKDATA'
#!/bin/bash
ds="$1"; mib="$2"; f="${3:-data.bin}"
zfs list -H -o name "$ds" >/dev/null 2>&1 || zfs create -p "$ds" || exit 1
mp="$(zfs get -H -o value mountpoint "$ds")"
base=/var/tmp/zfsb-ps/base.bin
[ -s "$base" ] || head -c 8388608 /dev/urandom > "$base"
n=$(( (mib + 7) / 8 ))
for i in $(seq 1 "$n"); do cat "$base"; done >> "$mp/$f"
EOF_MKDATA

    # anchorcheck.sh <dest> <ERE of datasets>: do the datasets' org.zfsbackup:anchor.<dest> properties name their newest snapshot?
    # prints SAME <n> or DIFF <n anchors> <n datasets with snapshots>
    cat > "$WORK/anchorcheck.sh" <<'EOF_ANCHOR'
#!/bin/bash
dest="$1"; pat="$2"; pool="${3:-zpsrc}"
newest="$(zfs list -H -r -t snapshot -s createtxg -o name "$pool" | awk -F@ '{n[$1]=$2} END{for(d in n) print d" "n[d]}' | grep -E "$pat" | sort)"
anch="$(zfs get -H -r -t filesystem,volume -s local -o name,value "org.zfsbackup:anchor.$dest" "$pool" | awk '$2!="-"{print $1" "$2}' | grep -E "$pat" | sort)"
if [ -n "$anch" ] && [ "$newest" = "$anch" ]; then echo "SAME $(printf '%s\n' "$anch" | wc -l)"; else echo "DIFF $(printf '%s\n' "$anch" | grep -c .) $(printf '%s\n' "$newest" | grep -c .)"; fi
EOF_ANCHOR

    # cpulog.py <out> [interval]: "ts busy idle py children n" every interval: system-wide busy/idle jiffies (/proc/stat) and, summed
    # over every process whose command line holds zfsbackup.daemon (the supervisor and its forked workers) or multiprocessing. (forkserver/spawn
    # workers: the 26.04 server's Python 3.14 starts workers through the forkserver), utime+stime (py) and cutime+cstime (children: the reaped zfs
    # send/receive/get/list processes). Stops on SIGTERM.
    cat > "$WORK/cpulog.py" <<'EOF_CPULOG'
import os, signal, sys, time
out, iv = sys.argv[1], (float(sys.argv[2]) if len(sys.argv) > 2 else 0.25)
fh = open(out, 'a', buffering=1)
signal.signal(signal.SIGTERM, lambda *_: sys.exit(0)); signal.signal(signal.SIGINT, lambda *_: sys.exit(0))
while True:
    with open('/proc/stat') as f:
        v = [int(x) for x in f.readline().split()[1:]]
    idle = v[3] + v[4]; busy = sum(v[:8]) - idle
    py = ch = n = 0
    for e in os.listdir('/proc'):
        if not e.isdigit():
            continue
        try:
            with open('/proc/%s/cmdline' % e, 'rb') as f:
                cmd = f.read()
                if b'zfsbackup.daemon' not in cmd and b'multiprocessing.' not in cmd:
                    continue
            with open('/proc/%s/stat' % e) as f:
                st = f.read().rsplit(')', 1)[1].split()
            py += int(st[11]) + int(st[12]); ch += int(st[13]) + int(st[14]); n += 1
        except (OSError, IndexError, ValueError):
            continue
    fh.write('%.3f %d %d %d %d %d\n' % (time.time(), busy, idle, py, ch, n))
    time.sleep(iv)
EOF_CPULOG

    # cpuan.py <cpulog> <t0> <t1>: key=value: CPU seconds (python = the daemon's own, children = its reaped zfs processes, busy =
    # the whole VM) over the window, and the VM's average utilisation in cores
    cat > "$WORK/cpuan.py" <<'EOF_CPUAN'
import os, sys
path, t0, t1 = sys.argv[1], float(sys.argv[2]), float(sys.argv[3])
rows = [tuple(float(x) for x in l.split()) for l in open(path) if l.strip()]
def at(t):
    best = rows[0]
    for r in rows:
        if r[0] <= t: best = r
        else: break
    return best
a, b = at(t0), at(t1)
wall = max(b[0] - a[0], 1e-6)
hz = 100.0
b2 = at(t1 + 1.0)      # a child's CPU is added to its parent's cutime/cstime only when it is reaped, a moment after it ends
print('py_cpu=%.2f' % ((b2[3] - a[3]) / hz)); print('child_cpu=%.2f' % ((b2[4] - a[4]) / hz))
print('busy_cpu=%.2f' % ((b[1] - a[1]) / hz)); print('idle_cpu=%.2f' % ((b[2] - a[2]) / hz))
print('cores_busy=%.2f' % ((b[1] - a[1]) / hz / wall)); print('ncpu=%d' % os.cpu_count()); print('wall=%.2f' % wall)
EOF_CPUAN

    # rawmix.sh <out> <spec>...: concurrent raw pipelines, all started at once. l:<src@snap>:<dst> = zfs send -L -c | zfs receive -u -F;
    # n:<src@snap>:<ip>:<port> = zfs send -L -c | nc -N. <out> gets t0 and the end of the last LOCAL pipeline (the nc ones end on the
    # server, ncrecv.sh)
    cat > "$WORK/rawmix.sh" <<'EOF_RAWMIX'
#!/bin/bash
out="$1"; shift
pids=()
t0=$(date +%s.%N)
for spec in "$@"; do
    IFS=: read -r kind src a b <<<"$spec"
    case "$kind" in
        l) ( zfs send -L -c "$src" | zfs receive -u -F "$a" ) & ;;
        n) ( zfs send -L -c "$src" | nc -N "$a" "$b" ) & ;;
    esac
    pids+=($!)
done
rc=0
for p in "${pids[@]}"; do wait "$p" || rc=1; done
t1=$(date +%s.%N)
{ echo "t0=$t0"; echo "t_end=$t1"; echo "rc=$rc"; } > "$out"
EOF_RAWMIX

    # ncrecv.sh <port> <dst> <out>: the server end of a raw network pipeline
    cat > "$WORK/ncrecv.sh" <<'EOF_NCRECV'
#!/bin/bash
port="$1"; dst="$2"; out="$3"
nc -l "$port" | zfs receive -u -F "$dst"; rc=$?
echo "t_end=$(date +%s.%N)" > "$out"; echo "rc=$rc" >> "$out"
EOF_NCRECV

    # trigger.sh <min in-flight incremental sends> <timeout s> <out file> <command...>: polls every 50 ms; the moment <min>
    # incremental `zfs send` processes are alive it records them and runs <command> (a reload), all inside the guest so the
    # host's exec latency never decides whether the sends are still running
    cat > "$WORK/trigger.sh" <<'EOF_TRIGGER'
#!/bin/bash
min="$1"; tmo="$2"; out="$3"; shift 3
end=$(( $(date +%s) + tmo ))
while [ "$(date +%s)" -lt "$end" ]; do
    n=$(pgrep -fc '[z]fs send .*-i ' || true)
    if [ "${n:-0}" -ge "$min" ]; then
        { echo "T_TRIGGER=$(date +%s.%N)"; pgrep -af '[z]fs send .*-i ' | sed 's/^/INFLIGHT /'; } > "$out"
        "$@" >> "$out" 2>&1; echo "RC=$?" >> "$out"; echo "T_DONE=$(date +%s.%N)" >> "$out"; exit 0
    fi
    sleep 0.05
done
echo "TIMEOUT" > "$out"
EOF_TRIGGER

    # freeze.sh <dm device> <timeout s> <out> <delay s>: waits for a `zfs receive` into the hang pool, then FREEZES the device
    # (`dmsetup suspend`): writes block, the receive sleeps in D state. Inside the guest so the host's latency does not decide.
    cat > "$WORK/freeze.sh" <<'EOF_FREEZE'
#!/bin/bash
dev="$1"; tmo="$2"; out="$3"; delay="${4:-2}"
end=$(( $(date +%s) + tmo ))
while [ "$(date +%s)" -lt "$end" ]; do
    if pgrep -f '[z]fs (receive|recv) .*zphang' >/dev/null; then
        sleep "$delay"
        timeout 60 dmsetup suspend "$dev"; rc=$?
        { echo "T_FREEZE=$(date +%s.%N)"; echo "SUSPEND_RC=$rc"; pgrep -af '[z]fs (receive|recv)' | sed 's/^/RECEIVE /'; } > "$out"
        exit 0
    fi
    sleep 0.05
done
echo TIMEOUT > "$out"
EOF_FREEZE

    # paircounts.py <period s>: stdin = filtered daemon log (Sending full|incremental, Nothing to send, with timestamps). Per pair
    # (dataset, transport: remote->remote1, local->local1): sends and attempts (sends + "Nothing to send" lines); the log window and the
    # most attempts a pair may have made in it (window / period + 2: one catch-up, one per slot). Prints "pair ..." lines and
    # window=, expected_max=, bad_sends= (pairs whose sends != 1), bad_attempts= (pairs over expected_max)
    cat > "$WORK/paircounts.py" <<'EOF_PAIRC'
import re, sys, datetime, collections
period = float(sys.argv[1])
rx = re.compile(r'(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d),(\d{3}) - (\S+) - (INFO|DEBUG) - (.*)')
sends, nothing, ts = collections.Counter(), collections.Counter(), []
for line in sys.stdin:
    m = rx.match(line)
    if not m:
        continue
    t = datetime.datetime.strptime(m.group(1), '%Y-%m-%d %H:%M:%S').timestamp() + int(m.group(2)) / 1000.0
    ts.append(t); lg, msg = m.group(3), m.group(5)
    tr = 'remote1' if lg.endswith('remote') else 'local1'
    a = re.match(r'Sending (?:full snapshot|incremental) (\S+?)(?:@|:)', msg)
    if a and lg.endswith(('remote', 'local')):
        sends[(a.group(1), tr)] += 1; continue
    n = re.match(r'Nothing to send for (\S+) -> (\S+): ', msg)
    if n:
        nothing[(n.group(1), n.group(2))] += 1
pairs = sorted(set(sends) | set(nothing))
window = (max(ts) - min(ts)) if ts else 0.0
exp = int(window / period) + 2
bad_s = bad_a = 0
for k in pairs:
    att = sends[k] + nothing[k]
    bad_s += sends[k] != 1; bad_a += att > exp
    print('pair %s %s sends=%d nothing=%d attempts=%d' % (k[0], k[1], sends[k], nothing[k], att))
print('window=%.1f\nexpected_max=%d\npairs=%d\nbad_sends=%d\nbad_attempts=%d' % (window, exp, len(pairs), bad_s, bad_a))
EOF_PAIRC

    # rereads.py: stdin = daemon log (DEBUG). Counts the owner reads of a container (`zfs get -H -o value -s local
    # org.zfsbackup:client_id X`) that start within 0.1 s after `zfs create ... X` was started: the race window in which, on OpenZFS 2.2,
    # the container is listed but its client_id cannot be read yet. Each such read is resolved by the busy path (a claim held:
    # INFO "is busy: another send is creating") or by the re-read under the claim; the re-read path itself logs nothing
    cat > "$WORK/rereads.py" <<'EOF_REREAD'
import re, sys, datetime
T = lambda m: datetime.datetime.strptime(m.group(1), '%Y-%m-%d %H:%M:%S').timestamp() + int(m.group(2)) / 1000.0
crt = re.compile(r'(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d),(\d{3}) .*Executing command: \S*zfs create -u -o canmount=noauto -o org\.zfsbackup:client_id=\S+ (\S+)\s*$')
get = re.compile(r'(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d),(\d{3}) .*Executing command: \S*zfs get -H -o value -s local org\.zfsbackup:client_id (\S+)\s*$')
created, creates, win = {}, 0, 0
for line in sys.stdin:
    m = crt.match(line)
    if m:
        created[m.group(3)] = T(m); creates += 1; continue
    m = get.match(line)
    if m and m.group(3) in created and 0 <= T(m) - created[m.group(3)] <= 0.1:
        win += 1
print('container_creates=%d\nreread_candidates=%d' % (creates, win))
EOF_REREAD

    # statuswatch.py <out> <glob>: appends a line (ts, dataset, destination, incomplete=..., writer pid) whenever a send record's
    # `incomplete` flag changes in a worker status file (polled every 50 ms)
    cat > "$WORK/statuswatch.py" <<'EOF_SW'
import glob, json, sys, time
out, pat = sys.argv[1], sys.argv[2]
fh = open(out, 'a', buffering=1); seen = {}
while True:
    for p in glob.glob(pat):
        try:
            d = json.load(open(p))
        except (OSError, ValueError):
            continue
        for r in d.get('records', []):
            if r.get('kind') != 'send':
                continue
            key = (r['dataset'], r.get('destination')); inc = bool(r.get('incomplete'))
            if seen.get(key) != inc:
                seen[key] = inc
                fh.write('%.3f\t%s\t%s\tincomplete=%s\tpid=%s\n' % (time.time(), key[0], key[1], inc, d.get('pid')))
    time.sleep(0.05)
EOF_SW

    # writer.sh <MiB> <interval s> <dataset>...: appends <MiB> MiB to every dataset every <interval> s until stopped
    cat > "$WORK/writer.sh" <<'EOF_WRITER'
#!/bin/bash
mib="$1"; iv="$2"; shift 2
while true; do
    for d in "$@"; do /opt/zfsb-ps/mkdata.sh "$d" "$mib" w.bin; done
    sleep "$iv"
done
EOF_WRITER

    # visprobe.sh <parent dataset> <n>: root cause probe for scenario 3. Is a dataset created with `-o org.zfsbackup:client_id=...`
    # already visible to `zfs list` while `zfs get -s local` cannot read the property yet? prints "visible=<n> without_prop=<m>"
    cat > "$WORK/visprobe.sh" <<'EOF_VIS'
#!/bin/bash
parent="$1"; n="$2"; seen=0; bad=0
zfs create -o canmount=off "$parent" >/dev/null 2>&1
for i in $(seq 1 "$n"); do
    d="$parent/t$i"
    zfs create -u -o canmount=noauto -o org.zfsbackup:client_id=probe "$d" &
    for k in $(seq 1 400); do
        if zfs list -H -o name "$d" >/dev/null 2>&1; then
            v="$(zfs get -H -o value -s local org.zfsbackup:client_id "$d" 2>/dev/null)"
            seen=$((seen+1)); [ "$v" = "probe" ] || bad=$((bad+1))
            break
        fi
    done
    wait
done
zfs destroy -r "$parent" >/dev/null 2>&1
echo "visible=$seen without_prop=$bad"
EOF_VIS

    # fp.sh <root> [ERE] [yes|no]: "<name relative to root (unless no)>@<snap> <guid>" of every snapshot under <root>, sorted
    cat > "$WORK/fp.sh" <<'EOF_FP'
#!/bin/bash
root="$1"; pat="${2:-.}"; strip="${3:-yes}"
if [ "$strip" = yes ]; then
    zfs list -H -r -t snapshot -o name,guid "$root" 2>/dev/null | sed "s#^$root/##" | grep -E "$pat" | sort
else
    zfs list -H -r -t snapshot -o name,guid "$root" 2>/dev/null | grep -E "$pat" | sort
fi
EOF_FP
}

put_guest_scripts() { # put_guest_scripts <vm>
    local vm="$1"
    msh "$vm" "mkdir -p '$GDIR' '$WDIR'"
    put_file "$vm" "$WORK/jget.py"        "$JGET_PY"             644
    put_file "$vm" "$WORK/sampler.py"     "$GDIR/sampler.py"     644
    put_file "$vm" "$WORK/analyze.py"     "$GDIR/analyze.py"     644
    put_file "$vm" "$WORK/pickalign.py"   "$GDIR/pickalign.py"   644
    put_file "$vm" "$WORK/logoverlap.py"  "$GDIR/logoverlap.py"  644
    put_file "$vm" "$WORK/mkdata.sh"      "$GDIR/mkdata.sh"      755
    put_file "$vm" "$WORK/anchorcheck.sh" "$GDIR/anchorcheck.sh" 755
    put_file "$vm" "$WORK/fp.sh"          "$GDIR/fp.sh"          755
    put_file "$vm" "$WORK/trigger.sh"     "$GDIR/trigger.sh"     755
    put_file "$vm" "$WORK/freeze.sh"      "$GDIR/freeze.sh"      755
    put_file "$vm" "$WORK/visprobe.sh"    "$GDIR/visprobe.sh"    755
    put_file "$vm" "$WORK/paircounts.py"  "$GDIR/paircounts.py"  644
    put_file "$vm" "$WORK/rereads.py"     "$GDIR/rereads.py"     644
    put_file "$vm" "$WORK/statuswatch.py" "$GDIR/statuswatch.py" 644
    put_file "$vm" "$WORK/writer.sh"      "$GDIR/writer.sh"      755
    put_file "$vm" "$WORK/cpulog.py"      "$GDIR/cpulog.py"      644
    put_file "$vm" "$WORK/cpuan.py"       "$GDIR/cpuan.py"       644
    put_file "$vm" "$WORK/rawmix.sh"      "$GDIR/rawmix.sh"      755
    put_file "$vm" "$WORK/ncrecv.sh"      "$GDIR/ncrecv.sh"      755
}

# ─────────────────────────────── Install ────────────────────────────────────
ship_tree() { # ship_tree <vm>
    local vm="$1" ref="$SRC_REF" top tarfile
    top="$(git -C "$(dirname "$0")" rev-parse --show-toplevel)" || die "not in a git repo"
    if [ "$ref" = "WORKTREE" ]; then
        local idx tree
        idx="$(mktemp -u "${TMPDIR:-/tmp}/zfsb-wt-index.XXXXXX")"
        tree="$(cd "$top" && GIT_INDEX_FILE="$idx" git read-tree HEAD && GIT_INDEX_FILE="$idx" git add -A && GIT_INDEX_FILE="$idx" git write-tree)" \
            || { rm -f "$idx"; die "could not snapshot the working tree"; }
        rm -f "$idx"
        ref="$(git -C "$top" commit-tree "$tree" -p HEAD -m 'parallel-senders scenario: working-tree snapshot')" \
            || die "could not snapshot the working tree"
    fi
    step "Shipping the tree ($(git -C "$top" rev-parse --short "$ref" 2>/dev/null || echo "$ref")) to $vm:$SRC_DIR"
    tarfile="$WORK/src.tar"
    git -C "$top" archive --format=tar "$ref" > "$tarfile" || die "git archive of '$ref' failed"
    multipass transfer "$tarfile" "$vm:/tmp/zfsbackup-src.tar" || die "source transfer failed for $vm"
    msh "$vm" "rm -rf '$SRC_DIR' && mkdir -p '$SRC_DIR' && tar -m -xf /tmp/zfsbackup-src.tar -C '$SRC_DIR' && rm -f /tmp/zfsbackup-src.tar" \
        || die "could not extract the tree on $vm"
    rm -f "$tarfile"
}

install_stack() { # install_stack <vm>
    local vm="$1"
    step "Provisioning $vm (ZFS, dmsetup, nc, Python venv, zfsbackup dependencies)"
    msh "$vm" '
        set -e
        export DEBIAN_FRONTEND=noninteractive
        cloud-init status --wait >/dev/null 2>&1 || true
        need=""
        command -v zfs      >/dev/null 2>&1 || need="$need zfsutils-linux"
        command -v dmsetup  >/dev/null 2>&1 || need="$need dmsetup"
        command -v nc       >/dev/null 2>&1 || need="$need netcat-openbsd"
        command -v tc       >/dev/null 2>&1 || need="$need iproute2"
        command -v curl     >/dev/null 2>&1 || need="$need curl"
        dpkg -s python3-venv >/dev/null 2>&1 || need="$need python3-venv"
        dpkg -s python3-pip  >/dev/null 2>&1 || need="$need python3-pip"
        if [ -n "$need" ]; then apt-get update -qq && apt-get install -y -qq $need; fi
        modprobe zfs
        zfs version | head -n1
    ' || die "package install failed on $vm"
    ship_tree "$vm"
    msh "$vm" "
        set -e
        test -x '$PY' || python3 -m venv '$VENV'
        '$PIP' install -q --upgrade pip
        '$PIP' install -q pyyaml 'flask>=3' flask-sock websocket-client requests 'cryptography>=43' \
            'sqlalchemy>=2.0' 'alembic>=1.13' 'click>=8.1'
        mkdir -p /etc/zfsbackup /var/lib/zfsbackup
    " || die "pip install failed on $vm"
    put_guest_scripts "$vm"
}

# ─────────────────────────────── Daemons and the CLI ────────────────────────
# extra systemd-run properties may be passed (the CPU cap). KillMode=mixed and TimeoutStopSec=90 are the shipped unit's.
start_daemon() { # start_daemon <vm> [systemd-run properties...]
    local vm="$1"; shift
    step "Starting the daemon on $vm (unit $UNIT${1:+, $*})"
    msh "$vm" "
        systemctl reset-failed $UNIT >/dev/null 2>&1 || true
        systemd-run --unit=$UNIT --collect -p KillMode=mixed -p TimeoutStopSec=90 $* \
            -p StandardOutput=append:'$LOG' -p StandardError=append:'$LOG' \
            env PYTHONPATH='$SRC_PP' '$PY' -m zfsbackup.daemon -c '$DB' $DAEMON_V
    " || die "could not start the daemon on $vm"
}
stop_daemon() { # stop_daemon <vm>: a plain `systemctl stop` (the unit's own budget)
    msh "$1" "{ systemctl stop $UNIT; systemctl reset-failed $UNIT; } >/dev/null 2>&1; true" || true
}
kill_daemon() { msh "$1" "{ systemctl stop $UNIT; systemctl reset-failed $UNIT; pkill -9 -f '[z]fsbackup.daemon'; } >/dev/null 2>&1; true" || true; }
daemon_active() { [ "$(msh "$1" "systemctl is-active $UNIT 2>&1 | head -n1" | trim)" = "active" ]; }
cli() { local vm="$1"; shift; msh "$vm" "env PYTHONPATH='$SRC_PP' '$PY' -m zfsbackup.cli -c '$DB' $* 2>&1"; }
cli_s() { cli "$SND" "$@"; }
cli_v() { cli "$SRV" "$@"; }
jfile() { msh "$1" "'$PY' '$JGET_PY' '$3' < '$2'"; }

loglines() { msh "$1" "wc -l < '$LOG'" | trim; }
lg_count() { msh "$1" "tail -n +$(( $2 + 1 )) '$LOG' | grep -cE -- '$3' || true" | trim; }          # lg_count <vm> <mark> <ERE>
lg_show()  { msh "$1" "tail -n +$(( $2 + 1 )) '$LOG' | grep -E -- '$3' | head -n ${4:-6} | cut -c1-230 || true"; }
wpid() { msh "$1" "grep -oE '(Started|Restarted|Reloaded) $2 worker \\(pid=[0-9]+\\)' '$LOG' | tail -n1 | grep -oE '[0-9]+'" | trim; }

# ─────────────────────────────── Pools ──────────────────────────────────────
mkpool() { # mkpool <vm> <pool> <size> [zpool options]
    msh "$1" "
        set -e
        zpool list '$2' >/dev/null 2>&1 && zpool destroy -f '$2'
        rm -f '$IMG_DIR/$2.img'; truncate -s '$3' '$IMG_DIR/$2.img'
        zpool create -f -o autotrim=on -O compression=off ${4:-} '$2' '$IMG_DIR/$2.img'
    " || die "pool $2 on $1 could not be created"
}
create_pools() {
    step "Creating the pools: $SRC_POOL, $TGT_POOL on $SND; $SRV_POOL on $SRV (file-backed, sparse $DISK_SIZE, in $IMG_DIR)"
    msh "$SND" "{ zpool destroy -f '$SRC_POOL'; zpool destroy -f '$TGT_POOL'; } >/dev/null 2>&1; true"
    msh "$SRV" "{ zpool destroy -f '$SRV_POOL'; } >/dev/null 2>&1; true"
    mkpool "$SND" "$SRC_POOL" "$DISK_SIZE"
    mkpool "$SND" "$TGT_POOL" "$DISK_SIZE"
    mkpool "$SRV" "$SRV_POOL" "$DISK_SIZE"
    snd "zfs create -o canmount=off '$TGT_A' && zfs create -o canmount=off '$TGT_B'" || die "targets on $SND"
    srv "zfs create -o canmount=off '$SRV_TARGET'" || die "target on $SRV"
}

# the frozen target of scenario 4: loop device -> dm linear -> pool (failmode=wait). `dmsetup suspend` freezes its I/O.
hang_pool_create() {
    step "Creating the hang pool $HANG_POOL on $SND (file -> loop -> dm linear $HANG_DEV; failmode=wait)"
    hang_pool_destroy
    snd "
        set -e
        modprobe dm_mod 2>/dev/null || true
        rm -f '$HANG_IMG'; truncate -s 3g '$HANG_IMG'
        loop=\$(losetup -f --show '$HANG_IMG')
        echo \"0 \$(blockdev --getsz \$loop) linear \$loop 0\" | dmsetup create '$HANG_DEV'
        zpool create -f -o failmode=wait -o autotrim=on -O compression=off -O mountpoint=none '$HANG_POOL' '/dev/mapper/$HANG_DEV'
        zfs create -o canmount=off '$HANG_T'
        zpool status '$HANG_POOL' | sed -n 1,8p
    " || die "the hang pool could not be created"
}
hang_resume() { snd "dmsetup resume '$HANG_DEV' >/dev/null 2>&1; true" || true; }
hang_pool_destroy() {
    snd "
        dmsetup resume '$HANG_DEV' >/dev/null 2>&1
        for i in \$(seq 1 30); do pgrep -x 'zfs|zpool' >/dev/null 2>&1 || break; sleep 1; done
        { zpool destroy -f '$HANG_POOL'; zpool export -f '$HANG_POOL'; dmsetup remove -f '$HANG_DEV'; } >/dev/null 2>&1
        for l in \$(losetup -j '$HANG_IMG' 2>/dev/null | cut -d: -f1); do losetup -d \$l >/dev/null 2>&1; done
        rm -f '$HANG_IMG'; true
    " || true
}

# destroy every dataset below the pool root / target (and with it its snapshots and anchors) and the daemon's leftovers
bg_stop() { msh "$SND" "systemctl stop zfsb-ps-writer zfsb-ps-statuswatch >/dev/null 2>&1; systemctl reset-failed zfsb-ps-writer zfsb-ps-statuswatch >/dev/null 2>&1; true" || true; }
scn_reset() {
    bg_stop
    [ "$SRV_DAEMON_OPTIONAL" = 1 ] || daemon_active "$SRV" || { warn "the server daemon was not running: restarting it"; server_fresh; }
    stop_daemon "$SND"
    samplers_stop
    throttle_off
    snd "
        for r in '$SRC_POOL' '$TGT_A' '$TGT_B'; do
            zfs list -H -o name -d 1 \"\$r\" 2>/dev/null | tail -n +2 | xargs -r -n1 zfs destroy -r
        done
        rm -rf '$DB.status'
        zpool set autotrim=on '$SRC_POOL'; zpool set autotrim=on '$TGT_POOL'; true
    " || true
    srv "zfs list -H -o name -d 1 '$SRV_TARGET' 2>/dev/null | tail -n +2 | xargs -r -n1 zfs destroy -r; zpool set autotrim=on '$SRV_POOL'; true" || true
}

# ─────────────────────────────── Server and enrollment ──────────────────────
SRV_IP=""; PIN_FP=""
write_server_config() {
    step "Writing the server config on $SRV"
    cat > "$WORK/server.yaml" <<EOF
# zfsbackup SERVER config (test_parallel_senders.sh)
snapshot_prefix: "autosnap"
check_interval: "1m"
prune_interval: "1h"
api_host: "0.0.0.0"
api_port: ${API_PORT}
remote_backup:
  enabled: true
  target_dataset: "${SRV_TARGET}"
datasets:
  - name: "${SRV_POOL}/placeholder"
    enabled: false
    frequency: "1h"
    retention:
      "1h": "1d"
EOF
    put_file "$SRV" "$WORK/server.yaml" "$CFG" 600
    local out; out="$(cli_v import --no-backup "$CFG")" || { printf '%s\n' "$out" >&2; die "server config import failed"; }
}
wait_health() { # the server's /health, from the sender
    local waited=0
    until msh "$SND" "curl -fsSk --connect-timeout 5 --max-time 10 -o /dev/null 'https://${SRV_IP}:${API_PORT}/health' >/dev/null 2>&1"; do
        [ "$waited" -ge "${1:-60}" ] && return 1
        sleep 2; waited=$((waited+2))
    done
}
remove_config() { # remove_config <vm>
    msh "$1" "{ rm -f '$CFG' '$DB' '$DB-wal' '$DB-shm' '$DB.daemon.pid' '$DB.writer.lock' '$DB.workers.lock'; rm -rf '$TLS_DIR' '$DB.status'; } >/dev/null 2>&1; true" || true
}
enroll_sender() {
    step "Enrolling $SND (id $CID) at https://${SRV_IP}:${API_PORT}"
    cat > "$WORK/enroll.yaml" <<EOF
snapshot_prefix: "autosnap"
check_interval: "10s"
prune_interval: "5m"
destinations:
  remote1:
    url: "https://${SRV_IP}:${API_PORT}"
datasets:
  - name: "${SRC_POOL}/placeholder"
    enabled: false
    frequency: "1h"
    retention:
      "1h": "1d"
EOF
    put_file "$SND" "$WORK/enroll.yaml" "$CFG" 600
    local out rc
    out="$(cli_s import --no-backup "$CFG")" || { printf '%s\n' "$out" >&2; die "sender config import failed"; }
    out="$(cli_s identity set-client-id "$CID")"; rc=$?
    [ "$rc" -eq 0 ] || die "identity set-client-id failed: $out"
    out="$(cli_v trust token create --json)" || die "could not mint a token on $SRV"
    local tok fp
    tok="$(printf '%s\n' "$out" | sed -n 's/^ *"token": *"\([^"]*\)".*/\1/p' | head -n1)"
    fp="$(printf '%s\n' "$out" | sed -n 's/^ *"server_fingerprint": *"\([^"]*\)".*/\1/p' | head -n1)"
    [ -n "$tok" ] && [ -n "$fp" ] || die "the token output had no token/fingerprint"
    local t; t="$(mktemp)" && chmod 600 "$t"; printf '%s\n' "$tok" > "$t"
    multipass transfer "$t" "$SND:/tmp/zbps-token" || die "token transfer failed"; rm -f "$t"
    msh "$SND" "install -m600 -o root -g root /tmp/zbps-token /root/zbps-token && rm -f /tmp/zbps-token"
    out="$(cli_s enroll remote1 --url "https://${SRV_IP}:${API_PORT}" --token-file /root/zbps-token)"; rc=$?
    msh "$SND" "rm -f /root/zbps-token"
    printf '%s\n' "$out" | sed 's/^/    CLI: /'
    [ "$rc" -eq 0 ] || die "enroll failed (exit $rc)"
    PIN_FP="$fp"
    msh "$SND" "mkdir -p '$WDIR' && printf '%s' '$fp' > '$WDIR/pin'"
    check "the sender's client id is $CID" "$CID" "$(cli_s identity show --json | tr -d '\n' | sed -n 's/.*"client_id": *"\([^"]*\)".*/\1/p')"
}
# --reuse: the pin and the id are in the DB of a previous run
recover_state() {
    SRV_IP="$(vm_ip "$SRV")"
    PIN_FP="$(msh "$SND" "cat '$WDIR/pin' 2>/dev/null" | trim)"
    [ -n "$PIN_FP" ] || die "--reuse: no pinned server fingerprint recorded on $SND (run without --reuse)"
    info "reusing the environment: server $SRV_IP, pin ${PIN_FP:0:16}..."
}

# ─────────────────────────────── Config ─────────────────────────────────────
dest_url() {
    case "$1" in
        remote1) echo "https://${SRV_IP}:${API_PORT}" ;;
        local1)  echo "zfs:${TGT_A}" ;;
        local2)  echo "zfs:${TGT_B}" ;;
        hung1)   echo "zfs:${HANG_T}" ;;
        *) die "unknown destination $1" ;;
    esac
}
# cfg_write <snapshot frequency> <remote frequency> "<destinations>" <dataset>=<dest,dest> ...   (then import it)
cfg_write() {
    local sf="$1" rf="$2" dl="$3" spec d name dests x; shift 3
    {
        printf '%s\n' '# zfsbackup config of test_parallel_senders.sh' 'snapshot_prefix: "autosnap"' 'check_interval: "10s"' \
            'prune_interval: "5m"' 'destinations:'
        for d in $dl; do
            printf '  %s:\n    url: "%s"\n' "$d" "$(dest_url "$d")"
            [ "$d" = remote1 ] && printf '    server_fingerprint: "%s"\n' "$PIN_FP"
        done
        echo 'datasets:'
        for spec in "$@"; do
            name="${spec%%=*}"; dests="${spec#*=}"
            printf '  - name: "%s"\n    enabled: true\n    frequency: "%s"\n    recursive: false\n    retention:\n      "1s": "7d"\n    remote:\n' "$name" "$sf"
            for x in ${dests//,/ }; do printf '      - destination: %s\n        frequency: "%s"\n' "$x" "$rf"; done
        done
    } > "$WORK/config.yaml"
    put_file "$SND" "$WORK/config.yaml" "$CFG" 600
    local out; out="$(cli_s import --no-backup "$CFG")" || { printf '%s\n' "$out" >&2; die "config import failed"; }
    check_lacks "the import reports no error" '[Ee]rror|refus' "$out"
}

# ─────────────────────────────── Throttle (CPU cap is a unit property; tc for the network) ──
THROTTLE_DEV=""
throttle_on() { # throttle_on <rate>: the SENDER's egress towards the server
    THROTTLE_DEV="$(msh "$SND" "ip -o route get '$SRV_IP' | sed -n 's/.* dev \\([^ ]*\\).*/\\1/p' | head -n1" | trim)"
    [ -n "$THROTTLE_DEV" ] || die "no interface towards $SRV_IP"
    msh "$SND" "tc qdisc del dev $THROTTLE_DEV root >/dev/null 2>&1; tc qdisc add dev $THROTTLE_DEV root tbf rate $1 burst 256kb latency 400ms" \
        || die "tc qdisc add failed"
    info "sender egress on $THROTTLE_DEV limited to $1"
}
throttle_off() {
    [ -n "$THROTTLE_DEV" ] || return 0
    msh "$SND" "tc qdisc del dev $THROTTLE_DEV root >/dev/null 2>&1; true" || true
    THROTTLE_DEV=""
}

# ─────────────────────────────── Samplers ───────────────────────────────────
SMP_T0=""
samplers_start() { # samplers_start <label>
    local vm
    for vm in "$SND" "$SRV"; do
        msh "$vm" "systemctl stop zfsb-ps-sampler >/dev/null 2>&1; systemctl reset-failed zfsb-ps-sampler >/dev/null 2>&1; rm -f '$WDIR/$1.tsv'; systemd-run --unit=zfsb-ps-sampler --collect python3 '$GDIR/sampler.py' '$WDIR/$1.tsv' 0.1 >/dev/null 2>&1; true" || true
    done
    SMP_T0="$(msh "$SND" "date +%s.%N" | trim)"
}
samplers_stop() { local vm; for vm in "$SND" "$SRV"; do msh "$vm" "systemctl stop zfsb-ps-sampler >/dev/null 2>&1; systemctl reset-failed zfsb-ps-sampler >/dev/null 2>&1; true" || true; done; }
AN=""
analyze() { # analyze <vm> <label> [t0] [t1] [receive-target prefix]: AN = the key=value lines
    AN="$(msh "$1" "python3 '$GDIR/analyze.py' '$WDIR/$2.tsv' ${3:--} ${4:--} ${5:-}")"
}
an() { printf '%s\n' "$AN" | sed -n "s/^$1=//p" | head -n1; }
keep_samples() { # keep_samples <vm> <label> <host name>
    get_file "$1" "$WDIR/$2.tsv" "$LOG_DIR/$3.tsv" || true
}

# ─────────────────────────────── ZFS helpers ────────────────────────────────
# WAIT_GUARD (a command, default: the server daemon is alive): a wait ends at once (status 2) when it fails, so a server that the
# kernel OOM-killed (1 GiB VM) fails the scenario in seconds instead of timing out after 15 minutes
SERVER_DEATHS=0
SRV_DAEMON_OPTIONAL=0               # 1 during the raw network legs of scenario 5 (no daemon on the server)
server_alive() { daemon_active "$SRV"; }
WAIT_GUARD="server_alive"
wait_for() { # wait_for <timeout> <predicate> [args...]
    local timeout="$1" waited=0; shift
    until "$@"; do
        [ "$waited" -ge "$timeout" ] && return 1
        if [ -n "$WAIT_GUARD" ] && ! $WAIT_GUARD; then
            SERVER_DEATHS=$((SERVER_DEATHS+1))
            finding "the server daemon on $SRV died while a scenario was waiting (kernel OOM kill: $(msh "$SRV" "journalctl -u $UNIT --no-pager -n 40 | grep -c oom-kill" | trim) oom-kill line(s) in its journal)"
            return 2
        fi
        sleep 3; waited=$((waited+3))
    done
}
# a fresh server daemon with the page cache dropped (run before every scenario and every remote round)
server_fresh() {
    stop_daemon "$SRV"
    msh "$SRV" "sync; echo 3 > /proc/sys/vm/drop_caches; true" || true
    start_daemon "$SRV"
    wait_health 90 || die "the server on $SRV did not answer /health"
}
mkdata() { snd "'$GDIR/mkdata.sh' '$1' '$2' ${3:-}"; }
snap_all() { # snap_all <ds>...: one snapshot name for all, taken now (a 2 s pause keeps names unique); prints the name
    sleep 2
    snd "n=autosnap_\$(date -u +%Y%m%d%H%M%S); for d in $*; do zfs snapshot \"\$d@\$n\" || exit 1; done; echo \$n" | trim
}
# anchors_same <dest> <ERE> <expected count>: every matching dataset's anchor.<dest> names its newest snapshot, <count> of them
anchors_same() { local r; r="$(snd "'$GDIR/anchorcheck.sh' '$1' '$2'" | trim)"; [ "$r" = "SAME$3" ]; }
anchor_state() { snd "'$GDIR/anchorcheck.sh' '$1' '$2'"; }
# fp_cmp <label> <sender fp args> <vm> <copy root> <ERE>: the copies' (name, guid) set equals the source's
fp_cmp() {
    local label="$1" vm="$2" root="$3" pat="$4"
    snd "'$GDIR/fp.sh' '$SRC_POOL' '$pat' no" > "$WORK/fp.src"
    msh "$vm" "'$GDIR/fp.sh' '$root' '$pat'" > "$WORK/fp.dst"
    local n d; n="$(grep -c . "$WORK/fp.src")"; d="$(diff "$WORK/fp.src" "$WORK/fp.dst" | head -n4 | tr '\n' '|')"
    if [ -z "$d" ] && [ "$n" -gt 0 ]; then pass "$label: all $n (snapshot, guid) pairs equal"; else fail "$label: $n source pairs, differences: ${d:-none (nothing to compare)}"; fi
}
copy_root() { # copy_root <local1|local2|remote1|hung1>: the dataset holding the copies of this sender
    case "$1" in
        remote1) echo "$SRV_TARGET/$CID" ;; local1) echo "$TGT_A/$CID" ;; local2) echo "$TGT_B/$CID" ;; hung1) echo "$HANG_T/$CID" ;;
    esac
}
copy_vm() { case "$1" in remote1) echo "$SRV" ;; *) echo "$SND" ;; esac; }
fp_dest() { fp_cmp "$1 ($2)" "$(copy_vm "$2")" "$(copy_root "$2")" "$3"; }
tokens() { # tokens <vm> <root>: copies holding a receive_resume_token
    msh "$1" "zfs get -H -r -o name,value receive_resume_token '$2' 2>/dev/null | awk '\$2!=\"-\"' | wc -l" | trim
}
daemon_problems() { # daemon_problems <vm> <mark> <label>: ERROR lines and tracebacks since the mark
    local e t
    e="$(lg_count "$1" "$2" ' - ERROR - ')"; t="$(lg_count "$1" "$2" 'Traceback')"
    check "$3: no ERROR line in the daemon log on $1" "0" "$e"
    check "$3: no Traceback in the daemon log on $1" "0" "$t"
    [ "$e" != "0" ] && lg_show "$1" "$2" ' - ERROR - ' 5 | sed 's/^/      /'
}
# the remote worker's status records: "<dataset>><dest>=<result>" lines
status_records() { msh "$SND" "'$PY' -c \"
import json,sys,glob
for p in glob.glob('$DB.status/*.json'):
    d=json.load(open(p))
    for r in d.get('records', d if isinstance(d, list) else []):
        if r.get('kind') == 'send': print('%s>%s=%s/%s' % (r['dataset'], r.get('destination'), r.get('result'), r.get('consecutive_failures', 0)))
\" | sort"; }

# ─────────────────────────────── Teardown ───────────────────────────────────
collect_artifacts() {
    get_file "$SND" "$LOG" "$LOG_DIR/sender-daemon.log" || true
    get_file "$SRV" "$LOG" "$LOG_DIR/server-daemon.log" || true
}
cleanup() {
    local rc=$?
    throttle_off
    if vm_running "$SND"; then
        hang_resume
        bg_stop
        samplers_stop
        collect_artifacts
        if [ "$KEEP" -eq 0 ]; then
            kill_daemon "$SND"; kill_daemon "$SRV"
            untune_vm
            if [ "$KEEP_POOLS" -eq 0 ]; then
                hang_pool_destroy
                msh "$SND" "{ zpool destroy -f '$SRC_POOL'; zpool destroy -f '$TGT_POOL'; rm -f '$IMG_DIR/$SRC_POOL.img' '$IMG_DIR/$TGT_POOL.img'; } >/dev/null 2>&1; true" || true
                msh "$SRV" "{ zpool destroy -f '$SRV_POOL'; rm -f '$IMG_DIR/$SRV_POOL.img'; } >/dev/null 2>&1; true" || true
            fi
        fi
    fi
    rm -rf "$WORK"
    echo
    echo "${BOLD}Results${NC}: $PASS_COUNT passed, $FAIL_COUNT failed   (logs and samples: $LOG_DIR)"
    local r; if [ "${#RESULTS[@]}" -gt 0 ]; then for r in "${RESULTS[@]}"; do echo "  $r"; done | grep -F "FAIL" || true; fi
    if [ "${#FINDINGS[@]}" -gt 0 ]; then echo "${BOLD}Findings${NC}:"; for r in "${FINDINGS[@]}"; do echo "  - $r"; done; fi
    if [ "$rc" -ne 0 ] || [ "$FAIL_COUNT" -gt 0 ]; then exit 1; fi
    exit 0
}
trap cleanup EXIT
trap 'exit 130' INT TERM

# ─────────────────────────────── Setup ──────────────────────────────────────
# The VMs are small (sender 2 GiB, server 1 GiB): with the default zfs_arc_max (up to ~65% of RAM, the pool images also sit in
# the page cache) two concurrent 400+ MiB receives got the server's daemon OOM-killed in a first run. The ARC is capped for the
# run (a module parameter, not persistent): sender 768 MiB, server 256 MiB, and the server's dirty-data limits are lowered
# (zfs_dirty_data_max 64 MiB, vm.dirty_bytes 96 MiB). Even so the 1 GiB server is OOM-killed by two concurrent uncapped
# receives of 600 MiB (its daemon is ~5 Python processes of ~100 MiB each); the scenarios keep remote streams at 192-256 MiB.
tune_vm() {
    msh "$SND" "echo $((768*1024*1024)) > /sys/module/zfs/parameters/zfs_arc_max; true" || true
    msh "$SRV" "echo $((256*1024*1024)) > /sys/module/zfs/parameters/zfs_arc_max; echo $((64*1024*1024)) > /sys/module/zfs/parameters/zfs_dirty_data_max; sysctl -w vm.dirty_background_bytes=33554432 vm.dirty_bytes=100663296 >/dev/null 2>&1; true" || true
}
# back to the defaults (0 = automatic for both module parameters; the vm.dirty_*_bytes settings fall back to the ratios)
untune_vm() {
    msh "$SND" "echo 0 > /sys/module/zfs/parameters/zfs_arc_max; true" || true
    msh "$SRV" "echo 0 > /sys/module/zfs/parameters/zfs_arc_max; echo 0 > /sys/module/zfs/parameters/zfs_dirty_data_max; sysctl -w vm.dirty_background_bytes=0 vm.dirty_bytes=0 >/dev/null 2>&1; true" || true
}
setup() {
    ensure_vm "$SND"; ensure_vm "$SRV"
    write_guest_scripts
    tune_vm
    SRV_IP="$(vm_ip "$SRV")"
    [ -n "$SRV_IP" ] || die "no IP for $SRV"
    if [ "$REUSE" -eq 1 ]; then
        put_guest_scripts "$SND"; put_guest_scripts "$SRV"
        daemon_active "$SRV" || { start_daemon "$SRV"; wait_health 90 || die "the server on $SRV did not answer /health"; }
        recover_state; return
    fi
    kill_daemon "$SND"; kill_daemon "$SRV"
    msh "$SND" "systemctl stop zfsb-ps-sampler >/dev/null 2>&1; true"
    hang_pool_destroy
    install_stack "$SND"; install_stack "$SRV"
    remove_config "$SND"; remove_config "$SRV"
    create_pools
    write_server_config
    : > "$WORK/empty"; msh "$SRV" ": > '$LOG'"; msh "$SND" ": > '$LOG'"
    start_daemon "$SRV"
    wait_health 90 || die "the server on $SRV did not answer /health"
    enroll_sender
    info "environment ready: sender $SND, server $SRV ($SRV_IP), client id $CID"
}

# ═══════════════════════════════ Scenario 1 - overlapping sends ═════════════════════════════════════════════════════════
scenario_1() {
    step "Scenario 1: overlapping sends - 6 datasets x (remote1 over mTLS, local1 = zfs:$TGT_A), ${S1_MIB} MiB each, unit CPUQuota=${CPU_QUOTA:-none}"
    scn_reset
    local i specs=() ds=()
    for i in 1 2 3 4 5 6; do
        mkdata "$SRC_POOL/p$i" "$S1_MIB" || die "could not create $SRC_POOL/p$i"
        ds+=("$SRC_POOL/p$i"); specs+=("$SRC_POOL/p$i=remote1,local1")
    done
    cfg_write 1d 20s "remote1 local1" "${specs[@]}"
    S1_SNAP="$(snap_all "${ds[@]}")"
    [ -n "$S1_SNAP" ] || die "no snapshot name"
    local ms mv; ms="$(loglines "$SND")"; mv="$(loglines "$SRV")"
    samplers_start s1
    start_daemon "$SND" ${CPU_QUOTA:+-p CPUQuota=$CPU_QUOTA}
    s1_done() { anchors_same remote1 '^zpsrc/p' 6 && anchors_same local1 '^zpsrc/p' 6; }
    if wait_for 600 s1_done; then pass "all 12 pairs completed (every dataset's anchor.remote1 and anchor.local1 names $S1_SNAP)"
    else fail "the 12 pairs did not all complete (remote1: $(anchor_state remote1 '^zpsrc/p'); local1: $(anchor_state local1 '^zpsrc/p'))"; fi
    info "observing 60 s more: a pair that has nothing pending must stay quiet (only 'Nothing to send' attempts, one per period)"
    sleep 60
    samplers_stop
    analyze "$SND" s1
    local ss="$AN"
    info "sender: $(an max_send) sends at most, $(an max_recv) local receives at most; seconds with >=2/3/4 sends alive: $(an sec_ge2)/$(an sec_ge3)/$(an sec_ge4); window $(an window) s for $(an nsends) sends"
    ge "(1) sends overlap in time: at least 3 zfs send processes alive at once" 3 "$(an max_send)"
    le "(1) the total cap holds: never more than 4 zfs send at once" 4 "$(an max_send)"
    le "(1) the per-destination cap holds on the sender: never more than 2 local zfs receive" 2 "$(an max_recv)"
    ge "(1) overlap lasted: seconds with >= 2 sends alive" 3 "$(an sec_ge2)"
    check "(1) the sender ran 12 sends (6 datasets x 2 destinations)" "12" "$(an nsends)"
    analyze "$SRV" s1
    info "server: $(an max_recv) receives at most at once (sampled on $SRV)"
    le "(1) the per-destination cap holds on the server: never more than 2 zfs receive from this client" 2 "$(an max_recv)"
    ge "(1) the server did receive concurrently (at least 2 receives at once)" 2 "$(an max_recv)"
    keep_samples "$SND" s1 s1-sender; keep_samples "$SRV" s1 s1-server
    printf '%s\n' "$ss" > "$LOG_DIR/s1-sender.analysis"
    local lo; lo="$(msh "$SND" "tail -n +$(( ms + 1 )) '$LOG' | grep -E 'zfsbackup[.](remote|local) - INFO - (Sending (full|incremental)|Backup complete)' | python3 '$GDIR/logoverlap.py'")"
    printf '%s\n' "$lo" > "$LOG_DIR/s1-log-overlap.txt"
    info "from the daemon log alone: $(printf '%s' "$lo" | grep -E 'max|sec' | tr '\n' ' ')"
    check "(1) the log shows 12 sends, each from its Sending line to its Backup complete line" "12" "$(printf '%s\n' "$lo" | sed -n 's/^log_sends=//p')"
    ge "(1) the daemon log shows >= 3 sends alive at once (second-resolution cross-check of the sampler)" 3 "$(printf '%s\n' "$lo" | sed -n 's/^log_max_total=//p')"
    le "(1) the daemon log never shows more than 4 sends alive at once" 4 "$(printf '%s\n' "$lo" | sed -n 's/^log_max_total=//p')"
    le "(1) ... nor more than 2 remote ones" 2 "$(printf '%s\n' "$lo" | sed -n 's/^log_max_remote=//p')"
    le "(1) ... nor more than 2 local ones" 2 "$(printf '%s\n' "$lo" | sed -n 's/^log_max_local=//p')"
    local pc; pc="$(msh "$SND" "tail -n +$(( ms + 1 )) '$LOG' | grep -E 'zfsbackup[.](remote|local) - (INFO - Sending (full|incremental)|DEBUG - Nothing to send)' | python3 '$GDIR/paircounts.py' 20")"
    printf '%s\n' "$pc" > "$LOG_DIR/s1-pair-counts.txt"
    info "sends and attempts per pair (period 20 s):"; printf '%s\n' "$pc" | sed 's/^/      /'
    check "(1) every pair sent exactly once (1 snapshot to send: no pair re-sends)" "0" "$(printf '%s\n' "$pc" | sed -n 's/^bad_sends=//p')"
    check "(1) no pair attempted more often than its period allows (window / 20 s + 2)" "0" "$(printf '%s\n' "$pc" | sed -n 's/^bad_attempts=//p')"
    check "(1) all 12 pairs are in the counts" "12" "$(printf '%s\n' "$pc" | sed -n 's/^pairs=//p')"
    fp_dest "(1) remote copies" remote1 '^zpsrc/p'
    fp_dest "(1) local copies" local1 '^zpsrc/p'
    check "(1) no receive_resume_token on the server copies" "0" "$(tokens "$SRV" "$(copy_root remote1)")"
    check "(1) no receive_resume_token on the local copies" "0" "$(tokens "$SND" "$(copy_root local1)")"
    daemon_problems "$SND" "$ms" "(1)"
    daemon_problems "$SRV" "$mv" "(1)"
    local rec; rec="$(status_records)"
    check "(1) the status file holds 12 send records, all ok with no consecutive failure" "12" "$(printf '%s\n' "$rec" | grep -c '=ok/0$' || true)"
    info "log evidence (sender):"; lg_show "$SND" "$ms" 'Sending full snapshot|Backup complete' 14 | sed 's/^/      /'
    stop_daemon "$SND"
}

# ═══════════════════════════════ Scenario 2 - reload during a long send ═════════════════════════════════════════════════
scenario_2() {
    step "Scenario 2: reload during a long send - 2 datasets x (remote1, local1), a 3-step backlog of ${S2_MIB} MiB steps, unit CPUQuota=${CPU_QUOTA:-none}"
    scn_reset
    local i ds=() specs=() names
    # two datasets whose pairs to BOTH destinations are due at the same instant (aligned names): all 4 pairs start together
    names="$(msh "$SND" "env PYTHONPATH='$SRC_PP' '$PY' '$GDIR/pickalign.py' '$CID' remote1,local1 0.02 '$SRC_POOL/r1x{n}' '$SRC_POOL/r2x{n}'")"
    ds=($names)
    [ "${#ds[@]}" -eq 2 ] || die "(2) no aligned names found ($names)"
    for i in 0 1; do mkdata "${ds[$i]}" 8 || die "mkdata"; specs+=("${ds[$i]}=remote1,local1"); done
    info "datasets (start-aligned): ${ds[*]}"
    cfg_write 1d 20s "remote1 local1" "${specs[@]}"
    local s0; s0="$(snap_all "${ds[@]}")"
    start_daemon "$SND" ${CPU_QUOTA:+-p CPUQuota=$CPU_QUOTA}
    s2_base() { anchors_same remote1 '^zpsrc/r' 2 && anchors_same local1 '^zpsrc/r' 2; }
    wait_for 240 s2_base && pass "(2) the baseline $s0 reached both destinations (4 anchors)" || fail "(2) baseline did not complete ($(anchor_state remote1 '^zpsrc/r') / $(anchor_state local1 '^zpsrc/r'))"
    local pid_old gen_old; pid_old="$(wpid "$SND" remote)"
    gen_old="$(cli_s status --json | tr -d '\n' | sed -n 's/.*"stored[^}]*"generation": *\([0-9]*\).*/\1/p')"
    info "remote worker pid before the reload: $pid_old (stored generation $gen_old)"
    # the backlog: three snapshots with ${S2_MIB} MiB of new data each (the log mark comes first: the first step starts at once)
    local k sn=() ms mv
    ms="$(loglines "$SND")"; mv="$(loglines "$SRV")"
    for k in 1 2 3; do
        for i in 0 1; do mkdata "${ds[$i]}" "$S2_MIB" "s$k.bin"; done
        sn+=("$(snap_all "${ds[@]}")")
    done
    info "backlog: ${sn[*]}"
    samplers_start s2
    # the reload: the moment two incremental sends are in flight (the guest-side trigger), change the config
    snd "rm -f '$WDIR/s2.trigger'"
    local trig_cmd="env PYTHONPATH='$SRC_PP' '$PY' -m zfsbackup.cli -c '$DB' set prune_interval 90s"
    msh "$SND" "'$GDIR/trigger.sh' 3 240 '$WDIR/s2.trigger' $trig_cmd"
    local trig; trig="$(msh "$SND" "cat '$WDIR/s2.trigger'")"
    printf '%s\n' "$trig" | sed 's/^/      /' | cut -c1-200
    check_has "(2) the reload was issued while >= 3 incremental sends were in flight" '^INFLIGHT' "$trig"
    ge "(2) how many incremental sends were in flight at the reload" 3 "$(printf '%s\n' "$trig" | grep -c '^INFLIGHT' || true)"
    check_has "(2) the config change succeeded (exit 0)" '^RC=0$' "$trig"
    local n_in; n_in="$(printf '%s\n' "$trig" | grep -c '^INFLIGHT' || true)"
    # the worker is replaced
    s2_replaced() { [ "$(wpid "$SND" remote)" != "$pid_old" ]; }
    wait_for 240 s2_replaced && pass "(2) the remote worker was replaced ($pid_old -> $(wpid "$SND" remote))" || fail "(2) the remote worker was not replaced"
    local out rc; out="$(cli_s status --wait 90)"; rc=$?
    check "(2) status --wait: the daemon is on the stored configuration (exit 0)" "0" "$rc"
    printf '%s\n' "$out" | sed 's/^/      status: /'
    check_has "(2) status reports applied = stored (in sync)" 'in sync|applied' "$out"
    local pidfile; pidfile="$(msh "$SND" "cat '$DB.daemon.pid'")"
    check_lacks "(2) the pid file shows no pending target= or deferred=" '^(target|deferred)=.+' "$pidfile"
    check_has "(2) the pid file shows applied=" '^applied=' "$pidfile"
    # the chain completes
    s2_done() { anchors_same remote1 '^zpsrc/r' 2 && anchors_same local1 '^zpsrc/r' 2; }
    wait_for 360 s2_done && pass "(2) every pair reached the newest snapshot ${sn[2]} after the reload" || fail "(2) the chains did not complete ($(anchor_state remote1 '^zpsrc/r') / $(anchor_state local1 '^zpsrc/r'))"
    sleep 3; samplers_stop
    # what happened to the in-flight steps, from the log (filtered in the guest)
    msh "$SND" "tail -n +$(( ms + 1 )) '$LOG' | grep -nE 'Sending (full|incremental)|Backup complete|Resumed|Reloaded remote worker|exited with code' | cut -c1-260" > "$WORK/s2.log"
    cp "$WORK/s2.log" "$LOG_DIR/s2-evidence.log"
    local rl; rl="$(grep -m1 'Reloaded remote worker' "$WORK/s2.log" | cut -d: -f1)"
    check_has "(2) the log shows the remote worker reloaded" '.' "$rl"
    local line ds sn_name cnt_in cnt_done ok=1
    while read -r line; do
        [ -n "$line" ] || continue
        local tgt="${line##* }"; ds="${tgt%@*}"; sn_name="${tgt#*@}"
        cnt_in="$(printf '%s\n' "$trig" | grep '^INFLIGHT' | grep -c " ${ds}@${sn_name}\$" || true)"
        cnt_done="$(awk -F: -v rl="${rl:-0}" -v pat="Backup complete: ${ds}@${sn_name} " '$1+0 < rl+0 && index($0, pat) {n++} END{print n+0}' "$WORK/s2.log")"
        if [ "$cnt_done" -ge "$cnt_in" ] && [ "$cnt_in" -gt 0 ]; then info "in flight at the reload: ${ds}@${sn_name} x$cnt_in -> completed before the worker was replaced ($cnt_done)"; else ok=0; info "in flight: ${ds}@${sn_name} x$cnt_in, completed before the replacement: $cnt_done"; fi
    done < <(printf '%s\n' "$trig" | grep '^INFLIGHT' | awk '{print $NF}' | sort -u | sed 's/^/x /')
    [ "$ok" -eq 1 ] && pass "(2) every in-flight step was FINISHED before the worker was replaced (no step cut)" || fail "(2) an in-flight step was not logged complete before the replacement"
    check "(2) no stream was torn: no receive_resume_token on the server copies" "0" "$(tokens "$SRV" "$(copy_root remote1)")"
    check "(2) no stream was torn: no receive_resume_token on the local copies" "0" "$(tokens "$SND" "$(copy_root local1)")"
    check "(2) nothing was resumed (no 'Resumed' line)" "0" "$(grep -c 'Resumed' "$WORK/s2.log" || true)"
    check "(2) nothing was re-sent from scratch after the baseline (no 'Sending full')" "0" "$(grep -c 'Sending full' "$WORK/s2.log" || true)"
    check "(2) exactly 12 incremental steps were sent (2 datasets x 2 destinations x 3 steps: none twice, none missing)" "12" "$(grep -c 'Sending incremental' "$WORK/s2.log" || true)"
    check "(2) exactly 12 steps were acknowledged" "12" "$(grep -c 'Backup complete' "$WORK/s2.log" || true)"
    fp_dest "(2) remote copies" remote1 '^zpsrc/r'
    fp_dest "(2) local copies" local1 '^zpsrc/r'
    daemon_problems "$SND" "$ms" "(2)"
    daemon_problems "$SRV" "$mv" "(2)"
    info "evidence (sender log around the reload):"; sed 's/^/      /' "$WORK/s2.log" | cut -c1-210 | head -40
    keep_samples "$SND" s2 s2-sender
    stop_daemon "$SND"
}

# ─── Scenario 2b: the destination's frequency (5 min) is longer than the dataset's (30 s) and the reload lands between chain steps ───
anchor_n() { snd "'$GDIR/anchorcheck.sh' '$1' '$2'" | awk '{print $2}' | trim; }   # how many datasets have an anchor.<dest>
scenario_2b() {
    step "Scenario 2b: reload BETWEEN chain steps, dataset frequency 30 s, destination frequency 5 min: the successor must continue the cut chain at start, not at the next 5-minute slot"
    scn_reset
    local i ds=() specs=() names
    names="$(msh "$SND" "env PYTHONPATH='$SRC_PP' '$PY' '$GDIR/pickalign.py' '$CID' remote1,local1 0.02 '$SRC_POOL/r1y{n}' '$SRC_POOL/r2y{n}'")"
    ds=($names)
    [ "${#ds[@]}" -eq 2 ] || die "(2b) no aligned names found ($names)"
    for i in 0 1; do mkdata "${ds[$i]}" 8 || die "mkdata"; specs+=("${ds[$i]}=remote1,local1"); done
    info "datasets (start-aligned): ${ds[*]}"
    cfg_write 30s 300s "remote1 local1" "${specs[@]}"
    snap_all "${ds[@]}" >/dev/null
    snd "rm -f '$WDIR/s2b.status' '$WDIR/s2b.trigger'; systemd-run --unit=zfsb-ps-statuswatch --collect python3 '$GDIR/statuswatch.py' '$WDIR/s2b.status' '$DB.status/remote-backup-worker.json' >/dev/null 2>&1; true"
    start_daemon "$SND" ${CPU_QUOTA:+-p CPUQuota=$CPU_QUOTA}
    s2b_base() { [ "$(anchor_n remote1 '^zpsrc/r')" = 2 ] && [ "$(anchor_n local1 '^zpsrc/r')" = 2 ]; }
    wait_for 240 s2b_base && pass "(2b) the baseline reached both destinations (4 anchors)" || fail "(2b) baseline did not complete"
    local pid_old ms mv base_snap; pid_old="$(wpid "$SND" remote)"; ms="$(loglines "$SND")"; mv="$(loglines "$SRV")"
    base_snap="$(snd "zfs get -H -r -t filesystem -s local -o value org.zfsbackup:anchor.remote1,org.zfsbackup:anchor.local1 '$SRC_POOL' | sort | tail -n1" | trim)"; base_snap="${base_snap#autosnap_}"
    info "the baseline's newest anchor: autosnap_$base_snap"
    # new data every 15 s; the snapshot worker (30 s) turns it into a backlog of ~48 MiB steps until the pair's next slot (up to 5 min)
    msh "$SND" "systemd-run --unit=zfsb-ps-writer --collect '$GDIR/writer.sh' 24 15 ${ds[*]} >/dev/null 2>&1; true"
    info "waiting for the pairs' next 5-minute slot (a chain of ~48 MiB steps starts then; the reload is issued the moment 3 incremental sends are in flight)"
    local trig_cmd="env PYTHONPATH='$SRC_PP' '$PY' -m zfsbackup.cli -c '$DB' set prune_interval 90s"
    msh "$SND" "'$GDIR/trigger.sh' 3 420 '$WDIR/s2b.trigger' $trig_cmd"
    local trig; trig="$(msh "$SND" "cat '$WDIR/s2b.trigger'")"
    printf '%s\n' "$trig" | sed 's/^/      /' | cut -c1-200
    check_has "(2b) the reload was issued while >= 3 incremental sends were in flight (between chain steps)" '^INFLIGHT' "$trig"
    check_has "(2b) the config change succeeded" '^RC=0$' "$trig"
    local t_trig; t_trig="$(printf '%s\n' "$trig" | sed -n 's/^T_TRIGGER=//p')"
    # only what the CHAIN had to deliver: names newer than the newest anchor of the baseline (a first full sends the newest only)
    snd "zfs list -H -t snapshot -o name -r '$SRC_POOL' | grep -E '^$SRC_POOL/r[12]y' | sort" | awk -v b="autosnap_$base_snap" -F@ '$2 > b' > "$WORK/need"
    info "snapshots that existed at the reload (the chain's cut point must be reached): $(grep -c . "$WORK/need")"
    s2b_replaced() { [ "$(wpid "$SND" remote)" != "$pid_old" ]; }
    wait_for 120 s2b_replaced && pass "(2b) the remote worker was replaced ($pid_old -> $(wpid "$SND" remote))" || fail "(2b) the remote worker was not replaced"
    local t_repl; t_repl="$(msh "$SND" "date +%s" | trim)"
    s2b_caught() {
        local d
        for d in remote1 local1; do
            msh "$(copy_vm "$d")" "'$GDIR/fp.sh' '$(copy_root "$d")' '^zpsrc/r[12]y'" | awk '{print $1}' | sort > "$WORK/have.$d"
            [ -z "$(comm -23 "$WORK/need" "$WORK/have.$d")" ] || return 1
        done
    }
    local t0; t0="$(date +%s)"
    if wait_for 240 s2b_caught; then
        local el=$(( $(date +%s) - t0 ))
        pass "(2b) the cut chain was finished by the successor: both destinations hold every snapshot up to the cut point, ${el} s after the replacement was seen"
        le "(2b) ... inside the catch-up window, far inside the 300 s a wait for the next slot would take" 150 "$el"
    else fail "(2b) the copies still lack snapshots 240 s after the replacement: $(comm -23 "$WORK/need" "$WORK/have.remote1" | wc -l | tr -d ' ') on remote1, $(comm -23 "$WORK/need" "$WORK/have.local1" | wc -l | tr -d ' ') on local1"; fi
    bg_stop
    local sw; sw="$(msh "$SND" "cat '$WDIR/s2b.status'")"
    printf '%s\n' "$sw" | sed 's/^/      statuswatch: /' | cut -c1-160 | head -12
    local inc; inc="$(printf '%s\n' "$sw" | grep -c "incomplete=True.*pid=$pid_old\$" || true)"
    ge "(2b) the status file showed incomplete: true for pair(s) written by the OLD worker (pid $pid_old) before the replacement" 1 "$inc"
    local when; when="$(printf '%s\n' "$sw" | grep "incomplete=True.*pid=$pid_old\$" | head -n1 | cut -f1)"
    [ -n "$when" ] && info "first incomplete record $(awk -v a="$when" -v b="$t_trig" 'BEGIN{printf "%.1f", a-b}') s after the reload request"
    msh "$SND" "tail -n +$(( ms + 1 )) '$LOG' | grep -nE 'Sending (full|incremental)|Backup complete|Reloaded remote worker' | cut -c1-260" > "$WORK/s2b.log"
    cp "$WORK/s2b.log" "$LOG_DIR/s2b-evidence.log"
    local rl_t first_t
    rl_t="$(grep -m1 'Reloaded remote worker' "$WORK/s2b.log" | sed -n 's/^[0-9]*:[0-9-]* \([0-9:]*\),.*/\1/p')"
    first_t="$(awk '/Reloaded remote worker/{f=1; next} f && /Sending incremental/{print; exit}' "$WORK/s2b.log" | sed -n 's/^[0-9]*:[0-9-]* \([0-9:]*\),.*/\1/p')"
    if [ -n "$rl_t" ] && [ -n "$first_t" ]; then
        local gap; gap="$(awk -v a="$first_t" -v b="$rl_t" 'function sec(x,  p){split(x,p,":"); return p[1]*3600+p[2]*60+p[3]} BEGIN{printf "%d", sec(a)-sec(b)}')"
        info "the replacement worker logged 'Reloaded' at $rl_t and sent its first step at $first_t (${gap} s later)"
        le "(2b) the successor continued the chain within the catch-up delay (frac x 30 s) plus startup, not at the next 300 s slot" 75 "$gap"
    else fail "(2b) no 'Reloaded' line followed by a 'Sending incremental' line in the log"; fi
    check "(2b) no step sent twice (same transport, same dataset, same from -> to)" "0" "$(sed 's/^[0-9]*:[0-9-]* [0-9:,]* - //;s/ - INFO - / /' "$WORK/s2b.log" | grep 'Sending incremental' | sort | uniq -d | wc -l | tr -d ' ')"
    check "(2b) nothing re-sent from scratch (no 'Sending full' after the baseline)" "0" "$(grep -c 'Sending full' "$WORK/s2b.log" || true)"
    s2b_idle() { [ "$(all_strays)" = "0" ]; }
    wait_for 120 s2b_idle || warn "(2b) zfs send/receive processes still running"
    check "(2b) no stream was torn: no receive_resume_token on either side (read while no send is running)" "0" "$(( $(tokens "$SRV" "$(copy_root remote1)") + $(tokens "$SND" "$(copy_root local1)") ))"
    daemon_problems "$SND" "$ms" "(2b)"
    daemon_problems "$SRV" "$mv" "(2b)"
    stop_daemon "$SND"
}

# ═══════════════════════════════ Scenario 3 - concurrent local sends ════════════════════════════════════════════════════
R3_ROUNDS="${R3_ROUNDS:-18}"
scenario_3() {
    step "Scenario 3: concurrent local sends into a MISSING container tree - $R3_ROUNDS rounds, 4 datasets to local1 (siblings and cousins), start-aligned names"
    local r names a b c d ms ok_rounds=0 busy_total=0 vp
    vp="$(msh "$SND" "'$GDIR/visprobe.sh' '$TGT_A/visprobe' 40")"
    info "root-cause probe (zfs only, no daemon): $vp"
    case "$vp" in *without_prop=0*) ;; *) info "(3) OpenZFS $(msh "$SND" "zfs version | head -n1" | trim) lists a freshly created container before 'zfs get -s local' can read its client_id (the premise of the re-read under the claim): $vp over 40 creates" ;; esac
    local reread_total=0 owner_errors=0
    for r in $(seq 1 "$R3_ROUNDS"); do
        scn_reset
        # four names whose pair fractions for local1 lie within 0.002 of each other: the scheduler wakes for all of them at once
        names="$(msh "$SND" "env PYTHONPATH='$SRC_PP' '$PY' '$GDIR/pickalign.py' '$CID' local1 0.002 '$SRC_POOL/g$r/a{n}' '$SRC_POOL/g$r/b{n}' '$SRC_POOL/h$r/x/c{n}' '$SRC_POOL/h$r/y/d{n}'")"
        set -- $names
        if [ "$#" -ne 4 ]; then fail "(3) round $r: no aligned names found ($names)"; continue; fi
        a="$1"; b="$2"; c="$3"; d="$4"
        info "round $r: siblings $a, $b (container $SRC_POOL/g$r missing on the target); cousins $c, $d (containers $SRC_POOL/h$r, h$r/x, h$r/y)"
        local ds
        for ds in "$a" "$b" "$c" "$d"; do mkdata "$ds" 32 || die "mkdata $ds"; done
        cfg_write 1d 20s "local1" "$a=local1" "$b=local1" "$c=local1" "$d=local1"
        snap_all "$a" "$b" "$c" "$d" >/dev/null 2>&1 || true
        ms="$(loglines "$SND")"
        samplers_start s3r$r
        start_daemon "$SND"
        r3_done() { anchors_same local1 "^$SRC_POOL/[gh]$r/" 4; }
        local t0; t0="$(date +%s)"
        if wait_for 300 r3_done; then pass "(3) round $r: all 4 copies completed (anchors on the newest snapshot, $(( $(date +%s) - t0 )) s)"; ok_rounds=$((ok_rounds+1))
        else fail "(3) round $r: not all 4 copies completed ($(anchor_state local1 "^$SRC_POOL/[gh]$r/"))"; fi
        sleep 22   # a full pair cycle: nothing may be retried or fail after completion
        samplers_stop
        analyze "$SND" "s3r$r"
        info "round $r: at most $(an max_send) sends alive at once, $(an max_recv) receives; $(an nsends) send processes"
        le "(3) round $r: the per-destination cap holds (never more than 2 local receives)" 2 "$(an max_recv)"
        fp_dest "(3) round $r local copies" local1 "^$SRC_POOL/[gh]$r/"
        # every container the receive made is owned and the copy tree is complete: 4 copies, their containers
        check "(3) round $r: the copy tree holds exactly the 4 datasets (+ containers $TGT_A/$CID, .../$SRC_POOL, g$r|h$r, h$r/x, h$r/y)" "4 4" \
            "$(msh "$SND" "echo \$(zfs list -H -r -t filesystem -o name '$(copy_root local1)' | grep -cE '/[gh]$r/(a|b|x/c|y/d)[0-9]+\$') \$(zfs list -H -r -t snapshot -o name '$(copy_root local1)' | grep -c .)")"
        check "(3) round $r: no receive_resume_token on any copy" "0" "$(tokens "$SND" "$(copy_root local1)")"
        daemon_problems "$SND" "$ms" "(3) round $r"
        local oe; oe="$(lg_count "$SND" "$ms" 'not recorded as a backup')"
        owner_errors=$((owner_errors + oe))
        check "(3) round $r: no ownership ERROR ('not recorded as a backup': a container another sender was creating)" "0" "$oe"
        local rr; rr="$(msh "$SND" "tail -n +$(( ms + 1 )) '$LOG' | grep -E 'Executing command: .*(zfs get -H -o value -s local org.zfsbackup:client_id|zfs create -u -o canmount=noauto)' | python3 '$GDIR/rereads.py' | sed -n 's/^reread_candidates=//p'" | trim)"
        reread_total=$((reread_total + rr))
        info "round $r: owner reads inside the creation race window (busy path or re-read under the claim): $rr"
        local busy; busy="$(lg_count "$SND" "$ms" 'is busy: another send is creating')"
        busy_total=$((busy_total + busy))
        info "round $r: $busy 'is busy: another send is creating ...' (the negotiate claim lost to a sibling) INFO message(s)"
        lg_show "$SND" "$ms" 'is busy: another send is creating|Backup complete' 8 | sed 's/^/      /'
        local rec; rec="$(status_records)"
        check "(3) round $r: the last status record of every pair is ok with no consecutive failure (no busy left)" "4" "$(printf '%s\n' "$rec" | grep -c '=ok/0$' || true)"
        local last; last="$(msh "$SND" "tail -n 60 '$LOG' | grep -cE 'is busy: another send|Negotiation failed|already exists' || true" | trim)"
        check "(3) round $r: nothing busy or failing in the last 60 log lines" "0" "$last"
        keep_samples "$SND" "s3r$r" "s3r$r-sender"
        stop_daemon "$SND"
    done
    check "(3) all $R3_ROUNDS rounds completed" "$R3_ROUNDS" "$ok_rounds"
    check "(3) no ownership ERROR in any of the $R3_ROUNDS rounds" "0" "$owner_errors"
    info "over $R3_ROUNDS rounds: busy path (INFO 'is busy: another send is creating') $busy_total time(s); owner reads inside the creation race window (0.1 s after a container's zfs create started; resolved by the busy path or the re-read under the claim) $reread_total time(s)"
    if [ "$busy_total" -eq 0 ]; then finding "(3) the negotiate claim was never contended in $R3_ROUNDS rounds: the race was not provoked, the check proves only that concurrent first sends complete"
    else info "the negotiate claim was lost $busy_total time(s) in $R3_ROUNDS rounds and every loser completed later"; fi
}

# ═══════════════════════════════ Scenario 4 - a hung local target pool ══════════════════════════════════════════════════
# receives into the frozen pool that sleep in the kernel (state D, or S waiting in cv_wait/txg_wait: the begin of a receive is a sync task)
hung_receives() { msh "$SND" "ps -eo stat,wchan:32,args | awk '/zfs (receive|recv) /' | grep '$HANG_POOL' | awk '\$1 ~ /^D/ || \$2 ~ /cv_wait|txg|zio/' | wc -l" | trim; }
all_strays() { msh "$SND" "pgrep -fc '[z]fs (send|receive|recv) ' || true" | trim; }
daemon_procs() { msh "$SND" "pgrep -fc '[z]fsbackup.daemon' || true" | trim; }

scenario_4() {
    step "Scenario 4: a HUNG local target pool ($HANG_POOL, dm-linear, frozen with dmsetup suspend) on $SND; remote1 keeps going; about 8 minutes"
    scn_reset
    hang_pool_create
    local i ds=() specs=()
    # remote1 (the server), hung1 (the pool that is frozen) and local1 (a healthy local pool: the registry of stuck children is
    # process-wide, so the abandoned child on hung1 also makes the daemon skip local1 - a documented trade-off, checked below)
    for i in 1 2 3; do mkdata "$SRC_POOL/h$i" 8 || die "mkdata"; ds+=("$SRC_POOL/h$i"); specs+=("$SRC_POOL/h$i=remote1,hung1,local1"); done
    cfg_write 1d 20s "remote1 hung1 local1" "${specs[@]}"
    local s0; s0="$(snap_all "${ds[@]}")"
    local ms mv; ms="$(loglines "$SND")"; mv="$(loglines "$SRV")"
    samplers_start s4
    start_daemon "$SND" ${CPU_QUOTA:+-p CPUQuota=$CPU_QUOTA}
    s4_base() { anchors_same remote1 '^zpsrc/h' 3 && anchors_same hung1 '^zpsrc/h' 3 && anchors_same local1 '^zpsrc/h' 3; }
    wait_for 240 s4_base && pass "(4) baseline $s0 reached remote1, local1 and the (still healthy) hung1 target" || fail "(4) the baseline did not complete"
    local mpid; mpid="$(msh "$SND" "systemctl show -p MainPID --value $UNIT" | trim)"
    # the backlog that will be in flight when the pool freezes
    for i in 1 2 3; do mkdata "$SRC_POOL/h$i" "$S4_MIB" big1.bin; done
    local s1; s1="$(snap_all "${ds[@]}")"
    snd "rm -f '$WDIR/s4.freeze'"
    msh "$SND" "'$GDIR/freeze.sh' '$HANG_DEV' 240 '$WDIR/s4.freeze' 0.4"
    local fz; fz="$(msh "$SND" "cat '$WDIR/s4.freeze'")"
    printf '%s\n' "$fz" | sed 's/^/      /' | cut -c1-200
    check_has "(4) the target pool was frozen (dmsetup suspend) while a send to it ran" '^T_FREEZE=' "$fz"
    check_has "(4) dmsetup suspend succeeded" '^SUSPEND_RC=0' "$fz"
    local t_freeze; t_freeze="$(printf '%s\n' "$fz" | sed -n 's/^T_FREEZE=//p')"
    local mf; mf="$(loglines "$SND")"
    s4_stuck() { [ "$(hung_receives)" -ge 1 ]; }
    wait_for 90 s4_stuck && pass "(4) the receive into $HANG_POOL is blocked in the kernel (D, or waiting on the txg): $(hung_receives) of them" \
        || fail "(4) no receive into the frozen pool is blocked in the kernel (the stream may have finished before the freeze)"
    # the other destination keeps being served: remote1 gets the whole backlog although hung1 is stuck
    s4_remote1() { anchors_same remote1 '^zpsrc/h' 3; }
    wait_for 300 s4_remote1 && pass "(4) remote1 received the backlog $s1 for all 3 datasets WHILE hung1 is frozen" || fail "(4) remote1 did not get the backlog ($(anchor_state remote1 '^zpsrc/h'))"
    check "(4) hung1 did NOT advance (its anchors still name the baseline, not $s1)" "no" "$(anchors_same hung1 '^zpsrc/h' 3 && echo yes || echo no)"
    check "(4) the daemon supervisor is still running (pid $mpid)" "yes" "$(msh "$SND" "[ -d /proc/$mpid ] && echo yes || echo no" | trim)"
    daemon_active "$SND" && pass "(4) the unit is active" || fail "(4) the unit is not active"
    # new snapshots while the hang lasts
    for i in 1 2 3; do mkdata "$SRC_POOL/h$i" 16 s2.bin; done
    local s2; s2="$(snap_all "${ds[@]}")"
    wait_for 300 s4_remote1 && pass "(4) a snapshot taken during the hang ($s2) also reached remote1" || fail "(4) $s2 did not reach remote1"
    samplers_stop
    analyze "$SND" s4 - - "$HANG_T/"
    info "sampler: at most $(an max_send) sends and $(an max_recv) local receives ($(an max_recv_pfx) into the frozen pool) at once, $(an max_d) process(es) in D state at once"
    le "(4) the caps hold during the hang: at most 4 sends in all" 4 "$(an max_send)"
    le "(4) ... and at most 2 receives into the frozen pool (its destination's cap)" 2 "$(an max_recv_pfx)"
    samplers_start s4b
    # the first TIMED zfs call on the frozen pool (a negotiate: `zfs set`/`zfs list`, 60 s budget, then TERM and KILL that a D-state
    # child survives) abandons the child; a stream that merely stalls is cut only after the pump's 600 s (LOCAL_STALL_TIMEOUT)
    info "waiting for a timed zfs call on the frozen pool to run out its 60 s budget; the daemon keeps serving remote1 meanwhile"
    s4_abandon() { [ "$(lg_count "$SND" "$ms" 'did not exit after SIGKILL')" -ge 1 ]; }
    wait_for 900 s4_abandon && pass "(4) a timed zfs call on the frozen pool ran out its budget; the child that survived SIGKILL was abandoned (logged)" \
        || fail "(4) no abandon message within 15 minutes of the freeze"
    local t_ab; t_ab="$(msh "$SND" "date +%s.%N" | trim)"
    info "abandon logged $(awk -v a="$t_ab" -v b="${t_freeze:-0}" 'BEGIN{printf "%.0f", a-b}') s after the freeze"
    lg_show "$SND" "$ms" 'did not exit after SIGKILL' 3 | sed 's/^/      /'
    # now every new local attempt is skipped, one message per pair and episode; remote1 is untouched
    for i in 1 2 3; do mkdata "$SRC_POOL/h$i" 16 s3.bin; done
    local s3; s3="$(snap_all "${ds[@]}")"
    wait_for 300 s4_remote1 && pass "(4) after the abandon a new snapshot ($s3) still reaches remote1" || fail "(4) $s3 did not reach remote1 after the abandon"
    sleep 45
    check "(4) trade-off (the stuck-child registry is per process): the healthy local destination local1 was skipped too, so it still lacks $s3" "no" "$(anchors_same local1 '^zpsrc/h' 3 && echo yes || echo no)"
    local skip1; skip1="$(lg_count "$SND" "$ms" 'zfs did not answer within 60 s on this host; skipping this destination')"
    ge "(4) the local destination was skipped with a message (WARNING, once per pair and episode)" 1 "$skip1"
    le "(4) at most one such message per dataset (3 datasets)" 3 "$skip1"
    lg_show "$SND" "$ms" 'zfs did not answer within' 3 | sed 's/^/      /'
    sleep 100     # five more 20 s cycles: no message may repeat
    local skip2; skip2="$(lg_count "$SND" "$ms" 'zfs did not answer within 60 s on this host; skipping this destination')"
    check "(4) no repeat of the message over the next 100 s (one per episode)" "$skip1" "$skip2"
    local w2; w2="$(lg_count "$SND" "$ms" ' - zfsbackup.local - WARNING - local backup to hung1 skipped')"
    ge "(4) the destination-wide 'local backup to hung1 skipped' WARNING was logged" 1 "$w2"
    le "(4) ... at most once per pair (the repeats are DEBUG)" 3 "$w2"
    check "(4) no Traceback in the daemon log" "0" "$(lg_count "$SND" "$ms" 'Traceback')"
    check "(4) no ERROR about remote1 (the healthy destination)" "0" "$(lg_count "$SND" "$ms" ' - ERROR - .*(https://|remote1)')"
    info "ERROR lines during the hang (expected: the stalled local transfers):"; lg_show "$SND" "$ms" ' - ERROR - ' 4 | sed 's/^/      /'
    samplers_stop
    # STOP: the supervisor must leave inside its budget although a receive is in D state and a sender thread is in its pump
    step "(4) stopping the daemon while the pool is still frozen"
    local t0 t1 el
    t0="$(msh "$SND" "date +%s.%N" | trim)"
    msh "$SND" "systemctl stop --no-block $UNIT; true"
    s4_gone() { [ "$(msh "$SND" "[ -d /proc/$mpid ] && echo yes || echo no" | trim)" = "no" ]; }
    wait_for 150 s4_gone && pass "(4) the supervisor process exited" || fail "(4) the supervisor (pid $mpid) was still alive 150 s after the stop"
    t1="$(msh "$SND" "date +%s.%N" | trim)"
    el="$(awk -v a="$t1" -v b="$t0" 'BEGIN{printf "%.0f", a-b}')"
    info "the supervisor left $el s after the stop request (the poll itself adds up to ~3 s)"
    le "(4) STOP completed within the supervisor's budget (30 s + escalation; unit TimeoutStopSec=90)" 90 "$el"
    check "(4) no zfsbackup daemon/worker process is left" "0" "$(daemon_procs)"
    info "zfs processes left while the pool is frozen: $(all_strays) (children stuck in the kernel, outliving their worker)"
    lg_show "$SND" "$ms" 'Daemon stopped|Shutting down|exited with code|SIGKILL|terminat' 8 | sed 's/^/      /'
    # RESUME: the strays exit by themselves, the pool is healthy
    hang_resume
    s4_nostrays() { [ "$(all_strays)" = "0" ]; }
    wait_for 120 s4_nostrays && pass "(4) after dmsetup resume the stray zfs processes exited" || fail "(4) zfs processes remain after the resume: $(all_strays)"
    check_has "(4) the hang pool is healthy again (zpool status -x)" 'is healthy' "$(msh "$SND" "zpool status -x '$HANG_POOL' 2>&1")"
    snd "systemctl reset-failed $UNIT >/dev/null 2>&1; true"
    info "resume tokens left on the copies by the cut streams: $(tokens "$SND" "$(copy_root hung1)")"
    # a restarted daemon finishes the job: every copy complete
    local m2; m2="$(loglines "$SND")"
    start_daemon "$SND"
    s4_conv() { anchors_same hung1 '^zpsrc/h' 3 && anchors_same remote1 '^zpsrc/h' 3 && anchors_same local1 '^zpsrc/h' 3; }
    wait_for 600 s4_conv && pass "(4) after the resume and a daemon restart every hung1 copy completed (anchors on $s3)" || fail "(4) hung1 did not converge ($(anchor_state hung1 '^zpsrc/h'))"
    fp_dest "(4) hung1 copies" hung1 '^zpsrc/h'
    fp_dest "(4) remote1 copies" remote1 '^zpsrc/h'
    fp_dest "(4) local1 copies" local1 '^zpsrc/h'
    check "(4) no receive_resume_token left on the hung1 copies" "0" "$(tokens "$SND" "$(copy_root hung1)")"
    check "(4) no receive_resume_token left on the remote1 copies" "0" "$(tokens "$SRV" "$(copy_root remote1)")"
    info "log after the restart:"; lg_show "$SND" "$m2" 'Resumed|Sending (full|incremental)|Backup complete' 6 | sed 's/^/      /'
    stop_daemon "$SND"
    keep_samples "$SND" s4 s4-sender; keep_samples "$SND" s4b s4b-sender
    hang_pool_destroy
}

# ═══════════════════════════════ Scenario 5 - throughput at 1, 2 and 4 concurrent senders ═══════════════════════════════
cpulog_start() { local vm; for vm in "$SND" "$SRV"; do msh "$vm" "systemctl stop zfsb-ps-cpulog >/dev/null 2>&1; systemctl reset-failed zfsb-ps-cpulog >/dev/null 2>&1; rm -f '$WDIR/$1.cpu'; systemd-run --unit=zfsb-ps-cpulog --collect python3 '$GDIR/cpulog.py' '$WDIR/$1.cpu' 0.25 >/dev/null 2>&1; true" || true; done; }
cpulog_stop()  { local vm; for vm in "$SND" "$SRV"; do msh "$vm" "systemctl stop zfsb-ps-cpulog >/dev/null 2>&1; systemctl reset-failed zfsb-ps-cpulog >/dev/null 2>&1; true" || true; done; }
CPU=""
cpuan() { CPU="$(msh "$1" "python3 '$GDIR/cpuan.py' '$WDIR/$2.cpu' $3 $4")"; }
cp_() { printf '%s\n' "$CPU" | sed -n "s/^$1=//p" | head -n1; }
THRU_FILE=""
# precreate "<dests>": the owned container tree <target>/<id> and <target>/<id>/<pool>, so that the first sends do not queue on the
# negotiate claim (two senders that find the same container missing: the loser is retried a cycle later) and the numbers are the
# transfers', not the container creation's
precreate() {
    local d t vm
    for d in $1; do
        case "$d" in
            remote1) vm="$SRV"; t="$SRV_TARGET" ;; local1) vm="$SND"; t="$TGT_A" ;; local2) vm="$SND"; t="$TGT_B" ;; *) continue ;;
        esac
        msh "$vm" "zfs create -u -o canmount=noauto -o org.zfsbackup:client_id=$CID '$t/$CID' && zfs create -u -o canmount=noauto -o org.zfsbackup:client_id=$CID '$t/$CID/$SRC_POOL'" || die "precreate $t"
    done
}
thru_mib() { case "$1" in *remote1*) echo "$THRU_REMOTE_MIB" ;; *) echo "$THRU_MIB" ;; esac; }
raw_round() { # raw_round <label> <n datasets> "<dests>": RAW_RATE (MiB/s), RAW_BUSY (cores), RAW_WALL
    local label="$1" n="$2" dests="$3" i d specs=() port=9100 srv_out=() ds=() t0 t_end_local wait_srv=0 mib; mib="$(thru_mib "$dests")"
    RAW_RATE=0; RAW_BUSY=0; RAW_WALL=0
    case "$dests" in *remote1*) SRV_DAEMON_OPTIONAL=1; WAIT_GUARD="" ;; esac
    scn_reset
    case "$dests" in *remote1*) stop_daemon "$SRV"; msh "$SRV" "sync; echo 3 > /proc/sys/vm/drop_caches; true" ;; esac   # the raw network leg needs no daemon (and its RAM)
    srv "zfs destroy -r '$SRV_POOL/raw' >/dev/null 2>&1; zfs create -o canmount=off '$SRV_POOL/raw'; rm -f '$WDIR'/nc*.out; true" || true
    snd "zfs destroy -r '$TGT_A/raw' >/dev/null 2>&1; zfs destroy -r '$TGT_B/raw' >/dev/null 2>&1; zfs create -o canmount=off '$TGT_A/raw'; zfs create -o canmount=off '$TGT_B/raw'; true" || true
    for i in $(seq 1 "$n"); do mkdata "$SRC_POOL/u$i" "$mib" || die "mkdata"; ds+=("$SRC_POOL/u$i"); done
    local sn; sn="$(snap_all "${ds[@]}")"
    for d in $dests; do
        for i in $(seq 1 "$n"); do
            case "$d" in
                local1) specs+=("l:$SRC_POOL/u$i@$sn:$TGT_A/raw/u$i") ;;
                local2) specs+=("l:$SRC_POOL/u$i@$sn:$TGT_B/raw/u$i") ;;
                remote1)
                    port=$((port+1)); srv_out+=("$WDIR/nc$port.out"); wait_srv=1
                    srv "systemd-run --unit=zfsb-ps-nc$port --collect '$GDIR/ncrecv.sh' $port '$SRV_POOL/raw/u$i' '$WDIR/nc$port.out' >/dev/null 2>&1; true"
                    specs+=("n:$SRC_POOL/u$i@$sn:$SRV_IP:$port") ;;
            esac
        done
    done
    [ "$wait_srv" -eq 1 ] && sleep 2
    cpulog_start "raw-$label"
    sleep 1
    snd "rm -f '$WDIR/raw-$label.out'; '$GDIR/rawmix.sh' '$WDIR/raw-$label.out' ${specs[*]}"
    local out; out="$(msh "$SND" "cat '$WDIR/raw-$label.out'")"
    t0="$(printf '%s\n' "$out" | sed -n 's/^t0=//p')"; t_end_local="$(printf '%s\n' "$out" | sed -n 's/^t_end=//p')"
    local t_end="$t_end_local" f te
    if [ "$wait_srv" -eq 1 ]; then
        raw_srv_done() { local f; for f in "${srv_out[@]}"; do msh "$SRV" "[ -s '$f' ]" || return 1; done; }
        WAIT_GUARD="" wait_for 120 raw_srv_done || warn "raw $label: the server end did not finish"
        for f in "${srv_out[@]}"; do
            te="$(msh "$SRV" "sed -n 's/^t_end=//p' '$f'" | trim)"
            [ -n "$te" ] && t_end="$(awk -v a="$t_end" -v b="$te" 'BEGIN{print (b>a)?b:a}')"
        done
    fi
    sleep 1; cpulog_stop
    RAW_WALL="$(awk -v a="$t_end" -v b="$t0" 'BEGIN{printf "%.2f", a-b}')"
    RAW_RATE="$(awk -v w="$RAW_WALL" -v m="$mib" -v k="${#specs[@]}" 'BEGIN{printf "%.1f", m*k/w}')"
    cpuan "$SND" "raw-$label" "$t0" "$t_end"; RAW_BUSY="$(cp_ cores_busy)"
    info "raw $label: ${#specs[@]} pipeline(s) x $mib MiB in $RAW_WALL s = $RAW_RATE MiB/s (sender VM $RAW_BUSY of $(cp_ ncpu) cores busy)"
    check "(5) raw $label: every pipeline succeeded" "rc=0" "$(printf '%s\n' "$out" | grep '^rc=')"
    snd "zfs destroy -r '$TGT_A/raw' >/dev/null 2>&1; zfs destroy -r '$TGT_B/raw' >/dev/null 2>&1; true" || true
    srv "zfs destroy -r '$SRV_POOL/raw' >/dev/null 2>&1; true" || true
    SRV_DAEMON_OPTIONAL=0; WAIT_GUARD="server_alive"
}
daemon_round() { # daemon_round <label> <n datasets> "<dests>": DM_RATE, DM_WINDOW, DM_PY, DM_CHILD, DM_BUSY, DM_MAX
    local label="$1" n="$2" dests="$3" i d names ds=() specs=() tpl=() mib; mib="$(thru_mib "$dests")"
    DM_RATE=0; DM_WINDOW=0; DM_PY=0; DM_CHILD=0; DM_BUSY=0; DM_MAX=0
    scn_reset
    case "$dests" in *remote1*) server_fresh ;; esac
    local csv; csv="$(printf '%s' "$dests" | tr ' ' ',')"
    for i in $(seq 1 "$n"); do tpl+=("$SRC_POOL/t${i}x{n}"); done
    names="$(msh "$SND" "env PYTHONPATH='$SRC_PP' '$PY' '$GDIR/pickalign.py' '$CID' '$csv' 0.02 $(printf "'%s' " "${tpl[@]}")")"
    set -- $names
    [ "$#" -eq "$n" ] || { fail "(5) $label: no start-aligned names found"; return 1; }
    for ds in "$@"; do mkdata "$ds" "$mib" || die "mkdata"; specs+=("$ds=${csv}"); done
    local all=("$@"); local sn; sn="$(snap_all "${all[@]}")"
    cfg_write 1d 20s "$dests" "${specs[@]}"
    precreate "$dests"
    local ms mv; ms="$(loglines "$SND")"; mv="$(loglines "$SRV")"
    samplers_start "dm-$label"; cpulog_start "dm-$label"
    start_daemon "$SND"
    local want=$n
    dm_done() { local x; for x in $dests; do anchors_same "$x" "^$SRC_POOL/t" "$want" || return 1; done; }
    wait_for 900 dm_done && pass "(5) $label: all $(( n * $(printf '%s\n' $dests | wc -l) )) pairs completed" || fail "(5) $label: not all pairs completed"
    sleep 3; samplers_stop; cpulog_stop
    analyze "$SND" "dm-$label"
    local first last; first="$(an first_send)"; last="$(an last_send_end)"
    DM_MAX="$(an max_send)"; DM_WINDOW="$(an window)"
    local total=$(( n * $(printf '%s\n' $dests | wc -l) ))
    DM_RATE="$(awk -v w="$DM_WINDOW" -v m="$mib" -v k="$total" 'BEGIN{printf "%.1f", (w>0)? m*k/w : 0}')"
    printf '%s\n' "$AN" > "$LOG_DIR/thru-$label.analysis"
    cpuan "$SND" "dm-$label" "$first" "$last"; DM_PY="$(cp_ py_cpu)"; DM_CHILD="$(cp_ child_cpu)"; DM_BUSY="$(cp_ cores_busy)"
    info "daemon $label: $total send(s) x $mib MiB, first send to last end $DM_WINDOW s = $DM_RATE MiB/s; max $DM_MAX concurrent; worker python ${DM_PY}s + children ${DM_CHILD}s CPU, VM $DM_BUSY of $(cp_ ncpu) cores busy"
    case "$dests" in *remote1*) cpuan "$SRV" "dm-$label" "$first" "$last"; info "  server VM: $(cp_ cores_busy) of $(cp_ ncpu) core(s) busy, its daemon's python ${CPU:+$(cp_ py_cpu)}s" ;; esac
    # a mixed round (remote1 + a local destination): the local legs of the same dataset end in a fraction of a second, so all
    # four need not be alive at the same instant; one fewer than the pairs is accepted there
    local want_max; want_max="$(awk -v t="$total" 'BEGIN{print (t>4)?4:t}')"
    case "$dests" in *remote1*local*) want_max=$((want_max - 1)) ;; esac
    ge "(5) $label: concurrency reached the number of pairs (one fewer in a mixed round)" "$want_max" "$DM_MAX"
    for d in $dests; do fp_dest "(5) $label" "$d" "^$SRC_POOL/t"; done
    daemon_problems "$SND" "$ms" "(5) $label"
    keep_samples "$SND" "dm-$label" "thru-$label-sender"
    stop_daemon "$SND"
}
scenario_5() {
    step "Scenario 5: throughput at 1, 2 and 4 concurrent senders; $THRU_MIB MiB per dataset ($THRU_REMOTE_MIB with remote1), $THRU_REPS repetition(s); raw 'zfs send | zfs receive' (and '| nc |') for comparison; unit uncapped"
    THRU_FILE="$LOG_DIR/throughput.tsv"
    DAEMON_V=""
    printf 'config\tsenders\tmib_each\trep\traw_MiBs\tdaemon_MiBs\tdaemon_over_raw\tmax_concurrent\tworker_py_cpu_s\tworker_children_cpu_s\tvm_cores_busy\n' > "$THRU_FILE"
    local cfgs=("L1|1|local1" "L2|2|local1" "L4|2|local1 local2" "N1|1|remote1" "N2|2|remote1" "N4|2|remote1 local1")
    local c label n dests rep senders
    for c in "${cfgs[@]}"; do
        IFS='|' read -r label n dests <<<"$c"
        senders=$(( n * $(printf '%s\n' $dests | wc -l) ))
        for rep in $(seq 1 "$THRU_REPS"); do
            step "(5) $label rep $rep: $n dataset(s) x [$dests] = $senders concurrent sender(s)"
            raw_round "$label-r$rep" "$n" "$dests"
            daemon_round "$label-r$rep" "$n" "$dests" || continue
            printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$label" "$senders" "$(thru_mib "$dests")" "$rep" "$RAW_RATE" "$DM_RATE" \
                "$(awk -v a="$DM_RATE" -v b="$RAW_RATE" 'BEGIN{printf "%.2f", (b>0)? a/b : 0}')" "$DM_MAX" "$DM_PY" "$DM_CHILD" "$DM_BUSY" >> "$THRU_FILE"
        done
    done
    DAEMON_V="-v"
    echo; info "THROUGHPUT (MiB/s per run; see $THRU_FILE):"; column -t -s "$(printf '\t')" "$THRU_FILE" | sed 's/^/    /'
    info "MEANS over the repetitions:"
    awk -F'\t' 'NR>1{n[$1]++; sn[$1]=$2; mib[$1]=$3; r[$1]+=$5; d[$1]+=$6; py[$1]+=$9; ch[$1]+=$10; mx[$1]=($8>mx[$1])?$8:mx[$1]} END{
        printf "    %-6s %-8s %-9s %-10s %-12s %-7s %-12s %-14s %-17s\n","config","senders","mib_each","raw_MiB/s","daemon_MiB/s","ratio","py_cpu_s","py_ms_per_MiB","child_cpu_s(mean)";
        for (k in n) printf "    %-6s %-8s %-9s %-10.1f %-12.1f %-7.2f %-12.2f %-14.2f %-17.2f\n", k, sn[k], mib[k], r[k]/n[k], d[k]/n[k], (r[k]>0)?d[k]/r[k]:0, py[k]/n[k], (py[k]/n[k])*1000/(sn[k]*mib[k]), ch[k]/n[k] }' "$THRU_FILE" | sort -k1,1 | tee "$LOG_DIR/throughput-means.txt"
}

# ═══════════════════════════════ main ═══════════════════════════════════════════════════════════════════════════════════
main() {
    setup
    scn 1 && scenario_1
    if scn 2; then scenario_2; scenario_2b; fi
    scn 3 && scenario_3
    scn 4 && scenario_4
    scn 5 && scenario_5
    return 0
}
main
