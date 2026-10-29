#!/usr/bin/env bash
#
# test_install.sh -- prove that the documented INSTALL of zfsbackup works on real systemd (plan item C22, B-8).
#
# Two disposable Multipass VMs, one per supported systemd generation (nothing else is touched):
#   zfsb-inst22  Ubuntu 22.04, systemd 249 (no Type=notify-reload: the pre-253 drop-in), Python 3.10 from the distro, so the
#                venv comes from a uv-managed interpreter, exactly as zfsbackup/README.md ("Install ...") describes
#   zfsb-inst24  Ubuntu 24.04, systemd 255 (Type=notify-reload), the distro's python3 (3.12) + `python3 -m venv`
#
# The artifacts are built on the HOST with `poetry build`: TWO wheels since the packaging split (`zfsbackup`, built at the repo root, which
# also makes the sdist carrying `packaging/`, and `libzfseasy`, built in libs/libzfseasy; the first depends on the second at the same
# version), or ONE wheel for a tree from before the split (SRC_REF/OLD_REF at an older ref: no libs/libzfseasy/pyproject.toml). They are
# shipped with `multipass transfer`, and installed the way the README does it: a venv at /opt/zfsbackup/venv, `systemd-sysusers`,
# `systemd-tmpfiles`, the unit (plus the drop-in below systemd 253), a file-backed pool (/var/tmp/zbinst.img, pool zbinst). The daemon
# is then driven as a real systemd unit, and the host is BOTH the backup client and the backup server (the destination is
# https://127.0.0.1:18443 and the host enrolls with itself), so one VM exercises the whole path including a real receive.
#
# Checks (per VM; every one is a numbered assertion in the report):
#   I1  provisioning: the pool, the interpreter, the systemd version
#   I2  install: both console scripts exist, `--version` of BOTH prints `<name> <version> (wire protocol N, config schema <head>)`,
#       the group zfsbackup exists, /var/lib/zfsbackup is 2770 root:zfsbackup and backups/ is 0700 root:root (tmpfiles), the unit is
#       loaded; below 253 the unit is Type=simple (notify-reload unknown) until the drop-in makes it Type=notify with an ExecReload=
#   I3  a start BEFORE `import` fails cleanly: the assertion (AssertPathExists=config.db) is reported, the unit stays inactive and
#       there is no restart loop (NRestarts=0, still inactive well past a second)
#   I4  `import`, then `zfsbackupd --test-config`; the database is 0660 group zfsbackup; its schema is the head `--version` names
#   I5  `check` with no daemon: exit 1, `--nagios` exit 2 (CRIT), the check unit/timer shipped in the sdist: the unit SUCCEEDS
#       though the verdict is unhealthy (SuccessExitStatus=1) and writes the .prom file; an unusable database (exit 78 / nagios 3
#       UNKNOWN) makes the unit FAIL ("the check could not run")
#   I6  `systemctl enable --now` reaches READY: `start` returns with the unit active, StatusText "generation N", running as root
#   I7  `systemctl reload`: one SIGHUP decision logged ("configuration unchanged"), StatusText unchanged, the same supervisor;
#       systemd 255: no ExecReload, the journal shows the reload, `reload` returns after READY; systemd 249: ExecReload= sends it
#   I8  `check` with a running daemon that is not yet enrolled: exit 1, overall WARN (never CRIT), `--nagios` exit 1, the .prom
#       file from the timer's unit says `zfsbackup_check_overall_state 1`
#   I9  the journal: a forced ERROR (an import that names a dataset that does not exist) is in `journalctl -p err` with PRIORITY 3,
#       the info lines ("Started ... worker") are PRIORITY 6 and are NOT in `-p err`
#   I10 enroll with itself (`trust token create`, `enroll --token-file`), then a REAL receive: the server copy exists with the same
#       guid, is mounted on the HOST (the daemon has no private mount namespace: the unit's mount namespace is init's), and the
#       data is the same
#   I11 `check`: exit 0, overall OK, JSON `schema` 1, `--prometheus --output FILE` (atomic, 0644, the metric names), `--nagios` exit 0;
#       the timer enables and its unit writes `zfsbackup_check_success 1`
#   I12 `backup`: as root (0700 directory, MANIFEST last, config.db 0600, the TLS private keys included, `--json`); as a group
#       operator: refused (exit 1) with the `--no-keys` pointer and nothing left behind, then `--no-keys` works (certificates, no keys)
#   I13 a clean `systemctl stop` well inside TimeoutStopSec=90 (Result=success, no process left, the pid file removed)
#   I14 the shipped restart policy: RestartForceExitStatus names 76 and SIGHUP, and a main process that dies of SIGHUP IS restarted
#   I15 UPGRADE: the wheel built from OLD_REF (default v1.0.0rc1, the last single-wheel release: dist `zfsbackup` holding both packages;
#       0cd236c is the milestone A tree: dist `libzfseasy`, schema 7c3e9d51b2a8, no `zfsbackupd`) is installed in the venv, its DB
#       imported and its daemon run; then the upgrade to the split wheels, which a plain `pip install` cannot do (see below), by the
#       runbook's procedure: stop the daemon, `pip uninstall -y <old dist>`, `pip install libzfseasy-*.whl zfsbackup-*.whl`,
#       `systemctl start zfsbackup`. First a throwaway venv shows why: the plain upgrade of an old `zfsbackup` single wheel to the
#       split wheels leaves `import libzfseasy` broken (pip installs the new libzfseasy, then uninstalls the old zfsbackup, whose RECORD
#       still lists the libzfseasy/ files). When the schema is behind (OLD_REF=0cd236c) the ExecStartPre migration writes ONE
#       pre-migration backup (`config.db.pre-<old rev>-<UTC>`, 0600) in /var/lib/zfsbackup/backups (0700 root); the DB is at the new
#       head, the unit reaches READY, the client id survived; a plain RESTART writes no second backup (with rc1 the schema is already at
#       head: no migration, no backup is expected, and the scenario says so)
#   I16 IN-PLACE UPGRADE UNDER A RUNNING DAEMON (the S-8 version-skew guard): the current tree is version N, a copy of it rebuilt as
#       N+1 (".post1" appended to BOTH versions and to the `libzfseasy==` pin in a temp copy). With N running as the real unit: `pip install` N+1 over it -> the
#       journal has the drain WARNING ("changed on disk ... will exit 76", priority 4, "N -> N+1"), the main process exits 76,
#       systemd restarts it EXACTLY once and only after RestartSec, `zfsbackupd --version` is N+1, the second start is clean (READY,
#       no restart loop, `status --wait` exit 0). Then a SAME-VERSION reinstall (`--force-reinstall`) is detected too ("was
#       reinstalled"), and so is a reinstall of the `libzfseasy` wheel ALONE ("libzfseasy ... was reinstalled": the guard watches both
#       distributions). Then with a long transfer in flight (the loopback throttled to 4 Mbit/s with `tc`, 300 MB of fresh data; not
#       SIGSTOP: any signal aborts a running zfs send/receive), a reinstall starts the drain but the daemon does NOT exit while the
#       transfer runs (draining= in the pid file, STATUS= says so, the API still answers /health); once the throttle is removed
#       the transfer completes (the snapshot is on the server) and only then does it exit 76 and restart. Finally a `systemctl stop`
#       during such a drain exits 0 (Result=success, status 0, never 76) and no restart follows. For the test, RestartSec=60 of the
#       shipped unit is shortened to 15 s by a drop-in (the shipped value is asserted first). NOT covered here: "a start in the
#       middle of an install exits 76" (needs a half-written install at an exact moment; the pytest suite covers it)
#
# The README's wording is followed as closely as a script can; where the README is wrong the scenario says so as a FINDING (listed
# at the end, not counted as a failure).
#
# Usage (from the repo root; needs multipass and poetry on the host, no sudo):
#   ./scenarios/test_install.sh                    # both VMs, everything (~25 min)
#   ./scenarios/test_install.sh --only-24          # one VM (--only-22 likewise)
#   ./scenarios/test_install.sh --no-upgrade       # skip I15 (and the OLD_REF wheel build)
#   ./scenarios/test_install.sh --no-inplace       # skip I16 (and the N+1 wheel builds)
#   ./scenarios/test_install.sh --keep             # leave the install, the pool and the daemon in place
#   ./scenarios/test_install.sh --purge            # delete zfsb-inst22 and zfsb-inst24 (and ONLY those) at the end
#   SRC_REF=<commit-ish> ./scenarios/test_install.sh   # build this tree (default HEAD); SRC_REF=WORKTREE builds the working tree as it
#                                                      # is (uncommitted and untracked files included; nothing is committed or stashed)
#   OLD_REF=<commit-ish>                           # the tree I15 upgrades FROM (default v1.0.0rc1; 0cd236c also exercises the migration)
#
# Multipass caveats are the usual ones (see scenarios/README.md): never send a `multipass exec`'s host stdout to /dev/null, deliver
# files with `multipass transfer`, and a guest command that exits 4 hangs the exec (every command runs in an inner shell that maps
# 4 to 104).

set -uo pipefail

# ─────────────────────────────── Configuration ──────────────────────────────
VM22="${INST22_VM:-zfsb-inst22}"
VM24="${INST24_VM:-zfsb-inst24}"
for _vm in "$VM22" "$VM24"; do
    case "$_vm" in zfsb-inst*) ;; *) echo "refusing VM name '$_vm': this scenario only touches zfsb-inst* VMs" >&2; exit 2 ;; esac
done

SRC_REF="${SRC_REF:-HEAD}"
OLD_REF="${OLD_REF:-v1.0.0rc1}"
OLD_REV=""                                  # the schema revision of OLD_REF: read from its database after its own `import`
[ "$OLD_REF" = 0cd236c ] && OLD_REV_EXPECT="7c3e9d51b2a8" || OLD_REV_EXPECT=""     # (the milestone A tree's revision is asserted)

POOL="zbinst"; IMG="/var/tmp/zbinst.img"; POOL_SIZE="4g"
API_PORT="18443"
APP="/opt/zfsbackup"; VENV="$APP/venv"      # the paths the shipped unit expects
CFGDIR="/var/lib/zfsbackup"; DB="$CFGDIR/config.db"; BAKDIR="$CFGDIR/backups"
PROM="/var/lib/node_exporter/textfile_collector/zfsbackup.prom"      # the unit's default output
G="/root/zbinst"                            # guest scratch: wheels, the sdist, config files, helpers
UNIT="zfsbackup"
OPUSER="zbop"                               # the group operator

VM_CPUS="${VM_CPUS:-2}"; VM_MEM="${VM_MEM:-2G}"; VM_DISK="${VM_DISK:-12G}"

DO22=1; DO24=1; UPGRADE=1; INPLACE=1; KEEP=0; PURGE=0
for arg in "$@"; do
    case "$arg" in
        --only-22) DO24=0 ;;
        --only-24) DO22=0 ;;
        --no-upgrade) UPGRADE=0 ;;
        --no-inplace) INPLACE=0 ;;
        --keep) KEEP=1 ;;
        --purge) PURGE=1 ;;
        -h|--help) grep '^#' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *) echo "Unknown option: $arg" >&2; exit 2 ;;
    esac
done
[ "$DO22" -eq 1 ] || [ "$DO24" -eq 1 ] || { echo "--only-22 and --only-24 exclude each other" >&2; exit 2; }

# ─────────────────────────────── Output helpers ─────────────────────────────
if [ -t 1 ]; then
    RED=$'\033[0;31m'; GREEN=$'\033[0;32m'; YELLOW=$'\033[0;33m'; BLUE=$'\033[0;34m'; BOLD=$'\033[1m'; NC=$'\033[0m'
else
    RED=; GREEN=; YELLOW=; BLUE=; BOLD=; NC=
fi
PASS_COUNT=0; FAIL_COUNT=0
declare -a RESULTS=() FINDINGS=() TIMINGS=()
TAG=""                                       # "inst22" / "inst24": prefixes every assertion

info()  { echo "${BLUE}==>${NC} $*"; }
step()  { echo "${BOLD}${BLUE}::${NC} ${BOLD}$*${NC}"; }
warn()  { echo "${YELLOW}WARN:${NC} $*" >&2; }
die()   { echo "${RED}FATAL:${NC} $*" >&2; exit 1; }
pass()  { PASS_COUNT=$((PASS_COUNT+1)); RESULTS+=("${GREEN}PASS${NC}  [$TAG] $1"); echo "  ${GREEN}✓ PASS${NC} $1"; }
fail()  { FAIL_COUNT=$((FAIL_COUNT+1)); RESULTS+=("${RED}FAIL${NC}  [$TAG] $1"); echo "  ${RED}✗ FAIL${NC} $1"; }
finding() { FINDINGS+=("[$TAG] $1"); echo "  ${YELLOW}! FINDING${NC} $1"; }   # a documentation / packaging defect, not a failed assertion
check() { # check "<description>" <expected> <actual>
    if [ "$2" = "$3" ]; then pass "$1 ($3)"; else fail "$1 (expected='$2' actual='$3')"; fi
}
check_re() { # check_re "<description>" "<ERE>" "<text>"
    if grep -qE -- "$2" <<<"$3"; then pass "$1"; else
        fail "$1 (no match for /$2/ in: $(printf '%s' "$3" | tr '\n' '|' | cut -c1-400))"
    fi
}
check_no_re() { # check_no_re "<description>" "<ERE>" "<text>"
    if grep -qE -- "$2" <<<"$3"; then
        fail "$1 (unexpected /$2/ in: $(printf '%s' "$3" | tr '\n' '|' | cut -c1-400))"
    else pass "$1"; fi
}
section() { SECTION_T0=$SECONDS; SECTION_NAME="$1"; echo; step "$1"; }
section_end() { TIMINGS+=("[$TAG] ${SECTION_NAME%% *}: $((SECONDS - SECTION_T0))s"); }

# ─────────────────────────────── Multipass helpers ──────────────────────────
# `multipass exec` busy-spins FOREVER when the guest command writes to stdout and the host sends that stdout to /dev/null:
# never do that; discard in the guest. A guest command that exits 4 hangs it too, so the command runs in an inner shell and
# status 4 becomes 104.
REMAP='bash -c "$1"; rc=$?; [ "$rc" -eq 4 ] && rc=104; exit "$rc"'
VM=""                                        # the VM under test
vsh()  { multipass exec "$VM" -- sudo bash -c "$REMAP" _ "$*"; }                          # as root
vasu() { local u="$1"; shift; multipass exec "$VM" -- sudo -u "$u" -H bash -c "$REMAP" _ "$*"; }   # as <user>, real initgroups

vm_exists()  { multipass info "$1" >/dev/null 2>&1; }
vm_running() { multipass info "$1" 2>/dev/null | awk -F': *' '/^State/{print $2}' | grep -qi running; }

ensure_vm() { # ensure_vm <name> <ubuntu release>
    if ! vm_exists "$1"; then
        step "Launching VM $1 (Ubuntu $2)"
        multipass launch "$2" --name "$1" --cpus "$VM_CPUS" --memory "$VM_MEM" --disk "$VM_DISK" || die "failed to launch $1"
    elif ! vm_running "$1"; then
        step "Starting existing VM $1"
        multipass start "$1" || die "failed to start $1"
    else
        info "VM $1 already running"
    fi
}

# put_file <host-file> <guest-path> <mode>  -- via `multipass transfer`, never piped stdin
put_file() {
    local base="/tmp/zbinst-xfer.$$.$RANDOM"
    multipass transfer "$1" "$VM:$base" || die "transfer of $1 failed"
    vsh "install -D -m '$3' '$base' '$2' && rm -f '$base'" || die "install of $2 failed"
}
put_text() { # put_text <guest-path> <mode>   (content on stdin)
    local tmp; tmp="$(mktemp "$WORK/xfer.XXXXXX")" || die "mktemp failed"
    cat > "$tmp"
    put_file "$tmp" "$1" "$2"
    rm -f "$tmp"
}

# wait_guest <timeout> <guest command>: poll until the command succeeds
wait_guest() {
    local timeout="$1" cmd="$2" waited=0
    until vsh "$cmd"; do
        [ "$waited" -ge "$timeout" ] && return 1
        sleep 2; waited=$((waited+2))
    done
}

# ─────────────────────────────── Host-side build ────────────────────────────
command -v multipass >/dev/null 2>&1 || die "multipass not found on the host"
command -v poetry    >/dev/null 2>&1 || die "poetry not found on the host (it builds the wheel and the sdist)"
command -v git       >/dev/null 2>&1 || die "git not found on the host"
TOP="$(git -C "$(dirname "$0")" rev-parse --show-toplevel)" || die "not in a git repo"
WORK="$(mktemp -d "${TMPDIR:-/tmp}/zbinst-work.XXXXXX")" || die "cannot make a scratch directory"
trap 'rm -rf "$WORK"' EXIT

# build_dist <ref|WORKTREE> <outdir>: the wheel + sdist of that tree built at its root (the zfsbackup distribution) and, for a tree
# from after the packaging split (libs/libzfseasy/pyproject.toml exists), the libzfseasy wheel built in libs/libzfseasy. A tree from
# before the split yields its single wheel only.
build_dist() {
    local ref="$1" out="$2" src
    mkdir -p "$out"
    if [ "$ref" = WORKTREE ]; then
        src="$TOP"
    else
        src="$WORK/src-$(basename "$out")"; mkdir -p "$src"
        git -C "$TOP" archive "$ref" | tar -x -C "$src" || die "git archive of '$ref' failed"
    fi
    (cd "$src" && poetry build -o "$out") > "$out.log" 2>&1 || { sed 's/^/    poetry: /' "$out.log" >&2; die "poetry build of '$ref' failed"; }
    if [ -f "$src/libs/libzfseasy/pyproject.toml" ]; then
        (cd "$src/libs/libzfseasy" && poetry build -f wheel -o "$out") > "$out.lib.log" 2>&1 \
            || { sed 's/^/    poetry: /' "$out.lib.log" >&2; die "poetry build of libs/libzfseasy of '$ref' failed"; }
    fi
}

step "Building the current tree on the host (SRC_REF=$SRC_REF)"
build_dist "$SRC_REF" "$WORK/new"
NEW_WHL="$(ls "$WORK"/new/zfsbackup-*.whl 2>/dev/null | head -n1)"; NEW_SDIST="$(ls "$WORK"/new/zfsbackup-*.tar.gz 2>/dev/null | head -n1)"
NEW_LIB_WHL="$(ls "$WORK"/new/libzfseasy-*.whl 2>/dev/null | head -n1)"      # empty for a tree from before the packaging split (one wheel)
[ -n "$NEW_WHL" ] && [ -n "$NEW_SDIST" ] || die "poetry build did not produce zfsbackup-*.whl and .tar.gz (see $WORK/new.log)"
tar tzf "$NEW_SDIST" | grep -q 'packaging/systemd/zfsbackup.service$' \
    || die "the sdist of '$SRC_REF' has no packaging/systemd/zfsbackup.service: this tree predates milestone C (use SRC_REF=WORKTREE)"
if [ -n "$NEW_LIB_WHL" ]; then info "wheels $(basename "$NEW_LIB_WHL") + $(basename "$NEW_WHL"), sdist $(basename "$NEW_SDIST")"
else info "ONE wheel $(basename "$NEW_WHL") (a tree from before the packaging split), sdist $(basename "$NEW_SDIST")"; fi
OLD_WHL=""; OLD_DIST=""
if [ "$UPGRADE" -eq 1 ]; then
    step "Building the OLD tree on the host (OLD_REF=$OLD_REF)"
    build_dist "$OLD_REF" "$WORK/old"
    OLD_WHL="$(ls "$WORK"/old/*.whl 2>/dev/null | head -n1)"
    [ -n "$OLD_WHL" ] || die "poetry build of '$OLD_REF' produced no wheel (see $WORK/old.log)"
    [ "$(ls "$WORK"/old/*.whl | wc -l | tr -d '[:space:]')" = 1 ] || die "OLD_REF '$OLD_REF' built more than one wheel: I15 upgrades FROM a single-wheel tree"
    OLD_DIST="$(basename "$OLD_WHL" | cut -d- -f1)"        # the distribution name the old wheel installs as
    info "old wheel $(basename "$OLD_WHL") (distribution $OLD_DIST)"
fi
BUMP_WHL=""; BUMP_LIB_WHL=""; BUMP_VER=""; CUR_VER=""
if [ "$INPLACE" -eq 1 ]; then
    step "Building version N+1 of the current tree on the host (a temp copy with \".post1\" appended to the version)"
    BSRC="$WORK/src-bump"; mkdir -p "$BSRC"
    if [ "$SRC_REF" = WORKTREE ]; then
        # the working tree as it is, minus .git and ignored files; a tracked file deleted in the tree only makes rsync say so
        (cd "$TOP" && git ls-files -z -co --exclude-standard | rsync -a --from0 --files-from=- ./ "$BSRC/") > "$WORK/bump-copy.log" 2>&1
        [ -f "$BSRC/pyproject.toml" ] || die "could not copy the working tree for the N+1 build (see $WORK/bump-copy.log)"
    else
        git -C "$TOP" archive "$SRC_REF" | tar -x -C "$BSRC" || die "git archive of '$SRC_REF' failed"
    fi
    CUR_VER="$(sed -n 's/^version = "\(.*\)"$/\1/p' "$BSRC/pyproject.toml" | head -n1)"
    [ -n "$CUR_VER" ] || die "no version = \"...\" line in pyproject.toml"
    BUMP_VER="$CUR_VER.post1"
    # bump_version <pyproject.toml>: the first `version = "..."` line becomes N+1
    bump_version() {
        awk -v nv="$BUMP_VER" '!done && /^version = "/ { print "version = \"" nv "\""; done=1; next } { print }' "$1" > "$1.new" && mv "$1.new" "$1"
    }
    bump_version "$BSRC/pyproject.toml"
    if [ -n "$NEW_LIB_WHL" ]; then
        # the split layout: libzfseasy's own pyproject follows (lockstep), and the root one pins it with `libzfseasy==<version>`
        [ -f "$BSRC/libs/libzfseasy/pyproject.toml" ] || die "split wheels were built but $BSRC/libs/libzfseasy/pyproject.toml is missing"
        bump_version "$BSRC/libs/libzfseasy/pyproject.toml"
        sed "s/\"libzfseasy==$CUR_VER\"/\"libzfseasy==$BUMP_VER\"/" "$BSRC/pyproject.toml" > "$BSRC/pyproject.toml.new" && mv "$BSRC/pyproject.toml.new" "$BSRC/pyproject.toml"
        grep -q "\"libzfseasy==$BUMP_VER\"" "$BSRC/pyproject.toml" || die "could not bump the libzfseasy== pin in the N+1 copy"
    fi
    (cd "$BSRC" && poetry build -f wheel -o "$WORK/bump") > "$WORK/bump.log" 2>&1 || { sed 's/^/    poetry: /' "$WORK/bump.log" >&2; die "poetry build of N+1 failed"; }
    if [ -n "$NEW_LIB_WHL" ]; then
        (cd "$BSRC/libs/libzfseasy" && poetry build -f wheel -o "$WORK/bump") > "$WORK/bump.lib.log" 2>&1 \
            || { sed 's/^/    poetry: /' "$WORK/bump.lib.log" >&2; die "poetry build of libzfseasy N+1 failed"; }
        BUMP_LIB_WHL="$(ls "$WORK"/bump/libzfseasy-*.whl 2>/dev/null | head -n1)"
        [ -n "$BUMP_LIB_WHL" ] || die "poetry build produced no libzfseasy N+1 wheel (see $WORK/bump.lib.log)"
    fi
    BUMP_WHL="$(ls "$WORK"/bump/zfsbackup-*.whl 2>/dev/null | head -n1)"
    [ -n "$BUMP_WHL" ] || die "poetry build produced no N+1 wheel (see $WORK/bump.log)"
    info "N = $CUR_VER ($(basename "$NEW_WHL")), N+1 = $BUMP_VER ($(basename "$BUMP_WHL")${BUMP_LIB_WHL:+ + $(basename "$BUMP_LIB_WHL")})"
fi
NEW_WHL_BASE="$(basename "$NEW_WHL")"; NEW_SDIST_BASE="$(basename "$NEW_SDIST")"
NEW_LIB_BASE=""; [ -n "$NEW_LIB_WHL" ] && NEW_LIB_BASE="$(basename "$NEW_LIB_WHL")"
BUMP_BASE=""; [ -n "$BUMP_WHL" ] && BUMP_BASE="$(basename "$BUMP_WHL")"
BUMP_LIB_BASE=""; [ -n "$BUMP_LIB_WHL" ] && BUMP_LIB_BASE="$(basename "$BUMP_LIB_WHL")"
# the wheels as the guest sees them (space-separated; a lone zfsbackup wheel for a pre-split tree). Filled by put_wheels.
NEW_GUEST=""; BUMP_GUEST=""; BUMP_LIB_GUEST=""
SDIST_DIR="${NEW_SDIST_BASE%.tar.gz}"

# ─────────────────────────────── Guest helper files ─────────────────────────
put_helpers() {
    put_text "$G/jget.py" 644 <<'PYEOF'
import json, sys
d = json.load(sys.stdin)
v = eval(sys.argv[1], {"d": d})
print("" if v is None else v)
PYEOF
    put_text "$G/dbrev.py" 644 <<'PYEOF'
import sqlite3, sys
c = sqlite3.connect("file:" + sys.argv[1] + "?mode=ro", uri=True)
print(c.execute("select version_num from alembic_version").fetchone()[0])
PYEOF
    # which distribution owns the libzfseasy/ files: the venv must hold two distributions after the packaging split
    put_text "$G/distinfo.py" 644 <<'PYEOF'
import importlib.metadata as m
def n(dist):
    return sum(1 for f in (m.files(dist) or ()) if str(f).startswith("libzfseasy/"))
def v(dist):
    try:
        return m.version(dist)
    except m.PackageNotFoundError:
        return "-"
print("libzfseasy=%s zfsbackup=%s same=%d lib_in_lib=%d lib_in_zb=%d" % (v("libzfseasy"), v("zfsbackup"), v("libzfseasy") == v("zfsbackup") != "-",
      n("libzfseasy") if v("libzfseasy") != "-" else 0, n("zfsbackup") if v("zfsbackup") != "-" else 0))
PYEOF
    # journald records as "PRIORITY<TAB>message" for the lines matching argv[1] (an ERE)
    put_text "$G/jprio.py" 644 <<'PYEOF'
import json, re, sys
pat = re.compile(sys.argv[1])
for line in sys.stdin:
    try:
        r = json.loads(line)
    except ValueError:
        continue
    m = r.get("MESSAGE")
    if isinstance(m, list):
        m = bytes(m).decode("utf-8", "replace")
    if m and pat.search(m):
        print("%s\t%s" % (r.get("PRIORITY"), m))
PYEOF
    # seconds between the unit's "Main process exited ... status=76" and its "Scheduled restart job" (journalctl -o short-unix)
    put_text "$G/restartgap.awk" 644 <<'AWKEOF'
/Main process exited, code=exited, status=76/ { a = $1 }
/Scheduled restart job/ { b = $1 }
END { if (a != "" && b != "") printf "%d\n", b - a; else print "none" }
AWKEOF
    # waitrx.sh <timeout>: wait for the daemon's stream `zfs receive` (not a short-lived `zfs receive -A`) to have read >= 1 MB, print
    # its pid. (Not SIGSTOP: any signal makes a running zfs send/receive abort -- "cannot send ...: signal received" -- so a
    # frozen transfer is not a thing; the loopback is throttled with tc instead.)
    put_text "$G/waitrx.sh" 755 <<'SHEOF'
#!/bin/bash
end=$((SECONDS + $1))
while [ "$SECONDS" -lt "$end" ]; do
    for pid in $(pgrep -f '^(/[^ ]*/)?zfs receive' | tr '\n' ' '); do
        args="$(ps -o args= -p "$pid" 2>/dev/null)"
        case "$args" in *" -A "*|*" -A") continue ;; esac
        rchar="$(awk '/^rchar:/ {print $2}' "/proc/$pid/io" 2>/dev/null)"
        if [ "${rchar:-0}" -ge 1000000 ]; then echo "$pid"; exit 0; fi
    done
    sleep 0.2
done
exit 1
SHEOF
}

# The client/server configuration. __PIN__ is the destination's server_fingerprint line once enrolled (an `import` is a full
# replace: without the pin it would un-enroll); __EXTRA__ is an optional additional dataset item.
config_yaml() { # config_yaml <pin fingerprint or ""> <extra dataset name or "">
    local y pin="" extra=""
    y="$(cat <<'YEOF'
# zfsbackup config written by test_install.sh: this host is its own backup client AND its own backup server
snapshot_prefix: "autosnap"
check_interval: "15s"
prune_interval: "1h"
api_host: "127.0.0.1"
api_port: __PORT__
remote_backup:
  enabled: true
  target_dataset: "__POOL__/backups"
destinations:
  self:
    url: "https://127.0.0.1:__PORT__"
__PIN__
datasets:
  - name: "__POOL__/data"
    enabled: true
    frequency: "10m"
    recursive: false
    retention:
      "1s": "1d"
    remote:
      - destination: self
        frequency: "30s"
__EXTRA__
YEOF
)"
    [ -n "$1" ] && pin="    server_fingerprint: \"$1\""
    if [ -n "$2" ]; then
        extra="  - name: \"$2\"
    enabled: true
    frequency: \"10m\"
    recursive: false
    retention:
      \"1s\": \"1d\""
    fi
    y="${y//__PORT__/$API_PORT}"; y="${y//__POOL__/$POOL}"; y="${y//__PIN__/$pin}"; y="${y//__EXTRA__/$extra}"
    printf '%s\n' "$y"
}
# The configuration the OLD (milestone A) tree imports: local snapshots only, nothing remote.
old_config_yaml() {
    cat <<'YEOF'
snapshot_prefix: "autosnap"
check_interval: "15s"
prune_interval: "1h"
datasets:
  - name: "__POOL__/data"
    enabled: true
    frequency: "10m"
    recursive: false
    retention:
      "1s": "1d"
YEOF
}

# ─────────────────────────────── Guest state helpers ────────────────────────
sd_prop() { vsh "systemctl show -p $1 --value $UNIT" | tr -d '\r'; }       # sd_prop <Property>
unit_active() { [ "$(sd_prop ActiveState)" = active ]; }
jl()      { vsh "journalctl -u $UNIT -o cat --no-pager $*"; }              # the unit's messages, e.g. jl --since @123
jl_epoch() { vsh 'date +%s' | tr -d '[:space:]'; }
guest_stat() { vsh "stat -c '%a %U %G' '$1' 2>/dev/null" | tr -d '\r'; }  # "<octal> <owner> <group>"
exists() { vsh "test -e '$1'"; }
cli() { vsh "zfsbackup-config $* 2>&1"; }                                  # root CLI on the default DB, stderr merged
dbrev() { vsh "python3 $G/dbrev.py '$1'" | tr -d '[:space:]'; }
head_rev() { vsh "$VENV/bin/zfsbackup-config --version" | sed -n 's/.*config schema \([0-9a-f]*\)).*/\1/p' | head -n1; }

# check helpers: results of `zfsbackup-config check` land in guest files so exit codes and bodies stay together
CK_RC=0
run_check() { vsh "zfsbackup-config check $* >$G/ck.out 2>$G/ck.err"; CK_RC=$?; }
ck_out()   { vsh "cat $G/ck.out"; }
ck_state() { vsh "python3 $G/jget.py 'd[\"state\"]' < $G/ck.out" | tr -d '[:space:]'; }
ck_entries() { vsh "python3 $G/jget.py '\"; \".join(\"%s:%s=%s\" % (e[\"source\"], e[\"subject\"], e[\"state\"]) for e in d[\"entries\"])' < $G/ck.out"; }

# ═════════════════════════════════ Per-VM flow ══════════════════════════════
provision() {
    section "I1 — provision $VM (ZFS, a file-backed pool, an interpreter)"
    vsh '
        set -e
        export DEBIAN_FRONTEND=noninteractive
        need=""
        command -v zfs  >/dev/null 2>&1 || need="$need zfsutils-linux"
        command -v curl >/dev/null 2>&1 || need="$need curl"
        command -v tc   >/dev/null 2>&1 || need="$need iproute2"
        dpkg -s python3-venv >/dev/null 2>&1 || need="$need python3-venv"
        if [ -n "$need" ]; then apt-get update -qq && apt-get install -y -qq $need; fi
        modprobe zfs 2>/dev/null || true
    ' || die "package install failed on $VM"
    SDV="$(vsh "systemctl --version | head -n1 | awk '{print \$2}'" | tr -d '[:space:]')"
    OSREL="$(vsh '. /etc/os-release; echo $VERSION_ID' | tr -d '[:space:]')"
    info "$VM: Ubuntu $OSREL, systemd $SDV, $(vsh 'zfs version 2>&1 | head -n1' | tr -d '\r')"
    local pyok; pyok="$(vsh "python3 -c 'import sys; print(int(sys.version_info >= (3, 11)))'" | tr -d '[:space:]')"
    if [ "$pyok" = 1 ]; then PYMODE=distro; else PYMODE=uv; fi
    info "python3 is $(vsh 'python3 --version' | tr -d '\r'): venv mode '$PYMODE'"
    # a fresh pool every run (the install state is wiped by reset_install, below)
    vsh "
        set -e
        { zpool destroy -f '$POOL'; } >/dev/null 2>&1 || true
        rm -f '$IMG'; truncate -s '$POOL_SIZE' '$IMG'
        zpool create '$POOL' '$IMG'
        zfs create '$POOL/data'
        zfs create '$POOL/backups'
        head -c 1500000 /dev/urandom > /$POOL/data/file1.bin
    " || die "pool creation failed on $VM"
    check "the pool $POOL is ONLINE" "ONLINE" "$(vsh "zpool list -H -o health '$POOL'" | tr -d '[:space:]')"
    if [ "$PYMODE" = uv ]; then
        # README: Ubuntu 22.04's python3 is 3.10. uv is installed into a throwaway venv, and manages the interpreter under /opt/zfsbackup/python
        vsh "
            set -e
            test -x /var/tmp/zbinst-uvtool/bin/uv || { python3 -m venv /var/tmp/zbinst-uvtool && /var/tmp/zbinst-uvtool/bin/pip install -q uv; }
            mkdir -p $APP/python
            UV_PYTHON_INSTALL_DIR=$APP/python /var/tmp/zbinst-uvtool/bin/uv python install 3.12
        " || die "could not install a uv-managed Python 3.12 on $VM"
    fi
    section_end
}

# Everything the install and the daemon leave behind, so each run is a fresh-install test. The uv interpreter ($APP/python) stays.
reset_install() {
    vsh '
        { systemctl stop zfsbackup zfsbackup-check.timer zfsbackup-check.service zbinst-old; systemctl reset-failed; } >/dev/null 2>&1
        pkill -9 -f "[z]fsbackup\.daemon" >/dev/null 2>&1
        pkill -9 -f "[z]fsbackupd" >/dev/null 2>&1
        rm -rf /etc/systemd/system/zfsbackup.service /etc/systemd/system/zfsbackup.service.d \
               /etc/systemd/system/zfsbackup-check.service /etc/systemd/system/zfsbackup-check.timer \
               /etc/systemd/system/timers.target.wants/zfsbackup-check.timer /etc/systemd/system/multi-user.target.wants/zfsbackup.service \
               /etc/sysusers.d/zfsbackup.conf /etc/tmpfiles.d/zfsbackup.conf \
               /opt/zfsbackup/venv /usr/local/bin/zfsbackup-config /usr/local/bin/zfsbackupd /var/lib/zfsbackup /var/lib/node_exporter \
               /srv/zbinst-backups /var/log/zbinst-old.log /root/zbinst
        rmdir /etc/sysusers.d 2>/dev/null     # stock Ubuntu has none; put it back to stock so the README step is tested as written
        id -u zbop >/dev/null 2>&1 && userdel -r zbop >/dev/null 2>&1
        getent group zfsbackup >/dev/null 2>&1 && groupdel zfsbackup >/dev/null 2>&1
        systemctl daemon-reload
        true
    ' || true
}

# put_wheels: ship the current tree's wheels (libzfseasy first when the tree has the split) and the N+1 wheels (if built) to $G;
# NEW_GUEST / BUMP_GUEST / BUMP_LIB_GUEST name them as the guest sees them (space-separated lists of paths, no quoting needed)
put_wheels() {
    NEW_GUEST=""; BUMP_GUEST=""; BUMP_LIB_GUEST=""
    if [ -n "$NEW_LIB_WHL" ]; then put_file "$NEW_LIB_WHL" "$G/$NEW_LIB_BASE" 644; NEW_GUEST="$G/$NEW_LIB_BASE "; fi
    put_file "$NEW_WHL" "$G/$NEW_WHL_BASE" 644; NEW_GUEST+="$G/$NEW_WHL_BASE"
    if [ -n "$BUMP_WHL" ]; then
        if [ -n "$BUMP_LIB_WHL" ]; then put_file "$BUMP_LIB_WHL" "$G/$BUMP_LIB_BASE" 644; BUMP_LIB_GUEST="$G/$BUMP_LIB_BASE"; BUMP_GUEST="$BUMP_LIB_GUEST "; fi
        put_file "$BUMP_WHL" "$G/$BUMP_BASE" 644; BUMP_GUEST+="$G/$BUMP_BASE"
    fi
}

# install_venv <wheel path on the guest>...: step 1 of the README (the venv at /opt/zfsbackup/venv, the program, the /usr/local/bin links).
# One argument per wheel: `libzfseasy-N.whl zfsbackup-N.whl` for the split layout (the second pins the first), one for a pre-split tree.
install_venv() {
    local w quoted=""
    for w in "$@"; do quoted="$quoted '$w'"; done
    if [ "$PYMODE" = uv ]; then
        vsh "
            set -e
            UV_PYTHON_INSTALL_DIR=$APP/python /var/tmp/zbinst-uvtool/bin/uv venv --python 3.12 $VENV
            /var/tmp/zbinst-uvtool/bin/uv pip install --python $VENV/bin/python $quoted
        " || die "uv venv / install of $* failed on $VM"
    else
        vsh "
            set -e
            python3 -m venv $VENV
            $VENV/bin/pip install -q $quoted
        " || die "venv / pip install of $* failed on $VM"
    fi
    vsh "ln -s $VENV/bin/zfsbackup-config /usr/local/bin/zfsbackup-config && ln -s $VENV/bin/zfsbackupd /usr/local/bin/zfsbackupd" || die "ln -s failed"
}

install_files() {
    section "I2 — install the program, the units, sysusers and tmpfiles (the README's steps)"
    reset_install
    put_helpers
    put_wheels
    put_file "$NEW_SDIST" "$G/$NEW_SDIST_BASE" 644
    install_venv $NEW_GUEST
    if [ -n "$NEW_LIB_WHL" ]; then
        local di; di="$(vsh "$VENV/bin/python $G/distinfo.py" | tr '\n' ' ')"
        check_re "the venv holds BOTH distributions, libzfseasy and zfsbackup, at the same version" 'libzfseasy=[^ ]+ zfsbackup=[^ ]+ same=1 ' "$di"
        check_re "libzfseasy's files are in the libzfseasy distribution, none in the zfsbackup one" 'lib_in_lib=[1-9][0-9]* lib_in_zb=0 ' "$di"
    fi
    # step 2 of the README: the system files, from the sdist (verbatim: `install -D`, stock Ubuntu has no /etc/sysusers.d)
    vsh "cd $G && tar xzf '$NEW_SDIST_BASE'" || die "cannot extract the sdist on $VM"
    vsh "
        set -e
        cd $G/$SDIST_DIR
        install -D -m 0644 packaging/sysusers.d/zfsbackup.conf /etc/sysusers.d/zfsbackup.conf
        install -D -m 0644 packaging/tmpfiles.d/zfsbackup.conf /etc/tmpfiles.d/zfsbackup.conf
        install -D -m 0644 packaging/systemd/zfsbackup.service /etc/systemd/system/zfsbackup.service
        systemd-sysusers
        systemd-tmpfiles --create zfsbackup.conf
        systemctl daemon-reload
    " || die "installing the system files failed on $VM"

    check "the venv has both console scripts" "yes yes" \
        "$(vsh "test -x $VENV/bin/zfsbackupd && echo yes || echo no") $(vsh "test -x $VENV/bin/zfsbackup-config && echo yes || echo no")"
    local vd vc
    check "both commands are on PATH, as the README uses them bare" "yes yes" \
        "$(vsh "command -v zfsbackupd >/dev/null && echo yes || echo no") $(vsh "command -v zfsbackup-config >/dev/null && echo yes || echo no")"
    vd="$(vsh "$VENV/bin/zfsbackupd --version 2>&1" | head -n1)"
    vc="$(vsh "zfsbackup-config --version 2>&1" | head -n1)"
    check_re "zfsbackupd --version names the wire protocol and the config schema" '^zfsbackupd [0-9][^ ]* \(wire protocol [0-9]+, config schema [0-9a-f]{12}\)$' "$vd"
    check_re "zfsbackup-config --version names the wire protocol and the config schema" '^zfsbackup-config [0-9][^ ]* \(wire protocol [0-9]+, config schema [0-9a-f]{12}\)$' "$vc"
    check "both CLIs print the same version, protocol and schema (after the program name)" "${vd#* }" "${vc#* }"
    HEAD_REV="$(printf '%s' "$vc" | sed -n 's/.*config schema \([0-9a-f]*\)).*/\1/p')"
    info "this tree's schema head: $HEAD_REV"
    check "the group zfsbackup exists (sysusers) and no zfsbackup user was created" "yes no" \
        "$(vsh 'getent group zfsbackup >/dev/null && echo yes || echo no') $(vsh 'id -u zfsbackup >/dev/null 2>&1 && echo yes || echo no')"
    check "/var/lib/zfsbackup is 2770 root:zfsbackup (tmpfiles)" "2770 root zfsbackup" "$(guest_stat "$CFGDIR")"
    check "/var/lib/zfsbackup/backups is 0700 root:root (tmpfiles)" "700 root root" "$(guest_stat "$BAKDIR")"
    check "the unit file is installed and loaded" "loaded" "$(sd_prop LoadState)"
    check "the unit runs the venv's zfsbackupd" "yes" \
        "$(vsh "systemctl cat $UNIT | grep -q '^ExecStart=$VENV/bin/zfsbackupd ' && echo yes || echo no" | tr -d '[:space:]')"
    if [ "${SDV:-0}" -ge 253 ] 2>/dev/null; then
        check "systemd $SDV >= 253: the unit's Type is notify-reload (no drop-in)" "notify-reload" "$(sd_prop Type)"
        check "... and it has no ExecReload= (systemd sends the reload signal itself)" "" "$(vsh "systemctl show -p ExecReload --value $UNIT" | tr -d '[:space:]')"
        PRE253=0
    else
        PRE253=1
        check "systemd $SDV < 253: WITHOUT the drop-in the unit is Type=simple (notify-reload is unknown)" "simple" "$(sd_prop Type)"
        check_re "... and systemd said so when it loaded the unit (\"Failed to parse service type\")" "Failed to parse service type" \
            "$(vsh "journalctl -b --no-pager -o cat -t systemd 2>&1 | grep 'zfsbackup.service' | head -n3")"
        vsh "
            set -e
            cd $G/$SDIST_DIR
            install -d /etc/systemd/system/zfsbackup.service.d
            install -m 0644 packaging/systemd/zfsbackup.service.d/pre-253.conf /etc/systemd/system/zfsbackup.service.d/
            systemctl daemon-reload
        " || die "installing the pre-253 drop-in failed"
        check "with the drop-in the unit is Type=notify" "notify" "$(sd_prop Type)"
        check_re "... and gets an ExecReload= that sends SIGHUP" "kill -HUP" "$(vsh "systemctl show -p ExecReload --value $UNIT")"
    fi
    check "NotifyAccess=main, Restart=on-failure, KillMode=mixed, TimeoutStopSec=90s" "main on-failure mixed 1min 30s" \
        "$(sd_prop NotifyAccess) $(sd_prop Restart) $(sd_prop KillMode) $(sd_prop TimeoutStopUSec)"
    section_end
}

start_before_import() {
    section "I3 — a start before \`import\` fails cleanly (AssertPathExists), with no restart loop"
    check "precondition: no config database yet" "no" "$(exists "$DB" && echo yes || echo no)"
    local out rc
    out="$(vsh "systemctl start $UNIT 2>&1")"; rc=$?
    if [ "$rc" -ne 0 ]; then pass "systemctl start fails (exit $rc)"; else fail "systemctl start succeeded without a database"; fi
    check_re "systemctl says why (a failed assertion, not a crash)" 'assert' "$(printf '%s' "$out" | tr 'A-Z' 'a-z')"
    sleep 4
    check "the unit is inactive (it never ran)" "inactive" "$(sd_prop ActiveState)"
    check "no restart was scheduled (NRestarts=0)" "0" "$(sd_prop NRestarts)"
    check "no main process was ever started" "0" "$(sd_prop ExecMainPID)"
    check_no_re "the journal has no Python traceback from a start attempt" "Traceback" "$(jl)"
    vsh "systemctl reset-failed $UNIT >/dev/null 2>&1; true" || true
    section_end
}

do_import() {
    section "I4 — \`import\` the configuration; \`zfsbackupd --test-config\`"
    config_yaml "" "" | put_text "$G/config.yaml" 600
    local out rc
    out="$(cli import "$G/config.yaml")"; rc=$?
    check "root's \`zfsbackup-config import\` exits 0" "0" "$rc"
    check "the database exists, mode 660 group zfsbackup" "660 zfsbackup" "$(guest_stat "$DB" | awk '{print $1, $3}')"
    check "the database is at the schema head \`--version\` names" "$HEAD_REV" "$(dbrev "$DB")"
    out="$(vsh "$VENV/bin/zfsbackupd --test-config 2>&1")"; rc=$?
    check "zfsbackupd --test-config exits 0" "0" "$rc"
    check_no_re "... and reports no error" "[Ee]rror|Traceback" "$out"
    section_end
}

check_no_daemon() {
    section "I5 — \`check\` with no daemon; the shipped check unit and timer"
    run_check ""
    check "check exits 1 (unhealthy) with the daemon down" "1" "$CK_RC"
    check_re "... and says no daemon is running" "no daemon is running" "$(ck_out)"
    run_check --json
    check "check --json: exit 1, state CRIT, schema 1" "1 CRIT 1" "$CK_RC $(ck_state) $(vsh "python3 $G/jget.py 'd[\"schema\"]' < $G/ck.out" | tr -d '[:space:]')"
    run_check --nagios
    check "check --nagios: exit 2 (CRIT)" "2" "$CK_RC"
    check_re "... one plugin line" '^ZFSBACKUP CRIT - ' "$(ck_out)"
    # the unit and the timer, from the sdist
    vsh "
        set -e
        cd $G/$SDIST_DIR
        install -m 0644 packaging/systemd/zfsbackup-check.service /etc/systemd/system/zfsbackup-check.service
        install -m 0644 packaging/systemd/zfsbackup-check.timer /etc/systemd/system/zfsbackup-check.timer
        mkdir -p /var/lib/node_exporter/textfile_collector
        systemctl daemon-reload
    " || die "installing the check unit failed"
    local rc
    vsh "systemctl start zfsbackup-check.service"; rc=$?
    check "the check unit run with an unhealthy verdict SUCCEEDS (SuccessExitStatus=1): start exits 0" "0" "$rc"
    check "... Result=success (the check itself exited 1, which SuccessExitStatus= accepts)" "success" "$(vsh 'systemctl show -p Result --value zfsbackup-check.service')"
    local prom; prom="$(vsh "cat $PROM 2>&1")"
    check_re "the .prom file was written (overall state 2: no daemon)" '^zfsbackup_check_overall_state 2$' "$prom"
    check_re "... zfsbackup_check_success 0 and a build_info with the version" '^zfsbackup_check_success 0$' "$prom"
    check_re "... build_info" '^zfsbackup_build_info\{version="[^"]+"\} 1$' "$prom"
    check "the .prom file is 0644 (node_exporter runs as another user)" "644" "$(guest_stat "$PROM" | awk '{print $1}')"
    # a database that cannot be used makes the unit FAIL: that is the one case a failed unit means
    vsh "mv '$DB' '$DB.zbinst-hold'" || die "could not move the database aside"
    run_check ""
    check "check with no database: exit 78" "78" "$CK_RC"
    run_check --nagios
    check "check --nagios with no database: exit 3 (UNKNOWN), never CRIT" "3" "$CK_RC"
    check_re "... ZFSBACKUP UNKNOWN" '^ZFSBACKUP UNKNOWN' "$(ck_out)"
    vsh "systemctl start zfsbackup-check.service >/dev/null 2>&1"; rc=$?
    if [ "$rc" -ne 0 ]; then pass "the check unit FAILS when the database is unusable (exit $rc)"; else fail "the check unit succeeded with no database"; fi
    check "... Result=exit-code, status 78" "exit-code 78" \
        "$(vsh 'systemctl show -p Result --value zfsbackup-check.service') $(vsh 'systemctl show -p ExecMainStatus --value zfsbackup-check.service')"
    check "check did not create a database" "no" "$(exists "$DB" && echo yes || echo no)"
    vsh "mv '$DB.zbinst-hold' '$DB'; systemctl reset-failed zfsbackup-check.service >/dev/null 2>&1; true" || die "could not restore the database"
    section_end
}

enable_now() {
    section "I6 — \`systemctl enable --now\` reaches READY"
    local out rc t0 t1
    t0=$SECONDS
    out="$(vsh "systemctl enable --now $UNIT 2>&1")"; rc=$?
    t1=$SECONDS
    check "enable --now exits 0 (start returned after READY)" "0" "$rc"
    [ "$rc" -eq 0 ] || echo "$out" | sed 's/^/    systemctl: /'
    check "ActiveState=active (not 'activating': READY=1 was received)" "active" "$(sd_prop ActiveState)"
    SUP="$(sd_prop MainPID)"
    check_re "the supervisor is running as root" '^root$' "$(vsh "ps -o user= -p $SUP" | tr -d '[:space:]')"
    local gen; gen="$(sd_prop StatusText)"
    check_re "StatusText carries the configuration generation" '^generation [0-9]+$' "$gen"
    check "the unit is enabled" "enabled" "$(vsh "systemctl is-enabled $UNIT" | tr -d '[:space:]')"
    check "no restart so far" "0" "$(sd_prop NRestarts)"
    check_re "the journal shows the workers started" "Started api worker" "$(jl)"
    check "the daemon's pid file names the supervisor" "$SUP" "$(vsh "sed -n 1p $DB.daemon.pid" | tr -d '[:space:]')"
    info "start took $((t1 - t0))s"
    section_end
}

do_reload() {
    section "I7 — \`systemctl reload\`"
    local gen mark rc t0 t1 n
    gen="$(sd_prop StatusText)"
    mark=$(( $(jl_epoch) - 1 ))
    t0=$SECONDS
    vsh "systemctl reload $UNIT"; rc=$?
    t1=$SECONDS
    check "systemctl reload exits 0" "0" "$rc"
    # on 255 reload returns after READY (the decision); on 249 ExecReload only sends the signal: wait for the decision
    wait_guest 20 "journalctl -u $UNIT -o cat --no-pager --since @$mark | grep -q 'SIGHUP: configuration unchanged'" || true
    n="$(jl --since @$mark | grep -c 'SIGHUP: configuration unchanged' | tr -d '[:space:]')"
    check "exactly one SIGHUP decision was logged (\"configuration unchanged\"): systemd sends ONE signal" "1" "$n"
    check "StatusText unchanged" "$gen" "$(sd_prop StatusText)"
    check "the same supervisor, still active" "active $SUP" "$(sd_prop ActiveState) $(sd_prop MainPID)"
    if [ "$PRE253" -eq 0 ]; then
        check_re "systemd 255: the journal shows the unit reloading and READY again (notify-reload)" "Reload(ing|ed)" \
            "$(vsh "journalctl -u $UNIT -b --no-pager -o cat --since @$mark | grep -i reload" )"
        check "systemd 255: reload returned only after READY (took $((t1 - t0))s, decision already logged)" "1" "$n"
    else
        check_re "systemd 249: the reload ran the drop-in's ExecReload (kill -HUP \$MAINPID)" "kill -HUP|ExecReload" \
            "$(vsh "journalctl -u $UNIT -b --no-pager -o short --since @$mark | grep -i reload; systemctl show -p ExecReload --value $UNIT")"
    fi
    section_end
}

check_warn() {
    section "I8 — \`check\` with a running, not yet enrolled daemon: WARN, not CRIT"
    # the daemon takes the first local snapshot at start; the destination has not been sent to (WARN until the oldest snapshot is old)
    wait_guest 90 "zfs list -H -t snapshot -o name -r '$POOL/data' | grep -q '@autosnap_'" || fail "no local snapshot was taken within 90s"
    run_check --json
    local ents; ents="$(ck_entries)"
    echo "    entries: $ents"
    check "check exits 1" "1" "$CK_RC"
    check "overall WARN (a daemon that runs and snapshots, but is not enrolled)" "WARN" "$(ck_state)"
    check_re "the daemon entries are OK (running, on the stored generation)" "daemon:process=OK" "$ents"
    check_re "the client certificate is a WARN (not enrolled)" "tls:client_certificate=WARN" "$ents"
    check_re "the local snapshot is OK" "snapshot:$POOL/data=OK" "$ents"
    run_check --nagios
    check "check --nagios: exit 1 (WARN)" "1" "$CK_RC"
    vsh "systemctl start zfsbackup-check.service"; local rc=$?
    check "the check unit succeeds on a WARN (exit 1 is not a unit failure)" "0 success" "$rc $(vsh 'systemctl show -p Result --value zfsbackup-check.service')"
    check_re "... and its .prom file says overall state 1" '^zfsbackup_check_overall_state 1$' "$(vsh "cat $PROM")"
    section_end
}

journal_priorities() {
    section "I9 — the journal: priorities reach \`journalctl -p err\`"
    local mark rc
    mark=$(( $(jl_epoch) - 1 ))
    # the only ERROR the shipped daemon logs at will: a configured dataset that does not exist
    config_yaml "" "$POOL/zbinst-missing" | put_text "$G/config-bad.yaml" 600
    cli import --no-backup "$G/config-bad.yaml" | sed 's/^/    CLI: /'
    wait_guest 60 "journalctl -u $UNIT -p err -o cat --no-pager --since @$mark | grep -q 'zbinst-missing does not exist'" \
        && pass "journalctl -p err shows the forced ERROR: \"Dataset $POOL/zbinst-missing does not exist\"" \
        || fail "the forced ERROR never reached journalctl -p err"
    local errs; errs="$(jl -p err --since @$mark)"
    check_no_re "-p err holds no info line (\"Started/Reloaded ... worker\")" "Started .* worker|Reloaded" "$errs"
    local pr; pr="$(vsh "journalctl -u $UNIT -o json --no-pager --since @$mark | python3 $G/jprio.py 'does not exist|(Started|Reloaded) .* worker'")"
    check_re "the ERROR line is stored with PRIORITY 3" $'^3\t.*does not exist' "$pr"
    check_re "the worker start lines are stored with PRIORITY 6 (info)" $'^6\t.*(Started|Reloaded) .* worker' "$pr"
    check_no_re "no worker start line has the error priority" $'^3\t.*(Started|Reloaded) .* worker' "$pr"
    check_no_re "the lines carry no sd-daemon \"<N>\" prefix in the journal text and no timestamp of their own" '^<[0-9]>|^20[0-9][0-9]-[0-9][0-9]-[0-9][0-9] ' "$(jl --since @$mark)"
    # put the good configuration back
    cli import --no-backup "$G/config.yaml" | sed 's/^/    CLI: /'
    wait_guest 60 "journalctl -u $UNIT -o cat --no-pager --since @$mark | grep -qE 'configuration generation [0-9]+ accepted'" || true
    check "the unit stayed active through two imports" "active" "$(sd_prop ActiveState)"
    section_end
}

enroll_and_receive() {
    section "I10 — enroll with itself; a REAL receive is visible on the host"
    local rc
    # the server's TLS identity exists because the daemon started it; mint a token and redeem it, all inside the guest
    vsh "
        set -e
        umask 077
        zfsbackup-config trust token create --json > $G/token.json
        python3 $G/jget.py 'd[\"token\"]' < $G/token.json > $G/token
        python3 $G/jget.py 'd[\"server_fingerprint\"]' < $G/token.json > $G/server_fp
        rm -f $G/token.json
    " || die "could not mint an enrollment token"
    SERVER_FP="$(vsh "cat $G/server_fp" | tr -d '[:space:]')"
    local out
    # the API worker may be mid-turnover after the imports: wait for /health, and retry once on exit 75 (the CLI's "retry"; enroll's own
    # client-id write reloads this very daemon, which restarts the API it is about to call: only possible because this host enrolls with itself)
    wait_guest 90 "curl -fsSk --connect-timeout 3 --max-time 6 -o /dev/null https://127.0.0.1:$API_PORT/health 2>/dev/null" || fail "the API never answered /health"
    out="$(vsh "zfsbackup-config enroll self --url https://127.0.0.1:$API_PORT --token-file $G/token 2>&1")"; rc=$?
    if [ "$rc" -eq 75 ]; then
        printf '%s\n' "$out" | sed 's/^/    CLI (first try): /'
        wait_guest 90 "curl -fsSk --connect-timeout 3 --max-time 6 -o /dev/null https://127.0.0.1:$API_PORT/health 2>/dev/null" || true
        out="$(vsh "zfsbackup-config enroll self --url https://127.0.0.1:$API_PORT --token-file $G/token 2>&1")"; rc=$?
        info "enroll needed a second try (exit 75 the first time: the API worker was restarting)"
    fi
    vsh "rm -f $G/token" || true
    printf '%s\n' "$out" | sed 's/^/    CLI: /'
    check "enroll with the token exits 0" "0" "$rc"
    CID="$(vsh "zfsbackup-config identity show --json 2>/dev/null | python3 $G/jget.py 'd[\"client_id\"]'" | tr -d '[:space:]')"
    check_re "the client has an id (generated by enroll)" '^[A-Za-z0-9][A-Za-z0-9._-]*$' "$CID"
    SDS="$POOL/backups/$CID/data"     # the server strips the client dataset's pool name
    PARENT="$POOL/backups/$CID"
    local waited=0
    until vsh "zfs list -H -t snapshot -o name -r '$SDS' 2>/dev/null | grep -q ."; do
        [ "$waited" -ge 150 ] && break; sleep 3; waited=$((waited+3))
    done
    local ssnap; ssnap="$(vsh "zfs list -H -t snapshot -o name -s createtxg -r '$SDS' 2>/dev/null | head -n1" | tr -d '[:space:]')"
    if [ -n "$ssnap" ]; then pass "the first snapshot reached the server dataset ($ssnap)"; else fail "nothing was received into $SDS within 150s"; fi
    local snapname="${ssnap#*@}"
    local cg sg
    cg="$(vsh "zfs get -H -o value guid '$POOL/data@$snapname' 2>/dev/null" | tr -d '[:space:]')"
    sg="$(vsh "zfs get -H -o value guid '$ssnap' 2>/dev/null" | tr -d '[:space:]')"
    if [ -n "$cg" ] && [ "$cg" = "$sg" ]; then pass "the received snapshot is byte-identical (guid $cg)"; else fail "guid client='$cg' server='$sg'"; fi
    check_re "the client's anchor for the destination is set" "^autosnap_" "$(vsh "zfs get -H -o value org.zfsbackup:anchor.self '$POOL/data' 2>/dev/null")"
    # visibility on the host. The server receives with `-u` (never mounts the copy), so the mounts the daemon makes itself are the
    # parents it creates (zfs create -p <target>/<client id>); the received data is read through a clone, as the README's restore says.
    local cm mounted pmounted
    cm="$(vsh "zfs get -H -o value canmount '$SDS'" | tr -d '[:space:]')"
    mounted="$(vsh "zfs get -H -o value mounted '$SDS'" | tr -d '[:space:]')"
    pmounted="$(vsh "zfs get -H -o value mounted '$PARENT'" | tr -d '[:space:]')"
    info "server copy: canmount=$cm mounted=$mounted; its parent $PARENT (created by the daemon): mounted=$pmounted"
    local nsd nsh
    nsd="$(vsh "readlink /proc/$(sd_prop MainPID)/ns/mnt" | tr -d '[:space:]')"; nsh="$(vsh 'readlink /proc/1/ns/mnt' | tr -d '[:space:]')"
    check "the daemon has no private mount namespace (the unit's mount namespace is init's)" "$nsh" "$nsd"
    # B-2a: the parents the daemon creates are canmount=noauto and never mounted, so nothing of the server tree appears on the host
    # by itself; the only mount in this scenario is the read-only restore clone below (the README's restore recipe)
    check "the parent the daemon created for the client ($PARENT) is canmount=noauto and not mounted (B-2a)" "noauto no" \
        "$(vsh "zfs get -H -o value canmount '$PARENT'" | tr -d '[:space:]') $pmounted"
    check "the received copy itself is not mounted (the receive uses -u)" "no" "$mounted"
    vsh "zfs create -p '$POOL/restore' && zfs clone -o readonly=on -o canmount=on -o mountpoint=/mnt/zbinst-restore '$SDS@$snapname' '$POOL/restore/data'" \
        && pass "the README's file-level restore: a read-only clone outside the target tree was created" || fail "could not clone the received snapshot"
    check_re "findmnt on the HOST shows the clone mounted" "zfs" "$(vsh "findmnt -n -o FSTYPE,TARGET /mnt/zbinst-restore 2>&1")"
    check "the restored file is identical to the client's file at that snapshot" \
        "$(vsh "md5sum /$POOL/data/.zfs/snapshot/$snapname/file1.bin | cut -d' ' -f1" | tr -d '[:space:]')" \
        "$(vsh "md5sum /mnt/zbinst-restore/file1.bin 2>&1 | cut -d' ' -f1" | tr -d '[:space:]')"
    vsh "{ zfs destroy '$POOL/restore/data'; zfs destroy '$POOL/restore'; } >/dev/null 2>&1; true"
    # the README's restore/rename examples must spell the copy the way the server names it (pool name stripped)
    if grep -q 'tank/backups/<client_id>/tank/' "$TOP/zfsbackup/README.md"; then
        finding "README restore/rename examples spell the server copy \`tank/backups/<client_id>/tank/data\`; the server strips the client dataset's pool name (the copy of $POOL/data is \`$SDS\`)"
    else
        pass "the README's restore/rename examples spell the server copy as the server names it (\`tank/backups/<client_id>/data\`)"
    fi
    # a second snapshot is incremental
    vsh "sleep 2; head -c 300000 /dev/urandom >> /$POOL/data/file1.bin; zfs snapshot '$POOL/data@autosnap_'\$(date -u +%Y%m%d%H%M%S)" || true
    waited=0
    until [ "$(vsh "zfs list -H -t snapshot -o name -r '$SDS' 2>/dev/null | grep -c ." | tr -d '[:space:]')" -ge 2 ]; do
        [ "$waited" -ge 120 ] && break; sleep 3; waited=$((waited+3))
    done
    check "a later snapshot follows (incrementally)" "yes" \
        "$([ "$(vsh "zfs list -H -t snapshot -o name -r '$SDS' 2>/dev/null | grep -c ." | tr -d '[:space:]')" -ge 2 ] && echo yes || echo no)"
    section_end
}

check_ok() {
    section "I11 — \`check\` is healthy; \`--prometheus --output\`, \`--nagios\`, the timer"
    # let the remote worker finish whatever it was sending (anchor == newest) before grading
    sleep 20
    run_check ""
    check "check exits 0" "0" "$CK_RC"
    [ "$CK_RC" -eq 0 ] || ck_out | sed 's/^/    check: /'
    check_re "... overall: OK" '^overall: OK ' "$(ck_out)"
    run_check --json
    check "check --json: schema 1, state OK" "1 OK" "$(vsh "python3 $G/jget.py 'd[\"schema\"]' < $G/ck.out" | tr -d '[:space:]') $(ck_state)"
    echo "    entries: $(ck_entries)"
    check_re "the entries cover snapshot, anchor (the destination), received (the server side), daemon and tls" \
        "snapshot:.*anchor:.*received:.*daemon:process=OK.*tls:client_certificate=OK" "$(ck_entries)"
    run_check --nagios
    check "check --nagios: exit 0" "0" "$CK_RC"
    check_re "... one OK line with perfdata" "^ZFSBACKUP OK - .* \| '" "$(ck_out)"
    run_check "--prometheus --output $G/out.prom"
    check "check --prometheus --output FILE exits 0 and prints nothing" "0 0" "$CK_RC $(vsh "wc -c < $G/ck.out" | tr -d '[:space:]')"
    local prom; prom="$(vsh "cat $G/out.prom")"
    check_re "the file has the overall state" '^zfsbackup_check_overall_state 0$' "$prom"
    check_re "... a snapshot age and state per dataset" "^zfsbackup_snapshot_age_seconds\{dataset=\"$POOL/data\"\} " "$prom"
    check_re "... an anchor age per destination" "^zfsbackup_anchor_age_seconds\{dataset=\"$POOL/data\",destination=\"self\"\} " "$prom"
    check_re "... a received age per client" "^zfsbackup_received_age_seconds\{dataset=\"[^\"]+\",client=\"$CID\"\} " "$prom"
    check_re "... zfsbackup_check_success 1 and a timestamp" '^zfsbackup_check_success 1$' "$prom"
    check "the file is 0644 and no temporary file was left beside it" "644 0" \
        "$(guest_stat "$G/out.prom" | awk '{print $1}') $(vsh "ls -A $G | grep -c '^\.out\.prom\.'" | tr -d '[:space:]')"
    # the timer
    vsh "systemctl enable --now zfsbackup-check.timer" ; local rc=$?
    check "systemctl enable --now zfsbackup-check.timer exits 0" "0" "$rc"
    check "the timer is active and scheduled" "active" "$(vsh 'systemctl show -p ActiveState --value zfsbackup-check.timer' | tr -d '[:space:]')"
    check_re "... list-timers shows it with a next run" "zfsbackup-check.timer" "$(vsh 'systemctl list-timers zfsbackup-check.timer --no-pager 2>&1')"
    # the timer fires at once when enabled on a booted host (OnBootSec=2min has elapsed): stop it first, or its run can land between the rm and our start
    vsh "systemctl disable --now zfsbackup-check.timer >/dev/null 2>&1; while systemctl is-active --quiet zfsbackup-check.service; do sleep 1; done; true" || true
    vsh "rm -f $PROM; systemctl start zfsbackup-check.service"; rc=$?
    check "the check unit run on a healthy host exits 0, Result=success" "0 success" \
        "$rc $(vsh 'systemctl show -p Result --value zfsbackup-check.service')"
    check_re "... and its .prom file says success" '^zfsbackup_check_success 1$' "$(vsh "cat $PROM")"
    vsh "systemctl disable --now zfsbackup-check.timer >/dev/null 2>&1; true" || true
    section_end
}

backup_cmd() {
    section "I12 — \`backup\` as root and as a group operator"
    local out rc dir
    out="$(vsh "zfsbackup-config backup --json /srv/zbinst-backups 2>$G/bk.err")"; rc=$?
    check "backup as root exits 0" "0" "$rc"
    dir="$(printf '%s' "$out" | python3 -c 'import json,sys; print(json.load(sys.stdin)["path"])' 2>/dev/null)"
    check_re "--json names the backup directory under the requested DIR" '^/srv/zbinst-backups/zfsbackup-' "$dir"
    check "the DIR zfsbackup created is 0700 root, and the backup directory is 0700" "700 root 700" \
        "$(guest_stat /srv/zbinst-backups | awk '{print $1, $2}') $(guest_stat "$dir" | awk '{print $1}')"
    check "config.db is 0600 and MANIFEST exists (written last)" "600 yes" "$(guest_stat "$dir/config.db" | awk '{print $1}') $(vsh "test -s '$dir/MANIFEST' && echo yes || echo no" | tr -d '[:space:]')"
    check "the TLS private keys are included (server.key and client.key, 0600)" "600 600" \
        "$(guest_stat "$dir/tls/server.key" | awk '{print $1}') $(guest_stat "$dir/tls/client.key" | awk '{print $1}')"
    check_re "the MANIFEST says keys_included: yes" "keys_included: yes" "$(vsh "cat '$dir/MANIFEST'")"
    check "the copy is a usable database at the same schema head and the same generation/client id" "$HEAD_REV" "$(dbrev "$dir/config.db")"
    check "the copy keeps the client id" "$CID" "$(vsh "zfsbackup-config -c '$dir/config.db' identity show --json 2>/dev/null | python3 $G/jget.py 'd[\"client_id\"]'" | tr -d '[:space:]')"
    # a group operator
    vsh "id -u $OPUSER >/dev/null 2>&1 || useradd -m -G zfsbackup $OPUSER" || die "could not create the operator"
    check "the operator is in the group zfsbackup, not root" "yes" "$(vsh "id -nG $OPUSER | tr ' ' '\n' | grep -qx zfsbackup && echo yes || echo no" | tr -d '[:space:]')"
    out="$(vasu "$OPUSER" "zfsbackup-config backup ~/zbbk 2>&1")"; rc=$?
    check "an operator's backup is REFUSED (exit 1): the keys are root's" "1" "$rc"
    check_re "... naming --no-keys" "\-\-no-keys" "$out"
    check "... and nothing was left behind (the DIR it created is removed)" "no" "$(vsh "test -e /home/$OPUSER/zbbk && echo yes || echo no" | tr -d '[:space:]')"
    out="$(vasu "$OPUSER" "zfsbackup-config backup --no-keys ~/zbbk 2>&1")"; rc=$?
    check "the operator's \`backup --no-keys\` exits 0" "0" "$rc"
    check_re "... and notes that the private keys were not copied" "keys were not copied" "$out"
    local odir; odir="$(vsh "ls -d /home/$OPUSER/zbbk/zfsbackup-* 2>/dev/null | head -n1" | tr -d '[:space:]')"
    check "the operator's backup has config.db, the certificates and no private key" "yes 0" \
        "$(vsh "test -s '$odir/config.db' && test -s '$odir/tls/server.crt' && echo yes || echo no" | tr -d '[:space:]') $(vsh "find '$odir' -name '*.key' | wc -l" | tr -d '[:space:]')"
    check "... owned by the operator, mode 0700" "700 $OPUSER" "$(guest_stat "$odir" | awk '{print $1, $2}')"
    check "the daemon was not disturbed by the backups (still the same supervisor)" "active $SUP" "$(sd_prop ActiveState) $(sd_prop MainPID)"
    section_end
}

clean_stop() {
    section "I13 — a clean \`systemctl stop\` well inside 90 s"
    local t0 t1 rc
    t0=$SECONDS
    vsh "systemctl stop $UNIT"; rc=$?
    t1=$SECONDS
    check "systemctl stop exits 0" "0" "$rc"
    if [ $((t1 - t0)) -lt 40 ]; then pass "the stop took $((t1 - t0))s (TimeoutStopSec=90)"; else fail "the stop took $((t1 - t0))s"; fi
    check "inactive, Result=success (no SIGKILL, no timeout)" "inactive success" "$(sd_prop ActiveState) $(sd_prop Result)"
    check "no daemon or worker process is left" "0" "$(vsh 'pgrep -fc "[z]fsbackupd|[z]fsbackup\.daemon" || true' | tr -d '[:space:]')"
    check "the pid file is gone" "no" "$(exists "$DB.daemon.pid" && echo yes || echo no)"
    check_re "the journal shows the stop was announced and clean (no 'Failed with result')" "Stopped|Deactivated" "$(vsh "journalctl -u $UNIT -b --no-pager -o short | tail -n 6")"
    check_no_re "... and the unit did not fail" "Failed with result" "$(vsh "journalctl -u $UNIT -b --no-pager -o short | tail -n 12")"
    run_check --json
    check "check after the stop: exit 1, CRIT (no daemon)" "1 CRIT" "$CK_RC $(ck_state)"
    section_end
}

restart_policy() {
    section "I14 — the restart policy: SIGHUP and 76 force a restart"
    local rf; rf="$(vsh "systemctl show -p RestartForceExitStatus --value $UNIT")"
    check_re "RestartForceExitStatus names 76" "76" "$rf"
    check_re "... and SIGHUP" "HUP" "$rf"
    # a main process that dies of SIGHUP is NOT a failure for systemd (SIGHUP is a "clean" signal): only RestartForceExitStatus restarts it
    put_text /etc/systemd/system/zfsbackup.service.d/zz-zbinst-hup.conf 644 <<'DROPEOF'
[Service]
Type=simple
ExecStartPre=
ExecStart=
ExecStart=/bin/sh -c 'kill -HUP $$$$'
RestartSec=2
DROPEOF
    vsh "systemctl daemon-reload; systemctl reset-failed $UNIT >/dev/null 2>&1; systemctl start $UNIT >/dev/null 2>&1; true"
    sleep 10
    local n; n="$(sd_prop NRestarts)"
    if [ "${n:-0}" -ge 2 ] 2>/dev/null; then pass "a main process killed by SIGHUP was restarted by the unit (NRestarts=$n in 10s)"; else fail "SIGHUP death was not restarted (NRestarts=$n)"; fi
    vsh "
        systemctl stop $UNIT >/dev/null 2>&1
        rm -f /etc/systemd/system/zfsbackup.service.d/zz-zbinst-hup.conf
        systemctl daemon-reload
        systemctl reset-failed $UNIT >/dev/null 2>&1
        true
    " || true
    section_end
}

# ───────────────────────────── I15: upgrade from OLD_REF ─────────────────────────────
upgrade() {
    section "I15 — UPGRADE from $OLD_REF to the split wheels: stop, pip uninstall the old dist, pip install both wheels, start"
    # the install state is reset, then the old wheel is installed and run
    vsh "
        { systemctl stop $UNIT zbinst-old; systemctl reset-failed; } >/dev/null 2>&1
        rm -rf /opt/zfsbackup/venv /usr/local/bin/zfsbackup-config /usr/local/bin/zfsbackupd $CFGDIR/config.db* $CFGDIR/backups/* /srv/zbinst-backups
        rm -rf /etc/systemd/system/zfsbackup.service.d
        zfs destroy -r '$POOL/backups' >/dev/null 2>&1; zfs create '$POOL/backups'
        zfs destroy -r '$POOL/data' >/dev/null 2>&1; zfs create '$POOL/data'
        head -c 500000 /dev/urandom > /$POOL/data/old.bin
        systemctl daemon-reload
        true
    " || die "could not reset for the upgrade"
    # the old wheel gets a directory of its own: it may carry the very same file name as the current tree's (both are 1.0.0rc1)
    local oldbase; oldbase="old/$(basename "$OLD_WHL")"
    put_file "$OLD_WHL" "$G/$oldbase" 644
    put_wheels
    install_venv "$G/$oldbase"
    # the pre-253 drop-in stays as the README says for the unit; the OLD tree has no zfsbackupd, so it runs as a transient unit
    [ "$PRE253" -eq 1 ] && vsh "install -d /etc/systemd/system/zfsbackup.service.d && install -m 0644 $G/$SDIST_DIR/packaging/systemd/zfsbackup.service.d/pre-253.conf /etc/systemd/system/zfsbackup.service.d/ && systemctl daemon-reload"
    old_config_yaml | sed "s|__POOL__|$POOL|g" | put_text "$G/config-old.yaml" 600
    local out rc
    out="$(cli import --no-backup "$G/config-old.yaml")"; rc=$?
    check "the OLD tree's \`import\` works (exit 0)" "0" "$rc"
    OLD_REV="$(dbrev "$DB")"
    if [ -n "$OLD_REV_EXPECT" ]; then check "the database is at the OLD schema revision $OLD_REV_EXPECT" "$OLD_REV_EXPECT" "$OLD_REV"
    else info "the old tree's schema revision: $OLD_REV (this tree's head: $HEAD_REV)"; fi
    local MIGRATES=0; [ "$OLD_REV" != "$HEAD_REV" ] && MIGRATES=1
    out="$(cli identity set-client-id zbinst-upgrade)"; rc=$?
    check "the old tree sets a client id (exit 0)" "0" "$rc"
    vsh "systemd-run --unit=zbinst-old --collect -p StandardOutput=append:/var/log/zbinst-old.log -p StandardError=append:/var/log/zbinst-old.log \
         $VENV/bin/python -m zfsbackup.daemon -c $DB >/dev/null 2>&1" || die "could not start the old daemon"
    wait_guest 60 "systemctl is-active --quiet zbinst-old && grep -q 'Started api worker\|Started snapshot worker' /var/log/zbinst-old.log" \
        && pass "the OLD daemon is running (transient unit zbinst-old)" || fail "the old daemon did not start: $(vsh 'tail -n 5 /var/log/zbinst-old.log' | tr '\n' '|')"
    # --- the README's upgrade: stop, install, start ---
    vsh "systemctl stop zbinst-old"; rc=$?
    check "the old daemon stops cleanly" "0" "$rc"
    local pipout
    # --- why the runbook says "uninstall first": a PLAIN install of the split wheels over an old single `zfsbackup` wheel deletes
    # libzfseasy. Shown on a COPY of the venv (the old wheel and its dependencies installed in it, so nothing is fetched), with wheels
    # whose zfsbackup version differs from the old one (same version: pip would call the zfsbackup wheel satisfied and nothing breaks).
    if [ -n "$NEW_LIB_WHL" ] && [ "$OLD_DIST" = zfsbackup ]; then
        local hv=/var/tmp/zbinst-hazard hout hwheels="" oldver newver bumpver
        oldver="$(basename "$OLD_WHL" | cut -d- -f2)"; newver="$(basename "$NEW_WHL" | cut -d- -f2)"
        bumpver=""; [ -n "$BUMP_WHL" ] && bumpver="$(basename "$BUMP_WHL" | cut -d- -f2)"
        if [ "$newver" != "$oldver" ]; then hwheels="$NEW_GUEST"
        elif [ -n "$bumpver" ] && [ "$bumpver" != "$oldver" ]; then hwheels="$BUMP_GUEST"; fi
        if [ -z "$hwheels" ]; then
            info "the hazard of a plain \`pip install\` over the old wheel is not shown: the current tree has the same version as $OLD_REF ($oldver) and no N+1 wheel was built (pip would call the zfsbackup wheel satisfied)"
        else
            if vsh "test -x $VENV/bin/pip"; then
                hout="$(vsh "
                    set -e
                    rm -rf $hv; cp -a $VENV $hv
                    $hv/bin/python -m pip install -q $hwheels
                    cd / && ZFS_CMD=/bin/true ZPOOL_CMD=/bin/true $hv/bin/python -c 'import libzfseasy; print(\"IMPORT_OK\")' 2>&1 | tail -n1
                " 2>&1)"
            else
                hout="$(vsh "
                    set -e
                    rm -rf $hv; cp -a $VENV $hv
                    /var/tmp/zbinst-uvtool/bin/uv pip install -q --python $hv/bin/python $hwheels
                    cd / && ZFS_CMD=/bin/true ZPOOL_CMD=/bin/true $hv/bin/python -c 'import libzfseasy; print(\"IMPORT_OK\")' 2>&1 | tail -n1
                " 2>&1)"
            fi
            if printf '%s' "$hout" | grep -q IMPORT_OK; then
                info "a plain install of the split wheels over the old single \`zfsbackup\` wheel ($OLD_REF) left \`libzfseasy\` importable ($PYMODE): the hazard the README warns of is pip's, and the README's uninstall-first procedure is safe with either installer"
            else
                pass "the hazard is real ($PYMODE): a PLAIN install of the split wheels over the old single \`zfsbackup\` wheel leaves \`import libzfseasy\` broken ($(printf '%s' "$hout" | tail -n1 | cut -c1-90))"
            fi
            vsh "rm -rf $hv" || true
        fi
    fi
    if [ -n "$NEW_LIB_WHL" ]; then
        # the runbook's procedure for the move from ONE wheel to TWO: the daemon is stopped (above), the old distribution is
        # uninstalled (never upgraded in place: see the hazard shown before), then both wheels are installed
        if vsh "test -x $VENV/bin/pip"; then
            pipout="$(vsh "$VENV/bin/pip uninstall -y $OLD_DIST 2>&1")"; rc=$?
            check "\`$VENV/bin/pip uninstall -y $OLD_DIST\` succeeds" "0" "$rc"
            [ "$rc" -eq 0 ] || echo "$pipout" | sed 's/^/    pip: /'
            pipout="$(vsh "$VENV/bin/pip install $NEW_GUEST 2>&1")"; rc=$?
            check "\`$VENV/bin/pip install libzfseasy-*.whl zfsbackup-*.whl\` succeeds" "0" "$rc"
        else
            info "a \`uv venv\` has no pip: the README's \`uv pip uninstall\` / \`uv pip install --python\` variant"
            pipout="$(vsh "/var/tmp/zbinst-uvtool/bin/uv pip uninstall --python $VENV/bin/python $OLD_DIST 2>&1")"; rc=$?
            check "\`uv pip uninstall $OLD_DIST\` succeeds" "0" "$rc"
            [ "$rc" -eq 0 ] || echo "$pipout" | sed 's/^/    uv: /'
            pipout="$(vsh "/var/tmp/zbinst-uvtool/bin/uv pip install --python $VENV/bin/python $NEW_GUEST 2>&1")"; rc=$?
            check "\`uv pip install libzfseasy-*.whl zfsbackup-*.whl\` succeeds" "0" "$rc"
        fi
        [ "$rc" -eq 0 ] || echo "$pipout" | sed 's/^/    pip: /'
        local di; di="$(vsh "$VENV/bin/python $G/distinfo.py" | tr '\n' ' ')"
        check_re "the venv holds both distributions at the same version, libzfseasy's files in the libzfseasy one" 'libzfseasy=[^ ]+ zfsbackup=[^ ]+ same=1 lib_in_lib=[1-9][0-9]* lib_in_zb=0 ' "$di"
        check "\`import libzfseasy\` works after the upgrade" "yes" \
            "$(vsh "cd / && ZFS_CMD=/bin/true ZPOOL_CMD=/bin/true $VENV/bin/python -c 'import libzfseasy; print(\"yes\")' 2>&1 | tail -n1" | tr -d '[:space:]')"
    else
        # a tree from before the split: one wheel, upgraded in place as the README said
        if vsh "test -x $VENV/bin/pip"; then
            pipout="$(vsh "$VENV/bin/pip install --upgrade '$G/$NEW_WHL_BASE' 2>&1")"; rc=$?
            check "the README's \`$VENV/bin/pip install --upgrade ./zfsbackup-NEW.whl\` succeeds" "0" "$rc"
        else
            info "a \`uv venv\` has no pip: the README's \`uv pip install --python --upgrade\` variant"
            pipout="$(vsh "/var/tmp/zbinst-uvtool/bin/uv pip install --python $VENV/bin/python --upgrade '$G/$NEW_WHL_BASE' 2>&1")"; rc=$?
            check "the same upgrade with \`uv pip install --upgrade\` succeeds" "0" "$rc"
        fi
        [ "$rc" -eq 0 ] || echo "$pipout" | sed 's/^/    pip: /'
    fi
    vsh "install -m 0644 $G/$SDIST_DIR/packaging/systemd/zfsbackup.service /etc/systemd/system/zfsbackup.service; systemctl daemon-reload"
    check "the new tree's zfsbackupd exists now and \`--version\` works" "yes" "$(vsh "$VENV/bin/zfsbackupd --version >/dev/null 2>&1 && echo yes || echo no" | tr -d '[:space:]')"
    check "the database is still at the OLD revision before the start (the upgrade does not migrate)" "$OLD_REV" "$(dbrev "$DB")"
    check "the backups directory is empty before the first start" "0" "$(vsh "ls -A $BAKDIR | wc -l" | tr -d '[:space:]')"
    local t0=$SECONDS umark; umark=$(( $(jl_epoch) - 1 ))
    out="$(vsh "systemctl start $UNIT 2>&1")"; rc=$?
    check "systemctl start zfsbackup exits 0 (ExecStartPre migrated, then READY)" "0" "$rc"
    [ "$rc" -eq 0 ] || { echo "$out" | sed 's/^/    systemctl: /'; vsh "journalctl -u $UNIT -n 20 --no-pager -o cat" | sed 's/^/    journal: /'; }
    info "start (with migration) took $((SECONDS - t0))s"
    check "the unit is active" "active" "$(sd_prop ActiveState)"
    check "the database is at the NEW head $HEAD_REV" "$HEAD_REV" "$(dbrev "$DB")"
    local bks; bks="$(vsh "ls -1 $BAKDIR")"     # (looked at before anything opens the copy: a WAL-mode copy grows -wal/-shm sidecars when read)
    if [ "$MIGRATES" -eq 1 ]; then
        check "exactly one pre-migration backup exists in $BAKDIR" "1" "$(printf '%s\n' "$bks" | grep -c .)"
        check_re "... named config.db.pre-$OLD_REV-<UTC time>" "^config\.db\.pre-$OLD_REV-[0-9]{8}T[0-9]{6}Z\$" "$bks"
        local bk; bk="$BAKDIR/$(printf '%s\n' "$bks" | head -n1)"
        check "the backup is 0600 root and the directory 0700 root" "600 root 700 root" "$(guest_stat "$bk" | awk '{print $1, $2}') $(guest_stat "$BAKDIR" | awk '{print $1, $2}')"
        vsh "cp '$bk' $G/bk-copy.db"
        check "the backup is a usable database still at the OLD revision" "$OLD_REV" "$(dbrev "$G/bk-copy.db")"
    else
        info "$OLD_REF is already at the schema head $HEAD_REV: no migration, so no pre-migration backup is expected (OLD_REF=0cd236c exercises that path)"
        check "no pre-migration backup was written (nothing to migrate)" "0" "$(printf '%s\n' "$bks" | grep -c .)"
    fi
    check "the client id survived the upgrade" "zbinst-upgrade" "$(vsh "zfsbackup-config identity show --json 2>/dev/null | python3 $G/jget.py 'd[\"client_id\"]'" | tr -d '[:space:]')"
    check_re "the dataset is still configured" "$POOL/data" "$(cli list datasets)"
    check_re "the daemon is on the stored generation (StatusText)" '^generation [0-9]+$' "$(sd_prop StatusText)"
    local ujl; ujl="$(jl --since @$umark)"
    if [ "$MIGRATES" -eq 1 ]; then
        check_re "the journal (ExecStartPre) says it backed up the database first" "Backed up $DB \\(schema $OLD_REV" "$ujl"
        check_re "... and that it migrated $OLD_REV -> $HEAD_REV" "Migrated the schema of $DB from $OLD_REV to $HEAD_REV" "$ujl"
    else
        check_re "the journal (ExecStartPre) says the schema is up to date" "Schema is up to date" "$ujl"
    fi
    # a plain restart writes no second backup
    umark=$(( $(jl_epoch) - 1 ))
    vsh "systemctl restart $UNIT"; rc=$?
    check "a plain restart exits 0" "0" "$rc"
    check "... ExecStartPre found the schema up to date" "1" "$(jl --since @$umark | grep -c 'Schema is up to date' | tr -d '[:space:]')"
    check "... and wrote no (second) backup (still $MIGRATES backup file(s))" "$MIGRATES" "$(vsh "ls -1 $BAKDIR | grep -vc -- '-\(wal\|shm\)\$'" | tr -d '[:space:]')"
    check "the unit is active again" "active" "$(sd_prop ActiveState)"
    vsh "systemctl stop $UNIT" || true
    section_end
}

# ───────────────────── I16: in-place upgrade under a running daemon (S-8) ─────────────────────
# pip_venv "<wheel on the guest>[ <wheel>...]" [reinstall]: install into the venv, pip or (a `uv venv` has no pip) uv; --no-deps keeps it
# offline. The first argument is a space-separated list (NEW_GUEST, BUMP_GUEST: both wheels of the split layout in one command, so pip
# installs them together as the runbook says; BUMP_LIB_GUEST: the library alone).
pip_venv() {
    local whls="$1" re="${2:-}" w quoted=""
    for w in $whls; do quoted="$quoted '$w'"; done
    if vsh "test -x $VENV/bin/pip"; then
        vsh "$VENV/bin/pip install -q --no-deps ${re:+--force-reinstall} $quoted 2>&1"
    else
        vsh "/var/tmp/zbinst-uvtool/bin/uv pip install --python $VENV/bin/python --no-deps ${re:+--reinstall} $quoted 2>&1"
    fi
}
venv_version() { vsh "$VENV/bin/zfsbackupd --version 2>&1" | head -n1 | awk '{print $2}' | tr -d '[:space:]'; }
SKEW_WARN='changed on disk while the daemon was running'
n_in_journal() { jl --since "@$1" | grep -c -- "$2" | tr -d '[:space:]'; }      # n_in_journal <mark> <fixed string>

# await_exit76_restart <old main pid> <mark> <label>: wait for the exit 76 and for systemd's restart onto a NEW main pid
await_exit76_restart() {
    local oldpid="$1" mark="$2" label="$3" gap
    wait_guest 150 "journalctl -u $UNIT -o cat --no-pager --since @$mark | grep -q 'Main process exited, code=exited, status=76'" \
        && pass "$label: the main process exited with status 76" || fail "$label: no 'status=76' in the journal within 150s"
    wait_guest 90 "[ \"\$(systemctl show -p MainPID --value $UNIT)\" != $oldpid ] && systemctl is-active --quiet $UNIT" \
        && pass "$label: systemd restarted the unit and it is active again (READY)" || fail "$label: the unit is not active again (state $(sd_prop ActiveState))"
    gap="$(vsh "journalctl -u $UNIT -o short-unix --no-pager --since @$mark | awk -f $G/restartgap.awk" | tr -d '[:space:]')"
    if [ "$gap" != none ] && [ "${gap:-0}" -ge 14 ] 2>/dev/null; then pass "$label: the restart came ${gap}s after the exit (RestartSec=15 for this test)"
    else fail "$label: the restart delay was '$gap' s (expected >= 14, RestartSec=15)"; fi
}

# after_restart_clean <old main pid> <mark> <label> <wanted version>: the second start is clean, not a loop
after_restart_clean() {
    local oldpid="$1" mark="$2" label="$3" want="$4" newpid out rc
    newpid="$(sd_prop MainPID)"
    check "$label: \`zfsbackupd --version\` shows $want" "$want" "$(venv_version)"
    check "$label: exactly one restart so far (\"Scheduled restart job\")" "1" "$(n_in_journal "$mark" 'Scheduled restart job')"
    check "$label: the drain WARNING was logged exactly once" "1" "$(n_in_journal "$mark" "$SKEW_WARN")"
    sleep 25
    check "$label: 25 s later: still the new supervisor, still one restart (no restart loop)" "active $newpid 1" \
        "$(sd_prop ActiveState) $(sd_prop MainPID) $(n_in_journal "$mark" 'Scheduled restart job')"
    [ "$newpid" != "$oldpid" ] && pass "$label: a new supervisor (pid $oldpid -> $newpid)" || fail "$label: same supervisor pid $newpid"
    check_re "$label: StatusText is the plain generation again" '^generation [0-9]+$' "$(sd_prop StatusText)"
    out="$(vsh "zfsbackup-config status --wait 30 2>&1")"; rc=$?
    check "$label: \`zfsbackup-config status --wait 30\` exits 0" "0" "$rc"
    [ "$rc" -eq 0 ] || printf '%s\n' "$out" | sed 's/^/    status: /'
    check_re "$label: the new daemon logged the version it runs" "zfsbackup $want" "$(jl --since @$mark)"
}

# slow_transfer <file tag>: throttle the loopback (tc tbf, 4 Mbit/s = 0.5 MB/s) and snapshot 300 MB of fresh data, so the remote worker's
# transfer to the local API takes ~10 minutes; returns once the stream `zfs receive` (pid in RX_PID) is up and has read >= 1 MB
RX_PID=""
slow_transfer() {
    vsh "tc qdisc replace dev lo root tbf rate 4mbit burst 256kb latency 1s" || { fail "could not throttle the loopback (tc)"; return 1; }
    vsh "head -c 300000000 /dev/urandom > /$POOL/data/skew-$1.bin; sync; zfs snapshot '$POOL/data@autosnap_'\$(date -u +%Y%m%d%H%M%S)" || { fail "could not create the test snapshot"; return 1; }
    RX_PID="$(vsh "bash $G/waitrx.sh 150" | tr -d '[:space:]')"
    if [ -z "$RX_PID" ]; then fail "no zfs receive was reading within 150s (the remote worker did not start a transfer)"; return 1; fi
    check_re "a throttled transfer is in flight: the stream's zfs receive ($RX_PID) is running" '^[RSD]' "$(vsh "ps -o stat= -p $RX_PID" | tr -d '[:space:]')"
}
unthrottle() { vsh "tc qdisc del dev lo root >/dev/null 2>&1; true" || true; }
rx_still_running() { # the stream's receive is alive and has not read the whole 300 MB
    vsh "kill -0 $RX_PID 2>/dev/null && [ \"\$(awk '/^rchar:/ {print \$2}' /proc/$RX_PID/io)\" -lt 250000000 ] && echo yes || echo no" | tr -d '[:space:]'
}

inplace_upgrade() {
    section "I16 — in-place \`pip install\` over a RUNNING daemon: drain, exit 76, one restart (S-8 version-skew guard)"
    local oldpid mark out rc
    put_wheels
    vsh "tc qdisc del dev lo root >/dev/null 2>&1; systemctl stop $UNIT >/dev/null 2>&1; systemctl reset-failed $UNIT >/dev/null 2>&1; pkill -9 -f '^(/[^ ]*/)?zfs (receive|send)' >/dev/null 2>&1; true" || true
    out="$(pip_venv "$NEW_GUEST" reinstall)"; rc=$?
    check "precondition: version N ($CUR_VER) is (re)installed in the venv" "0 $CUR_VER" "$rc $(venv_version)"
    check "the SHIPPED unit has RestartSec=60 and RestartForceExitStatus naming 76" "1min yes" \
        "$(sd_prop RestartUSec) $(vsh "systemctl show -p RestartForceExitStatus --value $UNIT | grep -q 76 && echo yes || echo no" | tr -d '[:space:]')"
    put_text /etc/systemd/system/zfsbackup.service.d/zz-zbinst-skew.conf 644 <<'DROPEOF'
[Service]
RestartSec=15
DROPEOF
    vsh "systemctl daemon-reload" || die "daemon-reload failed"
    check "for the test only: a drop-in shortens RestartSec to 15 s" "15s" "$(sd_prop RestartUSec)"
    vsh "systemctl start $UNIT >$G/start.out 2>&1"; rc=$?
    check "systemctl start returns after READY (N running)" "0 active" "$rc $(sd_prop ActiveState)"
    [ "$rc" -eq 0 ] || vsh "cat $G/start.out; journalctl -u $UNIT -n 15 --no-pager -o cat" | sed 's/^/    start: /'
    sleep 5
    check "the daemon runs N: zfsbackupd --version" "$CUR_VER" "$(venv_version)"

    # ---- cycle 1: N -> N+1, idle daemon
    oldpid="$(sd_prop MainPID)"; mark=$(( $(jl_epoch) - 1 ))
    out="$(pip_venv "$BUMP_GUEST")"; rc=$?
    check "C1 ($CUR_VER -> $BUMP_VER): pip install of N+1 over the running daemon succeeds" "0" "$rc"
    [ "$rc" -eq 0 ] || printf '%s\n' "$out" | sed 's/^/    pip: /'
    wait_guest 90 "journalctl -u $UNIT -o cat --no-pager --since @$mark | grep -q '$SKEW_WARN'" \
        && pass "C1: the drain WARNING appeared within 90 s (two reads, 10 s apart)" || fail "C1: no drain WARNING within 90 s"
    local w; w="$(jl --since @$mark | grep -- "$SKEW_WARN")"
    check_re "C1: the WARNING names the change (\"$CUR_VER -> $BUMP_VER\") and \"will exit 76\"" "$CUR_VER -> $BUMP_VER.*will exit 76" "$w"
    check_re "C1: ... and is stored with PRIORITY 4 (warning)" $'^4\t.*changed on disk' \
        "$(vsh "journalctl -u $UNIT -o json --no-pager --since @$mark | python3 $G/jprio.py 'changed on disk'")"
    await_exit76_restart "$oldpid" "$mark" "C1"
    after_restart_clean "$oldpid" "$mark" "C1" "$BUMP_VER"

    # ---- cycle 2: the same version reinstalled
    oldpid="$(sd_prop MainPID)"; mark=$(( $(jl_epoch) - 1 ))
    out="$(pip_venv "$BUMP_GUEST" reinstall)"; rc=$?
    check "C2 (same version $BUMP_VER): a forced reinstall succeeds" "0" "$rc"
    wait_guest 90 "journalctl -u $UNIT -o cat --no-pager --since @$mark | grep -q '$SKEW_WARN'" \
        && pass "C2: the drain WARNING appeared" || fail "C2: a same-version reinstall was not detected within 90 s"
    check_re "C2: the WARNING says the version was reinstalled" "$BUMP_VER was reinstalled" "$(jl --since @$mark | grep -- "$SKEW_WARN")"
    await_exit76_restart "$oldpid" "$mark" "C2"
    after_restart_clean "$oldpid" "$mark" "C2" "$BUMP_VER"

    # ---- cycle 2b: the library alone, reinstalled (the guard watches the libzfseasy distribution too)
    if [ -n "$BUMP_LIB_GUEST" ]; then
        oldpid="$(sd_prop MainPID)"; mark=$(( $(jl_epoch) - 1 ))
        out="$(pip_venv "$BUMP_LIB_GUEST" reinstall)"; rc=$?
        check "C2b (libzfseasy $BUMP_VER only): a forced reinstall of the library wheel alone succeeds" "0" "$rc"
        wait_guest 90 "journalctl -u $UNIT -o cat --no-pager --since @$mark | grep -q '$SKEW_WARN'" \
            && pass "C2b: the drain WARNING appeared" || fail "C2b: a reinstall of libzfseasy alone was not detected within 90 s"
        check_re "C2b: the WARNING names libzfseasy" "libzfseasy $BUMP_VER was reinstalled" "$(jl --since @$mark | grep -- "$SKEW_WARN")"
        await_exit76_restart "$oldpid" "$mark" "C2b"
        after_restart_clean "$oldpid" "$mark" "C2b" "$BUMP_VER"
    else
        info "C2b skipped: this tree has a single wheel (nothing separate to reinstall)"
    fi

    # ---- cycle 3: the drain waits for a transfer in flight
    SDS="$POOL/backups/$CID/data"
    oldpid="$(sd_prop MainPID)"; mark=$(( $(jl_epoch) - 1 ))
    if slow_transfer c3; then
        local snap; snap="$(vsh "zfs list -H -t snapshot -o name -s createtxg '$POOL/data' | tail -n1" | tr -d '[:space:]')"; snap="${snap#*@}"
        out="$(pip_venv "$BUMP_GUEST" reinstall)"
        wait_guest 90 "journalctl -u $UNIT -o cat --no-pager --since @$mark | grep -q '$SKEW_WARN'" \
            && pass "C3: the drain WARNING appeared during the throttled transfer" || fail "C3: no drain WARNING"
        sleep 20
        check "C3: in the drain, with the transfer still running: the daemon did NOT exit (same supervisor, active)" "active $oldpid" "$(sd_prop ActiveState) $(sd_prop MainPID)"
        check "C3: ... no exit 76 yet" "0" "$(n_in_journal "$mark" 'status=76')"
        check "C3: ... the transfer is still in flight (its zfs receive runs, < 250 MB read)" "yes" "$(rx_still_running)"
        check "C3: the pid file carries draining=" "1" "$(vsh "grep -c '^draining=' $DB.daemon.pid" | tr -d '[:space:]')"
        check_re "C3: StatusText says the daemon is draining" "draining" "$(sd_prop StatusText)"
        vsh "curl -fsSk --connect-timeout 3 --max-time 6 -o $G/health.out https://127.0.0.1:$API_PORT/health" \
            && pass "C3: the API worker still answers /health (it serves until the remote worker has left)" || fail "C3: /health does not answer during the drain"
        unthrottle     # the rest of the 300 MB now goes at full speed; the drain may end only after it
        await_exit76_restart "$oldpid" "$mark" "C3"
        check "C3: the transfer ran to its end: the snapshot $snap is on the server" "yes" \
            "$(vsh "zfs list -H -o name '$SDS@$snap' >/dev/null 2>&1 && echo yes || echo no" | tr -d '[:space:]')"
        after_restart_clean "$oldpid" "$mark" "C3" "$BUMP_VER"
    fi

    # ---- cycle 4: systemctl stop during a drain is an ordinary stop
    oldpid="$(sd_prop MainPID)"; mark=$(( $(jl_epoch) - 1 ))
    if slow_transfer c4; then
        out="$(pip_venv "$BUMP_GUEST" reinstall)"
        wait_guest 90 "journalctl -u $UNIT -o cat --no-pager --since @$mark | grep -q '$SKEW_WARN'" \
            && pass "C4: the drain WARNING appeared during the throttled transfer" || fail "C4: no drain WARNING"
        wait_guest 20 "grep -q '^draining=' $DB.daemon.pid" || true
        check "C4: draining (pid file draining=), the daemon still up" "1 active" \
            "$(vsh "grep -c '^draining=' $DB.daemon.pid" | tr -d '[:space:]') $(sd_prop ActiveState)"
        local t0=$SECONDS
        vsh "systemctl stop $UNIT"; rc=$?
        info "C4: the stop took $((SECONDS - t0))s"
        check "C4: \`systemctl stop\` during the drain exits 0" "0" "$rc"
        check "C4: inactive, Result=success, ExecMainStatus=0 (the ordinary exit code, not 76)" "inactive success 0" \
            "$(sd_prop ActiveState) $(sd_prop Result) $(sd_prop ExecMainStatus)"
        check "C4: the journal has no exit 76 for this run" "0" "$(n_in_journal "$mark" 'status=76')"
        sleep 25
        check "C4: 25 s later (RestartSec=15) no restart followed: still inactive, no restart scheduled" "inactive 0" \
            "$(sd_prop ActiveState) $(n_in_journal "$mark" 'Scheduled restart job')"
        unthrottle
        vsh "pkill -9 -f '^(/[^ ]*/)?zfs (receive|send)' >/dev/null 2>&1; true" || true
    fi

    # ---- put N back and remove the test drop-in
    vsh "
        tc qdisc del dev lo root >/dev/null 2>&1
        systemctl stop $UNIT >/dev/null 2>&1
        rm -f /etc/systemd/system/zfsbackup.service.d/zz-zbinst-skew.conf
        systemctl daemon-reload; systemctl reset-failed $UNIT >/dev/null 2>&1
        true
    " || true
    out="$(pip_venv "$NEW_GUEST" reinstall)"
    check "the venv is back on N ($CUR_VER)" "$CUR_VER" "$(venv_version)"
    section_end
}

run_vm() { # run_vm <vm> <ubuntu release> <tag>
    VM="$1"; TAG="$3"
    echo; echo "${BOLD}=================================================================${NC}"
    echo "${BOLD} $VM (Ubuntu $2)${NC}"
    echo "${BOLD}=================================================================${NC}"
    ensure_vm "$VM" "$2"
    vsh "mkdir -p $G" || die "cannot reach $VM"
    provision
    install_files
    start_before_import
    do_import
    check_no_daemon
    enable_now
    do_reload
    check_warn
    journal_priorities
    enroll_and_receive
    check_ok
    backup_cmd
    clean_stop
    restart_policy
    [ "$INPLACE" -eq 1 ] && inplace_upgrade
    [ "$UPGRADE" -eq 1 ] && upgrade
    if [ "$KEEP" -eq 0 ]; then
        vsh "{ systemctl stop $UNIT zbinst-old zfsbackup-check.timer; systemctl disable $UNIT; } >/dev/null 2>&1; true" || true
    fi
}

[ "$DO22" -eq 1 ] && run_vm "$VM22" 22.04 inst22
[ "$DO24" -eq 1 ] && run_vm "$VM24" 24.04 inst24

# ─────────────────────────────── Report ─────────────────────────────────────
echo
echo "${BOLD}=================================================================${NC}"
echo "${BOLD} Results${NC}"
echo "${BOLD}=================================================================${NC}"
for r in "${RESULTS[@]}"; do echo "  $r"; done
echo "-----------------------------------------------------------------"
echo "  ${GREEN}${PASS_COUNT} passed${NC}, ${RED}${FAIL_COUNT} failed${NC}"
if [ "${#FINDINGS[@]}" -gt 0 ]; then
    echo
    echo "${BOLD}Findings (documentation / packaging defects, not counted as failures)${NC}"
    for f in "${FINDINGS[@]}"; do echo "  - $f"; done
fi
echo
echo "${BOLD}Timings${NC}"
for t in "${TIMINGS[@]}"; do echo "  $t"; done

# ─────────────────────────────── Teardown ───────────────────────────────────
echo
if [ "$KEEP" -eq 1 ]; then
    step "Leaving the install in place (--keep). Inspect with:"
    echo "    multipass exec <vm> -- sudo systemctl status $UNIT"
    echo "    multipass exec <vm> -- sudo journalctl -u $UNIT -n 50"
else
    step "Tearing down (the install state is reset at the start of the next run; the pool is destroyed)"
    for _vm in "$VM22" "$VM24"; do
        { [ "$_vm" = "$VM22" ] && [ "$DO22" -eq 1 ]; } || { [ "$_vm" = "$VM24" ] && [ "$DO24" -eq 1 ]; } || continue
        VM="$_vm"; reset_install
        vsh "{ zpool destroy -f '$POOL'; rm -f '$IMG'; } >/dev/null 2>&1; true" || true
    done
fi
if [ "$PURGE" -eq 1 ]; then
    info "deleting $VM22 and $VM24 (--purge; these two only, never a global purge)"
    for _vm in "$VM22" "$VM24"; do vm_exists "$_vm" && multipass delete --purge "$_vm"; done
fi

[ "$FAIL_COUNT" -eq 0 ] && exit 0 || exit 1
