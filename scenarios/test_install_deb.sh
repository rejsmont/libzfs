#!/usr/bin/env bash
#
# test_install_deb.sh -- prove that the DEBIAN PACKAGES (debian/: python3-libzfseasy + zfsbackup, docs/packaging_plan.md section 2)
# install, upgrade, retrigger, purge and downgrade correctly on real systemd, on FRESH Multipass VMs of the two target distributions:
#   zfsb-pkg2604  Ubuntu 26.04   (container image ubuntu:26.04 builds the packages; ZFS is in main, the module ships with the kernel)
#   zfsb-pkg13    Debian 13      (a cloud image by URL, as scenarios/test_openzfs_matrix.sh does; ZFS is in `contrib`, as DKMS source)
# (test_install.sh is the wheel/virtualenv install on Ubuntu 22.04/24.04; this script is the .deb counterpart.)
#
# Building. The packages are built on the HOST's existing Linux build VM (BUILD_VM, default zfsb-deb: Ubuntu 26.04 with docker.io), in a
# `docker run` of the distribution's own image (debian:13 / ubuntu:26.04), the way .github/workflows/deb.yml does:
# `apt-get build-dep` then `dpkg-buildpackage -b -us -uc`. The source is the WORKING TREE as it is (debian/ may be untracked;
# nothing is committed, stashed or modified) or SRC_REF=<commit-ish>. Two builds per distribution:
#   cur   the tree as it is                                   (version X, e.g. 1.0.0~rc1-1, Python 1.0.0rc1)
#   bump  a COPY with a new debian/changelog entry X+test1 and the Python version (both pyproject.toml files and the libzfseasy== pin)
#         raised to <py>+test1, so `--version` visibly changes. The repository's debian/changelog is never touched.
# Only the build VM and the two VMs below are touched, nothing on the host's ZFS, never `foam` or `zfs-test`.
#
# Checks (per distribution; numbered assertions in the report). The VM is FRESH (an existing VM of that name is deleted first):
#   P0  provisioning: the OS, the kernel, systemd; (Debian) contrib, kernel headers + dkms + zfs-dkms; no zfsbackup and no zfs userland
#       installed beforehand
#   P1  `apt install ./python3-libzfseasy_*.deb ./zfsbackup_*.deb` (the current build) resolves from the archive: exit 0, zfsutils-linux
#       is pulled in, both packages are "ii", and `apt-get check` / `dpkg --audit` are clean
#   P2  after the install zfsbackup.service and zfsbackup-check.timer are neither ENABLED nor ACTIVE (neither is zfsbackup-check.service)
#   P3  the sysusers group `zfsbackup` exists (sysusers creates the group only, no user); /var/lib/zfsbackup is 2770 root:zfsbackup and
#       backups/ is 0700 root:root (tmpfiles); the units are in /usr/lib/systemd/system and none refers to /opt
#   P4  zfsbackupd and zfsbackup-config are in /usr/bin and their `--version` prints `<name> <py version> (wire protocol N, config
#       schema <head>)` with the protocol of the source tree; the modules come from /usr/lib/python3/dist-packages
#   P5  a file-backed pool is created (zbdeb), /usr/share/doc/zfsbackup/examples/config.example.yaml is adjusted (pool name, 1 m check
#       interval) and imported into /var/lib/zfsbackup/config.db, `--test-config` passes, `systemctl enable --now zfsbackup` reaches
#       active (Type=notify-reload, StatusText "generation N"), and one snapshot cycle happens (managed snapshots appear); zfsbackup-check.service, run once,
#       writes /var/lib/prometheus/node-exporter/zfsbackup.prom (0644, with zfsbackup_check_success)
#   P6  UPGRADE under a running daemon: `apt install ./new debs` -> the new preinst stops the daemon cleanly (journal: stopped, exit 0, no
#       status=76 drain, no SIGKILL of a leftover process, no traceback), the new postinst starts it: PID changed, active, enabled still, NRestarts 0, `--version` is the
#       new version, config.db unchanged (export hash) and every earlier snapshot still there
#   P7  the dpkg trigger: `apt-get install --reinstall python3-idna` (a watched dependency) with the service active restarts it (the PID
#       changes, active again); a reinstall of an unwatched package does not; with the service STOPPED a reinstall leaves it stopped
#   P8  `apt purge zfsbackup python3-libzfseasy` of a RUNNING, enabled daemon: stopped, units/binaries/modules/doc gone, the enable
#       symlink and deb-systemd-helper state gone, no process left; /var/lib/zfsbackup keeps config.db and its .tls directory (the
#       postrm says so on stderr); the GROUP zfsbackup is kept (the postrm tells the operator to `groupdel` it by hand; there is no
#       user); the config database still opens and has the schema head
#   P9  the runbook's disable-then-downgrade: install the new build again, `enable --now`, then `systemctl disable --now zfsbackup
#       zfsbackup-check.timer` and `apt install --allow-downgrades ./old debs`: the service stays stopped and disabled, `--version` is
#       the old one, the units were not re-enabled; then `enable --now` on the old build works
#
# Usage (from the repo root; needs multipass and git on the host, no sudo, no poetry):
#   ./scenarios/test_install_deb.sh                  # both distributions (~25-35 min, mostly the Debian DKMS build and the two builds)
#   ./scenarios/test_install_deb.sh --only-2604      # Ubuntu 26.04 only    (--only-13: Debian 13 only)
#   ./scenarios/test_install_deb.sh --keep           # do not delete the VMs at the end (the next run deletes and recreates them)
#   SRC_REF=<commit-ish> ./scenarios/test_install_deb.sh   # build this commit instead of the working tree (it must contain debian/)
#   DEB_OUT=<dir> ./scenarios/test_install_deb.sh    # the built packages go to <dir>/<ubuntu2604|debian13>/{cur,bump}/*.deb and are
#                                                    # REUSED, not rebuilt, when already there (default: a temp dir, removed at the end)
#   Env: BUILD_VM (default zfsb-deb; must start with zfsb-), PKG2604_VM / PKG13_VM (must start with zfsb-pkg), VM_CPUS / VM_MEM /
#   VM_DISK (default 2 / 3G / 10G), DEBIAN13_IMAGE (a URL or file:// of a Debian 13 cloud image; default the arm64 or amd64 generic cloud
#   image matching the host).
#
# Multipass caveats are the usual ones (scenarios/README.md): never send a `multipass exec`'s host stdout to /dev/null, deliver
# files with `multipass transfer`, and a guest command that exits 4 hangs the exec (every command runs in an inner shell that maps
# 4 to 104).

set -uo pipefail

# ─────────────────────────────── Configuration ──────────────────────────────
BUILD_VM="${BUILD_VM:-zfsb-deb}"
VM2604="${PKG2604_VM:-zfsb-pkg2604}"
VM13="${PKG13_VM:-zfsb-pkg13}"
case "$BUILD_VM" in zfsb-*) ;; *) echo "refusing BUILD_VM '$BUILD_VM': only zfsb-* VMs are touched" >&2; exit 2 ;; esac
for _vm in "$VM2604" "$VM13"; do
    case "$_vm" in zfsb-pkg*) ;; *) echo "refusing VM name '$_vm': this scenario only creates and deletes zfsb-pkg* VMs" >&2; exit 2 ;; esac
done

case "$(uname -m)" in
    arm64|aarch64) _arch=arm64 ;;
    *) _arch=amd64 ;;
esac
DEBIAN13_IMAGE="${DEBIAN13_IMAGE:-https://cloud.debian.org/images/cloud/trixie/latest/debian-13-genericcloud-$_arch.qcow2}"
SRC_REF="${SRC_REF:-WORKTREE}"
VM_CPUS="${VM_CPUS:-2}"; VM_MEM="${VM_MEM:-3G}"; VM_DISK="${VM_DISK:-10G}"

POOL="zbdeb"; IMG="/var/tmp/zbdeb.img"
CFGDIR="/var/lib/zfsbackup"; DB="$CFGDIR/config.db"
G="/root/zbdeb"                              # guest scratch: config files, logs, helpers
D="/var/tmp/zbdeb"                           # the .debs (readable by apt's _apt user: no unsandboxed-download warning)
UNIT="zfsbackup"
EXAMPLE="/usr/share/doc/zfsbackup/examples/config.example.yaml"

DO2604=1; DO13=1; KEEP=0
for arg in "$@"; do
    case "$arg" in
        --only-2604) DO13=0 ;;
        --only-13) DO2604=0 ;;
        --keep) KEEP=1 ;;
        -h|--help) grep '^#' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *) echo "Unknown option: $arg" >&2; exit 2 ;;
    esac
done
[ "$DO2604" -eq 1 ] || [ "$DO13" -eq 1 ] || { echo "--only-2604 and --only-13 exclude each other" >&2; exit 2; }

# ─────────────────────────────── Output helpers ─────────────────────────────
if [ -t 1 ]; then
    RED=$'\033[0;31m'; GREEN=$'\033[0;32m'; YELLOW=$'\033[0;33m'; BLUE=$'\033[0;34m'; BOLD=$'\033[1m'; NC=$'\033[0m'
else
    RED=; GREEN=; YELLOW=; BLUE=; BOLD=; NC=
fi
PASS_COUNT=0; FAIL_COUNT=0
declare -a RESULTS=() FINDINGS=() TIMINGS=()
TAG=""                                       # "ubuntu2604" / "debian13": prefixes every assertion

info()  { echo "${BLUE}==>${NC} $*"; }
step()  { echo "${BOLD}${BLUE}::${NC} ${BOLD}$*${NC}"; }
warn()  { echo "${YELLOW}WARN:${NC} $*" >&2; }
die()   { echo "${RED}FATAL:${NC} $*" >&2; exit 1; }
pass()  { PASS_COUNT=$((PASS_COUNT+1)); RESULTS+=("${GREEN}PASS${NC}  [$TAG] $1"); echo "  ${GREEN}✓ PASS${NC} $1"; }
fail()  { FAIL_COUNT=$((FAIL_COUNT+1)); RESULTS+=("${RED}FAIL${NC}  [$TAG] $1"); echo "  ${RED}✗ FAIL${NC} $1"; }
finding() { FINDINGS+=("[$TAG] $1"); echo "  ${YELLOW}! FINDING${NC} $1"; }
check() { # check "<description>" <expected> <actual>
    if [ "$2" = "$3" ]; then pass "$1 ($3)"; else fail "$1 (expected='$2' actual='$3')"; fi
}
check_ne() { # check_ne "<description>" <not this> <actual>
    if [ "$2" != "$3" ]; then pass "$1 ($2 -> $3)"; else fail "$1 (unchanged: '$3')"; fi
}
check_re() { # check_re "<description>" "<ERE>" "<text>"
    if grep -qE -- "$2" <<<"$3"; then pass "$1"; else
        fail "$1 (no match for /$2/ in: $(printf '%s' "$3" | tr '\n' '|' | cut -c1-500))"
    fi
}
check_no_re() { # check_no_re "<description>" "<ERE>" "<text>"
    if grep -qE -- "$2" <<<"$3"; then
        fail "$1 (unexpected /$2/ in: $(printf '%s' "$3" | tr '\n' '|' | cut -c1-500))"
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
vsh()  { multipass exec "$VM" -- sudo bash -c "$REMAP" _ "$*"; }                  # as root in the VM under test
bsh()  { multipass exec "$BUILD_VM" -- bash -c "$REMAP" _ "$*"; }                 # as the login user on the build VM

vm_exists()  { multipass info "$1" >/dev/null 2>&1; }
vm_running() { multipass info "$1" 2>/dev/null | awk -F': *' '/^State/{print $2}' | grep -qi running; }

fresh_vm() { # fresh_vm <name> <image ref> : delete a VM of that name if there is one, then launch a new one
    if vm_exists "$1"; then
        info "deleting the existing VM $1 (this scenario starts from a fresh one)"
        multipass delete --purge "$1" || die "could not delete $1"
    fi
    step "Launching VM $1 ($2)"
    multipass launch "$2" --name "$1" --cpus "$VM_CPUS" --memory "$VM_MEM" --disk "$VM_DISK" || die "failed to launch $1"
}

# put_file <host-file> <guest-path> <mode>  -- via `multipass transfer`, never piped stdin
put_file() {
    local base="/tmp/zbdeb-xfer.$$.$RANDOM"
    multipass transfer "$1" "$VM:$base" || die "transfer of $1 failed"
    vsh "install -D -m '$3' '$base' '$2' && rm -f '$base'" || die "install of $2 failed"
}

# wait_guest <timeout> <guest command>: poll until the command succeeds
wait_guest() {
    local timeout="$1" cmd="$2" waited=0
    until vsh "$cmd"; do
        [ "$waited" -ge "$timeout" ] && return 1
        sleep 2; waited=$((waited+2))
    done
}

# vapt <label> <apt-get arguments...>: apt-get in the VM, output kept in $G/apt-<label>.log (the tail is printed when it fails).
# The return value is apt's. Prints nothing when it succeeds.
vapt() {
    local label="$1" rc; shift
    vsh "mkdir -p $G; DEBIAN_FRONTEND=noninteractive apt-get -y -o DPkg::Lock::Timeout=600 $* > $G/apt-$label.log 2>&1"
    rc=$?
    [ "$rc" -eq 0 ] || { echo "  apt-get $* failed ($rc); tail of the log:"; vsh "tail -n 25 $G/apt-$label.log" | sed 's/^/    | /'; }
    return "$rc"
}
vval() { vsh "$@" 2>&1 | tr -d '\r' | sed -e 's/[[:space:]]*$//'; }      # a command's output, trailing blanks stripped (stderr included)
svc()  { vval "systemctl show -p $2 --value $1"; }                       # svc <unit> <property>
sstate() { vsh "systemctl is-active $1 2>&1; true" | tr -d '\r' | head -n1; }     # active / inactive / failed / activating
senabled() { vsh "systemctl is-enabled $1 2>&1; true" | tr -d '\r' | head -n1; }  # enabled / disabled / not-found / ...

# ─────────────────────────────── Host-side: the source ──────────────────────
command -v multipass >/dev/null 2>&1 || die "multipass not found on the host"
command -v git       >/dev/null 2>&1 || die "git not found on the host"
TOP="$(git -C "$(dirname "$0")" rev-parse --show-toplevel)" || die "not in a git repo"
WORK="$(mktemp -d "${TMPDIR:-/tmp}/zbdeb-work.XXXXXX")" || die "cannot make a scratch directory"
if [ -n "${DEB_OUT:-}" ]; then OUT="$DEB_OUT"; mkdir -p "$OUT" || die "cannot create $OUT"; else OUT="$WORK/out"; fi
trap 'rm -rf "$WORK"' EXIT

step "Packing the source ($SRC_REF)"
if [ "$SRC_REF" = WORKTREE ]; then
    # tracked and untracked (not ignored) files that exist now: a tracked file deleted in the tree is simply left out
    (cd "$TOP" && git ls-files -z -co --exclude-standard) | while IFS= read -r -d '' f; do
        [ -f "$TOP/$f" ] && printf '%s\n' "$f"
    done > "$WORK/files.txt"
    tar -cf "$WORK/src.tar" -C "$TOP" -T "$WORK/files.txt" || die "could not pack the working tree"
else
    git -C "$TOP" archive -o "$WORK/src.tar" "$SRC_REF" || die "git archive of '$SRC_REF' failed"
fi
tar -tf "$WORK/src.tar" | grep -q '^debian/rules$' || die "the source ($SRC_REF) has no debian/rules"
tar -xOf "$WORK/src.tar" debian/changelog > "$WORK/changelog" || die "no debian/changelog in the source"
tar -xOf "$WORK/src.tar" pyproject.toml > "$WORK/pyproject.toml" || die "no pyproject.toml in the source"
tar -xOf "$WORK/src.tar" zfsbackup/protocol.py > "$WORK/protocol.py" || die "no zfsbackup/protocol.py in the source"
CUR_DEB="$(sed -n '1s/^[^(]*(\(.*\)).*/\1/p' "$WORK/changelog")"
CUR_PY="$(sed -n 's/^version = "\(.*\)"$/\1/p' "$WORK/pyproject.toml" | head -n1)"
PROTO="$(sed -n 's/^PROTOCOL_VERSION = \([0-9]*\).*/\1/p' "$WORK/protocol.py" | head -n1)"
[ -n "$CUR_DEB" ] && [ -n "$CUR_PY" ] && [ -n "$PROTO" ] || die "could not read the versions (deb '$CUR_DEB', python '$CUR_PY', protocol '$PROTO')"
case "$CUR_DEB" in *-*) ;; *) die "debian/changelog version '$CUR_DEB' has no revision" ;; esac
BUMP_DEB="${CUR_DEB%-*}+test1-${CUR_DEB##*-}"
BUMP_PY="$CUR_PY+test1"
re_esc() { sed -e 's/[][\.*^$+?(){}|]/\\&/g' <<<"$1"; }
CUR_PY_RE="$(re_esc "$CUR_PY")"; BUMP_PY_RE="$(re_esc "$BUMP_PY")"
info "current: deb $CUR_DEB / python $CUR_PY; bump: deb $BUMP_DEB / python $BUMP_PY; wire protocol $PROTO"

# ─────────────────────────────── Host-side: the builds ──────────────────────
# build_debs <tag> <docker image>: leaves $OUT/<tag>/{cur,bump}/*.deb. Builds in a container on BUILD_VM.
cat > "$WORK/build.sh" <<'EOF_BUILD'
#!/bin/bash
# runs inside the distribution's container; /b is the bind-mounted work dir with src.tar
set -eu
export DEBIAN_FRONTEND=noninteractive
if [ -f /etc/apt/sources.list.d/debian.sources ]; then
    sed -i 's/^Components: main$/Components: main contrib/' /etc/apt/sources.list.d/debian.sources
fi
apt-get update -qq
apt-get install -y -qq --no-install-recommends build-essential devscripts equivs fakeroot dpkg-dev ca-certificates >/dev/null
mkdir -p /b/cur/src /b/bump/src /b/out/cur /b/out/bump
tar -xf /b/src.tar -C /b/cur/src
tar -xf /b/src.tar -C /b/bump/src

# The COPY becomes version X+test1 (debian/changelog of the copy, never the repository's) and so does the Python distribution, so that
# `zfsbackupd --version` changes with it.
cd /b/bump/src
printf 'zfsbackup (%s) unstable; urgency=medium\n\n  * Test build for scenarios/test_install_deb.sh: not a release.\n\n -- Test Build <test@example.invalid>  %s\n\n' \
    "$BUMP_DEB" "$(date -R)" | cat - debian/changelog > debian/changelog.new && mv debian/changelog.new debian/changelog
sed -i "0,/^version = \"$CUR_PY\"/s//version = \"$BUMP_PY\"/" pyproject.toml libs/libzfseasy/pyproject.toml
sed -i "s/\"libzfseasy==$CUR_PY\"/\"libzfseasy==$BUMP_PY\"/" pyproject.toml
grep -q "^version = \"$BUMP_PY\"" pyproject.toml && grep -q "^version = \"$BUMP_PY\"" libs/libzfseasy/pyproject.toml \
    && grep -q "\"libzfseasy==$BUMP_PY\"" pyproject.toml || { echo "could not bump the Python versions in the copy" >&2; exit 1; }

for v in cur bump; do
    cd /b/$v/src
    if [ "$v" = cur ]; then apt-get build-dep -y -qq --no-install-recommends ./ >/dev/null; fi
    echo "=== dpkg-buildpackage ($v)"
    dpkg-buildpackage -b -us -uc > /b/$v-build.log 2>&1 || { tail -n 40 /b/$v-build.log >&2; exit 1; }
    mv /b/$v/*.deb /b/out/$v/
done
chown -R "$HOST_UID:$HOST_GID" /b
ls -l /b/out/cur /b/out/bump
EOF_BUILD

build_debs() {
    local tag="$1" image="$2" home bdir f t0=$SECONDS
    if ls "$OUT/$tag/cur/"zfsbackup_*.deb "$OUT/$tag/bump/"zfsbackup_*.deb >/dev/null 2>&1; then
        info "reusing the packages in $OUT/$tag"; return 0
    fi
    step "Building the packages for $tag in $image on $BUILD_VM"
    vm_running "$BUILD_VM" || { vm_exists "$BUILD_VM" && multipass start "$BUILD_VM" || die "the build VM $BUILD_VM does not exist"; }
    # A VM that was suspended with the host keeps a stale clock, and apt (sqv) then refuses signatures "created after" it
    # ("The repository ... is not signed"): set the build VM's clock from the host's.
    bsh "sudo date -u -s @$(date +%s) >/dev/null 2>&1; true"
    home="$(bsh 'echo $HOME' | tr -d '\r')"
    bdir="$home/zbdeb-build-$tag"
    bsh "rm -rf '$bdir'; mkdir -p '$bdir'" || die "cannot make $bdir on $BUILD_VM"
    multipass transfer "$WORK/src.tar" "$BUILD_VM:$bdir/src.tar" || die "transfer of the source failed"
    multipass transfer "$WORK/build.sh" "$BUILD_VM:$bdir/build.sh" || die "transfer of build.sh failed"
    bsh "docker run --rm -v '$bdir:/b' -e BUMP_DEB='$BUMP_DEB' -e CUR_PY='$CUR_PY' -e BUMP_PY='$BUMP_PY' \
         -e HOST_UID=\$(id -u) -e HOST_GID=\$(id -g) '$image' bash /b/build.sh > '$bdir/docker.log' 2>&1" \
        || { bsh "tail -n 60 '$bdir/docker.log'" | sed 's/^/    build: /' >&2; die "the build of $tag failed (log: $BUILD_VM:$bdir/docker.log)"; }
    mkdir -p "$OUT/$tag/cur" "$OUT/$tag/bump"
    for v in cur bump; do
        for f in $(bsh "ls '$bdir/out/$v'" | tr -d '\r'); do
            multipass transfer "$BUILD_VM:$bdir/out/$v/$f" "$OUT/$tag/$v/$f" || die "could not fetch $f"
        done
    done
    bsh "rm -rf '$bdir'" || true
    info "built $tag in $((SECONDS - t0))s: $(ls "$OUT/$tag/cur" "$OUT/$tag/bump" | tr '\n' ' ')"
}

# ─────────────────────────────── Guest helper files ─────────────────────────
put_helpers() {
    local f
    cat > "$WORK/dbrev.py" <<'PYEOF'
import sqlite3, sys
c = sqlite3.connect("file:" + sys.argv[1] + "?mode=ro", uri=True)
print(c.execute("select version_num from alembic_version").fetchone()[0])
PYEOF
    for f in dbrev.py; do put_file "$WORK/$f" "$G/$f" 644; done
}

# ─────────────────────────────── One distribution ───────────────────────────
dpkg_version() { vval "dpkg-query -W -f='\${Version}' $1 2>/dev/null"; }

run_distro() { # run_distro <tag> <vm> <image ref>
    local tag="$1" image="$3" dir debs_cur debs_bump
    TAG="$tag"; VM="$2"
    dir="$OUT/$tag"
    local cur_ver_out bump_ver_out

    section "P0 provisioning ($tag)"
    fresh_vm "$VM" "$image"
    vsh "cloud-init status --wait" || true
    check_re "the guest is the expected distribution" "$( [ "$tag" = ubuntu2604 ] && echo '^ID=ubuntu VERSION_ID=26.04$' || echo '^ID=debian VERSION_ID=13$')" \
        "$(vval ". /etc/os-release; echo ID=\$ID VERSION_ID=\$VERSION_ID")"
    info "kernel $(vval 'uname -r'), $(vval 'systemctl --version | head -n1'), $(vval 'dpkg --print-architecture')"
    vsh "mkdir -p $G" || die "cannot make $G in the VM"
    for f in "$dir"/cur/*.deb;  do put_file "$f" "$D/cur/$(basename "$f")" 644; done
    for f in "$dir"/bump/*.deb; do put_file "$f" "$D/bump/$(basename "$f")" 644; done
    put_helpers
    if [ "$tag" = debian13 ]; then
        # ZFS is in `contrib`, as DKMS source: the kernel module is built against the running kernel's headers. This is the host's
        # prerequisite (the module), not part of the package under test; the userland (zfsutils-linux) is left to check P1.
        vsh "sed -i 's/^Components: main\$/Components: main contrib/' /etc/apt/sources.list.d/debian.sources; grep -E '^Components:' /etc/apt/sources.list.d/debian.sources" | sed 's/^/    | /'
        vapt update update || die "apt-get update failed"
        # --no-install-recommends: zfs-dkms must not drag zfsutils-linux in, P1 shows the package doing that
        vapt dkms install --no-install-recommends "linux-headers-\$(uname -r)" dkms zfs-dkms || die "could not install zfs-dkms"
        vsh "modprobe zfs" && pass "the zfs kernel module (DKMS) loads" || fail "the zfs kernel module (DKMS) does not load"
    else
        vapt update update || die "apt-get update failed"
    fi
    check "no zfsbackup or libzfseasy package is installed before the test" "" "$(vval "dpkg-query -W -f='\${Package} \${Status}\n' zfsbackup python3-libzfseasy 2>/dev/null | grep 'install ok installed'")"
    check "zfsutils-linux is NOT installed before P1 (the package has to pull it in)" "" "$(vval "dpkg-query -W -f='\${db:Status-Abbrev}' zfsutils-linux 2>/dev/null | grep '^ii' || true")"
    section_end

    # ── P1 ──
    section "P1 apt install of the .debs resolves from the archive ($tag)"
    vapt install-cur install "$D/cur/python3-libzfseasy_*.deb" "$D/cur/zfsbackup_*.deb"
    check "apt install ./python3-libzfseasy_*.deb ./zfsbackup_*.deb exits 0" 0 $?
    check "python3-libzfseasy is installed at the current version" "$CUR_DEB" "$(dpkg_version python3-libzfseasy)"
    check "zfsbackup is installed at the current version" "$CUR_DEB" "$(dpkg_version zfsbackup)"
    check "dpkg status of both" "ii ii" "$(vval "dpkg-query -W -f='\${db:Status-Abbrev}\n' python3-libzfseasy zfsbackup | tr -d ' ' | tr '\n' ' '")"
    check "zfsutils-linux was pulled in from the archive" "ii" "$(vval "dpkg-query -W -f='\${db:Status-Abbrev}' zfsutils-linux | tr -d ' '")"
    check_re "the install log shows zfsutils-linux fetched from the archive" "Get:[0-9]+ .*zfsutils-linux" "$(vval "grep -E '^Get:' $G/apt-install-cur.log")"
    check "apt-get check is clean" 0 "$(vsh "apt-get check >/dev/null 2>&1"; echo $?)"
    check "dpkg --audit is clean" "" "$(vval "dpkg --audit")"
    vsh "modprobe zfs" || true
    section_end

    # ── P2 ──
    section "P2 nothing is enabled or started by the install ($tag)"
    check "zfsbackup.service is not enabled" "disabled" "$(senabled zfsbackup.service)"
    check "zfsbackup-check.timer is not enabled" "disabled" "$(senabled zfsbackup-check.timer)"
    check "zfsbackup.service is not active" "inactive" "$(sstate zfsbackup.service)"
    check "zfsbackup-check.timer is not active" "inactive" "$(sstate zfsbackup-check.timer)"
    check "zfsbackup-check.service is not active" "inactive" "$(sstate zfsbackup-check.service)"
    check "no zfsbackupd process" "" "$(vval "pgrep -f '[z]fsbackupd' || true")"
    check "no enable symlink under /etc/systemd/system" "" "$(vval "ls /etc/systemd/system/*.wants/ 2>/dev/null | grep -i zfsbackup || true")"
    section_end

    # ── P3 ──
    section "P3 sysusers and tmpfiles ($tag)"
    check_re "the group zfsbackup exists" "^zfsbackup:" "$(vval "getent group zfsbackup")"
    check "sysusers creates a group only (no user zfsbackup)" "" "$(vval "getent passwd zfsbackup || true")"
    check "/var/lib/zfsbackup is 2770 root:zfsbackup" "2770 root zfsbackup" "$(vval "stat -c '%a %U %G' $CFGDIR")"
    check "/var/lib/zfsbackup/backups is 700 root:root" "700 root root" "$(vval "stat -c '%a %U %G' $CFGDIR/backups")"
    check "units are in /usr/lib/systemd/system" "zfsbackup-check.service zfsbackup-check.timer zfsbackup.service" \
        "$(vval "cd /usr/lib/systemd/system && ls zfsbackup* | tr '\n' ' '")"
    check "no shipped unit refers to /opt" "" "$(vval "grep -hv '^#' /usr/lib/systemd/system/zfsbackup*.service /usr/lib/systemd/system/zfsbackup*.timer | grep /opt/ || true")"
    check "the unit is Type=notify-reload" "notify-reload" "$(svc zfsbackup.service Type)"
    check_re "ExecStart is /usr/bin/zfsbackupd" "path=/usr/bin/zfsbackupd" "$(svc zfsbackup.service ExecStart)"
    section_end

    # ── P4 ──
    section "P4 console scripts ($tag)"
    local head
    check "zfsbackupd is /usr/bin/zfsbackupd" "/usr/bin/zfsbackupd" "$(vval 'command -v zfsbackupd')"
    check "zfsbackup-config is /usr/bin/zfsbackup-config" "/usr/bin/zfsbackup-config" "$(vval 'command -v zfsbackup-config')"
    cur_ver_out="$(vval "zfsbackupd --version")"
    check_re "zfsbackupd --version: version, wire protocol $PROTO, schema head" "^zfsbackupd $CUR_PY_RE \(wire protocol $PROTO, config schema [0-9a-f]{12}\)\$" "$cur_ver_out"
    check "zfsbackup-config --version prints the same line" "${cur_ver_out/zfsbackupd/zfsbackup-config}" "$(vval "zfsbackup-config --version")"
    head="$(sed -n 's/.*config schema \([0-9a-f]*\)).*/\1/p' <<<"$cur_ver_out")"
    check "the modules come from the distribution's dist-packages" "/usr/lib/python3/dist-packages/libzfseasy/__init__.py /usr/lib/python3/dist-packages/zfsbackup/__init__.py" \
        "$(vval "cd / && python3 -I -c 'import libzfseasy, zfsbackup; print(libzfseasy.__file__, zfsbackup.__file__)'")"
    check "python3-libzfseasy dist-info is the current version" "$CUR_PY" "$(vval "cd / && python3 -I -c 'import importlib.metadata as m; print(m.version(\"libzfseasy\"))'")"
    section_end

    # ── P5 ──
    section "P5 configure, enable --now, one snapshot cycle ($tag)"
    vsh "truncate -s 2G $IMG && zpool create -f -m none -O compression=lz4 $POOL $IMG && for d in ROOT/macos USERDATA/home USERDATA/media SRVDATA/postgres SRVDATA/lxd; do zfs create -p $POOL/\$d; done"
    check "the file-backed pool $POOL is ONLINE" "ONLINE" "$(vval "zpool list -H -o health $POOL")"
    check "the example configuration is installed" "yes" "$(vsh "test -s $EXAMPLE && echo yes || echo no" | tr -d '\r')"
    vsh "sed -e 's/libzfs_test/$POOL/g' -e 's/^check_interval: \"5m\"/check_interval: \"1m\"/' $EXAMPLE > $G/config.yaml"
    check "import into $DB succeeds" 0 "$(vsh "zfsbackup-config -c $DB import $G/config.yaml >$G/import.log 2>&1"; echo $?)"
    check "the database is at the schema head $head" "$head" "$(vval "python3 $G/dbrev.py $DB")"
    check "zfsbackupd --test-config passes" 0 "$(vsh "zfsbackupd --test-config -c $DB >$G/testconfig.log 2>&1"; echo $?)"
    check "the database is group zfsbackup (setgid directory)" "zfsbackup" "$(vval "stat -c %G $DB")"
    vsh "systemctl enable --now zfsbackup >$G/enable.log 2>&1"
    check "systemctl enable --now zfsbackup exits 0" 0 $?
    check "zfsbackup.service is active" "active" "$(sstate zfsbackup.service)"
    check "zfsbackup.service is enabled" "enabled" "$(senabled zfsbackup.service)"
    check_re "StatusText says generation N" "^generation [0-9]+\$" "$(svc zfsbackup.service StatusText)"
    check "the unit has not restarted" 0 "$(svc zfsbackup.service NRestarts)"
    if wait_guest 120 "zfs list -H -t snapshot -o name -r $POOL | grep -q '@autosnap_'"; then pass "a snapshot cycle happened (managed snapshots exist)"; else fail "no managed snapshot appeared within 120 s"; fi
    # one cycle covers every enabled dataset: 4 configured datasets (+ the recursive children, none here)
    sleep 5
    check "managed snapshots exist for the four enabled datasets" 4 "$(vval "zfs list -H -t snapshot -o name -r $POOL | grep -c '@autosnap_'")"
    check "the disabled dataset has none" 0 "$(vval "zfs list -H -t snapshot -o name $POOL/SRVDATA/lxd | grep -c '@autosnap_' || true")"
    # The packaged check unit writes where Debian's prometheus-node-exporter reads (that package creates the directory; it is
    # only Suggested, so the directory is made here, the way the package makes it).
    local prom=/var/lib/prometheus/node-exporter/zfsbackup.prom
    vsh "install -d -m 0755 /var/lib/prometheus/node-exporter; rm -f $prom"
    check "systemctl start zfsbackup-check.service exits 0" 0 "$(vsh "systemctl start zfsbackup-check.service >$G/check-unit.log 2>&1"; echo $?)"
    check "the check wrote $prom (mode 0644)" "644" "$(vval "stat -c %a $prom 2>&1")"
    check_re "the textfile carries zfsbackup_check_success" "^zfsbackup_check_success [01]\$" "$(vval "grep '^zfsbackup_check_success ' $prom")"
    section_end

    # ── P6 ──
    section "P6 upgrade to $BUMP_DEB under the running daemon ($tag)"
    local pid0 pid1 t0 hash0 hash1 snaps0 jr purge_out
    pid0="$(svc zfsbackup.service MainPID)"
    hash0="$(vval "zfsbackup-config -c $DB export | sha256sum | cut -d' ' -f1")"
    snaps0="$(vval "zfs list -H -t snapshot -o name -r $POOL | sort")"
    t0="$(vval 'date +%s')"
    sleep 1
    vapt upgrade install "$D/bump/python3-libzfseasy_*.deb" "$D/bump/zfsbackup_*.deb"
    check "apt install of the +test1 build exits 0" 0 $?
    check "zfsbackup is now $BUMP_DEB" "$BUMP_DEB" "$(dpkg_version zfsbackup)"
    check "python3-libzfseasy is now $BUMP_DEB" "$BUMP_DEB" "$(dpkg_version python3-libzfseasy)"
    wait_guest 60 "systemctl is-active zfsbackup >/dev/null 2>&1" || true
    check "zfsbackup.service is active after the upgrade" "active" "$(sstate zfsbackup.service)"
    pid1="$(svc zfsbackup.service MainPID)"
    check_ne "the daemon PID changed (stopped by the new preinst, started by the new postinst)" "$pid0" "$pid1"
    check "the old daemon process is gone" "" "$(vval "ps -p $pid0 -o pid= || true")"
    check "still enabled" "enabled" "$(senabled zfsbackup.service)"
    check "the unit was stopped and started, not restarted by systemd (NRestarts)" 0 "$(svc zfsbackup.service NRestarts)"
    bump_ver_out="$(vval "zfsbackupd --version")"
    check_re "zfsbackupd --version is the new version" "^zfsbackupd $BUMP_PY_RE \(wire protocol $PROTO, config schema $head\)\$" "$bump_ver_out"
    check_re "zfsbackup-config --version is the new version" "^zfsbackup-config $BUMP_PY_RE " "$(vval "zfsbackup-config --version")"
    hash1="$(vval "zfsbackup-config -c $DB export | sha256sum | cut -d' ' -f1")"
    check "the configuration database is unchanged (export hash)" "$hash0" "$hash1"
    check "every earlier snapshot is still there" "" "$(comm -23 <(printf '%s\n' "$snaps0") <(vval "zfs list -H -t snapshot -o name -r $POOL | sort"))"
    jr="$(vval "journalctl -u zfsbackup.service --since @$t0 --no-pager -o short-iso")"
    check_re "the journal shows the stop" "Stopping zfsbackup.service|Stopping ZFS Backup Daemon" "$jr"
    check_re "the stop was clean (Deactivated successfully)" "zfsbackup.service: Deactivated successfully" "$jr"
    check_re "the new daemon started" "Started zfsbackup.service|Started ZFS Backup Daemon" "$jr"
    check_no_re "no exit 76 drain, no failure, no kill in the journal" "status=76|Failed with result|code=(exited, status=[1-9]|killed|dumped)|timed out|will exit 76|draining" "$jr"
    # Under forkserver (Python 3.14) the supervisor reaps the forkserver itself before it exits; before that, KillMode=mixed
    # SIGKILLed it right after "Daemon stopped" on every stop.
    check_no_re "systemd killed no leftover process on the stop" "Killing process [0-9]+ .* with signal SIGKILL" "$jr"
    check_no_re "no traceback in the journal" "Traceback|Error:|CRITICAL" "$jr"
    section_end

    # ── P7 ──
    section "P7 the dpkg trigger on a watched dependency ($tag)"
    vsh "dpkg -s python3-idna >/dev/null 2>&1" || vapt idna install python3-idna
    pid0="$(svc zfsbackup.service MainPID)"
    vapt trig1 install --reinstall python3-idna
    check "apt-get install --reinstall python3-idna exits 0" 0 $?
    wait_guest 60 "systemctl is-active zfsbackup >/dev/null 2>&1" || true
    check "the service is active after the dependency was reinstalled" "active" "$(sstate zfsbackup.service)"
    pid1="$(svc zfsbackup.service MainPID)"
    check_ne "the trigger try-restarted the daemon (PID changed)" "$pid0" "$pid1"
    check "the try-restart is the package's, not systemd's Restart= (NRestarts)" 0 "$(svc zfsbackup.service NRestarts)"
    pid0="$pid1"
    vapt trig2 install --reinstall ca-certificates
    check "a reinstall of an unwatched package exits 0" 0 $?
    check "an unwatched package does not restart the daemon (PID unchanged)" "$pid0" "$(svc zfsbackup.service MainPID)"
    vsh "systemctl stop zfsbackup" ; check "systemctl stop exits 0" 0 $?
    check "the service is stopped" "inactive" "$(sstate zfsbackup.service)"
    vapt trig3 install --reinstall python3-idna
    check "reinstalling the watched dependency with the daemon stopped exits 0" 0 $?
    sleep 3
    check "with the service stopped, the trigger leaves it stopped" "inactive" "$(sstate zfsbackup.service)"
    check "it is still enabled (only stopped)" "enabled" "$(senabled zfsbackup.service)"
    check "no daemon process was started" "" "$(vval "pgrep -f '[z]fsbackupd' || true")"
    section_end

    # ── P8 ──
    section "P8 apt purge of a running, enabled daemon ($tag)"
    vsh "systemctl start zfsbackup" ; check "systemctl start exits 0" 0 $?
    check "the service is running before the purge" "active" "$(sstate zfsbackup.service)"
    local rev0
    rev0="$(vval "python3 $G/dbrev.py $DB")"
    purge_out="$(vsh "DEBIAN_FRONTEND=noninteractive apt-get -y purge zfsbackup python3-libzfseasy 2>&1; echo PURGE_RC=\$?" | tr -d '\r')"
    check "apt purge exits 0" "PURGE_RC=0" "$(tail -n1 <<<"$purge_out")"
    check_re "the postrm says that /var/lib/zfsbackup was left in place" "/var/lib/zfsbackup .* was left in place" "$purge_out"
    check_re "the postrm tells the operator to remove the group by hand" "groupdel zfsbackup" "$purge_out"
    check "zfsbackup is purged (dpkg status)" "" "$(vval "dpkg-query -W -f='\${db:Status-Abbrev}' zfsbackup 2>/dev/null | tr -d ' ' | grep -v '^\$' || true")"
    check "python3-libzfseasy is purged" "" "$(vval "dpkg-query -W -f='\${db:Status-Abbrev}' python3-libzfseasy 2>/dev/null | tr -d ' ' | grep -v '^\$' || true")"
    check "no zfsbackup process is left" "" "$(vval "pgrep -f '[z]fsbackupd|[z]fsbackup-config' || true")"
    check "the units are gone" "" "$(vval "ls /usr/lib/systemd/system/zfsbackup* /etc/systemd/system/zfsbackup* 2>/dev/null || true")"
    check "the service is unknown to systemd" "not-found" "$(vval "systemctl show -p LoadState --value zfsbackup.service")"
    check "the enable symlink is gone" "" "$(vval "ls /etc/systemd/system/*.wants/ 2>/dev/null | grep -i zfsbackup || true")"
    check "deb-systemd-helper left no state" "" "$(vval "ls /var/lib/systemd/deb-systemd-helper-enabled/ /var/lib/systemd/deb-systemd-helper-masked/ 2>/dev/null | grep -i zfsbackup || true")"
    check "the console scripts are gone" "" "$(vval "ls /usr/bin/zfsbackupd /usr/bin/zfsbackup-config 2>/dev/null || true")"
    check "the Python modules are gone" "" "$(vval "ls -d /usr/lib/python3/dist-packages/zfsbackup* /usr/lib/python3/dist-packages/libzfseasy* 2>/dev/null || true")"
    check "sysusers.d and tmpfiles.d snippets are gone" "" "$(vval "ls /usr/lib/sysusers.d/zfsbackup* /usr/lib/tmpfiles.d/zfsbackup* 2>/dev/null || true")"
    check "/usr/share/doc/zfsbackup is gone" "" "$(vval "ls -d /usr/share/doc/zfsbackup 2>/dev/null || true")"
    check "/var/lib/zfsbackup/config.db is kept" "yes" "$(vsh "test -s $DB && echo yes || echo no" | tr -d '\r')"
    check "/var/lib/zfsbackup/config.db.tls is kept, with the server identity" "server.crt server.key" "$(vval "ls $DB.tls | grep '^server\.' | tr '\n' ' '")"
    check "the configuration database still opens, at the schema head" "$rev0" "$(vval "python3 $G/dbrev.py $DB")"
    check "the daemon's directory and its mode are kept" "2770 root zfsbackup" "$(vval "stat -c '%a %U %G' $CFGDIR")"
    check_re "the group zfsbackup is KEPT (the postrm leaves groupdel to the operator)" "^zfsbackup:" "$(vval "getent group zfsbackup")"
    check "the zfs data is untouched" "yes" "$(vval "zfs list -H -t snapshot -o name -r $POOL | grep -q '@autosnap_' && echo yes || echo no")"
    section_end

    # ── P9 ──
    section "P9 disable, then downgrade per the runbook ($tag)"
    vapt reinstall install "$D/bump/python3-libzfseasy_*.deb" "$D/bump/zfsbackup_*.deb"
    check "a fresh install over the kept /var/lib/zfsbackup exits 0" 0 $?
    check "it is not enabled again by the install" "disabled" "$(senabled zfsbackup.service)"
    check "it is not started by the install" "inactive" "$(sstate zfsbackup.service)"
    vsh "systemctl enable --now zfsbackup >$G/enable2.log 2>&1" ; check "enable --now with the kept database exits 0" 0 $?
    check "the daemon is active on the kept database and TLS directory" "active" "$(sstate zfsbackup.service)"
    check_re "zfsbackupd --version is the new version" "^zfsbackupd $BUMP_PY_RE " "$(vval "zfsbackupd --version")"
    vsh "systemctl enable zfsbackup-check.timer >/dev/null 2>&1; systemctl disable --now zfsbackup zfsbackup-check.timer >$G/disable.log 2>&1"
    check "systemctl disable --now zfsbackup zfsbackup-check.timer exits 0" 0 $?
    check "disabled and stopped before the downgrade" "inactive disabled inactive disabled" \
        "$(sstate zfsbackup.service) $(senabled zfsbackup.service) $(sstate zfsbackup-check.timer) $(senabled zfsbackup-check.timer)"
    vapt downgrade install --allow-downgrades "$D/cur/python3-libzfseasy_*.deb" "$D/cur/zfsbackup_*.deb"
    check "apt install --allow-downgrades of the older build exits 0" 0 $?
    check "zfsbackup is back at the older version" "$CUR_DEB" "$(dpkg_version zfsbackup)"
    check "python3-libzfseasy is back at the older version" "$CUR_DEB" "$(dpkg_version python3-libzfseasy)"
    sleep 3
    check "the service stays stopped through the downgrade" "inactive" "$(sstate zfsbackup.service)"
    check "the service stays disabled" "disabled" "$(senabled zfsbackup.service)"
    check "the check timer stays stopped and disabled" "inactive disabled" "$(sstate zfsbackup-check.timer) $(senabled zfsbackup-check.timer)"
    check "no daemon process was started" "" "$(vval "pgrep -f '[z]fsbackupd' || true")"
    check_re "zfsbackupd --version is the older version" "^zfsbackupd $CUR_PY_RE " "$(vval "zfsbackupd --version")"
    vsh "systemctl enable --now zfsbackup >$G/enable3.log 2>&1" ; check "enable --now of the older build (the runbook's last step) exits 0" 0 $?
    check "the older daemon is active" "active" "$(sstate zfsbackup.service)"
    check "the unit has not restarted" 0 "$(svc zfsbackup.service NRestarts)"
    section_end

    # Anything dpkg/apt/needrestart/maintainer scripts said that looks like a problem: reported, not failed
    local noise
    noise="$(vval "cd $G && grep -HEi '^(W|E):|dpkg: (warning|error)|warning:|error:|traceback|failed' apt-install-cur.log apt-upgrade.log apt-trig*.log apt-reinstall.log apt-downgrade.log 2>/dev/null | grep -Ev 'Download is performed unsandboxed|dpkg: warning: downgrading|rehash: warning' | cut -c1-240 || true")"
    [ -z "$noise" ] || finding "apt/dpkg output worth a look:
$(sed 's/^/        /' <<<"$noise")"
    grep -qE 'warning|error|Traceback|failed' <<<"$purge_out" && finding "apt purge output worth a look: $(grep -E 'warning|error|Traceback|failed' <<<"$purge_out" | head -n3 | tr '\n' '|')"

    if [ "$KEEP" -eq 0 ]; then
        info "deleting $VM"
        multipass delete --purge "$VM" || warn "could not delete $VM"
    else
        info "kept $VM (the next run deletes and recreates it)"
    fi
}

# ─────────────────────────────── Run ────────────────────────────────────────
T_START=$SECONDS
[ "$DO2604" -eq 1 ] && build_debs ubuntu2604 ubuntu:26.04
[ "$DO13" -eq 1 ]   && build_debs debian13 debian:13
[ "$DO2604" -eq 1 ] && run_distro ubuntu2604 "$VM2604" 26.04
[ "$DO13" -eq 1 ]   && run_distro debian13 "$VM13" "$DEBIAN13_IMAGE"

echo; step "Summary"
for r in "${RESULTS[@]}"; do echo "  $r"; done
if [ "${#FINDINGS[@]}" -gt 0 ]; then
    echo; echo "${YELLOW}FINDINGS${NC} (packaging or documentation defects noticed, not counted as failures)"
    for f in "${FINDINGS[@]}"; do echo "  - $f"; done
fi
echo; echo "Section timings:"; for t in "${TIMINGS[@]}"; do echo "  $t"; done
echo; echo "${BOLD}${PASS_COUNT} passed, ${FAIL_COUNT} failed${NC} in $((SECONDS - T_START))s"
[ "$FAIL_COUNT" -eq 0 ]
