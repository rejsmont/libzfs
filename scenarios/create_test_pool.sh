#!/bin/bash
#
# Create a temp-file backed ZFS pool with nested datasets for testing.
# All datasets use mountpoint=none to avoid cluttering the desktop.
#
# Usage:
#   ./scenarios/create_test_pool.sh [pool_name]
#
# Default pool name: libzfs_test
#
# To destroy the pool afterwards:
#   zpool destroy <pool_name>

set -euo pipefail

POOL="${1:-libzfs_test}"
TMPFILE="/tmp/zpool_${POOL}.img"
CURRENT_USER="$(id -un)"

# Permissions delegated to the current user on the pool
ZFS_PERMISSIONS="create,destroy,snapshot,clone,rename,mount,bookmark,compression,mountpoint,quota,reservation,volsize,volblocksize"

cleanup_on_error() {
    echo "Error: setup failed, cleaning up..."
    sudo zpool destroy "$POOL" 2>/dev/null || true
    rm -f "$TMPFILE"
    exit 1
}
trap cleanup_on_error ERR

if zpool list "$POOL" &>/dev/null; then
    echo "Error: pool '$POOL' already exists — destroy it first:"
    echo "  sudo zpool destroy $POOL && rm -f $TMPFILE"
    exit 1
fi

echo "Creating temp file: $TMPFILE (512 MB)"
truncate -s 512m "$TMPFILE"

echo "Creating ZFS pool: $POOL (requires sudo)"
sudo zpool create -O mountpoint=none "$POOL" "$TMPFILE"

echo "Granting ZFS permissions to $CURRENT_USER..."
sudo zfs allow -u "$CURRENT_USER" "$ZFS_PERMISSIONS" "$POOL"

echo "Creating datasets (requires sudo — macOS requires root to mount even with delegation)..."

_zc() { sudo zfs create -o mountpoint=none "$1"; }

# Top-level containers
_zc "$POOL/ROOT"
_zc "$POOL/SRVDATA"
_zc "$POOL/USERDATA"

# ROOT subtree: OS datasets
_zc "$POOL/ROOT/macos"
_zc "$POOL/ROOT/macos/system"
_zc "$POOL/ROOT/macos/data"
_zc "$POOL/ROOT/nixos"

# SRVDATA subtree: server workloads with deeper nesting
_zc "$POOL/SRVDATA/lxd"
_zc "$POOL/SRVDATA/lxd/containers"
_zc "$POOL/SRVDATA/lxd/containers/web"
_zc "$POOL/SRVDATA/lxd/containers/db"
_zc "$POOL/SRVDATA/lxd/images"
_zc "$POOL/SRVDATA/postgres"
_zc "$POOL/SRVDATA/postgres/data"
_zc "$POOL/SRVDATA/postgres/wal"

# USERDATA subtree: user home dirs
_zc "$POOL/USERDATA/home"
_zc "$POOL/USERDATA/home/alice"
_zc "$POOL/USERDATA/home/bob"
_zc "$POOL/USERDATA/home/bob/projects"
_zc "$POOL/USERDATA/media"
_zc "$POOL/USERDATA/media/photos"
_zc "$POOL/USERDATA/media/videos"

echo ""
echo "Pool '$POOL' ready. Datasets:"
zfs list -r -o name,mountpoint "$POOL"

echo ""
echo "Delegated permissions for $CURRENT_USER:"
zfs allow "$POOL"
echo ""
echo "Backing file: $TMPFILE"
echo ""
echo "To destroy:  sudo zpool destroy $POOL && rm -f $TMPFILE"