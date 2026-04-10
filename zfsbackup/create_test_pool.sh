#!/bin/bash
#
# Create a temp-file backed ZFS pool with nested datasets for testing.
# All datasets use mountpoint=none to avoid cluttering the desktop.
#
# Usage:
#   ./create_test_pool.sh [pool_name]
#
# Default pool name: libzfs_test
#
# To destroy the pool afterwards:
#   zpool destroy <pool_name>

set -euo pipefail

POOL="${1:-libzfs_test}"
TMPFILE="/tmp/zpool_${POOL}.img"

cleanup_on_error() {
    echo "Error: setup failed, cleaning up..."
    zpool destroy "$POOL" 2>/dev/null || true
    rm -f "$TMPFILE"
    exit 1
}
trap cleanup_on_error ERR

if zpool list "$POOL" &>/dev/null; then
    echo "Error: pool '$POOL' already exists — destroy it first:"
    echo "  zpool destroy $POOL && rm -f $TMPFILE"
    exit 1
fi

echo "Creating temp file: $TMPFILE (512 MB)"
truncate -s 512m "$TMPFILE"

echo "Creating ZFS pool: $POOL"
zpool create -O mountpoint=none "$POOL" "$TMPFILE"

echo "Creating datasets..."

# Top-level containers
zfs create -o mountpoint=none "$POOL/ROOT"
zfs create -o mountpoint=none "$POOL/SRVDATA"
zfs create -o mountpoint=none "$POOL/USERDATA"

# ROOT subtree: OS datasets
zfs create -o mountpoint=none "$POOL/ROOT/macos"
zfs create -o mountpoint=none "$POOL/ROOT/macos/system"
zfs create -o mountpoint=none "$POOL/ROOT/macos/data"
zfs create -o mountpoint=none "$POOL/ROOT/nixos"

# SRVDATA subtree: server workloads with deeper nesting
zfs create -o mountpoint=none "$POOL/SRVDATA/lxd"
zfs create -o mountpoint=none "$POOL/SRVDATA/lxd/containers"
zfs create -o mountpoint=none "$POOL/SRVDATA/lxd/containers/web"
zfs create -o mountpoint=none "$POOL/SRVDATA/lxd/containers/db"
zfs create -o mountpoint=none "$POOL/SRVDATA/lxd/images"
zfs create -o mountpoint=none "$POOL/SRVDATA/postgres"
zfs create -o mountpoint=none "$POOL/SRVDATA/postgres/data"
zfs create -o mountpoint=none "$POOL/SRVDATA/postgres/wal"

# USERDATA subtree: user home dirs
zfs create -o mountpoint=none "$POOL/USERDATA/home"
zfs create -o mountpoint=none "$POOL/USERDATA/home/alice"
zfs create -o mountpoint=none "$POOL/USERDATA/home/bob"
zfs create -o mountpoint=none "$POOL/USERDATA/home/bob/projects"
zfs create -o mountpoint=none "$POOL/USERDATA/media"
zfs create -o mountpoint=none "$POOL/USERDATA/media/photos"
zfs create -o mountpoint=none "$POOL/USERDATA/media/videos"

echo ""
echo "Pool '$POOL' ready. Datasets:"
zfs list -r -o name,mountpoint "$POOL"

echo ""
echo "Backing file: $TMPFILE"
echo ""
echo "To destroy:  zpool destroy $POOL && rm -f $TMPFILE"