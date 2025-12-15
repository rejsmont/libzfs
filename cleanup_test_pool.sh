#!/bin/bash
# Cleanup script for removing ZFS test pool

set -e

POOL_NAME="${TEST_ZFS_POOL:-testpool}"
DISK_IMAGE="${ZFS_TEST_DISK:-/tmp/zfs-test-disk}"

echo "=========================================="
echo "ZFS Test Pool Cleanup"
echo "=========================================="
echo "Pool name: $POOL_NAME"
echo "Disk image: $DISK_IMAGE"
echo "=========================================="
echo ""

# Check if pool exists
if ! zpool list "$POOL_NAME" &> /dev/null; then
    echo "Pool '$POOL_NAME' does not exist. Nothing to clean up."
    
    # Check if disk image exists without pool
    if [ -f "$DISK_IMAGE" ]; then
        echo ""
        echo "Found orphaned disk image: $DISK_IMAGE"
        read -p "Remove it? (yes/no): " -r
        if [[ $REPLY == "yes" ]]; then
            rm "$DISK_IMAGE"
            echo "Disk image removed."
        fi
    fi
    
    exit 0
fi

echo "Current pool status:"
sudo zpool status "$POOL_NAME"
echo ""

read -p "Destroy pool '$POOL_NAME' and remove disk image? (yes/no): " -r
if [[ $REPLY != "yes" ]]; then
    echo "Aborted."
    exit 0
fi

echo ""
echo "Destroying pool..."
sudo zpool destroy "$POOL_NAME"

if [ -f "$DISK_IMAGE" ]; then
    echo "Removing disk image..."
    rm "$DISK_IMAGE"
fi

echo ""
echo "=========================================="
echo "Cleanup complete!"
echo "=========================================="
