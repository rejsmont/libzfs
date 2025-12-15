#!/bin/bash
# Setup script for creating a ZFS test pool for real integration tests

set -e

POOL_NAME="${TEST_ZFS_POOL:-testpool}"
DISK_IMAGE="${ZFS_TEST_DISK:-/tmp/zfs-test-disk}"
DISK_SIZE="${ZFS_TEST_SIZE:-512}"  # Size in MB

echo "=========================================="
echo "ZFS Test Pool Setup"
echo "=========================================="
echo "Pool name: $POOL_NAME"
echo "Disk image: $DISK_IMAGE"
echo "Disk size: ${DISK_SIZE}MB"
echo "=========================================="
echo ""

# Check if ZFS is installed
if ! command -v zfs &> /dev/null; then
    echo "ERROR: ZFS utilities not found!"
    echo ""
    echo "Install ZFS:"
    if [[ "$OSTYPE" == "darwin"* ]]; then
        echo "  brew install openzfs"
    elif [[ "$OSTYPE" == "linux-gnu"* ]]; then
        if [ -f /etc/debian_version ]; then
            echo "  sudo apt-get install zfsutils-linux"
        elif [ -f /etc/redhat-release ]; then
            echo "  sudo dnf install zfs"
        else
            echo "  (see your distribution's documentation)"
        fi
    fi
    exit 1
fi

# Check if pool already exists
if zpool list "$POOL_NAME" &> /dev/null; then
    echo "WARNING: Pool '$POOL_NAME' already exists!"
    echo ""
    read -p "Do you want to destroy it and recreate? (yes/no): " -r
    if [[ $REPLY == "yes" ]]; then
        echo "Destroying existing pool..."
        zpool destroy "$POOL_NAME" || {
            echo "ERROR: Failed to destroy pool. It may be in use."
            exit 1
        }
        if [ -f "$DISK_IMAGE" ]; then
            rm "$DISK_IMAGE"
        fi
    else
        echo "Keeping existing pool. Test suite will use it."
        exit 0
    fi
fi

# Check if disk image already exists
if [ -f "$DISK_IMAGE" ]; then
    echo "Disk image already exists at $DISK_IMAGE"
    read -p "Overwrite it? (yes/no): " -r
    if [[ $REPLY != "yes" ]]; then
        echo "Aborted."
        exit 1
    fi
    rm "$DISK_IMAGE"
fi

echo "Creating disk image..."
dd if=/dev/zero of="$DISK_IMAGE" bs=1M count=$DISK_SIZE status=progress

echo ""
echo "Creating ZFS pool..."
zpool create "$POOL_NAME" "$DISK_IMAGE"

echo ""
echo "=========================================="
echo "Test pool created successfully!"
echo "=========================================="
echo ""
echo "Pool status:"
sudo zpool status "$POOL_NAME"
echo ""
echo "To run real ZFS tests:"
echo "  pytest -m real_zfs"
echo ""
echo "To clean up when done:"
echo "  sudo zpool destroy $POOL_NAME"
echo "  rm $DISK_IMAGE"
echo ""
