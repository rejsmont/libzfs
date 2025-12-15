"""Real ZFS integration tests - these actually run ZFS commands.

REQUIREMENTS:
- ZFS must be installed and available
- Either root access OR a test pool must exist
- Run with: pytest -m real_zfs

SETUP:
For safe testing, create a test pool using a file-backed vdev:
    $ dd if=/dev/zero of=/tmp/zfs-test-disk bs=1M count=512
    $ zpool create testpool /tmp/zfs-test-disk

CLEANUP:
    $ zpool destroy testpool
    $ rm /tmp/zfs-test-disk

WARNING: These tests will create and destroy ZFS datasets. Do NOT run against
production pools.
"""

import pytest
import subprocess
import os
import libzfseasy as zfs
from libzfseasy.zfs import ZFS_BIN, ZPOOL_BIN
from libzfseasy.types import Filesystem, Volume, Snapshot, Bookmark, Dataset


# Skip all tests in this file if ZFS is not available
pytestmark = pytest.mark.real_zfs


def has_zfs():
    """Check if ZFS commands are available."""
    try:
        subprocess.run([ZFS_BIN, 'list'], capture_output=True, check=True)
        return True
    except (subprocess.CalledProcessError, FileNotFoundError):
        return False


def pool_exists(pool_name):
    """Check if a ZFS pool exists."""
    try:
        result = subprocess.run([ZPOOL_BIN, 'list', '-H', pool_name],
                              capture_output=True, check=True, text=True)
        return True
    except subprocess.CalledProcessError:
        return False


@pytest.fixture(scope='module')
def test_pool():
    """Get or create a test pool for real ZFS testing.
    
    This fixture will:
    1. Check for an existing test pool (testpool, test-pool, or from TEST_ZFS_POOL env var)
    2. If found, use it and clean it up before tests
    3. If not found, skip tests with a helpful message
    """
    # Check for pool name from environment or use default
    pool_name = os.environ.get('TEST_ZFS_POOL', 'testpool')
    
    if not has_zfs():
        pytest.skip('ZFS commands not available - install ZFS utilities')
    
    if not pool_exists(pool_name):
        pytest.skip(
            f'Test pool "{pool_name}" not found. Create it with:\n'
            f'  dd if=/dev/zero of=/tmp/zfs-test-disk bs=1M count=512\n'
            f'  sudo zpool create {pool_name} /tmp/zfs-test-disk\n'
            f'Or set TEST_ZFS_POOL environment variable to an existing pool.'
        )
    
    # Clean up any existing test datasets
    _cleanup_test_datasets(pool_name)
    
    yield Dataset.from_name(pool_name)
    
    # Clean up after all tests
    _cleanup_test_datasets(pool_name)


def _cleanup_test_datasets(pool_name):
    """Remove all test datasets from the pool."""
    try:
        result = subprocess.run([ZFS_BIN, 'list', '-H', '-r', '-o', 'name', pool_name],
                              capture_output=True, text=True, check=True)
        datasets = [line.strip() for line in result.stdout.split('\n') if line.strip()]
        
        # Destroy test datasets (but not the pool itself)
        for dataset in reversed(datasets):  # Reverse to destroy children first
            if dataset != pool_name and '/test_' in dataset:
                try:
                    subprocess.run([ZFS_BIN, 'destroy', '-r', dataset],
                                 capture_output=True, check=True)
                except subprocess.CalledProcessError:
                    pass  # Ignore errors during cleanup
    except subprocess.CalledProcessError:
        pass


class TestRealListCommand:
    """Test list command against real ZFS."""
    
    def test_list_pool(self, test_pool):
        """Test listing the test pool."""
        result = zfs.list(roots=test_pool)
        assert len(result) >= 1
        assert any(ds.name == test_pool.name for ds in result)
    
    def test_list_with_properties(self, test_pool):
        """Test listing with properties."""
        result = zfs.list(roots=test_pool, properties=['compression', 'mountpoint'])
        assert len(result) >= 1
        
        for ds in result:
            assert 'compression' in ds
            assert 'mountpoint' in ds
            assert ds['compression'].value is not None


class TestRealFilesystemOperations:
    """Test filesystem operations against real ZFS."""
    
    def test_create_list_destroy_filesystem(self, test_pool):
        """Test creating, listing, and destroying a filesystem."""
        fs_name = f'{test_pool}/test_filesystem'
        
        # Create filesystem
        fs = zfs.create.filesystem(fs_name, properties={'compression': 'lz4'})
        assert isinstance(fs, Filesystem)
        assert fs.name == fs_name
        
        # Verify it exists by listing
        result = zfs.list(roots=fs)
        assert len(result) == 1
        assert result[0].name == fs_name
        
        # Get properties
        result = zfs.list(roots=fs, properties=['compression'])
        assert result[0]['compression'].value == 'lz4'
        
        # Destroy filesystem
        zfs.destroy.dataset(fs, destroy=True)
        
        # Verify it's gone
        try:
            zfs.list(roots=fs)
            assert False, "Filesystem should have been destroyed"
        except:
            pass  # Expected - filesystem doesn't exist
    
    def test_set_and_get_property(self, test_pool):
        """Test setting and getting filesystem properties."""
        fs_name = f'{test_pool}/test_props'
        
        # Create filesystem
        fs = zfs.create.filesystem(fs_name)
        
        try:
            # Set property
            zfs.set(fs, {'compression': 'gzip-9'})
            
            # Get property
            result = zfs.get(fs, ['compression'])
            assert result[0]['compression'].value == 'gzip-9'
        finally:
            # Cleanup
            zfs.destroy.dataset(fs, destroy=True)
    
    def test_inherit_property(self, test_pool):
        """Test inheriting a property."""
        fs_name = f'{test_pool}/test_inherit'
        
        # Create filesystem with custom property
        fs = zfs.create.filesystem(fs_name, properties={'compression': 'gzip'})
        
        try:
            # Verify custom property
            result = zfs.get(fs, ['compression'])
            assert result[0]['compression'].value == 'gzip'
            
            # Inherit property
            zfs.inherit(fs, ['compression'])
            
            # Verify it's now inherited (should be different)
            result = zfs.get(fs, ['compression'])
            # It should now have the pool's compression setting
            assert result[0]['compression'].value != 'gzip' or result[0]['compression'].source != 'local'
        finally:
            # Cleanup
            zfs.destroy.dataset(fs, destroy=True)


class TestRealSnapshotOperations:
    """Test snapshot operations against real ZFS."""
    
    def test_create_and_destroy_snapshot(self, test_pool):
        """Test creating and destroying snapshots."""
        fs_name = f'{test_pool}/test_snap_fs'
        snap_name = f'{fs_name}@test_snapshot'
        
        # Create filesystem
        fs = zfs.create.filesystem(fs_name)
        
        try:
            # Create snapshot
            snap = zfs.snapshot(fs, 'test_snapshot')
            assert isinstance(snap, Snapshot)
            assert snap.name == snap_name
            
            # List snapshots
            result = zfs.list(roots=fs, types=['snapshot'])
            assert len(result) == 1
            assert result[0].name == snap_name
            
            # Destroy snapshot
            zfs.destroy.snapshots(snap, destroy=True)
            
            # Verify it's gone
            result = zfs.list(roots=fs, types=['snapshot'])
            assert len(result) == 0
        finally:
            # Cleanup
            zfs.destroy.dataset(fs, destroy=True, recursive=True)
    
    def test_recursive_snapshot(self, test_pool):
        """Test recursive snapshot creation."""
        parent_name = f'{test_pool}/test_recursive'
        child_name = f'{parent_name}/child'
        snap_name = 'recursive_snap'

        # Create parent and child filesystems
        parent = zfs.create.filesystem(parent_name)
        child = zfs.create.filesystem(child_name)
        
        try:
            # Create recursive snapshot
            result = zfs.snapshot(parent, snap_name, recursive=True)
            assert len(result) == 2
            assert all(isinstance(s, Snapshot) for s in result)
            
            # Verify both snapshots exist
            result = zfs.list(roots=parent, types=['snapshot'], recursive=True)
            assert len(result) == 2
            assert any(s.name == f'{parent_name}@{snap_name}' for s in result)
            assert any(s.name == f'{child_name}@{snap_name}' for s in result)
        finally:
            # Cleanup
            zfs.destroy.dataset(parent, destroy=True, recursive=True)


class TestRealCloneOperations:
    """Test clone operations against real ZFS."""
    
    def test_clone_snapshot(self, test_pool):
        """Test cloning a snapshot."""
        fs_name = f'{test_pool}/test_clone_source'
        snap_name = f'{fs_name}@test_snap'
        clone_name = f'{test_pool}/test_clone_target'
        
        # Create filesystem and snapshot
        fs = zfs.create.filesystem(fs_name)
        snap = zfs.snapshot(fs, 'test_snap')
        
        try:
            # Clone snapshot
            clone = zfs.clone(snap, clone_name)
            assert isinstance(clone, Filesystem)
            assert clone.name == clone_name
            
            # Verify clone exists
            result = zfs.list(roots=clone)
            assert len(result) == 1
            assert result[0].name == clone_name
            
            # Get origin property
            result = zfs.get(clone, ['origin'])
            assert result[0]['origin'].value == snap_name
        finally:
            # Cleanup (clone first, then source)
            try:
                zfs.destroy.dataset(clone, destroy=True)
            except:
                pass
            zfs.destroy.dataset(fs, destroy=True, recursive=True)


class TestRealBookmarkOperations:
    """Test bookmark operations against real ZFS."""
    
    def test_create_and_destroy_bookmark(self, test_pool):
        """Test creating and destroying bookmarks."""
        fs_name = f'{test_pool}/test_bookmark_fs'
        snap_name = f'{fs_name}@test_snap'
        bookmark_name = f'{fs_name}#test_bookmark'
        
        # Create filesystem and snapshot
        fs = zfs.create.filesystem(fs_name)
        snap = zfs.snapshot(fs, 'test_snap')
        
        try:
            # Create bookmark
            bookmark = zfs.bookmark(snap, 'test_bookmark')
            assert isinstance(bookmark, Bookmark)
            assert bookmark.name == bookmark_name
            
            # List bookmarks
            result = zfs.list(roots=fs_name, types=['bookmark'])
            assert len(result) == 1
            assert result[0].name == bookmark_name
            
            # Destroy bookmark
            zfs.destroy.dataset(bookmark, destroy=True)
            
            # Verify it's gone
            result = zfs.list(roots=fs_name, types=['bookmark'])
            assert len(result) == 0
        finally:
            # Cleanup
            zfs.destroy.dataset(fs, destroy=True, recursive=True)


class TestRealRenameOperations:
    """Test rename operations against real ZFS."""
    
    def test_rename_filesystem(self, test_pool):
        """Test renaming a filesystem."""
        old_name = f'{test_pool}/test_rename_old'
        new_name = f'{test_pool}/test_rename_new'
        
        # Create filesystem
        fs = zfs.create.filesystem(old_name)
        
        try:
            # Rename filesystem
            renamed = zfs.rename(fs, new_name)
            assert renamed.name == new_name
            
            # Verify old name doesn't exist
            try:
                zfs.list(roots=old_name)
                assert False, "Old filesystem name should not exist"
            except:
                pass  # Expected
            
            # Verify new name exists
            result = zfs.list(roots=new_name)
            assert len(result) == 1
            assert result[0].name == new_name
        finally:
            # Cleanup (try both names just in case)
            try:
                zfs.destroy.dataset(new_name, destroy=True)
            except:
                pass
            try:
                zfs.destroy.dataset(old_name, destroy=True)
            except:
                pass


class TestRealVolumeOperations:
    """Test volume operations against real ZFS."""
    
    def test_create_and_destroy_volume(self, test_pool):
        """Test creating and destroying a volume."""
        vol_name = f'{test_pool}/test_volume'
        
        # Create volume (100MB)
        vol = zfs.create.volume(vol_name, '100M')
        assert isinstance(vol, Volume)
        assert vol.name == vol_name
        
        try:
            # List volume
            result = zfs.list(roots=vol_name)
            assert len(result) == 1
            assert result[0].name == vol_name
            assert isinstance(result[0], Volume)
            
            # Get properties
            result = zfs.get(vol, ['volsize'])
            # Volume size should be 100M (104857600 bytes)
            assert result[0]['volsize'].value in ['100M', '104857600']
        finally:
            # Cleanup
            zfs.destroy.dataset(vol, destroy=True)
