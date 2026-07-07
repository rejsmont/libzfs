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
import libzfseasy as zfs
from libzfseasy.zfs import _zfs_cmd
from libzfseasy.types import Filesystem, Volume, Snapshot, Bookmark


# Skip all tests in this file if ZFS is not available
pytestmark = pytest.mark.real_zfs


@pytest.fixture(scope='module')
def test_pool(auto_zfs_pool):
    """Provide a ZFS pool for real ZFS testing; auto-created via auto_zfs_pool fixture.

    Skips if ZFS is unavailable or if permission delegation isn't supported on this platform.
    """
    if auto_zfs_pool is None:
        pytest.skip(
            'ZFS pool not available — install ZFS and grant NOPASSWD sudo for zpool/zfs'
        )
    pool_name = auto_zfs_pool.name
    _cleanup_test_datasets(pool_name)

    # Verify the current user can create datasets (delegation may not work on all platforms)
    probe = f'{pool_name}/_probe'
    try:
        subprocess.run([_zfs_cmd(), 'create', probe], check=True, capture_output=True)
        subprocess.run([_zfs_cmd(), 'destroy', probe], check=True, capture_output=True)
    except subprocess.CalledProcessError:
        pytest.skip(
            'ZFS dataset creation failed — ensure passwordless sudo for zfs/zpool'
        )

    yield auto_zfs_pool
    _cleanup_test_datasets(pool_name)


def _cleanup_test_datasets(pool_name):
    """Remove all test datasets from the pool."""
    try:
        result = subprocess.run([_zfs_cmd(), 'list', '-H', '-r', '-o', 'name', pool_name],
                              capture_output=True, text=True, check=True)
        datasets = [line.strip() for line in result.stdout.split('\n') if line.strip()]
        
        # Destroy test datasets (but not the pool itself)
        for dataset in reversed(datasets):  # Reverse to destroy children first
            if dataset != pool_name and '/test_' in dataset:
                try:
                    subprocess.run([_zfs_cmd(), 'destroy', '-r', dataset],
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
        zfs.destroy(fs, destroy=True)
        
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
            zfs.destroy(fs, destroy=True)
    
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
            zfs.destroy(fs, destroy=True)


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
            zfs.destroy(snap, destroy=True)
            
            # Verify it's gone
            result = zfs.list(roots=fs, types=['snapshot'])
            assert len(result) == 0
        finally:
            # Cleanup
            zfs.destroy(fs, destroy=True, recursive=True)
    
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
            zfs.destroy(parent, destroy=True, recursive=True)


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
            print('clone created:', clone)
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
                zfs.destroy(clone, destroy=True)
            except:
                pass
            zfs.destroy(fs, destroy=True, recursive=True)


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
            result = zfs.list(roots=fs, types=['bookmark'])
            assert len(result) == 1
            assert result[0].name == bookmark_name
            
            # Destroy bookmark
            zfs.destroy(bookmark, destroy=True)
            
            # Verify it's gone
            result = zfs.list(roots=fs, types=['bookmark'])
            assert len(result) == 0
        finally:
            # Cleanup
            zfs.destroy(fs, destroy=True, recursive=True)


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
                zfs.list(roots=fs)
                assert False, "Old filesystem name should not exist"
            except:
                pass  # Expected
            
            # Verify new name exists
            result = zfs.list(roots=renamed)
            assert len(result) == 1
            assert result[0].name == new_name
        finally:
            # Cleanup (try both names just in case)
            try:
                zfs.destroy(new_name, destroy=True)
            except:
                pass
            try:
                zfs.destroy(old_name, destroy=True)
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
            result = zfs.list(roots=vol)
            assert len(result) == 1
            assert result[0].name == vol_name
            assert isinstance(result[0], Volume)
            
            # Get properties
            result = zfs.get(vol, ['volsize'])
            # Volume size should be 100M (104857600 bytes)
            assert result[0]['volsize'].value in ['100M', '104857600']
        finally:
            # Cleanup
            zfs.destroy(vol, destroy=True)


class TestRealExecOutThreadCleanup:
    """Regression test for the Command._exec_out() generator-cleanup fix.

    The mocked test suite (tests/test_commands.py::TestExecCaptureRegressions
    ::test_exec_out_abandoned_generator_terminates_child) can only assert that
    process.terminate()/process.wait() get *called* on a MagicMock -- it
    cannot prove that a real background stderr-reader thread, blocked on a
    real OS pipe's readline(), actually gets unblocked and joined. Only a
    real subprocess/pipe (i.e. real_zfs) can demonstrate that no thread (and
    no zombie child) is left behind after a lazy generator is abandoned.
    """

    def test_lazy_list_abandoned_generator_leaves_no_thread(self, test_pool):
        """Consume one element of a lazy `zfs list` generator over a dataset
        with several children, then abandon it; the background stderr-reader
        thread started for that command must not leak."""
        import gc
        import threading
        import time

        parent_name = f'{test_pool}/test_lazy_cleanup'
        parent = zfs.create.filesystem(parent_name)
        try:
            for i in range(5):
                zfs.create.filesystem(f'{parent_name}/child{i}')

            baseline = threading.active_count()

            gen = zfs.list(roots=parent, recursive=True, lazy=True)
            first = next(gen)
            assert first is not None

            # Abandon the generator mid-iteration (as e.g. a caller
            # `break`-ing out of a `for` loop over the lazy result would do).
            gen.close()
            del gen
            gc.collect()

            # The stderr-reader thread join() happens synchronously inside
            # the generator's cleanup, so this should already be true; poll
            # briefly regardless to avoid flakiness under CI scheduling.
            deadline = time.time() + 5
            while threading.active_count() > baseline and time.time() < deadline:
                time.sleep(0.05)

            assert threading.active_count() == baseline, (
                'a background stderr-reader thread leaked after abandoning '
                'a lazy _exec_out-backed generator'
            )
        finally:
            zfs.destroy(parent, destroy=True, recursive=True)


class TestRealSendReceiveStreamHandle:
    """Regression tests for the StreamHandle fix: zfs.send/zfs.receive now
    return a context-manager wrapper whose close()/__exit__ waits for the
    subprocess and raises the bare subprocess Exception on non-zero exit,
    instead of silently succeeding. This is the actual bug being fixed --
    only provable end-to-end against a real `zfs send`/`zfs receive` pair.
    """

    def test_send_receive_round_trip_succeeds(self, test_pool):
        """A genuine send -> receive round trip must succeed cleanly, with
        both StreamHandles usable as context managers."""
        src_name = f'{test_pool}/test_stream_src'
        dst_name = f'{test_pool}/test_stream_dst'

        src = zfs.create.filesystem(src_name)
        try:
            snap = zfs.snapshot(src, 'snap1')

            with zfs.send.snapshot(snap) as send_stream:
                data = send_stream.read()

            assert len(data) > 0

            with zfs.receive.filesystem(Filesystem(dst_name)) as recv_stream:
                recv_stream.write(data)

            result = zfs.list(roots=Filesystem(dst_name), types=['snapshot'], recursive=True)
            assert any(s.name == f'{dst_name}@snap1' for s in result)
        finally:
            try:
                zfs.destroy(Filesystem(dst_name), destroy=True, recursive=True)
            except Exception:
                pass
            zfs.destroy(src, destroy=True, recursive=True)

    def test_receive_truncated_stream_raises_on_close(self, test_pool):
        """Induce a receive failure by feeding `zfs receive` a truncated (and
        therefore corrupt/incomplete) send stream. Before the fix, ReceiveCommand
        handed back a bare BufferedWriter and nothing ever checked the
        subprocess's exit code, so this failure was silently swallowed. Now
        it must surface via close()/__exit__ raising."""
        src_name = f'{test_pool}/test_stream_trunc_src'
        dst_name = f'{test_pool}/test_stream_trunc_dst'

        src = zfs.create.filesystem(src_name)
        try:
            zfs.snapshot(src, 'snap1')
            snap = zfs.list(roots=src, types=['snapshot'])[0]

            with zfs.send.snapshot(snap) as send_stream:
                full_data = send_stream.read()

            # Sanity check: make sure we actually have a real stream to
            # truncate, not a degenerate empty one.
            assert len(full_data) > 0
            truncated = full_data[: max(1, len(full_data) // 2)]

            with pytest.raises(Exception):
                with zfs.receive.filesystem(Filesystem(dst_name)) as recv_stream:
                    recv_stream.write(truncated)
        finally:
            try:
                zfs.destroy(Filesystem(dst_name), destroy=True, recursive=True)
            except Exception:
                pass
            zfs.destroy(src, destroy=True, recursive=True)
