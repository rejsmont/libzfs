"""Integration tests for libzfseasy - testing complete workflows."""

import pytest
from unittest.mock import MagicMock
import libzfseasy as zfs
from libzfseasy.types import Filesystem, Snapshot, Bookmark


class TestBasicWorkflow:
    """Test basic ZFS workflow operations."""
    
    @pytest.mark.integration
    @pytest.mark.subprocess
    def test_create_list_destroy_workflow(self, mock_subprocess, sample_pool):
        """Test creating, listing, and destroying a filesystem."""
        # Setup mock for create
        mock_subprocess.setup()
        
        # Create filesystem
        fs = zfs.create.filesystem(f'{sample_pool}/testfs', properties={'compression': 'lz4'})
        assert fs.name == f'{sample_pool}/testfs'
        assert fs['compression'].value == 'lz4'
        
        # Setup mock for list
        mock_subprocess.setup(stdout=[
            f'{sample_pool}/testfs\tfilesystem\tlz4\n'
        ])
        
        # List filesystems
        filesystems = zfs.list(roots=fs, properties=['compression'])
        assert len(filesystems) == 1
        assert filesystems[0].name == f'{sample_pool}/testfs'
        
        # Setup mock for destroy
        mock_subprocess.setup(stdout=['destroy\ttestpool/testfs\n'])
        
        # Destroy filesystem
        result = zfs.destroy.dataset(fs, destroy=True)
        assert len(result) == 1


class TestSnapshotWorkflow:
    """Test snapshot-related workflows."""
    
    @pytest.mark.integration
    @pytest.mark.subprocess
    def test_snapshot_creation_and_clone(self, mock_subprocess, sample_pool):
        """Test creating a snapshot and cloning it."""
        # Create filesystem
        mock_subprocess.setup()
        fs = zfs.create.filesystem(f'{sample_pool}/source')
        
        # Create snapshot
        mock_subprocess.setup()
        snap = zfs.snapshot(fs, 'snap1')
        assert snap.name == f'{sample_pool}/source@snap1'
        
        # Clone snapshot
        mock_subprocess.setup()
        clone = zfs.clone(snap, f'{sample_pool}/clone')
        assert clone.name == f'{sample_pool}/clone'
    
    @pytest.mark.integration
    @pytest.mark.subprocess
    def test_incremental_snapshot_workflow(self, mock_subprocess, sample_pool):
        """Test creating incremental snapshots."""
        # Create filesystem
        mock_subprocess.setup()
        fs = zfs.create.filesystem(f'{sample_pool}/data')
        
        # Create first snapshot
        mock_subprocess.setup()
        snap1 = zfs.snapshot(fs, 'snap1')
        
        # Create second snapshot
        mock_subprocess.setup()
        snap2 = zfs.snapshot(fs, 'snap2')
        
        # Send incremental stream
        process_mock = MagicMock()
        process_mock.stdout.peek.return_value = b'ZFS_DATA'
        process_mock.poll.return_value = None
        mock_subprocess.return_value = process_mock
        
        stream = zfs.send.snapshot(snap2, since=snap1)
        assert stream is not None


class TestBookmarkWorkflow:
    """Test bookmark-related workflows."""
    
    @pytest.mark.integration
    @pytest.mark.subprocess
    def test_snapshot_bookmark_workflow(self, mock_subprocess, sample_pool):
        """Test creating snapshot and converting to bookmark."""
        # Create filesystem
        mock_subprocess.setup()
        fs = zfs.create.filesystem(f'{sample_pool}/data')
        
        # Create snapshot
        mock_subprocess.setup()
        snap = zfs.snapshot(fs, 'snap1')
        
        # Create bookmark from snapshot
        mock_subprocess.setup()
        bookmark = zfs.bookmark(snap, 'mark1')
        assert bookmark.name == f'{sample_pool}/data#mark1'
        
        # Destroy snapshot (keep bookmark)
        mock_subprocess.setup(stdout=['destroy\ttestpool/data@snap1\n'])
        result = zfs.destroy.snapshots(snap, destroy=True)
        assert len(result) == 1


class TestPropertyManagement:
    """Test property get/set/inherit workflows."""
    
    @pytest.mark.integration
    @pytest.mark.subprocess
    def test_property_management_workflow(self, mock_subprocess, sample_pool):
        """Test getting, setting, and inheriting properties."""
        # Create filesystem
        mock_subprocess.setup()
        fs = zfs.create.filesystem(f'{sample_pool}/data')
        
        # Get initial properties
        mock_subprocess.setup(stdout=[
            f'{sample_pool}/data\tname\t{sample_pool}/data\t-\t-\n',
            f'{sample_pool}/data\ttype\tfilesystem\t-\t-\n',
            f'{sample_pool}/data\tcompression\toff\t-\tdefault\n',
        ])
        result = zfs.get(ds=fs, properties='compression')
        assert result[0]['compression'].value == 'off'
        assert result[0]['compression'].source == 'default'
        
        # Set property
        mock_subprocess.setup()
        fs = zfs.set(fs, {'compression': 'lz4'})
        assert fs['compression'].value == 'lz4'
        
        # Get updated property
        mock_subprocess.setup(stdout=[
            f'{sample_pool}/data\tname\t{sample_pool}/data\t-\t-\n',
            f'{sample_pool}/data\ttype\tfilesystem\t-\t-\n',
            f'{sample_pool}/data\tcompression\tlz4\t-\tlocal\n',
        ])
        result = zfs.get(ds=fs, properties='compression')
        assert result[0]['compression'].value == 'lz4'
        assert result[0]['compression'].source == 'local'
        
        # Inherit property
        mock_subprocess.setup()
        fs = zfs.inherit(fs, 'compression')


class TestSendReceiveWorkflow:
    """Test send/receive workflows."""
    
    @pytest.mark.integration
    @pytest.mark.subprocess
    def test_full_send_receive(self, mock_subprocess, sample_pool):
        """Test sending and receiving a snapshot."""
        # Create source filesystem
        mock_subprocess.setup()
        source = zfs.create.filesystem(f'{sample_pool}/source')
        
        # Create snapshot
        mock_subprocess.setup()
        snap = zfs.snapshot(source, 'backup')
        
        # Send snapshot
        send_process = MagicMock()
        send_process.stdout.peek.return_value = b'ZFS_DATA'
        send_process.poll.return_value = None
        mock_subprocess.return_value = send_process
        
        send_stream = zfs.send.snapshot(snap)
        assert send_stream is not None
        
        # Create target filesystem
        mock_subprocess.setup()
        target = zfs.create.filesystem(f'{sample_pool}/target')
        
        # Receive snapshot
        import io
        recv_process = MagicMock()
        recv_process.stdin = io.BufferedWriter(io.BytesIO())
        recv_process.poll.return_value = None
        mock_subprocess.return_value = recv_process
        
        recv_stream = zfs.receive.filesystem(target)
        assert recv_stream is not None


class TestMountWorkflow:
    """Test mount/unmount workflows."""
    
    @pytest.mark.integration
    @pytest.mark.subprocess
    def test_mount_unmount_workflow(self, mock_subprocess, sample_pool):
        """Test mounting and unmounting filesystems."""
        # Create filesystem
        mock_subprocess.setup()
        fs = zfs.create.filesystem(f'{sample_pool}/mounttest', mount=False)
        
        # Mount filesystem
        mock_subprocess.setup()
        zfs.mount(fs, None, None)
        
        # Unmount filesystem
        mock_subprocess.setup()
        zfs.unmount(fs)
    
    @pytest.mark.integration
    @pytest.mark.subprocess
    def test_mount_all_workflow(self, mock_subprocess):
        """Test mounting all filesystems."""
        # Mount all
        mock_subprocess.setup()
        zfs.mount(None, None, None)
        
        # Unmount all
        mock_subprocess.setup()
        zfs.unmount(None)


class TestRenameWorkflow:
    """Test rename workflows."""
    
    @pytest.mark.integration
    @pytest.mark.subprocess
    def test_rename_filesystem_workflow(self, mock_subprocess, sample_pool):
        """Test renaming a filesystem."""
        # Create filesystem
        mock_subprocess.setup()
        fs = zfs.create.filesystem(f'{sample_pool}/oldname')
        
        # Rename filesystem
        mock_subprocess.setup()
        new_fs = zfs.rename(fs, f'{sample_pool}/newname')
        assert new_fs.name == f'{sample_pool}/newname'
    
    @pytest.mark.integration
    @pytest.mark.subprocess
    def test_rename_snapshot_workflow(self, mock_subprocess, sample_pool):
        """Test renaming a snapshot."""
        # Create filesystem and snapshot
        mock_subprocess.setup()
        fs = zfs.create.filesystem(f'{sample_pool}/data')
        
        mock_subprocess.setup()
        snap = zfs.snapshot(fs, 'oldsnap')
        
        # Rename snapshot
        mock_subprocess.setup()
        new_snap = zfs.rename(snap, 'newsnap')
        assert new_snap.short == 'newsnap'


class TestComplexWorkflow:
    """Test complex multi-step workflows."""
    
    @pytest.mark.integration
    @pytest.mark.subprocess
    @pytest.mark.slow
    def test_backup_restore_workflow(self, mock_subprocess, sample_pool):
        """Test a complete backup and restore workflow."""
        # Step 1: Create source filesystem with data
        mock_subprocess.setup()
        source = zfs.create.filesystem(f'{sample_pool}/production', 
                                       properties={'compression': 'lz4', 'quota': '100G'})
        
        # Step 2: Create initial snapshot
        mock_subprocess.setup()
        snap1 = zfs.snapshot(source, 'daily-2024-01-01', recursive=True)
        
        # Step 3: Create second snapshot for incremental
        mock_subprocess.setup()
        snap2 = zfs.snapshot(source, 'daily-2024-01-02', recursive=True)
        
        # Step 4: Send incremental snapshot
        process_mock = MagicMock()
        process_mock.stdout.peek.return_value = b'ZFS_DATA'
        process_mock.poll.return_value = None
        mock_subprocess.return_value = process_mock
        
        stream = zfs.send.snapshot(snap2, since=snap1, replicate=True)
        assert stream is not None
        
        # Step 5: Create bookmark for snap1 before destroying
        mock_subprocess.setup()
        bookmark = zfs.bookmark(snap1, 'daily-2024-01-01')
        
        # Step 6: Destroy old snapshot
        mock_subprocess.setup(stdout=[f'destroy\t{sample_pool}/production@daily-2024-01-01\n'])
        result = zfs.destroy.snapshots(snap1, destroy=True)
        
        # Step 7: Create target filesystem
        mock_subprocess.setup()
        target = zfs.create.filesystem(f'{sample_pool}/backup')
        
        # Step 8: Receive snapshot
        import io
        recv_process = MagicMock()
        recv_process.stdin = io.BufferedWriter(io.BytesIO())
        recv_process.poll.return_value = None
        mock_subprocess.return_value = recv_process
        
        recv_stream = zfs.receive.filesystem(target, force=True)
        assert recv_stream is not None


class TestErrorHandling:
    """Test error handling in workflows."""
    
    @pytest.mark.integration
    @pytest.mark.subprocess
    def test_create_duplicate_filesystem(self, mock_subprocess, sample_pool):
        """Test creating duplicate filesystem raises error."""
        # Create filesystem
        mock_subprocess.setup()
        fs = zfs.create.filesystem(f'{sample_pool}/duplicate')
        
        # Try to create again (mock error)
        mock_subprocess.setup(stderr='filesystem already exists\n', returncode=1)
        
        with pytest.raises(Exception):
            zfs.create.filesystem(f'{sample_pool}/duplicate')
    
    @pytest.mark.integration
    @pytest.mark.subprocess
    def test_destroy_nonexistent_dataset(self, mock_subprocess, sample_pool):
        """Test destroying non-existent dataset raises error."""
        from libzfseasy.types import Filesystem
        
        fs = Filesystem(f'{sample_pool}/nonexistent')
        mock_subprocess.setup(stderr='dataset does not exist\n', returncode=1)
        
        with pytest.raises(Exception):
            zfs.destroy.dataset(fs, destroy=True)
