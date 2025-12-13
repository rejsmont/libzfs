"""Unit tests for libzfseasy.zfs command classes."""

import pytest
import io
from unittest.mock import MagicMock, call
from libzfseasy.zfs import (
    ListCommand, CreateCommand, SnapshotCommand, BookmarkCommand,
    DestroyCommand, RenameCommand, CloneCommand, GetCommand, SetCommand,
    InheritCommand, SendCommand, ReceiveCommand, MountCommand, UnMountCommand
)
from libzfseasy.types import Dataset, Filesystem, Volume, Snapshot, Bookmark


class TestListCommand:
    """Tests for ListCommand class."""
    
    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_list_basic(self, mock_subprocess, sample_pool):
        """Test basic list command."""
        mock_subprocess.setup(stdout=[
            f'{sample_pool}/filesystem\tfilesystem\n',
            f'{sample_pool}/volume\tvolume\n'
        ])
        
        cmd = ListCommand()
        result = cmd()
        
        assert len(result) == 2
        assert isinstance(result[0], Filesystem)
        assert result[0].name == f'{sample_pool}/filesystem'
        assert isinstance(result[1], Volume)
    
    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_list_with_properties(self, mock_subprocess, sample_pool):
        """Test list command with properties."""
        mock_subprocess.setup(stdout=[
            f'{sample_pool}/filesystem\tfilesystem\tlz4\t/mnt/test\n'
        ])
        
        cmd = ListCommand()
        result = cmd(properties=['compression', 'mountpoint'])
        
        assert len(result) == 1
        assert result[0]['compression'].value == 'lz4'
        assert result[0]['mountpoint'].value == '/mnt/test'
    
    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_list_recursive(self, mock_subprocess, sample_pool):
        """Test recursive list command."""
        mock_subprocess.setup(stdout=[
            f'{sample_pool}\tfilesystem\n',
            f'{sample_pool}/child\tfilesystem\n'
        ])
        
        cmd = ListCommand()
        root = Dataset(sample_pool)
        result = cmd(roots=root, recursive=True)
        
        assert len(result) == 2
    
    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_list_lazy(self, mock_subprocess, sample_pool):
        """Test lazy list command (returns generator)."""
        mock_subprocess.setup(stdout=[
            f'{sample_pool}/filesystem\tfilesystem\n'
        ])
        
        cmd = ListCommand()
        result = cmd(lazy=True)
        
        # Result should be a generator
        assert hasattr(result, '__iter__')
        items = list(result)
        assert len(items) == 1


class TestCreateCommand:
    """Tests for CreateCommand class."""
    
    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_create_filesystem(self, mock_subprocess, sample_pool):
        """Test creating a filesystem."""
        mock_subprocess.setup()
        
        cmd = CreateCommand()
        result = cmd.filesystem(f'{sample_pool}/newfs')
        
        assert isinstance(result, Filesystem)
        assert result.name == f'{sample_pool}/newfs'
        
        # Verify command was called
        call_args = mock_subprocess.call_args[0][0]
        assert 'create' in call_args
        assert f'{sample_pool}/newfs' in call_args
    
    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_create_filesystem_with_properties(self, mock_subprocess, sample_pool):
        """Test creating filesystem with properties."""
        mock_subprocess.setup()
        
        cmd = CreateCommand()
        properties = {'compression': 'lz4', 'quota': '10G'}
        result = cmd.filesystem(f'{sample_pool}/newfs', properties=properties)
        
        # Verify properties were passed
        call_args = mock_subprocess.call_args[0][0]
        assert '-o' in call_args
        assert any('compression=lz4' in arg for arg in call_args)
    
    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_create_filesystem_with_parents(self, mock_subprocess, sample_pool):
        """Test creating filesystem with parents flag."""
        mock_subprocess.setup()
        
        cmd = CreateCommand()
        result = cmd.filesystem(f'{sample_pool}/parent/child', parents=True)
        
        call_args = mock_subprocess.call_args[0][0]
        assert '-p' in call_args
    
    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_create_volume(self, mock_subprocess, sample_pool):
        """Test creating a volume."""
        mock_subprocess.setup()
        
        cmd = CreateCommand()
        result = cmd.volume(f'{sample_pool}/newvol', '10G')
        
        assert isinstance(result, Volume)
        assert result.name == f'{sample_pool}/newvol'
        
        call_args = mock_subprocess.call_args[0][0]
        assert 'create' in call_args
        assert '-V' in call_args
        assert '10G' in call_args
    
    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_create_volume_sparse(self, mock_subprocess, sample_pool):
        """Test creating a sparse volume."""
        mock_subprocess.setup()
        
        cmd = CreateCommand()
        result = cmd.volume(f'{sample_pool}/newvol', '10G', sparse=True)
        
        call_args = mock_subprocess.call_args[0][0]
        assert '-s' in call_args
    
    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_create_auto_detect_volume(self, mock_subprocess, sample_pool):
        """Test auto-detecting volume creation from size parameter."""
        mock_subprocess.setup()
        
        cmd = CreateCommand()
        result = cmd(f'{sample_pool}/newvol', size='10G')
        
        assert isinstance(result, Volume)


class TestSnapshotCommand:
    """Tests for SnapshotCommand class."""
    
    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_create_snapshot(self, mock_subprocess, sample_filesystem):
        """Test creating a snapshot."""
        mock_subprocess.setup()
        
        cmd = SnapshotCommand()
        result = cmd(sample_filesystem, 'snap1')
        
        assert isinstance(result, Snapshot)
        assert result.short == 'snap1'
        
        call_args = mock_subprocess.call_args[0][0]
        assert 'snapshot' in call_args
        assert f'{sample_filesystem.name}@snap1' in call_args
    
    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_create_snapshot_recursive(self, mock_subprocess, sample_filesystem):
        """Test creating recursive snapshot."""
        mock_subprocess.setup()
        
        cmd = SnapshotCommand()
        result = cmd(sample_filesystem, 'snap1', recursive=True)
        
        call_args = mock_subprocess.call_args[0][0]
        assert '-r' in call_args
    
    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_create_snapshot_with_properties(self, mock_subprocess, sample_filesystem):
        """Test creating snapshot with properties."""
        mock_subprocess.setup()
        
        cmd = SnapshotCommand()
        properties = {'custom:tag': 'important'}
        result = cmd(sample_filesystem, 'snap1', properties=properties)
        
        call_args = mock_subprocess.call_args[0][0]
        assert '-o' in call_args


class TestBookmarkCommand:
    """Tests for BookmarkCommand class."""
    
    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_create_bookmark(self, mock_subprocess, sample_snapshot):
        """Test creating a bookmark from snapshot."""
        mock_subprocess.setup()
        
        cmd = BookmarkCommand()
        result = cmd(sample_snapshot, 'bookmark1')
        
        assert isinstance(result, Bookmark)
        assert result.short == 'bookmark1'
        
        call_args = mock_subprocess.call_args[0][0]
        assert 'bookmark' in call_args


class TestDestroyCommand:
    """Tests for DestroyCommand class."""
    
    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_destroy_dataset(self, mock_subprocess, sample_filesystem):
        """Test destroying a dataset."""
        mock_subprocess.setup(stdout=['destroy\ttestpool/filesystem\n'])
        
        cmd = DestroyCommand()
        result = cmd.dataset(sample_filesystem, destroy=True)
        
        assert len(result) == 1
        assert 'testpool/filesystem' in result[0]
        
        call_args = mock_subprocess.call_args[0][0]
        assert 'destroy' in call_args
    
    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_destroy_dataset_dry_run(self, mock_subprocess, sample_filesystem):
        """Test dry-run destroy (destroy=False)."""
        mock_subprocess.setup(stdout=['destroy\ttestpool/filesystem\n'])
        
        cmd = DestroyCommand()
        result = cmd.dataset(sample_filesystem, destroy=False)
        
        call_args = mock_subprocess.call_args[0][0]
        assert '-n' in call_args
    
    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_destroy_dataset_recursive(self, mock_subprocess, sample_filesystem):
        """Test recursive destroy."""
        mock_subprocess.setup(stdout=['destroy\ttestpool/filesystem\n'])
        
        cmd = DestroyCommand()
        result = cmd.dataset(sample_filesystem, destroy=True, recursive=True)
        
        call_args = mock_subprocess.call_args[0][0]
        assert '-r' in call_args
    
    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_destroy_snapshots(self, mock_subprocess, sample_snapshot):
        """Test destroying snapshots."""
        mock_subprocess.setup(stdout=['destroy\ttestpool/filesystem@snap1\n'])
        
        cmd = DestroyCommand()
        result = cmd.snapshots(sample_snapshot, destroy=True)
        
        assert len(result) == 1
    
    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_destroy_bookmark(self, mock_subprocess, sample_bookmark):
        """Test destroying a bookmark."""
        mock_subprocess.setup()
        
        cmd = DestroyCommand()
        cmd.bookmark(sample_bookmark)
        
        call_args = mock_subprocess.call_args[0][0]
        assert 'destroy' in call_args


class TestRenameCommand:
    """Tests for RenameCommand class."""
    
    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_rename_filesystem(self, mock_subprocess, sample_filesystem):
        """Test renaming a filesystem."""
        mock_subprocess.setup()
        
        cmd = RenameCommand()
        result = cmd.filesystem(sample_filesystem, 'testpool/newname')
        
        assert isinstance(result, Filesystem)
        assert result.name == 'testpool/newname'
        
        call_args = mock_subprocess.call_args[0][0]
        assert 'rename' in call_args
        assert 'testpool/filesystem' in call_args
        assert 'testpool/newname' in call_args
    
    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_rename_snapshot(self, mock_subprocess, sample_snapshot):
        """Test renaming a snapshot."""
        mock_subprocess.setup()
        
        cmd = RenameCommand()
        result = cmd.snapshot(sample_snapshot, 'snap2')
        
        assert isinstance(result, Snapshot)
        assert result.short == 'snap2'


class TestCloneCommand:
    """Tests for CloneCommand class."""
    
    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_clone_snapshot(self, mock_subprocess, sample_snapshot):
        """Test cloning a snapshot."""
        mock_subprocess.setup()
        
        cmd = CloneCommand()
        result = cmd(sample_snapshot, 'testpool/clone')
        
        assert isinstance(result, Dataset)
        assert result.name == 'testpool/clone'
        
        call_args = mock_subprocess.call_args[0][0]
        assert 'clone' in call_args


class TestGetCommand:
    """Tests for GetCommand class."""
    
    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_get_properties(self, mock_subprocess, sample_filesystem):
        """Test getting properties."""
        mock_subprocess.setup(stdout=[
            'testpool/filesystem\tname\ttestpool/filesystem\t-\t-\n',
            'testpool/filesystem\ttype\tfilesystem\t-\t-\n',
            'testpool/filesystem\tcompression\tlz4\t-\tlocal\n',
        ])
        
        cmd = GetCommand()
        result = cmd(ds=sample_filesystem, properties='compression')
        
        assert len(result) == 1
        assert result[0]['compression'].value == 'lz4'
        assert result[0]['compression'].source == 'local'
    
    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_get_all_properties(self, mock_subprocess, sample_filesystem):
        """Test getting all properties."""
        mock_subprocess.setup(stdout=[
            'testpool/filesystem\tname\ttestpool/filesystem\t-\t-\n',
            'testpool/filesystem\ttype\tfilesystem\t-\t-\n',
            'testpool/filesystem\tcompression\tlz4\t-\tlocal\n',
        ])
        
        cmd = GetCommand()
        result = cmd(ds=sample_filesystem, properties='all')
        
        assert len(result) >= 1


class TestSetCommand:
    """Tests for SetCommand class."""
    
    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_set_property(self, mock_subprocess, sample_filesystem):
        """Test setting a property."""
        mock_subprocess.setup()
        
        cmd = SetCommand()
        properties = {'compression': 'gzip'}
        result = cmd(sample_filesystem, properties)
        
        assert result['compression'].value == 'gzip'
        
        call_args = mock_subprocess.call_args[0][0]
        assert 'set' in call_args
        assert 'compression=gzip' in call_args


class TestInheritCommand:
    """Tests for InheritCommand class."""
    
    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_inherit_property(self, mock_subprocess, sample_filesystem):
        """Test inheriting a property."""
        mock_subprocess.setup()
        
        cmd = InheritCommand()
        result = cmd(sample_filesystem, 'compression')
        
        call_args = mock_subprocess.call_args[0][0]
        assert 'inherit' in call_args
        assert 'compression' in call_args
    
    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_inherit_property_recursive(self, mock_subprocess, sample_filesystem):
        """Test inheriting property recursively."""
        mock_subprocess.setup()
        
        cmd = InheritCommand()
        result = cmd(sample_filesystem, 'compression', recursive=True)
        
        call_args = mock_subprocess.call_args[0][0]
        assert '-r' in call_args


class TestSendCommand:
    """Tests for SendCommand class."""
    
    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_send_snapshot(self, mock_subprocess, sample_snapshot):
        """Test sending a snapshot."""
        # Mock process with a readable stdout
        process_mock = MagicMock()
        process_mock.stdout.peek.return_value = b'ZFS_DATA'
        process_mock.poll.return_value = None
        mock_subprocess.return_value = process_mock
        
        cmd = SendCommand()
        result = cmd.snapshot(sample_snapshot)
        
        assert result is not None
        call_args = mock_subprocess.call_args[0][0]
        assert 'send' in call_args
    
    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_send_incremental(self, mock_subprocess, sample_filesystem):
        """Test sending incremental snapshot."""
        snap1 = Snapshot(sample_filesystem, 'snap1')
        snap2 = Snapshot(sample_filesystem, 'snap2')
        
        process_mock = MagicMock()
        process_mock.stdout.peek.return_value = b'ZFS_DATA'
        process_mock.poll.return_value = None
        mock_subprocess.return_value = process_mock
        
        cmd = SendCommand()
        result = cmd.snapshot(snap2, since=snap1)
        
        call_args = mock_subprocess.call_args[0][0]
        assert '-i' in call_args


class TestReceiveCommand:
    """Tests for ReceiveCommand class."""
    
    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_receive_filesystem(self, mock_subprocess, sample_filesystem):
        """Test receiving into a filesystem."""
        process_mock = MagicMock()
        process_mock.stdin = io.BufferedWriter(io.BytesIO())
        process_mock.poll.return_value = None
        mock_subprocess.return_value = process_mock
        
        cmd = ReceiveCommand()
        result = cmd.filesystem(sample_filesystem)
        
        assert result is not None
        call_args = mock_subprocess.call_args[0][0]
        assert 'receive' in call_args
    
    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_receive_with_force(self, mock_subprocess, sample_filesystem):
        """Test receiving with force flag."""
        process_mock = MagicMock()
        process_mock.stdin = io.BufferedWriter(io.BytesIO())
        process_mock.poll.return_value = None
        mock_subprocess.return_value = process_mock
        
        cmd = ReceiveCommand()
        result = cmd.filesystem(sample_filesystem, force=True)
        
        call_args = mock_subprocess.call_args[0][0]
        assert '-F' in call_args
    
    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_receive_abort(self, mock_subprocess, sample_filesystem):
        """Test aborting a receive."""
        mock_subprocess.setup()
        
        cmd = ReceiveCommand()
        cmd.abort(sample_filesystem)
        
        call_args = mock_subprocess.call_args[0][0]
        assert 'receive' in call_args
        assert '-A' in call_args


class TestMountCommand:
    """Tests for MountCommand class."""
    
    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_mount_filesystem(self, mock_subprocess, sample_filesystem):
        """Test mounting a filesystem."""
        mock_subprocess.setup()
        
        cmd = MountCommand()
        cmd(sample_filesystem, None, None)
        
        call_args = mock_subprocess.call_args[0][0]
        assert 'mount' in call_args
        assert str(sample_filesystem) in call_args
    
    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_mount_all(self, mock_subprocess):
        """Test mounting all filesystems."""
        mock_subprocess.setup()
        
        cmd = MountCommand()
        cmd(None, None, None)
        
        call_args = mock_subprocess.call_args[0][0]
        assert 'mount' in call_args
        assert '-a' in call_args
    
    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_mount_with_overlay(self, mock_subprocess, sample_filesystem):
        """Test mounting with overlay flag."""
        mock_subprocess.setup()
        
        cmd = MountCommand()
        cmd(sample_filesystem, None, None, overlay=True)
        
        call_args = mock_subprocess.call_args[0][0]
        assert '-O' in call_args


class TestUnMountCommand:
    """Tests for UnMountCommand class."""
    
    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_unmount_filesystem(self, mock_subprocess, sample_filesystem):
        """Test unmounting a filesystem."""
        mock_subprocess.setup()
        
        cmd = UnMountCommand()
        cmd(sample_filesystem)
        
        call_args = mock_subprocess.call_args[0][0]
        assert 'unmount' in call_args
        assert str(sample_filesystem) in call_args
    
    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_unmount_all(self, mock_subprocess):
        """Test unmounting all filesystems."""
        mock_subprocess.setup()
        
        cmd = UnMountCommand()
        cmd(None)
        
        call_args = mock_subprocess.call_args[0][0]
        assert 'unmount' in call_args
        assert '-a' in call_args
    
    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_unmount_with_force(self, mock_subprocess, sample_filesystem):
        """Test force unmounting."""
        mock_subprocess.setup()
        
        cmd = UnMountCommand()
        cmd(sample_filesystem, force=True)
        
        call_args = mock_subprocess.call_args[0][0]
        assert '-f' in call_args


class TestStringListArgument:
    """Tests for StringListArgument helper methods."""
    
    @pytest.mark.unit
    def test_slist_to_list_from_string(self):
        """Test converting comma-separated string to list."""
        from libzfseasy.zfs import StringListArgument
        result = StringListArgument._slist_to_list('a,b,c')
        assert result == ['a', 'b', 'c']
    
    @pytest.mark.unit
    def test_slist_to_list_from_list(self):
        """Test that list input returns list."""
        from libzfseasy.zfs import StringListArgument
        result = StringListArgument._slist_to_list(['a', 'b', 'c'])
        assert result == ['a', 'b', 'c']
    
    @pytest.mark.unit
    def test_slist_to_str(self):
        """Test converting list to comma-separated string."""
        from libzfseasy.zfs import StringListArgument
        result = StringListArgument._slist_to_str(['a', 'b', 'c'])
        assert result == 'a,b,c'


class TestDatasetListArgument:
    """Tests for DatasetListArgument helper methods."""
    
    @pytest.mark.unit
    def test_dslist_to_list(self, sample_filesystem, sample_volume):
        """Test converting dataset list."""
        from libzfseasy.zfs import DatasetListArgument
        result = DatasetListArgument._dslist_to_list([sample_filesystem, sample_volume])
        assert len(result) == 2
        assert result[0] == sample_filesystem
    
    @pytest.mark.unit
    def test_dslist_to_str(self, sample_filesystem, sample_volume):
        """Test converting dataset list to string."""
        from libzfseasy.zfs import DatasetListArgument
        result = DatasetListArgument._dslist_to_str([sample_filesystem, sample_volume])
        assert 'testpool/filesystem' in result
        assert 'testpool/volume' in result
