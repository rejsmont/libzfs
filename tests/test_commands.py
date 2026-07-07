"""Unit tests for libzfseasy.zfs command classes."""

import pytest
import io
import threading
from unittest.mock import MagicMock, call
from libzfseasy.zfs import (
    ListCommand, CreateCommand, SnapshotCommand, BookmarkCommand,
    DestroyCommand, RenameCommand, CloneCommand, GetCommand, SetCommand,
    InheritCommand, SendCommand, ReceiveCommand, MountCommand, UnMountCommand,
    AllowCommand, UnAllowCommand, LoadKeyCommand, UnLoadKeyCommand, ChangeKeyCommand,
    PropertyCommand, StringListArgument, DatasetListArgument, ZFSListArgument,
    Command,
)
from libzfseasy.types import Dataset, Filesystem, Volume, Snapshot, SnapshotRange, Bookmark


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

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_list_property_value_with_space_not_over_split(self, mock_subprocess, sample_pool):
        """Regression test: `zfs list -H` output is tab-delimited, but a property
        value (e.g. mountpoint) may legitimately contain spaces. The parser must
        split on tabs only, not on arbitrary whitespace (str.split()), or the
        value gets truncated/mangled."""
        mock_subprocess.setup(stdout=[
            f'{sample_pool}/filesystem\tfilesystem\t/mnt/my data\n'
        ])

        cmd = ListCommand()
        result = cmd(properties=['mountpoint'])

        assert len(result) == 1
        assert result[0].name == f'{sample_pool}/filesystem'
        assert isinstance(result[0], Filesystem)
        assert result[0]['mountpoint'].value == '/mnt/my data'

    @pytest.mark.unit
    def test_line_to_object_property_value_with_space(self, sample_pool):
        """Direct unit test of _line_to_object against a tab-delimited line
        whose value contains an internal space."""
        properties = ['name', 'type', 'mountpoint']
        line = f'{sample_pool}/filesystem\tfilesystem\t/mnt/my data'

        obj = ListCommand._line_to_object(line, properties)

        assert obj.name == f'{sample_pool}/filesystem'
        assert isinstance(obj, Filesystem)
        assert obj['mountpoint'].value == '/mnt/my data'


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
        # Mock two subprocess calls: ListCommand for recursive listing, then SnapshotCommand
        mock_subprocess.setup_multi(
            (['testpool/filesystem\tfilesystem'],),  # ListCommand output
            ([''],)  # SnapshotCommand output (no output expected)
        )
        
        cmd = SnapshotCommand()
        result = cmd(sample_filesystem, 'snap1', recursive=True)
        
        # Result should be a list of snapshots
        assert isinstance(result, list)
    
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
        # Mock two subprocess calls: CloneCommand, then ListCommand for verification
        mock_subprocess.setup_multi(
            ([''],),  # CloneCommand output (no output expected)
            (['testpool/clone\tfilesystem'],)  # ListCommand output
        )
        
        cmd = CloneCommand()
        result = cmd(sample_snapshot, 'testpool/clone')
        
        assert isinstance(result, Dataset)
        assert result.name == 'testpool/clone'
        
        # Check the first call (clone command), not the last (list command)
        call_args = mock_subprocess.call_args_list[0][0][0]
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


class TestPropertyCommand:
    """Tests for PropertyCommand._get_props."""

    @pytest.mark.unit
    def test_get_props_with_flag(self):
        result = PropertyCommand._get_props({'compression': 'lz4'}, flag=True)
        assert result == ['-o', 'compression=lz4']

    @pytest.mark.unit
    def test_get_props_without_flag(self):
        result = PropertyCommand._get_props({'compression': 'lz4'}, flag=False)
        assert result == ['compression=lz4']

    @pytest.mark.unit
    def test_get_props_empty(self):
        assert PropertyCommand._get_props({}) == []


class TestStringListArgument:
    """Tests for StringListArgument helper methods."""
    
    @pytest.mark.unit
    def test_slist_to_list_none(self):
        assert StringListArgument._slist_to_list(None) == []

    @pytest.mark.unit
    def test_slist_to_list_from_string(self):
        """Test converting comma-separated string to list."""
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

    @pytest.mark.unit
    def test_dslist_to_list_none(self):
        assert DatasetListArgument._dslist_to_list(None) == []

    @pytest.mark.unit
    def test_dslist_to_list_invalid_type(self, sample_filesystem):
        bm = Bookmark(sample_filesystem, 'bm1')
        with pytest.raises(ValueError, match='Expected Filesystem'):
            DatasetListArgument._dslist_to_list([bm])


class TestZFSListArgument:
    """Tests for ZFSListArgument helper methods."""

    @pytest.mark.unit
    def test_zlist_to_list_none(self):
        assert ZFSListArgument._zlist_to_list(None) == []

    @pytest.mark.unit
    def test_zlist_to_list_invalid_type(self):
        with pytest.raises(ValueError, match='Expected Filesystem'):
            ZFSListArgument._zlist_to_list(['not_a_zfs_object'])


class TestListCommandOptions:
    """Tests for ListCommand sort/depth/type options."""

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_list_with_depth(self, mock_subprocess, sample_filesystem):
        mock_subprocess.setup(stdout=[f'{sample_filesystem.name}\tfilesystem\n'])
        cmd = ListCommand()
        cmd(roots=sample_filesystem, depth=2)
        call_args = mock_subprocess.call_args[0][0]
        assert '-d' in call_args
        assert '2' in call_args

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_list_with_type_filter(self, mock_subprocess):
        mock_subprocess.setup(stdout=['testpool/fs\tfilesystem\n'])
        cmd = ListCommand()
        cmd(types='filesystem')
        call_args = mock_subprocess.call_args[0][0]
        assert '-t' in call_args
        assert 'filesystem' in call_args

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_list_sort_string_asc(self, mock_subprocess):
        mock_subprocess.setup(stdout=['testpool/fs\tfilesystem\n'])
        cmd = ListCommand()
        cmd(sort='creation', asc=True)
        call_args = mock_subprocess.call_args[0][0]
        assert '-s' in call_args
        assert 'creation' in call_args

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_list_sort_string_desc(self, mock_subprocess):
        mock_subprocess.setup(stdout=['testpool/fs\tfilesystem\n'])
        cmd = ListCommand()
        cmd(sort='creation', asc=False)
        call_args = mock_subprocess.call_args[0][0]
        assert 'S' in call_args
        assert 'creation' in call_args

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_list_sort_iterable(self, mock_subprocess):
        mock_subprocess.setup(stdout=['testpool/fs\tfilesystem\n'])
        cmd = ListCommand()
        cmd(sort=['creation', 'compression'])
        call_args = mock_subprocess.call_args[0][0]
        assert call_args.count('-s') >= 2

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_list_sort_dict(self, mock_subprocess):
        # dict is Iterable, so the iterable branch fires and keys are used as sort fields
        mock_subprocess.setup(stdout=['testpool/fs\tfilesystem\n'])
        cmd = ListCommand()
        cmd(sort={'creation': True, 'compression': False})
        call_args = mock_subprocess.call_args[0][0]
        assert call_args.count('-s') >= 2


class TestCreateCommandExtended:
    """Additional CreateCommand coverage."""

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_create_auto_detect_filesystem(self, mock_subprocess, sample_pool):
        mock_subprocess.setup()
        cmd = CreateCommand()
        result = cmd(f'{sample_pool}/newfs')
        assert isinstance(result, Filesystem)

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_create_volume_with_parents(self, mock_subprocess, sample_pool):
        mock_subprocess.setup()
        cmd = CreateCommand()
        cmd.volume(f'{sample_pool}/newvol', '10G', parents=True)
        call_args = mock_subprocess.call_args[0][0]
        assert '-p' in call_args

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_create_volume_with_properties(self, mock_subprocess, sample_pool):
        mock_subprocess.setup()
        cmd = CreateCommand()
        cmd.volume(f'{sample_pool}/newvol', '10G', properties={'volsize': '10G'})
        call_args = mock_subprocess.call_args[0][0]
        assert '-o' in call_args


class TestBookmarkCommandExtended:
    """Additional BookmarkCommand coverage."""

    @pytest.mark.unit
    def test_bookmark_invalid_input(self, sample_filesystem):
        cmd = BookmarkCommand()
        with pytest.raises(ValueError, match='Expected Bookmark or Snapshot'):
            cmd(sample_filesystem, 'bm1')


class TestDestroyCommandExtended:
    """Additional DestroyCommand coverage."""

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_destroy_dispatch_dataset(self, mock_subprocess, sample_filesystem):
        mock_subprocess.setup(stdout=['destroy\ttestpool/filesystem\n'])
        cmd = DestroyCommand()
        cmd(sample_filesystem, destroy=True)
        call_args = mock_subprocess.call_args[0][0]
        assert 'destroy' in call_args

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_destroy_dispatch_snapshot(self, mock_subprocess, sample_snapshot):
        mock_subprocess.setup(stdout=['destroy\ttestpool/filesystem@snap1\n'])
        cmd = DestroyCommand()
        cmd(sample_snapshot, destroy=True)
        call_args = mock_subprocess.call_args[0][0]
        assert 'destroy' in call_args

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_destroy_dispatch_snapshot_range(self, mock_subprocess, sample_filesystem):
        snap1 = Snapshot(sample_filesystem, 'snap1')
        snap2 = Snapshot(sample_filesystem, 'snap2')
        sr = SnapshotRange(first=snap1, last=snap2)
        mock_subprocess.setup(stdout=['destroy\ttestpool/filesystem@snap1%snap2\n'])
        cmd = DestroyCommand()
        cmd(sr, destroy=True)
        call_args = mock_subprocess.call_args[0][0]
        assert 'destroy' in call_args

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_destroy_dispatch_bookmark(self, mock_subprocess, sample_bookmark):
        mock_subprocess.setup()
        cmd = DestroyCommand()
        cmd(sample_bookmark)
        call_args = mock_subprocess.call_args[0][0]
        assert 'destroy' in call_args

    @pytest.mark.unit
    def test_destroy_dispatch_invalid_type(self):
        from libzfseasy.types import Property
        cmd = DestroyCommand()
        with pytest.raises(ValueError, match='Expected Filesystem'):
            cmd(Property('x'), destroy=True)

    @pytest.mark.unit
    def test_destroy_base_clones_without_recursive_raises(self):
        with pytest.raises(ValueError, match='Clones can only be set to True'):
            DestroyCommand._base(destroy=True, recursive=False, clones=True)

    @pytest.mark.unit
    def test_destroy_dataset_invalid_type(self):
        cmd = DestroyCommand()
        with pytest.raises(ValueError, match='Expected Filesystem or Volume'):
            cmd.dataset('not_a_dataset', destroy=True)

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_destroy_dataset_force(self, mock_subprocess, sample_filesystem):
        mock_subprocess.setup(stdout=['destroy\ttestpool/filesystem\n'])
        cmd = DestroyCommand()
        cmd.dataset(sample_filesystem, destroy=True, force=True)
        call_args = mock_subprocess.call_args[0][0]
        assert '-f' in call_args

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_destroy_dataset_lazy(self, mock_subprocess, sample_filesystem):
        mock_subprocess.setup(stdout=['destroy\ttestpool/filesystem\n'])
        cmd = DestroyCommand()
        result = cmd.dataset(sample_filesystem, destroy=True, lazy=True)
        assert hasattr(result, '__next__')

    @pytest.mark.unit
    def test_destroy_snapshots_invalid_type(self, sample_filesystem):
        from libzfseasy.types import Property
        cmd = DestroyCommand()
        with pytest.raises(ValueError, match='Expected Snapshot or SnapshotRange'):
            cmd.snapshots([Property('x')], destroy=True)

    @pytest.mark.unit
    def test_destroy_snapshots_different_datasets(self, sample_pool):
        fs1 = Filesystem(f'{sample_pool}/fs1')
        fs2 = Filesystem(f'{sample_pool}/fs2')
        snap1 = Snapshot(fs1, 'snap1')
        snap2 = Snapshot(fs2, 'snap2')
        cmd = DestroyCommand()
        with pytest.raises(ValueError, match='Snapshots must come from the same dataset'):
            cmd.snapshots([snap1, snap2], destroy=True)

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_destroy_snapshots_force(self, mock_subprocess, sample_snapshot):
        mock_subprocess.setup(stdout=['destroy\ttestpool/filesystem@snap1\n'])
        cmd = DestroyCommand()
        cmd.snapshots(sample_snapshot, destroy=True, force=True)
        call_args = mock_subprocess.call_args[0][0]
        assert '-d' in call_args

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_destroy_snapshots_lazy(self, mock_subprocess, sample_snapshot):
        mock_subprocess.setup(stdout=['destroy\ttestpool/filesystem@snap1\n'])
        cmd = DestroyCommand()
        result = cmd.snapshots(sample_snapshot, destroy=True, lazy=True)
        assert hasattr(result, '__next__')

    @pytest.mark.unit
    def test_destroy_bookmark_no_destroy_returns_none(self, sample_bookmark):
        cmd = DestroyCommand()
        result = cmd.bookmark(sample_bookmark, destroy=False)
        assert result is None

    @pytest.mark.unit
    def test_destroy_bookmark_invalid_type(self, sample_filesystem):
        cmd = DestroyCommand()
        with pytest.raises(ValueError, match='Expected Bookmark'):
            cmd.bookmark(sample_filesystem, destroy=True)


class TestRenameCommandExtended:
    """Additional RenameCommand coverage."""

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_rename_dispatch_volume(self, mock_subprocess, sample_volume):
        mock_subprocess.setup()
        cmd = RenameCommand()
        result = cmd(sample_volume, 'testpool/newvol')
        assert isinstance(result, Volume)

    @pytest.mark.unit
    def test_rename_dispatch_invalid_type(self):
        ds = Dataset('pool/ds')
        cmd = RenameCommand()
        with pytest.raises(ValueError, match='Expected Filesystem, Volume or Snapshot'):
            cmd(ds, 'pool/new')

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_rename_filesystem_force(self, mock_subprocess, sample_filesystem):
        mock_subprocess.setup()
        cmd = RenameCommand()
        cmd.filesystem(sample_filesystem, 'testpool/new', force=True)
        call_args = mock_subprocess.call_args[0][0]
        assert '-f' in call_args

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_rename_filesystem_parents(self, mock_subprocess, sample_filesystem):
        mock_subprocess.setup()
        cmd = RenameCommand()
        cmd.filesystem(sample_filesystem, 'testpool/parent/new', parents=True)
        call_args = mock_subprocess.call_args[0][0]
        assert '-p' in call_args

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_rename_filesystem_no_mount(self, mock_subprocess, sample_filesystem):
        mock_subprocess.setup()
        cmd = RenameCommand()
        cmd.filesystem(sample_filesystem, 'testpool/new', mount=False)
        call_args = mock_subprocess.call_args[0][0]
        assert '-u' in call_args

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_rename_volume(self, mock_subprocess, sample_volume):
        mock_subprocess.setup()
        cmd = RenameCommand()
        result = cmd.volume(sample_volume, 'testpool/newvol')
        assert isinstance(result, Volume)
        assert result.name == 'testpool/newvol'

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_rename_snapshot_full_name(self, mock_subprocess, sample_snapshot):
        mock_subprocess.setup()
        cmd = RenameCommand()
        result = cmd.snapshot(sample_snapshot, 'testpool/filesystem@snap2')
        assert result.short == 'snap2'

    @pytest.mark.unit
    def test_rename_snapshot_cross_dataset_raises(self, sample_snapshot):
        cmd = RenameCommand()
        with pytest.raises(ValueError, match='Snapshots can only be renamed within the parent dataset'):
            cmd.snapshot(sample_snapshot, 'testpool/other@snap2')


class TestExecCaptureRegressions:
    """Regression tests for two bugs around Command's stdout/stderr draining
    (libzfseasy/zfs.py):

    1. A deadlock/hang when reading large stdout with little or no stderr. The
       fix drains stdout and stderr independently (stderr is read on a
       dedicated background thread via `Command._start_stderr_reader`, decoupled
       from the stdout-reading loop in `_exec_capture`) so that a full stdout
       pipe can never be blocked behind a stderr read, and vice versa. The
       mock_subprocess fixture does not use real OS pipes (it drives
       MagicMock.readline via side_effect lists), so it cannot reproduce the
       actual blocking behavior of subprocess.PIPE. The tests below therefore
       focus on what is fully testable through the fixture: correct, complete
       parsing of a large multi-row result, and that stderr draining runs to
       completion independently. A true no-hang guarantee under real OS pipe
       backpressure would require a real subprocess/pipe based test (see
       tests/test_real_zfs.py).

    2. An IndexError raised when a stderr line is present but strips down to an
       empty string (e.g. stderr == '\\n' or 'warning\\n\\n'), because the code
       used to do `error[0].upper()` on the stripped (possibly empty) string
       without checking its length first. This logic now lives inside the
       stderr-reader thread's loop.
    """

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_list_large_stdout_returns_all_rows(self, mock_subprocess, sample_pool):
        """Feed a large `zfs list -H` output (well over 64KB) with empty stderr
        and assert every row is parsed back correctly without hanging."""
        row_count = 5000
        rows = [f'{sample_pool}/fs{i}\tfilesystem\n' for i in range(row_count)]
        # Sanity-check the payload actually exceeds 64KB, per the bug report.
        assert sum(len(r) for r in rows) > 64 * 1024

        mock_subprocess.setup(stdout=rows, stderr='')

        cmd = ListCommand()
        result = cmd()

        assert len(result) == row_count
        assert result[0].name == f'{sample_pool}/fs0'
        assert result[-1].name == f'{sample_pool}/fs{row_count - 1}'
        assert all(isinstance(o, Filesystem) for o in result)

    @pytest.mark.unit
    def test_stderr_reader_blank_line_no_indexerror(self):
        """Direct unit test of the stderr-draining helper: a stderr line
        consisting only of a newline strips to '', and indexing error[0] must
        not raise IndexError. Because the reader runs on a background thread,
        an uncaught exception there would not normally fail the test, so we
        install a temporary threading.excepthook to detect it."""
        process = MagicMock()
        process.stderr.readline.side_effect = ['\n', '   \n', 'real problem\n', '']

        errors = []
        thread_exceptions = []
        original_hook = threading.excepthook

        def hook(args):
            thread_exceptions.append(args)

        threading.excepthook = hook
        try:
            thread = Command._start_stderr_reader(process, errors)
            thread.join(timeout=5)
        finally:
            threading.excepthook = original_hook

        assert not thread.is_alive(), 'stderr reader thread did not finish'
        assert not any(isinstance(exc.exc_value, IndexError) for exc in thread_exceptions), \
            'stderr reader raised IndexError on a blank stderr line'
        # Blank/whitespace-only lines contribute nothing; the real line is kept.
        assert errors == ['Real problem']

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_command_blank_stderr_line_no_indexerror(self, mock_subprocess, sample_pool):
        """End-to-end: run a command through the mocked subprocess where a
        stderr line is present but blank after stripping; must not raise
        IndexError, whether that happens inline or on a background thread."""
        mock_subprocess.setup(stdout=[''], stderr=['\n'])

        thread_exceptions = []
        original_hook = threading.excepthook

        def hook(args):
            thread_exceptions.append(args)

        threading.excepthook = hook
        try:
            cmd = CreateCommand()
            try:
                cmd.filesystem(f'{sample_pool}/newfs')
            except IndexError:
                pytest.fail('Command._exec raised IndexError on a blank stderr line')
            # Give any background stderr-reader thread a moment to run and
            # report via excepthook before we check.
            for t in threading.enumerate():
                if t is not threading.main_thread():
                    t.join(timeout=1)
        finally:
            threading.excepthook = original_hook

        assert not any(isinstance(exc.exc_value, IndexError) for exc in thread_exceptions), \
            'A background thread raised IndexError while processing a blank stderr line'
