"""Unit tests for libzfseasy.zfs command classes."""

import pytest
import io
import os
import signal
import subprocess
import sys
import threading
import time
from unittest.mock import MagicMock, call
from libzfseasy.zfs import (
    ListCommand, CreateCommand, SnapshotCommand, BookmarkCommand,
    DestroyCommand, RenameCommand, CloneCommand, GetCommand, SetCommand,
    InheritCommand, SendCommand, ReceiveCommand, MountCommand, UnMountCommand,
    AllowCommand, UnAllowCommand, LoadKeyCommand, UnLoadKeyCommand, ChangeKeyCommand,
    PropertyCommand, StringListArgument, DatasetListArgument, ZFSListArgument,
    Command, _TERMINATE_TIMEOUT,
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

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_set_failure_raises_and_leaves_properties_unchanged(self, mock_subprocess, sample_pool):
        """SetCommand._set() now calls ds.update() only *after* cls._exec()
        succeeds, so a failing subprocess must both raise and leave the
        dataset's in-memory properties exactly as they were before the call."""
        fs = Filesystem(f'{sample_pool}/filesystem', {'compression': 'lz4'})
        mock_subprocess.setup(returncode=1, stderr='cannot set property: invalid property value')

        cmd = SetCommand()
        with pytest.raises(Exception):
            cmd(fs, {'compression': 'gzip'})

        # Property must still hold its original value, not the attempted one.
        assert fs['compression'].value == 'lz4'


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

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_inherit_partial_failure_commits_processed_props_only(self, mock_subprocess, sample_pool):
        """InheritCommand._inherit() now commits ds.update({prop: None}) for
        each property right after its own `zfs inherit` call succeeds
        (per-prop commit), instead of clearing every property up front. If a
        call partway through the list fails, properties already processed
        must be cleared, but the failed property and anything after it must
        retain their original values."""
        fs = Filesystem(f'{sample_pool}/filesystem', {
            'compression': 'lz4', 'quota': '10G', 'atime': 'on',
        })
        mock_subprocess.setup_multi(
            ([''],),               # inherit compression -- succeeds
            ([''], 'boom', 1),     # inherit quota -- fails
            # atime's `zfs inherit` call is never reached
        )

        cmd = InheritCommand()
        with pytest.raises(Exception):
            cmd(fs, ['compression', 'quota', 'atime'])

        # Processed before the failure: cleared (inherited -> no local value).
        assert fs['compression'] is None
        # The failed call and anything after it: untouched.
        assert fs['quota'].value == '10G'
        assert fs['atime'].value == 'on'


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
    def test_list_types_none_omits_dash_t(self, mock_subprocess):
        """types=None must not emit a bare '-t' flag (locks in existing,
        already-correct ListCommand guard: `if types:` before appending)."""
        mock_subprocess.setup(stdout=['testpool/fs\tfilesystem\n'])
        cmd = ListCommand()
        cmd(types=None)
        call_args = mock_subprocess.call_args[0][0]
        assert '-t' not in call_args

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

    @pytest.mark.unit
    def test_exec_capture_eof_but_poll_none_calls_wait(self):
        """Regression test for the EOF-but-still-running race: stdout hits
        EOF (readline() == '') while poll() still reports None (the child
        hasn't been reaped by the OS yet). _exec_capture must resolve the
        return code by blocking on process.wait() rather than returning
        None and leaving the caller's `while True` loop to spin on
        readline()/poll() until the child eventually shows up as exited."""
        process = MagicMock()
        process.stdout.readline.return_value = ''
        process.poll.return_value = None
        process.wait.return_value = 0

        result = Command._exec_capture(process, [])

        process.wait.assert_called_once()
        assert result is False

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_exec_out_abandoned_generator_terminates_child(self, mock_subprocess):
        """_exec_out() is a generator; if the caller abandons it mid-iteration
        (breaks out of a `for` loop, or just drops the reference and lets it
        get garbage collected) while the child is still running, the
        generator's `finally` block must terminate the child, reap it, and
        join the stderr-reader thread -- rather than leaking a background
        thread and an orphaned/zombie subprocess."""
        process = MagicMock()
        process.stdout.readline.side_effect = ['row1\n', 'row2\n', 'row3\n', '']
        process.stderr.readline.return_value = ''
        process.poll.return_value = None  # still "running" from the caller's POV
        process.wait.return_value = 0
        mock_subprocess.return_value = process

        gen = Command._exec_out(['zfs', 'list'])
        first = next(gen)
        assert first == 'row1'

        # Abandon the generator mid-iteration.
        gen.close()

        process.terminate.assert_called_once()
        assert process.wait.called

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_exec_out_abandoned_generator_escalates_to_kill_on_terminate_timeout(self, mock_subprocess):
        """Regression test for the SIGTERM-ignore escalation path: if the
        abandoned child doesn't honor terminate() within _TERMINATE_TIMEOUT
        seconds (process.wait(timeout=...) raises TimeoutExpired), the
        finally block must escalate to kill() and reap the child rather than
        blocking forever (or leaving a zombie/orphaned process). The existing
        abandoned-generator test uses a MagicMock whose wait() never raises,
        so it doesn't exercise this fallback."""
        process = MagicMock()
        process.stdout.readline.side_effect = ['row1\n', 'row2\n', 'row3\n', '']
        process.stderr.readline.return_value = ''
        process.poll.return_value = None  # still "running" from the caller's POV
        process.wait.side_effect = [
            subprocess.TimeoutExpired(cmd='zfs', timeout=_TERMINATE_TIMEOUT),
            0,  # wait() after kill()
            0,  # final wait() after stderr_thread.join()
        ]
        mock_subprocess.return_value = process

        gen = Command._exec_out(['zfs', 'list'])
        first = next(gen)
        assert first == 'row1'

        # Abandon the generator mid-iteration; the child ignores SIGTERM.
        gen.close()

        process.terminate.assert_called_once()
        process.kill.assert_called_once()
        assert process.wait.call_count == 3


class _RealChildHarness:
    """Drives `Command._exec_out` / `_exec` against a REAL child (`sys.executable -c ...`), not
    the `mock_subprocess` fixture.

    The MagicMock tests above cannot reproduce OS pipe back-pressure or a real EOF/exit race, so
    these use real pipes. They run in the default tier: the child is the test interpreter, never
    `zfs`. A watchdog thread bounds every run, so a deadlock regression fails the test (and kills
    the child) instead of hanging the suite.
    """

    def __init__(self, monkeypatch):
        self.procs = []
        self.polls = {}
        self.stderr_threads = []
        harness = self
        real_popen = subprocess.Popen  # the (guarded) class; subclassing keeps the guard

        class RecordingPopen(real_popen):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                harness.procs.append(self)
                harness.polls[self.pid] = 0

            def poll(self):
                harness.polls[self.pid] += 1
                return super().poll()

        monkeypatch.setattr(subprocess, 'Popen', RecordingPopen)
        real_start = Command._start_stderr_reader

        def recording_start(process, errors):
            thread = real_start(process, errors)
            harness.stderr_threads.append(thread)
            return thread

        monkeypatch.setattr(Command, '_start_stderr_reader', staticmethod(recording_start))

    @staticmethod
    def cmd(script):
        return [sys.executable, '-c', script]

    def run(self, fn, timeout=30):
        """Call `fn()` on a watchdog thread; return (result, exception)."""
        box = {}

        def target():
            try:
                box['result'] = fn()
            except BaseException as exc:  # noqa: BLE001 - reported to the test
                box['exc'] = exc

        thread = threading.Thread(target=target, daemon=True)
        thread.start()
        thread.join(timeout)
        if thread.is_alive():
            self.kill_all()
            thread.join(10)
            pytest.fail(f'Command did not finish within {timeout}s: pipe deadlock or spin regression')
        return box.get('result'), box.get('exc')

    def kill_all(self):
        for proc in self.procs:
            if proc.returncode is None:
                try:
                    proc.kill()
                except OSError:
                    pass
                try:
                    proc.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    pass


@pytest.fixture
def real_child(monkeypatch):
    harness = _RealChildHarness(monkeypatch)
    yield harness
    harness.kill_all()  # never leave a child behind, whatever the test did


_INTERLEAVED_CHILD = '''
import sys
for i in range(300):
    sys.stderr.write("warn %d " % i + "e" * 300 + "\\n"); sys.stderr.flush()
    sys.stdout.write("row %d " % i + "o" * 300 + "\\n"); sys.stdout.flush()
sys.exit({rc})
'''


class TestExecRealPipes:
    """The deadlock/spin fixes in `Command._exec_out` / `_exec_capture` (commit a8a3d88), proven
    with real OS pipes. Each test fails against the pre-fix code: (a) deadlocks without the
    stderr drain thread, (b) leaks the child and thread without the generator `finally`, (c)
    spins on readline()/poll() without the `wait()` at EOF."""

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_interleaved_large_stdout_and_stderr_returns_all_rows(self, real_child):
        """(a) >64 KB on BOTH pipes, interleaved: nothing blocks, every row comes back."""
        script = _INTERLEAVED_CHILD.format(rc=0)
        rows, exc = real_child.run(lambda: list(Command._exec_out(real_child.cmd(script))))

        assert exc is None
        assert len(rows) == 300
        assert rows[0].startswith('row 0 ooo') and rows[-1].startswith('row 299 ooo')
        assert sum(len(r) for r in rows) > 64 * 1024
        # fully consumed run: child reaped, stderr reader joined
        assert real_child.procs[0].returncode == 0
        assert not any(t.is_alive() for t in real_child.stderr_threads)

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_interleaved_large_output_with_failure_raises_collected_stderr(self, real_child):
        """(a) Same volume, rc != 0: every row is yielded first, then the exception carries ALL
        stderr lines (>64 KB of them), capitalised and in order."""
        script = _INTERLEAVED_CHILD.format(rc=3)
        seen = []

        def consume():
            for row in Command._exec_out(real_child.cmd(script)):
                seen.append(row)

        _, exc = real_child.run(consume)

        assert isinstance(exc, Exception)
        assert len(seen) == 300
        lines = str(exc).split('\n')
        assert len(lines) == 300
        assert lines[0].startswith('Warn 0 eee') and lines[-1].startswith('Warn 299 eee')
        assert sum(len(l) for l in lines) > 64 * 1024
        assert real_child.procs[0].returncode == 3
        assert not any(t.is_alive() for t in real_child.stderr_threads)

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_exec_interleaved_large_output_failure_raises(self, real_child):
        """(a) `Command._exec` (the no-output variant) shares the machinery."""
        script = _INTERLEAVED_CHILD.format(rc=1)
        _, exc = real_child.run(lambda: Command._exec(real_child.cmd(script)))
        assert isinstance(exc, Exception)
        assert len(str(exc).split('\n')) == 300

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_abandoned_generator_reaps_running_child(self, real_child):
        """(b) A long-running child, generator closed after one row: the child is terminated and
        reaped (no zombie, no orphan), and the stderr reader thread has ended."""
        script = (
            'import sys, time\n'
            'sys.stdout.write("first\\n"); sys.stdout.flush()\n'
            'sys.stderr.write("noise\\n"); sys.stderr.flush()\n'
            'time.sleep(120)\n'
        )
        gen = Command._exec_out(real_child.cmd(script))
        assert next(gen) == 'first'
        proc = real_child.procs[0]
        assert proc.returncode is None  # genuinely still running

        gen.close()

        assert proc.returncode == -signal.SIGTERM
        assert real_child.stderr_threads and not any(t.is_alive() for t in real_child.stderr_threads)
        with pytest.raises(ProcessLookupError):  # reaped: the pid is gone, not a zombie
            os.kill(proc.pid, 0)

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_abandoned_generator_kills_child_that_ignores_sigterm(self, real_child, monkeypatch):
        """(b) The SIGTERM-ignoring child is escalated to SIGKILL after _TERMINATE_TIMEOUT."""
        monkeypatch.setattr('libzfseasy.zfs._TERMINATE_TIMEOUT', 0.3)
        script = (
            'import signal, sys, time\n'
            'signal.signal(signal.SIGTERM, signal.SIG_IGN)\n'
            'sys.stdout.write("ready\\n"); sys.stdout.flush()\n'
            'time.sleep(120)\n'
        )
        gen = Command._exec_out(real_child.cmd(script))
        assert next(gen) == 'ready'
        proc = real_child.procs[0]

        _, exc = real_child.run(gen.close, timeout=30)

        assert exc is None
        assert proc.returncode == -signal.SIGKILL
        assert not any(t.is_alive() for t in real_child.stderr_threads)

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_abandoned_generator_leaves_no_session_orphan(self, real_child):
        """(b) The suite's own orphan check finds nothing for the abandoned child's pid."""
        from tests._hermetic import find_orphans

        script = 'import sys, time\nprint("first", flush=True)\ntime.sleep(120)\n'
        gen = Command._exec_out(real_child.cmd(script))
        assert next(gen) == 'first'
        pid = real_child.procs[0].pid
        gen.close()

        marker = os.environ.get('ZFSB_TEST_SESSION')
        if not marker:
            pytest.skip('no test-session marker in this run')
        orphans = find_orphans(marker, grace=1.0, fork_dir=None)
        assert pid not in [o[0] for o in orphans]

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_stdout_eof_before_exit_does_not_spin(self, real_child):
        """(c) The child closes stdout, lingers 0.3 s, then exits 0. `_exec_capture` must block in
        wait() rather than looping readline()/poll(): the poll count stays tiny (a spinning
        regression makes thousands of calls in that window)."""
        script = (
            'import os, time\n'
            'os.close(1)\n'
            'time.sleep(0.3)\n'
            'os._exit(0)\n'
        )
        start = time.monotonic()
        rows, exc = real_child.run(lambda: list(Command._exec_out(real_child.cmd(script))))
        elapsed = time.monotonic() - start

        assert exc is None
        assert rows == []
        assert elapsed >= 0.25  # it really did wait for the exit
        proc = real_child.procs[0]
        assert proc.returncode == 0
        assert real_child.polls[proc.pid] <= 5, f'poll() called {real_child.polls[proc.pid]} times: spin'

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_stdout_eof_before_failing_exit_raises_without_spinning(self, real_child):
        """(c) Same window, non-zero exit: the stderr text is raised, still no spin."""
        script = (
            'import os, sys, time\n'
            'sys.stderr.write("cannot open dataset\\n"); sys.stderr.flush()\n'
            'os.close(1)\n'
            'time.sleep(0.3)\n'
            'os._exit(1)\n'
        )
        _, exc = real_child.run(lambda: list(Command._exec_out(real_child.cmd(script))))

        assert isinstance(exc, Exception) and str(exc) == 'Cannot open dataset'
        proc = real_child.procs[0]
        assert real_child.polls[proc.pid] <= 5


class TestSpacesInsideNames:
    """Dataset components and snapshot/bookmark short names may contain spaces INSIDE (never
    leading/trailing); names are matched with fullmatch + ASCII. The command layer must keep such a
    name as ONE argv element (Popen gets a list, no shell) and must refuse a trailing newline or
    non-ASCII name before any process is started."""

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_list_returns_a_snapshot_with_a_spaced_short_name(self, mock_subprocess):
        mock_subprocess.setup(stdout=['tank/data@before upgrade\tsnapshot\n'])

        result = ListCommand()()

        assert len(result) == 1
        assert isinstance(result[0], Snapshot)
        assert result[0].short == 'before upgrade'
        assert result[0].name == 'tank/data@before upgrade'
        assert result[0].dataset.name == 'tank/data'

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_list_returns_a_filesystem_with_a_spaced_component(self, mock_subprocess):
        mock_subprocess.setup(stdout=['tank/my data\tfilesystem\n', 'tank/my data/sub dir\tfilesystem\n'])

        result = ListCommand()()

        assert [type(o) for o in result] == [Filesystem, Filesystem]
        assert [o.name for o in result] == ['tank/my data', 'tank/my data/sub dir']

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_list_with_properties_keeps_a_spaced_snapshot_row_whole(self, mock_subprocess):
        """The columns ListCommand requests (name,type,<props>) are tab-separated, so a space in the
        name must not shift the property columns."""
        mock_subprocess.setup(stdout=['tank/data@before upgrade\tsnapshot\t1024\n'])

        result = ListCommand()(properties=['used'])

        assert len(result) == 1 and isinstance(result[0], Snapshot)
        assert result[0].short == 'before upgrade'
        assert result[0]['used'].value == '1024'
        argv = mock_subprocess.call_args[0][0]
        assert argv[argv.index('-o') + 1] == 'name,type,used'

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_list_roots_with_a_spaced_name_is_one_argv_element(self, mock_subprocess):
        mock_subprocess.setup(stdout=['tank/my data\tfilesystem\n'])

        ListCommand()(roots=Filesystem('tank/my data'))

        argv = mock_subprocess.call_args[0][0]
        assert argv.count('tank/my data') == 1
        assert 'tank/my' not in argv and 'data' not in argv

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_snapshot_with_a_spaced_name_is_one_argv_element(self, mock_subprocess):
        mock_subprocess.setup()
        fs = Filesystem('tank/my data')

        result = SnapshotCommand()(fs, 'before upgrade')

        assert isinstance(result, Snapshot) and result.short == 'before upgrade'
        argv = mock_subprocess.call_args[0][0]
        assert isinstance(argv, list)
        assert argv[-1] == 'tank/my data@before upgrade'
        assert argv.count('tank/my data@before upgrade') == 1
        assert not any(a in ('tank/my', 'data@before', 'upgrade') for a in argv)

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_destroy_dataset_with_a_spaced_name_is_one_argv_element(self, mock_subprocess):
        mock_subprocess.setup(stdout=['destroy\ttank/my data\n'])

        result = DestroyCommand().dataset(Filesystem('tank/my data'), destroy=True)

        assert result == ['tank/my data']
        argv = mock_subprocess.call_args[0][0]
        assert argv[-1] == 'tank/my data' and argv.count('tank/my data') == 1
        assert 'tank/my' not in argv and 'data' not in argv

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_destroy_snapshots_with_spaced_names_is_one_argv_element(self, mock_subprocess):
        """Several snapshots of one dataset are joined with ',' into a single argument; a space
        inside a short name does not split it."""
        mock_subprocess.setup(stdout=['destroy\ttank/data@before upgrade\n'])
        fs = Filesystem('tank/data')
        snaps = [Snapshot(fs, 'before upgrade'), Snapshot(fs, 'after upgrade')]

        DestroyCommand().snapshots(snaps, destroy=True)

        argv = mock_subprocess.call_args[0][0]
        assert argv[-1] == 'tank/data@before upgrade,after upgrade'
        assert argv.count('tank/data@before upgrade,after upgrade') == 1
        assert not any(a in ('upgrade', 'before', 'after') or a.startswith('after') for a in argv)

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_destroy_bookmark_with_a_spaced_name_is_one_argv_element(self, mock_subprocess):
        mock_subprocess.setup()
        bookmark = Bookmark(Filesystem('tank/my data'), 'my mark')

        DestroyCommand().bookmark(bookmark, destroy=True)

        argv = mock_subprocess.call_args[0][0]
        assert argv[-1] == 'tank/my data#my mark' and argv.count('tank/my data#my mark') == 1

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_rename_filesystem_with_spaced_names_is_two_argv_elements(self, mock_subprocess):
        mock_subprocess.setup()

        RenameCommand().filesystem(Filesystem('tank/old name'), 'tank/new name')

        argv = mock_subprocess.call_args[0][0]
        assert argv[-2:] == ['tank/old name', 'tank/new name']
        assert argv.count('tank/old name') == 1 and argv.count('tank/new name') == 1

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_rename_snapshot_with_spaced_names_is_two_argv_elements(self, mock_subprocess):
        mock_subprocess.setup()
        snap = Snapshot(Filesystem('tank/data'), 'old snap')

        result = RenameCommand().snapshot(snap, 'new snap')

        assert result.short == 'new snap'
        argv = mock_subprocess.call_args[0][0]
        assert argv[-2:] == ['tank/data@old snap', 'tank/data@new snap']

    @pytest.mark.unit
    @pytest.mark.subprocess
    @pytest.mark.parametrize('bad', ['tank/bad\n', 'tank/bad name\n', 'tank/däta', 'tank/٣x'],
                             ids=['newline', 'space-newline', 'non-ascii', 'arabic-digit'])
    def test_a_bad_dataset_name_is_refused_before_any_popen(self, mock_subprocess, bad):
        mock_subprocess.setup()
        with pytest.raises(ValueError):
            CreateCommand().filesystem(bad)
        with pytest.raises(ValueError):
            Filesystem(bad)
        mock_subprocess.assert_not_called()

    @pytest.mark.unit
    @pytest.mark.subprocess
    @pytest.mark.parametrize('bad', ['snap\n', 'my snap\n', 'snäp', ' lead', 'trail '],
                             ids=['newline', 'space-newline', 'non-ascii', 'leading-space', 'trailing-space'])
    def test_a_bad_snapshot_name_is_refused_before_any_popen(self, mock_subprocess, bad):
        mock_subprocess.setup()
        fs = Filesystem('tank/data')
        with pytest.raises(ValueError):
            SnapshotCommand()(fs, bad)
        with pytest.raises(ValueError):
            Bookmark(fs, bad)
        with pytest.raises(ValueError):
            RenameCommand().snapshot(Snapshot(fs, 'ok'), bad)
        mock_subprocess.assert_not_called()

    @pytest.mark.unit
    @pytest.mark.subprocess
    @pytest.mark.parametrize('bad', ['tank/new\n', 'tank/näw'], ids=['newline', 'non-ascii'])
    def test_a_bad_rename_target_is_refused_before_any_popen(self, mock_subprocess, bad):
        mock_subprocess.setup()
        with pytest.raises(ValueError):
            RenameCommand().filesystem(Filesystem('tank/old'), bad)
        mock_subprocess.assert_not_called()


class TestGetCommandLenientProperties:
    """`zfs get` output is parsed leniently (unknown well-formed native names become extras); caller names stay strict."""

    QUOTA_NAMES = ['defaultuserquota', 'defaultgroupquota', 'defaultprojectquota', 'defaultuserobjquota',
                   'defaultgroupobjquota', 'defaultprojectobjquota']

    @classmethod
    def _output(cls):
        fs = 'testpool/filesystem'
        lines = [
            fs + '\tname\t' + fs + '\t-\t-\n',
            fs + '\ttype\tfilesystem\t-\t-\n',
            fs + '\tcompression\tlz4\t-\tlocal\n',
        ]
        for i, name in enumerate(cls.QUOTA_NAMES):
            lines.append('%s\t%s\t%dG\t%dM\tlocal\n' % (fs, name, i + 1, i + 1))
        lines += [
            fs + '\tfutureprop\tfuture-value\trecv-value\tinherited from testpool\n',
            fs + '\torg.example:note\thello\t-\tlocal\n',
            fs + '\tvolsize\t1G\t-\tdefault\n',
            fs + '@snap\tname\t' + fs + '@snap\t-\t-\n',
            fs + '@snap\ttype\tsnapshot\t-\t-\n',
            fs + '@snap\tfutureprop\tsnapval\t-\t-\n',
        ]
        return lines

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_get_all_parses_default_quotas_and_extras(self, mock_subprocess):
        mock_subprocess.setup(stdout=self._output())

        result = GetCommand()(properties='all')

        assert len(result) == 2
        fs, snap = result
        assert isinstance(fs, Filesystem)
        assert isinstance(snap, Snapshot)
        for i, name in enumerate(self.QUOTA_NAMES):
            assert fs[name].value == '%dG' % (i + 1)
            assert fs[name].source == 'local'
            assert fs[name].received == '%dM' % (i + 1)
            assert name not in fs._extra_props
            assert Filesystem._prop_names.index(name) in fs._props
        assert fs['compression'].value == 'lz4'
        assert fs['org.example:note'].value == 'hello'
        assert list(fs._user_props) == ['org.example:note']
        # unknown well-formed native names (and a name known only to another type) land in extras
        assert fs['futureprop'].value == 'future-value'
        assert fs['futureprop'].source == 'inherited from testpool'
        assert fs['futureprop'].received == 'recv-value'
        assert fs.futureprop.value == 'future-value'
        assert 'futureprop' in fs
        assert sorted(fs._extra_props) == ['futureprop', 'volsize']
        # the snapshot row is parsed too
        assert snap['futureprop'].value == 'snapval'
        assert snap._extra_props.keys() == {'futureprop'}

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_sources_filter_applies_to_extras(self, mock_subprocess):
        mock_subprocess.setup(stdout=self._output())

        result = GetCommand()(properties='all', sources='local')

        fs = result[0]
        assert fs['defaultuserquota'].value == '1G'
        assert fs['compression'].value == 'lz4'
        assert 'futureprop' not in fs._extra_props     # source 'inherited from ...'
        assert 'volsize' not in fs._extra_props        # source 'default'

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_sources_filter_inherited_matches_prefix_for_extras(self, mock_subprocess):
        mock_subprocess.setup(stdout=self._output())

        result = GetCommand()(properties='all', sources='inherited')

        fs = result[0]
        # 'inherited from testpool' matches 'inherited' (prefix); the full text is kept on the Property
        assert list(fs._extra_props) == ['futureprop']
        assert fs['futureprop'].source == 'inherited from testpool'
        assert 'volsize' not in fs._extra_props        # 'default'
        assert 'compression' not in fs                 # 'local'

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_sources_filter_inherited_matches_known_properties(self, mock_subprocess):
        fs = 'testpool/filesystem'
        mock_subprocess.setup(stdout=[
            fs + '\tname\t' + fs + '\t-\t-\n',
            fs + '\ttype\tfilesystem\t-\t-\n',
            fs + '\tcompression\tlz4\t-\tinherited from testpool\n',
            fs + '\tatime\ton\t-\tlocal\n',
            fs + '\tquota\t1G\t-\tinheritedx\n',
            fs + '\tfutureprop\tv\t-\tinherited from testpool\n',
        ])

        result = GetCommand()(properties='all', sources='inherited')

        obj = result[0]
        assert obj['compression'].value == 'lz4'
        assert obj['compression'].source == 'inherited from testpool'
        assert 'atime' not in obj                      # local
        assert 'quota' not in obj                      # 'inheritedx' is not 'inherited' nor 'inherited from ...'
        assert 'futureprop' in obj

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_sources_filter_other_sources_stay_exact(self, mock_subprocess):
        mock_subprocess.setup(stdout=self._output())
        result = GetCommand()(properties='all', sources='default')
        assert list(result[0]._extra_props) == ['volsize']

        mock_subprocess.setup(stdout=self._output())
        result = GetCommand()(properties='all', sources='local')
        assert 'volsize' not in result[0]._extra_props
        assert 'futureprop' not in result[0]._extra_props

        mock_subprocess.setup(stdout=self._output())
        result = GetCommand()(properties='all', sources=['default', 'inherited'])
        assert sorted(result[0]._extra_props) == ['futureprop', 'volsize']

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_malformed_native_name_from_zfs_is_still_refused(self, mock_subprocess):
        fs = 'testpool/filesystem'
        mock_subprocess.setup(stdout=[
            fs + '\tname\t' + fs + '\t-\t-\n',
            fs + '\ttype\tfilesystem\t-\t-\n',
            fs + '\tBad-Prop\tv\t-\tlocal\n',
        ])
        with pytest.raises(ValueError):
            GetCommand()(properties='all')

    @pytest.mark.unit
    def test_build_uses_lenient_update(self):
        from libzfseasy.types import Property
        obj = GetCommand._build('testpool/fs', 'filesystem', {'futureprop': Property('x', 'local', '-')})
        assert isinstance(obj, Filesystem)
        assert obj['futureprop'].value == 'x'
        vol = GetCommand._build('testpool/vol', 'volume', {'volsize': Property('1G', 'local', '-')})
        assert isinstance(vol, Volume)
        assert vol._extra_props == {}

    @pytest.mark.unit
    @pytest.mark.subprocess
    @pytest.mark.parametrize('properties', ['futureprop', ['compression', 'futureprop'], ['all', 'futureprop']])
    def test_caller_supplied_unknown_name_raises_before_popen(self, mock_subprocess, sample_filesystem, properties):
        mock_subprocess.setup()  # a regression then fails fast instead of hanging on an unconfigured mock
        with pytest.raises(ValueError):
            GetCommand()(sample_filesystem, properties)
        assert not mock_subprocess.called


class TestCallerSuppliedUnknownNamesStayStrict:

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_list_properties_rejected_before_popen(self, mock_subprocess):
        mock_subprocess.setup()  # a regression then fails fast instead of hanging on an unconfigured mock
        with pytest.raises(ValueError):
            ListCommand()(properties='futureprop')
        with pytest.raises(ValueError):
            ListCommand()(properties=['compression', 'futureprop'])
        assert not mock_subprocess.called

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_list_sort_rejected_before_popen(self, mock_subprocess):
        mock_subprocess.setup()
        with pytest.raises(ValueError):
            ListCommand()(sort='futureprop')
        assert not mock_subprocess.called

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_create_filesystem_and_volume_reject_before_popen(self, mock_subprocess):
        mock_subprocess.setup()
        with pytest.raises(ValueError):
            CreateCommand()('testpool/new', {'futureprop': 'v'})
        with pytest.raises(ValueError):
            CreateCommand().volume('testpool/vol', '1G', {'futureprop': 'v'})
        assert not mock_subprocess.called

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_set_rejects_and_leaves_no_extra(self, mock_subprocess, sample_pool):
        fs = Filesystem(sample_pool + '/fs')
        mock_subprocess.setup()
        with pytest.raises(ValueError):
            SetCommand()(fs, {'futureprop': 'v'})
        assert not mock_subprocess.called
        assert fs._extra_props == {}
        assert list(fs.properties) == []

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_inherit_rejects_unknown_name(self, mock_subprocess, sample_pool):
        fs = Filesystem(sample_pool + '/fs')
        mock_subprocess.setup()
        with pytest.raises(ValueError):
            InheritCommand()(fs, 'futureprop')
        assert not mock_subprocess.called
        assert fs._extra_props == {}

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_set_default_quota_is_accepted(self, mock_subprocess, sample_pool):
        fs = Filesystem(sample_pool + '/fs')
        mock_subprocess.setup()
        SetCommand()(fs, {'defaultuserquota': '1G'})
        assert 'defaultuserquota=1G' in mock_subprocess.call_args[0][0]
        assert fs['defaultuserquota'].value == '1G'

    @pytest.mark.unit
    @pytest.mark.subprocess
    @pytest.mark.parametrize('properties', [
        {'volblocksize': '8K'},                      # valid for a volume, not a filesystem
        {'Compression': 'lz4'},                      # malformed
        {'compression': 'lz4', 'futureprop': 'v'},   # bad name after a good one
        {'bad name': 'v'},
    ])
    def test_set_checks_every_name_before_exec(self, mock_subprocess, sample_pool, properties):
        fs = Filesystem(sample_pool + '/fs')
        mock_subprocess.setup()
        with pytest.raises(ValueError):
            SetCommand()(fs, properties)
        assert not mock_subprocess.called
        assert list(fs.properties) == []

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_set_valid_known_and_user_names_still_run_and_update(self, mock_subprocess, sample_pool):
        fs = Filesystem(sample_pool + '/fs')
        mock_subprocess.setup()
        SetCommand()(fs, {'compression': 'lz4', 'org:note': 'hi'})
        assert mock_subprocess.call_count == 1
        args = mock_subprocess.call_args[0][0]
        assert 'compression=lz4' in args and 'org:note=hi' in args
        assert fs['compression'].value == 'lz4'
        assert fs['org:note'].value == 'hi'

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_set_volume_property_on_volume_still_works(self, mock_subprocess, sample_pool):
        vol = Volume(sample_pool + '/vol')
        mock_subprocess.setup()
        SetCommand()(vol, {'volblocksize': '8K'})
        assert vol['volblocksize'].value == '8K'

    @pytest.mark.unit
    @pytest.mark.subprocess
    @pytest.mark.parametrize('props', ['volblocksize', 'futureprop', 'Compression', ['compression', 'futureprop'],
                                       ['futureprop', 'compression'], ['compression', 'atime', 'volblocksize']])
    def test_inherit_checks_all_names_before_running_anything(self, mock_subprocess, sample_pool, props):
        fs = Filesystem(sample_pool + '/fs', {'compression': 'lz4', 'atime': 'on'})
        mock_subprocess.setup()
        with pytest.raises(ValueError):
            InheritCommand()(fs, props)
        assert not mock_subprocess.called
        assert fs['compression'].value == 'lz4'        # nothing was inherited in memory either
        assert fs['atime'].value == 'on'

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_inherit_valid_names_still_run_and_update(self, mock_subprocess, sample_pool):
        fs = Filesystem(sample_pool + '/fs', {'compression': 'lz4', 'atime': 'on', 'org:k': 'v'})
        mock_subprocess.setup_multi(('',), ('',), ('',))
        InheritCommand()(fs, ['compression', 'atime', 'org:k'])
        assert mock_subprocess.call_count == 3
        assert [c[0][0][2] for c in mock_subprocess.call_args_list] == ['compression', 'atime', 'org:k']
        assert 'compression' not in fs and 'atime' not in fs and 'org:k' not in fs


class TestRenameKeepsExtraProperties:
    """Rename copies the old object's properties (including parsed extras) into the new object."""

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_rename_filesystem_with_extras(self, mock_subprocess):
        fs = Filesystem('testpool/old')
        fs.update({'compression': 'lz4', 'org:k': 'v', 'futureprop': 'x', 'volsize': '1G'}, lenient=True)
        mock_subprocess.setup()

        new = RenameCommand().filesystem(fs, 'testpool/new')

        assert isinstance(new, Filesystem)
        assert new.name == 'testpool/new'
        assert new['compression'].value == 'lz4'
        assert new['org:k'].value == 'v'
        assert new['futureprop'].value == 'x'
        assert new['volsize'].value == '1G'            # a volume-only name carried as an extra on a Filesystem
        assert sorted(new._extra_props) == ['futureprop', 'volsize']
        assert mock_subprocess.call_count == 1

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_rename_volume_with_extras(self, mock_subprocess):
        vol = Volume('testpool/oldvol')
        vol.update({'volsize': '1G', 'futureprop': 'x', 'quota': '5G'}, lenient=True)
        mock_subprocess.setup()

        new = RenameCommand().volume(vol, 'testpool/newvol')

        assert isinstance(new, Volume)
        assert new['volsize'].value == '1G'
        assert new['futureprop'].value == 'x'
        assert 'quota' in new._extra_props

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_rename_snapshot_with_extras(self, mock_subprocess):
        snap = Snapshot(Filesystem('testpool/fs'), 'old')
        snap.update({'used': '1M', 'futureprop': 'x'}, lenient=True)
        mock_subprocess.setup()

        new = RenameCommand().snapshot(snap, 'new')

        assert isinstance(new, Snapshot)
        assert new.short == 'new'
        assert new['used'].value == '1M'
        assert new['futureprop'].value == 'x'
        assert list(new._extra_props) == ['futureprop']

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_rename_dispatch_with_extras_does_not_mutate_the_original(self, mock_subprocess):
        fs = Filesystem('testpool/old')
        fs.update({'futureprop': 'x'}, lenient=True)
        mock_subprocess.setup()
        new = RenameCommand()(fs, 'testpool/new')
        new.update({'futureprop': None}, lenient=True)
        assert fs['futureprop'].value == 'x'
        assert fs.name == 'testpool/old'

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_rename_failure_raises(self, mock_subprocess):
        fs = Filesystem('testpool/old')
        fs.update({'futureprop': 'x'}, lenient=True)
        mock_subprocess.setup(returncode=1, stderr='cannot rename')
        with pytest.raises(Exception):
            RenameCommand().filesystem(fs, 'testpool/new')
