"""Unit tests for libzfseasy.zfs command classes."""

import pytest
import importlib
import io
import logging
import os
import shlex
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
    Command, _TERMINATE_TIMEOUT, ResumeToken, ResumeTokenError, _redact, _log_command,
)
from libzfseasy.types import (
    Dataset, Filesystem, Volume, Snapshot, SnapshotRange, Bookmark, ZFS, Validate, PropertyNames, InvalidNameError,
)


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
        """Test getting all properties. `zfs get all` prints `type` first and no `name` row."""
        mock_subprocess.setup(stdout=[
            'testpool/filesystem\ttype\tfilesystem\t-\t-\n',
            'testpool/filesystem\tcompression\tlz4\t-\tlocal\n',
        ])
        
        cmd = GetCommand()
        result = cmd(ds=sample_filesystem, properties='all')
        
        assert len(result) == 1
        assert isinstance(result[0], Filesystem)
        assert result[0]['compression'].value == 'lz4'


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

    @pytest.mark.unit
    @pytest.mark.subprocess
    @pytest.mark.parametrize('bad,typename', [
        pytest.param('testpool/fs@snap', 'str', id='str'),
        pytest.param(None, 'NoneType', id='None'),
        pytest.param(Bookmark(Filesystem('testpool/fs'), 'bm'), 'Bookmark', id='Bookmark'),
        pytest.param(5, 'int', id='int'),
    ])
    def test_send_refuses_anything_but_a_dataset_or_snapshot(self, mock_subprocess, bad, typename):
        for send in (SendCommand(), SendCommand._send):
            with pytest.raises(ValueError) as exc:
                send(bad)
            assert str(exc.value) == 'Expected Dataset or Snapshot, got %s instead' % typename
        mock_subprocess.assert_not_called()

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_send_still_dispatches_a_dataset_and_a_snapshot(self, mock_subprocess, sample_filesystem, sample_snapshot):
        process_mock = MagicMock()
        process_mock.stdout.peek.return_value = b'ZFS_DATA'
        process_mock.poll.return_value = None
        mock_subprocess.return_value = process_mock
        assert SendCommand()(sample_filesystem, since=None) is not None
        assert SendCommand()(sample_snapshot) is not None
        assert mock_subprocess.call_count == 2


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
        assert '-S' in call_args
        assert '-s' not in call_args
        assert call_args[call_args.index('-S') + 1] == 'creation'

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
        # the dict branch is checked before the generic Iterable one: True -> -s, False -> -S
        mock_subprocess.setup(stdout=['testpool/fs\tfilesystem\n'])
        cmd = ListCommand()
        cmd(sort={'creation': True, 'compression': False})
        call_args = mock_subprocess.call_args[0][0]
        assert call_args.count('-s') == 1
        assert call_args.count('-S') == 1
        assert call_args[call_args.index('-s') + 1] == 'creation'
        assert call_args[call_args.index('-S') + 1] == 'compression'


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
       dedicated background thread via `Command._start_stderr_reader_lines`, decoupled
       from the stdout-reading loop in `_exec_lines`) so that a full stdout
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
        """Direct unit test of the stderr-draining helper (`_start_stderr_reader_lines`, the one reader behind
        `_exec`, `_exec_out`, `_exec_lines` and non-raw `_exec_collect`): a stderr line
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
            thread = Command._start_stderr_reader_lines(process, errors)
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
    @pytest.mark.subprocess
    @pytest.mark.parametrize('rc, raises', [(0, False), (1, True)], ids=['clean', 'failing'])
    def test_exec_lines_eof_but_poll_none_calls_wait(self, mock_subprocess, rc, raises):
        """Regression test for the EOF-but-still-running race (formerly `_exec_capture`, folded into `_exec_lines`
        by D3): stdout hits EOF (readline() == '') while poll() still reports None (the child hasn't been reaped
        by the OS yet). `_exec_lines` must resolve the return code by blocking on process.wait() exactly once
        rather than leaving the loop to spin on readline()/poll(), and use that code (a non-zero one raises)."""
        process = MagicMock()
        process.stdout.readline.return_value = ''
        process.stderr.readline.return_value = ''
        process.poll.return_value = None
        process.wait.return_value = rc
        mock_subprocess.return_value = process

        if raises:
            with pytest.raises(Exception, match='exit code 1'):
                list(Command._exec_lines(['zfs', 'list']))
        else:
            assert list(Command._exec_lines(['zfs', 'list'])) == []

        process.wait.assert_called_once_with()
        process.terminate.assert_not_called()
        process.kill.assert_not_called()

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


def _wait_for_thread_count(baseline, limit=5.0):
    """Give the daemon threads of the slow-command watchdog (a cancelled one needs a moment to return) time to end
    before a `threading.active_count()` comparison."""
    deadline = time.monotonic() + limit
    while threading.active_count() > baseline and time.monotonic() < deadline:
        time.sleep(0.02)


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
        real_start = Command._start_stderr_reader_lines

        def recording_start(process, errors):
            thread = real_start(process, errors)
            harness.stderr_threads.append(thread)
            return thread

        monkeypatch.setattr(Command, '_start_stderr_reader_lines', staticmethod(recording_start))

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
    """The deadlock/spin fixes in `Command._exec_out` / `_exec_lines` (commit a8a3d88; `_exec_capture` was folded
    into `_exec_lines` by D3), proven
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
        stderr lines (>64 KB of them), in order; only the first is capitalised."""
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
        assert lines[0].startswith('Warn 0 eee') and lines[-1].startswith('warn 299 eee')
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
        """(c) The child closes stdout, lingers 0.3 s, then exits 0. `_exec_lines` must block in
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


def _drive(entry, cmd, sink=None):
    """Run `cmd` through one of the three entry points; return the lines it yielded (`_exec` yields none). Lines
    are appended to `sink` as they arrive, so a failing run still reports what came before the exception."""
    sink = [] if sink is None else sink
    if entry == '_exec':
        Command._exec(cmd)
    else:
        for line in getattr(Command, entry)(cmd):
            sink.append(line)
    return sink


_ENTRIES = ['_exec', '_exec_out', '_exec_lines']


class TestExecCoreCleanupMocked:
    """D3: `_exec` and `_exec_out` are layers over `_exec_lines`, so one cleanup path serves them all. These pin it
    with a mocked `Popen`: an interrupt terminates the child, a child that survives SIGKILL is abandoned with exactly
    one WARNING and never waited for a third time. (The real-pipe counterparts are in `TestExecCoreRealChildren`.)"""

    @staticmethod
    def _interrupted_process(pid=4242):
        process = MagicMock()
        process.pid = pid
        process.stdout.readline.side_effect = KeyboardInterrupt()
        process.stderr.readline.return_value = ''
        process.poll.return_value = None
        return process

    @pytest.mark.unit
    @pytest.mark.subprocess
    @pytest.mark.parametrize('entry', ['_exec', '_exec_out', '_exec_lines'])
    def test_child_that_survives_sigkill_is_abandoned_with_one_warning(self, mock_subprocess, monkeypatch, caplog,
                                                                       entry):
        monkeypatch.setattr('libzfseasy.zfs._TERMINATE_TIMEOUT', 0.05)
        process = self._interrupted_process()
        process.wait.side_effect = [subprocess.TimeoutExpired(cmd='zfs', timeout=0.05),
                                    subprocess.TimeoutExpired(cmd='zfs', timeout=0.05)]
        mock_subprocess.return_value = process
        cmd = ['/usr/local/sbin/zfs', 'destroy', 'tank/x']

        started = time.monotonic()
        with caplog.at_level(logging.DEBUG, logger='libzfseasy'):
            with pytest.raises(KeyboardInterrupt):
                list(getattr(Command, entry)(cmd)) if entry != '_exec' else Command._exec(cmd)
        assert time.monotonic() - started < 5            # nothing blocked on the unkillable child

        process.terminate.assert_called_once_with()
        process.kill.assert_called_once_with()
        assert process.wait.call_args_list == [call(timeout=0.05), call(timeout=0.05)]   # no third (unbounded) wait
        warnings = [r for r in caplog.records if r.name == 'libzfseasy' and r.levelno == logging.WARNING]
        assert len(warnings) == 1
        message = warnings[0].getMessage()
        assert message.startswith('zfs destroy (pid 4242) did not exit after SIGKILL')
        assert 'abandoning it' in message
        assert '/usr/local/sbin' not in message           # the base name, not the path

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_the_abandon_warning_redacts_a_url_credential(self, mock_subprocess, monkeypatch, caplog):
        monkeypatch.setattr('libzfseasy.zfs._TERMINATE_TIMEOUT', 0.05)
        process = self._interrupted_process(pid=77)
        process.wait.side_effect = [subprocess.TimeoutExpired(cmd='zfs', timeout=0.05)] * 2
        mock_subprocess.return_value = process

        with caplog.at_level(logging.DEBUG, logger='libzfseasy'):
            with pytest.raises(KeyboardInterrupt):
                Command._exec(['zfs', 'https://user:hunter2@host/key'])

        text = '\n'.join(r.getMessage() for r in caplog.records)
        assert 'hunter2' not in text and 'user:' not in text
        warning = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
        assert len(warning) == 1 and 'https://***@host/key' in warning[0] and '(pid 77)' in warning[0]

    @pytest.mark.unit
    @pytest.mark.subprocess
    @pytest.mark.parametrize('entry', _ENTRIES)
    @pytest.mark.parametrize('kill_error', [None, ProcessLookupError()], ids=['kill-ok', 'kill-raises'])
    def test_a_second_interrupt_during_the_cleanup_wait_kills_the_child(self, mock_subprocess, monkeypatch, entry,
                                                                       kill_error):
        """F1: the first interrupt starts SIGTERM + a bounded wait; a second one arriving in that wait must still send
        a (non-blocking) SIGKILL before it propagates, and never wait again."""
        monkeypatch.setattr('libzfseasy.zfs._TERMINATE_TIMEOUT', 0.05)
        process = self._interrupted_process()
        process.wait.side_effect = KeyboardInterrupt()
        process.kill.side_effect = kill_error
        mock_subprocess.return_value = process
        cmd = ['zfs', 'destroy', 'tank/x']

        with pytest.raises(KeyboardInterrupt):
            _drive(entry, cmd)

        process.terminate.assert_called_once_with()
        process.kill.assert_called_once_with()
        assert process.wait.call_count == 1

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_the_abandon_path_does_not_join_the_stderr_reader(self, mock_subprocess, monkeypatch):
        """F4: an abandoned child still holds stderr open, so joining its reader could only time out (another
        `_TERMINATE_TIMEOUT`): the abandon path returns without it."""
        monkeypatch.setattr('libzfseasy.zfs._TERMINATE_TIMEOUT', 1.0)
        release = threading.Event()
        joined = []
        monkeypatch.setattr(Command, '_join_stderr_reader', staticmethod(joined.append))
        process = self._interrupted_process()
        process.stderr.readline.side_effect = lambda *_limit: (release.wait(30), '')[1]     # a reader that never ends
        process.wait.side_effect = [subprocess.TimeoutExpired(cmd='zfs', timeout=1)] * 2
        mock_subprocess.return_value = process
        try:
            started = time.monotonic()
            with pytest.raises(KeyboardInterrupt):
                Command._exec(['zfs', 'destroy', 'tank/x'])
            assert time.monotonic() - started < 0.5
            assert joined == []
            assert process.wait.call_count == 2
        finally:
            release.set()

    @pytest.mark.unit
    @pytest.mark.subprocess
    @pytest.mark.parametrize('entry', _ENTRIES)
    def test_a_child_that_cannot_be_started_raises_its_own_error(self, entry, caplog):
        """F2: `Popen` is inside the `try` with `process` still None: a failed spawn raises the OSError itself, with
        no NameError/UnboundLocalError from the cleanup and no abandon warning."""
        with caplog.at_level(logging.DEBUG, logger='libzfseasy'):
            with pytest.raises(FileNotFoundError):
                _drive(entry, ['/nonexistent/dir/zfs', 'list'])
        assert not [r for r in caplog.records if r.levelno >= logging.WARNING]

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_exec_collect_cannot_start_either(self):
        with pytest.raises(FileNotFoundError):
            Command._exec_collect(['/nonexistent/dir/zfs', 'list'])

    @pytest.mark.unit
    @pytest.mark.subprocess
    @pytest.mark.parametrize('raw', [False, True])
    def test_exec_collect_reaps_the_child_when_the_reader_cannot_start(self, mock_subprocess, monkeypatch, raw):
        """F2: the stderr reader is started inside the `try`, so an interrupt (or a thread-start failure) there
        still kills and reaps the already spawned child, and the ORIGINAL exception propagates (in the raw branch the
        thread is assigned only after a successful start(), so the cleanup never joins an unstarted thread)."""
        process = MagicMock()
        process.pid = 9
        process.poll.return_value = None
        process.wait.return_value = -9
        mock_subprocess.return_value = process

        real_thread = threading.Thread

        class NoThread(real_thread):
            def start(self):
                raise KeyboardInterrupt()

        def boom(*_args):
            raise KeyboardInterrupt()

        monkeypatch.setattr(Command, '_start_stderr_reader_lines', staticmethod(boom))
        monkeypatch.setattr('libzfseasy.zfs.threading', MagicMock(Thread=NoThread))
        with pytest.raises(KeyboardInterrupt):
            Command._exec_collect(['zfs', 'x'], raw_stderr=raw)
        process.kill.assert_called_once()
        assert process.wait.called

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_destroy_lines_closes_its_source_when_exhausted_or_abandoned(self):
        closed = []

        def source():
            try:
                yield 'destroy\ttank/a'
                yield 'reclaim\t10'
                yield 'destroy\ttank/b'
            finally:
                closed.append(True)

        assert list(DestroyCommand._destroy_lines(source())) == ['tank/a', 'tank/b']
        assert closed == [True]

        src = source()                      # the caller keeps its own reference: only the explicit close can end it
        gen = DestroyCommand._destroy_lines(src)
        assert next(gen) == 'tank/a'
        gen.close()
        assert closed == [True, True]
        del src

        class Plain:                        # a source without close() (a list iterator) is still fine
            def __iter__(self):
                return iter(['destroy\ttank/c'])

        assert list(DestroyCommand._destroy_lines(Plain())) == ['tank/c']

    @staticmethod
    def _record_joins(monkeypatch):
        joined = []
        real = Command._join_stderr_reader
        monkeypatch.setattr(Command, '_join_stderr_reader', staticmethod(lambda t: (joined.append(t), real(t))))
        return joined

    @pytest.mark.unit
    @pytest.mark.subprocess
    @pytest.mark.parametrize('entry', _ENTRIES)
    def test_an_interrupt_in_the_term_wait_kills_once_and_skips_join_wait_and_warning(self, mock_subprocess,
                                                                                     monkeypatch, caplog, entry):
        """F1: whatever interrupts a cleanup step is re-raised after one non-blocking kill; the join and the final
        wait are skipped and nothing is reported as abandoned."""
        monkeypatch.setattr('libzfseasy.zfs._TERMINATE_TIMEOUT', 0.05)
        joined = self._record_joins(monkeypatch)
        process = self._interrupted_process()
        process.wait.side_effect = KeyboardInterrupt()
        mock_subprocess.return_value = process

        with caplog.at_level(logging.DEBUG, logger='libzfseasy'):
            with pytest.raises(KeyboardInterrupt):
                _drive(entry, ['zfs', 'destroy', 'tank/x'])

        process.terminate.assert_called_once()
        process.kill.assert_called_once()
        assert process.wait.call_count == 1 and joined == []
        assert not [r for r in caplog.records if r.levelno >= logging.WARNING]

    @pytest.mark.unit
    @pytest.mark.subprocess
    @pytest.mark.parametrize('kill_error', [None, ProcessLookupError()], ids=['kill-ok', 'kill-raises'])
    def test_an_interrupt_in_the_kill_wait_kills_a_second_time(self, mock_subprocess, monkeypatch, caplog,
                                                              kill_error):
        monkeypatch.setattr('libzfseasy.zfs._TERMINATE_TIMEOUT', 0.05)
        joined = self._record_joins(monkeypatch)
        process = self._interrupted_process()
        process.wait.side_effect = [subprocess.TimeoutExpired(cmd='zfs', timeout=0.05), KeyboardInterrupt()]
        process.kill.side_effect = [None, kill_error] if kill_error else None   # only the last-resort kill may fail
        mock_subprocess.return_value = process

        with caplog.at_level(logging.DEBUG, logger='libzfseasy'):
            with pytest.raises(KeyboardInterrupt) as info:
                Command._exec(['zfs', 'destroy', 'tank/x'])

        assert process.kill.call_count == 2                  # the escalation, then the last-resort one
        assert process.wait.call_count == 2 and joined == []
        assert not [r for r in caplog.records if r.levelno >= logging.WARNING]
        chain, exc = [], info.value.__context__
        while exc is not None:
            chain.append(type(exc))
            exc = exc.__context__
        assert KeyboardInterrupt in chain                        # the original interrupt is kept as context

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_a_plain_error_in_a_cleanup_step_is_also_followed_by_a_kill(self, mock_subprocess):
        process = self._interrupted_process()
        process.wait.side_effect = RuntimeError('boom')
        mock_subprocess.return_value = process
        with pytest.raises(RuntimeError, match='boom'):
            Command._exec(['zfs', 'set', 'a=b', 'tank'])
        process.kill.assert_called_once()

    @pytest.mark.unit
    @pytest.mark.subprocess
    @pytest.mark.parametrize('entry', _ENTRIES)
    @pytest.mark.parametrize('error', [KeyboardInterrupt(), OSError('no such binary')], ids=['interrupt', 'oserror'])
    def test_popen_failing_leaves_nothing_to_clean_up(self, mock_subprocess, monkeypatch, entry, error):
        """F2: `Popen` is inside the `try` but `process` is still None: the error itself propagates (no
        UnboundLocalError/AttributeError from the `finally`), and there is nothing to join or wait for."""
        joined = self._record_joins(monkeypatch)
        mock_subprocess.side_effect = error
        with pytest.raises(type(error)):
            _drive(entry, ['zfs', 'list'])
        assert joined == []

    @pytest.mark.unit
    @pytest.mark.subprocess
    @pytest.mark.parametrize('raw', [False, True])
    @pytest.mark.parametrize('error', [KeyboardInterrupt(), OSError('no such binary')], ids=['interrupt', 'oserror'])
    def test_exec_collect_popen_failing_leaves_nothing_to_clean_up(self, mock_subprocess, monkeypatch, raw, error):
        joined = self._record_joins(monkeypatch)
        mock_subprocess.side_effect = error
        with pytest.raises(type(error)):
            Command._exec_collect(['zfs', 'list'], raw_stderr=raw)
        assert joined == []

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_exec_collect_reader_start_failure_kills_waits_and_joins(self, mock_subprocess, monkeypatch):
        joined = self._record_joins(monkeypatch)
        process = MagicMock()
        process.pid = 9
        process.poll.return_value = None
        process.wait.return_value = -9
        mock_subprocess.return_value = process

        def boom(*_args):
            raise KeyboardInterrupt()

        monkeypatch.setattr(Command, '_start_stderr_reader_lines', staticmethod(boom))
        with pytest.raises(KeyboardInterrupt):
            Command._exec_collect(['zfs', 'x'])
        process.kill.assert_called_once()
        process.wait.assert_called_once()
        assert joined == [None]                              # the reader never existed; the join is a no-op

    @pytest.mark.unit
    @pytest.mark.subprocess
    @pytest.mark.parametrize('what', ['dataset', 'snapshots'])
    def test_an_interrupt_in_the_eager_destroy_terminates_the_child_before_it_reaches_the_caller(
            self, mock_subprocess, what):
        """F3, mocked: the KeyboardInterrupt comes from stdout mid-iteration; the exception (and with it the
        `dataset()` frame) is still referenced, yet the child was terminated and waited for."""
        process = self._interrupted_process()
        process.stdout.readline.side_effect = ['destroy\ttank/x\n', KeyboardInterrupt()]
        process.wait.return_value = -15
        mock_subprocess.return_value = process
        fs = Filesystem('tank/x')
        held = None
        try:
            if what == 'dataset':
                DestroyCommand().dataset(fs, destroy=True)
            else:
                DestroyCommand().snapshots([Snapshot(fs, 's1')], destroy=True)
        except KeyboardInterrupt as exc:
            held = exc
        assert held is not None
        process.terminate.assert_called_once()
        assert process.wait.called
        process.kill.assert_not_called()

    @pytest.mark.unit
    @pytest.mark.subprocess
    @pytest.mark.parametrize('what', ['dataset', 'snapshots'])
    def test_a_lazy_destroy_starts_nothing_up_front_and_close_reaps_the_child(self, mock_subprocess, what):
        process = MagicMock()
        process.pid = 3
        process.stdout.readline.side_effect = ['destroy\ttank/x\n', 'destroy\ttank/y\n', '']
        process.stderr.readline.return_value = ''
        process.poll.return_value = None
        process.wait.return_value = -15
        mock_subprocess.return_value = process
        fs = Filesystem('tank/x')

        if what == 'dataset':
            gen = DestroyCommand().dataset(fs, destroy=True, lazy=True)
        else:
            gen = DestroyCommand().snapshots([Snapshot(fs, 's1')], destroy=True, lazy=True)
        mock_subprocess.assert_not_called()                  # nothing closed, nothing even started yet
        process.terminate.assert_not_called()

        assert next(gen) == 'tank/x'
        gen.close()
        process.terminate.assert_called_once()
        assert process.wait.called

    @pytest.mark.unit
    @pytest.mark.subprocess
    @pytest.mark.parametrize('what', ['dataset', 'snapshots'])
    def test_close_reaps_the_child_while_the_closed_generator_is_still_referenced(self, mock_subprocess, what):
        """CPython 3.12.3 does not release a generator EXPRESSION's inner iterator on `close()` (only deallocation
        does), so a lazy destroy wrapped in `(o for o in result)` left the child running for as long as the caller
        kept the closed generator. The child must be gone at `close()`, whatever the caller still holds."""
        import inspect
        process = MagicMock()
        process.pid = 3
        process.stdout.readline.side_effect = ['destroy\ttank/x\n', 'destroy\ttank/y\n', '']
        process.stderr.readline.return_value = ''
        process.poll.return_value = None
        process.wait.return_value = -15
        mock_subprocess.return_value = process
        fs = Filesystem('tank/x')

        if what == 'dataset':
            gen = DestroyCommand().dataset(fs, destroy=True, lazy=True)
        else:
            gen = DestroyCommand().snapshots([Snapshot(fs, 's1')], destroy=True, lazy=True)
        assert inspect.isgenerator(gen)
        assert gen.gi_code.co_name != '<genexpr>'            # no wrapper whose inner iterator could outlive close()

        assert next(gen) == 'tank/x'
        process.terminate.assert_not_called()
        gen.close()
        held = gen                                           # the closed generator stays referenced
        process.terminate.assert_called_once()
        assert process.wait.called
        assert inspect.getgeneratorstate(held) == inspect.GEN_CLOSED

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_the_normal_interrupt_path_joins_and_waits_once_each(self, mock_subprocess, monkeypatch):
        """F4's counterpart: a child that honours SIGTERM is joined (once) and given the final wait."""
        monkeypatch.setattr('libzfseasy.zfs._TERMINATE_TIMEOUT', 0.05)
        joined = self._record_joins(monkeypatch)
        process = self._interrupted_process()
        process.wait.return_value = -15
        mock_subprocess.return_value = process
        with pytest.raises(KeyboardInterrupt):
            Command._exec(['zfs', 'set', 'a=b', 'tank'])
        assert len(joined) == 1
        assert process.wait.call_args_list == [call(timeout=0.05), call()]
        process.kill.assert_not_called()

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_the_label_of_a_malformed_argv_does_not_break_the_cleanup(self):
        from libzfseasy.zfs import _command_label
        assert _command_label(['zfs']) == 'zfs'
        assert _command_label([]) == 'command'
        assert _command_label(None) == 'command'

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_sigkill_that_works_is_not_an_abandon(self, mock_subprocess, monkeypatch, caplog):
        """The escalation path as before: SIGTERM ignored, SIGKILL reaps. wait: timed, timed, final unbounded
        (3 calls), no WARNING."""
        monkeypatch.setattr('libzfseasy.zfs._TERMINATE_TIMEOUT', 0.05)
        process = self._interrupted_process()
        process.wait.side_effect = [subprocess.TimeoutExpired(cmd='zfs', timeout=0.05), 0, 0]
        mock_subprocess.return_value = process

        with caplog.at_level(logging.DEBUG, logger='libzfseasy'):
            with pytest.raises(KeyboardInterrupt):
                Command._exec(['zfs', 'set', 'a=b', 'tank'])

        process.terminate.assert_called_once()
        process.kill.assert_called_once()
        assert process.wait.call_count == 3
        assert process.wait.call_args_list[2] == call()
        assert not [r for r in caplog.records if r.levelno >= logging.WARNING]

    @pytest.mark.unit
    @pytest.mark.subprocess
    @pytest.mark.parametrize('where', ['set', 'destroy', 'create'])
    def test_an_interrupt_during_a_write_command_terminates_the_child(self, mock_subprocess, where, sample_pool):
        """Before D3 `_exec` left the child running when the caller was interrupted; now every write command
        terminates and reaps it before the KeyboardInterrupt propagates."""
        process = self._interrupted_process()
        process.wait.return_value = -15
        mock_subprocess.return_value = process
        fs = Filesystem(f'{sample_pool}/filesystem')

        with pytest.raises(KeyboardInterrupt):
            if where == 'set':
                SetCommand()(fs, {'compression': 'lz4'})
            elif where == 'destroy':
                DestroyCommand().dataset(fs, destroy=True)
            else:
                CreateCommand().filesystem(f'{sample_pool}/newfs')

        process.terminate.assert_called_once()
        assert process.wait.called
        process.kill.assert_not_called()

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_an_interrupt_after_the_child_exited_does_not_signal_it(self, mock_subprocess):
        process = self._interrupted_process()
        process.poll.return_value = 0
        process.wait.return_value = 0
        mock_subprocess.return_value = process
        with pytest.raises(KeyboardInterrupt):
            Command._exec(['zfs', 'set', 'a=b', 'tank'])
        process.terminate.assert_not_called()
        process.kill.assert_not_called()

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_a_failing_start_of_the_stderr_reader_still_reaps_the_child(self, mock_subprocess, monkeypatch):
        """The reader is started inside the `try`: if starting its thread fails (or is interrupted) the child that
        was already spawned is terminated and reaped, not leaked."""
        process = MagicMock()
        process.pid = 5
        process.poll.return_value = None
        process.wait.return_value = -15
        mock_subprocess.return_value = process

        def boom(*_args):
            raise KeyboardInterrupt()

        monkeypatch.setattr(Command, '_start_stderr_reader_lines', staticmethod(boom))
        with pytest.raises(KeyboardInterrupt):
            Command._exec(['zfs', 'set', 'a=b', 'tank'])
        process.terminate.assert_called_once()
        assert process.wait.called


_BIG_ONE_LINE_STDERR = '''
import sys
for i in range(20):
    sys.stderr.write("e" * 10000); sys.stderr.flush()
    sys.stdout.write(("row %d " % i + "o" * 1000 + "\\n") * 20); sys.stdout.flush()
sys.stderr.write("\\n"); sys.stderr.flush()
sys.exit(4)
'''

_BIG_MANY_LINES_STDERR = '''
import sys
for i in range(2000):
    sys.stderr.write("warn %d " % i + "e" * 100 + "\\n"); sys.stderr.flush()
    if i % 10 == 0:
        sys.stdout.write(("row %d " % i + "o" * 1000 + "\\n") * 5); sys.stdout.flush()
sys.exit(1)
'''


@pytest.fixture
def interrupt_on_start(real_child, monkeypatch):
    """Arm a SIGALRM timer, from inside `Command._exec_lines`'s `try` (right after the stderr reader started), whose
    handler raises KeyboardInterrupt in the main thread: a real signal-driven interrupt at a deterministic point.
    `arm(ready_file=None, delay=0.3, second_after=None)`; with `ready_file` the handler waits for that file to exist
    (the child's signal that it is ready to ignore SIGTERM) before it raises; with `second_after` it raises a
    second KeyboardInterrupt that many seconds after the first (i.e. while the cleanup is waiting for the child)."""
    if threading.current_thread() is not threading.main_thread():
        pytest.skip('a signal-driven interrupt needs the main thread')
    state = {'old': signal.getsignal(signal.SIGALRM), 'fired': None, 'second': None}

    def arm(ready_file=None, delay=0.3, second_after=None):
        inner = Command._start_stderr_reader_lines

        def handler(signum, frame):
            if ready_file is not None and not os.path.exists(ready_file):
                signal.setitimer(signal.ITIMER_REAL, 0.05)
                return
            if state['fired'] is None:
                state['fired'] = time.monotonic()
                if second_after is not None:
                    signal.setitimer(signal.ITIMER_REAL, second_after)
            else:
                state['second'] = time.monotonic()
            raise KeyboardInterrupt()

        def start(process, errors):
            thread = inner(process, errors)
            signal.signal(signal.SIGALRM, handler)
            signal.setitimer(signal.ITIMER_REAL, delay)
            return thread

        monkeypatch.setattr(Command, '_start_stderr_reader_lines', staticmethod(start))

    safety = threading.Timer(40, real_child.kill_all)       # a regression that hangs the cleanup fails, not hangs
    safety.daemon = True
    safety.start()
    yield arm, state
    safety.cancel()
    signal.setitimer(signal.ITIMER_REAL, 0)
    signal.signal(signal.SIGALRM, state['old'])


_IGNORE_SIGTERM_CHILD = (
    'import signal, sys, time\n'
    'signal.signal(signal.SIGTERM, signal.SIG_IGN)\n'
    'open(sys.argv[1], "w").close()\n'
    'time.sleep(120)\n'
)


class TestExecCoreRealChildren:
    """D3 against REAL children (`sys.executable`, no pool): only real pipes and real signals can prove what the
    mocked `TestExecCoreCleanupMocked` assumes. Every run is bounded by a watchdog or by the patched
    `_TERMINATE_TIMEOUT`, so a regression fails instead of hanging."""

    # -- bytes in, text out --------------------------------------------------------------------------------------

    @pytest.mark.unit
    @pytest.mark.subprocess
    @pytest.mark.parametrize('entry', ['_exec_out', '_exec_lines'])
    def test_invalid_utf8_on_stdout_survives_as_surrogates_and_r_stays_inside_the_line(self, real_child, entry):
        script = ('import sys\n'
                  'sys.stdout.buffer.write(b"a\\rb \\xff\\n"); sys.stdout.flush()\n')
        result, exc = real_child.run(lambda: _drive(entry, real_child.cmd(script)))
        assert exc is None
        assert result == ['a\rb \udcff']
        assert real_child.procs[0].returncode == 0

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_exec_discards_invalid_utf8_stdout_without_raising(self, real_child):
        script = 'import sys\nsys.stdout.buffer.write(b"\\xff\\xfe\\n" * 100); sys.stdout.flush()\n'
        result, exc = real_child.run(lambda: _drive('_exec', real_child.cmd(script)))
        assert exc is None and result == []
        assert real_child.procs[0].returncode == 0

    @pytest.mark.unit
    @pytest.mark.subprocess
    @pytest.mark.parametrize('entry', _ENTRIES)
    def test_invalid_utf8_on_stderr_is_replaced_in_the_error_text(self, real_child, entry):
        script = ('import sys\n'
                  'sys.stderr.buffer.write(b"bad \\xff\\xfe thing\\n"); sys.stderr.flush()\n'
                  'sys.stdout.buffer.write(b"out \\xff\\n"); sys.stdout.flush()\n'
                  'sys.exit(3)\n')
        _, exc = real_child.run(lambda: _drive(entry, real_child.cmd(script)))
        assert isinstance(exc, Exception)
        assert str(exc) == 'Bad �� thing'
        assert real_child.procs[0].returncode == 3
        assert not any(t.is_alive() for t in real_child.stderr_threads)

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_exec_out_strips_crlf_and_drops_lone_cr_lines_but_keeps_an_inner_cr(self, real_child):
        script = ('import sys\n'
                  'sys.stdout.buffer.write(b"keep\\rinside\\nclean\\r\\n\\r\\n   \\nlast\\n\\r"); sys.stdout.flush()\n')
        rows, exc = real_child.run(lambda: _drive('_exec_out', real_child.cmd(script)))
        assert exc is None
        assert rows == ['keep\rinside', 'clean', 'last']          # '\r' never splits; blank and lone-'\r' dropped

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_exec_lines_keeps_every_line_exactly(self, real_child):
        script = ('import sys\n'
                  'sys.stdout.buffer.write(b"keep\\rinside\\nclean\\r\\n\\r\\n\\nlast\\n\\r"); sys.stdout.flush()\n')
        rows, exc = real_child.run(lambda: _drive('_exec_lines', real_child.cmd(script)))
        assert exc is None
        assert rows == ['keep\rinside', 'clean\r', '\r', '', 'last', '\r']

    # -- big stderr ------------------------------------------------------------------------------------------------

    @pytest.mark.unit
    @pytest.mark.subprocess
    @pytest.mark.parametrize('entry', _ENTRIES)
    def test_200kb_single_line_of_stderr_with_large_stdout_finishes_and_raises(self, real_child, entry):
        rows = []
        _, exc = real_child.run(lambda: _drive(entry, real_child.cmd(_BIG_ONE_LINE_STDERR), rows))
        assert isinstance(exc, Exception)
        text = str(exc)
        # read 64 KiB at a time: 3 x 65536 + 3392 characters, rejoined by 3 newlines
        assert len(text) == 200003
        assert text.count('\n') == 3
        assert text.startswith('E' + 'e' * 100)                  # only the first piece is capitalised
        if entry != '_exec':
            assert len(rows) == 400 and rows[0].startswith('row 0 ooo') and rows[-1].startswith('row 19 ooo')
        assert real_child.procs[0].returncode == 4
        assert not any(t.is_alive() for t in real_child.stderr_threads)

    @pytest.mark.unit
    @pytest.mark.subprocess
    @pytest.mark.parametrize('entry', _ENTRIES)
    def test_many_stderr_lines_are_capped_with_an_omitted_marker(self, real_child, entry):
        rows = []
        _, exc = real_child.run(lambda: _drive(entry, real_child.cmd(_BIG_MANY_LINES_STDERR), rows))
        assert isinstance(exc, Exception)
        lines = str(exc).split('\n')
        assert len(lines) == 401                                  # 200 head + marker + 200 tail
        assert lines[0].startswith('Warn 0 eee') and lines[199].startswith('warn 199 eee')
        assert lines[200] == '... 1600 lines omitted ...'
        assert lines[201].startswith('warn 1800 eee') and lines[-1].startswith('warn 1999 eee')
        if entry != '_exec':
            assert len(rows) == 1000
        assert real_child.procs[0].returncode == 1
        assert not any(t.is_alive() for t in real_child.stderr_threads)

    # -- a real interrupt --------------------------------------------------------------------------------------------

    @pytest.mark.unit
    @pytest.mark.subprocess
    @pytest.mark.parametrize('entry', _ENTRIES)
    def test_a_keyboard_interrupt_terminates_and_reaps_the_child(self, real_child, interrupt_on_start, entry):
        arm, _state = interrupt_on_start
        arm()
        script = ('import sys, time\n'
                  'sys.stderr.write("noise\\n"); sys.stderr.flush()\n'
                  'time.sleep(120)\n')
        started = time.monotonic()
        with pytest.raises(KeyboardInterrupt):
            _drive(entry, real_child.cmd(script))
        assert time.monotonic() - started < 15

        proc = real_child.procs[0]
        assert proc.returncode == -signal.SIGTERM
        assert real_child.stderr_threads and not any(t.is_alive() for t in real_child.stderr_threads)
        with pytest.raises(ProcessLookupError):                   # reaped, not a zombie
            os.kill(proc.pid, 0)
        self._assert_no_session_orphan(proc.pid)

    @pytest.mark.unit
    @pytest.mark.subprocess
    @pytest.mark.parametrize('entry', _ENTRIES)
    def test_a_keyboard_interrupt_kills_a_child_that_ignores_sigterm(self, real_child, interrupt_on_start,
                                                                     monkeypatch, tmp_path, entry):
        monkeypatch.setattr('libzfseasy.zfs._TERMINATE_TIMEOUT', 0.5)
        ready = tmp_path / 'ready'
        arm, state = interrupt_on_start
        arm(ready_file=str(ready))
        cmd = [sys.executable, '-c', _IGNORE_SIGTERM_CHILD, str(ready)]

        with pytest.raises(KeyboardInterrupt):
            _drive(entry, cmd)
        done = time.monotonic()

        proc = real_child.procs[0]
        assert ready.exists()                                     # SIGTERM was already being ignored
        assert proc.returncode == -signal.SIGKILL
        assert 0.4 <= done - state['fired'] < 10                  # about the patched bound, not 120 s
        assert not any(t.is_alive() for t in real_child.stderr_threads)
        with pytest.raises(ProcessLookupError):
            os.kill(proc.pid, 0)
        self._assert_no_session_orphan(proc.pid)

    @staticmethod
    def _assert_no_session_orphan(pid):
        from tests._hermetic import find_orphans
        marker = os.environ.get('ZFSB_TEST_SESSION')
        if not marker:
            pytest.skip('no test-session marker in this run')
        assert pid not in [o[0] for o in find_orphans(marker, grace=1.0, fork_dir=None)]

    # -- the explicit close: an exception's traceback keeps the suspended generator (and its child) alive ---------------

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_exec_reaps_the_child_while_the_interrupt_traceback_is_held(self, real_child, monkeypatch):
        """An interrupt that lands in `_exec`'s own loop (between two resumptions of `_exec_lines`, not inside it)
        leaves the generator suspended; the traceback keeps `_exec`'s frame, and so the generator, alive for as long
        as the caller holds the exception. `_exec` must close it explicitly: the child is reaped by the time the
        exception arrives, not whenever the traceback is dropped."""
        real = Command._exec_lines

        class Tripwire:
            """The real `_exec_lines`, except that the second `next()` raises, in the caller's frame."""
            def __init__(self, gen):
                self.gen, self.calls = gen, 0

            def __iter__(self):
                return self

            def __next__(self):
                self.calls += 1
                if self.calls == 2:
                    raise KeyboardInterrupt()
                return next(self.gen)

            def close(self):
                self.gen.close()

        monkeypatch.setattr(Command, '_exec_lines', staticmethod(lambda cmd: Tripwire(real(cmd))))
        script = 'import time\nprint("first", flush=True)\ntime.sleep(120)\n'
        held = None
        try:
            Command._exec(real_child.cmd(script))
        except KeyboardInterrupt as exc:
            held = exc
        assert held is not None and held.__traceback__ is not None       # still referenced below

        proc = real_child.procs[0]
        assert proc.returncode == -signal.SIGTERM
        assert not any(t.is_alive() for t in real_child.stderr_threads)

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_exec_out_reaps_the_child_while_a_thrown_exception_is_held(self, real_child):
        """The same for `_exec_out`: an exception thrown in at the `yield` leaves through its `finally`, which must
        close the inner generator even though the exception's traceback still references the frame."""
        script = 'import time\nprint("first", flush=True)\ntime.sleep(120)\n'
        gen = Command._exec_out(real_child.cmd(script))
        assert next(gen) == 'first'
        held = None
        try:
            gen.throw(RuntimeError('boom'))
        except RuntimeError as exc:
            held = exc
        assert held is not None and held.__traceback__ is not None

        proc = real_child.procs[0]
        assert proc.returncode == -signal.SIGTERM
        assert not any(t.is_alive() for t in real_child.stderr_threads)

    # -- review follow-ups: a second interrupt, an interrupt before the loop, the eager destroy ---------------------------

    @pytest.mark.unit
    @pytest.mark.subprocess
    @pytest.mark.parametrize('entry', _ENTRIES)
    def test_a_second_interrupt_during_the_cleanup_wait_kills_a_sigterm_ignoring_child(
            self, real_child, interrupt_on_start, monkeypatch, tmp_path, entry):
        """F1 with a real signal: the child ignores SIGTERM, so the first interrupt's cleanup waits (here up to 5 s);
        a second KeyboardInterrupt arrives 0.4 s into that wait. It propagates at once, and the child has been sent
        SIGKILL on the way out (not left running for 120 s)."""
        monkeypatch.setattr('libzfseasy.zfs._TERMINATE_TIMEOUT', 5)
        ready = tmp_path / 'ready'
        arm, state = interrupt_on_start
        arm(ready_file=str(ready), second_after=0.4)

        with pytest.raises(KeyboardInterrupt):
            _drive(entry, [sys.executable, '-c', _IGNORE_SIGTERM_CHILD, str(ready)])
        left = time.monotonic()

        assert state['second'] is not None                       # the second interrupt is what ended the call
        assert left - state['fired'] < 3                         # not the full bounded wait
        proc = real_child.procs[0]
        proc.wait(timeout=10)                                    # the SIGKILL was sent: it dies without any help
        assert proc.returncode == -signal.SIGKILL
        with pytest.raises(ProcessLookupError):
            os.kill(proc.pid, 0)
        self._assert_no_session_orphan(proc.pid)

    @pytest.mark.unit
    @pytest.mark.subprocess
    @pytest.mark.parametrize('entry', _ENTRIES)
    def test_an_interrupt_while_the_stderr_reader_starts_reaps_the_child(self, real_child, monkeypatch, entry):
        """F2: the reader start is inside the `try`, so the already spawned real child is terminated and reaped."""
        real_start = Command._start_stderr_reader_lines

        def start_then_interrupt(process, errors):
            real_start(process, errors)
            raise KeyboardInterrupt()

        monkeypatch.setattr(Command, '_start_stderr_reader_lines', staticmethod(start_then_interrupt))
        with pytest.raises(KeyboardInterrupt):
            _drive(entry, real_child.cmd('import time\ntime.sleep(120)\n'))

        proc = real_child.procs[0]
        assert proc.returncode == -signal.SIGTERM
        assert not any(t.is_alive() for t in real_child.stderr_threads)
        with pytest.raises(ProcessLookupError):
            os.kill(proc.pid, 0)

    @pytest.mark.unit
    @pytest.mark.subprocess
    @pytest.mark.parametrize('raw', [False, True])
    def test_exec_collect_reaps_a_real_child_when_the_reader_start_is_interrupted(self, real_child, monkeypatch, raw):
        real_start = Command._start_stderr_reader_lines

        def start_then_interrupt(process, errors):
            real_start(process, errors)
            raise KeyboardInterrupt()

        monkeypatch.setattr(Command, '_start_stderr_reader_lines', staticmethod(start_then_interrupt))
        if raw:
            real_thread = threading.Thread

            class Thread(real_thread):
                def start(self):
                    super().start()
                    raise KeyboardInterrupt()

            monkeypatch.setattr('libzfseasy.zfs.threading', MagicMock(Thread=Thread))
        with pytest.raises(KeyboardInterrupt):
            Command._exec_collect(real_child.cmd('import time\ntime.sleep(120)\n'), raw_stderr=raw)

        proc = real_child.procs[0]
        assert proc.returncode == -signal.SIGKILL
        with pytest.raises(ProcessLookupError):
            os.kill(proc.pid, 0)

    @staticmethod
    def _tripwire_exec_out(monkeypatch, real_child, level='source'):
        """Replace `Command._exec_out` by a wrapper around the REAL `_exec_out` of a real child that prints one
        destroy line and sleeps; its second `next()` raises KeyboardInterrupt in the *caller's* frame (the eager
        destroy comprehension), not inside the generator."""
        real = Command._exec_out
        script = 'import time\nprint("destroy\\ttank/x", flush=True)\ntime.sleep(120)\n'

        class Tripwire:
            def __init__(self, gen):
                self.gen, self.calls = gen, 0

            def __iter__(self):
                return self

            def __next__(self):
                self.calls += 1
                if self.calls == 2:
                    raise KeyboardInterrupt()
                return next(self.gen)

            def close(self):
                self.gen.close()

        if level == 'source':
            monkeypatch.setattr(Command, '_exec_out', staticmethod(lambda cmd: Tripwire(real(real_child.cmd(script)))))
        else:
            real_lines = DestroyCommand._destroy_lines
            monkeypatch.setattr(Command, '_exec_out', staticmethod(lambda cmd: real(real_child.cmd(script))))
            monkeypatch.setattr(DestroyCommand, '_destroy_lines', staticmethod(lambda src: Tripwire(real_lines(src))))

    @pytest.mark.unit
    @pytest.mark.subprocess
    @pytest.mark.parametrize('level', ['source', 'result'])
    @pytest.mark.parametrize('what', ['dataset', 'snapshots'])
    def test_an_interrupt_in_the_eager_destroy_comprehension_reaps_the_child(self, real_child, monkeypatch, what,
                                                                             level):
        """F3: the eager `destroy.dataset/snapshots` close their generators in a `finally`; with the exception (and
        so the `dataset()` frame) still referenced, the child is nevertheless already reaped. `level` is where the
        interrupt is raised: inside `_destroy_lines` while it pulls from `_exec_out` ('source': that generator's own
        `finally` ends it) or in the comprehension itself, between two items of `_destroy_lines` ('result': only the
        explicit closes in `dataset`/`snapshots` end the suspended generators)."""
        self._tripwire_exec_out(monkeypatch, real_child, level)
        fs = Filesystem('tank/x')
        held = None
        try:
            if what == 'dataset':
                DestroyCommand().dataset(fs, destroy=True)
            else:
                DestroyCommand().snapshots([Snapshot(fs, 's1')], destroy=True)
        except KeyboardInterrupt as exc:
            held = exc
        assert held is not None and held.__traceback__ is not None

        proc = real_child.procs[0]
        assert proc.returncode == -signal.SIGTERM
        assert not any(t.is_alive() for t in real_child.stderr_threads)

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_an_abandoned_lazy_destroy_reaps_the_child(self, real_child):
        script = 'import time\nprint("destroy\\ttank/x", flush=True)\ntime.sleep(120)\n'
        real = Command._exec_out
        gen = None
        try:
            Command._exec_out = staticmethod(lambda cmd: real(real_child.cmd(script)))
            gen = DestroyCommand().dataset(Filesystem('tank/x'), destroy=True, lazy=True)
        finally:
            Command._exec_out = real
        assert next(gen) == 'tank/x'
        proc = real_child.procs[0]
        assert proc.returncode is None
        gen.close()
        gen = None
        import gc
        gc.collect()
        assert proc.returncode == -signal.SIGTERM

    # -- an abandoned generator ------------------------------------------------------------------------------------

    @pytest.mark.unit
    @pytest.mark.subprocess
    @pytest.mark.parametrize('how', ['close', 'del-gc'])
    @pytest.mark.parametrize('entry', ['_exec_out', '_exec_lines'])
    def test_an_abandoned_generator_reaps_the_child(self, real_child, entry, how):
        script = 'import sys, time\nprint("first", flush=True)\ntime.sleep(120)\n'
        gen = getattr(Command, entry)(real_child.cmd(script))
        assert next(gen) == 'first'
        proc = real_child.procs[0]
        assert proc.returncode is None

        if how == 'close':
            gen.close()
        else:
            def drop():
                nonlocal gen
                gen = None
                import gc
                gc.collect()
            _, exc = real_child.run(drop)
            assert exc is None

        assert proc.returncode == -signal.SIGTERM
        assert not any(t.is_alive() for t in real_child.stderr_threads)
        with pytest.raises(ProcessLookupError):
            os.kill(proc.pid, 0)

    @pytest.mark.unit
    @pytest.mark.subprocess
    @pytest.mark.parametrize('how', ['close', 'del-gc'])
    def test_an_abandoned_exec_out_kills_a_child_that_ignores_sigterm(self, real_child, monkeypatch, how):
        monkeypatch.setattr('libzfseasy.zfs._TERMINATE_TIMEOUT', 0.5)
        script = ('import signal, sys, time\n'
                  'signal.signal(signal.SIGTERM, signal.SIG_IGN)\n'
                  'print("ready", flush=True)\n'
                  'time.sleep(120)\n')
        gen = Command._exec_out(real_child.cmd(script))
        assert next(gen) == 'ready'
        proc = real_child.procs[0]

        def drop():
            nonlocal gen
            if how == 'close':
                gen.close()
            gen = None
            import gc
            gc.collect()

        _, exc = real_child.run(drop, timeout=30)

        assert exc is None
        assert proc.returncode == -signal.SIGKILL
        assert not any(t.is_alive() for t in real_child.stderr_threads)
        with pytest.raises(ProcessLookupError):
            os.kill(proc.pid, 0)


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
            fs + '\ttype\tfilesystem\t-\t-\n',
            fs + '\tcompression\tlz4\t-\tlocal\n',
        ]
        for i, name in enumerate(cls.QUOTA_NAMES):
            lines.append('%s\t%s\t%dG\t%dM\tlocal\n' % (fs, name, i + 1, i + 1))
        lines += [
            fs + '\tfutureprop\tfuture-value\trecv-value\tinherited from testpool\n',
            fs + '\torg.example:note\thello\t-\tlocal\n',
            fs + '\tvolsize\t1G\t-\tdefault\n',
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
    def test_malformed_native_name_from_zfs_is_skipped_per_row(self, mock_subprocess, caplog):
        # B2: a row with an invalid property name is skipped (WARNING); the object itself is still returned and
        # keeps its other properties.
        fs = 'testpool/filesystem'
        mock_subprocess.setup(stdout=[
            fs + '\ttype\tfilesystem\t-\t-\n',
            fs + '\tBad-Prop\tv\t-\tlocal\n',
            fs + '\tcompression\tlz4\t-\tlocal\n',
        ])
        with caplog.at_level(logging.WARNING, logger='libzfseasy'):
            result = GetCommand()(properties='all')
        assert [r.name for r in result] == [fs]
        assert isinstance(result[0], Filesystem)
        assert result[0]['compression'].value == 'lz4'
        assert 'Bad-Prop' not in result[0]._extra_props
        assert any('invalid property name' in m for m in _skip_warnings(caplog))

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


def _argv(mock_subprocess, n=0):
    """The argv of the n-th Popen call, without the leading zfs binary."""
    return list(mock_subprocess.call_args_list[n][0][0])[1:]


class TestListSortDirection:
    """`-s` sorts ascending and `-S` descending; the dict branch is reachable and keeps its order."""

    LINE = ['testpool/fs\tfilesystem\n']

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_string_ascending_is_dash_s(self, mock_subprocess):
        mock_subprocess.setup(stdout=self.LINE)
        ListCommand()(sort='creation', asc=True)
        argv = _argv(mock_subprocess)
        assert argv[argv.index('-s') + 1] == 'creation'
        assert '-S' not in argv

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_string_descending_is_dash_capital_s(self, mock_subprocess):
        mock_subprocess.setup(stdout=self.LINE)
        ListCommand()(sort='creation', asc=False)
        argv = _argv(mock_subprocess)
        assert argv[argv.index('-S') + 1] == 'creation'
        assert '-s' not in argv
        assert 'S' not in argv      # the old bug emitted a bare 'S'

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_dict_true_is_s_and_false_is_capital_s(self, mock_subprocess):
        mock_subprocess.setup(stdout=self.LINE)
        ListCommand()(sort={'creation': True, 'compression': False})
        argv = _argv(mock_subprocess)
        assert argv[argv.index('-s') + 1] == 'creation'
        assert argv[argv.index('-S') + 1] == 'compression'
        assert argv.count('-s') == 1 and argv.count('-S') == 1

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_dict_order_is_preserved(self, mock_subprocess):
        mock_subprocess.setup(stdout=self.LINE)
        ListCommand()(sort={'used': False, 'creation': True, 'compression': False})
        argv = _argv(mock_subprocess)
        start = argv.index('-S')
        assert argv[start:start + 6] == ['-S', 'used', '-s', 'creation', '-S', 'compression']

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_dict_ignores_asc(self, mock_subprocess):
        mock_subprocess.setup(stdout=self.LINE)
        ListCommand()(sort={'creation': True}, asc=False)
        argv = _argv(mock_subprocess)
        assert '-s' in argv and '-S' not in argv

    @pytest.mark.unit
    @pytest.mark.subprocess
    @pytest.mark.parametrize('kind', [list, tuple])
    def test_plain_iterable_defaults_to_ascending(self, mock_subprocess, kind):
        mock_subprocess.setup(stdout=self.LINE)
        ListCommand()(sort=kind(['creation', 'compression']))
        argv = _argv(mock_subprocess)
        start = argv.index('-s')
        assert argv[start:start + 4] == ['-s', 'creation', '-s', 'compression']
        assert '-S' not in argv

    @pytest.mark.unit
    @pytest.mark.subprocess
    @pytest.mark.parametrize('kind', [list, tuple])
    def test_plain_iterable_with_asc_false_is_all_descending(self, mock_subprocess, kind):
        mock_subprocess.setup(stdout=self.LINE)
        ListCommand()(sort=kind(['creation', 'compression']), asc=False)
        argv = _argv(mock_subprocess)
        start = argv.index('-S')
        assert argv[start:start + 4] == ['-S', 'creation', '-S', 'compression']
        assert '-s' not in argv

    @pytest.mark.unit
    @pytest.mark.subprocess
    @pytest.mark.parametrize('sort', ['name', ['name'], {'name': True}])
    def test_name_is_an_accepted_sort_key(self, mock_subprocess, sort):
        mock_subprocess.setup(stdout=self.LINE)
        ListCommand()(sort=sort)
        argv = _argv(mock_subprocess)
        assert argv[argv.index('-s') + 1] == 'name'

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_name_descending(self, mock_subprocess):
        mock_subprocess.setup(stdout=self.LINE)
        ListCommand()(sort={'name': False, 'creation': True})
        argv = _argv(mock_subprocess)
        assert argv[argv.index('-S') + 1] == 'name'
        assert argv[argv.index('-s') + 1] == 'creation'

    @pytest.mark.unit
    def test_name_is_accepted_only_as_a_sort_key(self):
        ListCommand._check_sort_key('name')
        ListCommand._check_sort_key('creation')
        ListCommand._check_sort_key('user:x')
        assert 'name' not in PropertyNames.all
        with pytest.raises(ValueError):
            Validate.attribute('name')
        with pytest.raises(ValueError):
            ListCommand._check_sort_key('bogus')
        with pytest.raises(ValueError):
            ListCommand._check_sort_key('names')

    @pytest.mark.unit
    @pytest.mark.subprocess
    @pytest.mark.parametrize('sort', [
        'not_a_property', ['creation', 'not_a_property'], ['name', 'not_a_property'], {'creation': True, 'not_a_property': False},
    ])
    def test_invalid_sort_key_raises_before_exec(self, mock_subprocess, sort):
        mock_subprocess.setup(stdout=self.LINE)
        with pytest.raises(ValueError):
            ListCommand()(sort=sort)
        mock_subprocess.assert_not_called()

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_user_property_is_a_valid_sort_key(self, mock_subprocess):
        mock_subprocess.setup(stdout=self.LINE)
        ListCommand()(sort={'user:rank': False})
        argv = _argv(mock_subprocess)
        assert argv[argv.index('-S') + 1] == 'user:rank'


class TestMultipleRootsAreSeparateArgs:
    """Each root is its own argv element (zfs list/get take a space-separated dataset list), never comma-joined."""

    ROOTS = [Filesystem('testpool/a'), Filesystem('testpool/b'), Snapshot(Filesystem('testpool/c'), 'snap')]
    NAMES = ['testpool/a', 'testpool/b', 'testpool/c@snap']

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_list_roots_each_their_own_element(self, mock_subprocess):
        mock_subprocess.setup(stdout=[])
        ListCommand()(self.ROOTS)
        argv = _argv(mock_subprocess)
        assert argv[-3:] == self.NAMES
        assert not any(',' in a and '/' in a for a in argv)

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_list_single_root(self, mock_subprocess):
        mock_subprocess.setup(stdout=[])
        ListCommand()(Filesystem('testpool/a'))
        assert _argv(mock_subprocess)[-1] == 'testpool/a'

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_list_no_roots_adds_no_positional(self, mock_subprocess):
        mock_subprocess.setup(stdout=[])
        ListCommand()()
        assert _argv(mock_subprocess) == ['list', '-H', '-o', 'name,type']

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_get_roots_each_their_own_element(self, mock_subprocess):
        mock_subprocess.setup(stdout=[])
        GetCommand()(self.ROOTS, properties='compression')
        argv = _argv(mock_subprocess)
        assert argv[-3:] == self.NAMES
        assert argv[-4] == 'name,type,compression'
        assert not any(',' in a and '/' in a for a in argv)

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_get_single_root(self, mock_subprocess):
        mock_subprocess.setup(stdout=[])
        GetCommand()(Filesystem('testpool/a'), properties='compression')
        argv = _argv(mock_subprocess)
        assert argv[-2:] == ['name,type,compression', 'testpool/a']

    @pytest.mark.unit
    def test_get_options_accepts_a_string(self):
        assert GetCommand._get_options(properties=['all'], datasets='testpool/a') == ['all', 'testpool/a']

    @pytest.mark.unit
    @pytest.mark.parametrize('kind', [list, tuple])
    def test_get_options_accepts_a_list(self, kind):
        argv = GetCommand._get_options(properties=['all'], datasets=kind(['testpool/a', 'testpool/b']))
        assert argv == ['all', 'testpool/a', 'testpool/b']

    @pytest.mark.unit
    def test_get_options_empty_list_adds_nothing(self):
        assert GetCommand._get_options(properties=['all'], datasets=[]) == ['all']


class TestExecLogging:
    """Commands are logged at DEBUG on the 'libzfseasy' logger; nothing is ever printed."""

    @staticmethod
    def _records(caplog):
        return [r for r in caplog.records if r.name == 'libzfseasy' and r.levelno == logging.DEBUG]

    @staticmethod
    def _stream_process():
        process = MagicMock()
        process.stdout.peek.return_value = b'ZFS_DATA'
        process.poll.return_value = None
        process.stdin = io.BufferedWriter(io.BytesIO())
        return process

    @pytest.mark.unit
    def test_the_module_logger_is_named_libzfseasy(self):
        zfs_module = importlib.import_module('libzfseasy.zfs')
        assert isinstance(zfs_module.logger, logging.Logger)
        assert zfs_module.logger.name == 'libzfseasy'

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_exec_logs_at_debug(self, mock_subprocess, caplog, capsys):
        mock_subprocess.setup(stdout='')
        with caplog.at_level(logging.DEBUG, logger='libzfseasy'):
            Command._exec(['zfs', 'mount', '-a'])
        assert [r.getMessage() for r in self._records(caplog)] == ['Executing command: zfs mount -a']
        assert capsys.readouterr().out == ''

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_exec_out_logs_at_debug(self, mock_subprocess, caplog, capsys):
        mock_subprocess.setup(stdout=['x\n'])
        with caplog.at_level(logging.DEBUG, logger='libzfseasy'):
            assert list(Command._exec_out(['zfs', 'list', '-H'])) == ['x']
        assert [r.getMessage() for r in self._records(caplog)] == ['Executing command: zfs list -H']
        assert capsys.readouterr().out == ''

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_exec_stream_logs_at_debug(self, mock_subprocess, caplog, capsys):
        mock_subprocess.return_value = self._stream_process()
        with caplog.at_level(logging.DEBUG, logger='libzfseasy'):
            SendCommand._exec_stream(['zfs', 'send', 'testpool/fs@snap'])
        assert [r.getMessage() for r in self._records(caplog)] == ['Executing command: zfs send testpool/fs@snap']
        assert capsys.readouterr().out == ''

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_exec_stream_in_logs_at_debug(self, mock_subprocess, caplog, capsys):
        mock_subprocess.return_value = self._stream_process()
        with caplog.at_level(logging.DEBUG, logger='libzfseasy'):
            ReceiveCommand._exec_stream_in(['zfs', 'receive', 'testpool/ds'])
        assert [r.getMessage() for r in self._records(caplog)] == ['Executing command: zfs receive testpool/ds']
        assert capsys.readouterr().out == ''

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_arguments_with_spaces_are_shell_quoted(self, mock_subprocess, caplog):
        mock_subprocess.setup(stdout='')
        cmd = ['zfs', 'rename', 'testpool/my fs', "testpool/it's"]
        with caplog.at_level(logging.DEBUG, logger='libzfseasy'):
            Command._exec(cmd)
        (record,) = self._records(caplog)
        message = record.getMessage()
        assert message == 'Executing command: ' + shlex.join(cmd)
        assert "'testpool/my fs'" in message
        assert shlex.split(message[len('Executing command: '):]) == cmd

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_a_high_level_command_logs_its_argv_once(self, mock_subprocess, caplog, capsys, sample_filesystem):
        mock_subprocess.setup(stdout=[])
        with caplog.at_level(logging.DEBUG, logger='libzfseasy'):
            ListCommand()(sample_filesystem)
        messages = [r.getMessage() for r in self._records(caplog)]
        assert len(messages) == 1
        assert messages[0].endswith('list -H -o name,type ' + sample_filesystem.name)
        assert capsys.readouterr().out == ''

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_nothing_is_logged_above_the_debug_level(self, mock_subprocess, caplog):
        mock_subprocess.setup(stdout='')
        with caplog.at_level(logging.INFO, logger='libzfseasy'):
            Command._exec(['zfs', 'mount', '-a'])
        assert self._records(caplog) == []

    @pytest.mark.unit
    @pytest.mark.subprocess
    @pytest.mark.parametrize('debug', [True, False])
    def test_the_debug_flag_is_accepted_but_ignored(self, mock_subprocess, caplog, capsys, monkeypatch, debug):
        zfs_module = importlib.import_module('libzfseasy.zfs')
        monkeypatch.setattr(zfs_module, 'debug', debug)
        mock_subprocess.setup(stdout='')
        with caplog.at_level(logging.DEBUG, logger='libzfseasy'):
            Command._exec(['zfs', 'mount', '-a'])
        assert len(self._records(caplog)) == 1
        assert capsys.readouterr().out == ''


class TestGetLinesToObjectsNameChange:
    """A change of dataset name resets the accumulator to the *new* name (it used to reset to '')."""

    @staticmethod
    def _row(name, prop, value, source='local'):
        return '%s\t%s\t%s\t-\t%s' % (name, prop, value, source)

    @pytest.mark.unit
    def test_single_row_datasets_all_yield(self):
        lines = [
            self._row('testpool/a', 'type', 'filesystem', '-'),
            self._row('testpool/b', 'type', 'volume', '-'),
            self._row('testpool/c', 'type', 'filesystem', '-'),
        ]
        result = list(GetCommand._lines_to_objects(iter(lines), ['all']))
        assert [r.name for r in result] == ['testpool/a', 'testpool/b', 'testpool/c']
        assert [type(r) for r in result] == [Filesystem, Volume, Filesystem]

    @pytest.mark.unit
    def test_interleaved_datasets_keep_their_own_properties(self):
        lines = [
            self._row('testpool/a', 'type', 'filesystem', '-'),
            self._row('testpool/a', 'compression', 'lz4'),
            self._row('testpool/b', 'type', 'filesystem', '-'),
            self._row('testpool/b', 'compression', 'gzip'),
            self._row('testpool/b', 'atime', 'off'),
            self._row('testpool/c', 'type', 'filesystem', '-'),
            self._row('testpool/c', 'quota', '1G'),
        ]
        a, b, c = GetCommand._lines_to_objects(iter(lines), ['all'])
        assert (a.name, b.name, c.name) == ('testpool/a', 'testpool/b', 'testpool/c')
        assert a['compression'].value == 'lz4' and a['atime'] is None and a['quota'] is None
        assert b['compression'].value == 'gzip' and b['atime'].value == 'off' and b['quota'] is None
        assert c['quota'].value == '1G' and c['compression'] is None and c['atime'] is None

    @pytest.mark.unit
    def test_the_last_dataset_is_yielded(self):
        lines = [
            self._row('testpool/a', 'type', 'filesystem', '-'),
            self._row('testpool/b', 'type', 'filesystem', '-'),
            self._row('testpool/b', 'compression', 'zstd'),
        ]
        result = list(GetCommand._lines_to_objects(iter(lines), ['all']))
        assert len(result) == 2
        assert result[-1].name == 'testpool/b'
        assert result[-1]['compression'].value == 'zstd'

    @pytest.mark.unit
    def test_a_dataset_after_a_typeless_one_is_not_lost(self):
        lines = [
            self._row('testpool/a', 'compression', 'lz4'),
            self._row('testpool/b', 'type', 'filesystem', '-'),
            self._row('testpool/b', 'compression', 'gzip'),
        ]
        result = list(GetCommand._lines_to_objects(iter(lines), ['all']))
        assert [r.name for r in result] == ['testpool/b']
        assert result[0]['compression'].value == 'gzip'

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_end_to_end_through_get(self, mock_subprocess):
        # (names must be in scope of the requested root: B2 drops rows zfs's own iteration cannot have printed)
        # (an explicit property list: zfs prints every object's `name`, `type` and each requested property, in that
        # order; `a`'s `compression` is `default`, which `sources=['local']` filters out in the parser, so the test
        # still sees the properties stay with their own object)
        mock_subprocess.setup(stdout=[
            'testpool/a\tname\ttestpool/a\t-\t-\n',
            'testpool/a\ttype\tfilesystem\t-\t-\n',
            'testpool/a\tcompression\toff\t-\tdefault\n',
            'testpool/a/b\tname\ttestpool/a/b\t-\t-\n',
            'testpool/a/b\ttype\tvolume\t-\t-\n',
            'testpool/a/b\tcompression\tgzip\t-\tlocal\n',
        ])
        result = GetCommand()(Filesystem('testpool/a'), properties='compression', recursive=True, sources=['local'])
        assert [r.name for r in result] == ['testpool/a', 'testpool/a/b']
        assert result[0]['compression'] is None
        assert result[1]['compression'].value == 'gzip'


class TestRecursiveSnapshotChildListing:
    """The recursive child listing asks for filesystems and volumes only, never snapshots or bookmarks."""

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_child_listing_uses_filesystem_and_volume_types(self, mock_subprocess, sample_filesystem):
        mock_subprocess.setup_multi(
            (['testpool/filesystem\tfilesystem', 'testpool/filesystem/vol\tvolume'],),
            ('',),
        )
        result = SnapshotCommand()(sample_filesystem, 'snap1', recursive=True)

        assert mock_subprocess.call_count == 2
        listing = _argv(mock_subprocess, 0)
        assert listing[0] == 'list'
        assert listing[listing.index('-t') + 1] == 'filesystem,volume'
        assert '-r' in listing
        assert listing[-1] == 'testpool/filesystem'
        assert [str(s) for s in result] == ['testpool/filesystem@snap1', 'testpool/filesystem/vol@snap1']

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_snapshot_argv_is_unchanged(self, mock_subprocess, sample_filesystem):
        mock_subprocess.setup_multi(
            (['testpool/filesystem\tfilesystem'],),
            ('',),
        )
        SnapshotCommand()(sample_filesystem, 'snap1', recursive=True)
        assert _argv(mock_subprocess, 1) == ['snapshot', '-r', 'testpool/filesystem@snap1']

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_non_recursive_does_not_list(self, mock_subprocess, sample_filesystem):
        mock_subprocess.setup(stdout='')
        SnapshotCommand()(sample_filesystem, 'snap1')
        assert mock_subprocess.call_count == 1
        assert _argv(mock_subprocess) == ['snapshot', 'testpool/filesystem@snap1']


INVALID_DATASETS = ['pool/.system', 'pool/-x', 'pool/:x', 'pool/a ', 'pool/ a']
INVALID_SNAPSHOTS = ['pool/fs@ manual', 'pool/fs@manual ', 'pool/.system@snap']
INVALID_BOOKMARKS = ['pool/-x#bm', 'pool/fs# bm']


@pytest.fixture
def warned_names(monkeypatch):
    """A clean once-per-name registry (the real one is process-global) for the skip-and-warn tests."""
    zfs_module = importlib.import_module('libzfseasy.zfs')
    monkeypatch.setattr(zfs_module, '_warned_names', set())
    return zfs_module


def _skip_warnings(caplog):
    return [r.getMessage() for r in caplog.records if r.name == 'libzfseasy' and r.levelno == logging.WARNING]


class TestListSkipsInvalidNames:
    """`zfs list` rows whose names zfs allows but libzfseasy refuses are skipped; everything else is untouched."""

    GOOD = ['pool/a\tfilesystem\n', 'pool/b\tvolume\n', 'pool/c\tfilesystem\n']

    @staticmethod
    def _row(name):
        dstype = 'snapshot' if '@' in name else 'bookmark' if '#' in name else 'filesystem'
        return name + '\t' + dstype + '\n'

    @pytest.mark.unit
    @pytest.mark.subprocess
    @pytest.mark.parametrize('name', INVALID_DATASETS + INVALID_SNAPSHOTS + INVALID_BOOKMARKS)
    @pytest.mark.parametrize('position', [0, 1, 3])
    def test_invalid_row_is_skipped_wherever_it_sits(self, mock_subprocess, warned_names, name, position):
        lines = list(self.GOOD)
        lines.insert(position, self._row(name))
        mock_subprocess.setup(stdout=lines)
        result = ListCommand()()
        assert [r.name for r in result] == ['pool/a', 'pool/b', 'pool/c']
        assert [type(r) for r in result] == [Filesystem, Volume, Filesystem]

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_lazy_listing_skips_too(self, mock_subprocess, warned_names):
        mock_subprocess.setup(stdout=[self._row('pool/.system'), self.GOOD[0], self._row('pool/-x'), self.GOOD[1]])
        result = ListCommand()(lazy=True)
        assert hasattr(result, '__next__')
        assert [r.name for r in result] == ['pool/a', 'pool/b']

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_only_invalid_rows_gives_an_empty_list(self, mock_subprocess, warned_names):
        mock_subprocess.setup(stdout=[self._row(n) for n in INVALID_DATASETS])
        assert ListCommand()() == []

    @pytest.mark.unit
    def test_line_to_object_returns_none_for_an_invalid_name(self, warned_names):
        assert ListCommand._line_to_object('pool/.system\tfilesystem', ['name', 'type']) is None
        assert isinstance(ListCommand._line_to_object('pool/a\tfilesystem', ['name', 'type']), Filesystem)

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_properties_of_a_skipped_row_do_not_reach_neighbours(self, mock_subprocess, warned_names):
        mock_subprocess.setup(stdout=[
            'pool/a\tfilesystem\tlz4\n',
            'pool/.system\tfilesystem\tgzip\n',
            'pool/c\tfilesystem\t-\n',
        ])
        a, c = ListCommand()(properties='compression')
        assert a['compression'].value == 'lz4'
        assert c['compression'] is None

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_an_unknown_type_is_skipped_not_raised(self, mock_subprocess, warned_names, caplog):
        # B2: a row whose type is not filesystem/volume/snapshot/bookmark cannot be trusted (it is what a forged
        # row looks like), so it is skipped with a WARNING instead of aborting the listing.
        mock_subprocess.setup(stdout=['pool/a\tbogus\n'])
        with caplog.at_level(logging.WARNING, logger='libzfseasy'):
            result = ListCommand()()
        assert result == []
        warnings = _skip_warnings(caplog)
        assert len(warnings) == 1
        assert "unknown type 'bogus'" in warnings[0]


class TestListLenientColumns:
    """Columns that are `-` on some rows (a volsize on a filesystem) must not break the listing."""

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_volsize_dash_on_a_filesystem_row(self, mock_subprocess):
        mock_subprocess.setup(stdout=['pool/fs\tfilesystem\t-\n', 'pool/vol\tvolume\t1G\n'])
        fs, vol = ListCommand()(properties='volsize')
        assert isinstance(fs, Filesystem) and isinstance(vol, Volume)
        assert vol['volsize'].value == '1G'

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_filesystem_row_never_gets_a_volsize(self, mock_subprocess):
        mock_subprocess.setup(stdout=['pool/fs\tfilesystem\t-\n'])
        (fs,) = ListCommand()(properties='volsize')
        assert not any(k == 'volsize' for k, _ in fs.properties)

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_user_property_dash_gives_no_entry(self, mock_subprocess):
        mock_subprocess.setup(stdout=['pool/fs\tfilesystem\t-\tmine\n'])
        (fs,) = ListCommand()(properties=['user:unset', 'user:set'])
        assert 'user:unset' not in fs
        assert fs['user:unset'] is None
        assert fs['user:set'].value == 'mine'

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_ordinary_values_still_apply(self, mock_subprocess):
        mock_subprocess.setup(stdout=['pool/fs\tfilesystem\tlz4\ton\n'])
        (fs,) = ListCommand()(properties=['compression', 'atime'])
        assert fs['compression'].value == 'lz4'
        assert fs['atime'].value == 'on'


class TestGetExplicitPropertySequence:
    """An explicit property list is answered with `name`, `type` and each requested property, once, in that order
    (the full matrix, `rows()` included, is `TestGetExplicitSequence` in tests/test_framing.py)."""

    @staticmethod
    def _rows(name, props):
        return ['%s\t%s\t%s\t-\t%s\n' % (name, p, v, s) for p, v, s in
                [('name', name, '-'), ('type', 'filesystem', '-')] + props]

    @pytest.mark.unit
    @pytest.mark.subprocess
    @pytest.mark.parametrize('lazy', [False, True], ids=['list', 'lazy'])
    def test_the_requested_sequence_with_a_repeat_in_the_request_is_accepted(self, mock_subprocess, lazy):
        mock_subprocess.setup(stdout=self._rows('testpool/a', [('compression', 'lz4', 'local'),
                                                               ('atime', 'on', 'default')]))
        result = list(GetCommand()(Filesystem('testpool/a'), properties=['compression', 'atime', 'compression'],
                                   lazy=lazy))
        assert _argv(mock_subprocess)[-2:] == ['name,type,compression,atime', 'testpool/a']
        assert [(r.name, r['compression'].value, r['atime'].value) for r in result] == [('testpool/a', 'lz4', 'on')]

    @pytest.mark.unit
    @pytest.mark.subprocess
    @pytest.mark.parametrize('lazy', [False, True], ids=['list', 'lazy'])
    @pytest.mark.parametrize('shape,rows', [
        ('missing', [('compression', 'lz4', 'local')]),
        ('reordered', [('atime', 'on', 'default'), ('compression', 'lz4', 'local')]),
        ('unrequested', [('compression', 'lz4', 'local'), ('atime', 'on', 'default'), ('used', '1K', '-')]),
    ])
    def test_anything_else_refuses_the_object_and_keeps_its_neighbour(self, mock_subprocess, caplog, lazy, shape, rows):
        mock_subprocess.setup(stdout=self._rows('testpool/a', rows) + self._rows(
            'testpool/b', [('compression', 'gzip', 'local'), ('atime', 'off', 'local')]))
        with caplog.at_level(logging.WARNING, logger='libzfseasy'):
            result = list(GetCommand()(Filesystem('testpool'), properties=['compression', 'atime'], recursive=True,
                                       lazy=lazy))
        assert [r.name for r in result] == ['testpool/b']
        assert result[0]['compression'].value == 'gzip'
        assert len(_skip_warnings(caplog)) == 1


class TestGetSkipsInvalidNames:

    @staticmethod
    def _rows(name, dstype='filesystem', compression=None, explicit=False):
        # `explicit`: the rows of an explicit property list (`name`, `type`, ...); otherwise those of `all` (`type`, ...)
        rows = ['%s\tname\t%s\t-\t-' % (name, name)] if explicit else []
        rows.append('%s\ttype\t%s\t-\t-' % (name, dstype))
        if compression:
            rows.append('%s\tcompression\t%s\t-\tlocal' % (name, compression))
        return rows

    @pytest.mark.unit
    @pytest.mark.parametrize('bad', [
        ('pool/.system', 'filesystem'), ('pool/-x', 'volume'), ('pool/:x', 'filesystem'), ('pool/a ', 'filesystem'),
        ('pool/ a', 'filesystem'), ('pool/fs@ manual', 'snapshot'), ('pool/fs@manual ', 'snapshot'),
        ('pool/.system@snap', 'snapshot'), ('pool/-x#bm', 'bookmark'),
    ])
    @pytest.mark.parametrize('position', ['first', 'middle', 'last'])
    def test_invalid_dataset_is_skipped_without_property_bleed(self, warned_names, bad, position):
        a = self._rows('pool/a', compression='lz4')
        b = self._rows(bad[0], bad[1], compression='gzip')
        c = self._rows('pool/c', compression='zstd')
        order = {'first': [b, a, c], 'middle': [a, b, c], 'last': [a, c, b]}[position]
        lines = [line for block in order for line in block]
        result = list(GetCommand._lines_to_objects(iter(lines), ['all']))
        assert all(isinstance(r, ZFS) for r in result)
        assert sorted(r.name for r in result) == ['pool/a', 'pool/c']
        by_name = {r.name: r for r in result}
        assert by_name['pool/a']['compression'].value == 'lz4'
        assert by_name['pool/c']['compression'].value == 'zstd'

    @pytest.mark.unit
    def test_an_invalid_dataset_with_only_a_type_row(self, warned_names):
        lines = self._rows('pool/a') + self._rows('pool/.system') + self._rows('pool/c')
        result = list(GetCommand._lines_to_objects(iter(lines), ['all']))
        assert [r.name for r in result] == ['pool/a', 'pool/c']

    @pytest.mark.unit
    def test_an_invalid_last_dataset_with_only_a_type_row(self, warned_names):
        lines = self._rows('pool/a') + self._rows('pool/-x')
        assert [r.name for r in GetCommand._lines_to_objects(iter(lines), ['all'])] == ['pool/a']

    @pytest.mark.unit
    def test_build_returns_none_for_an_invalid_name(self, warned_names):
        assert GetCommand._build('pool/.system', 'filesystem', {}) is None
        assert GetCommand._build('pool/fs@ manual', 'snapshot', {}) is None
        assert isinstance(GetCommand._build('pool/a', 'filesystem', {}), Filesystem)

    @pytest.mark.unit
    def test_other_errors_still_propagate(self, warned_names, caplog):
        with caplog.at_level(logging.WARNING, logger='libzfseasy'):
            with pytest.raises(ValueError, match='not a valid ZFS type') as info:
                GetCommand._build('pool/a', 'bogus', {})
        assert not isinstance(info.value, InvalidNameError)
        assert _skip_warnings(caplog) == []

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_end_to_end_through_get(self, mock_subprocess, warned_names):
        lines = [line + '\n' for line in
                 self._rows('pool/a', compression='lz4', explicit=True) +
                 self._rows('pool/a/.system', compression='off', explicit=True) +
                 self._rows('pool/a/c', 'volume', compression='zstd', explicit=True)]
        mock_subprocess.setup(stdout=lines)
        result = GetCommand()(Filesystem('pool/a'), properties='compression', recursive=True)
        assert [r.name for r in result] == ['pool/a', 'pool/a/c']
        assert [r['compression'].value for r in result] == ['lz4', 'zstd']
        lazy_lines = [line + '\n' for line in
                      self._rows('pool/a/.system', compression='off', explicit=True) +
                      self._rows('pool/a/c', 'volume', compression='zstd', explicit=True)]
        mock_subprocess.setup(stdout=lazy_lines)
        lazy = GetCommand()(Filesystem('pool/a'), properties='compression', recursive=True, lazy=True)
        assert [r.name for r in lazy] == ['pool/a/c']


class TestSkipInvalidNameWarning:
    """One per-name WARNING per distinct name per process on the 'libzfseasy' logger, bounded by _MAX_WARNED_NAMES.

    A listing that logged a per-name WARNING also logs one summary line ("skipped N name(s) libzfseasy cannot
    represent in zfs <cmd> output"); these tests count the per-name records, so `_names_only` drops the summary."""

    @staticmethod
    def _list(mock_subprocess, *names):
        mock_subprocess.setup(stdout=[n + '\tfilesystem\n' for n in names] + ['pool/ok\tfilesystem\n'])
        return ListCommand()()

    @staticmethod
    def _names_only(caplog):
        return [m for m in _skip_warnings(caplog) if not m.startswith('skipped ')]

    @staticmethod
    def _summaries(caplog, level=logging.WARNING):
        return [r.getMessage() for r in caplog.records
                if r.name == 'libzfseasy' and r.levelno == level and r.getMessage().startswith('skipped ')]

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_message_names_the_row_and_the_reason(self, mock_subprocess, warned_names, caplog):
        with caplog.at_level(logging.WARNING, logger='libzfseasy'):
            self._list(mock_subprocess, 'pool/.system')
        (message,) = self._names_only(caplog)
        assert 'skipping' in message
        assert 'pool/.system' in message
        assert 'cannot represent' in message
        assert self._summaries(caplog) == ['skipped 1 name libzfseasy cannot represent in zfs list output']

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_a_repeated_name_warns_once(self, mock_subprocess, warned_names, caplog):
        with caplog.at_level(logging.WARNING, logger='libzfseasy'):
            self._list(mock_subprocess, 'pool/.system')
            self._list(mock_subprocess, 'pool/.system')
            self._list(mock_subprocess, 'pool/.system')
        assert len(self._names_only(caplog)) == 1
        assert len(self._summaries(caplog)) == 1        # the repeats are summarised at DEBUG

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_distinct_names_each_warn(self, mock_subprocess, warned_names, caplog):
        with caplog.at_level(logging.WARNING, logger='libzfseasy'):
            self._list(mock_subprocess, 'pool/.system', 'pool/-x', 'pool/a ')
        messages = self._names_only(caplog)
        assert len(messages) == 3
        for name in ('pool/.system', 'pool/-x', 'pool/a '):
            assert sum(name in m for m in messages) == 1
        assert self._summaries(caplog) == ['skipped 3 names libzfseasy cannot represent in zfs list output']

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_list_and_get_share_the_registry(self, mock_subprocess, warned_names, caplog):
        with caplog.at_level(logging.WARNING, logger='libzfseasy'):
            self._list(mock_subprocess, 'pool/.system')
            list(GetCommand._lines_to_objects(iter(['pool/.system\ttype\tfilesystem\t-\t-']), ['all']))
        assert len(self._names_only(caplog)) == 1

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_valid_rows_never_warn(self, mock_subprocess, warned_names, caplog):
        with caplog.at_level(logging.WARNING, logger='libzfseasy'):
            self._list(mock_subprocess)
        assert _skip_warnings(caplog) == []
        assert warned_names._warned_names == set()

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_the_registry_is_bounded(self, mock_subprocess, warned_names, caplog, monkeypatch):
        monkeypatch.setattr(warned_names, '_MAX_WARNED_NAMES', 2)
        with caplog.at_level(logging.WARNING, logger='libzfseasy'):
            self._list(mock_subprocess, 'pool/.a', 'pool/.b', 'pool/.c', 'pool/.d')
            self._list(mock_subprocess, 'pool/.e', 'pool/.a')
        messages = self._names_only(caplog)
        assert sum('pool/.a' in m for m in messages) == 1
        assert sum('pool/.b' in m for m in messages) == 1
        assert not any(n in m for m in messages for n in ('pool/.c', 'pool/.d', 'pool/.e'))
        assert sum('further ones are skipped silently' in m for m in messages) == 1
        assert len(messages) == 3
        assert len(warned_names._warned_names) <= 3      # two names plus the one overflow sentinel
        # The first listing logged per-name WARNINGs (and counts all four of its names); the second logged none.
        assert self._summaries(caplog) == ['skipped 4 names libzfseasy cannot represent in zfs list output']


class TestListNewNativeProperties:
    """`receive_resume_token` and the volume properties added to the per-type tables can be listed."""

    TOKEN = '1-c0858fc6f-c0-789c636064000310a501c49c50360710a715e5e7a69766a63040c1eabb735735ce8f8d5883e4b6dd'

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_receive_resume_token_argv(self, mock_subprocess):
        mock_subprocess.setup(stdout=[])
        ListCommand()(roots=Filesystem('pool/fs'), properties=['receive_resume_token'])
        assert _argv(mock_subprocess) == ['list', '-H', '-o', 'name,type,receive_resume_token', 'pool/fs']

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_token_row_and_dash_row(self, mock_subprocess):
        mock_subprocess.setup(stdout=[
            'pool/partial\tfilesystem\t' + self.TOKEN + '\n',
            'pool/complete\tfilesystem\t-\n',
        ])
        partial, complete = ListCommand()(properties='receive_resume_token')
        assert isinstance(partial, Filesystem) and isinstance(complete, Filesystem)
        assert partial['receive_resume_token'].value == self.TOKEN
        assert complete['receive_resume_token'] is None
        assert 'receive_resume_token' not in complete

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_token_dash_on_a_volume_row_does_not_break_the_listing(self, mock_subprocess):
        mock_subprocess.setup(stdout=['pool/v\tvolume\t-\n'])
        (vol,) = ListCommand()(properties='receive_resume_token')
        assert isinstance(vol, Volume)

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_volume_properties(self, mock_subprocess):
        mock_subprocess.setup(stdout=['pool/v\tvolume\tall\t0\toff\n'])
        (vol,) = ListCommand()(properties=['prefetch', 'special_small_blocks', 'volthreading'])
        assert vol['prefetch'].value == 'all'
        assert vol['special_small_blocks'].value == '0'
        assert vol['volthreading'].value == 'off'

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_snapshot_encryption_properties(self, mock_subprocess):
        mock_subprocess.setup(stdout=['pool/fs@s\tsnapshot\tpool/enc\tavailable\n'])
        (snap,) = ListCommand()(types='snapshot', properties=['encryptionroot', 'keystatus'])
        assert snap['encryptionroot'].value == 'pool/enc'
        assert snap['keystatus'].value == 'available'


# Real tokens and `zfs send -nv -t` output captured on a zfs-2.4.1 VM.
TOKEN_FULL = '1-a292306e5-b8-789c636064000310a500c4ec50360710e72765a52697303030419460caa7a515a7968064cae0f26c48f2499525a9c520f1d00aacfa4bf2d34b33531818f238834cbe7b33276a20c97382e5f312735319188a4a0af48b53f352f4131d8a0da16e0100ed3915db'
TOKEN_INCR = '1-e06a8f740-e0-789c636064000310a501c49c50360710a715e5e7a69766a63040c121db7a11f66796fe0a40363b92bafca4acd4e412060666a83a0cf9b4b4e2d412904c195c9e0d493ea9b224b518c40eaec0aabf241fe20acdab06df0f7931276a20c97382e5f312735319188a4a0af48b53f352f4931c8a8da06e0100e3ab1d54'
TOKEN_FULL_GONE = '1-aa54030d2-b8-789c636064000310a500c4ec50360710e72765a52697303030419460caa7a515a7968064cae0f26c48f2499525a9c520f1d00aacfa4bf2d34b33531818e66ddfb57f2abff03d0d24794eb07c5e626e2a03435149817e716a5e8a7eb243b121d42d004428179d'
TOKEN_INCR_GONE = '1-bf7b7f36f-e0-789c636064000310a501c49c50360710a715e5e7a69766a6304001c7fd390e4c91df5914806c762475f94959a9c9250c0c0d507518f26969c5a925209932b83c1b927c5265496a31902e48aac0aabf241fe20ab143ef244f0898a46820c97382e5f312735319188a4a0af48b53f352f4531c8a8da06e0100518f1e16'
TOKEN_RENAMED = '1-b183e6b6c-b8-789c636064000310a500c4ec50360710e72765a52697303030419460caa7a515a7968064cae0f26c48f2499525a9c520f1d00aacfa4bf2d34b33531818befa87715c5f36e59a06923c27583e2f31379581a1a8a440bf38352f453fd5a1d810ea1600488217c5'
TOKEN_SOURCE_GONE = '1-f87d8e914-e0-789c636064000310a501c49c50360710a715e5e7a69766a6304081abc3eacd47ce9e8d5300b2d991d4e52765a5269730303043d561c8a7a515a7968064cae0f26c48f2499525a9c52076700556fd25f910572c7c3e5df3ebdda3573490e439c1f27989b9a90c0c452505fac5a97929fa690ec54650b700005e8b210e'

FULL_CONTENTS = [
    'resume token contents:', 'nvlist version: 0', '\tobject = 0x2', '\toffset = 0x760000', '\tbytes = 0x785520',
    '\ttoguid = 0x61034bf73452096e', '\ttoname = rtp/send/a@s1',
]
INCR_CONTENTS = [
    'resume token contents:', 'nvlist version: 0', '\tfromguid = 0x4f39e607147f3dc2', '\tobject = 0x3',
    '\toffset = 0x760000', '\tbytes = 0x785308', '\ttoguid = 0x61034ac2f730d529', '\ttoname = rtp/send/b@s2',
]
ESTIMATES = ['full send of rtp/send/a@s1 estimated size is 21.2M', 'total estimated size is 21.2M']


def _lines(*blocks):
    return [line + '\n' for block in blocks for line in block]


class TestResumeInfo:
    """`zfs send -nv -t <token>` decoded into a ResumeToken (never sends anything)."""

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_argv_is_exactly_send_nv_t_token(self, mock_subprocess):
        mock_subprocess.setup(stdout=_lines(FULL_CONTENTS, ESTIMATES))
        SendCommand.resume_info(TOKEN_FULL)
        assert mock_subprocess.call_count == 1
        assert list(mock_subprocess.call_args[0][0])[1:] == ['send', '-nv', '-t', TOKEN_FULL]

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_reachable_through_the_public_send_singleton(self, mock_subprocess):
        mock_subprocess.setup(stdout=_lines(FULL_CONTENTS, ESTIMATES))
        assert SendCommand().resume_info(TOKEN_FULL).toname == 'rtp/send/a@s1'

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_valid_full_token(self, mock_subprocess):
        mock_subprocess.setup(stdout=_lines(FULL_CONTENTS, ESTIMATES))
        info = SendCommand.resume_info(TOKEN_FULL)
        assert info == ResumeToken(toname='rtp/send/a@s1', toguid=0x61034bf73452096e, fromguid=None, object=2,
                                   offset=0x760000, bytes=0x785520, flags=frozenset(), error=None)

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_valid_incremental_token(self, mock_subprocess):
        mock_subprocess.setup(stdout=_lines(INCR_CONTENTS, ['send from rtp/send/b@s1 to rtp/send/b@s2 estimated '
                                                            'size is 21.2M', 'total estimated size is 21.2M']))
        info = SendCommand.resume_info(TOKEN_INCR)
        assert info.fromguid == 0x4f39e607147f3dc2 == 5708846920751857090
        assert info.toguid == 0x61034ac2f730d529 == 6990513247840425257
        assert (info.object, info.offset, info.bytes) == (3, 7733248, 7885576)
        assert info.toname == 'rtp/send/b@s2'
        assert info.error is None

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_hex_values_become_ints(self, mock_subprocess):
        mock_subprocess.setup(stdout=_lines(FULL_CONTENTS))
        info = SendCommand.resume_info(TOKEN_FULL)
        for value in (info.toguid, info.object, info.offset, info.bytes):
            assert type(value) is int
        assert info.offset == 7733248 and info.bytes == 7886112

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_flags_with_one_are_counted_and_zero_are_not(self, mock_subprocess):
        mock_subprocess.setup(stdout=_lines(FULL_CONTENTS, ['\tembedok = 1', '\tcompressok = 1', '\trawok = 0',
                                                             '\tlargeblockok = 0', '\tsavedok = 1'], ESTIMATES))
        info = SendCommand.resume_info(TOKEN_FULL)
        assert info.flags == frozenset({'embedok', 'compressok', 'savedok'})
        assert isinstance(info.flags, frozenset)

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_raw_encrypted_send_flags(self, mock_subprocess):
        mock_subprocess.setup(stdout=_lines(FULL_CONTENTS, ['\tcompressok = 1', '\trawok = 1'], ESTIMATES))
        assert SendCommand.resume_info(TOKEN_FULL).flags == frozenset({'compressok', 'rawok'})

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_unknown_keys_estimates_and_junk_are_ignored(self, mock_subprocess):
        mock_subprocess.setup(stdout=_lines(
            ['resume token contents:', 'nvlist version: 0', '\tfuturekey = 0x5', '\tfuturenum = 1', '\tnoequals',
             '\tbytes = notanumber', '\tobject = 0x2'], FULL_CONTENTS[5:], ESTIMATES))
        info = SendCommand.resume_info(TOKEN_FULL)
        assert info == ResumeToken(toname='rtp/send/a@s1', toguid=0x61034bf73452096e, object=2)

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_output_before_the_contents_header_is_skipped(self, mock_subprocess):
        mock_subprocess.setup(stdout=_lines(['some banner'], FULL_CONTENTS, ESTIMATES))
        assert SendCommand.resume_info(TOKEN_FULL).toguid == 0x61034bf73452096e

    @pytest.mark.unit
    def test_resume_token_is_frozen(self):
        info = ResumeToken(toname='a@b')
        with pytest.raises(Exception):
            info.toname = 'x'
        assert info.flags == frozenset() and info.error is None and info.toguid is None

    @pytest.mark.unit
    def test_resume_token_error_is_a_value_error(self):
        assert issubclass(ResumeTokenError, ValueError)

    # Failures ------------------------------------------------------------------------------------------------------

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_stale_full_target_returns_the_contents_with_an_error(self, mock_subprocess):
        mock_subprocess.setup(
            stdout=_lines(['resume token contents:', 'nvlist version: 0', '\tobject = 0x2', '\toffset = 0x760000',
                           '\tbytes = 0x785520', '\ttoguid = 0xde130f95bfbab79e', '\ttoname = rtp/send/c@s1']),
            stderr=["cannot resume send: 'rtp/send/c@s1' used in the initial send no longer exists"],
            returncode=255)
        info = SendCommand.resume_info(TOKEN_FULL_GONE)
        assert info.toguid == 0xde130f95bfbab79e
        assert info.toname == 'rtp/send/c@s1'
        assert info.error is not None and 'no longer exists' in info.error

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_stale_incremental_target_returns_with_an_error(self, mock_subprocess):
        mock_subprocess.setup(
            stdout=_lines(['resume token contents:', 'nvlist version: 0', '\tfromguid = 0x4f75902409cdf08',
                           '\tobject = 0x80', '\toffset = 0x760000', '\tbytes = 0x786270',
                           '\ttoguid = 0x643410c819eec216', '\ttoname = rtp/send/d@s2']),
            stderr=["cannot resume send: 'rtp/send/d@s2' used in the initial send no longer exists"], returncode=255)
        info = SendCommand.resume_info(TOKEN_INCR_GONE)
        assert info.fromguid == 0x4f75902409cdf08 and info.object == 0x80
        assert 'no longer exists' in info.error

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_invalid_cross_device_link_returns_with_an_error(self, mock_subprocess):
        mock_subprocess.setup(
            stdout=_lines(['resume token contents:', 'nvlist version: 0', '\tfromguid = 0x5ecdcdc4b3ab4045',
                           '\tobject = 0x3', '\toffset = 0x760000', '\tbytes = 0x785308',
                           '\ttoguid = 0xd4c5ddf52997e7a1', '\ttoname = rtp/send/f@s2']),
            stderr=['cannot resume send: Invalid cross-device link'], returncode=255)
        info = SendCommand.resume_info(TOKEN_SOURCE_GONE)
        assert info.toname == 'rtp/send/f@s2'
        assert 'Invalid cross-device link' in info.error

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_nonzero_with_contents_and_no_stderr_still_reports_an_error(self, mock_subprocess):
        mock_subprocess.setup(stdout=_lines(FULL_CONTENTS), returncode=255)
        info = SendCommand.resume_info(TOKEN_FULL)
        assert info.toguid is not None
        assert info.error == 'Command failed with exit code 255'

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_corrupt_token_raises_with_zfs_text(self, mock_subprocess):
        mock_subprocess.setup(stdout=[], stderr=['cannot resume send: resume token is corrupt (incorrect checksum)'],
                              returncode=255)
        with pytest.raises(ResumeTokenError) as info:
            SendCommand.resume_info('1-abc-def-123')
        assert 'resume token is corrupt (incorrect checksum)' in str(info.value)
        assert isinstance(info.value, ValueError)

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_other_failure_is_a_bare_exception(self, mock_subprocess):
        mock_subprocess.setup(stdout=[], stderr=['some other failure'], returncode=1)
        with pytest.raises(Exception, match='Some other failure') as info:
            SendCommand.resume_info(TOKEN_FULL)
        assert type(info.value) is Exception

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_other_failure_without_stderr_names_the_exit_code(self, mock_subprocess):
        mock_subprocess.setup(stdout=[], returncode=2)
        with pytest.raises(Exception, match='exit code 2') as info:
            SendCommand.resume_info(TOKEN_FULL)
        assert type(info.value) is Exception

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_success_without_contents_raises(self, mock_subprocess):
        mock_subprocess.setup(stdout=_lines(ESTIMATES), returncode=0)
        with pytest.raises(ResumeTokenError):
            SendCommand.resume_info(TOKEN_FULL)

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_the_child_is_reaped_on_every_outcome(self, mock_subprocess):
        process = mock_subprocess.setup(stdout=_lines(FULL_CONTENTS), stderr=['cannot resume send: x'],
                                        returncode=255)
        assert SendCommand.resume_info(TOKEN_FULL).error
        process.wait.assert_called_once()

        process = mock_subprocess.setup(stdout=[], stderr=['cannot resume send: resume token is corrupt'],
                                        returncode=255)
        with pytest.raises(ResumeTokenError):
            SendCommand.resume_info(TOKEN_FULL)
        process.wait.assert_called_once()

    # Token shape ---------------------------------------------------------------------------------------------------

    @pytest.mark.unit
    @pytest.mark.subprocess
    @pytest.mark.parametrize('token', [
        None, 5, b'1-abc', ['1-abc'], '', '-x', '--help', '-t', ' ', ' 1-abc', '1-abc ', 'a b', 'a\nb', '1-abc\n',
        '1-abc\r', 'a\tb', 'tok\x00', 'tok\x1b[0m', 'tok\x7f', 'tök', '1-аbc', 'x' * 16385,
    ])
    def test_malformed_tokens_are_rejected_before_any_exec(self, mock_subprocess, token):
        mock_subprocess.setup(stdout=_lines(FULL_CONTENTS))
        with pytest.raises(ResumeTokenError, match='Not a valid ZFS resume token'):
            SendCommand.resume_info(token)
        mock_subprocess.assert_not_called()

    @pytest.mark.unit
    @pytest.mark.subprocess
    @pytest.mark.parametrize('token', ['1-abc-def-123', 'x' * 16384, '1-' + 'a' * 100, 'a-b', '!~'])
    def test_well_formed_tokens_reach_zfs(self, mock_subprocess, token):
        mock_subprocess.setup(stdout=_lines(FULL_CONTENTS))
        SendCommand.resume_info(token)
        assert list(mock_subprocess.call_args[0][0])[-1] == token

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_the_command_is_logged_once(self, mock_subprocess, caplog):
        mock_subprocess.setup(stdout=_lines(FULL_CONTENTS))
        with caplog.at_level(logging.DEBUG, logger='libzfseasy'):
            SendCommand.resume_info(TOKEN_FULL)
        messages = [r.getMessage() for r in caplog.records if r.name == 'libzfseasy']
        assert len(messages) == 1 and messages[0].endswith('send -nv -t ' + TOKEN_FULL)


class TestExecCollect:
    """`Command._exec_collect` returns (stdout_lines, error_lines, rc) and never raises on a non-zero exit."""

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_collects_lines_errors_and_rc(self, mock_subprocess):
        mock_subprocess.setup(stdout=['first\n', '\tindented = 1\n', '\n', 'last'], stderr=['boom happened', 'again'],
                              returncode=0)
        lines, errors, rc = Command._exec_collect(['zfs', 'x'])
        assert lines == ['first', '\tindented = 1', '', 'last']
        assert errors == ['Boom happened', 'again']
        assert rc == 0

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_nonzero_exit_after_output_does_not_raise(self, mock_subprocess):
        mock_subprocess.setup(stdout=['out\n'], stderr=['cannot do it'], returncode=255)
        assert Command._exec_collect(['zfs', 'x']) == (['out'], ['Cannot do it'], 255)

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_nonzero_exit_without_output(self, mock_subprocess):
        mock_subprocess.setup(stdout=[], returncode=3)
        assert Command._exec_collect(['zfs', 'x']) == ([], [], 3)

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_the_child_is_reaped(self, mock_subprocess):
        process = mock_subprocess.setup(stdout=['a\n'], returncode=1)
        Command._exec_collect(['zfs', 'x'])
        process.wait.assert_called_once()
        process.kill.assert_not_called()

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_an_interrupt_kills_the_child_and_joins_the_reader(self, mock_subprocess):
        release = threading.Event()
        process = MagicMock()
        process.stdout.readline.side_effect = ['partial\n', KeyboardInterrupt()]

        def blocked_stderr_read(*_limit):
            release.wait(timeout=10)       # bounded: a regression fails the join check instead of hanging
            return ''

        process.stderr.readline.side_effect = blocked_stderr_read
        process.poll.return_value = None
        process.kill.side_effect = release.set
        process.wait.return_value = -9
        mock_subprocess.return_value = process
        before = threading.active_count()

        with pytest.raises(KeyboardInterrupt):
            Command._exec_collect(['zfs', 'x'])

        process.kill.assert_called_once()
        process.wait.assert_called()
        assert release.is_set()
        _wait_for_thread_count(before)
        assert threading.active_count() == before     # the stderr reader was joined, not abandoned

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_an_interrupt_after_the_child_exited_does_not_kill(self, mock_subprocess):
        process = MagicMock()
        process.stdout.readline.side_effect = KeyboardInterrupt()
        process.stderr.readline.return_value = ''
        process.poll.return_value = 0
        mock_subprocess.return_value = process
        with pytest.raises(KeyboardInterrupt):
            Command._exec_collect(['zfs', 'x'])
        process.kill.assert_not_called()

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_exec_still_raises_on_nonzero(self, mock_subprocess):
        mock_subprocess.setup(stdout='', stderr=['cannot mount'], returncode=1)
        with pytest.raises(Exception, match='Cannot mount'):
            Command._exec(['zfs', 'mount'])

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_exec_out_still_yields_then_raises_on_nonzero(self, mock_subprocess):
        process = mock_subprocess.setup(stdout=['line\n'], stderr=['cannot list'], returncode=1)
        process.poll.side_effect = None
        process.poll.return_value = 1       # the generator's cleanup polls once more after the failure
        out = Command._exec_out(['zfs', 'list'])
        assert next(out) == 'line'
        with pytest.raises(Exception, match='Cannot list'):
            next(out)


class TestCommandLogRedaction:
    """URL credentials never reach the log, and nothing is formatted unless DEBUG is enabled."""

    SECRET = 'hunter2'

    @staticmethod
    def _messages(caplog):
        return [r.getMessage() for r in caplog.records if r.name == 'libzfseasy' and r.levelno == logging.DEBUG]

    @pytest.mark.unit
    @pytest.mark.parametrize('arg, expected', [
        ('https://user:hunter2@host/key', 'https://***@host/key'),
        ('https://user@host/key', 'https://***@host/key'),
        ('keylocation=https://user:hunter2@host:8443/key', 'keylocation=https://***@host:8443/key'),
        ('x=http://u:p@h1/a,y=https://v:w@h2/b', 'x=http://***@h1/a,y=https://***@h2/b'),
        ('svn+ssh://u:p@h/x', 'svn+ssh://***@h/x'),
        ('ssh://h/x', 'ssh://h/x'),
        ('https://host/path@thing', 'https://host/path@thing'),
        ('plain@name', 'plain@name'),
        ('pool/fs@snap', 'pool/fs@snap'),
        ('user@host:path', 'user@host:path'),
        ('file:///etc/key', 'file:///etc/key'),
        ('-o', '-o'),
        ('', ''),
    ])
    def test_redact_table(self, arg, expected):
        assert _redact(arg) == expected

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_plain_element(self, mock_subprocess, caplog):
        mock_subprocess.setup(stdout='')
        with caplog.at_level(logging.DEBUG, logger='libzfseasy'):
            Command._exec(['zfs', 'send', 'https://user:hunter2@host/key'])
        (message,) = self._messages(caplog)
        assert message == "Executing command: zfs send 'https://***@host/key'"
        assert self.SECRET not in caplog.text

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_keylocation_element_and_nested_dash_o(self, mock_subprocess, caplog):
        mock_subprocess.setup(stdout='')
        cmd = ['zfs', 'receive', '-o', 'keylocation=https://user:hunter2@host/key', '-o', 'compression=lz4',
               '-o', 'user:note=https://a:hunter2@b/c', 'pool/fs']
        with caplog.at_level(logging.DEBUG, logger='libzfseasy'):
            Command._exec(cmd)
        (message,) = self._messages(caplog)
        assert 'keylocation=https://***@host/key' in message
        assert 'user:note=https://***@b/c' in message
        assert 'compression=lz4' in message and message.endswith('pool/fs')
        assert self.SECRET not in caplog.text

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_the_real_argv_still_carries_the_credentials(self, mock_subprocess):
        mock_subprocess.setup(stdout='')
        cmd = ['zfs', 'create', '-o', 'keylocation=https://user:hunter2@host/key', 'pool/fs']
        Command._exec(cmd)
        assert list(mock_subprocess.call_args[0][0]) == cmd

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_unrelated_arguments_are_unchanged_and_still_quoted(self, mock_subprocess, caplog):
        mock_subprocess.setup(stdout='')
        cmd = ['zfs', 'rename', 'pool/my fs@snap', 'ssh://h/x', 'plain@name']
        with caplog.at_level(logging.DEBUG, logger='libzfseasy'):
            Command._exec(cmd)
        (message,) = self._messages(caplog)
        assert message == 'Executing command: ' + shlex.join(cmd)

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_log_command_formats_nothing_when_debug_is_off(self, mock_subprocess, caplog, monkeypatch):
        zfs_module = importlib.import_module('libzfseasy.zfs')
        join = MagicMock(side_effect=AssertionError('formatted with DEBUG off'))
        redact = MagicMock(side_effect=AssertionError('redacted with DEBUG off'))
        monkeypatch.setattr(zfs_module, 'shlex', MagicMock(join=join))
        monkeypatch.setattr(zfs_module, '_redact', redact)
        mock_subprocess.setup(stdout='')
        with caplog.at_level(logging.INFO, logger='libzfseasy'):
            Command._exec(['zfs', 'mount', '-a'])
            _log_command(['zfs', 'mount', '-a'])
        join.assert_not_called()
        redact.assert_not_called()

    @pytest.mark.unit
    def test_log_command_formats_when_debug_is_on(self, caplog, monkeypatch):
        zfs_module = importlib.import_module('libzfseasy.zfs')
        join = MagicMock(return_value='JOINED')
        monkeypatch.setattr(zfs_module, 'shlex', MagicMock(join=join))
        with caplog.at_level(logging.DEBUG, logger='libzfseasy'):
            _log_command(['zfs', 'mount', '-a'])
        join.assert_called_once_with(['zfs', 'mount', '-a'])
        assert self._messages(caplog) == ['Executing command: JOINED']

    @pytest.mark.unit
    @pytest.mark.subprocess
    @pytest.mark.parametrize('path', ['_exec', '_exec_out', '_exec_collect', '_exec_stream', '_exec_stream_in'])
    def test_every_exec_path_logs_redacted(self, mock_subprocess, caplog, path):
        cmd = ['zfs', 'receive', '-o', 'keylocation=https://user:hunter2@host/key', 'pool/fs']
        if path in ('_exec_stream', '_exec_stream_in'):
            process = MagicMock()
            process.stdout.peek.return_value = b'ZFS_DATA'
            process.poll.return_value = None
            process.stdin = io.BufferedWriter(io.BytesIO())
            mock_subprocess.return_value = process
        else:
            mock_subprocess.setup(stdout='')
        with caplog.at_level(logging.DEBUG, logger='libzfseasy'):
            owner = {'_exec_stream': SendCommand, '_exec_stream_in': ReceiveCommand}.get(path, Command)
            result = getattr(owner, path)(cmd)
            if path == '_exec_out':
                list(result)
        (message,) = self._messages(caplog)
        assert 'keylocation=https://***@host/key' in message
        assert self.SECRET not in caplog.text
        assert list(mock_subprocess.call_args[0][0]) == cmd


class TestListVolumeResumeToken:

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_volume_row_with_a_token(self, mock_subprocess):
        mock_subprocess.setup(stdout=['pool/v\tvolume\t' + TOKEN_FULL + '\n', 'pool/w\tvolume\t-\n',
                                      'pool/fs\tfilesystem\t-\n'])
        v, w, fs = ListCommand()(properties='receive_resume_token')
        assert v['receive_resume_token'].value == TOKEN_FULL
        assert w['receive_resume_token'] is None and 'receive_resume_token' not in w
        assert fs['receive_resume_token'] is None
        assert 'receive_resume_token' in v._prop_names and 'receive_resume_token' not in v._extra_props


class TestResumeInfoBlockOnStderr:
    """Which stream carries the `resume token contents:` block varies by OpenZFS version: stdout first, then stderr."""

    CANNOT = "cannot resume send: 'rtp/send/a@s1' used in the initial send no longer exists"

    @staticmethod
    def _nl(lines):
        return [line + '\n' for line in lines]

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_block_and_estimates_on_stderr_rc0(self, mock_subprocess):
        mock_subprocess.setup(stdout=[], stderr=_lines(FULL_CONTENTS, ESTIMATES), returncode=0)
        info = SendCommand.resume_info(TOKEN_FULL)
        assert info == ResumeToken(toname='rtp/send/a@s1', toguid=0x61034bf73452096e, object=2, offset=0x760000,
                                   bytes=0x785520)
        assert info.error is None

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_block_only_on_stderr_rc0(self, mock_subprocess):
        mock_subprocess.setup(stdout=[], stderr=_lines(INCR_CONTENTS), returncode=0)
        info = SendCommand.resume_info(TOKEN_INCR)
        assert info.fromguid == 0x4f39e607147f3dc2 and info.toname == 'rtp/send/b@s2'
        assert info.error is None

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_flags_on_stderr_are_parsed(self, mock_subprocess):
        mock_subprocess.setup(stdout=[], stderr=_lines(FULL_CONTENTS, ['\tembedok = 1', '\trawok = 0']), returncode=0)
        assert SendCommand.resume_info(TOKEN_FULL).flags == frozenset({'embedok'})

    @pytest.mark.unit
    @pytest.mark.subprocess
    @pytest.mark.parametrize('with_estimates', [False, True])
    @pytest.mark.parametrize('where', ['before', 'after'])
    def test_block_on_stderr_rc255_error_is_only_the_capitalised_zfs_line(self, mock_subprocess, with_estimates,
                                                                          where):
        around = ESTIMATES + ['send from rtp/send/b@s1 to rtp/send/b@s2 estimated size is 21.2M'] \
            if with_estimates else []
        stderr = FULL_CONTENTS + around + [self.CANNOT] if where == 'before' \
            else [self.CANNOT] + FULL_CONTENTS + around
        mock_subprocess.setup(stdout=[], stderr=_lines(stderr), returncode=255)
        info = SendCommand.resume_info(TOKEN_FULL_GONE)
        assert info.toguid == 0x61034bf73452096e
        assert info.error == 'Cannot resume send: \'rtp/send/a@s1\' used in the initial send no longer exists'

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_estimate_lines_never_leak_into_the_error(self, mock_subprocess):
        mock_subprocess.setup(stdout=[], stderr=_lines(FULL_CONTENTS, [
            'full send of rtp/send/a@s1 estimated size is 21.2M',
            'send from rtp/send/b@s1 to rtp/send/b@s2 estimated size is 1M',
            'total estimated size is 22M', 'cannot resume send: Invalid cross-device link']), returncode=255)
        error = SendCommand.resume_info(TOKEN_SOURCE_GONE).error
        assert error == 'Cannot resume send: Invalid cross-device link'

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_multi_line_error_keeps_every_remaining_line(self, mock_subprocess):
        mock_subprocess.setup(stdout=[], stderr=_lines(FULL_CONTENTS, ['cannot resume send: one', '  second line']),
                              returncode=255)
        assert SendCommand.resume_info(TOKEN_FULL).error == 'Cannot resume send: one\nsecond line'

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_block_on_stderr_rc255_with_nothing_else_reports_the_exit_code(self, mock_subprocess):
        mock_subprocess.setup(stdout=[], stderr=_lines(FULL_CONTENTS, ESTIMATES), returncode=255)
        assert SendCommand.resume_info(TOKEN_FULL).error == 'Command failed with exit code 255'

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_only_estimate_lines_on_stderr_rc0_raises(self, mock_subprocess):
        mock_subprocess.setup(stdout=[], stderr=_lines(ESTIMATES), returncode=0)
        with pytest.raises(ResumeTokenError, match='zfs printed no resume token contents'):
            SendCommand.resume_info(TOKEN_FULL)

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_estimate_lines_on_stdout_and_nothing_on_stderr_rc0_raises(self, mock_subprocess):
        mock_subprocess.setup(stdout=_lines(ESTIMATES), stderr=[], returncode=0)
        with pytest.raises(ResumeTokenError, match='zfs printed no resume token contents'):
            SendCommand.resume_info(TOKEN_FULL)

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_stdout_wins_when_both_streams_carry_a_block(self, mock_subprocess):
        other = ['resume token contents:', 'nvlist version: 0', '\ttoguid = 0x1', '\ttoname = other/x@y']
        mock_subprocess.setup(stdout=_lines(FULL_CONTENTS, ESTIMATES), stderr=_lines(other), returncode=0)
        info = SendCommand.resume_info(TOKEN_FULL)
        assert info.toname == 'rtp/send/a@s1' and info.toguid == 0x61034bf73452096e

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_stdout_wins_and_stderr_text_is_the_error_at_rc255(self, mock_subprocess):
        other = ['resume token contents:', 'nvlist version: 0', '\ttoguid = 0x1']
        mock_subprocess.setup(stdout=_lines(FULL_CONTENTS), stderr=_lines([self.CANNOT]), returncode=255)
        info = SendCommand.resume_info(TOKEN_FULL)
        assert info.toname == 'rtp/send/a@s1'
        assert info.error.startswith('Cannot resume send')
        mock_subprocess.setup(stdout=_lines(FULL_CONTENTS), stderr=_lines(other), returncode=0)
        assert SendCommand.resume_info(TOKEN_FULL).toguid == 0x61034bf73452096e

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_header_on_stdout_with_keys_on_stderr_uses_the_stdout_block_as_is(self, mock_subprocess):
        mock_subprocess.setup(stdout=_lines(['resume token contents:', 'nvlist version: 0']),
                              stderr=_lines(['\ttoguid = 0x61034bf73452096e', '\ttoname = rtp/send/a@s1']),
                              returncode=0)
        info = SendCommand.resume_info(TOKEN_FULL)
        assert info == ResumeToken()          # found on stdout; the stray stderr keys are not merged in

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_stderr_block_with_blank_and_noise_lines(self, mock_subprocess):
        mock_subprocess.setup(stdout=[], stderr=_lines(['', 'warning: something'], FULL_CONTENTS, ['']),
                              returncode=255)
        info = SendCommand.resume_info(TOKEN_FULL)
        assert info.toguid == 0x61034bf73452096e
        assert info.error == 'Warning: something'

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_corrupt_token_with_no_block_still_raises_with_zfs_text(self, mock_subprocess):
        mock_subprocess.setup(stdout=[], stderr=_lines(['cannot resume send: resume token is corrupt']),
                              returncode=255)
        with pytest.raises(ResumeTokenError, match='Cannot resume send: resume token is corrupt'):
            SendCommand.resume_info(TOKEN_FULL)

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_the_child_is_reaped_with_the_block_on_stderr(self, mock_subprocess):
        process = mock_subprocess.setup(stdout=[], stderr=_lines(FULL_CONTENTS, [self.CANNOT]), returncode=255)
        SendCommand.resume_info(TOKEN_FULL)
        process.wait.assert_called_once()

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_find_resume_block_span(self):
        from libzfseasy.zfs import _find_resume_block
        lines = ['x', 'resume token contents:', 'nvlist version: 0', '\ta = 1', '\tb = 2', 'full send of x', '\tz = 3']
        assert _find_resume_block(lines) == (1, 5)
        assert _find_resume_block(['resume token contents:']) == (0, 1)
        assert _find_resume_block(['resume token contents:', '\ta = 1']) == (0, 2)
        assert _find_resume_block(['nothing', 'here']) is None
        assert _find_resume_block([]) is None


class TestExecCollectRawStderr:

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_raw_stderr_keeps_tabs_blank_lines_and_case(self, mock_subprocess):
        mock_subprocess.setup(stdout=['out\n'], stderr=['\tindented = 1\n', '\n', '  spaced  \n', 'lower case\n'],
                              returncode=255)
        lines, errors, rc = Command._exec_collect(['zfs', 'x'], raw_stderr=True)
        assert lines == ['out']
        assert errors == ['\tindented = 1', '', '  spaced  ', 'lower case']
        assert rc == 255

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_default_mode_still_strips_drops_and_capitalises_the_first_line_only(self, mock_subprocess):
        mock_subprocess.setup(stdout=['out\n'], stderr=['\tindented = 1\n', '\n', '  spaced  \n', 'lower case\n'],
                              returncode=255)
        _, errors, _ = Command._exec_collect(['zfs', 'x'])
        assert errors == ['Indented = 1', 'spaced', 'lower case']

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_raw_mode_default_is_off(self, mock_subprocess):
        mock_subprocess.setup(stdout=[], stderr=['  x\n'], returncode=0)
        assert Command._exec_collect(['zfs', 'x'])[1] == ['X']

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_raw_mode_strips_only_the_line_ending(self, mock_subprocess):
        mock_subprocess.setup(stdout=[], stderr=['a\r\n', 'b\n', 'c'], returncode=0)
        assert Command._exec_collect(['zfs', 'x'], raw_stderr=True)[1] == ['a', 'b', 'c']

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_raw_mode_interrupt_kills_and_joins_the_reader(self, mock_subprocess):
        release = threading.Event()
        process = MagicMock()
        process.stdout.readline.side_effect = ['partial\n', KeyboardInterrupt()]

        def blocked_stderr_read():
            release.wait(timeout=10)
            return ''

        process.stderr.readline.side_effect = blocked_stderr_read
        process.poll.return_value = None
        process.kill.side_effect = release.set
        process.wait.return_value = -9
        mock_subprocess.return_value = process
        before = threading.active_count()
        with pytest.raises(KeyboardInterrupt):
            Command._exec_collect(['zfs', 'x'], raw_stderr=True)
        process.kill.assert_called_once()
        _wait_for_thread_count(before)
        assert threading.active_count() == before


_FAKE_RESUME_ZFS = r'''#!{python}
import os, sys
sys.stdout.write("resume token contents:\nnvlist version: 0\n\ttoguid = 0x1\n\ttoname = p/a@s\n")
sys.stdout.flush()
sys.stderr.buffer.write(b"locale=" + os.environ.get("LC_ALL", "").encode() + b"/" + os.environ.get("LANG", "").encode()
                        + b" bad \xff\xfe bytes\n")
sys.stderr.buffer.write(b"x" * 300000 + b"\n")
sys.stderr.flush()
sys.exit(255)
'''

_GRANDCHILD_HOLDS_STDERR = '''
import subprocess, sys
g = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"],
                     stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL)   # inherits stderr
sys.stdout.write("%d\\n" % g.pid)
sys.stdout.flush()
'''


class TestResumeInfoEnvironment:

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_resume_info_runs_zfs_in_the_c_locale(self, mock_subprocess, monkeypatch):
        monkeypatch.setenv('LC_ALL', 'de_DE.UTF-8')
        monkeypatch.setenv('LANG', 'de_DE.UTF-8')
        monkeypatch.setenv('LIBZFSEASY_TEST_KEEP', 'kept')
        before = dict(os.environ)
        mock_subprocess.setup(stdout=_lines(FULL_CONTENTS))
        SendCommand.resume_info(TOKEN_FULL)
        env = mock_subprocess.call_args.kwargs['env']
        assert env['LC_ALL'] == 'C' and env['LANG'] == 'C'
        assert env['LIBZFSEASY_TEST_KEEP'] == 'kept'
        assert {k: v for k, v in env.items() if k not in ('LC_ALL', 'LANG')} == \
            {k: v for k, v in before.items() if k not in ('LC_ALL', 'LANG')}
        assert env is not os.environ

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_the_parent_environment_is_untouched(self, mock_subprocess, monkeypatch):
        monkeypatch.setenv('LC_ALL', 'de_DE.UTF-8')
        monkeypatch.delenv('LANG', raising=False)
        before = dict(os.environ)
        mock_subprocess.setup(stdout=_lines(FULL_CONTENTS))
        SendCommand.resume_info(TOKEN_FULL)
        assert dict(os.environ) == before
        assert 'LANG' not in os.environ and os.environ['LC_ALL'] == 'de_DE.UTF-8'

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_c_locale_is_set_even_when_the_parent_has_none(self, mock_subprocess, monkeypatch):
        monkeypatch.delenv('LC_ALL', raising=False)
        monkeypatch.delenv('LANG', raising=False)
        mock_subprocess.setup(stdout=_lines(FULL_CONTENTS))
        SendCommand.resume_info(TOKEN_FULL)
        env = mock_subprocess.call_args.kwargs['env']
        assert env['LC_ALL'] == 'C' and env['LANG'] == 'C'

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_collect_decodes_utf8_with_replacement(self, mock_subprocess):
        mock_subprocess.setup(stdout=[], returncode=0)
        Command._exec_collect(['zfs', 'x'])
        kwargs = mock_subprocess.call_args.kwargs
        assert kwargs['encoding'] == 'utf-8' and kwargs['errors'] == 'replace'

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_collect_without_env_gets_the_c_locale(self, mock_subprocess):
        mock_subprocess.setup(stdout=[], returncode=0)
        Command._exec_collect(['zfs', 'x'])
        env = mock_subprocess.call_args.kwargs['env']
        assert env['LC_ALL'] == 'C' and env['LANG'] == 'C'

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_collect_passes_a_given_env_through(self, mock_subprocess):
        mock_subprocess.setup(stdout=[], returncode=0)
        env = {'A': '1'}
        Command._exec_collect(['zfs', 'x'], env=env)
        assert mock_subprocess.call_args.kwargs['env'] == {'A': '1'}

    @pytest.mark.unit
    @pytest.mark.subprocess
    @pytest.mark.parametrize('path', ['_exec', '_exec_out', '_exec_stream', '_exec_stream_in'])
    def test_other_exec_paths_get_the_c_locale(self, mock_subprocess, path):
        cmd = ['zfs', 'x']
        if path in ('_exec_stream', '_exec_stream_in'):
            process = MagicMock()
            process.stdout.peek.return_value = b'ZFS_DATA'
            process.poll.return_value = None
            process.stdin = io.BufferedWriter(io.BytesIO())
            mock_subprocess.return_value = process
        else:
            mock_subprocess.setup(stdout='')
        owner = {'_exec_stream': SendCommand, '_exec_stream_in': ReceiveCommand}.get(path, Command)
        result = getattr(owner, path)(cmd)
        if path == '_exec_out':
            list(result)
        env = mock_subprocess.call_args.kwargs['env']
        assert env['LC_ALL'] == 'C' and env['LANG'] == 'C'

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_ordinary_commands_get_the_c_locale(self, mock_subprocess):
        mock_subprocess.setup(stdout=[])
        ListCommand()()
        env = mock_subprocess.call_args.kwargs['env']
        assert env['LC_ALL'] == 'C' and env['LANG'] == 'C'


class TestExecCollectRobustness:

    @pytest.mark.unit
    @pytest.mark.subprocess
    @pytest.mark.parametrize('error', [OSError('broken pipe'), ValueError('I/O operation on closed file')])
    def test_raw_reader_survives_a_failing_readline(self, mock_subprocess, monkeypatch, error):
        crashed = []
        monkeypatch.setattr(threading, 'excepthook', lambda args: crashed.append(args))
        process = MagicMock()
        process.stdout.readline.side_effect = ['a\n', '']
        process.stderr.readline.side_effect = ['first\n', error]
        process.wait.return_value = 0
        mock_subprocess.return_value = process
        lines, errors, rc = Command._exec_collect(['zfs', 'x'], raw_stderr=True)
        assert (lines, errors, rc) == (['a'], ['first'], 0)
        assert crashed == []

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_the_failing_reader_still_lets_the_call_finish_with_no_stderr(self, mock_subprocess):
        process = MagicMock()
        process.stdout.readline.side_effect = ['']
        process.stderr.readline.side_effect = OSError('gone')
        process.wait.return_value = 7
        mock_subprocess.return_value = process
        assert Command._exec_collect(['zfs', 'x'], raw_stderr=True) == ([], [], 7)

    @pytest.mark.unit
    @pytest.mark.subprocess
    @pytest.mark.parametrize('raw', [False, True])
    def test_non_utf8_bytes_are_replaced_not_raised(self, real_child, raw):
        script = ('import sys\n'
                  'sys.stdout.buffer.write(b"out \\xff\\xfe ok\\n"); sys.stdout.flush()\n'
                  'sys.stderr.buffer.write(b"err \\xc3\\x28 bad\\n"); sys.stderr.flush()\n'
                  'sys.exit(3)\n')
        result, exc = real_child.run(lambda: Command._exec_collect(real_child.cmd(script), raw_stderr=raw))
        assert exc is None
        lines, errors, rc = result
        assert rc == 3
        assert lines == ['out \ufffd\ufffd ok']
        assert len(errors) == 1 and '\ufffd' in errors[0] and errors[0].lower().startswith('err ')

    @pytest.mark.unit
    @pytest.mark.subprocess
    @pytest.mark.parametrize('raw', [False, True])
    def test_hundreds_of_kb_of_stderr_do_not_block_the_child(self, real_child, raw):
        script = ('import sys\n'
                  'for i in range(400): sys.stderr.write("warn %d " % i + "e" * 1000 + "\\n")\n'
                  'sys.stderr.write("z" * 300000 + "\\n"); sys.stderr.flush()\n'
                  'sys.stdout.write("done\\n"); sys.exit(2)\n')
        result, exc = real_child.run(lambda: Command._exec_collect(real_child.cmd(script), raw_stderr=raw))
        assert exc is None
        lines, errors, rc = result
        assert lines == ['done'] and rc == 2
        assert len(errors) == 401                       # 200 head + the omitted-lines marker + 200 tail, either way
        if raw:
            assert len(errors[-1]) == 300000
            assert errors[200] == '... 1 lines omitted ...'
        else:
            # The non-raw reader reads at most _STDERR_MAX_LINE (64 KiB) at a time, so the 300000-char line arrives as
            # five pieces (4 x 65536 + 37856): 405 lines in all, the oldest 5 collapsed into the marker.
            assert [len(e) for e in errors[-5:]] == [65536] * 4 + [37856]
            assert errors[200] == '... 5 lines omitted ...'
            assert sum(len(e) for e in errors[-5:]) == 300000
        assert real_child.procs[0].returncode == 2

    @pytest.mark.unit
    @pytest.mark.subprocess
    @pytest.mark.parametrize('raw', [False, True])
    def test_a_grandchild_holding_stderr_cannot_hang_the_caller(self, real_child, monkeypatch, raw):
        monkeypatch.setattr('libzfseasy.zfs._TERMINATE_TIMEOUT', 0.5)
        started = time.monotonic()
        result, exc = real_child.run(
            lambda: Command._exec_collect(real_child.cmd(_GRANDCHILD_HOLDS_STDERR), raw_stderr=raw), timeout=20)
        elapsed = time.monotonic() - started
        grandchild = None
        try:
            assert exc is None
            lines, errors, rc = result
            grandchild = int(lines[0])
            assert rc == 0 and errors == []
            assert 0.4 <= elapsed < 10          # about the patched bound, not the grandchild's 60 s
        finally:
            if grandchild:
                try:
                    os.kill(grandchild, signal.SIGKILL)
                except OSError:
                    pass

    @pytest.mark.unit
    def test_join_stderr_reader_waits_only_once(self, monkeypatch):
        monkeypatch.setattr('libzfseasy.zfs._TERMINATE_TIMEOUT', 0.3)
        release = threading.Event()
        thread = threading.Thread(target=lambda: release.wait(10), daemon=True)
        thread.start()
        try:
            started = time.monotonic()
            Command._join_stderr_reader(thread)
            first = time.monotonic() - started
            started = time.monotonic()
            Command._join_stderr_reader(thread)
            second = time.monotonic() - started
            assert 0.25 <= first < 5
            assert second < 0.2
        finally:
            release.set()
            thread.join(5)

    @pytest.mark.unit
    @pytest.mark.subprocess
    @pytest.mark.parametrize('raw', [False, True])
    def test_the_reader_is_joined_once_on_a_normal_run(self, mock_subprocess, monkeypatch, raw):
        joined = []
        real = Command._join_stderr_reader
        monkeypatch.setattr(Command, '_join_stderr_reader', staticmethod(lambda t: (joined.append(t), real(t))))
        mock_subprocess.setup(stdout=['a\n'], stderr=['b'], returncode=0)
        Command._exec_collect(['zfs', 'x'], raw_stderr=raw)
        assert len(joined) == 1 and not joined[0].is_alive()

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_the_reader_is_joined_once_on_an_interrupt(self, mock_subprocess, monkeypatch):
        joined = []
        real = Command._join_stderr_reader
        monkeypatch.setattr(Command, '_join_stderr_reader', staticmethod(lambda t: (joined.append(t), real(t))))
        process = MagicMock()
        process.stdout.readline.side_effect = KeyboardInterrupt()
        process.stderr.readline.return_value = ''
        process.poll.return_value = None
        process.wait.return_value = -9
        mock_subprocess.return_value = process
        with pytest.raises(KeyboardInterrupt):
            Command._exec_collect(['zfs', 'x'])
        assert len(joined) == 1
        process.kill.assert_called_once()

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_resume_info_end_to_end_with_a_real_child(self, real_child, monkeypatch, tmp_path):
        script = tmp_path / 'fake_resume_zfs'
        script.write_text(_FAKE_RESUME_ZFS.format(python=sys.executable))
        script.chmod(0o755)
        monkeypatch.setenv('ZFS_CMD', str(script))
        monkeypatch.setenv('LC_ALL', 'de_DE.UTF-8')
        monkeypatch.setenv('LANG', 'de_DE.UTF-8')
        info, exc = real_child.run(lambda: SendCommand.resume_info(TOKEN_FULL))
        assert exc is None
        assert info.toguid == 1 and info.toname == 'p/a@s'
        # the child saw the C locale, its invalid bytes were replaced, and 300 KB of stderr did not block it
        assert info.error.startswith('Locale=C/C bad \ufffd\ufffd bytes')
        assert ('x' * 300000) in info.error.splitlines()
        assert os.environ['LC_ALL'] == 'de_DE.UTF-8'


# ---------------------------------------------------------------------------
# S1a: the C locale reaches every Popen
# ---------------------------------------------------------------------------


def _stream_process():
    process = MagicMock()
    process.stdout.peek.return_value = b'ZFS_DATA'
    process.poll.return_value = None
    process.stdin = io.BufferedWriter(io.BytesIO())
    return process


def _run_kind(kind, mock_subprocess):
    """Run one libzfseasy entry point that starts a child, against the mocked Popen."""
    if kind in ('exec_stream', 'exec_stream_in'):
        mock_subprocess.return_value = _stream_process()
    elif kind == 'resume_info':
        mock_subprocess.setup(stdout=_lines(FULL_CONTENTS))
    elif kind == 'list':
        mock_subprocess.setup(stdout=[])
    else:
        mock_subprocess.setup(stdout=[], returncode=0)
    cmd = ['zfs', 'x']
    if kind == 'exec':
        Command._exec(cmd)
    elif kind == 'exec_out':
        list(Command._exec_out(cmd))
    elif kind == 'exec_lines':
        list(Command._exec_lines(cmd))
    elif kind == 'collect':
        Command._exec_collect(cmd)
    elif kind == 'collect_raw':
        Command._exec_collect(cmd, raw_stderr=True)
    elif kind == 'exec_stream':
        SendCommand._exec_stream(cmd)
    elif kind == 'exec_stream_in':
        ReceiveCommand._exec_stream_in(cmd)
    elif kind == 'resume_info':
        SendCommand.resume_info(TOKEN_FULL)
    elif kind == 'list':
        ListCommand()()
    else:
        raise AssertionError(kind)


_ENV_KINDS = ['exec', 'exec_out', 'exec_lines', 'collect', 'collect_raw', 'exec_stream', 'exec_stream_in',
              'resume_info', 'list']


class TestChildEnvReachesEveryPopen:

    @pytest.fixture
    def hermetic_path(self, monkeypatch):
        from _hermetic import hermetic_env
        path = hermetic_env()['PATH']       # a private PATH with stub zfs/zpool only
        monkeypatch.setenv('PATH', path)
        return path

    @pytest.mark.unit
    @pytest.mark.subprocess
    @pytest.mark.parametrize('kind', _ENV_KINDS)
    @pytest.mark.parametrize('locale', [None, 'de_DE.UTF-8'], ids=['no-locale', 'caller-de_DE'])
    def test_every_popen_runs_in_the_c_locale_with_the_rest_of_the_environment(
            self, mock_subprocess, monkeypatch, hermetic_path, kind, locale):
        if locale is None:
            monkeypatch.delenv('LC_ALL', raising=False)
            monkeypatch.delenv('LANG', raising=False)
        else:
            monkeypatch.setenv('LC_ALL', locale)
            monkeypatch.setenv('LANG', locale)
        monkeypatch.setenv('LC_MESSAGES', 'de_DE.UTF-8')
        monkeypatch.setenv('LIBZFSEASY_TEST_KEEP', 'kept')
        before = dict(os.environ)

        _run_kind(kind, mock_subprocess)

        assert mock_subprocess.call_count == 1
        env = mock_subprocess.call_args.kwargs['env']
        assert env['LC_ALL'] == 'C' and env['LANG'] == 'C'
        assert env['PATH'] == hermetic_path
        assert env['LIBZFSEASY_TEST_KEEP'] == 'kept'
        assert env['LC_MESSAGES'] == 'de_DE.UTF-8'        # only LC_ALL and LANG are forced
        assert {k: v for k, v in env.items() if k not in ('LC_ALL', 'LANG')} == \
            {k: v for k, v in before.items() if k not in ('LC_ALL', 'LANG')}
        assert env is not os.environ
        assert dict(os.environ) == before                  # the parent is untouched

    @pytest.mark.unit
    @pytest.mark.subprocess
    @pytest.mark.parametrize('raw', [False, True])
    def test_collect_with_an_explicit_env_uses_exactly_that(self, mock_subprocess, monkeypatch, raw):
        monkeypatch.setenv('LC_ALL', 'de_DE.UTF-8')
        mock_subprocess.setup(stdout=[], returncode=0)
        Command._exec_collect(['zfs', 'x'], raw_stderr=raw, env={'ONLY': '1'})
        assert mock_subprocess.call_args.kwargs['env'] == {'ONLY': '1'}

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_collect_with_an_empty_explicit_env_is_not_replaced(self, mock_subprocess):
        mock_subprocess.setup(stdout=[], returncode=0)
        Command._exec_collect(['zfs', 'x'], env={})
        assert mock_subprocess.call_args.kwargs['env'] == {}

    @pytest.mark.unit
    def test_child_env_is_a_fresh_copy_each_call(self, monkeypatch):
        from libzfseasy.zfs import _child_env
        monkeypatch.setenv('LC_ALL', 'fr_FR.UTF-8')
        first = _child_env()
        first['MUTATED'] = '1'
        assert 'MUTATED' not in _child_env()
        assert os.environ['LC_ALL'] == 'fr_FR.UTF-8'

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_a_real_child_sees_the_c_locale_and_the_callers_environment(self, real_child, monkeypatch, hermetic_path):
        monkeypatch.setenv('LC_ALL', 'de_DE.UTF-8')
        monkeypatch.setenv('LANG', 'de_DE.UTF-8')
        monkeypatch.setenv('LIBZFSEASY_TEST_KEEP', 'kept')
        script = ('import os\n'
                  'for k in ("LC_ALL", "LANG", "PATH", "LIBZFSEASY_TEST_KEEP"):\n'
                  '    print(k + "=" + os.environ.get(k, "<unset>"))\n')
        for run in (lambda: list(Command._exec_out(real_child.cmd(script))),
                    lambda: list(Command._exec_lines(real_child.cmd(script))),
                    lambda: Command._exec_collect(real_child.cmd(script))[0]):
            lines, exc = real_child.run(run)
            assert exc is None
            assert lines == ['LC_ALL=C', 'LANG=C', 'PATH=' + hermetic_path, 'LIBZFSEASY_TEST_KEEP=kept']


# ---------------------------------------------------------------------------
# S1a: failure messages (`_failure_text`) -- unit, then with real children
# ---------------------------------------------------------------------------


class TestFailureText:

    @pytest.mark.unit
    @pytest.mark.parametrize('errors, rc, expected', [
        (['Boom'], 1, 'Boom'),
        (['First', 'Second'], 1, 'First\nSecond'),
        (['Boom'], -13, 'Boom'),                                    # stderr wins over the signal
        ([], 3, 'Command failed with exit code 3'),
        ([], 1, 'Command failed with exit code 1'),
        ([], -13, 'Command killed by signal SIGPIPE'),
        ([], -9, 'Command killed by signal SIGKILL'),
        ([], -15, 'Command killed by signal SIGTERM'),
        ([], -11, 'Command killed by signal SIGSEGV'),
        ([], -1, 'Command killed by signal SIGHUP'),
        ([], -255, 'Command killed by signal 255'),                 # not a signal number: the bare number
        ([], -100000, 'Command killed by signal 100000'),
        ([], None, 'Command failed with exit code None'),           # not an int: never a signal
        ([], '3', 'Command failed with exit code 3'),
        ([], -13.0, 'Command failed with exit code -13.0'),
        ([], True, 'Command failed with exit code True'),
    ])
    def test_failure_text(self, errors, rc, expected):
        from libzfseasy.zfs import _failure_text
        assert _failure_text(errors, rc) == expected

    @pytest.mark.unit
    def test_an_empty_bounded_error_list_counts_as_empty(self):
        from libzfseasy.zfs import _BoundedErrors, _failure_text
        assert _failure_text(_BoundedErrors(), -13) == 'Command killed by signal SIGPIPE'
        errors = _BoundedErrors()
        errors.append('Boom')
        assert _failure_text(errors, -13) == 'Boom'

    @pytest.mark.unit
    def test_a_signal_with_no_known_name_does_not_raise(self):
        from libzfseasy.zfs import _failure_text
        for rc in range(-130, 0):
            text = _failure_text([], rc)
            assert text.startswith('Command killed by signal ') and text.split()[-1]


_SIGPIPE_CHILD = ('import os, signal\n'
                  'signal.signal(signal.SIGPIPE, signal.SIG_DFL)\n'
                  'os.kill(os.getpid(), signal.SIGPIPE)\n')
_EXIT3_CHILD = 'import sys\nsys.exit(3)\n'
_STDERR_EXIT3_CHILD = 'import sys\nsys.stderr.write("disk on fire\\n")\nsys.stderr.flush()\nsys.exit(3)\n'
_STDERR_SIGPIPE_CHILD = ('import os, signal, sys\n'
                         'sys.stderr.write("disk on fire\\n")\nsys.stderr.flush()\n'
                         'signal.signal(signal.SIGPIPE, signal.SIG_DFL)\n'
                         'os.kill(os.getpid(), signal.SIGPIPE)\n')


def _drain_stream_in(cmd):
    handle = ReceiveCommand._exec_stream_in(cmd)
    handle.close()


_HELPER_RUNNERS = {
    '_exec': lambda cmd: Command._exec(cmd),
    '_exec_out': lambda cmd: list(Command._exec_out(cmd)),
    '_exec_lines': lambda cmd: list(Command._exec_lines(cmd)),
    '_exec_stream': lambda cmd: SendCommand._exec_stream(cmd),
    '_exec_stream_in': _drain_stream_in,
}


class TestFailureMessagesWithRealChildren:
    """Real `sys.executable` children (no pool, no zfs): the exception text for a signal death, a plain non-zero
    exit, and the precedence of stderr text."""

    @pytest.mark.unit
    @pytest.mark.subprocess
    @pytest.mark.parametrize('runner', sorted(_HELPER_RUNNERS))
    @pytest.mark.parametrize('script, rc, message', [
        (_SIGPIPE_CHILD, -signal.SIGPIPE, 'Command killed by signal SIGPIPE'),
        (_EXIT3_CHILD, 3, 'Command failed with exit code 3'),
        (_STDERR_EXIT3_CHILD, 3, 'Disk on fire'),
        (_STDERR_SIGPIPE_CHILD, -signal.SIGPIPE, 'Disk on fire'),
    ], ids=['sigpipe', 'exit3', 'stderr-exit3', 'stderr-sigpipe'])
    def test_helper_failure_message(self, real_child, runner, script, rc, message):
        _, exc = real_child.run(lambda: _HELPER_RUNNERS[runner](real_child.cmd(script)))
        assert type(exc) is Exception
        assert str(exc) == message
        assert real_child.procs[0].returncode == rc
        assert not any(t.is_alive() for t in real_child.stderr_threads)

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_exec_out_yields_its_rows_before_the_signal_message(self, real_child):
        script = 'import sys\nsys.stdout.write("row1\\nrow2\\n")\nsys.stdout.flush()\n' + _SIGPIPE_CHILD
        seen = []

        def consume():
            for row in Command._exec_out(real_child.cmd(script)):
                seen.append(row)

        _, exc = real_child.run(consume)
        assert seen == ['row1', 'row2']
        assert str(exc) == 'Command killed by signal SIGPIPE'

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_exec_collect_reports_a_signal_death_as_a_negative_rc(self, real_child):
        lines, errors, rc = real_child.run(lambda: Command._exec_collect(real_child.cmd(_SIGPIPE_CHILD)))[0]
        assert (lines, errors, rc) == ([], [], -signal.SIGPIPE)


_FAKE_CHILD_ZFS = '''#!{python}
import os, signal, sys
mode = os.environ.get("FAKE_ZFS_MODE", "")
if mode in ("stderr_exit3", "stderr_sigpipe"):
    sys.stderr.write("disk on fire\\n"); sys.stderr.flush()
if mode in ("sigpipe", "stderr_sigpipe"):
    signal.signal(signal.SIGPIPE, signal.SIG_DFL)
    os.kill(os.getpid(), signal.SIGPIPE)
if mode in ("exit3", "stderr_exit3"):
    sys.exit(3)
sys.exit(0)
'''


@pytest.fixture
def child_zfs(tmp_path, monkeypatch):
    """`ZFS_CMD` -> a script run by the test interpreter whose behaviour is chosen by `FAKE_ZFS_MODE`; PATH and
    `ZPOOL_CMD` are the hermetic ones, so nothing the code does can reach a real zfs."""
    from _hermetic import hermetic_env
    script = tmp_path / 'fake_zfs_child'
    script.write_text(_FAKE_CHILD_ZFS.format(python=sys.executable))
    script.chmod(0o755)
    env = hermetic_env()
    monkeypatch.setenv('PATH', env['PATH'])
    monkeypatch.setenv('ZPOOL_CMD', env['ZPOOL_CMD'])
    monkeypatch.setenv('ZFS_CMD', str(script))

    def mode(name):
        monkeypatch.setenv('FAKE_ZFS_MODE', name)
    return mode


_FS = Filesystem('p/a')


def _public_calls():
    def receive():
        ReceiveCommand.filesystem(_FS).close()

    def resume():
        SendCommand.resume(TOKEN_FULL)

    return {
        'list': lambda: ListCommand()(),
        'set': lambda: SetCommand()(_FS, {'org:x': '1'}),
        'destroy': lambda: DestroyCommand.dataset(_FS, destroy=True),
        'send.partial': lambda: SendCommand.partial(_FS),
        'send.resume': resume,
        'receive': receive,
    }


class TestFailureMessagesThroughTheCommands:
    """The same messages through the public commands, with `ZFS_CMD` pointed at a real child."""

    @pytest.mark.unit
    @pytest.mark.subprocess
    @pytest.mark.parametrize('name', sorted(_public_calls()))
    @pytest.mark.parametrize('mode, message', [
        ('sigpipe', 'Command killed by signal SIGPIPE'),
        ('exit3', 'Command failed with exit code 3'),
        ('stderr_exit3', 'Disk on fire'),
        ('stderr_sigpipe', 'Disk on fire'),
    ])
    def test_message(self, real_child, child_zfs, name, mode, message):
        child_zfs(mode)
        _, exc = real_child.run(_public_calls()[name])
        assert type(exc) is Exception and str(exc) == message
        assert all(p.returncode is not None for p in real_child.procs)

    @pytest.mark.unit
    @pytest.mark.subprocess
    @pytest.mark.parametrize('name', sorted(_public_calls()))
    def test_a_clean_exit_raises_nothing(self, real_child, child_zfs, name):
        child_zfs('ok')
        _, exc = real_child.run(_public_calls()[name])
        assert exc is None


# ---------------------------------------------------------------------------
# L2: destroying an empty list of snapshots
# ---------------------------------------------------------------------------


class TestDestroyEmptySnapshotList:

    @pytest.mark.unit
    @pytest.mark.subprocess
    @pytest.mark.parametrize('empty', [lambda: [], lambda: iter(()), lambda: (s for s in ()), lambda: (),
                                       lambda: set()], ids=['list', 'iter', 'generator', 'tuple', 'set'])
    @pytest.mark.parametrize('kwargs', [{}, {'destroy': True}, {'destroy': True, 'lazy': True},
                                        {'destroy': True, 'recursive': True, 'force': True}])
    def test_snapshots_raises_before_exec(self, mock_subprocess, empty, kwargs):
        with pytest.raises(ValueError, match='at least one'):
            DestroyCommand.snapshots(empty(), **kwargs)
        assert mock_subprocess.call_count == 0

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_destroy_of_an_empty_list_raises_before_exec(self, mock_subprocess):
        with pytest.raises(ValueError, match='at least one'):
            DestroyCommand()([], destroy=True)
        with pytest.raises(ValueError, match='at least one'):
            DestroyCommand()([])
        import libzfseasy
        with pytest.raises(ValueError, match='at least one'):
            libzfseasy.destroy([], destroy=True)
        assert mock_subprocess.call_count == 0

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_the_lazy_form_raises_at_the_call_not_at_the_first_next(self, mock_subprocess):
        with pytest.raises(ValueError):
            DestroyCommand.snapshots([], destroy=True, lazy=True)       # no iteration needed
        assert mock_subprocess.call_count == 0

    @pytest.mark.unit
    @pytest.mark.subprocess
    @pytest.mark.parametrize('lazy', [False, True], ids=['eager', 'lazy'])
    def test_an_empty_generator_is_refused_like_an_empty_list(self, mock_subprocess, lazy):
        with pytest.raises(ValueError, match='at least one'):
            DestroyCommand.snapshots((s for s in []), destroy=True, lazy=lazy)
        with pytest.raises(ValueError, match='at least one'):
            DestroyCommand.snapshots(iter(()), destroy=True, lazy=lazy)
        assert mock_subprocess.call_count == 0

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_a_one_element_iterator_still_runs(self, mock_subprocess, sample_snapshot):
        mock_subprocess.setup(stdout=['destroy\ttestpool/filesystem@snap1\n'])
        assert DestroyCommand.snapshots(iter([sample_snapshot]), destroy=True) == ['testpool/filesystem@snap1']
        assert mock_subprocess.call_args[0][0][-1] == 'testpool/filesystem@snap1'

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_the_wrong_element_type_message_is_unchanged(self, mock_subprocess):
        with pytest.raises(ValueError, match='Expected Snapshot or SnapshotRange'):
            DestroyCommand.snapshots(['not a snapshot'], destroy=True)
        assert mock_subprocess.call_count == 0

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_lazy_destroy_yields_its_lines_then_raises_on_a_failing_exit(self, mock_subprocess, sample_filesystem):
        a, b = Snapshot(sample_filesystem, 'a'), Snapshot(sample_filesystem, 'b')
        process = mock_subprocess.setup(
            stdout=['destroy\ttestpool/filesystem@a\n', 'destroy\ttestpool/filesystem@b\n'],
            stderr='cannot destroy: dataset is busy', returncode=1)
        process.poll.side_effect = None
        process.poll.return_value = 1
        gen = DestroyCommand.snapshots([a, b], destroy=True, lazy=True)
        assert mock_subprocess.call_count == 0             # nothing runs until the first next()
        seen = []
        with pytest.raises(Exception, match='Cannot destroy: dataset is busy') as excinfo:
            for line in gen:
                seen.append(line)
        assert seen == ['testpool/filesystem@a', 'testpool/filesystem@b']
        assert type(excinfo.value) is Exception            # the bare subprocess error, not a ValueError

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_non_lazy_destroy_raises_without_returning_the_lines(self, mock_subprocess, sample_snapshot):
        process = mock_subprocess.setup(stdout=['destroy\ttestpool/filesystem@snap1\n'], stderr='cannot destroy',
                                        returncode=1)
        process.poll.side_effect = None
        process.poll.return_value = 1
        with pytest.raises(Exception, match='Cannot destroy'):
            DestroyCommand.snapshots(sample_snapshot, destroy=True)


# ---------------------------------------------------------------------------
# One-shot iterables (generators, iterators) are accepted wherever a list is
# ---------------------------------------------------------------------------

_ONE_SHOT = pytest.mark.parametrize('make', [lambda items: (i for i in items), lambda items: iter(list(items))],
                                    ids=['generator', 'iterator'])


def _argv(mock_subprocess, n=0):
    return list(mock_subprocess.call_args_list[n][0][0])[1:]


_GET_ROWS = [
    'testpool/filesystem\tname\ttestpool/filesystem\t-\t-\n',
    'testpool/filesystem\ttype\tfilesystem\t-\t-\n',
    'testpool/filesystem\tcompression\tlz4\t-\tlocal\n',
    'testpool/filesystem\tquota\t10G\t-\tinherited from testpool\n',
    'testpool/filesystem@s\tname\ttestpool/filesystem@s\t-\t-\n',
    'testpool/filesystem@s\ttype\tsnapshot\t-\t-\n',
    'testpool/filesystem@s\tcompression\tlz4\t-\tlocal\n',
    'testpool/filesystem@s\tquota\t-\t-\t-\n',
]
# What zfs prints for `-o name,type,compression` (D5 follow-up: every requested property, in order, and nothing else).
_GET_ROWS_COMPRESSION = [r for r in _GET_ROWS if '\tquota\t' not in r]


class TestOneShotIterables:

    @pytest.mark.unit
    @pytest.mark.subprocess
    @_ONE_SHOT
    def test_receive_set_and_reset_overlap_is_still_caught(self, mock_subprocess, make):
        with pytest.raises(ValueError, match='cannot be both set'):
            ReceiveCommand.filesystem(Filesystem('p/a'), props={'org:x': '1'}, reset=make(['org:x']))
        assert mock_subprocess.call_count == 0

    @pytest.mark.unit
    @pytest.mark.subprocess
    @_ONE_SHOT
    def test_receive_one_shot_reset_gives_x_flags(self, mock_subprocess, make):
        mock_subprocess.setup(stdout='')
        ReceiveCommand.filesystem(Filesystem('p/a'), props={'org:k': 'v'}, reset=make(['mountpoint', 'org:x'])).close()
        assert _argv(mock_subprocess) == ['receive', '-o', 'org:k=v', '-x', 'mountpoint', '-x', 'org:x', 'p/a']

    @pytest.mark.unit
    @pytest.mark.subprocess
    @_ONE_SHOT
    def test_list_one_shot_properties(self, mock_subprocess, make):
        mock_subprocess.setup(stdout=[])
        ListCommand()(properties=make(['compression', 'mountpoint']))
        argv = _argv(mock_subprocess)
        assert argv[argv.index('-o') + 1] == 'name,type,compression,mountpoint'

    @pytest.mark.unit
    @pytest.mark.subprocess
    @_ONE_SHOT
    def test_list_one_shot_types_and_the_wanted_filter(self, mock_subprocess, make):
        mock_subprocess.setup(stdout=['testpool/fs@s\tsnapshot\n', 'testpool/fs\tfilesystem\n',
                                      'testpool/v\tvolume\n'])
        result = ListCommand()(types=make(['filesystem', 'volume']))
        argv = _argv(mock_subprocess)
        assert argv[argv.index('-t') + 1] == 'filesystem,volume'
        assert [(type(o), o.name) for o in result] == [(Filesystem, 'testpool/fs'), (Volume, 'testpool/v')]

    @pytest.mark.unit
    @pytest.mark.subprocess
    @_ONE_SHOT
    def test_list_one_shot_types_lazy(self, mock_subprocess, make):
        mock_subprocess.setup(stdout=['testpool/fs@s\tsnapshot\n', 'testpool/fs\tfilesystem\n'])
        result = list(ListCommand()(types=make(['filesystem', 'volume']), lazy=True))
        argv = _argv(mock_subprocess)
        assert argv[argv.index('-t') + 1] == 'filesystem,volume'
        assert [o.name for o in result] == ['testpool/fs']

    @pytest.mark.unit
    @pytest.mark.subprocess
    @_ONE_SHOT
    def test_get_one_shot_types_and_the_wanted_filter(self, mock_subprocess, make):
        mock_subprocess.setup(stdout=list(_GET_ROWS_COMPRESSION))
        result = GetCommand()(ds=Filesystem('testpool/filesystem'), properties=['compression'],
                              types=make(['filesystem', 'volume']))
        argv = _argv(mock_subprocess)
        assert argv[argv.index('-t') + 1] == 'filesystem,volume'
        assert [o.name for o in result] == ['testpool/filesystem']

    @pytest.mark.unit
    @pytest.mark.subprocess
    @_ONE_SHOT
    def test_get_one_shot_properties_and_sources(self, mock_subprocess, make):
        mock_subprocess.setup(stdout=list(_GET_ROWS))
        result = GetCommand()(ds=Filesystem('testpool/filesystem'), properties=make(['compression', 'quota']),
                              sources=make(['local']))
        argv = _argv(mock_subprocess)
        assert 'name,type,compression,quota' in argv
        (fs,) = [o for o in result if o.name == 'testpool/filesystem']
        assert fs['compression'].value == 'lz4'
        assert 'quota' not in fs                  # its source is `inherited`, not `local`

    @pytest.mark.unit
    @pytest.mark.subprocess
    @_ONE_SHOT
    def test_get_one_shot_types_lazy(self, mock_subprocess, make):
        mock_subprocess.setup(stdout=list(_GET_ROWS_COMPRESSION))
        result = list(GetCommand()(ds=Filesystem('testpool/filesystem'), properties=['compression'],
                                   types=make(['filesystem']), lazy=True))
        assert '-t' in _argv(mock_subprocess) and 'filesystem' in _argv(mock_subprocess)
        assert [o.name for o in result] == ['testpool/filesystem']

    @pytest.mark.unit
    @pytest.mark.subprocess
    @_ONE_SHOT
    def test_get_value_one_shot_sources(self, mock_subprocess, make):
        mock_subprocess.setup(stdout=['lz4\n'])
        assert GetCommand.value(Filesystem('testpool/filesystem'), 'compression', sources=make(['local', 'received'])) \
            == 'lz4'
        argv = _argv(mock_subprocess)
        assert argv[argv.index('-s') + 1] == 'local,received'

    @pytest.mark.unit
    @pytest.mark.subprocess
    @_ONE_SHOT
    def test_inherit_one_shot_props_runs_one_inherit_per_name(self, mock_subprocess, make):
        fs = Filesystem('testpool/filesystem', {'compression': 'lz4', 'quota': '10G'})
        mock_subprocess.setup_multi(([''],), ([''],))
        InheritCommand()(fs, make(['compression', 'quota']))
        assert mock_subprocess.call_count == 2
        assert _argv(mock_subprocess, 0) == ['inherit', 'compression', 'testpool/filesystem']
        assert _argv(mock_subprocess, 1) == ['inherit', 'quota', 'testpool/filesystem']
        assert 'compression' not in fs and 'quota' not in fs     # both were cleared after their own call

    @pytest.mark.unit
    @pytest.mark.subprocess
    @_ONE_SHOT
    def test_inherit_one_shot_invalid_name_runs_nothing(self, mock_subprocess, make):
        fs = Filesystem('testpool/filesystem', {'compression': 'lz4'})
        with pytest.raises(ValueError):
            InheritCommand()(fs, make(['compression', 'not-a-property']))
        assert mock_subprocess.call_count == 0

    @pytest.mark.unit
    @pytest.mark.subprocess
    @_ONE_SHOT
    def test_allow_one_shot_users_groups_permissions(self, mock_subprocess, make):
        mock_subprocess.setup()
        AllowCommand()(Filesystem('testpool/filesystem'), make(['create', 'destroy']),
                       users=make(['alice', 'bob']), groups=make(['ops', 'dev']))
        argv = _argv(mock_subprocess)
        assert argv[argv.index('-u') + 1] == 'alice,bob'
        assert argv[argv.index('-g') + 1] == 'ops,dev'
        assert argv[-2:] == ['create,destroy', 'testpool/filesystem']

    @pytest.mark.unit
    @pytest.mark.subprocess
    @_ONE_SHOT
    def test_get_one_shot_datasets(self, mock_subprocess, make):
        mock_subprocess.setup(stdout=list(_GET_ROWS))
        GetCommand()(ds=make([Filesystem('testpool/filesystem'), Snapshot(Filesystem('testpool/filesystem'), 's')]),
                     properties=['compression'])
        assert _argv(mock_subprocess)[-2:] == ['testpool/filesystem', 'testpool/filesystem@s']

    @pytest.mark.unit
    @pytest.mark.subprocess
    @_ONE_SHOT
    def test_list_one_shot_roots(self, mock_subprocess, make):
        mock_subprocess.setup(stdout=['testpool/a\tfilesystem\n'])
        ListCommand()(roots=make([Filesystem('testpool/a'), Filesystem('testpool/b')]))
        assert _argv(mock_subprocess)[-2:] == ['testpool/a', 'testpool/b']


class TestResumeInfoSignalDeath:
    """A child killed by a signal prints nothing: the message is `_failure_text`'s, never `exit code -13`."""

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_no_contents_and_no_stderr_raises_the_signal_message(self, mock_subprocess):
        mock_subprocess.setup(stdout=[], returncode=-signal.SIGPIPE)
        with pytest.raises(Exception) as excinfo:
            SendCommand.resume_info(TOKEN_FULL)
        assert type(excinfo.value) is Exception
        assert str(excinfo.value) == 'Command killed by signal SIGPIPE'

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_contents_and_no_stderr_sets_error_to_the_signal_message(self, mock_subprocess):
        mock_subprocess.setup(stdout=_lines(FULL_CONTENTS), returncode=-signal.SIGPIPE)
        info = SendCommand.resume_info(TOKEN_FULL)
        assert info.toname == 'rtp/send/a@s1'
        assert info.error == 'Command killed by signal SIGPIPE'

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_a_plain_non_zero_exit_without_stderr_keeps_its_wording(self, mock_subprocess):
        mock_subprocess.setup(stdout=[], returncode=3)
        with pytest.raises(Exception, match='^Command failed with exit code 3$'):
            SendCommand.resume_info(TOKEN_FULL)
        mock_subprocess.setup(stdout=_lines(FULL_CONTENTS), returncode=3)
        assert SendCommand.resume_info(TOKEN_FULL).error == 'Command failed with exit code 3'

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_stderr_text_still_wins_over_the_signal(self, mock_subprocess):
        mock_subprocess.setup(stdout=_lines(FULL_CONTENTS), stderr=['cannot resume send: target is busy'],
                              returncode=-signal.SIGPIPE)
        assert SendCommand.resume_info(TOKEN_FULL).error == 'Cannot resume send: target is busy'


# ---------------------------------------------------------------------------------------------------------------------
# Multi-line stderr: only the FIRST non-empty line is capitalised; continuation lines are the real zfs text
# ---------------------------------------------------------------------------------------------------------------------

CLONES_STDERR = ["cannot destroy 'p/fs@s': snapshot has dependent clones",
                 "use '-R' to destroy the following datasets:",
                 'p/clone3']
CLONES_MESSAGE = ("Cannot destroy 'p/fs@s': snapshot has dependent clones\n"
                  "use '-R' to destroy the following datasets:\n"
                  "p/clone3")

# (leading stderr lines, case id): the first NON-empty line is the one that is capitalised
LEADING_NOISE = [pytest.param([], id='no-blank'),
                 pytest.param(['\n'], id='blank-first'),
                 pytest.param(['   \n', '\n'], id='whitespace-first')]


class TestStderrFirstLineOnlyTextReaders:
    """The text stderr readers (`_start_stderr_reader_lines`, `_start_stderr_reader_bin`) capitalise only the first
    collected line. Mutation check: per-line capitalisation restored in `_start_stderr_reader_lines` fails the `_exec`,
    `_exec_out` and `_exec_collect` tests below (`_exec` and `_exec_out` are layers over `_exec_lines` since D3; the
    `_exec_lines` ones live in test_framing.py)."""

    @staticmethod
    def _stderr(noise):
        return noise + [line + '\n' for line in CLONES_STDERR]

    @pytest.mark.unit
    @pytest.mark.subprocess
    @pytest.mark.parametrize('noise', LEADING_NOISE)
    def test_exec_raises_with_only_the_first_line_capitalised(self, mock_subprocess, noise):
        mock_subprocess.setup(stdout=[], stderr=self._stderr(noise), returncode=1)
        with pytest.raises(Exception) as exc:
            Command._exec(['zfs', 'destroy', 'p/fs@s'])
        assert str(exc.value) == CLONES_MESSAGE

    @pytest.mark.unit
    @pytest.mark.subprocess
    @pytest.mark.parametrize('noise', LEADING_NOISE)
    def test_exec_out_raises_with_only_the_first_line_capitalised(self, mock_subprocess, noise):
        process = mock_subprocess.setup(stdout=['row\n'], stderr=self._stderr(noise), returncode=1)
        process.poll.side_effect = None
        process.poll.return_value = 1
        out = Command._exec_out(['zfs', 'destroy', 'p/fs@s'])
        assert next(out) == 'row'
        with pytest.raises(Exception) as exc:
            next(out)
        assert str(exc.value) == CLONES_MESSAGE

    @pytest.mark.unit
    @pytest.mark.subprocess
    @pytest.mark.parametrize('noise', LEADING_NOISE)
    def test_exec_collect_returns_only_the_first_line_capitalised(self, mock_subprocess, noise):
        mock_subprocess.setup(stdout=[], stderr=self._stderr(noise), returncode=1)
        _, errors, rc = Command._exec_collect(['zfs', 'destroy', 'p/fs@s'])
        assert rc == 1
        assert '\n'.join(errors) == CLONES_MESSAGE
        assert errors[2] == 'p/clone3'


class TestStderrFirstLineOnlyResumeInfo:
    """`SendCommand.resume_info` builds its error text from the raw stderr lines with the same rule."""

    @pytest.mark.unit
    @pytest.mark.subprocess
    @pytest.mark.parametrize('noise', LEADING_NOISE)
    def test_error_text_capitalises_only_the_first_line(self, mock_subprocess, noise):
        mock_subprocess.setup(stdout=[], returncode=255,
                              stderr=_lines(FULL_CONTENTS) + noise + _lines(CLONES_STDERR))
        info = SendCommand.resume_info(TOKEN_FULL)
        assert info.toguid == 0x61034bf73452096e
        assert info.error == CLONES_MESSAGE

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_a_resume_failure_without_a_block_raises_the_same_text(self, mock_subprocess):
        stderr = ["cannot resume send: 'p/a@s1' used in the initial send no longer exists",
                  "use 'p/Clone3'"]
        mock_subprocess.setup(stdout=[], stderr=_lines(stderr), returncode=255)
        with pytest.raises(Exception) as exc:
            SendCommand.resume_info(TOKEN_FULL)
        assert str(exc.value) == ("Cannot resume send: 'p/a@s1' used in the initial send no longer exists\n"
                                  "use 'p/Clone3'")
