"""Additional unit tests for libzfseasy.zfs — AllowCommand through MountCommand."""

import pytest
import io
from unittest.mock import MagicMock
from libzfseasy.zfs import (
    AllowCommand, UnAllowCommand, CloneCommand, GetCommand, SetCommand,
    InheritCommand, SendCommand, ReceiveCommand,
    LoadKeyCommand, UnLoadKeyCommand, ChangeKeyCommand,
    MountCommand, UnMountCommand,
)
from libzfseasy.types import Dataset, Filesystem, Volume, Snapshot, Bookmark, Property


class TestAllowCommand:

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_allow_basic(self, mock_subprocess, sample_filesystem):
        mock_subprocess.setup()
        AllowCommand()(sample_filesystem, 'create,destroy')
        call_args = mock_subprocess.call_args[0][0]
        assert 'allow' in call_args

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_allow_dataset_only(self, mock_subprocess, sample_filesystem):
        mock_subprocess.setup()
        AllowCommand()(sample_filesystem, 'create', dataset=True, children=False)
        call_args = mock_subprocess.call_args[0][0]
        assert '-l' in call_args

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_allow_children_only(self, mock_subprocess, sample_filesystem):
        mock_subprocess.setup()
        AllowCommand()(sample_filesystem, 'create', dataset=False, children=True)
        call_args = mock_subprocess.call_args[0][0]
        assert '-d' in call_args

    @pytest.mark.unit
    def test_allow_neither_dataset_nor_children_raises(self, sample_filesystem):
        with pytest.raises(ValueError, match='At least one of dataset or children'):
            AllowCommand()(sample_filesystem, 'create', dataset=False, children=False)

    @pytest.mark.unit
    def test_allow_everyone_with_users_raises(self, sample_filesystem):
        with pytest.raises(ValueError, match='Everyone and users or groups'):
            AllowCommand()(sample_filesystem, 'create', everyone=True, users='bob')

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_allow_with_users(self, mock_subprocess, sample_filesystem):
        mock_subprocess.setup()
        AllowCommand()(sample_filesystem, 'create', users='alice')
        call_args = mock_subprocess.call_args[0][0]
        assert '-u' in call_args

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_allow_with_groups(self, mock_subprocess, sample_filesystem):
        mock_subprocess.setup()
        AllowCommand()(sample_filesystem, 'create', groups='admins')
        call_args = mock_subprocess.call_args[0][0]
        assert '-g' in call_args

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_allow_create_method(self, mock_subprocess, sample_filesystem):
        mock_subprocess.setup()
        AllowCommand().create(sample_filesystem, 'create,destroy')
        call_args = mock_subprocess.call_args[0][0]
        assert '-c' in call_args

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_allow_set_method(self, mock_subprocess, sample_filesystem):
        mock_subprocess.setup()
        AllowCommand().set(sample_filesystem, '@myset', 'create,destroy')
        call_args = mock_subprocess.call_args[0][0]
        assert '-s' in call_args
        assert '@myset' in call_args

    @pytest.mark.unit
    def test_allow_set_invalid_name_raises(self, sample_filesystem):
        with pytest.raises(ValueError, match='is not a valid ZFS permission set name'):
            AllowCommand().set(sample_filesystem, 'badname', 'create')

    @pytest.mark.unit
    def test_allow_base_recursive_raises_typeerror(self):
        with pytest.raises(TypeError):
            AllowCommand._base(True, None, recursive=True)


class TestUnAllowCommand:

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_unallow_basic(self, mock_subprocess, sample_filesystem):
        mock_subprocess.setup()
        UnAllowCommand()(sample_filesystem, 'create,destroy')
        call_args = mock_subprocess.call_args[0][0]
        assert 'unallow' in call_args

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_unallow_create(self, mock_subprocess, sample_filesystem):
        mock_subprocess.setup()
        UnAllowCommand().create(sample_filesystem, 'create,destroy')
        call_args = mock_subprocess.call_args[0][0]
        assert 'unallow' in call_args
        assert '-c' in call_args

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_unallow_set(self, mock_subprocess, sample_filesystem):
        mock_subprocess.setup()
        UnAllowCommand().set(sample_filesystem, '@myset', 'create,destroy')
        call_args = mock_subprocess.call_args[0][0]
        assert 'unallow' in call_args
        assert '-s' in call_args

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_unallow_recursive(self, mock_subprocess, sample_filesystem):
        mock_subprocess.setup()
        UnAllowCommand()(sample_filesystem, 'create', recursive=True)
        call_args = mock_subprocess.call_args[0][0]
        assert '-r' in call_args


class TestCloneCommandExtended:

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_clone_with_parents(self, mock_subprocess, sample_snapshot):
        mock_subprocess.setup_multi(
            ([''],),
            (['testpool/clone\tfilesystem'],),
        )
        CloneCommand()(sample_snapshot, 'testpool/clone', parents=True)
        call_args = mock_subprocess.call_args_list[0][0][0]
        assert '-p' in call_args

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_clone_with_properties(self, mock_subprocess, sample_snapshot):
        mock_subprocess.setup_multi(
            ([''],),
            (['testpool/clone\tfilesystem'],),
        )
        CloneCommand()(sample_snapshot, 'testpool/clone', properties={'compression': 'lz4'})
        call_args = mock_subprocess.call_args_list[0][0][0]
        assert '-o' in call_args

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_clone_returns_list_when_multiple(self, mock_subprocess, sample_snapshot):
        mock_subprocess.setup_multi(
            ([''],),
            (['testpool/clone\tfilesystem', 'testpool/clone/child\tfilesystem'],),
        )
        result = CloneCommand()(sample_snapshot, 'testpool/clone')
        assert isinstance(result, list)
        assert len(result) == 2


class TestGetCommandExtended:

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_get_lazy(self, mock_subprocess, sample_filesystem):
        mock_subprocess.setup(stdout=[
            'testpool/filesystem\ttype\tfilesystem\t-\t-\n',
        ])
        result = GetCommand()(ds=sample_filesystem, lazy=True)
        assert hasattr(result, '__next__')

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_get_recursive(self, mock_subprocess, sample_filesystem):
        mock_subprocess.setup(stdout=[
            'testpool/filesystem\ttype\tfilesystem\t-\t-\n',
        ])
        GetCommand()(ds=sample_filesystem, recursive=True)
        call_args = mock_subprocess.call_args[0][0]
        assert '-r' in call_args

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_get_depth(self, mock_subprocess, sample_filesystem):
        mock_subprocess.setup(stdout=[
            'testpool/filesystem\ttype\tfilesystem\t-\t-\n',
        ])
        GetCommand()(ds=sample_filesystem, depth=2)
        call_args = mock_subprocess.call_args[0][0]
        assert '-d' in call_args
        assert '2' in call_args

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_get_multi_dataset_output(self, mock_subprocess, sample_filesystem):
        mock_subprocess.setup(stdout=[
            'testpool/fs1\ttype\tfilesystem\t-\t-\n',
            'testpool/fs1\tcompression\tlz4\t-\tlocal\n',
            'testpool/fs2\ttype\tfilesystem\t-\t-\n',
            'testpool/fs2\tcompression\tgzip\t-\tlocal\n',
        ])
        result = GetCommand()(ds=sample_filesystem, properties='compression')
        assert len(result) == 2


class TestSetCommandExtended:

    @pytest.mark.unit
    def test_set_get_props_with_flag(self):
        result = SetCommand._get_props({'compression': 'lz4'}, flag=True)
        assert result == ['-o', 'compression=lz4']


class TestInheritCommandExtended:

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_inherit_received(self, mock_subprocess, sample_filesystem):
        mock_subprocess.setup()
        InheritCommand()(sample_filesystem, 'compression', received=True)
        call_args = mock_subprocess.call_args[0][0]
        assert '-S' in call_args


class TestSendCommandExtended:

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_send_dispatch_dataset(self, mock_subprocess, sample_filesystem):
        process_mock = MagicMock()
        process_mock.stdout.peek.return_value = b'ZFS_DATA'
        process_mock.poll.return_value = None
        mock_subprocess.return_value = process_mock
        result = SendCommand()(sample_filesystem)
        call_args = mock_subprocess.call_args[0][0]
        assert 'send' in call_args
        assert result is not None

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_send_dispatch_snapshot(self, mock_subprocess, sample_snapshot):
        process_mock = MagicMock()
        process_mock.stdout.peek.return_value = b'ZFS_DATA'
        process_mock.poll.return_value = None
        mock_subprocess.return_value = process_mock
        result = SendCommand()(sample_snapshot)
        call_args = mock_subprocess.call_args[0][0]
        assert 'send' in call_args
        assert result is not None

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_send_dataset_with_since(self, mock_subprocess, sample_filesystem, sample_snapshot):
        process_mock = MagicMock()
        process_mock.stdout.peek.return_value = b'ZFS_DATA'
        process_mock.poll.return_value = None
        mock_subprocess.return_value = process_mock
        SendCommand().dataset(sample_filesystem, since=sample_snapshot)
        call_args = mock_subprocess.call_args[0][0]
        assert '-i' in call_args

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_send_redact(self, mock_subprocess, sample_snapshot, sample_filesystem):
        bm = Bookmark(sample_filesystem, 'bm1')
        process_mock = MagicMock()
        process_mock.stdout.peek.return_value = b'ZFS_DATA'
        process_mock.poll.return_value = None
        mock_subprocess.return_value = process_mock
        SendCommand().redact(sample_snapshot, bm)
        call_args = mock_subprocess.call_args[0][0]
        assert '--redact' in call_args

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_send_resume(self, mock_subprocess):
        process_mock = MagicMock()
        process_mock.stdout.peek.return_value = b'ZFS_DATA'
        process_mock.poll.return_value = None
        mock_subprocess.return_value = process_mock
        SendCommand().resume('resume_token_xyz')
        call_args = mock_subprocess.call_args[0][0]
        assert '-t' in call_args
        assert 'resume_token_xyz' in call_args

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_send_partial(self, mock_subprocess, sample_filesystem):
        process_mock = MagicMock()
        process_mock.stdout.peek.return_value = b'ZFS_DATA'
        process_mock.poll.return_value = None
        mock_subprocess.return_value = process_mock
        SendCommand().partial(sample_filesystem)
        call_args = mock_subprocess.call_args[0][0]
        assert '-S' in call_args

    @pytest.mark.unit
    def test_send_options_holds(self):
        assert '-h' in SendCommand._get_options(holds=True)

    @pytest.mark.unit
    def test_send_options_properties(self):
        assert '-p' in SendCommand._get_options(properties=True)

    @pytest.mark.unit
    def test_send_options_backup(self):
        assert '-b' in SendCommand._get_options(backup=True)

    @pytest.mark.unit
    def test_send_options_raw(self):
        assert '-w' in SendCommand._get_options(raw=True)

    @pytest.mark.unit
    def test_send_options_compressed(self):
        assert '-c' in SendCommand._get_options(compressed=True)

    @pytest.mark.unit
    def test_send_options_embed(self):
        assert '-e' in SendCommand._get_options(embed=True)

    @pytest.mark.unit
    def test_send_options_large_blocks(self):
        assert '-L' in SendCommand._get_options(large_blocks=True)

    @pytest.mark.unit
    def test_send_options_skip_missing_with_replicate(self):
        opts = SendCommand._get_options(skip_missing=True, replicate=True)
        assert '-s' in opts
        assert '-R' in opts

    @pytest.mark.unit
    def test_send_options_skip_missing_without_replicate_raises(self):
        with pytest.raises(ValueError, match='skip_missing can only be specified together with replicate'):
            SendCommand._get_options(skip_missing=True, replicate=False)

    @pytest.mark.unit
    def test_send_options_since_not_snapshot_raises(self, sample_filesystem):
        bm = Bookmark(sample_filesystem, 'bm1')
        with pytest.raises(ValueError, match='Expected Snapshot'):
            SendCommand._get_options(since=bm)

    @pytest.mark.unit
    def test_send_options_intermediate(self, sample_snapshot):
        opts = SendCommand._get_options(since=sample_snapshot, intermediate=True)
        assert '-I' in opts

    @pytest.mark.unit
    def test_send_options_since_incremental(self, sample_snapshot):
        opts = SendCommand._get_options(since=sample_snapshot)
        assert '-i' in opts


class TestReceiveCommandExtended:

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_receive_dispatch_dataset(self, mock_subprocess):
        process_mock = MagicMock()
        process_mock.stdin = io.BufferedWriter(io.BytesIO())
        process_mock.poll.return_value = None
        mock_subprocess.return_value = process_mock
        ds = Dataset('pool/ds')
        ReceiveCommand()(ds)
        call_args = mock_subprocess.call_args[0][0]
        assert 'receive' in call_args

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_receive_dataset_method(self, mock_subprocess):
        process_mock = MagicMock()
        process_mock.stdin = io.BufferedWriter(io.BytesIO())
        process_mock.poll.return_value = None
        mock_subprocess.return_value = process_mock
        ds = Dataset('pool/ds')
        result = ReceiveCommand().dataset(ds)
        assert result is not None
        call_args = mock_subprocess.call_args[0][0]
        assert 'receive' in call_args
        assert 'pool/ds' in call_args

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_receive_filesystem_with_origin(self, mock_subprocess, sample_filesystem, sample_snapshot):
        process_mock = MagicMock()
        process_mock.stdin = io.BufferedWriter(io.BytesIO())
        process_mock.poll.return_value = None
        mock_subprocess.return_value = process_mock
        ReceiveCommand().filesystem(sample_filesystem, origin=sample_snapshot)
        call_args = mock_subprocess.call_args[0][0]
        assert 'origin=' in ' '.join(call_args)

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_receive_filesystem_with_props(self, mock_subprocess, sample_filesystem):
        process_mock = MagicMock()
        process_mock.stdin = io.BufferedWriter(io.BytesIO())
        process_mock.poll.return_value = None
        mock_subprocess.return_value = process_mock
        ReceiveCommand().filesystem(sample_filesystem, props={'compression': 'lz4'})
        call_args = mock_subprocess.call_args[0][0]
        assert '-o' in call_args

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_receive_filesystem_with_reset(self, mock_subprocess, sample_filesystem):
        process_mock = MagicMock()
        process_mock.stdin = io.BufferedWriter(io.BytesIO())
        process_mock.poll.return_value = None
        mock_subprocess.return_value = process_mock
        ReceiveCommand().filesystem(sample_filesystem, reset=['compression'])
        call_args = mock_subprocess.call_args[0][0]
        assert '-x' in call_args

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_receive_filesystem_no_holds(self, mock_subprocess, sample_filesystem):
        process_mock = MagicMock()
        process_mock.stdin = io.BufferedWriter(io.BytesIO())
        process_mock.poll.return_value = None
        mock_subprocess.return_value = process_mock
        ReceiveCommand().filesystem(sample_filesystem, holds=False)
        call_args = mock_subprocess.call_args[0][0]
        assert '-h' in call_args

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_receive_filesystem_unmount(self, mock_subprocess, sample_filesystem):
        process_mock = MagicMock()
        process_mock.stdin = io.BufferedWriter(io.BytesIO())
        process_mock.poll.return_value = None
        mock_subprocess.return_value = process_mock
        ReceiveCommand().filesystem(sample_filesystem, unmount=True)
        call_args = mock_subprocess.call_args[0][0]
        assert '-M' in call_args

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_receive_filesystem_save(self, mock_subprocess, sample_filesystem):
        process_mock = MagicMock()
        process_mock.stdin = io.BufferedWriter(io.BytesIO())
        process_mock.poll.return_value = None
        mock_subprocess.return_value = process_mock
        ReceiveCommand().filesystem(sample_filesystem, save=True)
        call_args = mock_subprocess.call_args[0][0]
        assert '-s' in call_args

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_receive_filesystem_no_mount(self, mock_subprocess, sample_filesystem):
        process_mock = MagicMock()
        process_mock.stdin = io.BufferedWriter(io.BytesIO())
        process_mock.poll.return_value = None
        mock_subprocess.return_value = process_mock
        ReceiveCommand().filesystem(sample_filesystem, mount=False)
        call_args = mock_subprocess.call_args[0][0]
        assert '-u' in call_args

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_receive_filesystem_ignore_first(self, mock_subprocess, sample_filesystem):
        process_mock = MagicMock()
        process_mock.stdin = io.BufferedWriter(io.BytesIO())
        process_mock.poll.return_value = None
        mock_subprocess.return_value = process_mock
        ReceiveCommand().filesystem(sample_filesystem, ignore_first=True)
        call_args = mock_subprocess.call_args[0][0]
        assert '-d' in call_args

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_receive_filesystem_ignore_all(self, mock_subprocess, sample_filesystem):
        process_mock = MagicMock()
        process_mock.stdin = io.BufferedWriter(io.BytesIO())
        process_mock.poll.return_value = None
        mock_subprocess.return_value = process_mock
        ReceiveCommand().filesystem(sample_filesystem, ignore_all=True)
        call_args = mock_subprocess.call_args[0][0]
        assert '-e' in call_args

    @pytest.mark.unit
    def test_receive_get_props_with_flag(self):
        result = ReceiveCommand._get_props({'compression': 'lz4'}, flag=True)
        assert result == ['-o', 'compression=lz4']

    @pytest.mark.unit
    def test_receive_get_props_without_flag(self):
        result = ReceiveCommand._get_props({'compression': 'lz4'}, flag=False)
        assert result == ['compression=lz4']


class TestLoadKeyCommand:

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_load_key_basic(self, mock_subprocess, sample_filesystem):
        mock_subprocess.setup()
        LoadKeyCommand()(sample_filesystem)
        call_args = mock_subprocess.call_args[0][0]
        assert 'load-key' in call_args
        assert str(sample_filesystem) in call_args

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_load_key_with_location(self, mock_subprocess, sample_filesystem):
        mock_subprocess.setup()
        LoadKeyCommand()(sample_filesystem, location='/etc/key')
        call_args = mock_subprocess.call_args[0][0]
        assert '-L' in call_args
        assert '/etc/key' in call_args

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_load_key_recursive(self, mock_subprocess, sample_filesystem):
        mock_subprocess.setup()
        LoadKeyCommand()(sample_filesystem, recursive=True)
        call_args = mock_subprocess.call_args[0][0]
        assert '-r' in call_args

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_load_key_all(self, mock_subprocess):
        mock_subprocess.setup()
        LoadKeyCommand()(None)
        call_args = mock_subprocess.call_args[0][0]
        assert '-a' in call_args

    @pytest.mark.unit
    def test_load_key_all_with_location_raises(self):
        with pytest.raises(ValueError, match='Key location cannot be explicitly specified when loading all keys'):
            LoadKeyCommand()(None, location='/etc/key')

    @pytest.mark.unit
    def test_load_key_all_with_recursive_raises(self):
        with pytest.raises(ValueError, match='Recursive cannot be specified when loading all keys'):
            LoadKeyCommand()(None, recursive=True)

    @pytest.mark.unit
    def test_load_key_location_and_recursive_raises(self):
        with pytest.raises(ValueError, match='Key location cannot be explicitly specified when loading keys recursively'):
            LoadKeyCommand._get_options(location='/etc/key', recursive=True)


class TestUnLoadKeyCommand:

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_unload_key_basic(self, mock_subprocess, sample_filesystem):
        mock_subprocess.setup()
        UnLoadKeyCommand()(sample_filesystem)
        call_args = mock_subprocess.call_args[0][0]
        assert 'unload-key' in call_args
        assert str(sample_filesystem) in call_args

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_unload_key_recursive(self, mock_subprocess, sample_filesystem):
        mock_subprocess.setup()
        UnLoadKeyCommand()(sample_filesystem, recursive=True)
        call_args = mock_subprocess.call_args[0][0]
        assert '-r' in call_args

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_unload_key_all(self, mock_subprocess):
        mock_subprocess.setup()
        UnLoadKeyCommand()(None)
        call_args = mock_subprocess.call_args[0][0]
        assert '-a' in call_args

    @pytest.mark.unit
    def test_unload_key_all_with_recursive_raises(self):
        with pytest.raises(ValueError, match='Recursive cannot be specified when unloading all keys'):
            UnLoadKeyCommand()(None, recursive=True)


class TestChangeKeyCommand:

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_change_key_basic(self, mock_subprocess, sample_filesystem):
        mock_subprocess.setup()
        ChangeKeyCommand()(sample_filesystem)
        call_args = mock_subprocess.call_args[0][0]
        assert 'change-key' in call_args
        assert str(sample_filesystem) in call_args

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_change_key_inherit(self, mock_subprocess, sample_filesystem):
        mock_subprocess.setup()
        ChangeKeyCommand()(sample_filesystem, inherit=True)
        call_args = mock_subprocess.call_args[0][0]
        assert '-i' in call_args

    @pytest.mark.unit
    def test_change_key_inherit_with_location_raises(self, sample_filesystem):
        with pytest.raises(ValueError, match='Key location cannot be specified when inheriting keys'):
            ChangeKeyCommand()(sample_filesystem, inherit=True, location='/etc/key')

    @pytest.mark.unit
    def test_change_key_inherit_with_format_raises(self, sample_filesystem):
        with pytest.raises(ValueError, match='Key format cannot be specified when inheriting keys'):
            ChangeKeyCommand()(sample_filesystem, inherit=True, fmt='passphrase')

    @pytest.mark.unit
    def test_change_key_inherit_with_iterations_raises(self, sample_filesystem):
        with pytest.raises(ValueError, match='Number of pbkdf2 iterations cannot be specified when inheriting keys'):
            ChangeKeyCommand()(sample_filesystem, inherit=True, iterations=200000)

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_change_key_load(self, mock_subprocess, sample_filesystem):
        mock_subprocess.setup()
        ChangeKeyCommand()(sample_filesystem, load=True)
        call_args = mock_subprocess.call_args[0][0]
        assert '-l' in call_args

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_change_key_location(self, mock_subprocess, sample_filesystem):
        mock_subprocess.setup()
        ChangeKeyCommand()(sample_filesystem, location='/etc/key')
        call_args = mock_subprocess.call_args[0][0]
        assert any('keylocation' in a for a in call_args)

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_change_key_format(self, mock_subprocess, sample_filesystem):
        mock_subprocess.setup()
        ChangeKeyCommand()(sample_filesystem, fmt='passphrase')
        call_args = mock_subprocess.call_args[0][0]
        assert any('keyformat' in a for a in call_args)

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_change_key_iterations(self, mock_subprocess, sample_filesystem):
        mock_subprocess.setup()
        ChangeKeyCommand()(sample_filesystem, iterations=200000)
        call_args = mock_subprocess.call_args[0][0]
        assert any('pbkdf2iters' in a for a in call_args)


class TestMountCommandExtended:

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_mount_with_load_keys(self, mock_subprocess, sample_filesystem):
        mock_subprocess.setup()
        MountCommand()(sample_filesystem, None, None, load_keys=True)
        call_args = mock_subprocess.call_args[0][0]
        assert '-l' in call_args

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_mount_with_force(self, mock_subprocess, sample_filesystem):
        mock_subprocess.setup()
        MountCommand()(sample_filesystem, None, None, force=True)
        call_args = mock_subprocess.call_args[0][0]
        assert '-f' in call_args

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_mount_with_string_flags(self, mock_subprocess, sample_filesystem):
        mock_subprocess.setup()
        MountCommand()(sample_filesystem, 'ro,nodev', None)
        call_args = mock_subprocess.call_args[0][0]
        assert '-o' in call_args
        assert 'ro' in call_args

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_mount_with_list_flags(self, mock_subprocess, sample_filesystem):
        mock_subprocess.setup()
        MountCommand()(sample_filesystem, ['ro', 'nodev'], None)
        call_args = mock_subprocess.call_args[0][0]
        assert '-o' in call_args


class TestUnMountCommandExtended:

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_unmount_with_unload_keys(self, mock_subprocess, sample_filesystem):
        mock_subprocess.setup()
        UnMountCommand()(sample_filesystem, unload_keys=True)
        call_args = mock_subprocess.call_args[0][0]
        assert '-u' in call_args
