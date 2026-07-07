"""Additional unit tests for libzfseasy.zfs — AllowCommand through MountCommand."""

import pytest
import io
import os
import subprocess
from unittest.mock import MagicMock
from libzfseasy.zfs import (
    AllowCommand, UnAllowCommand, CloneCommand, GetCommand, SetCommand,
    InheritCommand, SendCommand, ReceiveCommand, StreamHandle,
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

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_allow_everyone_argv_no_none_token(self, mock_subprocess, sample_filesystem):
        """Regression test: AllowCommand(ds, perms, everyone=True) must build the
        argv with a bare '-e' flag (no trailing users argument), not ['-e', None]
        (users is None in this branch since everyone/users are mutually
        exclusive). A None token in argv would break subprocess.Popen or get
        stringified as the literal text 'None'."""
        mock_subprocess.setup()
        AllowCommand()(sample_filesystem, 'create', everyone=True)

        call_args = mock_subprocess.call_args[0][0]

        assert '-e' in call_args
        assert None not in call_args
        # Also guard against the None being coerced to the string 'None'
        assert 'None' not in call_args


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

    @pytest.mark.unit
    def test_get_options_types_none_omits_dash_t(self):
        """types=None must not emit a bare '-t' flag with no argument (this
        used to unconditionally do `cmd += ['-t', kwargs.get('types')]`,
        which appended a literal `None`)."""
        argv = GetCommand._get_options(types=None, properties=['all'])
        assert '-t' not in argv

    @pytest.mark.unit
    def test_get_options_types_empty_string_omits_dash_t(self):
        argv = GetCommand._get_options(types='', properties=['all'])
        assert '-t' not in argv

    @pytest.mark.unit
    def test_get_options_types_all_emits_dash_t(self):
        argv = GetCommand._get_options(types='all', properties=['all'])
        assert '-t' in argv
        assert argv[argv.index('-t') + 1] == 'all'


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


class TestStreamHandle:
    """Unit tests for StreamHandle, the send/receive context-manager wrapper."""

    @pytest.mark.unit
    def test_context_manager_round_trips_data(self):
        """__enter__ returns the wrapped stream and __exit__ closes it (and
        the process) cleanly when the process exits with rc == 0."""
        stream = io.BytesIO(b'hello-zfs-stream')
        process = MagicMock()
        process.wait.return_value = 0
        process.stderr = None

        with StreamHandle(process, stream) as s:
            data = s.read()

        assert data == b'hello-zfs-stream'
        assert stream.closed
        process.wait.assert_called_once()

    @pytest.mark.unit
    def test_close_raises_on_nonzero_exit_after_send_drain(self):
        """Send-side failure: the caller has finished reading (drained) the
        stream, but the underlying `zfs send` process exited non-zero.
        close()/__exit__ must surface that as the bare subprocess Exception
        instead of silently succeeding."""
        stream = io.BytesIO(b'')
        stream.read()  # simulate the caller having fully drained the stream
        process = MagicMock()
        process.wait.return_value = 1
        process.stderr = io.BytesIO(b'send failed: dataset does not exist\n')

        handle = StreamHandle(process, stream)
        with pytest.raises(Exception) as exc_info:
            handle.close()

        assert 'dataset does not exist' in str(exc_info.value)

    @pytest.mark.unit
    def test_close_raises_on_nonzero_exit_after_receive_stdin_close(self):
        """Receive-side failure: the caller has finished writing and the
        stdin pipe would normally just be closed, but `zfs receive` exited
        non-zero (e.g. an incompatible stream). __exit__ must raise rather
        than swallow the failure."""
        stream = io.BufferedWriter(io.BytesIO())
        stream.write(b'ZFS_DATA')
        process = MagicMock()
        process.wait.return_value = 1
        process.stderr = io.BytesIO(b'cannot receive: destination has been modified\n')

        with pytest.raises(Exception) as exc_info:
            with StreamHandle(process, stream) as s:
                pass

        assert 'destination has been modified' in str(exc_info.value)
        assert stream.closed

    @pytest.mark.unit
    def test_close_is_idempotent(self):
        """A second close() call must be a no-op: no second process.wait(),
        and it must not re-raise once the first close() already raised."""
        stream = io.BytesIO(b'')
        process = MagicMock()
        process.wait.return_value = 1
        process.stderr = io.BytesIO(b'boom\n')

        handle = StreamHandle(process, stream)
        with pytest.raises(Exception):
            handle.close()

        # Should not raise again, and should not call process.wait() again.
        handle.close()
        process.wait.assert_called_once()

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_send_empty_stream_rc_zero_returns_handle_no_raise(self, mock_subprocess):
        """An empty send stream -- the process exits 0 having produced no
        stdout at all -- must still hand back a usable StreamHandle, and
        closing it must not raise."""
        process = MagicMock()
        process.stdout.peek.return_value = b''
        process.poll.return_value = 0  # exits immediately, never produced output
        process.wait.return_value = 0
        mock_subprocess.return_value = process

        handle = SendCommand._exec_stream(['zfs', 'send', 'testpool/fs@snap'])

        assert isinstance(handle, StreamHandle)
        handle.close()
        process.wait.assert_called_once()

    @pytest.mark.unit
    def test_close_closes_stderr_pipe_and_is_idempotent_on_real_fds(self):
        """close() must close the real stderr pipe object it drained (not
        just leak the fd), and must be safe to call twice on real,
        OS-backed file objects -- no double-close exception. The wrapped
        stream here IS process.stdout (the send-side case), so the
        stdout-vs-self._stream branch must not attempt to close it a
        second time via _close_pipe."""
        stdout_r, stdout_w = os.pipe()
        stderr_r, stderr_w = os.pipe()
        os.close(stdout_w)
        os.close(stderr_w)
        stream = os.fdopen(stdout_r, 'rb')
        stderr_pipe = os.fdopen(stderr_r, 'rb')

        process = MagicMock()
        process.wait.return_value = 0
        process.stderr = stderr_pipe
        process.stdout = stream  # send side: wrapped stream IS process.stdout

        handle = StreamHandle(process, stream)
        handle.close()

        assert stream.closed
        assert stderr_pipe.closed

        # Idempotent: a second close() must not raise (e.g. an OSError
        # from double-closing an already-closed fd-backed file object),
        # and must not re-invoke process.wait().
        handle.close()
        process.wait.assert_called_once()


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
    @pytest.mark.subprocess
    def test_receive_uses_devnull_for_stdout(self, mock_subprocess, sample_filesystem):
        """Regression guard: nothing ever drains process.stdout on the
        receive side, so it must be opened as DEVNULL, not PIPE. If this
        regressed back to PIPE, a chatty child filling the pipe could block
        on write() forever, and StreamHandle.close()'s process.wait() would
        then deadlock waiting for a child stuck writing to an undrained
        pipe."""
        process_mock = MagicMock()
        process_mock.stdin = io.BufferedWriter(io.BytesIO())
        process_mock.poll.return_value = None
        mock_subprocess.return_value = process_mock

        ReceiveCommand().filesystem(sample_filesystem)

        _, call_kwargs = mock_subprocess.call_args
        assert call_kwargs['stdout'] == subprocess.DEVNULL
        assert call_kwargs['stdout'] != subprocess.PIPE

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

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_change_key_location_argv_tokens_split(self, mock_subprocess, sample_filesystem):
        """Regression test: `-o keylocation=...` must be built as two separate
        argv tokens ('-o', 'keylocation=...'), not a single '-o keylocation=...'
        token (which the real `zfs change-key` CLI would reject/mis-parse)."""
        mock_subprocess.setup()
        ChangeKeyCommand()(sample_filesystem, location='/etc/key')
        call_args = mock_subprocess.call_args[0][0]

        idx = call_args.index('-o')
        assert call_args[idx + 1] == 'keylocation=/etc/key'
        assert '-o keylocation=/etc/key' not in call_args

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_change_key_format_argv_tokens_split(self, mock_subprocess, sample_filesystem):
        """Regression test: `-o keyformat=...` must be two separate argv tokens."""
        mock_subprocess.setup()
        ChangeKeyCommand()(sample_filesystem, fmt='passphrase')
        call_args = mock_subprocess.call_args[0][0]

        idx = call_args.index('-o')
        assert call_args[idx + 1] == 'keyformat=passphrase'
        assert '-o keyformat=passphrase' not in call_args

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_change_key_iterations_argv_tokens_split(self, mock_subprocess, sample_filesystem):
        """Regression test: `-o pbkdf2iters=...` must be two separate argv tokens."""
        mock_subprocess.setup()
        ChangeKeyCommand()(sample_filesystem, iterations=200000)
        call_args = mock_subprocess.call_args[0][0]

        idx = call_args.index('-o')
        assert call_args[idx + 1] == 'pbkdf2iters=200000'
        assert '-o pbkdf2iters=200000' not in call_args

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_change_key_multiple_options_all_split_into_separate_tokens(
            self, mock_subprocess, sample_filesystem):
        """Regression test: when location, fmt and iterations are all given,
        every `-o key=value` pair must appear as its own pair of argv tokens,
        with exactly three '-o' flags (one per option)."""
        mock_subprocess.setup()
        ChangeKeyCommand()(sample_filesystem, location='/etc/key',
                           fmt='passphrase', iterations=200000)
        call_args = mock_subprocess.call_args[0][0]

        assert call_args.count('-o') == 3
        assert 'keylocation=/etc/key' in call_args
        assert 'keyformat=passphrase' in call_args
        assert 'pbkdf2iters=200000' in call_args
        # None of these should ever be glued to the '-o' flag as one token.
        for token in call_args:
            assert token == '-o' or not token.startswith('-o ')


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
