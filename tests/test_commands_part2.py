"""Additional unit tests for libzfseasy.zfs — AllowCommand through MountCommand."""

import pytest
import contextlib
import gc
import io
import os
import subprocess
import sys
import warnings
from unittest.mock import MagicMock
from libzfseasy.zfs import (
    AllowCommand, UnAllowCommand, CloneCommand, GetCommand, SetCommand,
    InheritCommand, SendCommand, ReceiveCommand, StreamHandle,
    LoadKeyCommand, UnLoadKeyCommand, ChangeKeyCommand,
    MountCommand, UnMountCommand, DestroyCommand, Command,
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
    def test_clone_with_mountpoint_argv(self, mock_subprocess, sample_snapshot):
        mock_subprocess.setup_multi(([''],), (['testpool/clone\tfilesystem\t/mnt/clone'],))
        result = CloneCommand()(sample_snapshot, 'testpool/clone', properties={'mountpoint': '/mnt/clone'})
        assert list(mock_subprocess.call_args_list[0][0][0])[1:] == [
            'clone', '-o', 'mountpoint=/mnt/clone', 'testpool/filesystem@snap1', 'testpool/clone']
        assert isinstance(result, Dataset) and result.name == 'testpool/clone'
        listing = list(mock_subprocess.call_args_list[1][0][0])
        assert listing[listing.index('-o') + 1] == 'name,type,mountpoint'

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_clone_with_user_property(self, mock_subprocess, sample_snapshot):
        mock_subprocess.setup_multi(([''],), (['testpool/clone\tfilesystem\tvalue'],))
        CloneCommand()(sample_snapshot, 'testpool/clone', properties={'user:note': 'hello world'})
        argv = list(mock_subprocess.call_args_list[0][0][0])
        assert argv[argv.index('-o') + 1] == 'user:note=hello world'

    @pytest.mark.unit
    @pytest.mark.subprocess
    @pytest.mark.parametrize('prop, text', [
        (Property('lz4', 'local'), 'compression=lz4'),
        (Property('lz4'), 'compression=lz4'),
        (Property(5), 'compression=5'),
    ])
    def test_clone_property_object_emits_its_raw_value(self, mock_subprocess, sample_snapshot, prop, text):
        mock_subprocess.setup_multi(([''],), (['testpool/clone\tfilesystem\tlz4'],))
        CloneCommand()(sample_snapshot, 'testpool/clone', properties={'compression': prop})
        argv = list(mock_subprocess.call_args_list[0][0][0])
        assert argv[argv.index('-o') + 1] == text
        assert not any('Property' in a or 'object at' in a for a in argv)

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_clone_several_properties_keep_their_order(self, mock_subprocess, sample_snapshot):
        mock_subprocess.setup_multi(([''],), (['testpool/clone\tfilesystem\t1\t2'],))
        CloneCommand()(sample_snapshot, 'testpool/clone', properties={'atime': 'off', 'compression': Property('lz4')})
        argv = list(mock_subprocess.call_args_list[0][0][0])[1:]
        assert argv == ['clone', '-o', 'atime=off', '-o', 'compression=lz4',
                        'testpool/filesystem@snap1', 'testpool/clone']

    @pytest.mark.unit
    @pytest.mark.subprocess
    @pytest.mark.parametrize('properties', [{'not_a_property': 'x'}, {'compression': 'lz4', 'bad key!': 'x'}])
    def test_clone_invalid_key_raises_before_exec(self, mock_subprocess, sample_snapshot, properties):
        mock_subprocess.setup_multi(([''],), (['testpool/clone\tfilesystem'],))
        with pytest.raises(ValueError):
            CloneCommand()(sample_snapshot, 'testpool/clone', properties=properties)
        mock_subprocess.assert_not_called()

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_clone_parents_emits_dash_p_before_the_names(self, mock_subprocess, sample_snapshot):
        mock_subprocess.setup_multi(([''],), (['testpool/a/b\tfilesystem'],))
        CloneCommand()(sample_snapshot, 'testpool/a/b', parents=True)
        assert list(mock_subprocess.call_args_list[0][0][0])[1:] == [
            'clone', '-p', 'testpool/filesystem@snap1', 'testpool/a/b']

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_clone_without_properties_has_no_dash_o(self, mock_subprocess, sample_snapshot):
        mock_subprocess.setup_multi(([''],), (['testpool/clone\tfilesystem'],))
        CloneCommand()(sample_snapshot, 'testpool/clone')
        assert '-o' not in list(mock_subprocess.call_args_list[0][0][0])

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
            'testpool/filesystem\tname\ttestpool/filesystem\t-\t-\n',
            'testpool/filesystem\ttype\tfilesystem\t-\t-\n',
            'testpool/filesystem\tcompression\tlz4\t-\tlocal\n',
            'testpool/filesystem/fs2\tname\ttestpool/filesystem/fs2\t-\t-\n',
            'testpool/filesystem/fs2\ttype\tfilesystem\t-\t-\n',
            'testpool/filesystem/fs2\tcompression\tgzip\t-\tlocal\n',
        ])
        # (the names must be in scope of the requested root and depth, or B2 drops them)
        result = GetCommand()(ds=sample_filesystem, properties='compression', recursive=True)
        assert [r.name for r in result] == ['testpool/filesystem', 'testpool/filesystem/fs2']
        assert [r['compression'].value for r in result] == ['lz4', 'gzip']

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


class TestSendStdoutEof:
    """`SendCommand._exec_capture_bin` when stdout hits EOF.

    EOF with the child still running must not spin on peek()/poll(): stderr is drained to its end first (a bare
    wait() can deadlock on a full stderr pipe), then the child is reaped and its exit status decides the outcome.
    """

    @staticmethod
    def _eof_process(order, rc, stderr_bytes=b''):
        """A mocked child whose stdout is at EOF while it is still running. stderr is served line by line from
        readline() (as the reader thread consumes it) and ends with b'' (EOF); nothing here can spin."""
        process = MagicMock()
        process.stdout.peek.return_value = b''
        process.poll.return_value = None  # still running when stdout hits EOF
        lines = stderr_bytes.splitlines(keepends=True)

        def _readline(*args):
            if lines:
                order.append('stderr.readline')
                return lines.pop(0)
            order.append('stderr.eof')
            return b''

        def _wait(*a, **kw):
            order.append('wait')
            return rc

        process.stderr.readline.side_effect = _readline
        process.wait.side_effect = _wait
        return process

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_eof_then_rc_zero_returns_empty_handle_and_collects_stderr_via_the_reader(self, mock_subprocess):
        order = []
        process = self._eof_process(order, rc=0, stderr_bytes=b'warning: nothing to send\n')
        mock_subprocess.return_value = process

        handle = SendCommand._exec_stream(['zfs', 'send', 'testpool/fs@snap'])

        assert isinstance(handle, StreamHandle)
        # The reader thread (not a blocking read() before wait()) consumed stderr; it was joined before returning.
        assert handle._errors == ['Warning: nothing to send']
        assert handle._stderr_thread is not None and not handle._stderr_thread.is_alive()
        assert order.count('wait') == 1 and 'stderr.eof' in order
        process.wait.assert_called_once()
        process.stderr.read.assert_not_called()

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_eof_then_nonzero_rc_raises_stderr_with_first_line_capitalised(self, mock_subprocess):
        order = []
        process = self._eof_process(
            order, rc=1, stderr_bytes=b'cannot open dataset: does not exist\n  second problem\n\n')
        mock_subprocess.return_value = process

        with pytest.raises(Exception) as exc_info:
            SendCommand._exec_stream(['zfs', 'send', 'testpool/fs@snap'])

        assert str(exc_info.value) == 'Cannot open dataset: does not exist\nsecond problem'
        assert order.count('wait') == 1
        assert 'stderr.eof' in order  # the reader reached EOF before the message was raised
        process.stderr.read.assert_not_called()

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_eof_nonzero_rc_with_empty_stderr_raises(self, mock_subprocess):
        order = []
        process = self._eof_process(order, rc=2, stderr_bytes=b'')
        mock_subprocess.return_value = process

        with pytest.raises(Exception):
            SendCommand._exec_stream(['zfs', 'send', 'testpool/fs@snap'])
        process.wait.assert_called_once()

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_eof_does_not_spin_on_peek(self, mock_subprocess):
        """The old behaviour looped on peek()/poll() until the child exited; now EOF is handled in one pass."""
        order = []
        process = self._eof_process(order, rc=0)
        mock_subprocess.return_value = process

        SendCommand._exec_stream(['zfs', 'send', 'testpool/fs@snap'])

        assert process.stdout.peek.call_count == 1
        assert process.poll.call_count == 1

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_peek_data_returns_stream_without_touching_stderr_or_wait(self, mock_subprocess):
        process = MagicMock()
        process.stdout.peek.return_value = b'ZFS_DATA'
        process.poll.return_value = None
        mock_subprocess.return_value = process

        handle = SendCommand._exec_stream(['zfs', 'send', 'testpool/fs@snap'])

        assert isinstance(handle, StreamHandle)
        process.stderr.read.assert_not_called()
        process.wait.assert_not_called()

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_rc_already_set_zero_returns_empty_handle_without_wait(self, mock_subprocess):
        process = MagicMock()
        process.stdout.peek.return_value = b''
        process.poll.return_value = 0
        mock_subprocess.return_value = process

        handle = SendCommand._exec_stream(['zfs', 'send', 'testpool/fs@snap'])

        assert isinstance(handle, StreamHandle)
        process.wait.assert_not_called()
        process.stderr.read.assert_not_called()

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_rc_already_set_nonzero_raises_without_wait(self, mock_subprocess):
        stderr_r, stderr_w = os.pipe()
        os.write(stderr_w, b'cannot send: permission denied\n')
        os.close(stderr_w)
        stderr = os.fdopen(stderr_r, 'rb')
        try:
            process = MagicMock()
            process.stdout.peek.return_value = b''
            process.stderr = stderr
            process.poll.return_value = 1
            mock_subprocess.return_value = process

            with pytest.raises(Exception) as exc_info:
                SendCommand._exec_stream(['zfs', 'send', 'testpool/fs@snap'])

            assert 'Cannot send: permission denied' in str(exc_info.value)
            process.wait.assert_not_called()
        finally:
            stderr.close()


class TestSendStdoutEofRealChild:
    """Real `sys.executable -c` children (never a real zfs): closed stdout plus a stderr flood larger than the pipe."""

    TIMEOUT = 20

    # Close fd 1 (stdout EOF for the parent) while staying alive, flood stderr past any pipe buffer (> 64 KB), exit.
    CHILD = (
        "import os, sys\n"
        "os.close(1)\n"
        "for i in range(3000):\n"
        "    sys.stderr.write('noise line %d xxxxxxxxxxxxxxxxxxxxxxxx\\n' % i)\n"
        "sys.stderr.write('disk exploded\\n')\n"
        "sys.stderr.flush()\n"
        "sys.exit({rc})\n"
    )

    def _run(self, monkeypatch, rc):
        import threading
        real_popen = subprocess.Popen
        procs = []

        polls = []

        def spying_popen(*a, **kw):
            p = real_popen(*a, **kw)
            real_poll = p.poll

            def counting_poll():
                polls.append(1)
                return real_poll()

            p.poll = counting_poll
            procs.append(p)
            return p

        monkeypatch.setattr(subprocess, 'Popen', spying_popen)
        outcome = {'polls': polls, 'procs': procs}

        def target():
            try:
                outcome['result'] = SendCommand._exec_stream([sys.executable, '-c', self.CHILD.format(rc=rc)])
            except BaseException as e:  # noqa: BLE001 - reported to the test
                outcome['error'] = e

        t = threading.Thread(target=target, daemon=True)
        t.start()
        t.join(self.TIMEOUT)
        try:
            assert not t.is_alive(), 'send EOF handling deadlocked or spun'
        finally:
            for p in procs:
                if p.poll() is None:
                    p.kill()
                    p.wait()
        return outcome

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_stderr_flood_nonzero_exit_raises_without_deadlock(self, monkeypatch):
        outcome = self._run(monkeypatch, rc=1)

        assert 'result' not in outcome
        assert isinstance(outcome.get('error'), Exception)
        assert 'disk exploded' in str(outcome['error'])
        # A regression to the old spin (peek()/poll() in a tight loop until the child exits) would poll thousands of
        # times while the child floods stderr; the fixed code polls once per capture and then blocks in wait().
        assert len(outcome['polls']) <= 3, 'send EOF handling spun: %d poll() calls' % len(outcome['polls'])

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_stderr_flood_nonzero_exit_closes_pipes_and_reaps(self, monkeypatch):
        outcome = self._run(monkeypatch, rc=1)

        assert isinstance(outcome.get('error'), Exception)
        (proc,) = outcome['procs']
        assert proc.stdout.closed
        assert proc.stderr.closed
        assert proc.returncode is not None

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_stderr_flood_zero_exit_returns_empty_handle(self, monkeypatch):
        outcome = self._run(monkeypatch, rc=0)

        assert 'error' not in outcome
        handle = outcome['result']
        assert isinstance(handle, StreamHandle)
        handle.close()
        assert len(outcome['polls']) <= 3, 'send EOF handling spun: %d poll() calls' % len(outcome['polls'])


class TestSendFailureClosesPipes:
    """A failed send closes process.stdout / process.stderr before raising (no leaked pipe, no ResourceWarning)."""

    @staticmethod
    def _spawn(code):
        return subprocess.Popen([sys.executable, '-c', code], stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    @staticmethod
    @contextlib.contextmanager
    def _leak_watch(fds):
        """Fail if a ResourceWarning attributable to *this test's* pipes (or a still-running child) is emitted.

        Garbage left behind by earlier tests (e.g. an unclosed text-mode file) is collected, and its warnings discarded,
        before the action; afterwards only warnings naming one of ``fds`` (the caller appends the pipe descriptors it
        spawns) or an unreaped subprocess count.
        """
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            gc.collect()
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter('always')
            yield
            gc.collect()
        ours = [str(w.message) for w in caught
                if issubclass(w.category, ResourceWarning)
                and (any('name=%d>' % fd in str(w.message) for fd in fds) or 'subprocess' in str(w.message))]
        assert not ours, ours

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_nonzero_exit_without_stdout_closes_pipes(self):
        code = "import sys; sys.stderr.write('pool is busy\\n'); sys.exit(1)"
        fds = []
        with self._leak_watch(fds):
            proc = self._spawn(code)
            fds += [proc.stdout.fileno(), proc.stderr.fileno()]
            proc.wait()  # exited before the capture: poll() deterministically sees rc != 0 (the rc branch)
            errors = []
            raised = None
            try:
                SendCommand._exec_capture_bin(proc, errors)
            except Exception as e:  # noqa: BLE001
                raised = e
            assert raised is not None
            assert 'Pool is busy' in str(raised)
            assert proc.stdout.closed
            assert proc.stderr.closed
            assert proc.returncode == 1
            del raised, proc
            gc.collect()

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_stdout_eof_then_nonzero_exit_closes_pipes(self):
        code = ("import os, sys, time\n"
                "os.close(1)\n"
                "time.sleep(0.2)\n"
                "sys.stderr.write('stream broke\\n'); sys.stderr.flush()\n"
                "sys.exit(3)\n")
        fds = []
        with self._leak_watch(fds):
            proc = self._spawn(code)
            fds += [proc.stdout.fileno(), proc.stderr.fileno()]
            raised = None
            try:
                SendCommand._exec_capture_bin(proc, [])
            except Exception as e:  # noqa: BLE001
                raised = e
            try:
                assert raised is not None
                assert 'Stream broke' in str(raised)
                assert proc.stdout.closed
                assert proc.stderr.closed
                assert proc.returncode == 3
            finally:
                if proc.poll() is None:
                    proc.kill()
                    proc.wait()
            del raised, proc
            gc.collect()

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_exec_stream_failure_leaves_no_open_pipes(self, monkeypatch):
        procs = []
        fds = []
        real_popen = subprocess.Popen

        def spying_popen(*a, **kw):
            p = real_popen(*a, **kw)
            procs.append(p)
            fds.extend([p.stdout.fileno(), p.stderr.fileno()])  # recorded now: the pipes are closed by the failure
            return p

        monkeypatch.setattr(subprocess, 'Popen', spying_popen)
        code = "import sys; sys.stderr.write('nope\\n'); sys.exit(2)"
        with self._leak_watch(fds):
            with pytest.raises(Exception, match='Nope'):
                SendCommand._exec_stream([sys.executable, '-c', code])
            (proc,) = procs
            assert proc.stdout.closed and proc.stderr.closed
            assert proc.returncode == 2
            procs.clear()
            del proc
            gc.collect()

    @pytest.mark.unit
    @pytest.mark.subprocess
    @pytest.mark.parametrize('poll_value', [1, None])
    def test_mocked_process_without_pipes_still_raises_send_error(self, poll_value):
        process = MagicMock()
        process.stdout = None
        process.stderr = None
        process.poll.return_value = poll_value
        process.wait.return_value = 1
        with pytest.raises(Exception) as exc:
            SendCommand._exec_capture_bin(process, ['Earlier error'])
        assert 'Earlier error' in str(exc.value)

    @pytest.mark.unit
    @pytest.mark.subprocess
    @pytest.mark.parametrize('poll_value', [1, None])
    def test_mock_pipes_are_closed_and_close_errors_are_swallowed(self, poll_value):
        process = MagicMock()
        process.stdout.peek.return_value = b''
        process.stdout.closed = False
        process.stdout.close.side_effect = OSError('boom')
        process.stderr.closed = False
        process.stderr.read.return_value = b''
        process.poll.return_value = poll_value
        process.wait.return_value = 1
        with pytest.raises(Exception) as exc:
            SendCommand._exec_capture_bin(process, ['send failed'])
        assert not isinstance(exc.value, OSError)
        assert 'send failed' in str(exc.value)
        process.stdout.close.assert_called_once_with()
        process.stderr.close.assert_called_once_with()

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_already_closed_pipes_are_not_closed_again(self):
        process = MagicMock()
        process.stdout.peek.return_value = b''
        process.stdout.closed = True
        process.stderr.closed = True
        process.poll.return_value = 1
        with pytest.raises(Exception):
            SendCommand._exec_capture_bin(process, ['x'])
        process.stdout.close.assert_not_called()
        process.stderr.close.assert_not_called()


class _StreamGuard:
    """Run a test body on a thread under a watchdog so a regression (a deadlock or a spin) fails instead of hanging.

    Real `sys.executable -c` children only (never a real zfs). `Popen` is spied so every child is killed at the end,
    and after the body no new thread (the stderr readers in particular) may still be alive.
    """

    TIMEOUT = 30

    def __init__(self, monkeypatch):
        self.procs = []
        real_popen = subprocess.Popen

        def spying_popen(*a, **kw):
            p = real_popen(*a, **kw)
            self.procs.append(p)
            return p

        monkeypatch.setattr(subprocess, 'Popen', spying_popen)
        self._real_popen = real_popen
        self._monkeypatch = monkeypatch

    def run(self, body):
        import threading
        import time
        before = set(threading.enumerate())
        outcome = {}

        def target():
            try:
                outcome['value'] = body()
                deadline = time.monotonic() + 5
                while time.monotonic() < deadline:
                    stray = [t for t in threading.enumerate()
                             if t not in before and t is not threading.current_thread() and t.is_alive()]
                    if not stray:
                        break
                    time.sleep(0.05)
                outcome['stray'] = stray
            except BaseException as e:  # noqa: BLE001 - re-raised in the test thread
                outcome['error'] = e

        t = threading.Thread(target=target, daemon=True)
        t.start()
        t.join(self.TIMEOUT)
        hung = t.is_alive()
        for p in self.procs:
            if p.poll() is None:
                p.kill()
                p.wait()
        if hung:
            t.join(5)
            pytest.fail('stream handling hung for %ds (deadlock or spin)' % self.TIMEOUT)
        if 'error' in outcome:
            raise outcome['error']
        assert not outcome['stray'], 'stderr reader thread(s) left running: %r' % outcome['stray']
        return outcome['value']


def _script(*lines):
    return '\n'.join(lines) + '\n'


class TestStreamStderrReaderRealChild:
    """The send/receive stderr reader thread against real children (R5)."""

    # ~240 KB of stderr, written before any stdout: without a concurrent reader the child would block on a full pipe.
    SEND_FLOOD_THEN_DATA = _script(
        "import sys",
        "for i in range(4000):",
        "    sys.stderr.write('stderr line %d ' % i + 'x' * 40 + '\\n')",
        "sys.stderr.flush()",
        "sys.stdout.buffer.write(bytes(range(256)) * 4096)",
        "sys.stdout.buffer.flush()",
    )

    @pytest.fixture
    def guard(self, monkeypatch):
        return _StreamGuard(monkeypatch)

    @staticmethod
    def _child(code):
        return [sys.executable, '-c', code]

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_send_200kb_stderr_then_1mb_stdout_reads_everything_and_closes_clean(self, guard):
        def body():
            handle = SendCommand._exec_stream(self._child(self.SEND_FLOOD_THEN_DATA))
            data = handle.read()
            assert len(data) == 1024 * 1024
            assert data == bytes(range(256)) * 4096
            handle.close()  # exit 0: must not raise
            assert handle._stderr_thread is not None and not handle._stderr_thread.is_alive()
            # 4000 stderr lines are bounded: 200 first + one marker + 200 last
            assert len(handle._errors) == 401
            assert handle._errors[0].startswith('Stderr line 0')
            assert handle._errors[199].startswith('stderr line 199')
            assert handle._errors[200] == '... 3600 lines omitted ...'
            assert handle._errors[201].startswith('stderr line 3800')
            assert handle._errors[-1].startswith('stderr line 3999')
            return handle

        guard.run(body)
        (proc,) = guard.procs
        assert proc.returncode == 0
        assert proc.stdout.closed and proc.stderr.closed

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_send_startup_failure_raises_with_all_lines_up_to_the_last(self, guard):
        code = _script(
            "import sys",
            "for i in range(400):",
            "    sys.stderr.write('noise line %d ' % i + 'y' * 40 + '\\n')",
            "sys.stderr.write('fatal: it broke\\n')",
            "sys.exit(1)",
        )  # ~20 KB

        def body():
            with pytest.raises(Exception) as exc:
                SendCommand._exec_stream(self._child(code))
            return str(exc.value)

        message = guard.run(body)
        lines = message.split('\n')
        assert lines[0].startswith('Noise line 0')
        assert lines[-1] == 'fatal: it broke'
        assert len(lines) == 401
        (proc,) = guard.procs
        assert proc.returncode == 1
        assert proc.stdout.closed and proc.stderr.closed

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_receive_200kb_stderr_then_exit_1_close_raises_first_and_last_lines(self, guard):
        # Hold the child until stdin reaches EOF so the startup poll sees it running; close() then closes stdin.
        code = _script(
            "import sys",
            "sys.stdin.buffer.read()",
            "for i in range(4000):",
            "    sys.stderr.write('recv line %d ' % i + 'z' * 40 + '\\n')",
            "sys.stderr.write('cannot receive: stream is bad\\n')",
            "sys.exit(1)",
        )

        def body():
            handle = ReceiveCommand._exec_stream_in(self._child(code))
            assert isinstance(handle, StreamHandle)
            handle.write(b'some stream bytes')
            with pytest.raises(Exception) as exc:
                handle.close()
            return str(exc.value), handle

        message, handle = guard.run(body)
        lines = message.split('\n')
        assert lines[0].startswith('Recv line 0')
        assert lines[-1] == 'cannot receive: stream is bad'
        assert len(lines) == 401                       # bounded: 200 first + marker + 200 last
        assert lines[200] == '... 3601 lines omitted ...'
        assert lines[199].startswith('recv line 199')
        assert not handle._stderr_thread.is_alive()
        (proc,) = guard.procs
        assert proc.returncode == 1 and proc.stdin.closed and proc.stderr.closed

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_receive_startup_failure_raises_immediately(self, guard, monkeypatch):
        code = _script("import sys", "sys.stderr.write('cannot receive: no such pool\\n')", "sys.exit(1)")
        real_popen = subprocess.Popen  # the guard's spy

        def popen_then_wait(*a, **kw):
            p = real_popen(*a, **kw)
            p.wait()  # the child is gone before the startup poll, deterministically
            return p

        monkeypatch.setattr(subprocess, 'Popen', popen_then_wait)

        def body():
            with pytest.raises(Exception) as exc:
                ReceiveCommand._exec_stream_in(self._child(code))
            return str(exc.value)

        assert guard.run(body) == 'Cannot receive: no such pool'
        (proc,) = guard.procs
        assert proc.returncode == 1 and proc.stdin.closed and proc.stderr.closed

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_receive_startup_failure_without_stderr_reports_the_exit_code(self, guard, monkeypatch):
        real_popen = subprocess.Popen

        def popen_then_wait(*a, **kw):
            p = real_popen(*a, **kw)
            p.wait()
            return p

        monkeypatch.setattr(subprocess, 'Popen', popen_then_wait)

        def body():
            with pytest.raises(Exception) as exc:
                ReceiveCommand._exec_stream_in(self._child('import sys; sys.exit(4)'))
            return str(exc.value)

        assert 'exit code 4' in guard.run(body)

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_empty_send_stream_returns_a_handle_that_reads_nothing_and_closes_clean(self, guard):
        def body():
            handle = SendCommand._exec_stream(self._child('pass'))
            assert isinstance(handle, StreamHandle)
            assert handle.read() == b''
            handle.close()
            handle.close()
            return handle

        guard.run(body)
        (proc,) = guard.procs
        assert proc.returncode == 0 and proc.stdout.closed and proc.stderr.closed

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_send_output_then_nonzero_exit_close_raises_exactly_the_stderr(self, guard):
        code = _script("import sys",
                       "sys.stdout.buffer.write(b'DATA'); sys.stdout.buffer.flush()",
                       "sys.stderr.write('boom\\n'); sys.stderr.flush()",
                       "sys.exit(3)")

        def body():
            handle = SendCommand._exec_stream(self._child(code))
            assert handle.read() == b'DATA'
            with pytest.raises(Exception) as exc:
                handle.close()
            return str(exc.value)

        assert guard.run(body) == 'Boom'

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_send_failing_after_partial_stdout_close_raises_first_and_last_lines(self, guard):
        code = _script(
            "import sys",
            "sys.stdout.buffer.write(b'p' * (300 * 1024)); sys.stdout.buffer.flush()",
            "for i in range(1000):",
            "    sys.stderr.write('late line %d ' % i + 'q' * 40 + '\\n')",
            "sys.stderr.write('send failed: i/o error\\n')",
            "sys.exit(1)",
        )

        def body():
            handle = SendCommand._exec_stream(self._child(code))
            assert len(handle.read()) == 300 * 1024
            with pytest.raises(Exception) as exc:
                handle.close()
            return str(exc.value)

        lines = guard.run(body).split('\n')
        assert lines[0].startswith('Late line 0')
        assert lines[-1] == 'send failed: i/o error'
        assert len(lines) == 401
        assert lines[200] == '... 601 lines omitted ...'

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_close_is_idempotent_after_success_and_after_failure(self, guard):
        failing = _script("import sys",
                          "sys.stdout.buffer.write(b'D'); sys.stdout.buffer.flush()",
                          "sys.stderr.write('nope\\n'); sys.exit(2)")

        def body():
            ok = SendCommand._exec_stream(self._child('import sys; sys.stdout.buffer.write(b"x")'))
            assert ok.read() == b'x'
            ok.close()
            ok.close()
            bad = SendCommand._exec_stream(self._child(failing))
            bad.read()
            with pytest.raises(Exception, match='Nope'):
                bad.close()
            bad.close()  # already closed: no second raise, no second wait
            with bad:    # context-manager exit after close is also harmless
                pass

        guard.run(body)
        assert len(guard.procs) == 2 and all(p.returncode is not None for p in guard.procs)

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_context_manager_exit_raises_on_failure(self, guard):
        code = _script("import sys",
                       "sys.stdout.buffer.write(b'D'); sys.stdout.buffer.flush()",
                       "sys.stderr.write('ctx failure\\n'); sys.exit(1)")

        def body():
            with pytest.raises(Exception, match='Ctx failure'):
                with SendCommand._exec_stream(self._child(code)) as stream:
                    stream.read()

        guard.run(body)

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_no_reader_thread_remains_after_close_or_startup_failure(self, guard):
        import threading

        def body():
            before = set(threading.enumerate())
            ok = SendCommand._exec_stream(self._child(self.SEND_FLOOD_THEN_DATA))
            ok.read()
            ok.close()
            with pytest.raises(Exception):
                SendCommand._exec_stream(self._child("import sys; sys.stderr.write('x\\n'); sys.exit(1)"))
            rx = ReceiveCommand._exec_stream_in(self._child("import sys; sys.stdin.buffer.read()"))
            rx.close()
            return [t for t in threading.enumerate() if t not in before and t is not threading.current_thread()]

        assert guard.run(body) == []

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_keyboard_interrupt_during_startup_terminates_child_and_leaks_nothing(self, guard, monkeypatch):
        """An interrupt while blocked in the startup peek() must not leave the child, a reader thread or an fd."""
        spy_popen = subprocess.Popen

        class InterruptingStdout:
            def __init__(self, real):
                self._real = real

            def peek(self, n=1):
                raise KeyboardInterrupt

            def __getattr__(self, name):
                return getattr(self._real, name)

        def popen_with_interrupting_stdout(*a, **kw):
            p = spy_popen(*a, **kw)
            p.stdout = InterruptingStdout(p.stdout)
            return p

        monkeypatch.setattr(subprocess, 'Popen', popen_with_interrupting_stdout)
        sleeper = _script("import time", "time.sleep(60)")

        def body():
            gc.collect()
            fds_before = len(os.listdir('/dev/fd'))
            with pytest.raises(KeyboardInterrupt):
                SendCommand._exec_stream(self._child(sleeper))
            gc.collect()
            return fds_before, len(os.listdir('/dev/fd'))

        before, after = guard.run(body)
        (proc,) = guard.procs
        assert proc.returncode is not None and proc.returncode != 0      # terminated, reaped
        assert proc.stderr.closed and proc.stdout.closed
        assert after <= before

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_non_utf8_stderr_is_decoded_with_replacement(self, guard):
        code = _script("import sys",
                       "sys.stderr.buffer.write(b'bad \\xff\\xfe byte\\n'); sys.stderr.buffer.flush()",
                       "sys.exit(1)")

        def body():
            with pytest.raises(Exception) as exc:
                SendCommand._exec_stream(self._child(code))
            return str(exc.value)

        message = guard.run(body)
        assert message.startswith('Bad ')
        assert '�' in message and message.endswith(' byte')


def _open_pipes(process):
    """MagicMock attributes are truthy, so `pipe.closed` must be set False for `_close_pipe` to act."""
    for name in ('stdin', 'stdout', 'stderr'):
        getattr(process, name).closed = False
    return process


class TestStreamStderrReaderMocked:
    """The reader thread, `_abort_stream`, `_exec_capture_bin` and `StreamHandle.close` with mocks that cannot spin."""

    @pytest.mark.unit
    def test_magicmock_stderr_ends_the_reader_without_spinning(self):
        process = MagicMock()  # readline() returns a MagicMock, never bytes
        errors = []
        thread = Command._start_stderr_reader_bin(process, errors)
        thread.join(5)
        assert not thread.is_alive()
        assert process.stderr.readline.call_count == 1
        assert errors == []

    @pytest.mark.unit
    @pytest.mark.parametrize('bad', ['text line\n', '', None, 42])
    def test_non_bytes_lines_end_the_reader_quietly(self, bad):
        process = MagicMock()
        process.stderr.readline.return_value = bad
        errors = []
        thread = Command._start_stderr_reader_bin(process, errors)
        thread.join(5)
        assert not thread.is_alive()
        assert process.stderr.readline.call_count == 1
        assert errors == []

    @pytest.mark.unit
    def test_no_stderr_pipe_means_no_thread(self):
        process = MagicMock()
        process.stderr = None
        assert Command._start_stderr_reader_bin(process, []) is None

    @pytest.mark.unit
    def test_reader_decodes_leniently_capitalises_and_skips_blank_lines(self):
        process = MagicMock()
        process.stderr.readline.side_effect = [b'  first \xff problem\n', b'\n', b'   \n', b'second\n', b'']
        errors = []
        Command._start_stderr_reader_bin(process, errors).join(5)
        assert errors == ['First � problem', 'second']

    @pytest.mark.unit
    def test_reader_survives_a_pipe_closed_under_it(self):
        process = MagicMock()
        process.stderr.readline.side_effect = [b'one\n', ValueError('I/O operation on closed file')]
        errors = []
        thread = Command._start_stderr_reader_bin(process, errors)
        thread.join(5)
        assert not thread.is_alive()
        assert errors == ['One']

    @pytest.mark.unit
    def test_join_is_bounded_and_tolerates_none(self, monkeypatch):
        import threading
        import time
        import libzfseasy.zfs as zfs_mod
        Command._join_stderr_reader(None)
        monkeypatch.setattr(zfs_mod, '_TERMINATE_TIMEOUT', 0.2)
        release = threading.Event()
        stuck = threading.Thread(target=release.wait, daemon=True)
        stuck.start()
        try:
            start = time.monotonic()
            Command._join_stderr_reader(stuck)
            assert time.monotonic() - start < 3
            assert stuck.is_alive()
        finally:
            release.set()
            stuck.join(5)

    @pytest.mark.unit
    def test_abort_stream_terminates_a_running_child_and_joins_the_reader(self):
        process = _open_pipes(MagicMock())
        process.returncode = None
        process.poll.return_value = None
        thread = MagicMock()
        thread.is_alive.return_value = False
        Command._abort_stream(process, thread)
        process.terminate.assert_called_once()
        process.wait.assert_called()
        thread.join.assert_called()
        process.stdin.close.assert_called()
        process.stdout.close.assert_called()
        process.stderr.close.assert_called()

    @pytest.mark.unit
    def test_abort_stream_escalates_to_kill_and_never_raises(self):
        process = MagicMock()
        process.returncode = None
        process.poll.return_value = None
        process.wait.side_effect = [subprocess.TimeoutExpired('zfs', 1), 0]
        Command._abort_stream(process, None)
        process.kill.assert_called_once()

        broken = MagicMock()
        broken.returncode = None
        broken.poll.side_effect = OSError('gone')
        Command._abort_stream(broken, None)  # must not raise

    @pytest.mark.unit
    def test_abort_stream_does_not_close_stderr_under_a_stuck_reader(self):
        process = _open_pipes(MagicMock())
        process.returncode = 1
        thread = MagicMock()
        thread.is_alive.return_value = True
        Command._abort_stream(process, thread)
        process.stderr.close.assert_not_called()
        process.stdout.close.assert_called()

    @pytest.mark.unit
    def test_capture_bin_without_a_reader_drains_stderr_before_waiting(self):
        order = []
        process = MagicMock()
        process.stdout.peek.return_value = b''
        process.poll.return_value = None
        process.stderr.read.side_effect = lambda: order.append('read') or b'legacy problem\n'
        process.wait.side_effect = lambda *a, **k: order.append('wait') or 1
        errors = []
        with pytest.raises(Exception, match='Legacy problem'):
            SendCommand._exec_capture_bin(process, errors)
        assert order == ['read', 'wait']
        assert errors == ['Legacy problem']

    @pytest.mark.unit
    def test_capture_bin_with_a_reader_never_reads_stderr_itself(self):
        process = MagicMock()
        process.stdout.peek.return_value = b''
        process.poll.return_value = 0
        thread = MagicMock()
        thread.is_alive.return_value = False
        assert SendCommand._exec_capture_bin(process, [], thread) is False
        process.stderr.read.assert_not_called()
        thread.join.assert_called_once()

    @pytest.mark.unit
    def test_capture_bin_data_returns_stdout_without_waiting_or_joining(self):
        process = MagicMock()
        process.stdout.peek.return_value = b'ZFS'
        thread = MagicMock()
        assert SendCommand._exec_capture_bin(process, [], thread) is process.stdout
        process.wait.assert_not_called()
        thread.join.assert_not_called()

    @pytest.mark.unit
    def test_handle_with_a_thread_joins_before_raising(self):
        order = []
        errors = []
        process = MagicMock()
        process.wait.side_effect = lambda *a, **k: order.append('wait') or 1
        thread = MagicMock()
        thread.is_alive.return_value = False

        def join(*a, **k):
            order.append('join')
            errors.append('Late diagnostic')  # the reader finishing while we join

        thread.join.side_effect = join
        stream = MagicMock()
        stream.closed = False
        handle = StreamHandle(process, stream, errors, thread)
        with pytest.raises(Exception) as exc:
            handle.close()
        assert str(exc.value) == 'Late diagnostic'
        assert order == ['wait', 'join']
        process.stderr.read.assert_not_called()
        stream.close.assert_called_once()

    @pytest.mark.unit
    def test_handle_with_a_stuck_thread_still_raises_and_leaves_stderr_open(self):
        process = MagicMock()
        process.wait.return_value = 1
        thread = MagicMock()
        thread.is_alive.return_value = True
        stream = MagicMock()
        stream.closed = False
        handle = StreamHandle(process, stream, ['Seen so far'], thread)
        with pytest.raises(Exception, match='Seen so far'):
            handle.close()
        process.stderr.close.assert_not_called()

    @pytest.mark.unit
    def test_handle_without_a_thread_falls_back_to_reading_stderr(self):
        process = _open_pipes(MagicMock())
        process.wait.return_value = 1
        process.stderr.read.return_value = b'oops: failed\n  and more\n'
        stream = MagicMock()
        stream.closed = False
        handle = StreamHandle(process, stream)
        with pytest.raises(Exception) as exc:
            handle.close()
        assert str(exc.value) == 'Oops: failed\nand more'
        process.stderr.read.assert_called_once()
        process.stderr.close.assert_called_once()

    @pytest.mark.unit
    def test_handle_success_does_not_raise_and_is_idempotent(self):
        process = MagicMock()
        process.wait.return_value = 0
        thread = MagicMock()
        thread.is_alive.return_value = False
        stream = MagicMock()
        stream.closed = False
        handle = StreamHandle(process, stream, [], thread)
        handle.close()
        handle.close()
        process.wait.assert_called_once()
        stream.close.assert_called_once()


class TestStreamHandleCloseAndAbortRealChild:
    """Pipe errors at close, interrupts during startup and grandchildren holding stderr (R5 follow-up)."""

    @pytest.fixture
    def guard(self, monkeypatch):
        return _StreamGuard(monkeypatch)

    @staticmethod
    def _child(code):
        return [sys.executable, '-c', code]

    @staticmethod
    def _receive_child(exit_code, message=None):
        """Reads one byte of the stream, then (optionally) reports on stderr and exits without reading any more."""
        lines = ["import sys", "sys.stdin.buffer.read(1)"]
        if message:
            lines.append("sys.stderr.write(%r + '\\n'); sys.stderr.flush()" % message)
        lines.append("sys.exit(%d)" % exit_code)
        return _script(*lines)

    @staticmethod
    def _released_and_gone(handle, proc):
        handle.write(b'a')
        handle.flush()  # the child reads this byte, reports, exits
        proc.wait(timeout=20)

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_receive_exit_nonzero_with_pending_bytes_raises_stderr_text_caused_by_the_pipe_error(self, guard):
        def body():
            handle = ReceiveCommand._exec_stream_in(self._child(self._receive_child(1, 'receive aborted: bad stream')))
            (proc,) = guard.procs
            self._released_and_gone(handle, proc)
            handle.write(b'bytes still buffered when the child is long gone')
            with pytest.raises(Exception) as exc:
                handle.close()
            assert not handle._stderr_thread.is_alive()
            handle.close()  # second close is a no-op
            return exc.value

        error = guard.run(body)
        assert str(error) == 'Receive aborted: bad stream'
        assert isinstance(error.__cause__, BrokenPipeError)
        (proc,) = guard.procs
        assert proc.returncode == 1
        assert proc.stdin.closed and proc.stderr.closed

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_receive_exit_zero_with_pending_bytes_reraises_the_pipe_error(self, guard):
        def body():
            handle = ReceiveCommand._exec_stream_in(self._child(self._receive_child(0)))
            (proc,) = guard.procs
            self._released_and_gone(handle, proc)
            handle.write(b'bytes still buffered when the child is long gone')
            with pytest.raises(BrokenPipeError):
                handle.close()
            assert not handle._stderr_thread.is_alive()
            handle.close()

        guard.run(body)
        (proc,) = guard.procs
        assert proc.returncode == 0 and proc.stdin.closed and proc.stderr.closed

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_grandchild_holding_stderr_costs_one_bounded_join_on_startup_failure(self, guard, monkeypatch):
        import signal
        import time
        import libzfseasy.zfs as zfs_mod
        monkeypatch.setattr(zfs_mod, '_TERMINATE_TIMEOUT', 1.0)
        code = _script(
            "import subprocess, sys",
            "g = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)'],",
            "                     stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL)",  # inherits stderr
            "sys.stderr.write('grandchild %d\\n' % g.pid); sys.stderr.flush()",
            "sys.exit(1)",
        )

        def body():
            start = time.monotonic()
            with pytest.raises(Exception) as exc:
                SendCommand._exec_stream(self._child(code))
            elapsed = time.monotonic() - start
            pid = int(str(exc.value).split()[-1])
            try:
                (proc,) = guard.procs
                assert proc.returncode == 1
                return elapsed
            finally:
                os.kill(pid, signal.SIGKILL)  # releases stderr, so the (daemon) reader ends
                time.sleep(0.2)
                guard.procs[0].stderr.close()

        elapsed = guard.run(body)
        # One join of ~1 s (the reader never sees EOF); the abort path must not wait it out a second time.
        assert 0.8 <= elapsed < 1.9, elapsed

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_keyboard_interrupt_from_thread_start_kills_the_child_and_closes_pipes(self, guard, monkeypatch):
        import threading
        sleeper = _script("import time", "time.sleep(60)")
        original_start = threading.Thread.start
        state = {'armed': False}

        def start(self_thread):
            if state['armed']:
                raise KeyboardInterrupt
            return original_start(self_thread)

        def body():
            monkeypatch.setattr(threading.Thread, 'start', start)
            state['armed'] = True
            try:
                with pytest.raises(KeyboardInterrupt):
                    SendCommand._exec_stream(self._child(sleeper))
            finally:
                state['armed'] = False

        guard.run(body)
        (proc,) = guard.procs
        assert proc.returncode is not None and proc.returncode != 0
        assert proc.stdout.closed and proc.stderr.closed

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_keyboard_interrupt_from_thread_start_on_receive_kills_the_child(self, guard, monkeypatch):
        import threading
        original_start = threading.Thread.start
        state = {'armed': False}

        def start(self_thread):
            if state['armed']:
                raise KeyboardInterrupt
            return original_start(self_thread)

        def body():
            monkeypatch.setattr(threading.Thread, 'start', start)
            state['armed'] = True
            try:
                with pytest.raises(KeyboardInterrupt):
                    ReceiveCommand._exec_stream_in(self._child("import sys; sys.stdin.buffer.read()"))
            finally:
                state['armed'] = False

        guard.run(body)
        (proc,) = guard.procs
        assert proc.returncode is not None
        assert proc.stdin.closed and proc.stderr.closed

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_second_interrupt_inside_abort_still_kills_a_sigterm_ignoring_child(self, guard, monkeypatch):
        """First interrupt: the startup peek (after the child announced it is ready). Second interrupt: the wait()
        that `_abort_stream` does after SIGTERM. The child ignores SIGTERM, so only the kill in the `finally` can
        reap it; the interrupt must still propagate and the pipes must be closed."""
        spy_popen = subprocess.Popen

        class InterruptingStdout:
            def __init__(self, real):
                self._real = real

            def peek(self, n=1):
                self._real.peek(1)  # the child is ready (it has installed its SIGTERM handler)
                raise KeyboardInterrupt

            def __getattr__(self, name):
                return getattr(self._real, name)

        def popen_with_interrupts(*a, **kw):
            p = spy_popen(*a, **kw)
            p.stdout = InterruptingStdout(p.stdout)
            real_wait = p.wait

            def wait(*args, **kwargs):
                if not state['interrupted']:
                    state['interrupted'] = True
                    raise KeyboardInterrupt  # the second interrupt, mid-cleanup
                return real_wait(*args, **kwargs)

            p.wait = wait
            return p

        state = {'interrupted': False}
        monkeypatch.setattr(subprocess, 'Popen', popen_with_interrupts)
        stubborn = _script("import signal, sys, time",
                           "signal.signal(signal.SIGTERM, signal.SIG_IGN)",
                           "sys.stdout.buffer.write(b'r'); sys.stdout.buffer.flush()",
                           "time.sleep(60)")

        def body():
            with pytest.raises(KeyboardInterrupt):
                SendCommand._exec_stream(self._child(stubborn))
            (proc,) = guard.procs
            # Only the kill in `_abort_stream`'s finally can end this child (it ignores SIGTERM).
            assert proc.wait(timeout=15) == -9
            return proc

        guard.run(body)
        (proc,) = guard.procs
        # stdout is closed; stderr is deliberately left to the (still running) reader, which the guard saw end.
        assert proc.stdout.closed

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_real_child_with_huge_stderr_completes_with_a_bounded_message(self, guard):
        code = _script(
            "import sys",
            "w = sys.stderr.write",
            "for i in range(100000):",
            "    w('line %d ' % i + 'p' * 50 + '\\n')",
            "w('L' * (1024 * 1024) + '\\n')",
            "w('final line\\n')",
            "sys.exit(1)",
        )  # ~7 MB of stderr, one 1 MiB line

        def body():
            with pytest.raises(Exception) as exc:
                SendCommand._exec_stream(self._child(code))
            return str(exc.value)

        message = guard.run(body)
        lines = message.split('\n')
        assert len(lines) <= 401
        assert lines[0].startswith('Line 0 ')
        assert lines[-1] == 'final line'
        assert any(l.startswith('... ') and l.endswith(' lines omitted ...') for l in lines)
        assert len(message) < 200_000


class TestStreamHandleCloseMocked:

    @staticmethod
    def _handle(rc, stream_error=None, errors=None):
        process = _open_pipes(MagicMock())
        process.wait.return_value = rc
        thread = MagicMock()
        thread.is_alive.return_value = False
        stream = MagicMock()
        stream.closed = False
        if stream_error is not None:
            stream.close.side_effect = stream_error
        return StreamHandle(process, stream, errors if errors is not None else [], thread), process, thread

    @pytest.mark.unit
    @pytest.mark.parametrize('pipe_error', [BrokenPipeError('pipe'), OSError('io'), ValueError('closed file')])
    def test_pipe_error_with_nonzero_exit_raises_stderr_chained_from_it(self, pipe_error):
        handle, process, thread = self._handle(1, pipe_error, ['Real diagnostic'])
        with pytest.raises(Exception) as exc:
            handle.close()
        assert str(exc.value) == 'Real diagnostic'
        assert exc.value.__cause__ is pipe_error
        # the reap, the reader join and the stderr close all still happened
        process.wait.assert_called_once()
        thread.join.assert_called()
        process.stderr.close.assert_called_once()
        assert handle._closed is True

    @pytest.mark.unit
    @pytest.mark.parametrize('pipe_error', [BrokenPipeError('pipe'), OSError('io'), ValueError('closed file')])
    def test_pipe_error_with_exit_zero_is_reraised(self, pipe_error):
        handle, process, thread = self._handle(0, pipe_error)
        with pytest.raises(type(pipe_error)) as exc:
            handle.close()
        assert exc.value is pipe_error
        process.wait.assert_called_once()
        process.stderr.close.assert_called_once()
        handle.close()  # no second raise
        process.wait.assert_called_once()

    @pytest.mark.unit
    def test_non_pipe_errors_from_the_stream_close_are_not_swallowed(self):
        handle, process, _ = self._handle(0, RuntimeError('unexpected'))
        with pytest.raises(RuntimeError):
            handle.close()

    @pytest.mark.unit
    def test_interrupt_in_wait_leaves_the_handle_open_so_a_later_close_completes(self):
        handle, process, thread = self._handle(0)
        process.wait.side_effect = [KeyboardInterrupt, 0]
        with pytest.raises(KeyboardInterrupt):
            handle.close()
        assert handle._closed is False
        thread.join.assert_not_called()
        handle.close()
        assert handle._closed is True
        assert process.wait.call_count == 2
        thread.join.assert_called()
        process.stderr.close.assert_called_once()
        handle.close()
        assert process.wait.call_count == 2


class TestJoinAndAbortMocked:

    @pytest.mark.unit
    def test_join_stderr_reader_joins_each_thread_at_most_once(self):
        thread = MagicMock()
        Command._join_stderr_reader(thread)
        Command._join_stderr_reader(thread)
        Command._join_stderr_reader(thread)
        thread.join.assert_called_once()

    @pytest.mark.unit
    def test_join_stderr_reader_second_call_does_not_wait_out_a_stuck_thread_again(self, monkeypatch):
        import threading
        import time
        import libzfseasy.zfs as zfs_mod
        monkeypatch.setattr(zfs_mod, '_TERMINATE_TIMEOUT', 0.4)
        release = threading.Event()
        stuck = threading.Thread(target=release.wait, daemon=True)
        stuck.start()
        try:
            start = time.monotonic()
            Command._join_stderr_reader(stuck)
            first = time.monotonic() - start
            start = time.monotonic()
            Command._join_stderr_reader(stuck)
            second = time.monotonic() - start
            assert first >= 0.3 and second < 0.2
        finally:
            release.set()
            stuck.join(5)

    @pytest.mark.unit
    def test_abort_after_a_normal_reap_does_not_kill(self):
        process = _open_pipes(MagicMock())
        process.returncode = None
        process.poll.return_value = None
        process.wait.return_value = 0          # SIGTERM honoured
        Command._abort_stream(process, None)
        process.terminate.assert_called_once()
        process.kill.assert_not_called()

        done = _open_pipes(MagicMock())
        done.returncode = 1                    # already reaped by its own failure
        Command._abort_stream(done, None)
        done.terminate.assert_not_called()
        done.kill.assert_not_called()

    @pytest.mark.unit
    def test_abort_after_sigterm_timeout_kills_exactly_once(self):
        process = _open_pipes(MagicMock())
        process.returncode = None
        process.poll.return_value = None
        process.wait.side_effect = [subprocess.TimeoutExpired('zfs', 1), 0]
        Command._abort_stream(process, None)
        process.kill.assert_called_once()

    @pytest.mark.unit
    def test_second_interrupt_in_abort_wait_kills_closes_pipes_and_propagates(self):
        process = _open_pipes(MagicMock())
        process.returncode = None
        process.poll.return_value = None
        process.wait.side_effect = KeyboardInterrupt
        with pytest.raises(KeyboardInterrupt):
            Command._abort_stream(process, None)
        process.kill.assert_called_once()
        process.stdin.close.assert_called_once()
        process.stdout.close.assert_called_once()
        process.stderr.close.assert_called_once()

    @pytest.mark.unit
    def test_interrupt_in_the_reader_join_still_closes_pipes(self):
        process = _open_pipes(MagicMock())
        process.returncode = 1
        thread = MagicMock()
        thread.join.side_effect = KeyboardInterrupt
        thread.is_alive.return_value = False
        with pytest.raises(KeyboardInterrupt):
            Command._abort_stream(process, thread)
        process.stdout.close.assert_called_once()
        process.stderr.close.assert_called_once()

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_keyboard_interrupt_from_peek_on_a_mocked_send_aborts(self, mock_subprocess):
        process = _open_pipes(MagicMock())
        process.returncode = None
        process.poll.return_value = None
        process.wait.return_value = -15
        process.stdout.peek.side_effect = KeyboardInterrupt
        process.stderr.readline.return_value = b''   # the reader ends at once; nothing can spin
        mock_subprocess.return_value = process
        with pytest.raises(KeyboardInterrupt):
            SendCommand._exec_stream(['zfs', 'send', 'pool/fs@s'])
        process.terminate.assert_called_once()
        process.stdout.close.assert_called_once()
        process.stderr.close.assert_called_once()

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_keyboard_interrupt_from_thread_start_on_a_mocked_receive_aborts(self, mock_subprocess, mocker):
        process = _open_pipes(MagicMock())
        process.returncode = None
        process.poll.return_value = None
        process.wait.return_value = -15
        mock_subprocess.return_value = process
        mocker.patch('threading.Thread.start', side_effect=KeyboardInterrupt)
        with pytest.raises(KeyboardInterrupt):
            ReceiveCommand._exec_stream_in(['zfs', 'receive', 'pool/fs'])
        process.terminate.assert_called_once()
        process.stdin.close.assert_called_once()
        process.stderr.close.assert_called_once()


class TestBoundedErrors:
    """`_BoundedErrors`: first 200 + `... N lines omitted ...` + last 200 (at most 401 entries)."""

    @staticmethod
    def _filled(n):
        from libzfseasy.zfs import _BoundedErrors
        errors = _BoundedErrors()
        for i in range(n):
            errors.append('e%d' % i)
        return errors

    @pytest.mark.unit
    def test_behaves_as_a_list_of_strings(self):
        errors = self._filled(3)
        assert isinstance(errors, list)
        assert errors == ['e0', 'e1', 'e2']
        assert '\n'.join(errors) == 'e0\ne1\ne2'
        assert list(self._filled(0)) == []

    @pytest.mark.unit
    @pytest.mark.parametrize('n', [1, 399, 400])
    def test_up_to_400_lines_are_kept_without_a_marker(self, n):
        errors = self._filled(n)
        assert list(errors) == ['e%d' % i for i in range(n)]

    @pytest.mark.unit
    def test_401_lines_omit_exactly_one(self):
        errors = self._filled(401)
        assert len(errors) == 401
        assert errors[:200] == ['e%d' % i for i in range(200)]
        assert errors[200] == '... 1 lines omitted ...'
        assert errors[201:] == ['e%d' % i for i in range(201, 401)]

    @pytest.mark.unit
    def test_many_lines_keep_head_marker_tail_in_order(self):
        errors = self._filled(1000)
        assert len(errors) == 401
        assert errors[:200] == ['e%d' % i for i in range(200)]
        assert errors[200] == '... 600 lines omitted ...'
        assert errors[201:] == ['e%d' % i for i in range(800, 1000)]

    @pytest.mark.unit
    def test_size_stays_bounded_and_the_count_is_exact(self):
        errors = self._filled(20000)
        assert len(errors) == 401
        assert errors[200] == '... 19600 lines omitted ...'
        assert errors[0] == 'e0' and errors[-1] == 'e19999'
        assert sum(1 for e in errors if e.startswith('... ')) == 1

    @pytest.mark.unit
    def test_each_stream_gets_its_own_instance(self):
        from libzfseasy.zfs import _BoundedErrors
        a, b = _BoundedErrors(), _BoundedErrors()
        for i in range(500):
            a.append('x%d' % i)
        assert len(b) == 0
        b.append('y')
        assert b == ['y']


class TestStderrLineTruncation:
    """The reader reads at most 64 KiB per readline; an over-long line is cut and the rest of it discarded."""

    @staticmethod
    def _read(data):
        from libzfseasy.zfs import _BoundedErrors
        process = MagicMock()
        process.stderr = io.BytesIO(data)
        errors = _BoundedErrors()
        Command._start_stderr_reader_bin(process, errors).join(10)
        return list(errors)

    @pytest.mark.unit
    def test_overlong_line_is_truncated_and_following_lines_are_unaffected(self):
        data = b'a' * 200_000 + b'\nnext line\nlast\n'
        errors = self._read(data)
        assert len(errors) == 3
        assert errors[0] == 'A' + 'a' * (64 * 1024 - 1) + ' [truncated]'
        assert errors[1:] == ['next line', 'last']

    @pytest.mark.unit
    def test_a_line_that_just_fits_is_not_truncated(self):
        body = b'b' * (64 * 1024 - 2)                      # + '\n' = exactly the read limit - 1
        assert self._read(body + b'\nafter\n') == ['B' + 'b' * (64 * 1024 - 3), 'after']
        exact = b'c' * (64 * 1024 - 1)                      # + '\n' = exactly the read limit, ends with a newline
        assert self._read(exact + b'\nafter\n') == ['C' + 'c' * (64 * 1024 - 2), 'after']

    @pytest.mark.unit
    def test_line_one_byte_over_the_limit_is_truncated(self):
        errors = self._read(b'd' * (64 * 1024) + b'\nafter\n')
        assert errors[0].endswith(' [truncated]') and errors[1:] == ['after']

    @pytest.mark.unit
    def test_overlong_unterminated_final_line_ends_the_reader(self):
        errors = self._read(b'first\n' + b'e' * 300_000)
        assert errors[0] == 'First'
        assert len(errors) == 2 and errors[1].endswith(' [truncated]')

    @pytest.mark.unit
    def test_several_overlong_lines_in_a_row(self):
        data = (b'x' * 150_000 + b'\n') * 3 + b'done\n'
        errors = self._read(data)
        assert [e.endswith(' [truncated]') for e in errors] == [True, True, True, False]
        assert errors[-1] == 'done'

    @pytest.mark.unit
    def test_readline_is_always_called_with_the_limit(self):
        process = MagicMock()
        process.stderr.readline.side_effect = [b'one\n', b'']
        Command._start_stderr_reader_bin(process, []).join(5)
        assert all(c.args == (64 * 1024,) for c in process.stderr.readline.call_args_list)


class TestDestroyDropsReclaimLines:
    """`zfs destroy -v -p` prints `destroy\\t<name>` per object plus a final `reclaim\\t<bytes>` summary (R7)."""

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_dataset_omits_the_reclaim_line(self, mock_subprocess):
        mock_subprocess.setup(stdout=['destroy\tpool/fs\n', 'reclaim\t0\n'])
        assert DestroyCommand().dataset(Filesystem('pool/fs'), destroy=False) == ['pool/fs']

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_snapshots_list_omits_the_reclaim_line(self, mock_subprocess):
        mock_subprocess.setup(stdout=['destroy\tpool/fs@a\n', 'destroy\tpool/fs@b\n', 'reclaim\t12345\n'])
        snaps = [Snapshot(Filesystem('pool/fs'), 'a'), Snapshot(Filesystem('pool/fs'), 'b')]
        assert DestroyCommand().snapshots(snaps, destroy=False) == ['pool/fs@a', 'pool/fs@b']

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_lazy_variants_omit_the_reclaim_line(self, mock_subprocess):
        mock_subprocess.setup(stdout=['destroy\tpool/fs\n', 'reclaim\t0\n'])
        result = DestroyCommand().dataset(Filesystem('pool/fs'), destroy=False, lazy=True)
        assert hasattr(result, '__next__')
        assert list(result) == ['pool/fs']

        mock_subprocess.setup(stdout=['destroy\tpool/fs@a\n', 'reclaim\t0\n'])
        result = DestroyCommand().snapshots(Snapshot(Filesystem('pool/fs'), 'a'), destroy=False, lazy=True)
        assert hasattr(result, '__next__')
        assert list(result) == ['pool/fs@a']

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_a_name_merely_containing_reclaim_is_kept(self, mock_subprocess):
        mock_subprocess.setup(stdout=['destroy\tpool/reclaim\n', 'destroy\tpool/reclaim@reclaim\n',
                                      'destroy\treclaimpool/x\n', 'reclaim\t0\n'])
        result = DestroyCommand().dataset(Filesystem('pool/reclaim'), destroy=False)
        assert result == ['pool/reclaim', 'pool/reclaim@reclaim', 'reclaimpool/x']

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_only_reclaim_output_is_empty(self, mock_subprocess):
        mock_subprocess.setup(stdout=['reclaim\t0\n'])
        assert DestroyCommand().dataset(Filesystem('pool/fs'), destroy=False) == []

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_recursive_output_lists_all_child_snapshots(self, mock_subprocess):
        mock_subprocess.setup(stdout=['destroy\tpool/fs@a\n', 'destroy\tpool/fs/child@a\n', 'reclaim\t0\n'])
        result = DestroyCommand().snapshots(Snapshot(Filesystem('pool/fs'), 'a'), destroy=False, recursive=True)
        assert result == ['pool/fs@a', 'pool/fs/child@a']
        assert '-r' in mock_subprocess.call_args[0][0]

    @pytest.mark.unit
    def test_destroy_lines_helper_directly(self):
        lines = ['destroy\ta\n', '  reclaim\t7  ', 'destroy\tb', 'reclaim\t0']
        assert list(DestroyCommand._destroy_lines(lines)) == ['a', 'b']
        assert list(DestroyCommand._destroy_lines([])) == []


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
    @pytest.mark.subprocess
    def test_load_key_all_argv_is_exactly_load_key_dash_a(self, mock_subprocess):
        mock_subprocess.setup()
        LoadKeyCommand()(None, location=None, recursive=False)
        assert list(mock_subprocess.call_args[0][0])[1:] == ['load-key', '-a']

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_load_key_all_with_a_location_or_recursive_never_runs_zfs(self, mock_subprocess):
        with pytest.raises(ValueError, match='Key location cannot be explicitly specified when loading all keys'):
            LoadKeyCommand()(None, location='/etc/key')
        with pytest.raises(ValueError, match='Recursive cannot be specified when loading all keys'):
            LoadKeyCommand()(None, recursive=True)
        mock_subprocess.assert_not_called()

    @pytest.mark.unit
    def test_load_key_requires_ds_to_be_passed_explicitly(self):
        with pytest.raises(TypeError):
            LoadKeyCommand()()

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
    @pytest.mark.subprocess
    def test_unload_key_all_argv_is_exactly_unload_key_dash_a(self, mock_subprocess):
        mock_subprocess.setup()
        UnLoadKeyCommand()(None, recursive=False)
        assert list(mock_subprocess.call_args[0][0])[1:] == ['unload-key', '-a']

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_unload_key_all_with_recursive_never_runs_zfs(self, mock_subprocess):
        with pytest.raises(ValueError, match='Recursive cannot be specified when unloading all keys'):
            UnLoadKeyCommand()(None, recursive=True)
        mock_subprocess.assert_not_called()

    @pytest.mark.unit
    def test_unload_key_requires_ds_to_be_passed_explicitly(self):
        with pytest.raises(TypeError):
            UnLoadKeyCommand()()

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

    # `iterations` is the one `-o` value formatted from a number rather than checked by Validate.value ----------------

    @pytest.mark.unit
    @pytest.mark.subprocess
    @pytest.mark.parametrize('bad', [0, -1, -200000, '100000', True, False, 1.5, 200000.0, [200000], b'1'],
                             ids=repr)
    def test_change_key_iterations_must_be_a_positive_int(self, mock_subprocess, sample_filesystem, bad):
        mock_subprocess.setup()         # so that a missing check fails the assertion below instead of hanging
        with pytest.raises(ValueError, match='^Number of pbkdf2 iterations must be a positive integer, got ') as exc:
            ChangeKeyCommand()(sample_filesystem, iterations=bad)
        assert str(exc.value).endswith(repr(bad))
        mock_subprocess.assert_not_called()

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_change_key_iterations_argv_is_exact(self, mock_subprocess, sample_filesystem):
        mock_subprocess.setup()
        ChangeKeyCommand()(sample_filesystem, iterations=200000)
        assert list(mock_subprocess.call_args[0][0])[1:] == [
            'change-key', '-o', 'pbkdf2iters=200000', str(sample_filesystem)]

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_change_key_iterations_one_is_accepted(self, mock_subprocess, sample_filesystem):
        mock_subprocess.setup()
        ChangeKeyCommand()(sample_filesystem, iterations=1)       # zfs enforces its own minimum
        assert 'pbkdf2iters=1' in mock_subprocess.call_args[0][0]

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_change_key_without_iterations_adds_no_pbkdf2iters(self, mock_subprocess, sample_filesystem):
        mock_subprocess.setup()
        ChangeKeyCommand()(sample_filesystem, iterations=None)
        assert not any('pbkdf2iters' in a for a in mock_subprocess.call_args[0][0])

    @pytest.mark.unit
    @pytest.mark.subprocess
    @pytest.mark.parametrize('bad', [0, -1, '100000', True, 1.5], ids=repr)
    def test_change_key_iterations_are_checked_before_the_inherit_check(self, mock_subprocess, sample_filesystem, bad):
        """A bad number (0 included) is reported as such, not as "cannot be specified when inheriting"."""
        mock_subprocess.setup()
        with pytest.raises(ValueError, match='must be a positive integer'):
            ChangeKeyCommand()(sample_filesystem, inherit=True, iterations=bad)
        mock_subprocess.assert_not_called()

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_change_key_inherit_with_valid_iterations_keeps_the_inherit_message(self, mock_subprocess,
                                                                                sample_filesystem):
        mock_subprocess.setup()
        with pytest.raises(ValueError, match='^Number of pbkdf2 iterations cannot be specified when inheriting keys$'):
            ChangeKeyCommand()(sample_filesystem, inherit=True, iterations=200000)
        mock_subprocess.assert_not_called()

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_change_key_inherit_with_none_iterations_runs(self, mock_subprocess, sample_filesystem):
        mock_subprocess.setup()
        ChangeKeyCommand()(sample_filesystem, inherit=True, iterations=None)
        assert list(mock_subprocess.call_args[0][0])[1:] == ['change-key', '-i', str(sample_filesystem)]

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


class TestMountCommandDefaults:
    """`ds`, `flags` and `properties` are optional."""

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_no_arguments_mounts_everything(self, mock_subprocess):
        mock_subprocess.setup()
        MountCommand()()
        assert list(mock_subprocess.call_args[0][0])[1:] == ['mount', '-a']

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_only_a_filesystem(self, mock_subprocess, sample_filesystem):
        mock_subprocess.setup()
        MountCommand()(sample_filesystem)
        assert list(mock_subprocess.call_args[0][0])[1:] == ['mount', sample_filesystem.name]

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_options_without_flags_or_properties(self, mock_subprocess, sample_filesystem):
        mock_subprocess.setup()
        MountCommand()(sample_filesystem, overlay=True, force=True)
        assert list(mock_subprocess.call_args[0][0])[1:] == ['mount', '-O', '-f', sample_filesystem.name]

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_private_entry_point_defaults(self, mock_subprocess):
        mock_subprocess.setup()
        MountCommand._mount()
        assert list(mock_subprocess.call_args[0][0])[1:] == ['mount', '-a']


class TestUnMountCommandExtended:

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_unmount_with_unload_keys(self, mock_subprocess, sample_filesystem):
        mock_subprocess.setup()
        UnMountCommand()(sample_filesystem, unload_keys=True)
        call_args = mock_subprocess.call_args[0][0]
        assert '-u' in call_args


# ---------------------------------------------------------------------------------------------------------------------
# Multi-line stderr: only the FIRST non-empty line is capitalised; continuation lines are the real zfs text
# ---------------------------------------------------------------------------------------------------------------------

CLONES_STDERR_LINES = ["cannot destroy 'p/fs@s': snapshot has dependent clones",
                       "use '-R' to destroy the following datasets:",
                       "p/clone3"]
CLONES_MESSAGE = ("Cannot destroy 'p/fs@s': snapshot has dependent clones\n"
                  "use '-R' to destroy the following datasets:\n"
                  "p/clone3")
# Leading noise: the first NON-empty line is the one that is capitalised.
BLANK_LEADS = [pytest.param(b'', id='no-blank'),
               pytest.param(b'\n', id='blank-first'),
               pytest.param(b'   \n\n', id='whitespace-first')]
CLONES_STDERR_BYTES = b''.join(line.encode() + b'\n' for line in CLONES_STDERR_LINES)


class TestStderrFirstLineOnlyBinaryPaths:
    """Binary stderr reader, the `_exec_capture_bin` no-thread fallback and the `StreamHandle.close` remainder
    fallback all go through `_append_error`.

    Mutation check (per-line capitalisation restored at one site, each verified separately): in
    `_start_stderr_reader_bin` it fails 11 tests (the 3 reader tests, the truncation-suffix test, the 3
    `_exec_stream` tests, the 4 real-child tests); in the `StreamHandle.close` fallback it fails the 3 `close`
    fallback tests; in the `_exec_capture_bin` no-thread fallback it fails the 3 capture tests.
    """

    @pytest.fixture
    def guard(self, monkeypatch):
        return _StreamGuard(monkeypatch)

    @staticmethod
    def _child(code):
        return [sys.executable, '-c', code]

    @pytest.mark.unit
    @pytest.mark.parametrize('lead', BLANK_LEADS)
    def test_binary_reader_capitalises_only_the_first_non_empty_line(self, lead):
        process = MagicMock()
        process.stderr.readline.side_effect = (lead.splitlines(keepends=True)
                                               + CLONES_STDERR_BYTES.splitlines(keepends=True) + [b''])
        errors = []
        Command._start_stderr_reader_bin(process, errors).join(5)
        assert '\n'.join(errors) == CLONES_MESSAGE

    @pytest.mark.unit
    def test_binary_reader_keeps_the_truncation_suffix_on_the_first_line_only(self):
        process = MagicMock()
        from libzfseasy.zfs import _BoundedErrors
        process.stderr = io.BytesIO(b'x' * 200_000 + b'\ncontinuation line\n')
        errors = _BoundedErrors()
        Command._start_stderr_reader_bin(process, errors).join(10)
        assert errors[0] == 'X' + 'x' * (64 * 1024 - 1) + ' [truncated]'
        assert errors[1] == 'continuation line'

    @pytest.mark.unit
    @pytest.mark.subprocess
    @pytest.mark.parametrize('lead', BLANK_LEADS)
    def test_exec_stream_startup_failure_keeps_continuation_lines_verbatim(self, mock_subprocess, lead):
        order = []
        process = TestSendStdoutEof._eof_process(order, rc=1, stderr_bytes=lead + CLONES_STDERR_BYTES)
        mock_subprocess.return_value = process
        with pytest.raises(Exception) as exc:
            SendCommand._exec_stream(['zfs', 'send', 'p/fs@s'])
        assert str(exc.value) == CLONES_MESSAGE

    @pytest.mark.unit
    @pytest.mark.parametrize('lead', BLANK_LEADS)
    def test_capture_without_a_reader_thread_drains_stderr_with_the_same_rule(self, lead):
        process = MagicMock()
        process.stdout.peek.return_value = b''
        process.poll.return_value = 1
        process.stderr.read.return_value = lead + CLONES_STDERR_BYTES
        errors = []
        with pytest.raises(Exception) as exc:
            SendCommand._exec_capture_bin(process, errors, None)
        assert str(exc.value) == CLONES_MESSAGE
        assert errors == CLONES_MESSAGE.split('\n')

    @pytest.mark.unit
    @pytest.mark.parametrize('lead', BLANK_LEADS)
    def test_close_fallback_without_a_thread_reads_stderr_with_the_same_rule(self, lead):
        process = _open_pipes(MagicMock())
        process.wait.return_value = 1
        process.stderr.read.return_value = lead + CLONES_STDERR_BYTES
        stream = MagicMock()
        stream.closed = False
        handle = StreamHandle(process, stream)
        with pytest.raises(Exception) as exc:
            handle.close()
        assert str(exc.value) == CLONES_MESSAGE

    @pytest.mark.unit
    @pytest.mark.subprocess
    @pytest.mark.parametrize('lead', ['', '\\n'], ids=['no-blank', 'blank-first'])
    def test_real_send_child_failing_at_startup(self, guard, lead):
        code = _script("import sys",
                       "sys.stderr.write(%r)" % (lead.replace('\\n', '\n')
                                                  + '\n'.join(CLONES_STDERR_LINES) + '\n'),
                       "sys.exit(1)")

        def body():
            with pytest.raises(Exception) as exc:
                SendCommand._exec_stream(self._child(code))
            return str(exc.value)

        assert guard.run(body) == CLONES_MESSAGE

    @pytest.mark.unit
    @pytest.mark.subprocess
    @pytest.mark.parametrize('lead', ['', '\\n'], ids=['no-blank', 'blank-first'])
    def test_real_receive_child_failing_at_close(self, guard, lead):
        code = _script("import sys",
                       "sys.stdin.buffer.read()",
                       "sys.stderr.write(%r)" % (lead.replace('\\n', '\n')
                                                  + '\n'.join(CLONES_STDERR_LINES) + '\n'),
                       "sys.exit(1)")

        def body():
            handle = ReceiveCommand._exec_stream_in(self._child(code))
            handle.write(b'some stream bytes')
            with pytest.raises(Exception) as exc:
                handle.close()
            return str(exc.value)

        assert guard.run(body) == CLONES_MESSAGE
