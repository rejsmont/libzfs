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

import os
import shutil
import time

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


def _settle_udev():
    """Wait for udev to finish probing new block devices (a no-op where there is no udevadm, e.g. macOS)."""
    udevadm = shutil.which('udevadm')
    if udevadm:
        subprocess.run([udevadm, 'settle', '--timeout=10'], capture_output=True)


def _destroy_volume_when_idle(vol, attempts=20, delay=0.5):
    """Destroy a zvol, first letting udev settle and then retrying while it reports "dataset is busy"."""
    _settle_udev()
    for attempt in range(attempts):
        try:
            zfs.destroy(vol, destroy=True)
            return
        except Exception as exc:
            if 'busy' not in str(exc).lower() or attempt == attempts - 1:
                raise
            time.sleep(delay)
            _settle_udev()


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
            # Cleanup. udev probes a new zvol (blkid etc.) and holds its device node for a moment, so an immediate
            # destroy can fail with "dataset is busy" (seen on OpenZFS 2.1.5): let udev finish, then retry briefly.
            _destroy_volume_when_idle(vol)


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


class TestRealGetLargeOutput:
    """`zfs get all -r` over enough datasets that stdout exceeds 64 KB (the OS pipe buffer).

    The mocked suite and tests/test_commands.py::TestExecRealPipes prove the exec path with a
    stand-in child; this proves the same non-blocking, complete read against real `zfs` output.
    """

    DATASETS = 25  # ~80+ properties per dataset: comfortably past 64 KB

    def test_get_all_recursive_exceeds_pipe_buffer(self, test_pool):
        parent_name = f'{test_pool}/test_get_large'
        parent = zfs.create.filesystem(parent_name)
        try:
            names = [parent_name]
            for i in range(self.DATASETS):
                zfs.create.filesystem(f'{parent_name}/child{i}')
                names.append(f'{parent_name}/child{i}')

            # Sanity: the raw output really is larger than a pipe buffer.
            raw = subprocess.run(
                [_zfs_cmd(), 'get', '-H', '-r', '-o', 'name,property,value,received,source',
                 'all', parent_name],
                capture_output=True, text=True, check=True,
            ).stdout
            assert len(raw.encode()) > 64 * 1024, 'not enough output to exercise the pipe buffer'

            result = zfs.get(parent, properties='all', recursive=True)

            assert sorted(o.name for o in result) == sorted(names)
            assert all(isinstance(o, Filesystem) for o in result)
            assert all(o['compression'].value for o in result)
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


# ---------------------------------------------------------------------------------------------------------------------
# B1-B3: framing safety against the real `zfs list -H` / `zfs get -H`
# ---------------------------------------------------------------------------------------------------------------------

def _raw_zfs(*args, stdin=None):
    """Run the real `zfs` directly (bypassing libzfseasy, which refuses the values these tests need to plant).
    Falls back to `sudo -n` when delegation does not cover the subcommand (e.g. `send` on a delegated pool).
    Returns the CompletedProcess; the caller decides what a failure means."""
    argv = [os.fsencode(a) for a in (_zfs_cmd(),) + args]
    result = subprocess.run(argv, input=stdin, capture_output=True)
    if result.returncode != 0 and b'permission denied' in result.stderr.lower() and shutil.which('sudo'):
        result = subprocess.run([b'sudo', b'-n'] + argv, input=stdin, capture_output=True)
    return result


def _raw_zfs_ok(*args, stdin=None):
    result = _raw_zfs(*args, stdin=stdin)
    if result.returncode != 0:
        err = result.stderr.decode('utf-8', 'replace')
        if 'permission denied' in err.lower():
            pytest.skip('this pool user may not run `zfs %s` (delegation/sudo unavailable): %s' % (args[0], err.strip()))
        raise AssertionError('zfs %s failed: %s' % (' '.join(map(str, args[:3])), err))
    return result


@pytest.fixture(scope='class')
def family(test_pool):
    """Create the hostile family; yield its parent name; destroy it."""
    parent = f'{test_pool}/{TestRealFraming.PARENT}'
    zfs.create.filesystem(parent)
    try:
        for name, value in TestRealFraming.LOCAL.items():
            zfs.create.filesystem(f'{parent}/{name}')
            _raw_zfs_ok('set', f'{TestRealFraming.PROP}={value.format(p=parent)}', f'{parent}/{name}')
        # received properties: a `send -p` stream carries the source's local properties as RECEIVED ones
        forged_recv = f'X\n{parent}/victim\tfilesystem\tRECV'
        for src, value in (('src', forged_recv), ('src2', 'r1\tr2')):
            zfs.create.filesystem(f'{parent}/{src}')
            _raw_zfs_ok('set', f'{TestRealFraming.PROP}={value}', f'{parent}/{src}')
            _raw_zfs_ok('snapshot', f'{parent}/{src}@s')
        stream = _raw_zfs_ok('send', '-p', f'{parent}/src@s').stdout
        _raw_zfs_ok('receive', '-u', f'{parent}/recv', stdin=stream)
        stream = _raw_zfs_ok('send', '-p', f'{parent}/src2@s').stdout
        _raw_zfs_ok('receive', '-u', f'{parent}/recv2', stdin=stream)
        _raw_zfs_ok('receive', '-u', f'{parent}/recv3', stdin=stream)
        _raw_zfs_ok('set', f'{TestRealFraming.PROP}=l1\tl2', f'{parent}/recv3')       # local over received
        yield parent
    finally:
        try:
            zfs.destroy(Filesystem(parent), destroy=True, recursive=True)
        except Exception:
            _cleanup_test_datasets(test_pool.name)


class TestRealFraming:
    """B1-B3 against real `zfs`: values holding newlines, tabs, `\\xff`, nothing, trailing spaces, `\\r` and (the attack)
    a forged row -- locally set, and received through `zfs send -p` -- must neither crash the parsers nor produce a
    forged object, and `zfs.get.value` must hand every value back byte for byte.

    The values are planted with the `zfs` binary itself, because libzfseasy refuses to write them. All datasets live
    under one `test_framing` parent that is destroyed (recursively) at the end."""

    PARENT = 'test_framing'
    # name -> value planted locally. `forger` carries a whole forged row for `victim`.
    LOCAL = {
        'victim': 'victimcfg',
        'attacker': 'line1\nline2',
        'forger': 'X\n{p}/victim\tfilesystem\tFORGED',
        'tab': 'a\tb',
        'ff': '\udcff\udcfe',
        'empty': '',
        'trail': 'trail  ',
        'cr': 'cr\rx',
    }
    PROP = 'org.t:v'

    @staticmethod
    def _value(obj, prop='org.t:v'):
        p = obj[prop]
        return None if p is None else p.value

    def _names(self, parent):
        return ['%s/%s' % (parent, n) for n in
                list(self.LOCAL) + ['src', 'src2', 'recv', 'recv2', 'recv3']] + [parent]

    def test_list_recursive_has_no_forged_object_and_keeps_the_framing_safe_values(self, family):
        result = zfs.list(roots=Filesystem(family), recursive=True, properties=[self.PROP])
        by = {o.name: o for o in result}
        assert len(by) == len(result)
        assert set(by) <= set(self._names(family))                       # nothing the pool does not have
        assert all(isinstance(o, Filesystem) for o in result)
        # `victim` is named by a forged row AND exists: nothing says which is real, so it is not trusted
        assert f'{family}/victim' not in by
        # a value with a newline ends its row early and leaves a fragment behind: not trusted
        assert f'{family}/attacker' not in by
        # tabs shift columns: not recoverable in `list`
        assert not {f'{family}/{n}' for n in ('tab', 'recv2', 'recv3', 'src2')} & set(by)
        assert self._value(by[f'{family}/empty']) == ''
        assert self._value(by[f'{family}/trail']) == 'trail  '
        assert self._value(by[f'{family}/cr']) == 'cr\rx'
        assert self._value(by[f'{family}/ff']).encode('utf-8', 'surrogateescape') == b'\xff\xfe'

    def test_lazy_list_never_raises_and_never_returns_a_fragment(self, family):
        result = list(zfs.list(roots=Filesystem(family), recursive=True, properties=[self.PROP], lazy=True))
        names = [o.name for o in result]
        assert set(names) <= set(self._names(family))
        assert f'{family}/attacker' not in names and f'{family}/tab' not in names

    def test_each_dataset_listed_alone(self, family):
        """The `_get_prop` shape: one dataset, one property. Fragments are skipped rather than raised."""
        got = {}
        for name in self.LOCAL:
            result = zfs.list(roots=Filesystem(f'{family}/{name}'), properties=[self.PROP])
            assert len(result) <= 1
            got[name] = None if not result else self._value(result[0])
        assert got['victim'] == 'victimcfg'
        assert got['empty'] == '' and got['trail'] == 'trail  ' and got['cr'] == 'cr\rx'
        assert got['attacker'] is None and got['tab'] is None
        # `forger` listed alone prints its own row, then the forged `victim` row, which is outside its scope
        assert got['forger'] is None

    def test_get_recursive_has_no_forged_object(self, family):
        result = zfs.get(Filesystem(family), properties=[self.PROP], recursive=True)
        by = {o.name: o for o in result}
        assert len(by) == len(result)
        assert set(by) <= set(self._names(family)) | {n for n in by if '@' in n}
        assert f'{family}/victim' not in by
        assert self._value(by[f'{family}/tab']) == 'a\tb'
        assert self._value(by[f'{family}/recv2']) == 'r1\tr2'
        assert by[f'{family}/recv2'][self.PROP].received == 'r1\tr2'
        assert self._value(by[f'{family}/src2']) == 'r1\tr2'
        assert self._value(by[f'{family}/recv3']) is None             # local tab value over a received one: ambiguous
        assert self._value(by[f'{family}/empty']) == ''
        assert self._value(by[f'{family}/trail']) == 'trail  '
        assert self._value(by[f'{family}/cr']) == 'cr\rx'
        assert self._value(by[f'{family}/ff']).encode('utf-8', 'surrogateescape') == b'\xff\xfe'
        assert self._value(by[f'{family}/attacker']) is None          # its rows are fragments

    def test_get_lazy_does_not_raise(self, family):
        result = list(zfs.get(Filesystem(family), properties=[self.PROP], recursive=True, lazy=True))
        assert all(o.name.startswith(family) for o in result)

    def test_get_value_round_trips_every_value_exactly(self, family):
        expected = {
            'victim': 'victimcfg', 'attacker': 'line1\nline2', 'forger': f'X\n{family}/victim\tfilesystem\tFORGED',
            'tab': 'a\tb', 'ff': '\udcff\udcfe', 'empty': '', 'trail': 'trail  ', 'cr': 'cr\rx',
        }
        for name, want in expected.items():
            assert zfs.get.value(Filesystem(f'{family}/{name}'), self.PROP) == want, name
            assert zfs.get.value(f'{family}/{name}', self.PROP, sources='local') == want, name
        assert zfs.get.value(f'{family}/ff', self.PROP).encode('utf-8', 'surrogateescape') == b'\xff\xfe'

    def test_get_value_with_received_properties(self, family):
        assert zfs.get.value(f'{family}/recv', self.PROP) == f'X\n{family}/victim\tfilesystem\tRECV'
        assert zfs.get.value(f'{family}/recv', self.PROP, sources='received') == \
            f'X\n{family}/victim\tfilesystem\tRECV'
        assert zfs.get.value(f'{family}/recv', self.PROP, sources='local') is None        # not set locally
        assert zfs.get.value(f'{family}/recv2', self.PROP) == 'r1\tr2'
        assert zfs.get.value(f'{family}/recv2', self.PROP, sources=['local', 'inherited']) is None
        assert zfs.get.value(f'{family}/recv3', self.PROP) == 'l1\tl2'
        assert zfs.get.value(f'{family}/recv3', self.PROP, sources='local') == 'l1\tl2'
        assert zfs.get.value(f'{family}/src', self.PROP, sources='local') == f'X\n{family}/victim\tfilesystem\tRECV'

    def test_get_value_unset_native_snapshot_and_errors(self, family):
        assert zfs.get.value(f'{family}/victim', 'org.t:never_set') is None
        assert zfs.get.value(f'{family}/victim', 'org.t:never_set', raw=True) == '-'
        assert zfs.get.value(f'{family}/victim', 'compression') in ('on', 'off', 'lz4', 'inherit')
        assert zfs.get.value(Filesystem(f'{family}/victim'), 'type') == 'filesystem'
        # a snapshot inherits the user property of its dataset
        assert zfs.get.value(f'{family}/src@s', self.PROP) == f'X\n{family}/victim\tfilesystem\tRECV'
        assert zfs.get.value(f'{family}/src@s', self.PROP, sources='local') is None
        with pytest.raises(Exception) as info:
            zfs.get.value(f'{family}/does_not_exist', self.PROP)
        assert type(info.value) is Exception and 'does not exist' in str(info.value).lower()

    def test_get_value_refusals_do_not_reach_zfs(self, family):
        for prop in ('all', '-s', 'org.t:v,org.t:w', 'Compression'):
            with pytest.raises(ValueError):
                zfs.get.value(f'{family}/victim', prop)
        with pytest.raises(ValueError):
            zfs.get.value(Filesystem(f'{family}/victim'), 'volsize')

    def test_b1_refusals_leave_the_pool_untouched(self, family):
        def tree():
            return sorted(o.name for o in zfs.list(roots=Filesystem(family), recursive=True, types='all'))

        before = tree()
        victim = Filesystem(f'{family}/victim')
        for bad in ('x\ny', 'x\ty', 'x\ry', 'x\x00y'):
            for attempt in (
                lambda: zfs.set(victim, {self.PROP: bad}),
                lambda: zfs.create.filesystem(f'{family}/b1new', {self.PROP: bad}),
                lambda: zfs.create.volume(f'{family}/b1vol', '8M', {self.PROP: bad}),
                lambda: zfs.snapshot(victim, 'b1snap', properties={self.PROP: bad}),
                lambda: zfs.snapshot(Filesystem(family), 'b1snap', recursive=True, properties={self.PROP: bad}),
                lambda: zfs.clone(Snapshot(Filesystem(f'{family}/src'), 's'), f'{family}/b1clone',
                                  properties={self.PROP: bad}),
            ):
                with pytest.raises(ValueError):
                    attempt()
        assert tree() == before                                         # nothing created, no snapshot taken
        assert zfs.get.value(victim, self.PROP) == 'victimcfg'
        assert not zfs.exists(Filesystem(f'{family}/b1new'))

    def test_a_good_value_still_round_trips_through_libzfseasy(self, family):
        victim = Filesystem(f'{family}/victim')
        value = '  {"k": "v = 1%", "u": "café 中"}  '
        try:
            zfs.set(victim, {self.PROP: value})
            assert zfs.get.value(victim, self.PROP, sources='local') == value
            (listed,) = zfs.list(roots=victim, properties=[self.PROP])
            assert self._value(listed) == value
            (got,) = zfs.get(victim, properties=[self.PROP])
            assert self._value(got) == value
        finally:
            zfs.set(victim, {self.PROP: 'victimcfg'})


# ---------------------------------------------------------------------------------------------------------------------
# Milestone A / B2: the rows `zfs get -H` prints FIRST for every object (the "lead rule" GetCommand relies on)
# ---------------------------------------------------------------------------------------------------------------------

_GET_COLUMNS = 'name,property,value,received,source'


@pytest.fixture(scope='class')
def lead_tree(test_pool):
    """A filesystem with a child, a volume, snapshots of both and a bookmark, under one `test_lead` parent."""
    parent = f'{test_pool}/test_lead'
    fs, child, vol = f'{parent}/fs', f'{parent}/fs/child', f'{parent}/vol'
    zfs.create.filesystem(parent)
    try:
        zfs.create.filesystem(fs)
        zfs.create.filesystem(child)
        zfs.create.volume(vol, '8M')
        _raw_zfs_ok('set', 'org.t:v=hello', fs)
        zfs.snapshot(Filesystem(parent), 's1', recursive=True)
        zfs.bookmark(Snapshot(Filesystem(fs), 's1'), 'bm')
        yield {'parent': parent, 'fs': fs, 'child': child, 'vol': vol, 'snap': f'{fs}@s1', 'bookmark': f'{fs}#bm'}
    finally:
        try:
            _destroy_volume_when_idle(Volume(vol))
        except Exception:
            pass
        try:
            zfs.destroy(Filesystem(parent), destroy=True, recursive=True)
        except Exception:
            _cleanup_test_datasets(test_pool.name)


def _get_groups(*args):
    """Run the real `zfs get -H -o name,property,value,received,source <args>` and return the rows as
    `[(name, [(property, value, received, source), ...]), ...]`: one entry per run of consecutive rows of one name."""
    out = _raw_zfs_ok('get', '-H', '-o', _GET_COLUMNS, *args).stdout.decode('utf-8', 'surrogateescape')
    groups = []
    for line in out.split('\n'):
        if not line:
            continue
        name, *rest = line.split('\t')
        assert len(rest) == 4, line
        if not groups or groups[-1][0] != name:
            groups.append((name, []))
        groups[-1][1].append(tuple(rest))
    return groups


class TestRealGetLeadRows:
    """`GetCommand` drops an object whose first rows are not the ones zfs prints first (`type` for `all`, the listed
    order for an explicit list), because a forged row run merged into the NEXT object's group would otherwise go
    unseen. That rule was derived from the OpenZFS sources; this checks it against the real binary (2.1 / 2.2 / 2.4)."""

    def test_all_opens_every_group_with_type_and_has_no_name_row(self, lead_tree):
        for kind in ('parent', 'fs', 'vol', 'snap', 'bookmark'):
            (group,) = _get_groups('all', lead_tree[kind])
            name, rows = group
            assert name == lead_tree[kind]
            assert rows[0][0] == 'type', (kind, rows[:3])
            assert rows[0][1] == {'parent': 'filesystem', 'fs': 'filesystem', 'vol': 'volume',
                                  'snap': 'snapshot', 'bookmark': 'bookmark'}[kind]
            assert 'name' not in [r[0] for r in rows], kind

    def test_all_recursive_opens_every_object_with_type(self, lead_tree):
        groups = _get_groups('-r', '-t', 'all', 'all', lead_tree['parent'])
        names = [g[0] for g in groups]
        assert len(set(names)) == len(names), 'a name is not contiguous'
        for expected in ('parent', 'fs', 'child', 'vol', 'snap', 'bookmark'):
            assert lead_tree[expected] in names
        assert any(n.endswith('/vol@s1') for n in names) and any(n.endswith('/child@s1') for n in names)
        for name, rows in groups:
            assert rows[0][0] == 'type', (name, rows[:2])
            assert 'name' not in [r[0] for r in rows], name

    def test_all_recursive_with_depth_opens_every_object_with_type(self, lead_tree):
        groups = _get_groups('-d', '1', '-t', 'all', 'all', lead_tree['fs'])
        assert {g[0] for g in groups} >= {lead_tree['fs'], lead_tree['snap'], lead_tree['bookmark'], lead_tree['child']}
        assert all(rows[0][0] == 'type' and 'name' not in [r[0] for r in rows] for _, rows in groups)

    def test_explicit_list_opens_every_group_with_name_then_type(self, lead_tree):
        props = 'name,type,compression,used,org.t:v'
        groups = _get_groups('-r', '-t', 'all', props, lead_tree['parent'])
        assert len(groups) >= 8
        for name, rows in groups:
            assert [r[0] for r in rows] == props.split(','), name
            assert rows[0][1] == name                                  # the `name` row repeats the name column
        by = dict(groups)
        assert by[lead_tree['bookmark']][1][1] == 'bookmark'
        assert by[lead_tree['vol']][1][1] == 'volume'

    def test_explicit_list_follows_the_requested_order_so_the_caller_must_prepend_name_and_type(self, lead_tree):
        (group,) = _get_groups('used,type,name', lead_tree['fs'])
        assert [r[0] for r in group[1]] == ['used', 'type', 'name']
        (group,) = _get_groups('type,name', lead_tree['fs'])
        assert [r[0] for r in group[1]] == ['type', 'name']

    def test_source_filter_drops_the_type_row_so_the_filter_stays_client_side(self, lead_tree):
        """`-s local` would remove the `type` row (and with it the lead): that is why GetCommand never passes `-s`."""
        (group,) = _get_groups('-s', 'local', 'all', lead_tree['fs'])
        assert 'type' not in [r[0] for r in group[1]]

    @pytest.mark.parametrize('sources', [None, 'all', 'local', 'default', 'inherited', 'temporary', 'received',
                                         ['local', 'inherited'], ['default', 'received']])
    @pytest.mark.parametrize('properties', ['all', ['compression', 'org.t:v'], ['type'], ['used', 'type', 'used']])
    def test_get_returns_every_object_for_both_forms_and_every_sources_value(self, lead_tree, properties, sources):
        parent = Filesystem(lead_tree['parent'])
        listed = sorted(o.name for o in zfs.list(roots=parent, recursive=True, types='all'))
        assert len(listed) >= 8
        kwargs = {} if sources is None else {'sources': sources}
        result = zfs.get(parent, properties=properties, recursive=True, types='all', **kwargs)
        assert sorted(o.name for o in result) == listed
        classes = {o.name: type(o) for o in result}
        assert classes[lead_tree['fs']] is Filesystem and classes[lead_tree['vol']] is Volume
        assert classes[lead_tree['snap']] is Snapshot and classes[lead_tree['bookmark']] is Bookmark
        lazy = sorted(o.name for o in zfs.get(parent, properties=properties, recursive=True, types='all',
                                              lazy=True, **kwargs))
        assert lazy == listed

    def test_get_properties_are_kept_for_the_matching_sources(self, lead_tree):
        fs = Filesystem(lead_tree['fs'])
        (local,) = zfs.get(fs, properties=['org.t:v'], sources='local')
        assert local['org.t:v'].value == 'hello' and local['org.t:v'].source == 'local'
        (dflt,) = zfs.get(fs, properties=['org.t:v'], sources='default')
        assert dflt['org.t:v'] is None
        (child,) = zfs.get(Filesystem(lead_tree['child']), properties=['org.t:v'], sources='inherited')
        assert child['org.t:v'].value == 'hello'
        (everything,) = zfs.get(fs, properties='all', sources='local')
        assert everything['org.t:v'].value == 'hello'

    def test_get_all_without_a_name_row_still_yields_each_kind_alone_and_by_type(self, lead_tree):
        from libzfseasy.types import ZFS
        for kind, cls in (('fs', Filesystem), ('vol', Volume), ('snap', Snapshot), ('bookmark', Bookmark)):
            (obj,) = zfs.get(ZFS.from_name(lead_tree[kind]), properties='all')
            assert type(obj) is cls and obj.name == lead_tree[kind]
        only_snaps = zfs.get(Filesystem(lead_tree['parent']), properties=['used'], recursive=True, types='snapshot')
        assert only_snaps and all(isinstance(o, Snapshot) for o in only_snaps)

    def test_get_type_alone_returns_the_objects(self, lead_tree):
        """`properties=['type']` is sent as `name,type` (de-duplicated): every object still comes back."""
        result = zfs.get(Filesystem(lead_tree['parent']), properties=['type'], recursive=True, types='all')
        assert len(result) >= 8
        assert {type(o) for o in result} == {Filesystem, Volume, Snapshot, Bookmark}

