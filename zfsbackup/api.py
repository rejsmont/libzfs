"""Flask API for inspecting daemon configuration and dataset snapshots."""

import contextlib
import functools
import json
import logging
import threading
import time
from typing import Callable, Optional

from flask import Flask, jsonify, request
from flask_sock import Sock
from simple_websocket import ConnectionClosed

import libzfseasy as zfs
from libzfseasy.types import Dataset, Filesystem

from zfsbackup.backup_manager import (
    DatasetManager,
    PROP_CONFIG, PROP_CLIENT_ID, PROP_SOURCE_DATASET,
    read_zfsbackup_properties,
)
from zfsbackup.config import BackupConfig
from zfsbackup.remote import WS_IDLE_TIMEOUT

logger = logging.getLogger(__name__)

#: Longest `StreamGate.wait_idle` sleeps before re-checking its abort callback.
_GATE_POLL = 0.5

#: The refusal body for `negotiate`/`stream` while the API worker is draining
#: for a configuration reload (HTTP 503). `retry` tells a client this is
#: transient: the replacement worker is up within moments of the drain ending.
RELOADING_ERROR = {'error': 'server is reloading; retry later', 'retry': True}

#: The values of `/stream`'s required `?mode=` query parameter. `fresh`: the
#: client is sending a new stream and any partial receive state on the server
#: dataset is discarded first. `resume`: the client is continuing the transfer
#: the server's resume token describes; nothing is discarded.
STREAM_MODES = ('fresh', 'resume')

#: The refusal body for `stream` while another stream (or its abort) for the
#: SAME server dataset is running in this API worker. `retry` marks it as a
#: transient skip for the client, exactly like `RELOADING_ERROR`: never a
#: reason to fall back to a fresh send.
BUSY_ERROR = {'error': 'busy', 'retry': True}

#: Substrings of `zfs receive -A`'s stderr that mean "nothing to abort": rc 1
#: "'<ds>' does not have any resumable receive state to abort"; rc 2 "cannot
#: open '<ds>': dataset does not exist". In this flow the server dataset
#: always exists first (negotiate or `_receive_stream` creates it), so `-A`
#: destroys only the saved `<ds>/%recv` state and rc 1 is the normal "already
#: clean". rc 2 only arises for a dataset that a FULL receive created
#: outside this flow (a legacy or manual one), which the abort itself
#: destroys.
_ABORT_ALREADY_CLEAN = (
    'does not have any resumable receive state',
    'dataset does not exist',
)


class StreamGate:
    """Counts in-flight inbound streams and refuses new ones while draining.

    The API worker hosts the server side of `zfs receive -s`; cycling it in the
    middle of a receive would tear an inbound stream. On a reload the worker
    calls `start_draining()` (new streams are refused with a retryable 503,
    read-only endpoints keep serving) and then `wait_idle()`s for the streams
    already running to finish before it exits.
    """

    def __init__(self) -> None:
        self._cond = threading.Condition()
        self._active = 0
        self._draining = False

    def begin(self) -> bool:
        """Register a stream; `False` (nothing registered) while draining."""
        with self._cond:
            if self._draining:
                return False
            self._active += 1
            return True

    def end(self) -> None:
        with self._cond:
            if self._active > 0:
                self._active -= 1
            self._cond.notify_all()

    def start_draining(self) -> None:
        with self._cond:
            self._draining = True
            self._cond.notify_all()

    @property
    def draining(self) -> bool:
        with self._cond:
            return self._draining

    @property
    def active(self) -> int:
        with self._cond:
            return self._active

    def wait_idle(
        self, timeout: Optional[float], abort: Optional[Callable[[], bool]] = None
    ) -> bool:
        """Wait until no stream is active -> `True`; `False` on `timeout`
        (`None` waits indefinitely) or when `abort()` (optional) turns true. Sleeps in
        slices of at most 0.5s so `abort` is re-evaluated promptly (it is
        called WITHOUT the gate's lock held)."""
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            with self._cond:
                if self._active == 0:
                    return True
                remaining = _GATE_POLL if deadline is None else min(
                    _GATE_POLL, deadline - time.monotonic()
                )
                if remaining > 0:
                    self._cond.wait(timeout=remaining)
                    if self._active == 0:
                        return True
            if deadline is not None and time.monotonic() >= deadline:
                return False
            if abort is not None and abort():
                return False


def create_app(config: BackupConfig) -> Flask:
    app = Flask(__name__)
    gate = StreamGate()
    app.extensions['zfsbackup.stream_gate'] = gate
    app.config['PROPAGATE_EXCEPTIONS'] = True
    sock = Sock(app)
    manager = DatasetManager(config)
    #: The server datasets a `/stream` is working on right now, claimed
    #: non-blocking around its whole check -> abort -> create -> receive
    #: sequence: two streams for the same dataset (a client retry after a
    #: timeout, say) must not race one client's abort against the other's
    #: receive. A name is in the set exactly while its stream runs, so the
    #: set never grows beyond the number of concurrent streams.
    streams_in_progress: set = set()
    streams_guard = threading.Lock()

    def _claim_dataset(name: str) -> bool:
        with streams_guard:
            if name in streams_in_progress:
                return False
            streams_in_progress.add(name)
            return True

    def _release_dataset(name: str) -> None:
        with streams_guard:
            streams_in_progress.discard(name)

    remote_backup = config.remote_backup
    enabled = bool(remote_backup and remote_backup.enabled)
    target = remote_backup.target_dataset if remote_backup else None

    def _gated(view):
        """Count `view` as an in-flight remote operation. While the worker
        drains for a reload, new ones are refused with the retryable 503, and
        a RELOAD drain waits for the ones running -- so negotiate/register/
        resume_token (which run `zfs` and create datasets) are not cut off
        mid-call, and a refused negotiate cannot be mistaken by the client
        for "no common snapshot" (-> a needless full send)."""
        @functools.wraps(view)
        def wrapper(*args, **kwargs):
            disabled = _require_enabled()  # a disabled server says so, always
            if disabled is not None:
                return disabled
            if not gate.begin():
                return jsonify(RELOADING_ERROR), 503
            try:
                return view(*args, **kwargs)
            finally:
                gate.end()
        return wrapper

    def _require_enabled():
        if enabled and target:
            return None
        return jsonify({'error': 'remote_backup disabled'}), 503

    def _server_dataset(client_id: str, client_dataset: str) -> str:
        """Derive the server-side dataset path from the client's dataset name."""
        parts = client_dataset.split('/', 1)
        relative = parts[1] if len(parts) > 1 else ''
        return f"{target}/{client_id}/{relative}" if relative else f"{target}/{client_id}"

    def _resume_token(server_dataset: str) -> Optional[str]:
        """The `receive_resume_token` of the server dataset -- a filesystem
        or a volume, found by name alone -- or `None` (no dataset, or no
        token). Raises on any other failure: callers must not guess."""
        ds = Dataset(server_dataset)
        if not zfs.exists(ds):
            return None
        results = zfs.list(roots=ds, properties=['receive_resume_token'])
        if not results:
            return None
        prop = results[0]['receive_resume_token']
        if prop is None:
            return None
        val = str(prop)
        return val if val != 'none' else None

    def _source_conflict(server_dataset: str, dataset: str) -> Optional[str]:
        """An error message when `server_dataset` exists and records a
        different client dataset as its source, else `None`. `_server_dataset`
        drops the pool, so `tank/data` and `backup/data` map to the same
        server dataset and would otherwise silently share (and overwrite) it.
        A dataset without the property (never negotiated) is not a conflict.
        Raises if the dataset's properties cannot be read."""
        ds = Dataset(server_dataset)
        if not zfs.exists(ds):
            return None
        recorded = read_zfsbackup_properties(ds).get(PROP_SOURCE_DATASET)
        if recorded and recorded != dataset:
            return (
                f"server dataset {server_dataset} already holds backups of "
                f"{recorded!r}; refusing to mix in {dataset!r}"
            )
        return None

    @app.route('/health')
    def health():
        return jsonify({'status': 'ok'})

    @app.route('/config')
    def get_config():
        return jsonify({
            'snapshot_prefix': config.snapshot_prefix,
            'check_interval_seconds': config.check_interval.total_seconds(),
            'prune_interval_seconds': config.effective_prune_interval.total_seconds(),
            'api_host': config.api_host,
            'api_port': config.api_port,
            'dry_run': config.dry_run,
        })

    @app.route('/datasets')
    def get_datasets():
        return jsonify([
            {
                'name': dsi.name,
                'frequency_seconds': dsi.frequency.total_seconds(),
                'recursive': dsi.recursive,
                'retention_rules': [
                    {
                        'age_seconds': r.age.total_seconds(),
                        'keep_for_seconds': r.keep_for.total_seconds(),
                    }
                    for r in dsi.config.retention_rules
                ],
            }
            for dsi in manager.datasets
        ])

    @app.route('/datasets/<path:name>/snapshots')
    def get_snapshots(name: str):
        dsi = next((d for d in manager.datasets if d.name == name), None)
        if dsi is None:
            return jsonify({'error': f'Dataset {name!r} not configured'}), 404
        snapshots = manager.list_snapshots(dsi)
        return jsonify([
            {
                'name': s.name,
                'full_name': s.full_name,
                'timestamp': s.timestamp.isoformat() if s.timestamp else None,
                'age_seconds': s.age.total_seconds(),
            }
            for s in snapshots
        ])

    @app.route('/backup/register', methods=['POST'])
    @_gated
    def backup_register():
        disabled = _require_enabled()
        if disabled is not None:
            return disabled
        data = request.get_json(force=True)
        client_id = data.get('client_id')
        if not client_id:
            return jsonify({'error': 'client_id required'}), 400

        client_root = f"{target}/{client_id}"
        if not zfs.exists(Dataset(client_root)):
            if config.dry_run:
                return jsonify({'target_dataset': client_root, 'dry_run': True})
            zfs.create(Filesystem(client_root), parents=True)

        return jsonify({'target_dataset': client_root})

    @app.route('/backup/<client_id>/<path:dataset>/negotiate', methods=['POST'])
    @_gated
    def backup_negotiate(client_id: str, dataset: str):
        disabled = _require_enabled()
        if disabled is not None:
            return disabled
        data = request.get_json(force=True)
        client_snapshots: list = data.get('snapshots', [])
        ds_config_encoded: str = data.get('config', '')

        server_dataset = _server_dataset(client_id, dataset)
        server_fs = Filesystem(server_dataset)
        server_fs_exists = zfs.exists(server_fs)

        # D7: another client dataset already owns this server dataset. Checked
        # before anything is written, so its recorded source is not overwritten.
        if server_fs_exists:
            try:
                conflict = _source_conflict(server_dataset, dataset)
            except Exception as e:
                return jsonify({'error': f'Failed to check {server_dataset}: {e}'}), 500
            if conflict is not None:
                return jsonify({'error': conflict}), 409

        # Store client config on server dataset (create dataset if needed)
        if ds_config_encoded:
            if not server_fs_exists:
                if not config.dry_run:
                    zfs.create(server_fs, parents=True)
                    server_fs_exists = True
            if not config.dry_run:
                zfs.set(server_fs, {
                    PROP_CONFIG: ds_config_encoded,
                    PROP_CLIENT_ID: client_id,
                    PROP_SOURCE_DATASET: dataset,
                })

        # Find the most recent snapshot present on both sides
        common_snapshot = None
        if server_fs_exists:
            try:
                server_snaps = zfs.list(roots=server_fs, types='snapshot', properties=['creation'])
                server_names = {s.short for s in server_snaps}
                for snap_name in client_snapshots:
                    if snap_name in server_names:
                        common_snapshot = snap_name
                        break
            except Exception as e:
                return jsonify({'error': f'Failed to list server snapshots: {e}'}), 500

        return jsonify({
            'common_snapshot': common_snapshot,
            'server_dataset': server_dataset,
        })

    @sock.route('/backup/<client_id>/<path:dataset>/stream')
    def backup_stream(ws, client_id: str, dataset: str):
        if not (enabled and target):
            try:
                ws.send(json.dumps({'error': 'remote_backup disabled'}))
            except Exception:
                pass
            return
        mode = request.args.get('mode')
        if mode not in STREAM_MODES:
            problem = (
                "stream mode required: use ?mode=fresh or ?mode=resume" if mode is None
                else f"unknown stream mode {mode!r}: use ?mode=fresh or ?mode=resume"
            )
            try:
                ws.send(json.dumps({'error': problem}))
            except Exception:
                pass
            return
        if not gate.begin():
            try:
                ws.send(json.dumps(RELOADING_ERROR))
            except Exception:
                pass
            return
        try:
            # `?from=` (the incremental base the client negotiated) is
            # informational only: `zfs receive` takes the base from the stream.
            logger.debug(
                "stream for %s/%s: mode=%s from=%s",
                client_id, dataset, mode, request.args.get('from'),
            )
            _receive_stream(ws, client_id, dataset, mode)
        finally:
            gate.end()

    def _receive_stream(ws, client_id: str, dataset: str, mode: str) -> None:
        server_dataset = _server_dataset(client_id, dataset)
        # Validate the name before it becomes a key of anything.
        try:
            Filesystem(server_dataset)
        except Exception as e:
            try:
                ws.send(json.dumps({'error': f'Invalid dataset name {server_dataset!r}: {e}'}))
            except Exception:
                pass
            return
        if not _claim_dataset(server_dataset):
            try:
                ws.send(json.dumps(BUSY_ERROR))
            except Exception:
                pass
            return
        try:
            _receive_stream_locked(ws, client_id, dataset, mode)
        finally:
            _release_dataset(server_dataset)

    def _receive_stream_locked(ws, client_id: str, dataset: str, mode: str) -> None:
        server_dataset = _server_dataset(client_id, dataset)
        server_fs = Filesystem(server_dataset)

        # D7: refuse to receive into a server dataset another client dataset
        # owns (read-only, so also in a dry run).
        try:
            conflict = _source_conflict(server_dataset, dataset)
        except Exception as e:
            ws.send(json.dumps({'error': f'Failed to check {server_dataset}: {e}'}))
            return
        if conflict is not None:
            ws.send(json.dumps({'error': conflict}))
            return

        if config.dry_run:
            ws.send(json.dumps({'status': 'ok', 'dry_run': True}))
            return

        if mode == 'fresh':
            # D3: a fresh stream cannot continue a partial receive. Discard
            # its saved state first, or `zfs receive` refuses with "contains
            # partially-complete state". Here the dataset always exists (the
            # D7 check above and negotiate see to that), so `receive -A`
            # destroys only its `%recv` state; the recreate below is for a
            # dataset a FULL receive created outside this flow, which the
            # abort destroys whole -- nothing before it may assume the
            # dataset still exists. `resume` never reaches this.
            try:
                token = _resume_token(server_dataset)
            except Exception as e:
                ws.send(json.dumps({
                    'error': f'Failed to check {server_dataset} for partial receive state: {e}'
                }))
                return
            if token:
                logger.warning(
                    "discarding the partial receive state of %s: the client "
                    "is sending a fresh stream", server_dataset,
                )
                try:
                    zfs.receive.abort(Dataset(server_dataset))
                except Exception as e:
                    # Idempotent: already clean is fine, anything else refuses.
                    if not any(m in str(e).lower() for m in _ABORT_ALREADY_CLEAN):
                        logger.error(
                            "cannot discard the partial receive state of %s: %s",
                            server_dataset, e,
                        )
                        ws.send(json.dumps({
                            'error': (
                                f'Failed to discard the partial receive state of '
                                f'{server_dataset}: {e}'
                            )
                        }))
                        return

        if not zfs.exists(server_fs):
            # Recreated (or created here): stamp the ownership properties
            # again, or a dataset without `source_dataset` loses the D7 check
            # and one without `client_id` is not found as this client's.
            try:
                zfs.create(server_fs, parents=True)
                zfs.set(server_fs, {
                    PROP_CLIENT_ID: client_id,
                    PROP_SOURCE_DATASET: dataset,
                })
            except Exception as e:
                ws.send(json.dumps({
                    'error': f'Failed to create {server_dataset}: {e}'
                }))
                return

        # zfs.receive.filesystem() returns a StreamHandle wrapping the `zfs
        # receive` process's stdin. A startup failure raises here; it never
        # returns None.
        try:
            writer = zfs.receive.filesystem(server_fs, force=True, save=True, mount=False)
        except Exception as e:
            ws.send(json.dumps({'error': f'Failed to start zfs receive: {e}'}))
            return

        reaped = False  # `writer.close()` already called on the write-error path
        try:
            while True:
                data = ws.receive(timeout=WS_IDLE_TIMEOUT)
                if data is None:
                    raise TimeoutError(
                        f"no data from the client for {WS_IDLE_TIMEOUT}s"
                    )
                if isinstance(data, str):
                    msg = json.loads(data)
                    if msg.get('done'):
                        break
                else:
                    try:
                        writer.write(data)
                    except OSError as pipe_error:
                        # `zfs receive` stopped reading (typically it has
                        # already exited, e.g. a checksum mismatch): the pipe
                        # error is only the symptom. `close()` reaps the
                        # process and raises zfs's own diagnostic; report
                        # that to the client and the log. If it raises
                        # nothing, the pipe error is all there is.
                        reaped = True
                        try:
                            writer.close()
                        except Exception as receive_error:
                            logger.error(
                                "zfs receive for %s failed: %s (stream write: %s)",
                                server_dataset, receive_error, pipe_error,
                            )
                            raise receive_error from pipe_error
                        logger.error(
                            "writing to zfs receive for %s failed: %s",
                            server_dataset, pipe_error,
                        )
                        raise
            # writer.close() (StreamHandle.__exit__/close()) waits on the
            # `zfs receive` subprocess and raises if it exited non-zero. That
            # must happen only after all chunks have been written above (done
            # here); if it raises, we fall through to the `except Exception`
            # branch below, which reports the error to the client instead of
            # sending 'status: ok'.
            reaped = True  # the handler below must not close a second time
            writer.close()
            ws.send(json.dumps({'status': 'ok'}))
        except ConnectionClosed:
            # Client already disconnected; best-effort reap the `zfs
            # receive` process. Any error it raises here can't be reported
            # back (the socket is gone) so it's intentionally suppressed.
            if not reaped:
                with contextlib.suppress(Exception):
                    writer.close()
        except Exception as e:
            if not reaped:
                with contextlib.suppress(Exception):
                    writer.close()
            with contextlib.suppress(Exception):
                ws.send(json.dumps({'error': str(e)}))

    @app.route('/backup/<client_id>/<path:dataset>/resume_token')
    @_gated
    def backup_resume_token(client_id: str, dataset: str):
        disabled = _require_enabled()
        if disabled is not None:
            return disabled
        server_dataset = _server_dataset(client_id, dataset)
        try:
            return jsonify({'resume_token': _resume_token(server_dataset)})
        except Exception as e:
            return jsonify({'error': str(e)}), 500


    return app
