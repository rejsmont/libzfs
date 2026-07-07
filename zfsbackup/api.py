"""Flask API for inspecting daemon configuration and dataset snapshots."""

import contextlib
import json

from flask import Flask, jsonify, request
from flask_sock import Sock
from simple_websocket import ConnectionClosed

import libzfseasy as zfs
from libzfseasy.types import Dataset, Filesystem

from zfsbackup.backup_manager import (
    DatasetManager,
    PROP_CONFIG, PROP_CLIENT_ID, PROP_SOURCE_DATASET,
)
from zfsbackup.config import BackupConfig


def create_app(config: BackupConfig) -> Flask:
    app = Flask(__name__)
    app.config['PROPAGATE_EXCEPTIONS'] = True
    sock = Sock(app)
    manager = DatasetManager(config)

    remote_backup = config.remote_backup
    enabled = bool(remote_backup and remote_backup.enabled)
    target = remote_backup.target_dataset if remote_backup else None

    def _require_enabled():
        if enabled and target:
            return None
        return jsonify({'error': 'remote_backup disabled'}), 503

    def _server_dataset(client_id: str, client_dataset: str) -> str:
        """Derive the server-side dataset path from the client's dataset name."""
        parts = client_dataset.split('/', 1)
        relative = parts[1] if len(parts) > 1 else ''
        return f"{target}/{client_id}/{relative}" if relative else f"{target}/{client_id}"

    @app.route('/health')
    def health():
        return jsonify({'status': 'ok'})

    @app.route('/config')
    def get_config():
        return jsonify({
            'snapshot_prefix': config.snapshot_prefix,
            'check_interval_seconds': config.check_interval.total_seconds(),
            'prune_interval_seconds': config.prune_interval.total_seconds(),
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
        server_dataset = _server_dataset(client_id, dataset)
        server_fs = Filesystem(server_dataset)

        if not zfs.exists(server_fs):
            if not config.dry_run:
                zfs.create(server_fs, parents=True)

        if config.dry_run:
            ws.send(json.dumps({'status': 'ok', 'dry_run': True}))
            return

        # zfs.receive.filesystem() returns a StreamHandle wrapping the `zfs
        # receive` process's stdin. Startup failures now raise immediately
        # instead of returning None; the `is None` check below is kept for
        # defensive/backwards compatibility but real callers should expect
        # an exception here.
        try:
            writer = zfs.receive.filesystem(server_fs, force=True, save=True, mount=False)
        except Exception as e:
            ws.send(json.dumps({'error': f'Failed to start zfs receive: {e}'}))
            return
        if writer is None:
            ws.send(json.dumps({'error': 'Failed to start zfs receive'}))
            return

        try:
            while True:
                data = ws.receive()
                if isinstance(data, str):
                    msg = json.loads(data)
                    if msg.get('done'):
                        break
                else:
                    writer.write(data)
            # writer.close() (StreamHandle.__exit__/close()) waits on the
            # `zfs receive` subprocess and raises if it exited non-zero. That
            # must happen only after all chunks have been written above (done
            # here); if it raises, we fall through to the `except Exception`
            # branch below, which reports the error to the client instead of
            # sending 'status: ok'.
            writer.close()
            ws.send(json.dumps({'status': 'ok'}))
        except ConnectionClosed:
            # Client already disconnected; best-effort reap the `zfs
            # receive` process. Any error it raises here can't be reported
            # back (the socket is gone) so it's intentionally suppressed.
            with contextlib.suppress(Exception):
                writer.close()
        except Exception as e:
            with contextlib.suppress(Exception):
                writer.close()
            with contextlib.suppress(Exception):
                ws.send(json.dumps({'error': str(e)}))

    @app.route('/backup/<client_id>/<path:dataset>/resume_token')
    def backup_resume_token(client_id: str, dataset: str):
        disabled = _require_enabled()
        if disabled is not None:
            return disabled
        server_dataset = _server_dataset(client_id, dataset)
        server_fs = Filesystem(server_dataset)
        if not zfs.exists(server_fs):
            return jsonify({'resume_token': None})
        try:
            results = zfs.list(roots=server_fs, properties=['receive_resume_token'])
            token = None
            if results:
                prop = results[0]['receive_resume_token']
                if prop is not None:
                    val = str(prop)
                    token = val if val != 'none' else None
            return jsonify({'resume_token': token})
        except Exception as e:
            return jsonify({'error': str(e)}), 500


    return app
