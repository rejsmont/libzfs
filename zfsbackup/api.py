"""Flask API for inspecting daemon configuration and dataset snapshots."""

from flask import Flask, jsonify

from zfsbackup.config import BackupConfig
from zfsbackup.backup_manager import DatasetManager


def create_app(config: BackupConfig) -> Flask:
    app = Flask(__name__)
    app.config['PROPAGATE_EXCEPTIONS'] = True
    manager = DatasetManager(config)

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

    return app
