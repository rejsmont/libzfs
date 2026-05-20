"""Client-side remote backup: identity management and ZFS stream transfer."""

import json
import logging
import re
import socket
import uuid
from pathlib import Path
from typing import Optional

import requests
import websocket

import libzfseasy as zfs
from zfsbackup.backup_manager import DatasetInfo, DatasetManager
from zfsbackup.config import BackupConfig, Destination, RemoteDatasetConfig


logger = logging.getLogger(__name__)


class ClientIdentity:
    """Persistent client identity in the form <hostname>-<uuid8>."""

    def __init__(self, id_file: Path):
        self.id_file = id_file
        self._client_id: Optional[str] = None

    @property
    def client_id(self) -> str:
        if self._client_id is None:
            self._client_id = self._load_or_generate()
        return self._client_id

    def _load_or_generate(self) -> str:
        if self.id_file.exists():
            value = self.id_file.read_text().strip()
            if value:
                return value
        client_id = self._generate()
        self.id_file.parent.mkdir(parents=True, exist_ok=True)
        self.id_file.write_text(client_id)
        logger.info(f"Generated new client ID: {client_id} (saved to {self.id_file})")
        return client_id

    @staticmethod
    def _generate() -> str:
        hostname = socket.gethostname()
        sanitized = re.sub(r'[^a-zA-Z0-9._-]', '-', hostname)
        return f"{sanitized}-{uuid.uuid4().hex[:8]}"


class RemoteBackupManager:
    """Negotiates and transfers ZFS snapshots to configured remote destinations."""

    def __init__(self, config: BackupConfig, local_manager: DatasetManager):
        self.config = config
        self.local_manager = local_manager
        self.identity = ClientIdentity(config.client_id_file)

    def backup_dataset(self, dsi: DatasetInfo, remote_cfg: RemoteDatasetConfig) -> bool:
        """Send a backup of dsi to the configured destination. Returns True on success."""
        dest = self.config.destinations.get(remote_cfg.destination)
        if dest is None:
            logger.error(f"Unknown destination '{remote_cfg.destination}' for {dsi.name}")
            return False

        base_url = dest.url.rstrip('/')
        client_id = self.identity.client_id

        if not self._register(base_url, client_id):
            return False

        snapshots = self.local_manager.list_snapshots(dsi)
        if not snapshots:
            logger.debug(f"No snapshots available to send for {dsi.name}")
            return True

        latest = snapshots[0]
        common_snap_name = self._negotiate(base_url, client_id, dsi, snapshots)

        # Resolve the common snapshot object for incremental send
        common_snap = None
        if common_snap_name:
            common_snap = next((s for s in snapshots if s.name == common_snap_name), None)

        return self._transfer(base_url, client_id, dsi, latest, common_snap, remote_cfg.destination)

    def _register(self, base_url: str, client_id: str) -> bool:
        try:
            resp = requests.post(
                f"{base_url}/backup/register",
                json={'client_id': client_id, 'hostname': socket.gethostname()},
                timeout=30,
            )
            resp.raise_for_status()
            return True
        except Exception as e:
            logger.error(f"Registration failed at {base_url}: {e}")
            return False

    def _negotiate(self, base_url: str, client_id: str, dsi: DatasetInfo, snapshots) -> Optional[str]:
        try:
            resp = requests.post(
                f"{base_url}/backup/{client_id}/{dsi.name}/negotiate",
                json={
                    'snapshots': [s.name for s in snapshots],
                    'config': dsi.config.to_property(),
                },
                timeout=30,
            )
            resp.raise_for_status()
            return resp.json().get('common_snapshot')
        except Exception as e:
            logger.error(f"Negotiation failed for {dsi.name} at {base_url}: {e}")
            return None

    def _transfer(
        self,
        base_url: str,
        client_id: str,
        dsi: DatasetInfo,
        latest,
        common_snap,
        destination: str,
    ) -> bool:
        stream = self._get_resume_stream(base_url, client_id, dsi, latest, common_snap)
        if stream is None:
            return False

        if self.config.dry_run:
            logger.info(
                f"[DRY RUN] Would send {dsi.name}@{latest.name}"
                + (f" (incremental from {common_snap.name})" if common_snap else " (full)")
                + f" -> {base_url}"
            )
            return True

        ws_base = base_url.replace('https://', 'wss://', 1).replace('http://', 'ws://', 1)
        ws_url = f"{ws_base}/backup/{client_id}/{dsi.name}/stream"
        if common_snap:
            ws_url += f"?from={common_snap.name}"

        ws = websocket.WebSocket()
        try:
            ws.connect(ws_url)
            chunk_size = 256 * 1024
            while True:
                chunk = stream.read(chunk_size)
                if not chunk:
                    break
                ws.send_binary(chunk)
            ws.send(json.dumps({'done': True}))
            result = json.loads(ws.recv())
            if result.get('status') != 'ok':
                logger.error(
                    f"Transfer failed for {dsi.name}: {result.get('error', 'unknown error')}"
                )
                return False
        except Exception as e:
            logger.error(f"Transfer failed for {dsi.name} -> {base_url}: {e}")
            return False
        finally:
            ws.close()

        old_anchor = self.local_manager.get_anchor(dsi, destination)
        self.local_manager.set_anchor(dsi, destination, latest.name)
        if old_anchor and old_anchor != latest.name:
            logger.debug(f"Previous anchor {old_anchor} superseded by {latest.name}")

        logger.info(
            f"Backup complete: {dsi.name}@{latest.name}"
            + (f" (incremental from {common_snap.name})" if common_snap else " (full)")
            + f" -> {base_url}"
        )
        return True

    def _get_resume_stream(self, base_url, client_id, dsi, latest, common_snap):
        """Try to resume an interrupted transfer; fall back to a fresh send."""
        try:
            resp = requests.get(
                f"{base_url}/backup/{client_id}/{dsi.name}/resume_token",
                timeout=10,
            )
            if resp.ok:
                token = resp.json().get('resume_token')
                if token:
                    logger.info(f"Resuming interrupted transfer for {dsi.name}")
                    return zfs.send.resume(token)
        except Exception:
            pass  # resume check is best-effort

        try:
            if common_snap:
                logger.info(
                    f"Sending incremental {dsi.name}: {common_snap.name} -> {latest.name}"
                )
                return zfs.send.snapshot(latest.snapshot, since=common_snap.snapshot)
            else:
                logger.info(f"Sending full snapshot {dsi.name}@{latest.name}")
                return zfs.send.snapshot(latest.snapshot)
        except Exception as e:
            logger.error(f"Failed to start zfs send for {dsi.name}: {e}")
            return None
