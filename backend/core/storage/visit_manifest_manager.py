"""
Visit-Level Manifest Manager for Smart Incremental Sync (Phase 1)
Tracks individual visit processing state to enable efficient incremental updates.
"""
from __future__ import annotations
import json
import logging
from pathlib import Path
from typing import Any, Dict, Optional, Set
from datetime import datetime, timezone
import os

class VisitManifestManager:
    """
    Manages visit-level manifests to track which visits have been processed.
    Path structure: data/processed/{branchId}/{date}/visits/{visitId}.json

    Each visit manifest contains:
    - visitId: str
    - customerId: str
    - processedAt: ISO8601 timestamp
    - updatedAt: ISO8601 timestamp from upstream API
    - imageHashes: Dict[eventId, hash] for detecting image changes
    - status: 'processed' | 'failed'
    - error: Optional error message if status='failed'
    """

    def __init__(self, storage_root: str = "data"):
        self.storage_root = Path(storage_root) / "processed"
        self.logger = logging.getLogger(__name__)

    def _get_visit_manifest_path(self, branch_id: str, date: str, visit_id: str) -> Path:
        """Returns the path to a specific visit manifest file."""
        return self.storage_root / str(branch_id) / str(date) / "visits" / f"{visit_id}.json"

    def _get_visits_dir(self, branch_id: str, date: str) -> Path:
        """Returns the directory containing all visit manifests for a branch/date."""
        return self.storage_root / str(branch_id) / str(date) / "visits"

    def load_visit_manifest(self, branch_id: str, date: str, visit_id: str) -> Optional[Dict[str, Any]]:
        """Load a single visit manifest."""
        manifest_path = self._get_visit_manifest_path(branch_id, date, visit_id)
        if not manifest_path.exists():
            return None

        try:
            with open(manifest_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            self.logger.error(f"Failed to load visit manifest {manifest_path}: {e}")
            return None

    def save_visit_manifest(
        self,
        branch_id: str,
        date: str,
        visit_id: str,
        custom_id: str,
        updated_at: Optional[str],
        image_hashes: Optional[Dict[str, str]] = None,
        status: str = 'processed',
        error: Optional[str] = None
    ) -> bool:
        """Save a visit manifest with atomic write."""
        manifest_path = self._get_visit_manifest_path(branch_id, date, visit_id)
        manifest_path.parent.mkdir(parents=True, exist_ok=True)

        manifest_data = {
            "visitId": str(visit_id),
            "customerId": str(custom_id),
            "processedAt": datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),
            "updatedAt": updated_at,
            "imageHashes": image_hashes or {},
            "status": status,
            "error": error
        }

        try:
            # Atomic write
            tmp_path = manifest_path.parent / f".tmp_{visit_id}.json"
            with open(tmp_path, 'w', encoding='utf-8') as f:
                json.dump(manifest_data, f, ensure_ascii=False, indent=2)
                f.flush()
                os.fsync(f.fileno())

            tmp_path.replace(manifest_path)
            return True
        except Exception as e:
            self.logger.error(f"Failed to save visit manifest {manifest_path}: {e}")
            if tmp_path.exists():
                try:
                    tmp_path.unlink()
                except:
                    pass
            return False

    def get_all_processed_visit_ids(self, branch_id: str, date: str) -> Set[str]:
        """
        Returns a set of all visitIds that have been successfully processed.
        Used for determining which visits are new vs. updates.
        """
        visits_dir = self._get_visits_dir(branch_id, date)
        if not visits_dir.exists():
            return set()

        processed_ids = set()
        try:
            for manifest_file in visits_dir.glob("*.json"):
                if manifest_file.name.startswith(".tmp_"):
                    continue

                try:
                    with open(manifest_file, 'r', encoding='utf-8') as f:
                        data = json.load(f)

                    if data.get("status") == "processed":
                        processed_ids.add(str(data.get("visitId")))
                except Exception as e:
                    self.logger.error(f"Failed to read manifest {manifest_file}: {e}")
                    continue

        except Exception as e:
            self.logger.error(f"Failed to scan visit manifests in {visits_dir}: {e}")

        return processed_ids

    def needs_reprocessing(
        self,
        branch_id: str,
        date: str,
        visit_id: str,
        upstream_updated_at: Optional[str]
    ) -> bool:
        """
        Determines if a visit needs reprocessing based on updatedAt timestamp.
        Returns True if:
        - Visit manifest doesn't exist
        - Manifest status is 'failed'
        - upstream_updated_at is newer than manifest's updatedAt
        """
        manifest = self.load_visit_manifest(branch_id, date, visit_id)

        if manifest is None:
            return True

        if manifest.get("status") == "failed":
            return True

        if upstream_updated_at is None:
            # If upstream doesn't provide updatedAt, we assume it's new
            return True

        manifest_updated_at = manifest.get("updatedAt")
        if manifest_updated_at is None:
            return True

        try:
            # Compare timestamps
            upstream_ts = datetime.fromisoformat(upstream_updated_at.replace('Z', '+00:00'))
            manifest_ts = datetime.fromisoformat(manifest_updated_at.replace('Z', '+00:00'))

            # Reprocess if upstream is newer
            return upstream_ts > manifest_ts
        except Exception as e:
            self.logger.error(f"Failed to compare timestamps for visit {visit_id}: {e}")
            # On error, reprocess to be safe
            return True

    def delete_visit_manifest(self, branch_id: str, date: str, visit_id: str) -> bool:
        """Delete a visit manifest (useful for cleanup or restart scenarios)."""
        manifest_path = self._get_visit_manifest_path(branch_id, date, visit_id)
        if not manifest_path.exists():
            return True

        try:
            manifest_path.unlink()
            self.logger.info(f"Deleted visit manifest: {manifest_path}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to delete visit manifest {manifest_path}: {e}")
            return False

    def cleanup_orphaned_manifests(
        self,
        branch_id: str,
        date: str,
        valid_visit_ids: Set[str]
    ) -> int:
        """
        Remove manifest files for visits that no longer exist in the API.
        Returns the number of manifests deleted.
        """
        visits_dir = self._get_visits_dir(branch_id, date)
        if not visits_dir.exists():
            return 0

        deleted_count = 0
        try:
            for manifest_file in visits_dir.glob("*.json"):
                if manifest_file.name.startswith(".tmp_"):
                    continue

                visit_id = manifest_file.stem
                if visit_id not in valid_visit_ids:
                    try:
                        manifest_file.unlink()
                        deleted_count += 1
                        self.logger.info(f"Cleaned up orphaned manifest: {manifest_file}")
                    except Exception as e:
                        self.logger.error(f"Failed to delete orphaned manifest {manifest_file}: {e}")

        except Exception as e:
            self.logger.error(f"Failed to cleanup orphaned manifests in {visits_dir}: {e}")

        return deleted_count
