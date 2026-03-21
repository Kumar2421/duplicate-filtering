from __future__ import annotations
import json
import logging
from pathlib import Path
from typing import Any, Dict
import time
 

from ..config.settings import settings

class JsonClusterWriter:
    def __init__(self, storage_root: str | None = None):
        self.storage_root = Path(storage_root or settings.STORAGE_PATH).parent / "processed"
        self.logger = logging.getLogger(__name__)

    def save_visit_clusters(self, branch_id: str, date: str, data: Dict[str, Any]) -> str:
        """
        Saves the unified visit-clusters.json for a specific branch and date.
        Path: data/processed/{branchId}/{date}/visit-clusters.json
        """
        out_dir = self.storage_root / str(branch_id) / str(date)
        out_dir.mkdir(parents=True, exist_ok=True)
        
        file_path = out_dir / "visit-clusters.json"
        tmp_path = out_dir / f"visit-clusters.json.tmp.{int(time.time() * 1000)}"
        
        try:
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            tmp_path.replace(file_path)
            self.logger.info(f"Saved visit clusters to {file_path}")
            return str(file_path)
        except Exception as e:
            self.logger.error(f"Failed to save visit clusters: {e}")
            raise

    def load_visit_clusters(self, branch_id: str, date: str) -> Dict[str, Any] | None:
        """Loads existing clusters for a branch and date."""
        file_path = self.storage_root / str(branch_id) / str(date) / "visit-clusters.json"
        if not file_path.exists():
            return None
        try:
            if file_path.stat().st_size == 0:
                return None
        except Exception:
            pass
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except json.JSONDecodeError as e:
            self.logger.error(f"Failed to load clusters: {e}")
            return None
        except Exception as e:
            self.logger.error(f"Failed to load clusters: {e}")
            return None
