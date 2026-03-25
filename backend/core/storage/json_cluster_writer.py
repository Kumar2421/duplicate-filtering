from __future__ import annotations
import json
import logging
from pathlib import Path
from typing import Any, Dict
import time
import os
 

from ..config.settings import settings

class JsonClusterWriter:
    def __init__(self, storage_root: str | None = None):
        self.storage_root = Path(storage_root or settings.STORAGE_PATH).parent / "processed"
        self.logger = logging.getLogger(__name__)

    def save_visit_clusters(self, branch_id: str, date: str, data: Dict[str, Any]) -> str:
        """
        Saves the unified visit-clusters.json for a specific branch and date.
        Path: data/processed/{branchId}/{date}/visit-clusters.json
        The 'meta' data is placed at the top of the file for better visibility.
        """
        out_dir = Path(self.storage_root) / str(branch_id) / str(date)
        try:
            out_dir.mkdir(parents=True, exist_ok=True)
            self.logger.debug(f"Ensured directory exists: {out_dir}")
        except Exception as e:
            self.logger.error(f"Failed to create directory {out_dir}: {e}")
            raise
        
        file_path = out_dir / "visit-clusters.json"
        
        # Re-order data to put meta at the top
        ordered_data = {}
        if "meta" in data:
            ordered_data["meta"] = data["meta"]
        
        # Add other keys (branchId, date, clusters, etc.)
        for key, value in data.items():
            if key != "meta":
                ordered_data[key] = value
        
        try:
            # Atomic write using a temporary file in the same directory
            tmp_path = out_dir / f".tmp_{int(time.time())}_visit-clusters.json"
            
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(ordered_data, f, ensure_ascii=False, indent=2)
                f.flush()
                os.fsync(f.fileno()) # Ensure it's written to disk
                
            tmp_path.replace(file_path)
            self.logger.info(f"Successfully saved visit clusters to {file_path}")
            return str(file_path)
        except Exception as e:
            self.logger.error(f"Failed to save visit clusters to {file_path}: {e}")
            if 'tmp_path' in locals() and tmp_path.exists():
                try:
                    tmp_path.unlink()
                except:
                    pass
            raise

    def load_visit_clusters(self, branch_id: str, date: str) -> Dict[str, Any] | None:
        """Loads existing clusters for a branch and date."""
        file_path = Path(self.storage_root) / str(branch_id) / str(date) / "visit-clusters.json"
        if not file_path.exists():
            return None
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            self.logger.error(f"Failed to load clusters: {e}")
            return None
