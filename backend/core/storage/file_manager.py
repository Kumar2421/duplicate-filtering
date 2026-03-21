from __future__ import annotations

import hashlib
import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import cv2
import numpy as np

from ..config.settings import settings


@dataclass(frozen=True)
class StoredImage:
    local_path: str
    sha256: Optional[str]
    width: int
    height: int


class FileManager:
    def __init__(self, storage_root: str | None = None):
        self.storage_root = Path(storage_root or settings.STORAGE_PATH)
        self.logger = logging.getLogger(__name__)

    def _visit_root(self, branch_id: str, date: str, visit_id: str) -> Path:
        p = self.storage_root / str(branch_id) / str(date) / str(visit_id)
        p.mkdir(parents=True, exist_ok=True)
        return p

    def original_file_path(self, branch_id: str, date: str, visit_id: str, event_id: str, ext: str = ".jpg") -> Path:
        # Simplified: images stored directly in the visit folder
        return self._visit_root(branch_id, date, visit_id) / f"{event_id}{ext}"

    def read_original(self, branch_id: str, date: str, visit_id: str, event_id: str, ext: str = ".jpg") -> Optional[bytes]:
        file_path = self.original_file_path(branch_id, date, visit_id, event_id, ext=ext)
        if not file_path.exists():
            return None
        return file_path.read_bytes()

    def processed_file_path(self, branch_id: str, date: str, visit_id: str, event_id: str, ext: str = ".jpg") -> Path:
        # Simplified: images stored directly in the visit folder
        return self._visit_root(branch_id, date, visit_id) / f"processed_{event_id}{ext}"

    def has_original(self, branch_id: str, date: str, visit_id: str, event_id: str, ext: str = ".jpg") -> bool:
        return self.original_file_path(branch_id, date, visit_id, event_id, ext=ext).exists()

    def has_processed(self, branch_id: str, date: str, visit_id: str, event_id: str, ext: str = ".jpg") -> bool:
        return self.processed_file_path(branch_id, date, visit_id, event_id, ext=ext).exists()

    def _sha256(self, data: bytes) -> str:
        return hashlib.sha256(data).hexdigest()

    def save_original_bytes(
        self,
        branch_id: str,
        date: str,
        visit_id: str,
        event_id: str,
        content: bytes,
        ext: str = ".jpg",
        compute_hash: bool = True,
    ) -> tuple[str, Optional[str]]:
        if not content:
            self.logger.error(f"FILE_MANAGER: Attempted to save empty content for {visit_id}/{event_id}")
            return "", None

        file_path = self.original_file_path(branch_id, date, visit_id, event_id, ext=ext)

        sha = self._sha256(content) if compute_hash else None
        self.logger.info(f"FILE_MANAGER: Saving {len(content)} bytes to {file_path}. SHA256: {sha}")

        if file_path.exists() and sha is not None:
            try:
                existing = file_path.read_bytes()
                if self._sha256(existing) == sha:
                    self.logger.info(f"FILE_MANAGER: Identical file already exists at {file_path}")
                    return str(file_path), sha
            except Exception as e:
                self.logger.warning(f"FILE_MANAGER: Error checking existing file: {e}")

        try:
            file_path.write_bytes(content)
            # Verify write
            if file_path.stat().st_size == 0:
                self.logger.error(f"FILE_MANAGER: CRITICAL - File written to {file_path} but size is 0!")
            else:
                self.logger.info(f"FILE_MANAGER: Successfully wrote {file_path.stat().st_size} bytes")
        except Exception as e:
            self.logger.error(f"FILE_MANAGER: Failed to write bytes to {file_path}: {e}")
            raise

        return str(file_path), sha

    def validate_and_decode(self, content: bytes) -> Optional[np.ndarray]:
        nparr = np.frombuffer(content, np.uint8)
        img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
        return img

    def resize_if_needed(self, img: np.ndarray) -> np.ndarray:
        h, w = img.shape[:2]
        max_dim = settings.MAX_IMAGE_DIM
        if max(h, w) <= max_dim:
            return img

        scale = max_dim / float(max(h, w))
        new_w = int(w * scale)
        new_h = int(h * scale)
        resized = cv2.resize(img, (new_w, new_h), interpolation=cv2.INTER_AREA)
        return resized

    def save_processed_image(
        self,
        branch_id: str,
        date: str,
        visit_id: str,
        event_id: str,
        img: np.ndarray,
        ext: str = ".jpg",
        quality: int = 92,
    ) -> StoredImage:
        file_path = self.processed_file_path(branch_id, date, visit_id, event_id, ext=ext)

        params = [int(cv2.IMWRITE_JPEG_QUALITY), int(quality)]
        ok, encoded = cv2.imencode(ext, img, params)
        if not ok:
            raise ValueError("Failed to encode processed image")

        data = encoded.tobytes()
        file_path.write_bytes(data)

        h, w = img.shape[:2]
        return StoredImage(local_path=str(file_path), sha256=self._sha256(data), width=w, height=h)
