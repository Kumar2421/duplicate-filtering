from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional

import numpy as np

from .model_manager import ModelManager
from .quality_filter import QualityFilter, QualityResult


@dataclass(frozen=True)
class EmbeddingResult:
    embedding: Optional[list[float]]
    quality: float
    passed: bool
    reason: Optional[str]


class EmbeddingService:
    def __init__(self, model_manager: ModelManager, quality_filter: QualityFilter):
        self.model_manager = model_manager
        self.quality_filter = quality_filter
        self.logger = logging.getLogger(__name__)

    def extract_face_features(self, img_bgr: np.ndarray) -> EmbeddingResult:
        """Alias for extract_embedding to match IngestionPipeline expectation."""
        return self.extract_embedding(img_bgr)

    def extract_embedding(self, img_bgr: np.ndarray) -> EmbeddingResult:
        app = self.model_manager.get_app()
        faces = app.get(img_bgr)
        if not faces:
            self.logger.warning("ML_RECOGNITION: No faces detected in image")
            return EmbeddingResult(embedding=None, quality=0.0, passed=False, reason="no_face")

        face0 = faces[0]
        q: QualityResult = self.quality_filter.score(face0, img_bgr)
        
        self.logger.info(f"ML_QUALITY: Score={q.quality:.2f}, Passed={q.passed}, Reason={q.reason}")
        
        if not q.passed:
            return EmbeddingResult(embedding=None, quality=q.quality, passed=False, reason=q.reason)

        emb = getattr(face0, "embedding", None)
        if emb is None:
            self.logger.warning("ML_RECOGNITION: Face detected but no embedding extracted")
            return EmbeddingResult(embedding=None, quality=q.quality, passed=False, reason="no_embedding")

        if isinstance(emb, np.ndarray):
            emb_list = emb.astype(float).tolist()
        else:
            emb_list = list(emb)

        self.logger.info("ML_RECOGNITION: Embedding successfully extracted")
        return EmbeddingResult(embedding=emb_list, quality=q.quality, passed=True, reason=None)
