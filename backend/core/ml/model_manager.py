from __future__ import annotations

import logging
from typing import Optional

from insightface.app import FaceAnalysis

from ...utils.gpu_manager import get_device


class ModelManager:
    def __init__(self, model_name: str = "buffalo_l"):
        self.logger = logging.getLogger(__name__)
        self.model_name = model_name
        self._app: Optional[FaceAnalysis] = None

    def get_app(self) -> FaceAnalysis:
        if self._app is None:
            # Force CPU mode by setting ctx_id=-1 as requested
            app = FaceAnalysis(name=self.model_name, providers=['CPUExecutionProvider'])
            app.prepare(ctx_id=-1)
            self._app = app
            self.logger.info("InsightFace model initialized on CPU")
        return self._app
