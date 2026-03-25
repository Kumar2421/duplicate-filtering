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
            # ctx_id=0 uses GPU (CUDA), ctx_id=-1 uses CPU
            ctx_id = get_device()
            providers = ['CUDAExecutionProvider', 'CPUExecutionProvider'] if ctx_id == 0 else ['CPUExecutionProvider']
            
            app = FaceAnalysis(name=self.model_name, providers=providers)
            app.prepare(ctx_id=ctx_id)
            self._app = app
            
            device_str = "GPU" if ctx_id == 0 else "CPU"
            self.logger.info(f"InsightFace model initialized on {device_str}")
        return self._app
