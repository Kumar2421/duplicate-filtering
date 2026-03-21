from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import cv2
import numpy as np


@dataclass(frozen=True)
class QualityResult:
    quality: float
    passed: bool
    reason: Optional[str]


class QualityFilter:
    def __init__(self, min_quality: float):
        self.min_quality = float(min_quality)

    def _blur_score(self, face_crop: np.ndarray) -> float:
        gray = cv2.cvtColor(face_crop, cv2.COLOR_BGR2GRAY)
        v = cv2.Laplacian(gray, cv2.CV_64F).var()
        # Map variance to 0..1-ish range with a soft cap.
        return float(min(1.0, v / 300.0))

    def score(self, face, full_image_bgr: np.ndarray) -> QualityResult:
        det = float(getattr(face, "det_score", 0.0) or 0.0)

        kps = getattr(face, "kps", None)
        if kps is None or len(kps) < 2:
            pose = 0.5
        else:
            try:
                # Eye distance heuristic (bigger => more likely frontal/usable crop)
                dx = float(kps[0][0] - kps[1][0])
                dy = float(kps[0][1] - kps[1][1])
                dist = (dx * dx + dy * dy) ** 0.5
                pose = float(min(1.0, dist / 60.0))
            except Exception:
                pose = 0.5

        blur = 0.5
        bbox = getattr(face, "bbox", None)
        if bbox is not None and full_image_bgr is not None:
            try:
                x1, y1, x2, y2 = [int(v) for v in bbox]
                h, w = full_image_bgr.shape[:2]
                x1 = max(0, min(w - 1, x1))
                x2 = max(0, min(w, x2))
                y1 = max(0, min(h - 1, y1))
                y2 = max(0, min(h, y2))
                if x2 > x1 and y2 > y1:
                    crop = full_image_bgr[y1:y2, x1:x2]
                    if crop.size > 0:
                        blur = self._blur_score(crop)
            except Exception:
                pass

        # Normalize det_score (usually 0..1), blur (0..1), pose (0..1)
        # Final quality is weighted average
        quality_score = 0.55 * det + 0.25 * blur + 0.20 * pose
        
        # Strict mandatory filters
        passed = True
        reason = None
        
        if det < 0.4:
            passed = False
            reason = "low_detection_confidence"
        elif blur < 0.1: # Very blurry
            passed = False
            reason = "too_blurry"
        elif quality_score < self.min_quality:
            passed = False
            reason = "quality_below_threshold"
            
        return QualityResult(quality=float(quality_score), passed=passed, reason=reason)
