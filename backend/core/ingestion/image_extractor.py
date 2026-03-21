from __future__ import annotations

from typing import Any, Dict, List

from .visit_normalizer import NormalizedImage, extract_images


def extract_all_images(visit: Dict[str, Any]) -> List[NormalizedImage]:
    return extract_images(visit)
