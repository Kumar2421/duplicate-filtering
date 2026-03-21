from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional


@dataclass(frozen=True)
class EmbeddingPoint:
    point_id: str
    vector: list[float]
    payload: Dict[str, Any]


@dataclass(frozen=True)
class ImageContext:
    visitId: str
    customerId: str
    branchId: str
    date: str
    eventId: Optional[str]
    imageType: str
    url: str
