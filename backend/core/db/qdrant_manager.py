from __future__ import annotations

import hashlib
import logging
import time
import uuid
from typing import Iterable, List, Optional

from qdrant_client import QdrantClient
from qdrant_client.http import models

from ..config.settings import settings
from ..ingestion.types import EmbeddingPoint


class QdrantManager:
    def __init__(
        self,
        collection_name: Optional[str] = None,
        vector_size: Optional[int] = None,
        path: Optional[str] = None,
    ):
        self.logger = logging.getLogger(__name__)
        self.collection_name = collection_name or settings.QDRANT_COLLECTION
        self.vector_size = int(vector_size or settings.QDRANT_VECTOR_SIZE)
        self.client = QdrantClient(path=path or settings.QDRANT_PATH)
        self._ensure_collection()

    def _ensure_collection(self):
        try:
            collections = self.client.get_collections().collections
            exists = any(c.name == self.collection_name for c in collections)
            if exists:
                return

            distance = models.Distance.COSINE
            if str(settings.QDRANT_DISTANCE).upper() == "DOT":
                distance = models.Distance.DOT
            elif str(settings.QDRANT_DISTANCE).upper() == "EUCLID":
                distance = models.Distance.EUCLID

            self.client.recreate_collection(
                collection_name=self.collection_name,
                vectors_config=models.VectorParams(size=self.vector_size, distance=distance),
            )
            self.logger.info(f"Qdrant collection created: {self.collection_name}")
        except Exception as e:
            self.logger.error(f"Error ensuring Qdrant collection: {e}")
            raise

    def upsert_batch(self, points: List[EmbeddingPoint]) -> int:
        if not points:
            return 0

        structs = [
            models.PointStruct(
                id=p.point_id,
                vector=p.vector,
                payload=p.payload,
            )
            for p in points
        ]

        self.client.upsert(collection_name=self.collection_name, points=structs)
        return len(points)

    def upsert_in_batches(self, points: Iterable[EmbeddingPoint], batch_size: int) -> int:
        total = 0
        buf: List[EmbeddingPoint] = []

        for p in points:
            buf.append(p)
            if len(buf) >= batch_size:
                total += self.upsert_batch(buf)
                buf = []

        if buf:
            total += self.upsert_batch(buf)

        return total


def make_point_id(visit_id: str, event_id: Optional[str], image_type: str, ts_ms: Optional[int] = None) -> str:
    ts_ms = int(ts_ms or (time.time() * 1000))
    eid = event_id if event_id is not None else "primary"
    # Create a deterministic UUID from the combined string to satisfy Qdrant's requirement
    seed = f"{visit_id}:{image_type}:{eid}:{ts_ms}"
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, seed))
