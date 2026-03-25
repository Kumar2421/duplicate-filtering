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
            if not exists:
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

            # Ensure Payload Indexes for branchId and date for fast filtering
            self.client.create_payload_index(
                collection_name=self.collection_name,
                field_name="branchId",
                field_schema=models.PayloadSchemaType.KEYWORD,
            )
            self.client.create_payload_index(
                collection_name=self.collection_name,
                field_name="date",
                field_schema=models.PayloadSchemaType.KEYWORD,
            )
            self.logger.info(f"Qdrant payload indexes ensured for branchId and date")
        except Exception as e:
            self.logger.error(f"Error ensuring Qdrant collection or indexes: {e}")
            raise

    def event_exists(self, event_id: str, branch_id: str, date: str) -> bool:
        """
        Checks if a specific eventId (watermark) already exists for this branch and date.
        """
        if not event_id:
            return False
        try:
            result = self.client.scroll(
                collection_name=self.collection_name,
                scroll_filter=models.Filter(
                    must=[
                        models.FieldCondition(key="eventId", match=models.MatchValue(value=str(event_id))),
                        models.FieldCondition(key="branchId", match=models.MatchValue(value=str(branch_id))),
                        models.FieldCondition(key="date", match=models.MatchValue(value=str(date))),
                    ]
                ),
                limit=1,
                with_payload=False,
                with_vectors=False
            )
            return len(result[0]) > 0
        except Exception as e:
            self.logger.error(f"Error checking event existence: {e}")
            return False

    def visit_exists(self, branch_id: str, date: str, visit_id: str) -> bool:
        """
        Checks if a visit already exists in the collection.
        """
        try:
            result = self.client.scroll(
                collection_name=self.collection_name,
                scroll_filter=models.Filter(
                    must=[
                        models.FieldCondition(key="branchId", match=models.MatchValue(value=str(branch_id))),
                        models.FieldCondition(key="date", match=models.MatchValue(value=str(date))),
                        models.FieldCondition(key="visitId", match=models.MatchValue(value=str(visit_id))),
                    ]
                ),
                limit=1,
                with_payload=False,
                with_vectors=False
            )
            return len(result[0]) > 0
        except Exception as e:
            self.logger.error(f"Error checking visit existence: {e}")
            return False

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


def make_point_id(visit_id: str, branch_id: str, date: str, event_id: Optional[str] = None, image_type: str = "primary") -> str:
    """
    Creates a deterministic UUID from the combined branch, date, visit, and event context.
    This prevents cross-branch or cross-date ID collisions in Qdrant.
    """
    eid = event_id if event_id is not None else "primary"
    # Seed contains full context to ensure uniqueness across branches and dates
    seed = f"{branch_id}:{date}:{visit_id}:{image_type}:{eid}"
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, seed))
