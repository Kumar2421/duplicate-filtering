from __future__ import annotations
import logging
import uuid
import time
from typing import List, Optional, Dict, Any
from qdrant_client.http import models
import numpy as np

from core.db.qdrant_manager import QdrantManager
from core.config.settings import settings

class EmployeeEnrollmentService:
    def __init__(self, qdrant_manager: QdrantManager):
        self.qdrant = qdrant_manager
        self.collection_name = "employee_enrollment"
        self.logger = logging.getLogger(__name__)
        self._ensure_collection()

    def _ensure_collection(self):
        self.qdrant._ensure_collection(self.collection_name)

    def _normalize(self, embedding: List[float]) -> List[float]:
        emb_np = np.array(embedding, dtype=np.float32)
        norm = np.linalg.norm(emb_np)
        if norm > 1e-6:
            emb_np = emb_np / norm
        return emb_np.tolist()

    def enroll_employee(
        self, 
        branch_id: str, 
        employee_id: str, 
        name: str, 
        embedding: List[float], 
        metadata: Optional[Dict[str, Any]] = None
    ) -> bool:
        try:
            # Always normalize enrollment embeddings
            normalized_emb = self._normalize(embedding)
            
            point_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, f"employee:{branch_id}:{employee_id}"))
            
            payload = {
                "branchId": branch_id,
                "employeeId": employee_id,
                "name": name,
                "enrolledAt": time.time(),
                "isEmployee": True
            }
            if metadata:
                payload.update(metadata)

            self.qdrant.client.upsert(
                collection_name=self.collection_name,
                points=[
                    models.PointStruct(
                        id=point_id,
                        vector=normalized_emb,
                        payload=payload
                    )
                ]
            )
            self.logger.info(f"Enrolled employee {employee_id} in branch {branch_id}")
            return True
        except Exception as e:
            self.logger.error(f"Error enrolling employee: {e}")
            return False

    def list_employees(self, branch_id: str) -> List[Dict[str, Any]]:
        try:
            scroll_result = self.qdrant.client.scroll(
                collection_name=self.collection_name,
                scroll_filter=models.Filter(
                    must=[
                        models.FieldCondition(key="branchId", match=models.MatchValue(value=str(branch_id)))
                    ]
                ),
                limit=100,
                with_payload=True,
                with_vectors=False
            )
            return [p.payload for p in scroll_result[0]]
        except Exception as e:
            self.logger.error(f"Error listing employees: {e}")
            return []

    def identify_employee(self, branch_id: str, embedding: List[float], threshold: float = 0.7) -> Optional[Dict[str, Any]]:
        try:
            # Always normalize query embeddings
            normalized_emb = self._normalize(embedding)
            
            search_result = self.qdrant.client.query_points(
                collection_name=self.collection_name,
                query=normalized_emb,
                query_filter=models.Filter(
                    must=[
                        models.FieldCondition(key="branchId", match=models.MatchValue(value=str(branch_id)))
                    ]
                ),
                limit=1,
                score_threshold=threshold
            )
            points = getattr(search_result, "points", [])
            if points:
                res = points[0].payload.copy()
                res["similarity"] = float(points[0].score)
                return res
            return None
        except Exception as e:
            self.logger.error(f"Error identifying employee: {e}")
            return None

    def delete_employee(self, branch_id: str, employee_id: str) -> bool:
        try:
            point_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, f"employee:{branch_id}:{employee_id}"))
            self.qdrant.client.delete(
                collection_name=self.collection_name,
                points_selector=models.PointIdsList(points=[point_id])
            )
            return True
        except Exception as e:
            self.logger.error(f"Error deleting employee: {e}")
            return False
