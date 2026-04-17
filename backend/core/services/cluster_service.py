from __future__ import annotations
import logging
import os
import numpy as np
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional
from pathlib import Path
from qdrant_client.http import models

from ..db.qdrant_manager import QdrantManager
from ..clustering.cluster_engine import ClusterEngine
from ..clustering.classifier import ClusterClassifier
from ..config.settings import settings
from backend.utils.cluster_loader import get_data_root

class ClusterService:
    def __init__(self, qdrant_manager: QdrantManager):
        self.qdrant = qdrant_manager
        self.engine = ClusterEngine(threshold=float(getattr(settings, "SIM_THRESHOLD", 0.7)))
        self.classifier = ClusterClassifier()
        self.logger = logging.getLogger(__name__)

    def _cosine(self, a: np.ndarray, b: np.ndarray) -> float:
        na = float(np.linalg.norm(a))
        nb = float(np.linalg.norm(b))
        if na == 0.0 or nb == 0.0:
            return -1.0
        return float(np.dot(a / na, b / nb))

    def _pick_primary_point(self, pts: List[Any]) -> Optional[Any]:
        primary_candidates: List[Any] = []
        for p in pts:
            payload = p.payload or {}
            is_primary = payload.get("isPrimary")
            image_type = payload.get("imageType")
            # Important: during ingestion we may mark a non-primary event as isPrimary=True when splitting
            # multiple faces into separate groups within a visit.
            # For cross-visit identity resolution, only treat a point as "primary" if it is truly the
            # dedicated primary image type. Otherwise we risk anchoring on a weak/off-angle event frame.
            if str(image_type) == "primary" or (is_primary is True and str(image_type) == "primary"):
                primary_candidates.append(p)

        if not primary_candidates:
            return None

        def _q(p: Any) -> float:
            payload = p.payload or {}
            q = payload.get("quality")
            try:
                return float(q) if q is not None else 0.0
            except Exception:
                return 0.0

        primary_candidates.sort(key=_q, reverse=True)
        return primary_candidates[0]

    def _build_visit_vectors_from_points(self, points: List[Any], *, threshold: float) -> List[Dict[str, Any]]:
        top_k = int(getattr(settings, "MAX_PER_VISIT", 5))
        if top_k <= 0:
            top_k = 5

        intra_visit_threshold = float(getattr(settings, "INTRA_VISIT_THRESHOLD", threshold))

        by_visit: Dict[str, List[Any]] = {}
        for p in points:
            payload = p.payload or {}
            visit_id = payload.get("visitId")
            if visit_id is None:
                continue
            by_visit.setdefault(str(visit_id), []).append(p)

        visits: List[Dict[str, Any]] = []
        for visit_id, pts in by_visit.items():
            # Choose primary as the identity anchor for this visit.
            # This protects against a visit containing multiple people (wrong face picked on some events).
            primary_point = self._pick_primary_point(pts)

            # Fallback: if primary missing, use best quality point.
            if primary_point is None:
                best_q = -1.0
                for p in pts:
                    if p.vector is None:
                        continue
                    payload = p.payload or {}
                    q = payload.get("quality")
                    try:
                        qf = float(q) if q is not None else 0.0
                    except Exception:
                        qf = 0.0
                    if qf > best_q:
                        best_q = qf
                        primary_point = p

            if primary_point is None or primary_point.vector is None:
                continue

            primary_vec = np.array(primary_point.vector, dtype=np.float32)

            accepted: List[tuple[float, Any]] = []
            for p in pts:
                if p.vector is None:
                    continue
                payload = p.payload or {}
                q = payload.get("quality")
                try:
                    qf = float(q) if q is not None else 0.0
                except Exception:
                    qf = 0.0

                cand_vec = np.array(p.vector, dtype=np.float32)
                sim = self._cosine(primary_vec, cand_vec)

                # Always keep the primary point itself; otherwise only keep points consistent with primary.
                if p is primary_point or sim >= intra_visit_threshold:
                    accepted.append((qf, p))

            if not accepted:
                continue

            accepted.sort(key=lambda t: t[0], reverse=True)
            selected = accepted[:top_k]

            vectors = [np.array(p.vector, dtype=np.float32) for _, p in selected]
            vec = np.mean(vectors, axis=0)
            n = float(np.linalg.norm(vec))
            if n > 0:
                vec = vec / n

            rep_point = primary_point
            payload = rep_point.payload or {}

            visits.append(
                {
                    "groupId": payload.get("groupId") or payload.get("visitId"),
                    "visitId": payload.get("visitId"),
                    "customerId": payload.get("customerId"),
                    "branchId": payload.get("branchId"),
                    "date": payload.get("date"),
                    "isEmployee": payload.get("isEmployee", False),
                    "image": payload.get("webPath") or payload.get("url"),
                    "refImage": payload.get("rawVisit", {}).get("refImage") if isinstance(payload.get("rawVisit"), dict) else payload.get("refImage"),
                    "anchorId": payload.get("anchorId"),
                    "eventId": payload.get("eventId"),
                    "entryEventIds": payload.get("entryEventIds") or [],
                    "exitEventIds": payload.get("exitEventIds") or [],
                    "isDeleted": payload.get("isDeleted", False),
                    "vector": vec,
                }
            )

        return visits

    async def get_clusters_for_date(self, branch_id: str, date: str, existing_data: Optional[Dict[str, Any]] = None, force_reprocess: bool = False, total_api_visits: int = 0, threshold: Optional[float] = None) -> Dict[str, Any]:
        """
        Fetch embeddings from Qdrant, build visit vectors, cluster, and classify.
        Supports incremental updates by merging with existing_data.
        """
        self.logger.info(f"Starting identity resolution for {branch_id} on {date}")
        
        # If force_reprocess is True, we don't merge with existing_data (start fresh)
        if force_reprocess:
            self.logger.info(f"CLUSTER_SERVICE: force_reprocess is TRUE, ignoring existing_data")
            existing_data = None
        
        # Step 1: Fetch all points for branch + date from Qdrant
        self.logger.info(f"Searching in collection: {self.qdrant.collection_name}")
        points = self.qdrant.client.scroll(
            collection_name=self.qdrant.collection_name,
            scroll_filter=models.Filter(
                must=[
                    models.FieldCondition(key="branchId", match=models.MatchValue(value=str(branch_id))),
                    models.FieldCondition(key="date", match=models.MatchValue(value=str(date))),
                ]
            ),
            limit=10000,
            with_payload=True,
            with_vectors=True
        )[0]

        if not points:
            self.logger.warning(f"No points found in Qdrant for branchId={branch_id} and date={date}")
            return {
                "branchId": branch_id, 
                "date": date, 
                "clusters": [], 
                "meta": {
                    "totalClusters": 0,
                    "totalVisits": 0,
                    "totalProcessedUnique": 0,
                    "totalApiVisits": total_api_visits,
                    "balance": total_api_visits,
                    "lastUpdated": datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),
                }
            }

        effective_threshold = float(threshold) if threshold is not None else float(self.engine.threshold)
        self.engine.threshold = effective_threshold
        final_groups = self._build_visit_vectors_from_points(points, threshold=effective_threshold)
        if not final_groups:
            self.logger.warning(f"No usable visit vectors found for branchId={branch_id}, date={date}. Total points={len(points)}")
            return existing_data or {
                "branchId": branch_id,
                "date": date,
                "clusters": [],
                "meta": {
                    "totalClusters": 0,
                    "totalVisits": 0,
                    "totalApiVisits": total_api_visits,
                    "balance": total_api_visits,
                    "lastUpdated": datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),
                },
            }

        # Step 3: Incremental Clustering (clusters will contain visits/groups)
        existing_clusters = existing_data.get("clusters", []) if existing_data else []
        clusters = self.engine.cluster_visits(final_groups, existing_clusters=existing_clusters)

        # Step 4: Classification
        classified_clusters = self.classifier.classify_clusters(clusters)

        # Step 5: Format for output (New Schema)
        output_clusters = []
        unique_processed_visits = set()
        for c in classified_clusters:
            # Find existing cluster to preserve action states
            existing_cluster = next((ec for ec in existing_clusters if ec["clusterId"] == c["clusterId"]), None)
            action_map = {}
            if existing_cluster:
                for ev in existing_cluster.get("visits", []):
                    if "action" in ev:
                        action_map[ev["visitId"]] = ev["action"]

            serializable_visits = []
            for v in c["visits"]:
                # Use a unique key for the visit to prevent React duplicate key errors.
                # Since multiple visits might have the same visitId in different event contexts,
                # we combine visitId and eventId (if present).
                visit_id_str = str(v.get("visitId"))
                unique_processed_visits.add(visit_id_str)
                event_id_str = str(v.get("eventId")) if v.get("eventId") else "primary"
                unique_key = f"{visit_id_str}_{event_id_str}"

                # Image path resolution: prefer local /images/ path if available
                branch_id_v = v.get("branchId")
                date_v = v.get("date")
                visit_id_v = v.get("visitId")
                
                image_url = v.get("image")
                all_images = []

                if branch_id_v and date_v and visit_id_v:
                    # Construct simplified local path: /images/{branchId}/{date}/{visitId}/primary.jpg
                    # This matches the FastAPI mount: app.mount("/images", StaticFiles(directory="data/raw"))
                    local_primary = f"/images/{branch_id_v}/{date_v}/{visit_id_v}/primary.jpg"
                    image_url = local_primary

                    # Scan the visit folder for ALL available images
                    # Path: data/raw/{branchId}/{date}/{visitId}/
                    visit_dir = Path(get_data_root()) / "raw" / str(branch_id_v) / str(date_v) / str(visit_id_v)
                    if visit_dir.exists():
                        for f in visit_dir.iterdir():
                            if f.is_file() and f.suffix.lower() in [".jpg", ".jpeg", ".png", ".webp"]:
                                is_primary = (f.name == "primary.jpg")
                                
                                # Extract eventId from filename if possible
                                # Filenames are usually {eventId}.jpg or primary.jpg
                                event_id = None
                                if not is_primary:
                                    event_id = f.stem # name without extension
                                
                                all_images.append({
                                    "url": f"/images/{branch_id_v}/{date_v}/{visit_id_v}/{f.name}",
                                    "name": f.name,
                                    "isPrimary": is_primary,
                                    "eventId": event_id
                                })
                        
                        # Sort to put primary image first
                        all_images.sort(key=lambda x: not x["isPrimary"])

                # Build visit object with new required fields
                v_copy = {
                    "uniqueKey": unique_key,
                    "visitId": visit_id_v,
                    "customerId": v.get("customerId"),
                    "isEmployee": v.get("isEmployee", False),
                    "conflictIds": [cid for cid in c["customerIds"] if cid != v.get("customerId")],
                    "eventId": v.get("eventId"),
                    "branchId": branch_id_v,
                    "date": date_v,
                    "image": image_url,
                    "allImages": all_images,
                    "refImage": v.get("refImage"),
                    "groupId": v.get("groupId"),
                    "similarityScore": float(v.get("similarityScore", 1.0)),
                    "entryEventIds": v.get("entryEventIds", []),
                    "exitEventIds": v.get("exitEventIds", []),
                    "isDeleted": v.get("isDeleted", False),
                    "action": action_map.get(v.get("visitId"), {
                        "status": "pending",
                        "updatedAt": None
                    })
                }
                serializable_visits.append(v_copy)

            output_clusters.append({
                "clusterId": c["clusterId"],
                "type": c["type"],
                "customerIds": c["customerIds"],
                "visits": serializable_visits,
                "stats": {
                    "totalVisits": len(serializable_visits),
                    "uniqueCustomers": len(set(c["customerIds"]))
                }
            })

        total_processed_unique = len(unique_processed_visits)
        balance = max(0, total_api_visits - total_processed_unique)

        return {
            "branchId": branch_id,
            "date": date,
            "clusters": output_clusters,
            "meta": {
                "totalClusters": len(output_clusters),
                "totalVisits": sum(len(c["visits"]) for c in output_clusters),
                "totalProcessedUnique": total_processed_unique,
                "totalApiVisits": total_api_visits,
                "balance": balance,
                "lastUpdated": datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
            }
        }
