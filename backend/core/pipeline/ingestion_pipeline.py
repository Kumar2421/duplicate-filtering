import uuid
import time
import numpy as np
import logging
from typing import Iterable, Dict, Any, List, Optional, Tuple
from dataclasses import dataclass

from ..config.settings import settings
from ..db.qdrant_manager import EmbeddingPoint, make_point_id
from ..ml.embedding_service import EmbeddingService
from ..storage.file_manager import FileManager
from ..storage.json_cluster_writer import JsonClusterWriter
from ..ingestion.visit_normalizer import normalize_visit, NormalizedImage

@dataclass
class PipelineMetrics:
    total_visits_processed: int = 0
    total_images_found: int = 0
    total_images_downloaded: int = 0
    total_embeddings_extracted: int = 0
    total_embeddings_stored: int = 0
    
    def to_dict(self):
        return {
            "total_visits_processed": self.total_visits_processed,
            "total_images_found": self.total_images_found,
            "total_images_downloaded": self.total_images_downloaded,
            "total_embeddings_extracted": self.total_embeddings_extracted,
            "total_embeddings_stored": self.total_embeddings_stored,
        }

class IngestionPipeline:
    def __init__(
        self,
        embedding_service: EmbeddingService,
        qdrant_manager: Any,
        file_manager: FileManager,
        downloader: Any
    ):
        self.embedding_service = embedding_service
        self.qdrant = qdrant_manager
        self.file_manager = file_manager
        self.downloader = downloader
        self.manifest_writer = JsonClusterWriter()
        self.logger = logging.getLogger(__name__)

    def _visit_manifest_path(self, branch_id: str, date: str, visit_id: str) -> str:
        return f"{branch_id}/{date}/visits/{visit_id}.json"

    async def _process_single_image(
        self,
        visit_ctx: Dict[str, Any],
        img: NormalizedImage,
        metrics: PipelineMetrics,
        dl: Any = None,
        extra_payload: Optional[Dict[str, Any]] = None,
        point_visit_id: Optional[str] = None
    ) -> Optional[Tuple[float, EmbeddingPoint]]:
        try:
            image_data = None
            if dl is not None:
                # HttpDownloader returns DownloadResult(content=bytes).
                # Keep backwards-compat for any older downloader returning .data.
                image_data = getattr(dl, "content", None)
                if image_data is None:
                    image_data = getattr(dl, "data", None)

            if image_data:
                self.logger.info(f"DEBUG: Saving downloaded image for {img.url} ({len(image_data)} bytes)")
                metrics.total_images_downloaded += 1
                # Save the original image to disk
                local_path, _ = self.file_manager.save_original_bytes(
                    str(visit_ctx["branchId"]),
                    str(visit_ctx["date"]),
                    str(visit_ctx["visitId"]),
                    str(img.eventId) if img.eventId else "primary",
                    image_data
                )
                self.logger.info(f"DEBUG: Saved to {local_path}")
            else:
                self.logger.info(f"DEBUG: Reading existing image from disk for {img.url}")
                image_data = self.file_manager.read_original(
                    str(visit_ctx["branchId"]),
                    str(visit_ctx["date"]),
                    str(visit_ctx["visitId"]),
                    str(img.eventId) if img.eventId else "primary"
                )

            if not image_data:
                return None

            # Fix: EmbeddingService.extract_face_features expects a numpy array (BGR), not raw bytes.
            img_bgr = self.file_manager.validate_and_decode(image_data)
            if img_bgr is None:
                self.logger.warning(f"Failed to decode image for {img.url}")
                return None

            result = self.embedding_service.extract_face_features(img_bgr)
            if not result or result.embedding is None:
                return None

            metrics.total_embeddings_extracted += 1
            
            payload = {
                "visitId": str(visit_ctx["visitId"]),
                "customerId": str(visit_ctx["customerId"]),
                "branchId": str(visit_ctx["branchId"]),
                "date": str(visit_ctx["date"]),
                "eventId": str(img.eventId) if img.eventId else None,
                "imageType": img.imageType,
                "url": img.url,
                "timestamp": time.time(),
                "quality": float(result.quality)
            }
            if extra_payload:
                payload.update(extra_payload)

            pid = make_point_id(
                visit_id=point_visit_id or str(visit_ctx["visitId"]),
                event_id=payload.get("eventId"),
                image_type=str(payload.get("imageType")),
                ts_ms=int(time.time() * 1000)
            )

            vec = np.array(result.embedding, dtype=np.float32)
            norm = np.linalg.norm(vec)
            if norm > 0:
                vec = vec / norm

            return float(result.quality), EmbeddingPoint(
                point_id=pid,
                vector=vec.tolist(),
                payload=payload
            )
        except Exception as e:
            import traceback
            self.logger.error(f"Error processing image: {e}\n{traceback.format_exc()}")
            return None

    async def process_visits(self, visits: Iterable[Dict[str, Any]], force_reprocess: bool = False) -> Dict[str, Any]:
        metrics = PipelineMetrics()
        points_to_store: List[EmbeddingPoint] = []

        global_cap = int(settings.MAX_GLOBAL_EMBEDDINGS)
        per_group_cap = int(getattr(settings, "MAX_EMBEDDINGS_PER_GROUP", 5))

        active_groups: List[Dict[str, Any]] = []

        for raw_visit in visits:
            if len(points_to_store) >= global_cap:
                break

            try:
                visit_ctx = normalize_visit(raw_visit)
                
                # Check Layer 2: Qdrant existence
                if not force_reprocess and self.qdrant.visit_exists(
                    str(visit_ctx["branchId"]),
                    str(visit_ctx["date"]),
                    str(visit_ctx["visitId"])
                ):
                    self.logger.info(f"INGESTION: Skipping existing visit {visit_ctx['visitId']}")
                    continue

                metrics.total_visits_processed += 1

                images: List[NormalizedImage] = list(visit_ctx.get("images") or [])
                entry_event_ids = [str(img.eventId) for img in images if img.imageType == "entry" and img.eventId]
                exit_event_ids = [str(img.eventId) for img in images if img.imageType == "exit" and img.eventId]
                
                metrics.total_images_found += len(images)

                # Fetch and store ALL images discovered in the visit
                urls_to_download = []
                img_map = {}
                for img in images:
                    event_file_id = str(img.eventId) if img.eventId else "primary"
                    
                    # WATERMARK CHECK: Skip if eventId already exists in Qdrant
                    if img.eventId and self.qdrant.event_exists(img.eventId):
                        self.logger.info(f"INGESTION: Skipping already processed eventId {img.eventId}")
                        continue

                    # Check if we already have the original image on disk
                    if not self.file_manager.has_original(
                        str(visit_ctx["branchId"]), 
                        str(visit_ctx["date"]), 
                        str(visit_ctx["visitId"]), 
                        event_file_id
                    ):
                        urls_to_download.append(img.url)
                        img_map[img.url] = img

                if urls_to_download:
                    self.logger.info(f"Downloading {len(urls_to_download)} images for visit {visit_ctx['visitId']}")
                    results = await self.downloader.download_batch(urls_to_download)
                    for res in results:
                        if res.content:
                            img = img_map.get(res.url)
                            if img:
                                event_file_id = str(img.eventId) if img.eventId else "primary"
                                self.file_manager.save_original_bytes(
                                    str(visit_ctx["branchId"]),
                                    str(visit_ctx["date"]),
                                    str(visit_ctx["visitId"]),
                                    event_file_id,
                                    res.content
                                )

                primary_img = next((i for i in images if i.imageType == "primary"), None)
                if not primary_img:
                    continue

                anchor_id = f"{visit_ctx.get('visitId')}_primary"

                # Process primary image for embedding
                primary_res = await self._process_single_image(
                    visit_ctx=visit_ctx,
                    img=primary_img,
                    metrics=metrics,
                    dl=None, # Already saved above
                    extra_payload={
                        "anchorId": anchor_id,
                        "isPrimary": True,
                        "entryEventIds": entry_event_ids,
                        "exitEventIds": exit_event_ids,
                        "isDeleted": False
                    }
                )
                if not primary_res:
                    continue

                _, primary_point = primary_res
                primary_vec = np.array(primary_point.vector, dtype=np.float32)

                matched_group = None
                for g in active_groups:
                    sim = float(np.dot(primary_vec, g["anchor"]))
                    if sim >= float(getattr(settings, "PRIMARY_MATCH_THRESHOLD", 0.7)):
                        matched_group = g
                        break

                if matched_group is None:
                    matched_group = {
                        "groupId": str(uuid.uuid4()),
                        "anchor": primary_vec,
                        "stored": 0,
                    }
                    active_groups.append(matched_group)

                group_id = matched_group["groupId"]
                primary_point.payload["groupId"] = group_id
                points_to_store.append(primary_point)
                matched_group["stored"] += 1

                for i in images:
                    if i is primary_img:
                        continue
                    if int(matched_group["stored"]) >= int(per_group_cap):
                        continue

                    # Process secondary images for embedding (they were already downloaded/saved above)
                    img_res = await self._process_single_image(
                        visit_ctx=visit_ctx,
                        img=i,
                        metrics=metrics,
                        dl=None, # Already saved above
                        extra_payload={
                            "anchorId": anchor_id,
                            "isPrimary": False,
                            "groupId": group_id,
                            "isDeleted": False,
                            "entryEventIds": entry_event_ids,
                            "exitEventIds": exit_event_ids
                        },
                        point_visit_id=group_id
                    )
                    if not img_res:
                        continue

                    _, img_point = img_res
                    img_vec = np.array(img_point.vector, dtype=np.float32)
                    sim = float(np.dot(img_vec, matched_group["anchor"]))

                    if sim < float(getattr(settings, "STRICT_MATCH_THRESHOLD", 0.75)):
                        split_group_id = str(uuid.uuid4())
                        img_point.payload.update({
                            "groupId": split_group_id,
                            "anchorId": f"{visit_ctx.get('visitId')}_split_{split_group_id[:8]}",
                            "isPrimary": True
                        })
                        active_groups.append({
                            "groupId": split_group_id,
                            "anchor": img_vec,
                            "stored": 1
                        })
                        points_to_store.append(img_point)
                    else:
                        points_to_store.append(img_point)
                        matched_group["stored"] += 1

            except Exception as e:
                self.logger.error(f"Visit error: {e}")
                continue

        if points_to_store:
            stored = self.qdrant.upsert_in_batches(points_to_store, batch_size=int(settings.BATCH_SIZE))
            metrics.total_embeddings_stored = stored
            self.logger.info(f"Upserted {stored} points")

        return {"metrics": metrics.to_dict()}
