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
from ..storage.visit_manifest_manager import VisitManifestManager
from ..ingestion.visit_normalizer import normalize_visit, NormalizedImage, _coerce_bool
from ..metrics.processing_metrics import ProcessingMetricsManager

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
        self.visit_manifest_manager = VisitManifestManager()
        self.metrics_manager = ProcessingMetricsManager()
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
            
            # Phase 2: Null handling for time data - extract and normalize
            raw_visit = visit_ctx.get("raw") or {}
            entry_time = raw_visit.get("entryTime")
            exit_time = raw_visit.get("exitTime")

            # Normalize time fields - ensure they're valid ISO8601 or None
            def _normalize_time_field(time_val: Any) -> Optional[str]:
                if time_val is None or str(time_val).strip() == "":
                    return None
                try:
                    # Validate it's a reasonable datetime string
                    if isinstance(time_val, str) and len(time_val) >= 10:
                        return str(time_val)
                    return None
                except Exception:
                    return None

            entry_time = _normalize_time_field(entry_time)
            exit_time = _normalize_time_field(exit_time)

            payload = {
                "visitId": str(visit_ctx["visitId"]),
                "customerId": str(visit_ctx["customerId"]),
                "branchId": str(visit_ctx["branchId"]),
                "date": str(visit_ctx["date"]),
                "isEmployee": visit_ctx.get("isEmployee", False),
                "isDeleted": visit_ctx.get("isDeleted", False),
                "entryTime": entry_time,
                "exitTime": exit_time,
                "eventId": str(img.eventId) if img.eventId else None,
                "imageType": img.imageType,
                "url": img.url,
                "timestamp": time.time(),
                "quality": float(result.quality)
            }
            if extra_payload:
                payload.update(extra_payload)

            pid = make_point_id(
                visit_id=str(visit_ctx["visitId"]),
                branch_id=str(visit_ctx["branchId"]),
                date=str(visit_ctx["date"]),
                event_id=payload.get("eventId"),
                image_type=str(payload.get("imageType"))
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

    async def process_visits(self, visits: Iterable[Dict[str, Any]], force_reprocess: bool = False, target_date: Optional[str] = None, target_branch_id: Optional[str] = None, deep_sync: bool = False) -> Dict[str, Any]:
        """
        Process visits with smart incremental sync (Phase 1).
        Only reprocesses visits that have changed based on updatedAt timestamp.
        If deep_sync is True, it forces flag checks for all visits.
        """
        metrics = PipelineMetrics()
        points_to_store: List[EmbeddingPoint] = []

        global_cap = int(settings.MAX_GLOBAL_EMBEDDINGS)
        per_group_cap = int(getattr(settings, "MAX_EMBEDDINGS_PER_GROUP", 5))

        active_groups: List[Dict[str, Any]] = []

        # Phase 1: Track processed visits for smart incremental sync
        visits_processed_count = 0
        visits_skipped_count = 0

        for raw_visit in visits:
            if not deep_sync and len(points_to_store) >= global_cap:
                break

            try:
                visit_ctx = normalize_visit(raw_visit)

                # Override branchId if target_branch_id is provided
                if target_branch_id:
                    visit_ctx["branchId"] = str(target_branch_id)

                # STRICT DATE FILTERING: Skip visits that don't match the requested sync date
                if target_date and str(visit_ctx.get("date")) != str(target_date):
                    self.logger.warning(f"INGESTION: Skipping visit {visit_ctx['visitId']} from {visit_ctx['date']} (Requested: {target_date})")
                    continue

                # Phase 1: Smart Incremental Sync - Check if visit needs reprocessing
                visit_id = str(visit_ctx.get("visitId"))
                branch_id = str(visit_ctx.get("branchId"))
                date_str = str(visit_ctx.get("date"))
                upstream_updated_at = raw_visit.get("updatedAt")

                if not deep_sync and not force_reprocess and target_date and target_branch_id:
                    needs_processing = self.visit_manifest_manager.needs_reprocessing(
                        target_branch_id, target_date, visit_id, upstream_updated_at
                    )
                    if not needs_processing:
                        visits_skipped_count += 1
                        self.logger.debug(f"INGESTION: Skipping visit {visit_id} - no changes since last sync")
                        continue

                # NOTE: Do NOT skip an entire visit based on existence.
                # Visits can receive new events/images over time; we dedupe at the event/image level
                # using Qdrant eventId checks and stable point IDs.

                metrics.total_visits_processed += 1
                visits_processed_count += 1

                images: List[NormalizedImage] = list(visit_ctx.get("images") or [])
                entry_event_ids = [str(img.eventId) for img in images if img.imageType == "entry" and img.eventId]
                exit_event_ids = [str(img.eventId) for img in images if img.imageType == "exit" and img.eventId]
                
                metrics.total_images_found += len(images)

                # Fetch and store ALL images discovered in the visit
                urls_to_download = []
                img_map = {}
                
                # Pre-calculate point IDs to check existence in Qdrant
                img_pids = []
                img_to_pid = {}
                img_obj_to_pid = {}
                for img in images:
                    pid = make_point_id(
                        visit_id=str(visit_ctx["visitId"]),
                        branch_id=str(visit_ctx["branchId"]),
                        date=str(visit_ctx["date"]),
                        event_id=str(img.eventId) if img.eventId else None,
                        image_type=str(img.imageType)
                    )
                    img_pids.append(pid)
                    img_to_pid[img.url] = pid
                    img_obj_to_pid[id(img)] = pid

                # Check which points already exist in Qdrant to skip embedding extraction
                existing_pids = []
                existing_points_map = {}
                if not force_reprocess and img_pids:
                    try:
                        # Batch check existence in Qdrant
                        result = self.qdrant.client.retrieve(
                            collection_name=self.qdrant.collection_name,
                            ids=img_pids,
                            with_payload=True,
                            with_vectors=True
                        )
                        for p in result:
                            existing_pids.append(str(p.id))
                            existing_points_map[str(p.id)] = p
                        
                        if existing_pids:
                            self.logger.info(f"INGESTION: Found {len(existing_pids)} existing points for visit {visit_ctx['visitId']}")
                    except Exception as e:
                        self.logger.error(f"Error checking point existence: {e}")

                existing_pids_set = set(existing_pids)
                
                # Update payloads for existing points if isEmployee/isDeleted changed
                for pid in existing_pids:
                    p = existing_points_map[pid]
                    payload = p.payload or {}
                    
                    # Check if critical flags need updating
                    current_is_employee = _coerce_bool(payload.get("isEmployee"))
                    new_is_employee = _coerce_bool(visit_ctx.get("isEmployee"))
                    
                    current_is_deleted = _coerce_bool(payload.get("isDeleted"))
                    new_is_deleted = _coerce_bool(visit_ctx.get("isDeleted"))
                    
                    if current_is_employee != new_is_employee or current_is_deleted != new_is_deleted:
                        self.logger.info(f"INGESTION: Updating payload for existing point {pid} (isEmployee: {current_is_employee}->{new_is_employee}, isDeleted: {current_is_deleted}->{new_is_deleted})")
                        payload["isEmployee"] = new_is_employee
                        payload["isDeleted"] = new_is_deleted
                        
                        # Mark that this visit needs its JSON patched
                        visit_ctx["flags_changed"] = True
                        
                        # Use the existing vector
                        vec = p.vector
                        if isinstance(vec, dict):
                            vec = next(iter(vec.values()), None)
                        
                        points_to_store.append(EmbeddingPoint(
                            point_id=pid,
                            vector=vec,
                            payload=payload
                        ))

                # Only process images whose pointId does not exist in Qdrant.
                # This is the core optimization to avoid disk reads + ML embedding re-extraction.
                if force_reprocess:
                    images_to_process = list(images)
                else:
                    images_to_process = [img for img in images if img_obj_to_pid.get(id(img)) not in existing_pids_set]

                if not images_to_process:
                    continue

                for img in images_to_process:
                    event_file_id = str(img.eventId) if img.eventId else "primary"

                    # Check if we already have the original image on disk
                    # MODIFICATION: If force_reprocess is True, we ignore the disk check and force re-download
                    if force_reprocess or not self.file_manager.has_original(
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

                primary_pid = make_point_id(
                    visit_id=str(visit_ctx["visitId"]),
                    branch_id=str(visit_ctx["branchId"]),
                    date=str(visit_ctx["date"]),
                    event_id=None,
                    image_type="primary",
                )

                primary_exists = (not force_reprocess) and (primary_pid in existing_pids)

                # Determine if we need to embed primary in this run.
                # If primary already exists, we should NOT re-embed; instead we will retrieve its vector for anchoring.
                primary_to_embed = None if primary_exists else primary_img

                # If primary exists but we still have secondary images to process, retrieve the primary vector
                # so we can compute similarity and grouping without reading/embedding the primary again.
                primary_vec = None
                primary_payload_existing = None
                if primary_exists:
                    try:
                        recs = self.qdrant.client.retrieve(
                            collection_name=self.qdrant.collection_name,
                            ids=[primary_pid],
                            with_payload=True,
                            with_vectors=True,
                        )
                        if recs:
                            rec = recs[0]
                            primary_payload_existing = getattr(rec, "payload", None)
                            vec = getattr(rec, "vector", None)
                            # Qdrant may return vector as list or dict (named vectors)
                            if isinstance(vec, dict):
                                vec = next(iter(vec.values()), None)
                            if isinstance(vec, list):
                                primary_vec = np.array(vec, dtype=np.float32)
                    except Exception as e:
                        self.logger.error(f"Error retrieving primary vector for anchor (pid={primary_pid}): {e}")

                anchor_id = f"{visit_ctx.get('visitId')}_primary"

                primary_point = None
                if primary_to_embed is not None:
                    # Process primary image for embedding (only when primary point is missing)
                    primary_res = await self._process_single_image(
                        visit_ctx=visit_ctx,
                        img=primary_to_embed,
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

                if primary_vec is None:
                    # If primary exists but we couldn't retrieve its vector, we can't anchor grouping.
                    # Avoid expensive fallbacks (disk read + ML) and skip this visit for now.
                    continue

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
                if primary_point is not None:
                    primary_point.payload["groupId"] = group_id
                    points_to_store.append(primary_point)
                    matched_group["stored"] += 1

                for i in images_to_process:
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

                # Phase 1: Save visit manifest after successful processing
                if target_date and target_branch_id:
                    try:
                        self.visit_manifest_manager.save_visit_manifest(
                            branch_id=target_branch_id,
                            date=target_date,
                            visit_id=visit_id,
                            custom_id=str(visit_ctx.get("customerId")),
                            updated_at=upstream_updated_at,
                            status="processed"
                        )
                    except Exception as manifest_err:
                        self.logger.error(f"Failed to save visit manifest for {visit_id}: {manifest_err}")

            except Exception as e:
                self.logger.error(f"Visit error: {e}")
                # Phase 1: Mark visit as failed in manifest
                if target_date and target_branch_id and 'visit_id' in locals():
                    try:
                        self.visit_manifest_manager.save_visit_manifest(
                            branch_id=target_branch_id,
                            date=target_date,
                            visit_id=visit_id,
                            custom_id=str(visit_ctx.get("customerId", "unknown")),
                            updated_at=raw_visit.get("updatedAt"),
                            status="failed",
                            error=str(e)[:500]
                        )
                    except Exception:
                        pass
                continue

        if points_to_store:
            stored = self.qdrant.upsert_in_batches(points_to_store, batch_size=int(settings.BATCH_SIZE))
            metrics.total_embeddings_stored = stored
            self.logger.info(f"Upserted {stored} points")
            return {
                "metrics": metrics.to_dict(),
                "upserted_count": stored,
                "visits_processed": visits_processed_count,
                "visits_skipped": visits_skipped_count,
                "visits_with_flags_changed": [str(v["visitId"]) for v in visits if isinstance(v, dict) and v.get("flags_changed")]
            }

        return {
            "metrics": metrics.to_dict(),
            "upserted_count": 0,
            "visits_processed": visits_processed_count,
            "visits_skipped": visits_skipped_count,
            "visits_with_flags_changed": [str(v["visitId"]) for v in visits if isinstance(v, dict) and v.get("flags_changed")]
        }
