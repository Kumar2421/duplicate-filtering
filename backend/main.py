import json
import logging
import os
import asyncio
import signal
import sys
import pytz
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from fastapi import FastAPI, Depends, Query, BackgroundTasks, HTTPException, status
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from jose import JWTError, jwt
from passlib.context import CryptContext
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime, timedelta
import numpy as np
import httpx
import uuid
import base64


def _load_dotenv_file(file_path: str):
    try:
        p = Path(file_path)
        if not p.exists() or not p.is_file():
            return

        for raw_line in p.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" not in line:
                continue
            k, v = line.split("=", 1)
            k = k.strip()
            v = v.strip().strip('"').strip("'")
            if not k:
                continue
            # Don't overwrite already-set env vars (PM2 env / shell exports win)
            os.environ.setdefault(k, v)
    except Exception:
        # Keep startup resilient; env file is optional.
        return


# Load env from a dotenv-style file early (before config + service init)
_DEFAULT_ENV_FILE = str(Path(__file__).resolve().parents[1] / "ecosystem.prod.config.js")
_load_dotenv_file(os.getenv("ENV_FILE", _DEFAULT_ENV_FILE))

class DeleteEventRequest(BaseModel):
    branchId: Optional[str] = None
    visitId: str
    eventId: str
    api_key: Optional[str] = None

class ConformationAction(BaseModel):
    id: str
    eventId: str
    approve: bool
    branchId: Optional[str] = None
    date: Optional[str] = None
    api_key: Optional[str] = None

class ConvertAction(BaseModel):
    branchId: Optional[str] = None
    customerId1: str
    customerId2: str
    toEmployee: bool
    api_key: Optional[str] = None

class CompareFacesRequest(BaseModel):
    image1: str
    image2: str
    return_crops: Optional[bool] = False
    crop_padding: Optional[int] = 0
    threshold: Optional[float] = None

from backend.services.api_service import APIService
from backend.services.ml_service import MLService
from backend.services.db_service import DBService
from backend.utils.normalizer import normalize_visit_data
from backend.core.pipeline.ingestion_pipeline import IngestionPipeline, PipelineMetrics
from backend.core.storage.json_cluster_writer import JsonClusterWriter
from backend.core.ml.embedding_service import EmbeddingService
from backend.core.ml.model_manager import ModelManager
from backend.core.ml.quality_filter import QualityFilter
from backend.core.storage.file_manager import FileManager
from backend.core.storage.http_downloader import HttpDownloader as ImageDownloader
from backend.utils.cluster_loader import load_clusters, get_flattened_visits, get_filtered_duplicates
from backend.api.check_enrollment import create_check_enrollment_router
from backend.core.clustering.similarity import cosine_similarity
from backend.services.analytics_auth_service import AnalyticsAuthService
from backend.core.metrics.processing_metrics import ProcessingMetricsManager

# Configuration setup
CONFIG_PATH = os.path.join(os.path.dirname(__file__), "config.json")

def load_config():
    with open(CONFIG_PATH, "r") as f:
        return json.load(f)

def save_config(new_config):
    with open(CONFIG_PATH, "w") as f:
        json.dump(new_config, f, indent=2)

config = load_config()

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response

class PrivateNetworkMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        response = await call_next(request)
        response.headers["Access-Control-Allow-Private-Network"] = "true"
        return response

# Initialization logics
app = FastAPI(title="Duplicate Detection Platform Middleware")

convert_jobs = {}

# Add Private Network Access Middleware BEFORE CORS
app.add_middleware(PrivateNetworkMiddleware)

# CORS middleware for React
app.add_middleware(
    CORSMiddleware,
    # NOTE: allow_credentials=True cannot be used with allow_origins=['*'].
    allow_origins=[
        "https://duplicate.tools.thefusionapps.com",
        "http://localhost:9002",
        "http://localhost:5173",
        "http://127.0.0.1:9002",
        "http://127.0.0.1:5173",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"],
)

# Apply GZip compression to all responses
app.add_middleware(GZipMiddleware, minimum_size=1000)

# Expose locally stored data (images + manifests) via HTTP
from backend.utils.cluster_loader import get_data_root
data_root = get_data_root()
raw_root = os.path.join(data_root, "raw")

os.makedirs(data_root, exist_ok=True)
os.makedirs(raw_root, exist_ok=True)

app.mount("/data", StaticFiles(directory=data_root), name="data")

@app.get("/images/{file_path:path}")
async def serve_image(file_path: str):
    raw_root_path = Path(raw_root).resolve()
    # The file_path from URL already contains branchId/date/visitId/filename
    target = (raw_root_path / file_path).resolve()

    # Prevent path traversal
    if raw_root_path not in target.parents and target != raw_root_path:
        logging.error(f"FORBIDDEN ACCESS: {target} is not under {raw_root_path}")
        raise HTTPException(status_code=403, detail="Forbidden")

    if not target.exists() or not target.is_file():
        logging.error(f"IMAGE NOT FOUND: {target}")
        # List directory to see what's actually there if it's a directory issue
        if target.parent.exists():
            logging.error(f"Directory exists. Contents of {target.parent}: {os.listdir(target.parent)}")
        else:
            logging.error(f"Parent directory does not exist: {target.parent}")
        raise HTTPException(status_code=404, detail="Image not found")

    return FileResponse(str(target))

# Shared service instances
# Use a thread pool for CPU-bound tasks (like face embedding extraction)
# to avoid blocking the main event loop.
executor = ThreadPoolExecutor(max_workers=os.cpu_count() or 4)

def shutdown_handler(sig, frame):
    logging.info(f"Shutdown signal {sig} received. Cleaning up...")
    try:
        executor.shutdown(wait=False, cancel_futures=True)
    except Exception as e:
        logging.error(f"Error shutting down executor: {e}")
    # Use os._exit to bypass normal sys.exit and atexit handlers
    # which can cause "sys.meta_path is None" or hang on thread joins
    os._exit(0)

# Register signal handlers for graceful CTRL+C and termination
signal.signal(signal.SIGINT, shutdown_handler)
signal.signal(signal.SIGTERM, shutdown_handler)

api_service = APIService(
    base_url=config["api"]["base_url"],
    limit=config["api"].get("limit", 50),
    category=config["api"].get("category", "potential"),
    time_range=config["api"].get("timeRange", "0,300,18000"),
    enabled=config["api"].get("enabled", True),
    configs=config["api"].get("configs", []),
    auth_service=AnalyticsAuthService(),
)

# Use one shared QdrantManager/client to avoid "Storage folder already accessed"
from backend.core.db.qdrant_manager import QdrantManager
qdrant_manager = QdrantManager(
    collection_name=config["qdrant"]["collection"],
    vector_size=512,
    path=config["qdrant"]["path"]
)

# Note: In real setup, ml_service may take time to init buffalo_l. 
model_manager = ModelManager(
    model_name=config["model"]["name"]
)
quality_filter = QualityFilter(
    min_quality=0.4
)
embedding_service = EmbeddingService(
    model_manager=model_manager,
    quality_filter=quality_filter
)
file_manager = FileManager()
downloader = ImageDownloader()

# Replace the old db_service with one using the shared client
db_service = DBService(
    collection_name=config["qdrant"]["collection"],
    vector_size=512,
    client=qdrant_manager.client
)

from backend.core.services.cluster_service import ClusterService
ingestion_pipeline = IngestionPipeline(
    embedding_service=embedding_service,
    qdrant_manager=qdrant_manager,
    file_manager=file_manager,
    downloader=downloader
)
cluster_writer = JsonClusterWriter()
cluster_service = ClusterService(qdrant_manager=qdrant_manager)
# Phase 5: Initialize metrics manager
metrics_manager = ProcessingMetricsManager()

app.include_router(
    create_check_enrollment_router(
        config=config,
        model_manager=model_manager,
        embedding_service=embedding_service,
        file_manager=file_manager,
        qdrant_manager=qdrant_manager,
    )
)


async def _read_image_bytes(image: str) -> bytes:
    if image.startswith("/") or image.startswith("./"):
        p = Path(image)
        if not p.exists() or not p.is_file():
            raise HTTPException(status_code=404, detail=f"Local image file not found: {image}")
        return p.read_bytes()

    async with httpx.AsyncClient() as client:
        resp = await client.get(image, timeout=20.0)
        if resp.status_code != 200:
            raise HTTPException(status_code=400, detail=f"Failed to fetch image from URL: {image}")
        return resp.content


def _extract_face_meta(*, img_bgr, return_crops: bool, crop_padding: int):
    try:
        app_insight = model_manager.get_app()
        faces = app_insight.get(img_bgr) or []
    except Exception:
        faces = []

    bboxes = []
    crops_b64 = []
    for f in faces:
        bbox = getattr(f, "bbox", None)
        det_score = getattr(f, "det_score", None)
        if bbox is None:
            continue
        bb = [int(v) for v in bbox]
        bboxes.append(
            {
                "x1": bb[0],
                "y1": bb[1],
                "x2": bb[2],
                "y2": bb[3],
                "score": float(det_score) if det_score is not None else None,
            }
        )

        if return_crops:
            from backend.api.check_enrollment import crop_by_bbox
            import base64
            import cv2

            crop = crop_by_bbox(img_bgr, bb, pad=int(crop_padding or 0))
            if crop is not None:
                ok, buf = cv2.imencode(".jpg", crop)
                if ok:
                    crops_b64.append(base64.b64encode(buf.tobytes()).decode("utf-8"))

    return {
        "persons_count": len(faces),
        "bboxes": bboxes,
        "crops": crops_b64 if return_crops else None,
    }


@app.post("/api/compare-faces")
async def compare_faces(payload: CompareFacesRequest):
    th = float(payload.threshold) if payload.threshold is not None else float(config["model"]["threshold"])

    img1_bytes = await _read_image_bytes(payload.image1)
    img2_bytes = await _read_image_bytes(payload.image2)

    img1_bgr = file_manager.validate_and_decode(img1_bytes)
    img2_bgr = file_manager.validate_and_decode(img2_bytes)

    if img1_bgr is None:
        raise HTTPException(status_code=400, detail="Invalid image1 data")
    if img2_bgr is None:
        raise HTTPException(status_code=400, detail="Invalid image2 data")

    meta1 = _extract_face_meta(
        img_bgr=img1_bgr,
        return_crops=bool(payload.return_crops),
        crop_padding=int(payload.crop_padding or 0),
    )
    meta2 = _extract_face_meta(
        img_bgr=img2_bgr,
        return_crops=bool(payload.return_crops),
        crop_padding=int(payload.crop_padding or 0),
    )

    r1 = embedding_service.extract_face_features(img1_bgr)
    r2 = embedding_service.extract_face_features(img2_bgr)

    if not r1 or r1.embedding is None:
        return {
            "ok": False,
            "reason": "embedding_failed_image1",
            "threshold": th,
            "image1": {"passed": False, "quality": float(getattr(r1, 'quality', 0.0) or 0.0), "reason": getattr(r1, "reason", None), **meta1},
            "image2": {"passed": bool(getattr(r2, 'passed', False)), "quality": float(getattr(r2, 'quality', 0.0) or 0.0), "reason": getattr(r2, "reason", None), **meta2},
            "similarity": None,
            "is_match": False,
            "model_meta": {"name": config["model"]["name"], "threshold": th},
        }

    if not r2 or r2.embedding is None:
        return {
            "ok": False,
            "reason": "embedding_failed_image2",
            "threshold": th,
            "image1": {"passed": bool(getattr(r1, 'passed', False)), "quality": float(getattr(r1, 'quality', 0.0) or 0.0), "reason": getattr(r1, "reason", None), **meta1},
            "image2": {"passed": False, "quality": float(getattr(r2, 'quality', 0.0) or 0.0), "reason": getattr(r2, "reason", None), **meta2},
            "similarity": None,
            "is_match": False,
            "model_meta": {"name": config["model"]["name"], "threshold": th},
        }

    v1 = np.array(r1.embedding, dtype=np.float32)
    v2 = np.array(r2.embedding, dtype=np.float32)

    n1 = float(np.linalg.norm(v1))
    n2 = float(np.linalg.norm(v2))
    if n1 > 0:
        v1 = v1 / n1
    if n2 > 0:
        v2 = v2 / n2
    sim = cosine_similarity(v1, v2)

    return {
        "ok": True,
        "threshold": th,
        "similarity": float(sim),
        "is_match": bool(sim >= th),
        "image1": {"passed": bool(r1.passed), "quality": float(r1.quality), "reason": r1.reason, **meta1},
        "image2": {"passed": bool(r2.passed), "quality": float(r2.quality), "reason": r2.reason, **meta2},
        "model_meta": {"name": config["model"]["name"], "threshold": th},
    }

# Global tracking for sync tasks to prevent overlaps
active_sync_tasks = set()  # Set of (branchId, date)
sync_semaphore = asyncio.Semaphore(2) # Limit parallel branch syncs to 2 to save resources

# Add a simple background task runner for sync
async def sync_branch(branch_cfg: dict, date_str: str, restart_enabled: bool, model_threshold: Optional[float] = None, deep_sync: bool = False):
    """
    Processes a single branch: ingest new visits and update clusters.
    Phase 5: Now with comprehensive metrics tracking.
    """
    b_id = branch_cfg.get("branchId")
    if not b_id:
        return

    task_key = (b_id, date_str)
    if task_key in active_sync_tasks:
        logging.info(f"PIPELINE: Sync already in progress for {b_id} on {date_str}. Skipping.")
        return

    # Phase 5: Start metrics tracking
    sync_metrics = metrics_manager.start_sync(b_id, date_str)

    async with sync_semaphore:
        active_sync_tasks.add(task_key)
        try:
            api_key_override = branch_cfg.get("api_key")
            logging.info(f"PIPELINE: Syncing branch {b_id} for {date_str} (Deep Sync: {deep_sync})")
            
            # If restart is enabled, wipe existing data for this branch and date
            if restart_enabled:
                logging.info(f"PIPELINE: Restart enabled. Wiping ALL data for {b_id} on {date_str}")
                # 1. Clear Qdrant points for this branch and date
                try:
                    from qdrant_client.http import models as q_models
                    qdrant_manager.client.delete(
                        collection_name=qdrant_manager.collection_name,
                        points_selector=q_models.Filter(
                            must=[
                                q_models.FieldCondition(key="branchId", match=q_models.MatchValue(value=str(b_id))),
                                q_models.FieldCondition(key="date", match=q_models.MatchValue(value=str(date_str))),
                            ]
                        )
                    )
                    await asyncio.sleep(1)
                except Exception as q_err:
                    logging.error(f"Error wiping Qdrant for {b_id}: {q_err}")

                # 2. Deep Wipe Local Directories (Processed AND Raw)
                import shutil
                from pathlib import Path
                for folder in ["processed", "raw"]:
                    target_dir = Path(get_data_root()) / folder / str(b_id) / str(date_str)
                    if target_dir.exists():
                        shutil.rmtree(target_dir)
                        logging.info(f"Deep Wiped {folder} dir: {target_dir}")

            # 1. Load existing cluster manifest to get lastUpdated
            latest_manifest = cluster_writer.load_visit_clusters(b_id, date_str)
            
            last_updated = None
            if not restart_enabled and not deep_sync:
                # Prefer persisted cursor over manifest meta (meta can change for reasons unrelated to upstream updates).
                last_updated = api_service.load_last_updated_cursor(b_id, date_str)

                # Backwards-compat: if no cursor yet, fall back to manifest meta.
                if not last_updated and not deep_sync and latest_manifest and "meta" in latest_manifest:
                    last_updated = latest_manifest["meta"].get("lastUpdated")
            
            # If still no last_updated, check if branch-specific startDate exists
            if not last_updated and not deep_sync:
                last_updated_str = branch_cfg.get("startDate")
                if last_updated_str:
                    last_updated = f"{last_updated_str}T00:00:00.000Z"
            
            # 2. Fetch new visits from API page by page and process immediately
            pages_processed = 0
            day_visits_all = await api_service.fetch_visits_for_date(b_id, date_str, api_key_override=api_key_override)
            total_api_visits = len(day_visits_all)

            if deep_sync:
                logging.info(f"PIPELINE: Deep Sync active for {b_id} on {date_str}. Total API visits to scan: {total_api_visits}")

            # Phase 5: Update metrics with total API visits
            metrics_manager.update_sync(b_id, date_str, total_api_visits=total_api_visits)

            max_processed_updated_at = None

            async for new_visits in api_service.fetch_incremental_pages(b_id, date_str, last_updated, api_key_override=api_key_override, deep_sync=deep_sync):
                logging.info(f"PIPELINE: Processing {len(new_visits)} visits for {b_id} on {date_str} (Deep Sync: {deep_sync})")

                # Phase 5: Update metrics with new visits fetched
                metrics_manager.update_sync(
                    b_id, date_str,
                    new_visits_fetched=sync_metrics.new_visits_fetched + len(new_visits),
                    api_pages_fetched=sync_metrics.api_pages_fetched + 1
                )

                # 3. Ingest (Embeddings -> Qdrant)
                ingest_res = await ingestion_pipeline.process_visits(new_visits, force_reprocess=restart_enabled, target_date=date_str, target_branch_id=b_id, deep_sync=deep_sync)
                logging.info(f"PIPELINE: Ingestion completed for {b_id} - {ingest_res.get('metrics')} - Upserted {ingest_res.get('upserted_count', 0)} points")

                # Patch isEmployee/isDeleted flags in the local JSON manifest if they changed in Qdrant
                changed_visit_ids = ingest_res.get("visits_with_flags_changed", [])
                if changed_visit_ids:
                    # Reload manifest to ensure we are patching the latest state
                    latest_manifest = cluster_writer.load_visit_clusters(b_id, date_str)
                    if latest_manifest:
                        modified_json = False
                        # Create a map of the new normalized visits for quick lookup
                        # new_visits was already normalized inside ingestion_pipeline.process_visits
                        # but we need to re-normalize or access the already normalized ones
                        from backend.core.ingestion.visit_normalizer import normalize_visit
                        new_normalized_map = {str(v.get("id")): normalize_visit(v) for v in new_visits if v.get("id")}
                        
                        for cluster in latest_manifest.get("clusters", []):
                            for visit in cluster.get("visits", []):
                                vid = str(visit.get("visitId"))
                                if vid in changed_visit_ids and vid in new_normalized_map:
                                    norm_v = new_normalized_map[vid]
                                    
                                    new_is_employee = norm_v.get("isEmployee", False)
                                    new_is_deleted = norm_v.get("isDeleted", False)
                                    
                                    if visit.get("isEmployee") != new_is_employee or visit.get("isDeleted") != new_is_deleted:
                                        logging.info(f"PIPELINE: Patching flags for visit {vid}: isEmployee({visit.get('isEmployee')}->{new_is_employee}), isDeleted({visit.get('isDeleted')}->{new_is_deleted})")
                                        visit["isEmployee"] = new_is_employee
                                        visit["isDeleted"] = new_is_deleted
                                        modified_json = True
                                        
                        if modified_json:
                            cluster_writer.save_visit_clusters(b_id, date_str, latest_manifest)
                            logging.info(f"PIPELINE: Patched {len(changed_visit_ids)} visits in visit-clusters.json for {b_id} on {date_str}")


                # Phase 5: Update metrics with ingestion results
                ing_metrics = ingest_res.get('metrics', {})
                metrics_manager.update_sync(
                    b_id, date_str,
                    images_found=sync_metrics.images_found + ing_metrics.get('total_images_found', 0),
                    images_downloaded=sync_metrics.images_downloaded + ing_metrics.get('total_images_downloaded', 0),
                    embeddings_extracted=sync_metrics.embeddings_extracted + ing_metrics.get('total_embeddings_extracted', 0),
                    points_upserted=sync_metrics.points_upserted + ingest_res.get('upserted_count', 0),
                    visit_manifests_saved=sync_metrics.visit_manifests_saved + ingest_res.get('visits_processed', 0)
                )

                # Advance cursor based on upstream visit.updatedAt (UTC ISO8601).
                for v in new_visits:
                    v_updated_at = v.get("updatedAt")
                    if not v_updated_at:
                        continue
                    if max_processed_updated_at is None or str(v_updated_at) > str(max_processed_updated_at):
                        max_processed_updated_at = str(v_updated_at)

                # 4. Identity Resolution (Clustering)
                latest_manifest = cluster_writer.load_visit_clusters(b_id, date_str)
                
                cluster_res = await cluster_service.get_clusters_for_date(
                    branch_id=b_id, 
                    date=date_str, 
                    existing_data=latest_manifest,
                    total_api_visits=total_api_visits,
                    force_reprocess=restart_enabled,
                    threshold=model_threshold,
                )
                
                cluster_writer.save_visit_clusters(b_id, date_str, cluster_res)
                logging.info(f"PIPELINE: Clustering updated for {b_id} - {cluster_res.get('meta')}")

                # Phase 5: Update clustering metrics
                cluster_meta = cluster_res.get('meta', {})
                clusters_list = cluster_res.get('clusters', [])
                conflicts = sum(1 for c in clusters_list if c.get('type') == 'conflict')
                duplicates = sum(1 for c in clusters_list if c.get('type') == 'duplicate')

                metrics_manager.update_sync(
                    b_id, date_str,
                    clusters_created=len(clusters_list),
                    conflicts_detected=conflicts,
                    duplicates_detected=duplicates
                )

                pages_processed += 1

            if not restart_enabled and max_processed_updated_at:
                api_service.save_last_updated_cursor(b_id, date_str, max_processed_updated_at)

            if pages_processed == 0:
                logging.info(f"PIPELINE: No new visits found for {b_id} on {date_str}")

            # Phase 5: Mark sync as completed
            metrics_manager.complete_sync(b_id, date_str, status="completed")

        except Exception as b_err:
            logging.error(f"PIPELINE ERROR for branch {b_id}: {b_err}")
            # Phase 5: Mark sync as failed
            metrics_manager.complete_sync(b_id, date_str, status="failed", error_message=str(b_err)[:500])
        finally:
            active_sync_tasks.discard(task_key)

async def run_pipeline_sync():
    """
    Background task that fetches new visits since lastUpdated and processes them in real-time.
    Iterates over all branches configured in config.json in parallel.
    """
    while True:
        try:
            # Reload config to pick up dynamic changes from Dashboard
            current_config = load_config()
            interval = current_config["api"].get("fetchInterval", 2) # Default 2 mins
            
            # Check if API fetching is disabled in config
            if not current_config.get("api", {}).get("enabled", True):
                logging.info("PIPELINE: API fetching is disabled in config. Skipping sync.")
                await asyncio.sleep(interval * 60)
                continue

            # Get branches from config
            api_configs = current_config["api"].get("configs", [])
            if not api_configs:
                logging.warning("PIPELINE: No branch configurations found in config.json")
                await asyncio.sleep(interval * 60)
                continue

            # Keep the live APIService in sync with config (even though Option B auth doesn't rely on configs).
            try:
                api_service.configs = api_configs
            except Exception:
                pass

            now_ist = datetime.now(pytz.timezone('Asia/Kolkata'))

            # Real-time continuous ingestion: always sync for today's date.
            # Per-branch 'startDate' in config is used inside sync_branch only as an initial
            # lastUpdated baseline when no manifest meta is available.
            api_configs = current_config["api"].get("configs", [])

            # Drop invalid entries to avoid dead tasks
            api_configs = [c for c in api_configs if c and str(c.get("branchId") or "").strip()]
            if not api_configs:
                logging.warning("PIPELINE: No valid branchId entries found in config.json")
                await asyncio.sleep(interval * 60)
                continue

            today_str = now_ist.strftime("%Y-%m-%d")
            
            restart_enabled = current_config.get("api", {}).get("restart", False)
            deep_sync_global = current_config.get("api", {}).get("deep_sync", False)
            sync_window_days = current_config.get("api", {}).get("syncWindowDays", 3)

            model_threshold = None
            try:
                model_threshold = float(current_config.get("model", {}).get("threshold"))
            except Exception:
                model_threshold = None

            # Phase 3 & 7: Optimized window iteration (Newest first)
            def _iter_dates(start_date_str: Optional[str], end_date_str: str, limit_days: int = 3, sync_start: Optional[str] = None, sync_end: Optional[str] = None):
                """
                Iterate dates. 
                If sync_start and sync_end are provided, iterate that range.
                If only sync_start is provided, iterate from sync_start to today.
                Otherwise, iterate last N days.
                """
                try:
                    today_dt = datetime.strptime(str(end_date_str), "%Y-%m-%d").date()
                    
                    if sync_start:
                        # Use explicit range if provided
                        start_dt = datetime.strptime(str(sync_start), "%Y-%m-%d").date()
                        if sync_end:
                            end_dt = datetime.strptime(str(sync_end), "%Y-%m-%d").date()
                        else:
                            end_dt = today_dt
                    else:
                        # Default last N days logic
                        end_dt = today_dt
                        start_dt = end_dt - timedelta(days=limit_days - 1)

                    cur = end_dt
                    while cur >= start_dt:
                        yield cur.strftime("%Y-%m-%d")
                        cur = cur - timedelta(days=1)
                except Exception as e:
                    logging.error(f"PIPELINE: Error in _iter_dates: {e}")
                    return

            # Phase 3 & 7: Enhanced branch sync with 0-balance skipping
            async def _sync_branch_range(cfg: dict):
                """
                Process last 3 days for a branch. Skip if balance is 0 for previous dates.
                """
                branch_id = cfg.get("branchId")
                today_dt = datetime.now().date()
                
                deep_sync_enabled = deep_sync_global or cfg.get("deep_sync", False)
                restart_branch = restart_enabled or cfg.get("restart", False)

                try:
                    # Priority for date range:
                    # 1. Branch-specific syncStartDate/syncEndDate
                    # 2. Global api.syncStartDate/api.syncEndDate (if we want to add them later)
                    # 3. Default window-based sync
                    
                    s_start = cfg.get("syncStartDate")
                    s_end = cfg.get("syncEndDate")
                    
                    # Strictly last N days or custom range, newest first
                    dates_to_sync = list(_iter_dates(None, today_str, limit_days=sync_window_days, sync_start=s_start, sync_end=s_end))
                    logging.info(f"PIPELINE: Branch {branch_id} will sync dates: {dates_to_sync}")

                    for d in dates_to_sync:
                        # Balance-Aware Skipping for previous dates
                        if not deep_sync_enabled and not restart_branch and d != today_str:
                            manifest = cluster_writer.load_visit_clusters(branch_id, d)
                            if manifest and manifest.get("meta", {}).get("balance") == 0:
                                logging.info(f"PIPELINE: Skipping {branch_id} for {d} (Balance is 0)")
                                continue

                        try:
                            await sync_branch(cfg, d, restart_branch, model_threshold, deep_sync=deep_sync_enabled)
                        except Exception as date_err:
                            logging.error(f"PIPELINE: Failed to sync {branch_id} for {d}: {date_err}")
                            continue

                except Exception as branch_err:
                    logging.error(f"PIPELINE: Failed to process date range for branch {branch_id}: {branch_err}")

            # Phase 7: Parallel sync across branches
            tasks = [_sync_branch_range(cfg) for cfg in api_configs]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            
            for i, res in enumerate(results):
                b_id = api_configs[i].get("branchId", "Unknown")
                if isinstance(res, Exception):
                    logging.error(f"PIPELINE: Sync failed for branch {b_id}: {res}", exc_info=True)
                else:
                    logging.info(f"PIPELINE: Sync completed for branch {b_id}")

            # Reset restart/deep_sync flags after one full cycle
            config_modified = False
            if restart_enabled:
                current_config["api"]["restart"] = False
                config_modified = True
            if current_config.get("api", {}).get("deep_sync", False):
                current_config["api"]["deep_sync"] = False
                config_modified = True
                
            if config_modified:
                save_config(current_config)

        except Exception as e:
            logging.error(f"PIPELINE GLOBAL ERROR: {e}", exc_info=True)
        
        logging.info(f"PIPELINE: Sleeping for {interval} minutes...")
        await asyncio.sleep(interval * 60)

@app.on_event("startup")
async def startup_event():
    logging.info("Application starting up...")
    # Start the background pipeline sync
    asyncio.create_task(run_pipeline_sync())

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Background Processing Task 
async def process_visits_background(visits: List[dict]):
    """
    Normalized data is ready to be converted into embeddings and stored.
    This is Phase 1 processing (embedding generation).
    """
    for visit in visits:
        norm_visit = normalize_visit_data(visit)
        if not norm_visit:
            continue
            
        img_url = norm_visit.get("image")
        face_id = str(norm_visit.get("face_id"))
        
        if not img_url:
            continue
            
        # Extract embedding
        # Result here is a list or None
        # embedding = await ml_service.get_embedding(img_url)
        # if embedding:
        #     # Insert to Qdrant
        #     db_service.insert_embedding(face_id, embedding, norm_visit)
        pass # To avoid heavy image downloading for now, we just pass.


@app.get("/fetch-visits")
async def fetch_visits(
    branchId: str, 
    startDate: str, 
    endDate: str,
    timeRange: Optional[str] = None,
):
    """
    API endpoint: /fetch-visits?branchId=&startDate=&endDate=
    """
    logger.info(f"Received fetch request: branch={branchId}, {startDate} to {endDate}")
    
    try:
        # Fetch visits range (returns {visits: [...], total: N})
        return await api_service.fetch_visits(branchId, startDate, endDate, time_range=timeRange)
        
    except Exception as e:
        logger.error(f"Error in fetch-visits: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


class IngestRequest(BaseModel):
    branchId: str
    date: str
    api_key: Optional[str] = None

import time

# Global tracking for ingestions triggered via API
active_ingestions = {} # (branchId, date) -> start_timestamp

@app.post("/api/ingest")
async def ingest_visits(
    payload: IngestRequest,
    background_tasks: BackgroundTasks
):
    """
    Trigger ingestion and identity resolution for a specific date and branch.
    Also persists the branch configuration for continuous background syncing.
    """
    global config
    
    if not payload.branchId or not str(payload.branchId).strip():
        raise HTTPException(status_code=400, detail="branchId is required")

    # Update global config and persist to disk for continuous sync
    # NOTE: Do NOT persist api_key (Option B uses login + branch switch tokens).
    current_configs = config["api"].get("configs", [])
    existing_cfg = next((c for c in current_configs if c.get("branchId") == payload.branchId), None)
    
    if existing_cfg:
        # We don't overwrite the date in config to avoid jumping back/forth, 
        # but we use the payload date for the immediate sync below.
        pass
    else:
        current_configs.append({
            "branchId": payload.branchId,
            "startDate": payload.date
        })
        config["api"]["configs"] = current_configs
        save_config(config)
        # Update the live api_service so the background loop sees it
        api_service.configs = current_configs

    branch_cfg = {
        "branchId": payload.branchId,
        # Optional manual override (not persisted); if provided, used only for this immediate ingestion.
        "api_key": payload.api_key,
    }
    
    # Record start time to differentiate from old manifest files
    active_ingestions[(payload.branchId, payload.date)] = time.time()
    
    background_tasks.add_task(sync_branch, branch_cfg, payload.date, False)
    return {"status": "started", "message": f"Ingestion started for {payload.branchId}. Added to continuous sync pipeline."}

@app.get("/api/ingest/status")
async def get_ingest_status(branchId: str, date: str):
    """
    Check if the manifest file exists for the given branch and date.
    """
    data = load_clusters(branchId, date)
    start_time = active_ingestions.get((branchId, date))

    if data:
        # If we have a record of when this ingestion started, check if the file is newer
        if start_time:
            # manifest usually has lastUpdated or we can check file mtime
            data_root = get_data_root()
            manifest_path = os.path.join(data_root, "processed", branchId, date, "visit-clusters.json")
            if os.path.exists(manifest_path):
                mtime = os.path.getmtime(manifest_path)
                if mtime > start_time:
                    # File is newer than our request, it's actually finished
                    return {"status": "completed", "lastUpdated": data.get("meta", {}).get("lastUpdated")}
                else:
                    return {"status": "processing", "message": "Manifest exists but is old. Still syncing..."}
        
        return {"status": "completed", "lastUpdated": data.get("meta", {}).get("lastUpdated")}
    
    return {"status": "processing"}


@app.get("/api/visits")
async def get_all_visits(
    branchId: str = Query(...),
    date: str = Query(...)
):
    """
    Returns all visits for a branch/date from processed cluster files.
    """
    data = load_clusters(branchId, date)
    if not data:
        raise HTTPException(status_code=404, detail="No processed data found for this date/branch")
    
    visits = get_flattened_visits(data)
    return {
        "branchId": branchId,
        "date": date,
        "visits": visits,
        "total": len(visits)
    }

@app.get("/api/duplicates")
async def get_duplicate_clusters(
    branchId: str = Query(...),
    date: str = Query(...)
):
    """
    Returns only duplicate/conflict clusters from processed files.
    """
    data = load_clusters(branchId, date)
    if not data:
        raise HTTPException(status_code=404, detail="No processed data found for this date/branch")
    
    clusters = get_filtered_duplicates(data)
    return {
        "branchId": branchId,
        "date": date,
        "clusters": clusters,
        "total": len(clusters)
    }

@app.get("/api/branches")
async def get_available_branches():
    """
    Returns unique branch IDs that have either processed or raw data folders.
    """
    data_root = get_data_root()
    branches = set()
    
    # Check 'processed' folder
    proc_path = os.path.join(data_root, "processed")
    if os.path.exists(proc_path):
        for b in os.listdir(proc_path):
            if os.path.isdir(os.path.join(proc_path, b)):
                branches.add(b)
                
    # Check 'raw' folder
    raw_path = os.path.join(data_root, "raw")
    if os.path.exists(raw_path):
        for b in os.listdir(raw_path):
            if os.path.isdir(os.path.join(raw_path, b)):
                branches.add(b)
                
    return {"branches": sorted(list(branches))}

@app.get("/api/available-dates")
async def get_available_dates(branchId: str = Query(...)):
    """
    Returns unique date folder names that have either processed or raw data.
    """
    from backend.utils.cluster_loader import get_data_root
    data_root = get_data_root()
    
    dates = set()
    
    # Date folders in 'processed'
    proc_path = os.path.join(data_root, "processed", branchId)
    if os.path.exists(proc_path):
        for d in os.listdir(proc_path):
            if os.path.isdir(os.path.join(proc_path, d)):
                dates.add(d)
                
    # Date folders in 'raw'
    raw_path = os.path.join(data_root, "raw", branchId)
    if os.path.exists(raw_path):
        for d in os.listdir(raw_path):
            if os.path.isdir(os.path.join(raw_path, d)):
                dates.add(d)
                
    return {"dates": sorted(list(dates), reverse=True)}

# Auth Configuration
SECRET_KEY = os.getenv("AUTH_SECRET_KEY", "7b22686f73744e616d65223a223130332e3138362e3232312e3230227d")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60 * 24 * 7 # 7 days

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="api/auth/login")

class Token(BaseModel):
    access_token: str
    token_type: str

async def get_current_user(token: str = Depends(oauth2_scheme)):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise credentials_exception
    except JWTError:
        raise credentials_exception
    return username

def create_access_token(data: dict, expires_delta: Optional[timedelta] = None):
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=15)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt

@app.post("/api/auth/login", response_model=Token)
async def login(form_data: OAuth2PasswordRequestForm = Depends()):
    admin_user = os.getenv("ADMIN_USERNAME", "admin")
    admin_password = os.getenv("ADMIN_PASSWORD", "admin123")
    
    if form_data.username != admin_user or form_data.password != admin_password:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={"sub": form_data.username}, expires_delta=access_token_expires
    )
    return {"access_token": access_token, "token_type": "bearer"}

@app.get("/api/auth/me")
async def read_users_me(current_user: str = Depends(get_current_user)):
    return {"username": current_user}

@app.get("/api/auth/branch-token")
async def get_branch_token(branchId: str, current_user: str = Depends(get_current_user)):
    """
    Fetch a branch-specific token from Analytics API (Option B flow).
    """
    try:
        token = await api_service.auth_service.get_branch_token(branchId)
        return {"branchId": branchId, "token": token}
    except Exception as e:
        logger.error(f"Failed to fetch branch token for {branchId}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.put("/api/conformation/action")
async def conformation_action(action: ConformationAction, current_user: str = Depends(get_current_user)):
    """
    Proxy to external conformation API and local logging.
    """
    # Use branchId/date from request or fallback to config if not provided
    branch_id = action.branchId or config["api"].get("branchId", "TMJ-CBE")
    date = action.date or datetime.now().strftime("%Y-%m-%d")

    result = await api_service.send_conformation_action(
        branch_id=branch_id,
        date=date,
        action_data=action.dict(),
        api_key_override=action.api_key
    )

    if not result["success"]:
        raise HTTPException(status_code=result["status_code"] if "status_code" in result else 500, detail=result["error"])
    
    return result

@app.post("/api/convert")
async def convert_action(action: ConvertAction, current_user: str = Depends(get_current_user)):
    branch_id = action.branchId or config["api"].get("branchId", "TMJ-CBE")

    payload = {
        "customerId1": action.customerId1,
        "customerId2": action.customerId2,
        "toEmployee": action.toEmployee
    }

    job_id = str(uuid.uuid4())
    convert_jobs[job_id] = {
        "status": "queued",
        "success": None,
        "result": None,
        "error": None,
        "created_at": datetime.utcnow().isoformat() + "Z",
        "branch_id": branch_id,
    }

    # Guard against a very common upstream failure mode:
    # the provided JWT belongs to a different branch than the selected branch.
    # Upstream often returns a generic 400 ("Errro Occured") for this.
    token_branch_id = None
    try:
        if action.api_key and action.api_key.count('.') >= 2:
            payload_b64 = action.api_key.split('.')[1]
            payload_b64 += '=' * (-len(payload_b64) % 4)
            payload_json = base64.urlsafe_b64decode(payload_b64.encode('utf-8')).decode('utf-8')
            payload_obj = json.loads(payload_json)
            token_branch_id = payload_obj.get('branchData', {}).get('branchId')
    except Exception:
        token_branch_id = None

    if token_branch_id and token_branch_id != branch_id:
        convert_jobs[job_id]["status"] = "error"
        convert_jobs[job_id]["success"] = False
        convert_jobs[job_id]["error"] = f"API key belongs to branch '{token_branch_id}' but you selected '{branch_id}'. Please use the correct branch API key."
        return {"success": True, "jobId": job_id, "status": "error"}

    async def _run_convert_job():
        convert_jobs[job_id]["status"] = "running"
        try:
            result = await api_service.send_convert_action(
                branch_id=branch_id,
                payload=payload,
                api_key_override=action.api_key,
                timeout_seconds=60.0,
                connect_timeout_seconds=10.0,
            )

            convert_jobs[job_id]["result"] = result
            convert_jobs[job_id]["success"] = bool(result.get("success"))

            if result.get("success"):
                convert_jobs[job_id]["status"] = "success"
            else:
                convert_jobs[job_id]["status"] = "error"
                convert_jobs[job_id]["error"] = result.get("error")
        except Exception as e:
            convert_jobs[job_id]["status"] = "error"
            convert_jobs[job_id]["success"] = False
            convert_jobs[job_id]["error"] = f"Internal Server Error: {str(e)}"

    # BackgroundTasks runs sync callables after the response is sent; it does not await async callables.
    # Use asyncio.create_task so the coroutine is actually executed.
    asyncio.create_task(_run_convert_job())
    return {"success": True, "jobId": job_id, "status": "queued"}

@app.get("/api/convert/status")
async def convert_status(jobId: str = Query(...)):
    job = convert_jobs.get(jobId)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return job

# Helper for delete statistics
DELETE_STATS_FILE = os.path.join("data", "state", "delete_stats.json")

def load_delete_stats(branch_id: Optional[str] = None, date_str: Optional[str] = None):
    if not os.path.exists(DELETE_STATS_FILE):
        return {"total_deleted": 0, "date_deleted": 0}
    try:
        with open(DELETE_STATS_FILE, "r") as f:
            stats = json.load(f)
            
        # Simplified: if branch and date provided, return that specific count
        if branch_id and date_str:
            return {
                "total_deleted": stats.get("total_deleted", 0),
                "date_deleted": stats.get(branch_id, {}).get(date_str, 0)
            }
        return {"total_deleted": stats.get("total_deleted", 0), "date_deleted": 0}
    except:
        return {"total_deleted": 0, "date_deleted": 0}

def increment_delete_count(branch_id: str, date_str: str):
    stats = {}
    if os.path.exists(DELETE_STATS_FILE):
        try:
            with open(DELETE_STATS_FILE, "r") as f:
                stats = json.load(f)
        except:
            stats = {}
    
    # Global total
    stats["total_deleted"] = stats.get("total_deleted", 0) + 1
    
    # Branch-Date based nesting
    if branch_id not in stats:
        stats[branch_id] = {}
    
    stats[branch_id][date_str] = stats[branch_id].get(date_str, 0) + 1
    
    os.makedirs(os.path.dirname(DELETE_STATS_FILE), exist_ok=True)
    with open(DELETE_STATS_FILE, "w") as f:
        json.dump(stats, f, indent=2)
    return stats["total_deleted"]

@app.get("/api/delete-stats")
async def get_delete_stats(branchId: Optional[str] = Query(None), date: Optional[str] = Query(None)):
    return load_delete_stats(branchId, date)

@app.delete("/api/delete-event")
async def delete_event(action: DeleteEventRequest, current_user: str = Depends(get_current_user)):
    """
    Proxy to external delete event API.
    """
    branch_id = action.branchId or config["api"].get("branchId", "TMJ-CBE")
    
    try:
        result = await api_service.send_delete_event(
            branch_id=branch_id,
            visit_id=action.visitId,
            event_id=action.eventId,
            api_key_override=action.api_key
        )
    except Exception as e:
        print(f"ERROR in delete_event: {type(e).__name__}: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")

    if not result["success"]:
        raise HTTPException(status_code=result["status_code"] if "status_code" in result else 500, detail=result["error"])
    
    # Increment delete count
    # Find the date for this visit to increment date-specific count
    event_date = None
    try:
        from backend.utils.cluster_loader import get_data_root
        branch_proc_path = os.path.join(get_data_root(), "processed", branch_id)
        if os.path.exists(branch_proc_path):
            # Check selected date first for efficiency
            selected_date = action.date or datetime.now().strftime("%Y-%m-%d")
            selected_manifest = os.path.join(branch_proc_path, selected_date, "visit-clusters.json")
            
            found_in_selected = False
            if os.path.exists(selected_manifest):
                with open(selected_manifest, 'r') as f:
                    data = json.load(f)
                for cluster in data.get("clusters", []):
                    for visit in cluster.get("visits", []):
                        if str(visit.get("visitId")) == str(action.visitId):
                            event_date = selected_date
                            found_in_selected = True
                            break
                    if found_in_selected: break
            
            if not found_in_selected:
                for d in os.listdir(branch_proc_path):
                    manifest_path = os.path.join(branch_proc_path, d, "visit-clusters.json")
                    if os.path.exists(manifest_path):
                        with open(manifest_path, 'r') as f:
                            data = json.load(f)
                        for cluster in data.get("clusters", []):
                            for visit in cluster.get("visits", []):
                                if str(visit.get("visitId")) == str(action.visitId):
                                    event_date = d
                                    break
                            if event_date: break
                    if event_date: break
    except:
        pass
    
    increment_delete_count(branch_id, event_date or datetime.now().strftime("%Y-%m-%d"))

    # Persistent mark as deleted in local JSON
    try:
        from backend.utils.cluster_loader import get_data_root
        import json
        branch_proc_path = os.path.join(get_data_root(), "processed", branch_id)
        if os.path.exists(branch_proc_path):
            # Check all date folders to find the visit and event
            for d in os.listdir(branch_proc_path):
                manifest_path = os.path.join(branch_proc_path, d, "visit-clusters.json")
                if os.path.exists(manifest_path):
                    with open(manifest_path, 'r') as f:
                        data = json.load(f)
                    
                    modified = False
                    for cluster in data.get("clusters", []):
                        for visit in cluster.get("visits", []):
                            if str(visit.get("visitId")) == str(action.visitId):
                                # Mark the visit itself as deleted if it's the primary event
                                if action.eventId == 'primary' or action.eventId is None:
                                    visit["isDeleted"] = True
                                    modified = True
                                
                                # Also mark individual image in allImages if present
                                for img in visit.get("allImages", []):
                                    if str(img.get("eventId")) == str(action.eventId) or (action.eventId == 'primary' and img.get("isPrimary")):
                                        img["isDeleted"] = True
                                        modified = True
                    
                    if modified:
                        with open(manifest_path, 'w') as f:
                            json.dump(data, f, indent=2)
                        logging.info(f"Updated local manifest {d} for deleted event: {action.eventId}")
    except Exception as e:
        logging.error(f"Failed to update local manifest for delete-event: {e}")

    return result

class DeepDeleteRequest(BaseModel):
    branchId: Optional[str] = None
    customerId: str
    api_key: Optional[str] = None

@app.delete("/api/deep-delete")
async def deep_delete(action: DeepDeleteRequest, current_user: str = Depends(get_current_user)):
    """
    Proxy to external deep delete API.
    """
    branch_id = action.branchId or config["api"].get("branchId", "TMJ-CBE")
    
    try:
        result = await api_service.send_deep_delete(
            branch_id=branch_id,
            customer_id=action.customerId,
            api_key_override=action.api_key
        )
    except Exception as e:
        print(f"ERROR in deep_delete: {type(e).__name__}: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")

    if not result["success"]:
        raise HTTPException(status_code=result["status_code"] if "status_code" in result else 500, detail=result["error"])
    
    # Increment delete count
    # Find the date for this customer to increment date-specific count
    customer_date = None
    try:
        from backend.utils.cluster_loader import get_data_root
        branch_proc_path = os.path.join(get_data_root(), "processed", branch_id)
        if os.path.exists(branch_proc_path):
            # No 'date' in DeepDeleteRequest, so we must scan date folders
            for d in os.listdir(branch_proc_path):
                manifest_path = os.path.join(branch_proc_path, d, "visit-clusters.json")
                if os.path.exists(manifest_path):
                    with open(manifest_path, 'r') as f:
                        data = json.load(f)
                    for cluster in data.get("clusters", []):
                        for visit in cluster.get("visits", []):
                            if str(visit.get("customerId")) == str(action.customerId):
                                customer_date = d
                                break
                        if customer_date: break
                if customer_date: break
    except:
        pass

    increment_delete_count(branch_id, customer_date or datetime.now().strftime("%Y-%m-%d"))

    # Persistent mark as deleted in local JSON
    try:
        from backend.utils.cluster_loader import get_data_root
        import json
        branch_proc_path = os.path.join(get_data_root(), "processed", branch_id)
        if os.path.exists(branch_proc_path):
            for d in os.listdir(branch_proc_path):
                manifest_path = os.path.join(branch_proc_path, d, "visit-clusters.json")
                if os.path.exists(manifest_path):
                    with open(manifest_path, 'r') as f:
                        data = json.load(f)
                    
                    modified = False
                    for cluster in data.get("clusters", []):
                        for visit in cluster.get("visits", []):
                            if str(visit.get("customerId")) == str(action.customerId):
                                visit["isDeleted"] = True
                                modified = True
                    
                    if modified:
                        with open(manifest_path, 'w') as f:
                            json.dump(data, f, indent=2)
                        logging.info(f"Updated local manifest {d} for deep-deleted customer: {action.customerId}")
    except Exception as e:
        logging.error(f"Failed to update local manifest for deep-delete: {e}")

    return result

@app.get("/system-metrics")
async def get_system_metrics(branchId: Optional[str] = Query(None), date: Optional[str] = Query(None)):
    """
    Returns actual system metrics based on processed data for the given branch and date.
    If branchId or date are not provided, it falls back to mock data or config defaults.
    """
    import random

    # Fallback to config if not provided
    branch_id = branchId or config["api"].get("branchId", "TMJ-CBE")
    date_str = date or datetime.now().strftime("%Y-%m-%d")

    try:
        data = load_clusters(branch_id, date_str)
        if data:
            clusters = data.get("clusters", [])
            total_visits = data.get("meta", {}).get("totalVisits", 0)
            total_images = data.get("meta", {}).get("totalProcessedUnique", 0) # Use unique images/visits
            unique_customers = data.get("meta", {}).get("uniqueCustomers", 0)

            # Count conflicts and duplicates
            conflict_count = 0
            duplicate_count = 0

            for c in clusters:
                has_conflict = c.get("type") == "conflict" or any(v.get("conflictIds") and len(v.get("conflictIds")) > 0 for v in c.get("visits", []))
                if has_conflict:
                    conflict_count += 1
                elif c.get("type") == "duplicate" or len(c.get("visits", [])) >= 2:
                    duplicate_count += 1

            return {
                "gpuUsage": random.randint(10, 80),
                "cpuUsage": random.randint(20, 60),
                "memoryUsage": random.randint(30, 70),
                "stats": {
                    "totalVisits": total_visits,
                    "totalImages": total_images,
                    "uniqueCustomers": unique_customers or len(set(cid for c in clusters for cid in c.get("customerIds", []))),
                    "duplicateCases": duplicate_count,
                    "conflictCases": conflict_count
                }
            }
    except Exception as e:
        logging.error(f"Error calculating system metrics: {e}")

    # Fallback/Mock
    return {
        "gpuUsage": random.randint(10, 80),
        "cpuUsage": random.randint(20, 60),
        "memoryUsage": random.randint(30, 70),
        "stats": {
            "totalVisits": 2542,
            "totalImages": 8642,
            "uniqueCustomers": 341,
            "duplicateCases": 12,
            "conflictCases": 0
        }
    }

@app.get("/api/processing-metrics/dashboard")
async def get_processing_metrics_dashboard():
    """
    Phase 5: Returns comprehensive dashboard summary of all processing metrics.
    """
    try:
        dashboard_data = metrics_manager.get_dashboard_summary()
        return {
            "success": True,
            "data": dashboard_data
        }
    except Exception as e:
        logging.error(f"Error fetching dashboard metrics: {e}")
        return {
            "success": False,
            "error": str(e),
            "data": {
                "total_syncs": 0,
                "active_syncs": 0,
                "completed_syncs": 0,
                "failed_syncs": 0
            }
        }

@app.get("/api/processing-metrics/sync")
async def get_sync_metrics(branchId: str = Query(...), date: str = Query(...)):
    """
    Phase 5: Returns detailed metrics for a specific sync operation.
    """
    try:
        sync_metrics = metrics_manager.get_sync_metrics(branchId, date)
        if sync_metrics:
            from dataclasses import asdict
            return {
                "success": True,
                "data": asdict(sync_metrics)
            }
        else:
            return {
                "success": False,
                "error": "No metrics found for this branch/date",
                "data": None
            }
    except Exception as e:
        logging.error(f"Error fetching sync metrics: {e}")
        return {
            "success": False,
            "error": str(e),
            "data": None
        }

@app.get("/api/processing-metrics/recent")
async def get_recent_syncs(limit: int = Query(10, ge=1, le=100)):
    """
    Phase 5: Returns recent sync operations across all branches.
    """
    try:
        recent_syncs = metrics_manager.get_recent_syncs(limit=limit)
        from dataclasses import asdict
        return {
            "success": True,
            "data": [asdict(s) for s in recent_syncs]
        }
    except Exception as e:
        logging.error(f"Error fetching recent syncs: {e}")
        return {
            "success": False,
            "error": str(e),
            "data": []
        }

if __name__ == "__main__":
    import uvicorn
    # Use log_level="info" for visibility
    # The signal handlers for SIGINT/SIGTERM are already registered globally in main.py
    # so uvicorn will handle the loop stop and then our handlers will clean up the executor.
    uvicorn.run(app, host="0.0.0.0", port=8009, log_level="info")
