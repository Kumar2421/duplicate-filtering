import json
import logging
import os
import asyncio
import signal
import sys
from concurrent.futures import ThreadPoolExecutor
from fastapi import FastAPI, Depends, Query, BackgroundTasks, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime, timedelta

class ConformationAction(BaseModel):
    id: str
    eventId: str
    approve: bool
    branchId: Optional[str] = None
    date: Optional[str] = None

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

# Configuration setup
def load_config():
    with open(os.path.join(os.path.dirname(__file__), "config.json"), "r") as f:
        return json.load(f)

config = load_config()

# Initialization logics
app = FastAPI(title="Duplicate Detection Platform Middleware")

# CORS middleware for React
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # In prod, restrict this to React origin
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
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
app.mount("/images", StaticFiles(directory=raw_root), name="images")

# Shared service instances
# Use a thread pool for CPU-bound tasks (like face embedding extraction)
# to avoid blocking the main event loop.
executor = ThreadPoolExecutor(max_workers=os.cpu_count() or 4)

def shutdown_handler(sig, frame):
    logging.info(f"Shutdown signal {sig} received. Cleaning up...")
    executor.shutdown(wait=False)
    # Force exit to ensure all threads and locks are released
    sys.exit(0)

# Register signal handlers for graceful CTRL+C and termination
signal.signal(signal.SIGINT, shutdown_handler)
signal.signal(signal.SIGTERM, shutdown_handler)

api_service = APIService(
    base_url=config["api"]["base_url"],
    api_key=config["api"].get("api_key"),
    limit=config["api"].get("limit", 50),
    category=config["api"].get("category", "potential"),
    time_range=config["api"].get("timeRange", "0,300,18000"),
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

# Auto-trigger logic
@app.on_event("startup")
async def startup_event():
    logging.info("Application starting up...")
    # Check if auto-trigger is enabled in config
    if config.get("api", {}).get("auto_ingest", True):
        branch_id = config["api"].get("branchId", "TMJ-CBE")
        start_date = config["api"].get("startDate", "2026-03-20")
        end_date = config["api"].get("endDate", "2026-03-20")
        time_range = config["api"].get("timeRange", "0,300,18000")
        
        logging.info(f"AUTO-TRIGGER: Initiating ingestion for branch={branch_id}, range={start_date} to {end_date}")
        
        async def run_continuous_ingestion():
            """
            Background task for continuous fetching and processing using IST.
            """
            branch_id = config["api"].get("branchId", "TMJ-CBE")
            interval = config["api"].get("fetchInterval", 5) # Default 5 mins
            
            logging.info(f"BACKGROUND_PIPE: Starting continuous ingestion for {branch_id}")
            
            async for date_str, day_visits in api_service.fetch_visits_continuously(branch_id, interval):
                try:
                    # Process in batches of 50
                    batch_size = 50
                    for i in range(0, len(day_visits), batch_size):
                        batch = day_visits[i : i + batch_size]
                        
                        # Define sync-async wrappers for ThreadPoolExecutor
                        loop = asyncio.get_running_loop()
                        
                        def process_sync(v_batch):
                            new_loop = asyncio.new_event_loop()
                            try:
                                return new_loop.run_until_complete(ingestion_pipeline.process_visits(v_batch))
                            finally:
                                new_loop.close()

                        # 1. Ingest (Embeddings -> Qdrant)
                        ingest_res = await loop.run_in_executor(executor, process_sync, batch)
                        logging.info(f"BACKGROUND_PIPE BATCH INGESTED: {date_str} - {ingest_res.get('metrics')}")

                        # 2. Identity Resolution
                        existing_data = cluster_writer.load_visit_clusters(branch_id, date_str)
                        
                        def cluster_sync(b_id, d_str, e_data):
                            new_loop = asyncio.new_event_loop()
                            try:
                                return new_loop.run_until_complete(cluster_service.get_clusters_for_date(
                                    branch_id=b_id, 
                                    date=d_str, 
                                    existing_data=e_data
                                ))
                            finally:
                                new_loop.close()

                        cluster_res = await loop.run_in_executor(executor, cluster_sync, branch_id, date_str, existing_data)
                        
                        # Save updated JSON
                        cluster_writer.save_visit_clusters(branch_id, date_str, cluster_res)
                        logging.info(f"BACKGROUND_PIPE BATCH CLUSTERED: {date_str} - {cluster_res.get('meta')}")
                        
                        await asyncio.sleep(0.1)
                except Exception as e:
                    logging.error(f"BACKGROUND_PIPE ERROR: {e}", exc_info=True)

        # Start the background task
        asyncio.create_task(run_continuous_ingestion())

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


@app.post("/ingest-visits")
async def ingest_visits(
    branchId: str,
    startDate: str,
    endDate: str,
    background_tasks: BackgroundTasks,
    timeRange: Optional[str] = None,
):
    """
    Runs the ingestion -> image handling -> quality filter -> embedding -> Qdrant storage pipeline.
    No recognition/duplicate logic is performed.
    """

    async def _run():
        try:
            # Fetch raw visits using the API service (timeRange can be overridden per request)
            start = datetime.strptime(startDate, "%Y-%m-%d")
            end = datetime.strptime(endDate, "%Y-%m-%d")
            delta = end - start
            date_list = [(start + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(delta.days + 1)]

            raw_visits: List[dict] = []
            for date_str in date_list:
                day_visits = await api_service.fetch_visits_for_date(branchId, date_str, time_range=timeRange)
                raw_visits.extend(day_visits)

            # Use thread pool for heavy processing
            loop = asyncio.get_running_loop()
            
            def process_sync(v_list):
                new_loop = asyncio.new_event_loop()
                try:
                    return new_loop.run_until_complete(ingestion_pipeline.process_visits(v_list))
                finally:
                    new_loop.close()

            result = await loop.run_in_executor(executor, process_sync, raw_visits)
            logger.info(f"Ingestion completed: {result.get('metrics')}")
        except Exception as e:
            logger.error(f"Ingestion failed: {e}")

    background_tasks.add_task(_run)
    return {"status": "started"}


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

@app.put("/api/conformation/action")
async def conformation_action(action: ConformationAction):
    """
    Proxy to external conformation API and local logging.
    """
    # Use branchId/date from request or fallback to config if not provided
    branch_id = action.branchId or config["api"].get("branchId", "TMJ-CBE")
    date = action.date or datetime.now().strftime("%Y-%m-%d")

    result = await api_service.send_conformation_action(
        branch_id=branch_id,
        date=date,
        action_data=action.dict()
    )

    if not result["success"]:
        raise HTTPException(status_code=result["status_code"] if "status_code" in result else 500, detail=result["error"])
    
    return result

@app.get("/system-metrics")
async def get_system_metrics():
    """
    Returns mock metrics for UI consumption.
    """
    import random
    return {
        "gpuUsage": random.randint(10, 80),
        "cpuUsage": random.randint(20, 60),
        "memoryUsage": random.randint(30, 70),
        "stats": {
            "totalVisits": random.randint(1000, 5000),
            "totalImages": random.randint(5000, 10000),
            "uniqueCustomers": random.randint(200, 1000),
            "duplicateCases": random.randint(5, 50)
        }
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
