import json
import os
import logging
from typing import Dict, List, Any, Optional

logger = logging.getLogger(__name__)

# Basic in-memory cache: (branchId, date) -> data
_CACHE: Dict[str, Any] = {}

def get_data_root() -> str:
    """
    Tries to find the 'data' directory in the project root.
    Checks: ./data, ../data, and absolute path.
    """
    # 1. Check current directory
    if os.path.exists("data"):
        return os.path.abspath("data")
    
    # 2. Check parent directory (if we are in 'backend')
    if os.path.exists("../data"):
        return os.path.abspath("../data")
    
    # 3. Fallback to project root if known (here we assume we are within duplicate-filtering)
    # Searching for a marker file like 'package.json' or 'main.py' might be overkill, 
    # but let's try a simple absolute path search for 'duplicate-filtering/data'
    # Actually, the most reliable way in this specific project:
    return os.path.abspath("data")

def extract_filename(image_field: Any) -> str:
    """
    Extracts filename from various possible image field formats (string, dict, etc.)
    """
    if not image_field:
        return ""
    if isinstance(image_field, str):
        # Extracts from URL or Path
        return os.path.basename(image_field).split('?')[0]
    if isinstance(image_field, dict):
        return image_field.get("fileName", "")
    return str(image_field)

def build_image_url(branch_id: str, date: str, visit_id: str, file_name: str) -> str:
    """
    Builds the URL for locally served images.
    Format: /images/{branchId}/{date}/{visitId}/{fileName}
    """
    if not file_name:
        return ""
    return f"/images/{branch_id}/{date}/{visit_id}/{file_name}"

def load_clusters(branch_id: str, date: str, use_cache: bool = True) -> Optional[Dict[str, Any]]:
    """
    Loads clusters from data/processed/{branchId}/{date}/visit-clusters.json.
    Scans each visit folder to include ALL available images.
    """
    cache_key = f"{branch_id}_{date}"
    if use_cache and cache_key in _CACHE:
        return _CACHE[cache_key]

    data_root = get_data_root()
    path = os.path.join(data_root, "processed", branch_id, date, "visit-clusters.json")
    
    if not os.path.exists(path):
        logger.warning(f"CLUSTER_LOADER: JSON path not found: {path}")
        return None

    try:
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            
            # Enrich visits with ALL local images
            storage_root = os.path.join(data_root, "raw")
            
            for cluster in data.get("clusters", []):
                for v in cluster.get("visits", []):
                    visit_id = v.get("visitId") or v.get("id", "")
                    visit_path = os.path.join(storage_root, branch_id, date, visit_id)
                    
                    v["allImages"] = []
                    
                    # 1. Identify primary filename
                    primary_filename = extract_filename(v.get("image"))
                    if not primary_filename: primary_filename = "primary.jpg"
                    
                    # 2. Scan folder for ALL images
                    if os.path.exists(visit_path):
                        files = os.listdir(visit_path)
                        for file in files:
                            if file.lower().endswith(('.jpg', '.jpeg', '.png', '.webp')):
                                is_primary = (file == primary_filename)
                                v["allImages"].append({
                                    "url": build_image_url(branch_id, date, visit_id, file),
                                    "name": file,
                                    "isPrimary": is_primary
                                })
                    else:
                        logger.debug(f"CLUSTER_LOADER: Visit path NOT FOUND: {visit_path}")
                    
                    # Ensure primary image is at least represented if it was in the metadata but folder scan failed
                    if not any(img["isPrimary"] for img in v["allImages"]):
                        v["imageUrl"] = build_image_url(branch_id, date, visit_id, primary_filename)
                        v["allImages"].insert(0, {
                            "url": v["imageUrl"],
                            "name": primary_filename,
                            "isPrimary": True
                        })
                    else:
                        # Set main imageUrl to the primary image from the folder scan
                        primary_img = next(img for img in v["allImages"] if img["isPrimary"])
                        v["imageUrl"] = primary_img["url"]

            if use_cache:
                _CACHE[cache_key] = data
            return data
    except Exception as e:
        logger.error(f"CLUSTER_LOADER: Error loading cluster file {path}: {e}")
        return None

def get_flattened_visits(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Flattens clusters into a single list of visits.
    """
    visits = []
    for cluster in data.get("clusters", []):
        for visit in cluster.get("visits", []):
            visit["clusterId"] = cluster.get("clusterId")
            visit["clusterType"] = cluster.get("type", "unknown")
            visit["date"] = data.get("date")
            visit["branchId"] = data.get("branchId")
            
            if "image" not in visit:
                visit["image"] = visit.get("imageUrl", "")
                
            visits.append(visit)
    return visits

def get_filtered_duplicates(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Returns only clusters of type 'duplicate' or 'conflict'.
    """
    return [
        c for c in data.get("clusters", [])
        if c.get("type") in ["duplicate", "conflict"]
    ]
