import json
import os
import logging
from typing import Dict, List, Any, Optional
from pathlib import Path

logger = logging.getLogger(__name__)

# Basic in-memory cache: (branchId, date) -> data
_CACHE: Dict[str, tuple[float, Any]] = {}

def get_data_root() -> str:
    """
    Tries to find the 'data' directory in the project root.
    Checks: ./data, ../data, and absolute path.
    """
    # 0. Most reliable: resolve relative to this file location
    # cluster_loader.py -> backend/utils/cluster_loader.py
    # project root is two levels above 'backend'
    try:
        project_root = Path(__file__).resolve().parents[2]
        candidate = project_root / "data"
        if candidate.exists():
            return str(candidate)
    except Exception:
        pass

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

    data_root = get_data_root()
    path = os.path.join(data_root, "processed", branch_id, date, "visit-clusters.json")
    
    if not os.path.exists(path):
        logger.warning(f"CLUSTER_LOADER: JSON path not found: {path}")
        return None

    # Cache invalidation based on file modified time (mtime)
    file_mtime = os.path.getmtime(path)
    if use_cache and cache_key in _CACHE:
        cached_mtime, cached_data = _CACHE[cache_key]
        if cached_mtime == file_mtime:
            return cached_data

    try:
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            
            # Enrich visits with ALL local images
            storage_root = os.path.join(data_root, "raw")
            
            # Use a list to collect processed clusters to ensure we don't skip any
            processed_clusters = []
            
            for cluster in data.get("clusters", []):
                for v in cluster.get("visits", []):
                    visit_id = v.get("visitId") or v.get("id", "")
                    visit_path = os.path.join(storage_root, branch_id, date, visit_id)
                    
                    v["allImages"] = v.get("allImages", [])
                    
                    # 1. Identify primary filename
                    primary_filename = extract_filename(v.get("image"))
                    if not primary_filename: primary_filename = "primary.jpg"

                    # 1.5. Normalize existing images in allImages: ensure eventId derived from name if missing
                    # Also ensure isPrimary is correctly set based on primary_filename
                    for img in v["allImages"]:
                        is_primary = img.get("isPrimary") or (img.get("name") == primary_filename)
                        img["isPrimary"] = is_primary
                        if is_primary:
                            img["eventId"] = None
                        elif not img.get("eventId") and img.get("name"):
                            # Derive eventId from filename (remove extension)
                            img["eventId"] = os.path.splitext(img["name"])[0]
                    
                    # 2. Scan folder for ALL images if not already populated or if we want to refresh
                    if os.path.exists(visit_path):
                        files = os.listdir(visit_path)
                        existing_names = {img["name"] for img in v["allImages"]}
                        for file in files:
                            if file.lower().endswith(('.jpg', '.jpeg', '.png', '.webp')) and file not in existing_names:
                                is_primary = (file == primary_filename)
                                v["allImages"].append({
                                    "url": build_image_url(branch_id, date, visit_id, file),
                                    "name": file,
                                    "isPrimary": is_primary,
                                    "eventId": None if is_primary else file.split('.')[0]
                                })
                    
                    # Ensure primary image is at least represented
                    if not any(img.get("isPrimary") for img in v["allImages"]):
                        img_url = v.get("image") or build_image_url(branch_id, date, visit_id, primary_filename)
                        v["imageUrl"] = img_url
                        v["allImages"].insert(0, {
                            "url": img_url,
                            "name": primary_filename,
                            "isPrimary": True,
                            "eventId": None
                        })
                    else:
                        primary_img = next(img for img in v["allImages"] if img.get("isPrimary"))
                        v["imageUrl"] = primary_img["url"]
                
                processed_clusters.append(cluster)

            data["clusters"] = processed_clusters

            if use_cache:
                _CACHE[cache_key] = (file_mtime, data)
            return data
    except Exception as e:
        logger.error(f"CLUSTER_LOADER: Error loading cluster file {path}: {e}")
        return None

def get_flattened_visits(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Flattens clusters into a single list of visits.
    Each visit in the cluster is treated as a separate visit record.
    """
    visits = []
    for cluster in data.get("clusters", []):
        for visit in cluster.get("visits", []):
            # Create a copy to avoid modifying the original if shared
            v_copy = visit.copy()
            v_copy["clusterId"] = cluster.get("clusterId")
            v_copy["clusterType"] = cluster.get("type", "unknown")
            v_copy["date"] = data.get("date")
            v_copy["branchId"] = data.get("branchId")
            v_copy["customerIds"] = cluster.get("customerIds", []) # Important for UI display
            
            if "image" not in v_copy:
                v_copy["image"] = v_copy.get("imageUrl", "")
                
            visits.append(v_copy)
    return visits

def get_filtered_duplicates(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Returns clusters that are either explicitly typed as 'duplicate'/'conflict'
    OR have multiple visits (which implies a duplicate was found).
    """
    filtered = []
    for c in data.get("clusters", []):
        is_flagged = False
        
        # 1. Explicit cluster type
        if c.get("type") in ["duplicate", "conflict"]:
            is_flagged = True
        
        # 2. Check for multiple visits (Duplicate)
        if not is_flagged and len(c.get("visits", [])) >= 2:
            is_flagged = True

        # 3. Check visits for conflictIds (Identity Issues) or isEmployee flag
        if not is_flagged:
            for v in c.get("visits", []):
                if (v.get("conflictIds") and len(v.get("conflictIds")) > 0) or v.get("isEmployee") is True:
                    is_flagged = True
                    break
        
        if is_flagged:
            filtered.append(c)
            
    return filtered
