import re
import logging
from typing import List, Dict, Any, Optional

logger = logging.getLogger(__name__)

CDN_BASE_DOMAIN = "https://cdn.analytics.thefusionapps.com"

def detect_version_from_payload(payload: Dict[str, Any]) -> str:
    """
    Scans the payload for any existing URLs to detect the version (e.g., /v4/).
    Defaults to v2 if no version is found.
    """
    version_pattern = re.compile(r'/v(\d+)/')
    
    # Check common URL fields in the payload
    fields_to_check = ["image", "imageUrl", "refImage"]
    for field in fields_to_check:
        val = payload.get(field)
        if isinstance(val, str):
            match = version_pattern.search(val)
            if match:
                return f"v{match.group(1)}"
    
    return "v2"

def build_image_url(version: str, file_name: str, branch_id: Optional[str] = None, customer_id: Optional[str] = None) -> str:
    """
    Constructs the image URL based on version and fileName rules.
    """
    if not file_name:
        return ""
        
    # If it's already a full URL, return it
    if file_name.startswith("http"):
        return file_name

    if version == "v4" and branch_id and customer_id:
        return f"https://cdn.thefusionapps.com/v4/{branch_id}/{customer_id}.jpg"
        
    # Default reconstruction logic
    return f"{CDN_BASE_DOMAIN}/{version}/{file_name}"

def resolve_image_data(item: Dict[str, Any], default_version: str = "v2") -> Optional[str]:
    """
    Discovers image data from an item (visit or event) and reconstructs the URL.
    Priority: image > imageUrl > fileName
    """
    image_url = item.get("image") or item.get("imageUrl")
    
    if image_url and isinstance(image_url, str) and image_url.startswith("http"):
        return image_url
        
    file_name = item.get("fileName")
    if file_name:
        return build_image_url(default_version, file_name)
        
    return None

def deduplicate_images(images: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Removes duplicate images based on the URL.
    """
    seen = set()
    unique = []

    for img in images:
        url = img.get("url")
        if url and url not in seen:
            seen.add(url)
            unique.append(img)

    return unique

def extract_images(visit: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Collects primary image, refImage, and events images for a visit.
    """
    images = []
    branch_id = visit.get("branchId")
    customer_id = visit.get("customerId")

    # 1. Primary
    if visit.get("image"):
        images.append({
            "url": visit["image"],
            "source": "primary"
        })
    elif visit.get("imageUrl"):
        images.append({
            "url": visit["imageUrl"],
            "source": "primary"
        })
    elif visit.get("refImage"):
        images.append({
            "url": visit["refImage"],
            "source": "primary"
        })

    # 2. Reference
    if visit.get("refImage") and not any(img["url"] == visit["refImage"] for img in images):
        images.append({
            "url": visit["refImage"],
            "source": "ref"
        })

    # 3. Entry events
    for e in visit.get("entryEventIds", []):
        if e.get("fileName") and e.get("version"):
            images.append({
                "url": build_image_url(e["version"], e["fileName"], branch_id, customer_id),
                "source": "entry",
                "eventId": e.get("eventId"),
                "fileName": e["fileName"],
                "version": e["version"]
            })

    # 4. Exit events
    for e in visit.get("exitEventIds", []):
        if e.get("fileName") and e.get("version"):
            images.append({
                "url": build_image_url(e["version"], e["fileName"], branch_id, customer_id),
                "source": "exit",
                "eventId": e.get("eventId"),
                "fileName": e["fileName"],
                "version": e["version"]
            })

    return images

def normalize_visit(visit: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalizes a single visit with all its extracted images.
    """
    extracted_images = deduplicate_images(extract_images(visit))
    # Pick primary image for the 'image' field compatibility
    main_image = next((img["url"] for img in extracted_images if img["source"] == "primary"), None)
    if not main_image and extracted_images:
        main_image = extracted_images[0]["url"]

    return {
        "id": visit.get("id"),
        "visitId": visit.get("id") or visit.get("visitId"),
        "customerId": visit.get("customerId"),
        "branchId": visit.get("branchId"),
        "date": visit.get("date"),
        "isEmployee": visit.get("isEmployee", False),
        "image": main_image,
        "images": extracted_images
    }

def normalize_visit_data(raw_visit: dict) -> Optional[Dict[str, Any]]:
    """
    Facade for existing calls to normalize_visit_data.
    """
    try:
        return normalize_visit(raw_visit)
    except Exception as e:
        logger.error(f"Error normalizing visit data: {str(e)}")
        return None

def fetch_and_prepare(api_response: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Final pipeline: Takes full API response and prepares the visit list.
    """
    visits = api_response.get("visits", [])
    if not isinstance(visits, list):
        return []

    return [
        normalize_visit(v)
        for v in visits
    ]
