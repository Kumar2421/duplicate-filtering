from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from ..config.settings import settings
from ...utils.normalizer import detect_version_from_payload, resolve_image_data


@dataclass(frozen=True)
class NormalizedImage:
    eventId: Optional[str]
    imageType: str  # primary|entry|exit
    url: str
    source: Dict[str, Any]


def extract_images(visit: Dict[str, Any]) -> List[NormalizedImage]:
    images: List[NormalizedImage] = []
    
    # Detect version from the whole visit payload (e.g., from refImage or other URLs)
    detected_version = detect_version_from_payload(visit)

    # 1. Main Visit Image (Primary)
    # Priority: visit["image"] > visit["imageUrl"] > visit["fileName"]
    primary_url = resolve_image_data(visit, default_version=detected_version)
    if primary_url:
        images.append(
            NormalizedImage(
                eventId=None,
                imageType="primary",
                url=primary_url,
                source=visit,
            )
        )

    # 2. Entry Event Images
    for e in visit.get("entryEventIds", []) or []:
        if isinstance(e, str):
            # If it's just a string (ID), we can't reconstruct a URL without a filename
            continue
            
        event_url = resolve_image_data(e, default_version=detected_version)
        if event_url:
            images.append(
                NormalizedImage(
                    eventId=str(e.get("eventId")) if e.get("eventId") is not None else None,
                    imageType="entry",
                    url=event_url,
                    source=e,
                )
            )

    # 3. Exit Event Images
    for e in visit.get("exitEventIds", []) or []:
        if isinstance(e, str):
            continue
            
        event_url = resolve_image_data(e, default_version=detected_version)
        if event_url:
            images.append(
                NormalizedImage(
                    eventId=str(e.get("eventId")) if e.get("eventId") is not None else None,
                    imageType="exit",
                    url=event_url,
                    source=e,
                )
            )

    return images


def normalize_visit(visit: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "visitId": visit.get("id"),
        "customerId": visit.get("customerId"),
        "branchId": visit.get("branchId"),
        "date": visit.get("date"),
        "raw": visit,
        "images": extract_images(visit),
    }
