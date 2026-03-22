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
    images_list: List[NormalizedImage] = []
    seen_urls = set()
    
    # Detect version from the whole visit payload (e.g., from refImage or other URLs)
    detected_version = detect_version_from_payload(visit)

    # 1. Main Visit Image (Primary)
    # Priority: visit["image"] > visit["imageUrl"] > visit["fileName"]
    primary_url = resolve_image_data(visit, default_version=detected_version)
    if primary_url and primary_url not in seen_urls:
        seen_urls.add(primary_url)
        images_list.append(
            NormalizedImage(
                eventId=None,
                imageType="primary",
                url=primary_url,
                source=visit,
            )
        )

    # 2. Entry Event Images
    entry_events = visit.get("entryEventIds", [])
    if isinstance(entry_events, list):
        for e in entry_events:
            if not isinstance(e, dict):
                continue
            event_url = resolve_image_data(e, default_version=detected_version)
            if event_url and event_url not in seen_urls:
                seen_urls.add(event_url)
                images_list.append(
                    NormalizedImage(
                        eventId=str(e.get("eventId")) if e.get("eventId") is not None else None,
                        imageType="entry",
                        url=event_url,
                        source=e,
                    )
                )

    # 3. Exit Event Images
    exit_events = visit.get("exitEventIds", [])
    if isinstance(exit_events, list):
        for e in exit_events:
            if not isinstance(e, dict):
                continue
            event_url = resolve_image_data(e, default_version=detected_version)
            if event_url and event_url not in seen_urls:
                seen_urls.add(event_url)
                images_list.append(
                    NormalizedImage(
                        eventId=str(e.get("eventId")) if e.get("eventId") is not None else None,
                        imageType="exit",
                        url=event_url,
                        source=e,
                    )
                )

    return images_list


def normalize_visit(visit: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "visitId": visit.get("id"),
        "customerId": visit.get("customerId"),
        "branchId": visit.get("branchId"),
        "date": visit.get("date"),
        "raw": visit,
        "images": extract_images(visit),
    }
