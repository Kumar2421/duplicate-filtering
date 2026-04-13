from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from ..config.settings import settings
from ...utils.normalizer import detect_version_from_payload, resolve_image_data


def _coerce_bool(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        v = value.strip().lower()
        if v in {"true", "1", "yes", "y", "t"}:
            return True
        if v in {"false", "0", "no", "n", "f", ""}:
            return False
    return bool(value)


def _get_nested(d: Dict[str, Any], *keys: str) -> Any:
    cur: Any = d
    for k in keys:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(k)
    return cur


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
    is_employee_val = (
        visit.get("isEmployee")
        if "isEmployee" in visit
        else _get_nested(visit, "customer", "isEmployee")
    )
    if is_employee_val is None:
        is_employee_val = _get_nested(visit, "rawVisit", "isEmployee")

    is_deleted_val = (
        visit.get("isDeleted")
        if "isDeleted" in visit
        else _get_nested(visit, "customer", "isDeleted")
    )
    if is_deleted_val is None:
        is_deleted_val = _get_nested(visit, "rawVisit", "isDeleted")

    return {
        "visitId": visit.get("id"),
        "customerId": visit.get("customerId"),
        "branchId": visit.get("branchId"),
        "date": visit.get("date"),
        "isEmployee": _coerce_bool(is_employee_val),
        "isDeleted": _coerce_bool(is_deleted_val),
        "raw": visit,
        "images": extract_images(visit),
    }
