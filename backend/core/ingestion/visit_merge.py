"""
Merge primary (duplicate/all) visit-stats rows with employee-category rows by visit id.
Employee feed wins for isEmployee and authoritative visit fields when present.
"""
from __future__ import annotations

import copy
from typing import Any, Dict, Iterable, List, Optional


def visit_row_id(visit: Dict[str, Any]) -> Optional[str]:
    vid = visit.get("id")
    if vid is None:
        vid = visit.get("visitId")
    return str(vid) if vid is not None else None


def merge_primary_with_employee(
    primary: Dict[str, Any],
    employee: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    """
    Returns a new dict: start from primary, overlay employee when employee row exists.
    Employee category is authoritative for staff classification and related fields.
    """
    if not employee:
        return copy.deepcopy(primary)

    out = copy.deepcopy(primary)
    # Upstream employee classification
    if "isEmployee" in employee:
        out["isEmployee"] = employee["isEmployee"]
    elif employee.get("customer") and isinstance(employee["customer"], dict):
        ce = employee["customer"].get("isEmployee")
        if ce is not None:
            out["isEmployee"] = ce

    for key in (
        "duration",
        "entryTime",
        "exitTime",
        "entryEventIds",
        "exitEventIds",
        "image",
        "imageUrl",
        "refImage",
        "updatedAt",
        "customer",
        "isDeleted",
        "isConverted",
        "convertedData",
    ):
        if key in employee and employee[key] is not None:
            val = employee[key]
            if key in ("entryEventIds", "exitEventIds", "customer") and isinstance(val, (dict, list)):
                out[key] = copy.deepcopy(val)
            else:
                out[key] = val

    return out


def index_visits_by_id(visits: Iterable[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for v in visits:
        if not isinstance(v, dict):
            continue
        vid = visit_row_id(v)
        if vid:
            out[vid] = v
    return out


def merge_incremental_batch_with_employees(
    primary_batch: List[Dict[str, Any]],
    primary_by_id: Dict[str, Dict[str, Any]],
    emp_by_id: Dict[str, Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Merge each visit in an incremental primary batch with same-day employee row if any."""
    merged: List[Dict[str, Any]] = []
    for v in primary_batch:
        if not isinstance(v, dict):
            continue
        vid = visit_row_id(v)
        if not vid:
            merged.append(copy.deepcopy(v))
            continue
        base = primary_by_id.get(vid, v)
        merged.append(merge_primary_with_employee(base, emp_by_id.get(vid)))
    return merged


def build_employee_catchup_batch(
    primary_by_id: Dict[str, Dict[str, Any]],
    emp_by_id: Dict[str, Dict[str, Any]],
    ids_seen_incremental: Iterable[str],
) -> List[Dict[str, Any]]:
    """
    Visits that appear in the employee feed for the day but were not in any incremental
    primary batch this run (e.g. isEmployee flipped hours later without primary updatedAt).
    """
    seen = set(ids_seen_incremental)
    batch: List[Dict[str, Any]] = []
    for eid, ev in emp_by_id.items():
        if eid in seen:
            continue
        base = primary_by_id.get(eid, ev)
        batch.append(merge_primary_with_employee(base, ev))
    return batch
