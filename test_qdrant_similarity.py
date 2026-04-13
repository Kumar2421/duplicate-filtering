import argparse
import itertools
import math
import sys
from typing import Dict, List, Optional, Tuple

import numpy as np

from backend.core.db.qdrant_manager import QdrantManager


def _cosine(a: np.ndarray, b: np.ndarray) -> float:
    na = float(np.linalg.norm(a))
    nb = float(np.linalg.norm(b))
    if na == 0.0 or nb == 0.0:
        return float("nan")
    return float(np.dot(a / na, b / nb))


def _safe_float(v) -> Optional[float]:
    try:
        if v is None:
            return None
        return float(v)
    except Exception:
        return None


def _pick_primary(points: List[dict]) -> Optional[dict]:
    for p in points:
        payload = p.get("payload") or {}
        if payload.get("isPrimary") is True or str(payload.get("imageType")) == "primary":
            return p
        # Some payloads may have eventId=None for primary
        if payload.get("eventId") is None and str(payload.get("imageType")) == "primary":
            return p
    return None


def _pick_best_quality(points: List[dict]) -> Optional[dict]:
    best = None
    best_q = -math.inf
    for p in points:
        payload = p.get("payload") or {}
        q = _safe_float(payload.get("quality"))
        if q is None:
            continue
        if q > best_q:
            best_q = q
            best = p
    return best


def _scroll_points(qdrant: QdrantManager, branch_id: str, date: str, customer_id: str, limit: int = 200) -> List[dict]:
    # Avoid importing qdrant models globally to keep script lightweight.
    from qdrant_client.http import models

    pts, _ = qdrant.client.scroll(
        collection_name=qdrant.collection_name,
        scroll_filter=models.Filter(
            must=[
                models.FieldCondition(key="branchId", match=models.MatchValue(value=str(branch_id))),
                models.FieldCondition(key="date", match=models.MatchValue(value=str(date))),
                models.FieldCondition(key="customerId", match=models.MatchValue(value=str(customer_id))),
            ]
        ),
        limit=int(limit),
        with_payload=True,
        with_vectors=True,
    )

    out: List[dict] = []
    for p in pts:
        payload = getattr(p, "payload", None)
        vector = getattr(p, "vector", None)
        out.append({"payload": payload, "vector": vector})
    return out


def _vector_of(point: Optional[dict]) -> Optional[np.ndarray]:
    if not point:
        return None
    vec = point.get("vector")
    if vec is None:
        return None
    try:
        return np.array(vec, dtype=np.float32)
    except Exception:
        return None


def _describe_point(point: Optional[dict]) -> str:
    if not point:
        return "None"
    payload = point.get("payload") or {}
    return (
        f"imageType={payload.get('imageType')} eventId={payload.get('eventId')} "
        f"quality={payload.get('quality')} visitId={payload.get('visitId')} url={payload.get('url')}"
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Fetch vectors from Qdrant and print pairwise cosine similarity")
    parser.add_argument("--branch", required=True)
    parser.add_argument("--date", required=True)
    parser.add_argument(
        "--customer-ids",
        required=True,
        help="Comma-separated list, e.g. visitor-301674,visitor-302226",
    )
    parser.add_argument("--limit", type=int, default=200)
    args = parser.parse_args()

    branch_id = str(args.branch)
    date = str(args.date)
    customer_ids = [c.strip() for c in str(args.customer_ids).split(",") if c.strip()]

    try:
        qdrant = QdrantManager()
    except Exception as e:
        print("ERROR: Failed to initialize Qdrant client against local Qdrant DB.")
        print(f"Python: {sys.executable}")
        print(f"Error: {e}")
        print("\nIf your backend runs fine but this script fails, you likely have a qdrant-client/version mismatch.")
        print("Run this script using the backend virtualenv interpreter, for example:")
        print("  backend/venv/bin/python test_qdrant_similarity.py --branch TMJ-CBE --date 2026-04-07 --customer-ids visitor-...")
        return 2

    records: Dict[str, Dict[str, Optional[np.ndarray]]] = {}
    picked_meta: Dict[str, Dict[str, str]] = {}

    for cid in customer_ids:
        pts = _scroll_points(qdrant, branch_id=branch_id, date=date, customer_id=cid, limit=args.limit)
        primary = _pick_primary(pts)
        bestq = _pick_best_quality(pts)

        records[cid] = {
            "primary": _vector_of(primary),
            "bestq": _vector_of(bestq),
        }
        picked_meta[cid] = {
            "primary": _describe_point(primary),
            "bestq": _describe_point(bestq),
            "total_points": str(len(pts)),
        }

    print("=== Points Summary ===")
    for cid in customer_ids:
        print(f"{cid}: total_points={picked_meta[cid]['total_points']}")
        print(f"  primary: {picked_meta[cid]['primary']}")
        print(f"  bestq  : {picked_meta[cid]['bestq']}")

    def _pairwise(label: str):
        print(f"\n=== Pairwise Cosine Similarity ({label}) ===")
        for a, b in itertools.combinations(customer_ids, 2):
            va = records[a].get(label)
            vb = records[b].get(label)
            if va is None or vb is None:
                print(f"{a} vs {b}: missing vector")
                continue
            sim = _cosine(va, vb)
            print(f"{a} vs {b}: {sim:.4f}")

    _pairwise("primary")
    _pairwise("bestq")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
