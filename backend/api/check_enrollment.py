from __future__ import annotations

import base64
import logging
from pathlib import Path
from typing import Optional

import cv2
import httpx
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel


class EnrollmentCheck(BaseModel):
    image: str
    branch: str
    date: str
    branchId: Optional[str] = None
    return_crops: Optional[bool] = False
    crop_padding: Optional[int] = 0
    run_liveness: Optional[bool] = False
    liveness_backend: Optional[str] = "opencv"


def crop_by_bbox(img_bgr, bbox, pad: int = 0):
    if img_bgr is None or bbox is None:
        return None

    h, w = img_bgr.shape[:2]
    x1, y1, x2, y2 = [int(v) for v in bbox]

    x1 = max(0, x1 - int(pad))
    y1 = max(0, y1 - int(pad))
    x2 = min(w, x2 + int(pad))
    y2 = min(h, y2 + int(pad))

    if x2 <= x1 or y2 <= y1:
        return None

    return img_bgr[y1:y2, x1:x2]


def create_check_enrollment_router(*, config, model_manager, embedding_service, file_manager, qdrant_manager) -> APIRouter:
    router = APIRouter()

    def _deepface_liveness(*, img_bgr, face_bbox: Optional[list[int]], pad: int, backend: str):
        try:
            from deepface import DeepFace
        except Exception as ie:
            return {
                "ok": False,
                "error": (
                    f"DeepFace import failed: {ie}. "
                    "DeepFace must be installed in the same Python environment that runs the backend (PM2 uses backend/venv)."
                ),
            }

        try:
            crop_bgr = crop_by_bbox(img_bgr, face_bbox, pad=pad) if face_bbox else img_bgr
            if crop_bgr is None:
                return {
                    "ok": False,
                    "error": "No face crop available for liveness",
                }

            crop_rgb = cv2.cvtColor(crop_bgr, cv2.COLOR_BGR2RGB)
            faces = DeepFace.extract_faces(
                img_path=crop_rgb,
                detector_backend=backend,
                enforce_detection=False,
                anti_spoofing=True,
            )

            if not faces:
                return {
                    "ok": False,
                    "error": "DeepFace returned no faces",
                }

            f0 = faces[0] or {}
            is_real = f0.get("is_real", None)
            score = f0.get("antispoof_score", None)

            return {
                "ok": True,
                "is_real": bool(is_real) if is_real is not None else None,
                "score": float(score) if score is not None else None,
                "backend": backend,
            }
        except Exception as e:
            return {
                "ok": False,
                "error": str(e),
                "backend": backend,
            }

    @router.post("/api/check-enrollment")
    async def check_enrollment(payload: EnrollmentCheck):
        """
        Checks if an image is already enrolled in Qdrant for a specific branch and date.
        Returns enrollment status and model metadata.
        """
        try:
            # 1. Fetch image bytes (Support local paths or URLs)
            image_bytes = None
            if payload.image.startswith("/") or payload.image.startswith("./"):
                # Local file path
                file_path = Path(payload.image)
                if not file_path.exists():
                    raise HTTPException(status_code=404, detail=f"Local image file not found: {payload.image}")
                image_bytes = file_path.read_bytes()
                logging.info(f"ENROLLMENT_CHECK: Read {len(image_bytes)} bytes from local path: {payload.image}")
            else:
                # Remote URL
                async with httpx.AsyncClient() as client:
                    resp = await client.get(payload.image, timeout=10.0)
                    if resp.status_code != 200:
                        raise HTTPException(status_code=400, detail=f"Failed to fetch image from URL: {payload.image}")
                    image_bytes = resp.content
                    logging.info(f"ENROLLMENT_CHECK: Downloaded {len(image_bytes)} bytes from URL: {payload.image}")

            # 2. Decode and Extract Embedding
            img_bgr = file_manager.validate_and_decode(image_bytes)
            if img_bgr is None:
                raise HTTPException(status_code=400, detail="Invalid image data")

            faces = []
            try:
                app_insight = model_manager.get_app()
                faces = app_insight.get(img_bgr) or []
            except Exception as fe:
                logging.error(f"ENROLLMENT_CHECK: Face detection failed: {fe}")
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

                if payload.return_crops:
                    crop = crop_by_bbox(img_bgr, bb, pad=int(payload.crop_padding or 0))
                    if crop is not None:
                        ok, buf = cv2.imencode(".jpg", crop)
                        if ok:
                            crops_b64.append(base64.b64encode(buf.tobytes()).decode("utf-8"))

            liveness = None
            if bool(payload.run_liveness):
                first_bbox = None
                if bboxes:
                    first_bbox = [bboxes[0]["x1"], bboxes[0]["y1"], bboxes[0]["x2"], bboxes[0]["y2"]]
                backend = str(payload.liveness_backend or "opencv")
                liveness = _deepface_liveness(
                    img_bgr=img_bgr,
                    face_bbox=first_bbox,
                    pad=int(payload.crop_padding or 0),
                    backend=backend,
                )

            result = embedding_service.extract_face_features(img_bgr)
            if not result or result.embedding is None:
                return {
                    "enrolled": False,
                    "reason": "No face detected or embedding failed",
                    "liveness": liveness,
                    "persons_count": len(faces),
                    "bboxes": bboxes,
                    "crops": crops_b64 if payload.return_crops else None,
                    "model_meta": {
                        "name": config["model"]["name"],
                        "threshold": config["model"]["threshold"],
                        "quality_threshold": config["model"]["quality_filter"]["min_confidence"],
                    },
                }

            # 3. Check Qdrant for similarity in this branch/date
            from qdrant_client.http import models as q_models

            query_res = qdrant_manager.client.query_points(
                collection_name=qdrant_manager.collection_name,
                query=result.embedding,
                query_filter=q_models.Filter(
                    must=[
                        q_models.FieldCondition(key="branchId", match=q_models.MatchValue(value=str(payload.branch))),
                        q_models.FieldCondition(key="date", match=q_models.MatchValue(value=str(payload.date))),
                    ]
                ),
                limit=1,
                score_threshold=float(config["model"]["threshold"]),
            )

            search_result = getattr(query_res, "points", []) or []
            is_enrolled = len(search_result) > 0

            return {
                "enrolled": is_enrolled,
                "score": search_result[0].score if is_enrolled else 0.0,
                "match": search_result[0].payload if is_enrolled else None,
                "liveness": liveness,
                "persons_count": len(faces),
                "bboxes": bboxes,
                "crops": crops_b64 if payload.return_crops else None,
                "model_meta": {
                    "name": config["model"]["name"],
                    "threshold": config["model"]["threshold"],
                    "quality_score": float(result.quality),
                    "version": "v1.0.0",
                },
            }

        except Exception as e:
            logging.error(f"Error in enrollment check: {e}")
            raise HTTPException(status_code=500, detail=str(e))

    return router
