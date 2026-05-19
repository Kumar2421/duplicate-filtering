from __future__ import annotations
import logging
from fastapi import APIRouter, HTTPException, Depends, Query, Body
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
import base64
import cv2
import numpy as np
import httpx
from pathlib import Path

from core.services.employee_enrollment_service import EmployeeEnrollmentService
from core.ml.embedding_service import EmbeddingService
from core.storage.file_manager import FileManager

class EmployeeEnrollmentRequest(BaseModel):
    branchId: str
    employeeId: str
    name: str
    image: str  # Base64 or local path

def create_employees_router(
    employee_enrollment_service: EmployeeEnrollmentService,
    embedding_service: EmbeddingService,
    file_manager: FileManager
) -> APIRouter:
    router = APIRouter()
    logger = logging.getLogger(__name__)

    @router.post("/employees/enroll")
    async def enroll_employee(payload: EmployeeEnrollmentRequest):
        try:
            # 1. Decode image from various possible formats
            image_bytes = None
            source_type = "unknown"
            
            try:
                if payload.image.startswith("data:image"):
                    # Handle base64 data URI
                    source_type = "base64_uri"
                    header, encoded = payload.image.split(",", 1)
                    image_bytes = base64.b64decode(encoded)
                elif payload.image.startswith(("http://", "https://")):
                    # Handle URL
                    source_type = "url"
                    async with httpx.AsyncClient() as client:
                        resp = await client.get(payload.image, timeout=20.0)
                        if resp.status_code == 200:
                            image_bytes = resp.content
                        else:
                            raise HTTPException(status_code=400, detail=f"Failed to fetch image from URL: {resp.status_code}")
                elif payload.image.startswith("/"):
                    # Handle local absolute path
                    source_type = "local_path"
                    p = Path(payload.image)
                    if p.exists() and p.is_file():
                        image_bytes = p.read_bytes()
                    else:
                        raise HTTPException(status_code=404, detail=f"Local image file not found: {payload.image}")
                else:
                    # Attempt direct base64 decode as fallback
                    source_type = "base64_fallback"
                    try:
                        image_bytes = base64.b64decode(payload.image)
                    except Exception:
                        raise HTTPException(status_code=400, detail="Invalid image data format. Must be base64, URL, or local path.")
            except Exception as e:
                logger.error(f"Error reading image source ({source_type}): {e}")
                raise HTTPException(status_code=400, detail=f"Error reading image source: {str(e)}")

            if not image_bytes:
                raise HTTPException(status_code=400, detail="Could not retrieve image bytes from the provided source")

            img_bgr = file_manager.validate_and_decode(image_bytes)
            if img_bgr is None:
                logger.error(f"Image decoding failed for source type: {source_type}")
                raise HTTPException(status_code=400, detail="Failed to decode image data into a valid format")

            # 2. Extract embedding
            result = embedding_service.extract_face_features(img_bgr)
            if not result or result.embedding is None:
                raise HTTPException(status_code=400, detail="No face detected in the provided image")

            # 3. Save Image Branch-wise
            image_url = file_manager.save_employee_image(payload.branchId, payload.employeeId, img_bgr)

            # 4. Enroll in Qdrant
            success = employee_enrollment_service.enroll_employee(
                branch_id=payload.branchId,
                employee_id=payload.employeeId,
                name=payload.name,
                embedding=result.embedding.tolist() if isinstance(result.embedding, np.ndarray) else result.embedding,
                metadata={
                    "quality": float(result.quality), 
                    "source_type": source_type,
                    "image": image_url
                }
            )

            if not success:
                raise HTTPException(status_code=500, detail="Failed to enroll employee in database")

            return {"success": True, "message": f"Employee {payload.employeeId} enrolled successfully", "image": image_url}

        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Enrollment API unexpected error: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=f"Internal server error during enrollment: {str(e)}")

    @router.get("/employees/list")
    async def list_employees(branchId: str = Query(...)):
        employees = employee_enrollment_service.list_employees(branchId)
        return {"success": True, "employees": employees}

    @router.delete("/employees/{employee_id}")
    async def delete_employee(employee_id: str, branchId: str = Query(...)):
        success = employee_enrollment_service.delete_employee(branchId, employee_id)
        if not success:
            raise HTTPException(status_code=500, detail="Failed to delete employee")
        return {"success": True, "message": f"Employee {employee_id} deleted"}

    return router
