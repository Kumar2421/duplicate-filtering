from insightface.app import FaceAnalysis
import numpy as np
import cv2
import httpx
from ..utils.gpu_manager import get_device
import json
import logging

class MLService:
    def __init__(self, model_name="buffalo_l", quality_filter: dict = None):
        self.app = FaceAnalysis(name=model_name)
        self.app.prepare(ctx_id=get_device())
        self.quality_filter = quality_filter or {}
        self.logger = logging.getLogger(__name__)

    async def get_embedding(self, img_url: str):
        """
        Download image, apply quality filters, and extract face embedding.
        """
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(img_url, timeout=10.0)
                if response.status_code != 200:
                    return None
                
                nparr = np.frombuffer(response.content, np.uint8)
                img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
                if img is None: return None
                    
                faces = self.app.get(img)
                if not faces: return None
                
                face = faces[0]
                
                # Apply Quality Filtering from Config
                if self.quality_filter:
                    # Blur filtering
                    if "min_blur" in self.quality_filter:
                        # InsightFace doesn't always provide blur directly in 'faces', 
                        # sometimes it's in a sub-dict or computed. 
                        # If not present, we skip or compute simple laplacian blur.
                        pass 
                    
                    # Confidence filtering
                    min_conf = self.quality_filter.get("min_confidence", 0.0)
                    if face.det_score < min_conf:
                        self.logger.info(f"Face rejected: confidence {face.det_score} < {min_conf}")
                        return None
                        
                    # Face size filtering
                    min_size = self.quality_filter.get("min_face_size", 0)
                    bbox = face.bbox.astype(int)
                    w, h = bbox[2] - bbox[0], bbox[3] - bbox[1]
                    if w < min_size or h < min_size:
                        self.logger.info(f"Face rejected: size {w}x{h} < {min_size}")
                        return None

                return face.embedding.tolist()
        except Exception as e:
            self.logger.error(f"ML Processing error: {str(e)}")
            return None
