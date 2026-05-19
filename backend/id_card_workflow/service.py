import os
import torch
import torch.nn as nn
from torchvision import models, transforms
from PIL import Image
import shutil
import uuid
import json
from datetime import datetime
from pathlib import Path
from fastapi import UploadFile, File, Form, HTTPException
from fastapi.responses import FileResponse

# Configuration
BASE_DIR = Path("/home/fusion-gpu/fusion-projects/duplicate-filtering/backend/id_card_workflow")
STORAGE_DIR = BASE_DIR / "storage"
PENDING_DIR = STORAGE_DIR / "pending"
VERIFIED_DIR = STORAGE_DIR / "verified"
ID_CARD_DIR = VERIFIED_DIR / "id_card"
NO_ID_CARD_DIR = VERIFIED_DIR / "no_id_card"
MODEL_PATH = "/home/fusion-gpu/fusion-projects/duplicate-filtering/training/id_card_classifier_v2.pth"
ID_THRESHOLD = 0.5  # Classification threshold

# Global Model Instance (Lazy loaded)
_id_model = None
_id_device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
_id_transform = transforms.Compose([
    transforms.Resize((224, 224)),
    transforms.ToTensor(),
    transforms.Normalize([0.485, 0.456, 0.406], [0.229, 0.224, 0.225])
])

def init_id_workflow():
    """Create directories and initialize model"""
    for d in [PENDING_DIR, ID_CARD_DIR, NO_ID_CARD_DIR]:
        d.mkdir(parents=True, exist_ok=True)
    
    global _id_model
    if _id_model is None:
        _id_model = models.efficientnet_b0()
        num_ftrs = _id_model.classifier[1].in_features
        _id_model.classifier[1] = nn.Linear(num_ftrs, 2)
        
        if os.path.exists(MODEL_PATH):
            _id_model.load_state_dict(torch.load(MODEL_PATH, map_location=_id_device))
            _id_model.to(_id_device)
            _id_model.eval()
            print(f"ID Workflow: Model loaded from {MODEL_PATH}")
        else:
            print(f"ID Workflow: Warning - Model not found at {MODEL_PATH}")

async def process_image_internal(file_path: Path, original_filename: str):
    """
    Core logic moved from FastAPI endpoint to internal function.
    """
    file_id = str(uuid.uuid4())
    ext = Path(original_filename).suffix or ".jpg"
    filename = f"{file_id}{ext}"
    pending_path = PENDING_DIR / filename
    
    # Move from temp to pending
    shutil.move(str(file_path), str(pending_path))
    
    # Inference
    try:
        if _id_model is None:
            init_id_workflow()
            
        image = Image.open(pending_path).convert('RGB')
        input_tensor = _id_transform(image).unsqueeze(0).to(_id_device)
        
        with torch.no_grad():
            outputs = _id_model(input_tensor)
            probabilities = torch.nn.functional.softmax(outputs, dim=1)
            prob_id = probabilities[0][0].item()
            prob_no_id = probabilities[0][1].item()
        
        # Use configurable threshold for classification
        prediction = "id_card" if prob_id >= ID_THRESHOLD else "no_id_card"
        confidence = prob_id if prediction == "id_card" else prob_no_id
        
        # Save metadata
        meta = {
            "filename": filename,
            "prediction": prediction,
            "confidence": confidence,
            "timestamp": datetime.now().isoformat()
        }
        with open(pending_path.with_suffix(".json"), "w") as f:
            json.dump(meta, f)
            
        return {"status": "success", "file_id": file_id, "prediction": prediction, "confidence": confidence}
    except Exception as e:
        return {"status": "error", "message": str(e)}

def list_pending_internal():
    images = []
    for f in PENDING_DIR.glob("*.json"):
        with open(f, "r") as meta_file:
            images.append(json.load(meta_file))
    return images

def get_image_path(filename: str):
    file_path = PENDING_DIR / filename
    if not file_path.exists():
        return None
    return file_path

def verify_image_internal(filename: str, is_id_card: bool):
    src_img = PENDING_DIR / filename
    src_meta = src_img.with_suffix(".json")
    
    if not src_img.exists():
        return False
    
    target_dir = ID_CARD_DIR if is_id_card else NO_ID_CARD_DIR
    shutil.move(str(src_img), str(target_dir / filename))
    
    if src_meta.exists():
        os.remove(src_meta)
    return True
