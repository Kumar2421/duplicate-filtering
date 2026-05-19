import os
import cv2
import glob
import yaml
from pathlib import Path
from tqdm import tqdm

def convert_yolo_to_classification(data_yaml_path, output_base_dir):
    """
    Converts a YOLO detection dataset to a classification dataset by cropping bounding boxes.
    Positive: The actual object (ID Card).
    Negative: Random background crops from the same image that don't overlap with the object.
    """
    with open(data_yaml_path, 'r') as f:
        data = yaml.safe_load(f)
    
    # Paths in YAML might be relative or absolute, adjusting based on user's structure
    # The user's YAML shows paths like /home/fusion-gpu/fusion-projects/idcard-deduction/...
    # But the files are actually in /home/fusion-gpu/fusion-projects/duplicate-filtering/dataset /...
    
    dataset_dir = Path(data_yaml_path).parent
    splits = ['train', 'val', 'test']
    
    for split in splits:
        print(f"Processing {split} split...")
        
        # Determine image and label directories
        # Roboflow usually structure: split/images and split/labels
        img_dir = dataset_dir / split / 'images'
        lbl_dir = dataset_dir / split / 'labels'
        
        if not img_dir.exists():
            print(f"Warning: {img_dir} does not exist. Skipping.")
            continue
            
        # Output dirs for classification
        out_pos = Path(output_base_dir) / split / 'id_card'
        out_neg = Path(output_base_dir) / split / 'no_id_card'
        out_pos.mkdir(parents=True, exist_ok=True)
        out_neg.mkdir(parents=True, exist_ok=True)
        
        img_files = glob.glob(str(img_dir / "*.jpg")) + glob.glob(str(img_dir / "*.png")) + glob.glob(str(img_dir / "*.jpeg"))
        
        for img_path in tqdm(img_files):
            img = cv2.imread(img_path)
            if img is None:
                continue
            
            h, w, _ = img.shape
            basename = Path(img_path).stem
            lbl_path = lbl_dir / f"{basename}.txt"
            
            bboxes = []
            if lbl_path.exists():
                with open(lbl_path, 'r') as f:
                    lines = f.readlines()
                    for line in lines:
                        parts = line.strip().split()
                        if len(parts) == 5:
                            cls, x_c, y_c, bw, bh = map(float, parts)
                            # Convert YOLO normalized to pixel coordinates
                            x1 = int((x_c - bw/2) * w)
                            y1 = int((y_c - bh/2) * h)
                            x2 = int((x_c + bw/2) * w)
                            y2 = int((y_c + bh/2) * h)
                            
                            # Clip to image boundaries
                            x1, y1 = max(0, x1), max(0, y1)
                            x2, y2 = min(w, x2), min(h, y2)
                            
                            if x2 > x1 and y2 > y1:
                                crop = img[y1:y2, x1:x2]
                                crop_name = f"{basename}_pos_{len(bboxes)}.jpg"
                                cv2.imwrite(str(out_pos / crop_name), crop)
                                bboxes.append((x1, y1, x2, y2))
            
            # Generate Negative samples (Background crops)
            # We take a few crops that don't overlap significantly with any ID card
            num_neg_per_img = 2
            attempts = 0
            negs_created = 0
            while negs_created < num_neg_per_img and attempts < 10:
                attempts += 1
                # Random size between 10% and 30% of image
                nw = int(w * (0.1 + 0.2 * 0.5)) 
                nh = int(h * (0.1 + 0.2 * 0.5))
                nx1 = int((w - nw) * 0.5) # simplified for now, could be random
                ny1 = int((h - nh) * 0.5)
                # Actually random
                import random
                nx1 = random.randint(0, max(0, w - nw))
                ny1 = random.randint(0, max(0, h - nh))
                nx2, ny2 = nx1 + nw, ny1 + nh
                
                # Check overlap with any bbox
                overlap = False
                for bx1, by1, bx2, by2 in bboxes:
                    # Simple intersection check
                    ix1, iy1 = max(nx1, bx1), max(ny1, by1)
                    ix2, iy2 = min(nx2, bx2), min(ny2, by2)
                    if ix2 > ix1 and iy2 > iy1:
                        area_int = (ix2 - ix1) * (iy2 - iy1)
                        if area_int > 0:
                            overlap = True
                            break
                
                if not overlap:
                    neg_crop = img[ny1:ny2, nx1:nx2]
                    neg_name = f"{basename}_neg_{negs_created}.jpg"
                    cv2.imwrite(str(out_neg / neg_name), neg_crop)
                    negs_created += 1

if __name__ == "__main__":
    # Existing Datasets
    yaml1 = "/home/fusion-gpu/fusion-projects/duplicate-filtering/dataset /ID_Card_Detection.v1i.yolov8/data.yaml"
    yaml2 = "/home/fusion-gpu/fusion-projects/duplicate-filtering/dataset /id_card_new.v1i.yolov8/data.yaml"
    # New Dataset from Roboflow
    yaml3 = "/home/fusion-gpu/fusion-projects/duplicate-filtering/training/roboflow_new/data.yaml"
    
    output_dir = "/home/fusion-gpu/fusion-projects/duplicate-filtering/training/classification_dataset"
    
    # Process all datasets
    yamls = [yaml1, yaml2, yaml3]
    for y in yamls:
        if os.path.exists(y):
            print(f"\nConverting Dataset: {y}")
            convert_yolo_to_classification(y, output_dir)
        else:
            print(f"Skipping missing dataset: {y}")
    
    print("\nConversion Complete!")
