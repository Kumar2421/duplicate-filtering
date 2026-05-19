import json
import os
import requests
from pathlib import Path
import time

def download_employee_images(json_file):
    # Setup base directory
    base_dir = Path("data/employee_images")
    base_dir.mkdir(parents=True, exist_ok=True)
    
    with open(json_file, 'r') as f:
        data = json.load(f)
    
    employees = data.get("employees-temp", [])
    total = len(employees)
    print(f"Found {total} employees in JSON.")

    for i, emp in enumerate(employees):
        emp_id = emp.get("employeeId")
        img_str = emp.get("employeeImages", "")
        
        if not emp_id or not img_str:
            continue
            
        # Clean image string (it looks like "{url}")
        img_url = img_str.strip("{}").replace("\\/", "/")
        
        if not img_url.startswith("http"):
            continue

        # Create folder for employee
        emp_dir = base_dir / emp_id
        emp_dir.mkdir(exist_ok=True)
        
        # Determine filename
        ext = ".jpg"
        if ".png" in img_url.lower():
            ext = ".png"
        
        target_path = emp_dir / f"image_0{ext}"
        
        if target_path.exists():
            print(f"[{i+1}/{total}] Skipping {emp_id}, already exists.")
            continue

        print(f"[{i+1}/{total}] Downloading for {emp_id}...")
        try:
            response = requests.get(img_url, timeout=10)
            if response.status_code == 200:
                with open(target_path, 'wb') as f:
                    f.write(response.content)
                # Small sleep to be polite to the CDN
                time.sleep(0.1)
            else:
                print(f"  FAILED: Status {response.status_code}")
        except Exception as e:
            print(f"  ERROR: {str(e)}")

if __name__ == "__main__":
    JSON_PATH = "employees-temp 2026-05-05 10-48-47.json"
    download_employee_images(JSON_PATH)
