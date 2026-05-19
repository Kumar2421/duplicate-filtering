import os
import requests
from pathlib import Path
from typing import List, Dict, Any

def bulk_enroll_employees(images_base_dir: str, branch_id: str, api_url: str):
    images_path = Path(images_base_dir).resolve()
    if not images_path.exists():
        print(f"Error: Images directory {images_path} not found.")
        return

    # Scan the directory for subfolders (each folder name is an employeeId)
    emp_folders = [f for f in images_path.iterdir() if f.is_dir()]
    total = len(emp_folders)
    
    print(f"Starting bulk enrollment for {total} employees in branch {branch_id}...")
    print(f"Using absolute path base: {images_path}")

    success_count = 0
    fail_count = 0

    for i, emp_dir in enumerate(emp_folders):
        emp_id = emp_dir.name
        name = emp_id 
        
        # Look for files starting with 'image' and having an extension
        image_files = sorted(list(emp_dir.glob("image*.*")))
        if not image_files:
            print(f"[{i+1}/{total}] Skipping {emp_id}: No image found in {emp_dir}.")
            continue
            
        # Use absolute path so the backend can find it regardless of its CWD
        image_file_abs = str(image_files[0].resolve())

        # Prepare payload with absolute path
        payload = {
            "branchId": branch_id,
            "employeeId": emp_id,
            "name": name,
            "image": image_file_abs
        }

        print(f"[{i+1}/{total}] Enrolling {emp_id}... ", end="", flush=True)
        try:
            response = requests.post(f"{api_url}/api/employees/enroll", json=payload, timeout=60)
            if response.status_code == 200:
                print("SUCCESS")
                success_count += 1
            else:
                print(f"FAILED (Status {response.status_code}: {response.text})")
                fail_count += 1
        except Exception as e:
            print(f"ERROR: {e}")
            fail_count += 1

    print(f"\nEnrollment Finished!")
    print(f"Total Folders: {total}")
    print(f"Success: {success_count}")
    print(f"Failed: {fail_count}")

if __name__ == "__main__":
    # Correct path as discovered via 'ls' earlier
    IMAGES_DIR = "employee-download"
    BRANCH_ID = "TMJ-CBE"
    API_URL = "http://localhost:8009"
    
    bulk_enroll_employees(IMAGES_DIR, BRANCH_ID, API_URL)
