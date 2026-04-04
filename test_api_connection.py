import asyncio
import httpx
import json
import urllib3
import sys


# Suppress insecure request warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

async def debug_missing_visits():
    # Target branch and date
    branch_id = "TMJ-CBE"
    date = "2026-03-30"
    
    # Using the API key from config for TMJ-CBE
    api_key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VyRGF0YSI6eyJpZCI6MTAsInVzZXJOYW1lIjoiRnVzaW9uIFN1cHBvcnQiLCJ0ZW5hbnRJZCI6MTAsInJvbGUiOiJ0ZW5hbnQiLCJzbHVnIjoiYW5hbHl0aWNzIn0sImJyYW5jaERhdGEiOnsiaWQiOjUyLCJicmFuY2hJZCI6IlRNSi1DQkUifSwiaWF0IjoxNzc0OTM5MzQ3LCJleHAiOjE3NzUwMjU3NDd9.O5VC2W450XoYGdfQn5J3eVQ16GvkLY2IDiG0t6JhgsY"
    
    base_url = "https://api.analytics.thefusionapps.com/api/v4/retail/visit-stats"
    
    # Missing visitor IDs provided by user
    missing_visitors = ["visitor-291236", "visitor-291967", "visitor-291970", "visitor-291822", "visitor-292131"]
    
    # Parameters matching APIService.fetch_page
    params = {
        "branchId": branch_id,
        "date": date,
        "page": 0,
        "limit": 1000,
        "category": "all",
        "excludeEmployee": "true", # User says this should be true
        "excludeSingleEvent": "true",
        "excludeMissedService": "true",
        "isGroup": "true"
    }
    
    headers = {
        "Authorization": f"Bearer {api_key}",
        "x-api-key": api_key
    }

    print(f"--- Debugging Missing Visits for {branch_id} on {date} ---")
    print(f"Parameters: {json.dumps(params, indent=2)}")
    
    found_missing = {vid: False for vid in missing_visitors}
    all_visits = []
    page = 0
    
    async with httpx.AsyncClient(verify=False, timeout=60.0) as client:
        while True:
            params["page"] = page
            print(f"Fetching page {page} with params: {params}")
            try:
                # Add retry logic for 502 errors
                for attempt in range(3):
                    response = await client.get(base_url, params=params, headers=headers)
                    if response.status_code == 200:
                        break
                    print(f"Attempt {attempt+1} failed with {response.status_code}. Retrying...")
                    await asyncio.sleep(2)
                
                if response.status_code != 200:
                    print(f"Final Error: Received status code {response.status_code}")
                    break
                
                data = response.json()
                visits = data.get("visits", [])
                if not visits:
                    print("No more visits found.")
                    break
                
                all_visits.extend(visits)
                
                # Check for missing visitors on this page
                for v in visits:
                    v_id = v.get("customerId")
                    if v_id in found_missing:
                        found_missing[v_id] = True
                        print(f"FOUND: {v_id} on page {page} (isEmployee: {v.get('isEmployee')}, visitId: {v.get('id')})")
                
                if len(visits) < params["limit"]:
                    break
                    
                page += 1
                if page > 20: # Increased page limit
                    print("Reached page limit safety break.")
                    break
                    
            except Exception as e:
                print(f"Exception during fetch: {e}")
                break

        print("\n--- Summary ---")
        print(f"Total visits fetched: {len(all_visits)}")
        for vid, found in found_missing.items():
            status = "FOUND" if found else "NOT FOUND"
            print(f"{vid}: {status}")

        if not all(found_missing.values()):
            print("\nChecking if those visitors appear if we change parameters...")
            # Try without filters
            params["excludeEmployee"] = "false"
            params["excludeSingleEvent"] = "false"
            params["excludeMissedService"] = "false"
            params["page"] = 0
            print(f"\nRetrying Page 0 with all filters OFF...")
            try:
                response = await client.get(base_url, params=params, headers=headers)
                if response.status_code == 200:
                    data = response.json()
                    visits = data.get("visits", [])
                    for v in visits:
                        v_id = v.get("customerId")
                        if v_id in missing_visitors:
                            print(f"FOUND {v_id} with filters OFF! (isEmployee: {v.get('isEmployee')}, visitId: {v.get('id')})")
                else:
                    print(f"Retry failed with status {response.status_code}")
            except Exception as e:
                print(f"Exception during retry: {e}")

if __name__ == "__main__":
    asyncio.run(debug_missing_visits())

