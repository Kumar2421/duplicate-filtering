import httpx
import asyncio
import json
from datetime import datetime

async def test_ingestion():
    url = "http://localhost:8000/ingest-visits"
    params = {
        "branchId": "TMJ-CBE",
        "startDate": "2026-03-20",
        "endDate": "2026-03-20",
        "timeRange": "0,300,18000"
    }
    
    print(f"Testing ingestion with params: {params}")
    
    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(url, params=params, timeout=30.0)
            print(f"Status Code: {response.status_code}")
            print(f"Response Body: {json.dumps(response.json(), indent=2)}")
            
            if response.status_code == 200:
                print("Ingestion task started successfully in background.")
            else:
                print("Ingestion task failed to start.")
                
        except Exception as e:
            print(f"Error during test: {e}")

if __name__ == "__main__":
    # Wait a bit for server to start
    import time
    time.sleep(5)
    asyncio.run(test_ingestion())
