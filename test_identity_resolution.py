import httpx
import asyncio
import json

async def test_clustering():
    # 1. Trigger ingestion first to ensure we have data in Qdrant
    ingest_url = "http://localhost:8000/ingest-visits"
    params = {
        "branchId": "TMJ-CBE",
        "startDate": "2026-03-20",
        "endDate": "2026-03-20",
        "timeRange": "0,300,18000"
    }
    
    async with httpx.AsyncClient() as client:
        print(f"Triggering ingestion...")
        await client.post(ingest_url, params=params)
        
        # Wait for background ingestion to process some visits
        print("Waiting 15s for ingestion to process...")
        await asyncio.sleep(15)
        
        # 2. Test /visits endpoint (triggers clustering and saves JSON)
        visits_url = "http://localhost:8000/visits"
        print(f"Testing /visits endpoint...")
        resp = await client.get(visits_url, params={"branchId": "TMJ-CBE", "date": "2026-03-20"})
        
        if resp.status_code == 200:
            data = resp.json()
            print(f"Success! Found {data.get('total')} visits in clusters.")
            print(f"Metrics: {data.get('metrics')}")
        else:
            print(f"Failed /visits: {resp.status_code} - {resp.text}")

        # 3. Test /duplicates endpoint
        dupes_url = "http://localhost:8000/duplicates"
        print(f"Testing /duplicates endpoint...")
        resp = await client.get(dupes_url, params={"branchId": "TMJ-CBE", "date": "2026-03-20"})
        
        if resp.status_code == 200:
            data = resp.json()
            print(f"Success! Found {data.get('total')} duplicate/conflict clusters.")
        else:
            print(f"Failed /duplicates: {resp.status_code}")

if __name__ == "__main__":
    asyncio.run(test_clustering())
