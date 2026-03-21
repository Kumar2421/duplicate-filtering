import httpx
import asyncio
import json

async def inspect_qdrant():
    # We must use the API to inspect Qdrant to avoid locking errors
    url = "http://127.0.0.1:8000/visits"
    params = {"branchId": "TMJ-CBE", "date": "2026-03-20"}
    
    async with httpx.AsyncClient() as client:
        try:
            print("Inspecting Qdrant data via /visits endpoint...")
            resp = await client.get(url, params=params)
            if resp.status_code == 200:
                data = resp.json()
                print(f"Total visits found in clusters: {data.get('total')}")
                if data.get('total') > 0:
                    print("Sample visit payload from cluster:")
                    print(json.dumps(data['visits'][0], indent=2))
                else:
                    print("No visits found. This might mean points aren't being stored or filter doesn't match.")
            else:
                print(f"Error calling /visits: {resp.status_code}")
        except Exception as e:
            print(f"Failed to connect to backend: {e}")

if __name__ == "__main__":
    asyncio.run(inspect_qdrant())
