import httpx
import asyncio
import json

async def test_convert():
    # Using the correct local proxy URL
    url = "http://127.0.0.1:8000/api/convert"
    api_key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VyRGF0YSI6eyJpZCI6MTAsInVzZXJOYW1lIjoiRnVzaW9uIFN1cHBvcnQiLCJ0ZW5hbnRJZCI6MTAsInJvbGUiOiJ0ZW5hbnQiLCJzbHVnIjoiYW5hbHl0aWNzIn0sImJyYW5jaERhdGEiOnsiaWQiOjM0LCJicmFuY2hJZCI6IkVBLU5BVFVSQUxTIn0sImlhdCI6MTc3NDYxOTM4MywiZXhwIjoxNzc0NzA1NzgzfQ.5_IW_0ucX1SFmoHR8FgVtcPBRlJdTHJPRuEJz5zejhQ"
    
    payload = {
        "customerId1": "visitor-216196",
        "customerId2": "visitor-82168",
        "toEmployee": False,
        "branchId": "EA-NAT",
        "api_key": api_key
    }
    
    headers = {
        "Content-Type": "application/json",
        "accept": "application/json, text/plain, */*"
    }
    
    print(f"Testing Local Proxy Convert API: {url}")
    print(f"Payload: {json.dumps(payload, indent=2)}")
    
    try:
        async with httpx.AsyncClient() as client:
            res = await client.post(url, json=payload, headers=headers, timeout=120.0)
            print(f"Status Code: {res.status_code}")
            print(f"Response Body: {res.text}")
    except httpx.HTTPError as e:
        print(f"HTTP Error: {type(e).__name__}: {str(e)}")
    except Exception as e:
        print(f"Unexpected Error: {type(e).__name__}: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(test_convert())
