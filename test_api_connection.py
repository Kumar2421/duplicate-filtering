import asyncio
import httpx
import json
import urllib3

# Suppress insecure request warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

async def test_delete():
    visit_id = "685153"
    event_id = "a447086b-d8e8-40ff-b4b7-1f3de06f931c-3282026-123219PM"
    url = f"https://api.analytics.thefusionapps.com/api/v2/retail/delete/event/{visit_id}/{event_id}"
    
    headers = {
        "Authorization": "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VyRGF0YSI6eyJpZCI6MTAsInVzZXJOYW1lIjoiRnVzaW9uIFN1cHBvcnQiLCJ0ZW5hbnRJZCI6MTAsInJvbGUiOiJ0ZW5hbnQiLCJzbHVnIjoiYW5hbHl0aWNzIn0sImJyYW5jaERhdGEiOnsiaWQiOjM0LCJicmFuY2hJZCI6IkVBLU5BVFVSQUxTIn0sImlhdCI6MTc3NDYxOTM4MywiZXhwIjoxNzc0NzA1NzgzfQ.5_IW_0ucX1SFmoHR8FgVtcPBRlJdTHJPRuEJz5zejhQ",
        "Content-Type": "application/json",
        "accept": "application/json, text/plain, */*",
        "referer": "https://analytics.develop.thefusionapps.com/",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36 Edg/146.0.0.0",
        "dnt": "1",
        "sec-ch-ua": '\"Chromium\";v=\"146\", \"Not-A.Brand\";v=\"24\", \"Microsoft Edge\";v=\"146\"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '\"Windows\"',
        "origin": "https://analytics.develop.thefusionapps.com",
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-site",
        "x-cache-bypass": "true"
    }

    print(f"Testing POST to: {url}")
    async with httpx.AsyncClient(verify=False) as client:
        try:
            response = await client.post(url, headers=headers)
            print(f"Status Code: {response.status_code}")
            print(f"Response Text: {response.text}")
        except Exception as e:
            print(f"Error during request: {e}")

if __name__ == "__main__":
    asyncio.run(test_delete())
