import httpx
import asyncio
import logging
import pytz
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, AsyncGenerator
from backend.utils.normalizer import normalize_visit_data, fetch_and_prepare

class APIService:
    def __init__(
        self,
        base_url: str,
        api_key: str = None,
        limit: int = 50,
        category: str = "potential",
        time_range: str = "0,300,18000",
    ):
        self.base_url = base_url
        self.api_key = api_key
        self.limit = limit
        self.category = category
        self.time_range = time_range
        self.logger = logging.getLogger(__name__)

    async def fetch_page(
        self,
        branch_id: str,
        date: str,
        page: int,
        time_range: Optional[str] = None,
        retries: int = 3,
    ) -> Dict[str, Any]:
        """
        Fetch a single page for a specific date with retry logic.
        """
        effective_time_range = time_range if time_range is not None else self.time_range
        params = {
            "branchId": branch_id,
            "date": date,
            "page": page,
            "limit": self.limit,
            "category": self.category,
            "timeRange": effective_time_range,
            "excludeEmployee": "true",
            "excludeSingleEvent": "true",
            "excludeMissedService": "true",
            "isGroup": "true"
        }
        
        headers = {}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
            # Keep x-api-key as fallback if needed, but 401 often means Bearer is expected for JWTs
            headers["x-api-key"] = self.api_key

        for attempt in range(retries):
            try:
                async with httpx.AsyncClient() as client:
                    response = await client.get(self.base_url, params=params, headers=headers, timeout=20.0)
                    if response.status_code == 200:
                        return response.json()
                    
                    self.logger.warning(f"Attempt {attempt+1}: Received {response.status_code} for {date} page {page}")
            except Exception as e:
                self.logger.error(f"Attempt {attempt+1}: Error fetching {date} page {page}: {str(e)}")
            
            if attempt < retries - 1:
                await asyncio.sleep(1) # Backoff
        
        return {}

    async def fetch_visits_for_date(self, branch_id: str, date: str, time_range: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Loops through all pages for a single date.
        """
        all_visits = []
        page = 0
        
        while True:
            data = await self.fetch_page(branch_id, date, page, time_range=time_range)
            visits = data.get("visits", [])
            
            if not visits:
                break
                
            all_visits.extend(visits)
            self.logger.info(f"Date {date}: Page {page} fetched ({len(visits)} visits)")
            
            # If we got less than limit, it might be the last page
            if len(visits) < self.limit:
                break
                
            page += 1
            
        return all_visits

    async def fetch_visits_continuously(self, branch_id: str, interval_minutes: int = 5) -> AsyncGenerator[tuple[str, List[Dict[str, Any]]], None]:
        """
        Continuously fetch visits for the current date (IST/UTC+5:30).
        Yields (date_str, visits) periodically.
        """
        ist = pytz.timezone('Asia/Kolkata')
        
        while True:
            try:
                now_ist = datetime.now(ist)
                date_str = now_ist.strftime("%Y-%m-%d")
                self.logger.info(f"CONTINUOUS_FETCH: Initiating fetch for {date_str} at {now_ist.strftime('%H:%M:%S')} IST")
                
                # Use the existing logic to fetch all pages for today
                day_visits = await self.fetch_visits_for_date(branch_id, date_str)
                
                if day_visits:
                    yield date_str, day_visits
                else:
                    self.logger.info(f"CONTINUOUS_FETCH: No visits found yet for {date_str}")
                
            except Exception as e:
                self.logger.error(f"CONTINUOUS_FETCH: Error in loop: {e}", exc_info=True)
            
            self.logger.info(f"CONTINUOUS_FETCH: Sleeping for {interval_minutes} minutes...")
            await asyncio.sleep(interval_minutes * 60)

    async def fetch_visits(
        self,
        branch_id: str,
        start_date: str,
        end_date: str,
        time_range: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Deliverable: Primary function to fetch and normalize visit data for a date range.
        Returns {visits: [...], total: N}
        """
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")
        
        delta = end - start
        date_list = [(start + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(delta.days + 1)]
        
        raw_results = []
        for date_str in date_list:
            day_visits = await self.fetch_visits_for_date(branch_id, date_str, time_range=time_range)
            raw_results.extend(day_visits)
        
        # Normalize data
        normalized_visits = []
        for v in raw_results:
            nv = normalize_visit_data(v)
            if nv:
                normalized_visits.append(nv)
        
        return {
            "visits": normalized_visits,
            "total": len(normalized_visits),
        }
    async def send_conformation_action(self, branch_id: str, date: str, action_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Proxy call to v3 conformation/action API.
        Saves local log of actions sent.
        """
        api_url = "https://api.analytics.thefusionapps.com/api/v3/conformation/action"
        
        payload = {
            "id": str(action_data["id"]), # clusterId -> conformation_id
            "eventId": str(action_data["eventId"]),
            "approve": bool(action_data["approve"])
        }

        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
            "accept": "application/json, text/plain, */*"
        }

        try:
            async with httpx.AsyncClient() as client:
                res = await client.put(api_url, json=payload, headers=headers, timeout=30.0)
                
                result = {
                    "success": res.status_code in [200, 201],
                    "status_code": res.status_code,
                    "response": res.json() if res.content else None,
                    "error": None if res.is_success else res.text[:500]
                }
                
                # Local Logging
                self._log_action_locally(branch_id, date, payload, result)
                
                return result

        except Exception as e:
            self.logger.error(f"Error in send_conformation_action: {str(e)}")
            return {"success": False, "error": str(e)}

        return {"success": False, "error": "Unknown error in proxy pipeline"}

    def _log_action_locally(self, branch_id: str, date: str, payload: Dict[str, Any], result: Dict[str, Any]):
        """
        Saves result to core/conformation_actions_sent/{branchId}/{date}/actions.json
        """
        import json
        import os
        from datetime import datetime
        
        log_dir = os.path.join("core", "conformation_actions_sent", branch_id, date)
        os.makedirs(log_dir, exist_ok=True)
        log_file = os.path.join(log_dir, "actions.json")
        
        log_entry = {
            "clusterId": payload["id"],
            "eventId": payload["eventId"],
            "approve": payload["approve"],
            "status": "success" if result["success"] else "failed",
            "timestamp": datetime.now().isoformat()
        }
        
        # Load existing LOGs if any
        current_logs = []
        if os.path.exists(log_file):
            try:
                with open(log_file, "r") as f:
                    current_logs = json.load(f)
                    if not isinstance(current_logs, list):
                        current_logs = []
            except:
                current_logs = []
        
        current_logs.append(log_entry)
        
        try:
            with open(log_file, "w") as f:
                json.dump(current_logs, f, indent=2)
        except Exception as e:
            self.logger.error(f"Failed to write local action log: {e}")
