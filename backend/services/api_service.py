import json
import httpx
import asyncio
import logging
import pytz
import os
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional, AsyncGenerator, Set
from backend.utils.normalizer import normalize_visit_data, fetch_and_prepare

class APIService:
    def __init__(
        self,
        base_url: str,
        limit: int = 50,
        category: str = "potential",
        time_range: str = "0,300,18000",
        enabled: bool = True,
        configs: List[Dict[str, Any]] = None
    ):
        self.base_url = base_url
        self.limit = limit
        self.category = category
        self.time_range = time_range
        self.enabled = enabled
        self.configs = configs or []
        self.logger = logging.getLogger(__name__)
        self.state_dir = "data/state"
        os.makedirs(self.state_dir, exist_ok=True)

    def _get_api_key_for_branch(self, branch_id: str) -> Optional[str]:
        for cfg in self.configs:
            if cfg.get("branchId") == branch_id:
                return cfg.get("api_key")
        return None

    def _get_state_file(self, branch_id: str, date_str: str) -> str:
        return os.path.join(self.state_dir, f"seen_visits_{branch_id}_{date_str}.json")

    def _load_seen_visits(self, branch_id: str, date_str: str) -> Set[str]:
        state_file = self._get_state_file(branch_id, date_str)
        if os.path.exists(state_file):
            try:
                with open(state_file, 'r') as f:
                    return set(json.load(f))
            except Exception as e:
                self.logger.error(f"Error loading seen visits: {e}")
        return set()

    def _save_seen_visits(self, branch_id: str, date_str: str, seen_ids: Set[str]):
        state_file = self._get_state_file(branch_id, date_str)
        try:
            with open(state_file, 'w') as f:
                json.dump(list(seen_ids), f)
        except Exception as e:
            self.logger.error(f"Error saving seen visits: {e}")

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
        if not self.enabled:
            self.logger.info("API Fetching is disabled in config.")
            return {}

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
        api_key = self._get_api_key_for_branch(branch_id)
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"
            headers["x-api-key"] = api_key

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

    async def fetch_incremental_pages(self, branch_id: str, date: str, last_updated: Optional[str] = None) -> AsyncGenerator[List[Dict[str, Any]], None]:
        """
        Polls the API page by page. 
        Yields a page only if it contains visits newer than last_updated.
        Stops once a page contains visits older than last_updated (since they are sorted newest first).
        """
        page = 0
        last_ts = None
        if last_updated:
            try:
                # Normalize last_updated to UTC-aware datetime
                last_ts = datetime.fromisoformat(last_updated.replace('Z', '+00:00'))
                if last_ts.tzinfo is None:
                    last_ts = last_ts.replace(tzinfo=timezone.utc)
                else:
                    last_ts = last_ts.astimezone(timezone.utc)
            except Exception as e:
                self.logger.error(f"Error parsing last_updated '{last_updated}': {e}")
                last_ts = None

        while True:
            data = await self.fetch_page(branch_id, date, page)
            visits = data.get("visits", [])
            if not visits:
                break

            # OPTIMIZATION: If we are on page 0 and have a last_ts, check the first visit.
            # If the most recent visit is already older than or equal to last_ts,
            # we can stop immediately because the API returns visits sorted newest first.
            if page == 0 and last_ts:
                first_v = visits[0]
                v_updated_at = first_v.get("updatedAt")
                if v_updated_at:
                    try:
                        v_ts = datetime.fromisoformat(v_updated_at.replace('Z', '+00:00'))
                        if v_ts.tzinfo is None:
                            v_ts = v_ts.replace(tzinfo=timezone.utc)
                        else:
                            v_ts = v_ts.astimezone(timezone.utc)
                        
                        if v_ts <= last_ts:
                            self.logger.info(f"API_WATCH: No new data since {last_updated}. Skipping scan.")
                            break
                    except Exception as e:
                        self.logger.error(f"Error parsing first visit updatedAt: {e}")

            new_in_page = []
            reached_old_data = False
            
            for v in visits:
                v_updated_at = v.get("updatedAt")
                if not v_updated_at:
                    new_in_page.append(v)
                    continue
                
                try:
                    # Normalize v_ts to UTC-aware datetime
                    v_ts = datetime.fromisoformat(v_updated_at.replace('Z', '+00:00'))
                    if v_ts.tzinfo is None:
                        v_ts = v_ts.replace(tzinfo=timezone.utc)
                    else:
                        v_ts = v_ts.astimezone(timezone.utc)
                    
                    if not last_ts or v_ts > last_ts:
                        new_in_page.append(v)
                    else:
                        reached_old_data = True
                except Exception as e:
                    self.logger.error(f"Error parsing updatedAt for visit: {e}")
                    # If we can't parse, we skip it to be safe (don't treat as new by default)
                    continue

            if new_in_page:
                self.logger.info(f"API_WATCH: Yielding {len(new_in_page)} new visits from page {page}")
                yield new_in_page

            if reached_old_data or len(visits) < self.limit:
                break
            
            page += 1

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

        api_key = self._get_api_key_for_branch(branch_id)
        headers = {
            "Authorization": f"Bearer {api_key}" if api_key else "",
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

    async def send_convert_action(self, branch_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Proxy call to v3 convert API.
        """
        api_url = "https://api.analytics.thefusionapps.com/api/v3/convert"
        
        api_key = self._get_api_key_for_branch(branch_id)
        headers = {
            "Authorization": f"Bearer {api_key}" if api_key else "",
            "Content-Type": "application/json",
            "accept": "application/json, text/plain, */*"
        }

        try:
            async with httpx.AsyncClient() as client:
                res = await client.post(api_url, json=payload, headers=headers, timeout=30.0)
                
                return {
                    "success": res.status_code in [200, 201],
                    "status_code": res.status_code,
                    "response": res.json() if res.content else None,
                    "error": None if res.is_success else res.text[:500]
                }
        except Exception as e:
            self.logger.error(f"Error in send_convert_action: {str(e)}")
            return {"success": False, "error": str(e)}

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
