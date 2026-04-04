import base64
import json
import logging
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional

import httpx


@dataclass
class _CachedToken:
    token: str
    exp_epoch: Optional[int]
    fetched_at_epoch: int


class AnalyticsAuthService:
    def __init__(
        self,
        *,
        email: Optional[str] = None,
        password: Optional[str] = None,
        device_id: Optional[str] = None,
        login_url: Optional[str] = None,
        branch_switch_url: Optional[str] = None,
        origin: Optional[str] = None,
        referer: Optional[str] = None,
        accept_language: Optional[str] = None,
        token_refresh_skew_seconds: int = 120,
    ):
        self.logger = logging.getLogger(__name__)

        self.email = email or os.getenv("ANALYTICS_EMAIL")
        self.password = password or os.getenv("ANALYTICS_PASSWORD")
        self.device_id = device_id or os.getenv("ANALYTICS_DEVICE_ID")

        self.login_url = login_url or os.getenv("ANALYTICS_LOGIN_URL") or "https://auth.analytics.thefusionapps.com/api/auth/login"
        self.branch_switch_url = (
            branch_switch_url
            or os.getenv("ANALYTICS_BRANCH_SWITCH_URL")
            or "https://api.analytics.thefusionapps.com/api/branch/branches/change"
        )

        self.origin = origin or os.getenv("ANALYTICS_ORIGIN") or "https://analytics.develop.thefusionapps.com"
        self.referer = referer or os.getenv("ANALYTICS_REFERER") or "https://analytics.develop.thefusionapps.com/"
        self.accept_language = accept_language or os.getenv("ANALYTICS_ACCEPT_LANGUAGE") or "en-US,en;q=0.9,en-IN;q=0.8"

        self.token_refresh_skew_seconds = int(token_refresh_skew_seconds)

        self._tenant_token: Optional[_CachedToken] = None
        self._branch_tokens: Dict[str, _CachedToken] = {}

    def _base_headers(self) -> Dict[str, str]:
        headers: Dict[str, str] = {
            "accept": "application/json, text/plain, */*",
            "accept-language": self.accept_language,
            "origin": self.origin,
            "referer": self.referer,
            "dnt": "1",
            "x-cache-bypass": "true",
        }
        return headers

    @staticmethod
    def _decode_jwt_exp_epoch(token: str) -> Optional[int]:
        try:
            parts = token.split(".")
            if len(parts) < 2:
                return None
            payload_b64 = parts[1]
            padding = "=" * (-len(payload_b64) % 4)
            payload_raw = base64.urlsafe_b64decode(payload_b64 + padding)
            payload = json.loads(payload_raw.decode("utf-8"))
            exp = payload.get("exp")
            if exp is None:
                return None
            return int(exp)
        except Exception:
            return None

    def _is_token_valid(self, cached: Optional[_CachedToken]) -> bool:
        if not cached or not cached.token:
            return False
        if cached.exp_epoch is None:
            return True
        now = int(time.time())
        return (cached.exp_epoch - now) > self.token_refresh_skew_seconds

    @staticmethod
    def _get_json_path(obj: Any, path: str) -> Optional[Any]:
        cur: Any = obj
        for part in path.split("."):
            if not part:
                return None
            if not isinstance(cur, dict) or part not in cur:
                return None
            cur = cur[part]
        return cur

    @classmethod
    def _extract_token_from_response(cls, data: Any) -> Optional[str]:
        if isinstance(data, str) and data.count(".") >= 2:
            return data

        if not isinstance(data, dict):
            return None

        candidates = [
            "token",
            "accessToken",
            "data.token",
            "data.accessToken",
            "result.token",
            "payload.token",
        ]
        for p in candidates:
            v = cls._get_json_path(data, p)
            if isinstance(v, str) and v:
                return v
        return None

    async def _post_json(self, url: str, headers: Dict[str, str], payload: Dict[str, Any], timeout_seconds: float = 20.0) -> Any:
        timeout = httpx.Timeout(timeout_seconds, connect=min(10.0, timeout_seconds))
        async with httpx.AsyncClient(verify=False, timeout=timeout, http2=True) as client:
            res = await client.post(url, headers=headers, json=payload)
            if res.status_code >= 400:
                raise RuntimeError(f"Upstream error {res.status_code}: {res.text[:500]}")
            return res.json() if res.content else None

    async def get_tenant_token(self, *, force_refresh: bool = False) -> str:
        if not force_refresh and self._is_token_valid(self._tenant_token):
            return self._tenant_token.token

        if not self.email or not self.password:
            raise RuntimeError("Missing ANALYTICS_EMAIL / ANALYTICS_PASSWORD env vars")

        headers = {**self._base_headers(), "content-type": "application/json"}
        payload: Dict[str, Any] = {"email": self.email, "password": self.password}
        if self.device_id:
            payload["device_id"] = self.device_id

        data = await self._post_json(self.login_url, headers, payload)
        token = self._extract_token_from_response(data)
        if not token:
            raise RuntimeError("Login succeeded but token not found in response")

        cached = _CachedToken(token=token, exp_epoch=self._decode_jwt_exp_epoch(token), fetched_at_epoch=int(time.time()))
        self._tenant_token = cached
        return token

    async def get_branch_token(self, branch_id: str, *, force_refresh: bool = False) -> str:
        branch_id = str(branch_id)
        cached = self._branch_tokens.get(branch_id)
        if not force_refresh and self._is_token_valid(cached):
            return cached.token

        tenant_token = await self.get_tenant_token(force_refresh=False)

        headers = {**self._base_headers(), "content-type": "application/json"}
        headers["Authorization"] = f"Bearer {tenant_token}"

        payload: Dict[str, Any] = {"branchId": branch_id}
        data = await self._post_json(self.branch_switch_url, headers, payload)

        token = self._extract_token_from_response(data)
        if not token:
            # Some APIs might return the new token embedded deeper; keep error readable.
            raise RuntimeError(f"Branch switch succeeded but token not found in response for branchId={branch_id}")

        new_cached = _CachedToken(token=token, exp_epoch=self._decode_jwt_exp_epoch(token), fetched_at_epoch=int(time.time()))
        self._branch_tokens[branch_id] = new_cached
        return token

    async def get_auth_headers(self, branch_id: str) -> Dict[str, str]:
        token = await self.get_branch_token(branch_id)
        headers = self._base_headers()
        headers["Authorization"] = f"Bearer {token}"
        return headers
