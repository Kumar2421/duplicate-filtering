from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Optional

import httpx

from ..config.settings import settings


@dataclass(frozen=True)
class DownloadResult:
    url: str
    content: Optional[bytes]
    status_code: Optional[int]
    error: Optional[str]


class HttpDownloader:
    def __init__(self):
        self._timeout = httpx.Timeout(settings.DOWNLOAD_TIMEOUT_SECONDS)

    async def download_batch(self, urls: List[str], retries: int | None = None) -> List[DownloadResult]:
        """Download multiple URLs in parallel using a single HTTP client."""
        retries = settings.DOWNLOAD_RETRIES if retries is None else retries
        results = []
        
        async with httpx.AsyncClient(timeout=self._timeout, follow_redirects=True) as client:
            tasks = [self._download_with_client(client, url, retries) for url in urls]
            results = await asyncio.gather(*tasks)
        
        return results

    async def _download_with_client(self, client: httpx.AsyncClient, url: str, retries: int) -> DownloadResult:
        last_error: Optional[str] = None
        for attempt in range(retries):
            try:
                resp = await client.get(url)
                if resp.status_code == 200 and resp.content:
                    return DownloadResult(url=url, content=resp.content, status_code=resp.status_code, error=None)
                last_error = f"http_status={resp.status_code}"
            except Exception as e:
                last_error = str(e)

            if attempt < retries - 1:
                await asyncio.sleep(0.5 * (attempt + 1)) # Incremental backoff

        return DownloadResult(url=url, content=None, status_code=None, error=last_error)
