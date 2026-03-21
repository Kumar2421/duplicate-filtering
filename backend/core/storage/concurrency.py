from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager


@asynccontextmanager
async def bounded_semaphore(limit: int):
    sem = asyncio.Semaphore(limit)

    async def run(coro):
        async with sem:
            return await coro

    yield run
