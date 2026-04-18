import asyncio
from typing import Any

import aiohttp

from utils.config import REQUEST_TIMEOUT, RETRY_ATTEMPTS, RETRY_BACKOFF
from utils.logger import get_logger

logger = get_logger(__name__)

type JsonPayload = dict[str, Any] | list[Any] | None


async def fetch_json(
    session: aiohttp.ClientSession,
    url: str,
    sem: asyncio.Semaphore,
) -> JsonPayload:
    """GET JSON with retry + exponential backoff."""
    async with sem:
        for attempt in range(1, RETRY_ATTEMPTS + 1):
            try:
                async with session.get(
                    url,
                    timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT),
                ) as resp:
                    if resp.status == 404:
                        return None
                    resp.raise_for_status()
                    return await resp.json()
            except (TimeoutError, aiohttp.ClientError) as exc:
                wait = RETRY_BACKOFF * (2 ** (attempt - 1))
                logger.warning(
                    "Attempt %d/%d failed for %s: %s. Retrying in %.1fs",
                    attempt,
                    RETRY_ATTEMPTS,
                    url,
                    exc,
                    wait,
                )
                if attempt == RETRY_ATTEMPTS:
                    logger.error("Giving up on %s", url)
                    return None
                await asyncio.sleep(wait)
