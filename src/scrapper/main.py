import asyncio
import time

import aiohttp

from scrapper.fetch import fetch_members, fetch_proceedings, fetch_sitting
from scrapper.write import write_all_parquet
from utils.config import CONCURRENCY
from utils.logger import get_logger

logger = get_logger(__name__)


async def fetch_data() -> None:
    sem = asyncio.Semaphore(CONCURRENCY)

    connector = aiohttp.TCPConnector(limit=CONCURRENCY + 2)
    headers = {"Accept": "application/json"}

    t0 = time.perf_counter()
    async with aiohttp.ClientSession(connector=connector, headers=headers) as session:
        # 1. Members
        members = await fetch_members(session, sem)

        # 2. Proceedings
        proceedings = await fetch_proceedings(session, sem)
        sitting_numbers = [p.sitting for p in proceedings]

        # 3. All sittings concurrently (bounded by semaphore)
        tasks = [fetch_sitting(session, sem, s) for s in sitting_numbers]
        results = await asyncio.gather(*tasks)

    all_votings = [voting for batch, _ in results for voting in batch]
    write_all_parquet(proceedings, all_votings, members)

    elapsed = time.perf_counter() - t0
    logger.info(
        "Done in %.1fs — %d members, %d votings, %d total vote rows",
        elapsed,
        len(members),
        len(all_votings),
        sum(len(voting.votes) for voting in all_votings),
    )


if __name__ == "__main__":
    asyncio.run(fetch_data())
