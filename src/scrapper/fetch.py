import asyncio

import aiohttp

from utils.config import BASE_URL, TERM
from utils.logger import get_logger
from utils.models import Member, Proceeding, Voting

from .helpers import fetch_json
from .validate import (
    validate_members,
    validate_proceedings,
    validate_voting_detail,
    validate_votings,
)

logger = get_logger(__name__)


async def fetch_members(
    session: aiohttp.ClientSession,
    sem: asyncio.Semaphore,
) -> list[Member]:
    """Fetch all members and validate their ids and names."""
    url = f"{BASE_URL}/term{TERM}/MP"
    data = await fetch_json(session, url, sem)
    if not data:
        return []

    members = validate_members(data)
    logger.info("Found %d members", len(members))
    return members


async def fetch_proceedings(
    session: aiohttp.ClientSession,
    sem: asyncio.Semaphore,
) -> list[Proceeding]:
    """Fetch all proceedings (sittings) for the term."""
    url = f"{BASE_URL}/term{TERM}/proceedings"
    data = await fetch_json(session, url, sem)
    if not data:
        return []
    proceedings = validate_proceedings(data)
    logger.info("Found %d proceedings", len(proceedings))
    return proceedings


async def fetch_voting_details(
    session: aiohttp.ClientSession,
    sem: asyncio.Semaphore,
    sitting: int,
    voting: Voting,
) -> Voting | None:
    """Fetch individual MP votes for one voting. Skips ON_LIST kind."""
    if voting.kind != "ELECTRONIC":
        return None

    voting_number = voting.voting_number
    url = f"{BASE_URL}/term{TERM}/votings/{sitting}/{voting_number}"
    detail = await fetch_json(session, url, sem)
    if not detail:
        return None

    return validate_voting_detail(detail)


async def fetch_sitting(
    session: aiohttp.ClientSession,
    sem: asyncio.Semaphore,
    sitting: int,
) -> tuple[list[Voting], int]:
    """Fetch all ELECTRONIC votings for one sitting. Returns (votings, sitting)."""
    url = f"{BASE_URL}/term{TERM}/votings/{sitting}"
    votings = await fetch_json(session, url, sem)
    if not votings:
        logger.warning("No votings found for sitting %d", sitting)
        return [], sitting

    validated_votings = validate_votings(votings)

    if not validated_votings:
        logger.warning("No valid votings found for sitting %d", sitting)
        return [], sitting

    # Fan out voting detail requests concurrently
    tasks = [fetch_voting_details(session, sem, sitting, v) for v in validated_votings]
    results = await asyncio.gather(*tasks)

    detailed_votings = [voting for voting in results if voting is not None]
    vote_rows = sum(len(voting.votes) for voting in detailed_votings)
    logger.info(
        "Sitting %3d: %d votings → %d vote rows",
        sitting,
        len(detailed_votings),
        vote_rows,
    )
    return detailed_votings, sitting
