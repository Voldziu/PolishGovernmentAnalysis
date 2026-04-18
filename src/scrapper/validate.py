from pydantic import ValidationError

from utils.models import Member, Proceeding, Vote, Voting

from .helpers import JsonPayload


def validate_proceedings(raw_proceedings: JsonPayload) -> list[Proceeding]:
    """Validate raw proceedings payload into Proceeding models."""
    if not isinstance(raw_proceedings, list):
        return []

    validated_proceedings: list[Proceeding] = []
    for raw_proceeding in raw_proceedings:
        if not isinstance(raw_proceeding, dict):
            continue
        try:
            validated_proceedings.append(Proceeding.model_validate(raw_proceeding))
        except ValidationError:
            continue

    return validated_proceedings


def validate_votings(raw_votings: JsonPayload) -> list[Voting]:
    """Validate raw voting list payload into Voting models."""
    if not isinstance(raw_votings, list):
        return []

    validated_votings: list[Voting] = []
    for raw_voting in raw_votings:
        if not isinstance(raw_voting, dict):
            continue
        try:
            validated_votings.append(Voting.model_validate(raw_voting))
        except ValidationError:
            continue

    return validated_votings


def validate_members(raw_members: JsonPayload) -> list[Member]:
    """Validate raw member payload into Member models."""
    if not isinstance(raw_members, list):
        return []

    validated_members: list[Member] = []
    for raw_member in raw_members:
        if not isinstance(raw_member, dict):
            continue
        try:
            validated_members.append(Member.model_validate(raw_member))
        except ValidationError:
            continue

    return validated_members


def validate_voting_detail(raw_voting_detail: JsonPayload) -> Voting | None:
    """Validate raw voting detail payload into a Voting model with validated votes."""
    if not isinstance(raw_voting_detail, dict):
        return None

    raw_topic = raw_voting_detail.get("topic")
    raw_title = raw_voting_detail.get("title")
    topic = raw_topic if isinstance(raw_topic, str) and raw_topic else raw_title

    payload = dict(raw_voting_detail)
    payload["topic"] = topic if isinstance(topic, str) else ""
    payload["votes"] = validate_member_votes(raw_voting_detail.get("votes"))

    try:
        return Voting.model_validate(payload)
    except ValidationError:
        return None


def validate_member_votes(raw_votes: JsonPayload) -> list[Vote]:
    """Validate raw member vote payload into Vote models."""
    if not isinstance(raw_votes, list):
        return []

    validated_votes: list[Vote] = []
    for raw_vote in raw_votes:
        if not isinstance(raw_vote, dict):
            continue
        try:
            validated_votes.append(Vote.model_validate(raw_vote))
        except ValidationError:
            continue

    return validated_votes
