from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from .voting_identifiers import VotingIdentifiers


class Vote(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    mp_id: int = Field(alias="MP", serialization_alias="MP")
    club: str = ""
    vote: Literal["YES", "NO", "ABSTAIN"]


class VoteParquetRow(Vote, VotingIdentifiers):
    model_config = ConfigDict(populate_by_name=True)

    @classmethod
    def from_vote(
        cls,
        vote: Vote,
        sitting: int,
        voting_number: int,
    ) -> "VoteParquetRow":
        """Build parquet row from Vote while adding voting identifiers."""
        return cls(
            sitting=sitting,
            voting_number=voting_number,
            **vote.model_dump(),
        )
