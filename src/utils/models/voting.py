from pydantic import BaseModel, ConfigDict, Field

from .vote import Vote
from .voting_identifiers import VotingIdentifiers


class VotingMeta(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    date: str = ""
    kind: str = ""
    title: str = ""
    topic: str = ""


class VotingResults(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    yes: int = 0
    no: int = 0
    abstain: int = 0
    total_voted: int = Field(
        default=0,
        alias="totalVoted",
        serialization_alias="totalVoted",
    )


class Voting(VotingResults, VotingMeta, VotingIdentifiers):
    model_config = ConfigDict(populate_by_name=True)

    votes: list[Vote] = Field(default_factory=list)


class VotingParquetRow(VotingResults, VotingMeta, VotingIdentifiers):
    model_config = ConfigDict(populate_by_name=True)

    @classmethod
    def from_voting(cls, voting: Voting) -> "VotingParquetRow":
        """Build parquet row from Voting with shared meta/results models."""
        return cls(
            **voting.model_dump(),
        )
