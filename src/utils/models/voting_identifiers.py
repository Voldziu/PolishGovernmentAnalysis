from pydantic import BaseModel, ConfigDict, Field


class VotingIdentifiers(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    sitting: int = 0
    voting_number: int = Field(
        alias="votingNumber",
        serialization_alias="votingNumber",
    )
