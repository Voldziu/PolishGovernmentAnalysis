from pydantic import BaseModel, ConfigDict, Field


class Proceeding(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    sitting: int = Field(alias="number", serialization_alias="sitting")
    title: str = ""
