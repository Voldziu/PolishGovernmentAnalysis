from pydantic import BaseModel, ConfigDict, Field


class Member(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    mp_id: int = Field(alias="id", serialization_alias="MP")
    club: str = ""
    first_name: str = Field(alias="firstName", serialization_alias="firstName")
    last_name: str = Field(alias="lastName", serialization_alias="lastName")
