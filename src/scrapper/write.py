from pathlib import Path
from typing import Literal, Union, get_args, get_origin

import pyarrow as pa
import pyarrow.parquet as pq
from pydantic import BaseModel

from utils.config import OUT_DIR, PARQUET_COMPRESSION
from utils.logger import get_logger
from utils.models import Member, Proceeding, VoteParquetRow, Voting, VotingParquetRow

logger = get_logger(__name__)

SRC_ROOT = Path(__file__).resolve().parents[2]
PARQUET_OUT_DIR = SRC_ROOT / OUT_DIR


def _ensure_data_dir(data_dir: Path | None = None) -> Path:
    """Create the data directory when missing and return its path."""
    target_dir = data_dir or PARQUET_OUT_DIR
    target_dir.mkdir(parents=True, exist_ok=True)
    return target_dir


def _annotation_to_pa(annotation: object) -> pa.DataType:
    origin = get_origin(annotation)

    if origin in (Literal,):
        return pa.string()

    if origin in (Union,):
        non_none_args = [arg for arg in get_args(annotation) if arg is not type(None)]
        if len(non_none_args) == 1:
            return _annotation_to_pa(non_none_args[0])
        return pa.string()

    if annotation is int:
        return pa.int64()
    if annotation is float:
        return pa.float64()
    if annotation is bool:
        return pa.bool_()
    if annotation is str:
        return pa.string()

    return pa.string()


def _schema_from_model(
    model_cls: type[BaseModel],
    exclude: set[str] | None = None,
) -> pa.Schema:
    excluded = exclude or set()
    fields: list[tuple[str, pa.DataType]] = []
    for field_name, field in model_cls.model_fields.items():
        if field_name in excluded:
            continue
        serialized_name = field.serialization_alias or field_name
        fields.append((serialized_name, _annotation_to_pa(field.annotation)))
    return pa.schema(fields)


def format_votes_to_parquet_rows(votings: list[Voting]) -> list[VoteParquetRow]:
    """Flatten votes into a compact row format without redundant voting metadata."""
    rows: list[VoteParquetRow] = []
    for voting in votings:
        for member_vote in voting.votes:
            vote_row = VoteParquetRow.from_vote(
                vote=member_vote,
                voting_number=voting.voting_number,
                sitting=voting.sitting,
            )
            rows.append(vote_row)
    return rows


def format_votings_to_parquet_rows(votings: list[Voting]) -> list[VotingParquetRow]:
    """Flatten modular voting data into one metadata+results row per voting."""
    return [VotingParquetRow.from_voting(voting) for voting in votings]


def write_proceedings_parquet(
    proceedings: list[Proceeding],
    data_dir: Path | None = None,
    compression: str = PARQUET_COMPRESSION,
    filename: str = "proceedings.parquet",
) -> Path:
    """Serialize proceedings to data/proceedings.parquet."""
    target_dir = _ensure_data_dir(data_dir)
    output_path = target_dir / filename
    records = [proceeding.model_dump(by_alias=True) for proceeding in proceedings]
    table = pa.Table.from_pylist(records, schema=_schema_from_model(Proceeding))
    pq.write_table(table, output_path, compression=compression)
    logger.info("Saved %d proceedings to %s", len(records), output_path)
    return output_path


def write_votings_parquet(
    votings: list[Voting],
    data_dir: Path | None = None,
    compression: str = PARQUET_COMPRESSION,
    filename: str = "votings.parquet",
) -> Path:
    """Serialize voting metadata to data/votings.parquet."""
    target_dir = _ensure_data_dir(data_dir)
    output_path = target_dir / filename
    records = [
        row.model_dump(by_alias=True) for row in format_votings_to_parquet_rows(votings)
    ]
    table = pa.Table.from_pylist(records, schema=_schema_from_model(VotingParquetRow))
    pq.write_table(table, output_path, compression=compression)
    logger.info("Saved %d votings to %s", len(records), output_path)
    return output_path


def write_votes_parquet(
    votings: list[Voting],
    data_dir: Path | None = None,
    compression: str = PARQUET_COMPRESSION,
    filename: str = "votes.parquet",
) -> Path:
    """Serialize flattened member votes to data/votes.parquet."""
    target_dir = _ensure_data_dir(data_dir)
    output_path = target_dir / filename
    records = [
        row.model_dump(by_alias=True) for row in format_votes_to_parquet_rows(votings)
    ]
    table = pa.Table.from_pylist(records, schema=_schema_from_model(VoteParquetRow))
    pq.write_table(table, output_path, compression=compression)
    logger.info("Saved %d votes to %s", len(records), output_path)
    return output_path


def write_members_parquet(
    members: list[Member],
    data_dir: Path | None = None,
    compression: str = PARQUET_COMPRESSION,
    filename: str = "members.parquet",
) -> Path:
    """Serialize members to data/members.parquet."""
    target_dir = _ensure_data_dir(data_dir)
    output_path = target_dir / filename
    records = [member.model_dump(by_alias=True) for member in members]
    table = pa.Table.from_pylist(records, schema=_schema_from_model(Member))
    pq.write_table(table, output_path, compression=compression)
    logger.info("Saved %d members to %s", len(records), output_path)
    return output_path


def write_all_parquet(
    proceedings: list[Proceeding],
    votings: list[Voting],
    members: list[Member],
    data_dir: Path | None = None,
    compression: str = PARQUET_COMPRESSION,
) -> dict[str, Path]:
    """Write proceedings, votings, votes, and members to parquet files."""
    return {
        "proceedings": write_proceedings_parquet(proceedings, data_dir, compression),
        "votings": write_votings_parquet(votings, data_dir, compression),
        "votes": write_votes_parquet(votings, data_dir, compression),
        "members": write_members_parquet(members, data_dir, compression),
    }
