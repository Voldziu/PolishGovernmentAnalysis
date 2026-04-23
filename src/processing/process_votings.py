import argparse
from pathlib import Path

from processing.votings import process_votings
from utils.config import OUT_DIR

SRC_ROOT = Path(__file__).resolve().parents[2]
PARQUET_OUT_DIR = SRC_ROOT / OUT_DIR


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Classify and weight Sejm votings for downstream MP network analysis.",
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=PARQUET_OUT_DIR / "votings.parquet",
        help="Input votings parquet path.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=PARQUET_OUT_DIR / "votings_processed.parquet",
        help="Output parquet path with labels and vote_weight.",
    )
    parser.add_argument(
        "--review-output",
        type=Path,
        default=PARQUET_OUT_DIR / "votings_review.parquet",
        help="Output parquet path with low-confidence rows.",
    )
    parser.add_argument(
        "--model",
        default="SpeakLeash/bielik-11b-v2.2-instruct:Q4_K_M",
        help="Ollama model name.",
    )
    parser.add_argument(
        "--ollama-url",
        default="http://localhost:11434",
        help="Ollama base URL.",
    )
    parser.add_argument(
        "--temperature",
        type=float,
        default=0.0,
        help="LLM temperature.",
    )
    parser.add_argument(
        "--review-threshold",
        type=float,
        default=0.75,
        help="Rows below confidence threshold are exported for manual review.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=4,
        help="Number of concurrent classification workers.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=128,
        help="Number of rows submitted to workers at once.",
    )
    parser.add_argument(
        "--progress-every",
        type=int,
        default=50,
        help="Log progress and ETA every N processed rows.",
    )
    parser.add_argument(
        "--skip-compatibility-above",
        type=float,
        default=0.9,
        help="Skip votings where max(yes, no) / totalVoted is above this threshold.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Process only first N rows (for smoke tests).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    process_votings(
        input_path=args.input,
        output_path=args.output,
        review_output_path=args.review_output,
        model=args.model,
        ollama_url=args.ollama_url,
        temperature=args.temperature,
        review_threshold=args.review_threshold,
        limit=args.limit,
        workers=args.workers,
        batch_size=args.batch_size,
        progress_every=args.progress_every,
        skip_compatibility_above=args.skip_compatibility_above,
    )


if __name__ == "__main__":
    main()

