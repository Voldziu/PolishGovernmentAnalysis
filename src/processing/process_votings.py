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
        "--use-llm",
        action="store_true",
        help="Use local Ollama model for ambiguous rows.",
    )
    parser.add_argument(
        "--model",
        default="qwen2.5:3b-instruct",
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
        use_llm=args.use_llm,
        model=args.model,
        ollama_url=args.ollama_url,
        temperature=args.temperature,
        review_threshold=args.review_threshold,
        limit=args.limit,
    )


if __name__ == "__main__":
    main()

