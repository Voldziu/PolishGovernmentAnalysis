import json
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from threading import Lock
from typing import Any

import pandas as pd

from utils.logger import get_logger

logger = get_logger(__name__)

ALLOWED_LABELS = {
    "procedural",
    "personnel",
    "legislative",
    "oversight",
    "budget",
    "eu",
    "international",
    "other",
}


@dataclass(slots=True)
class Classification:
    label: str
    sub_label: str
    is_procedural: bool
    confidence: float
    salience: int
    llm_used: bool = False


def normalize_text(text: str) -> str:
    if not isinstance(text, str):
        return ""
    return " ".join(text.lower().split())


def build_prompt(title: str, topic: str) -> str:
    return f"""Sklasyfikuj glosowanie parlamentarne na podstawie opisu.
Zwroc TYLKO poprawny JSON (bez markdownu i bez dodatkowego tekstu) z kluczami:
label, sub_label, is_procedural, confidence, salience.

Dozwolone label:
- procedural
- personnel
- legislative
- oversight
- budget
- eu
- international
- other

Dozwolone salience: liczba calkowita od 1 do 5.
confidence: liczba od 0.0 do 1.0.

Kontekst glosowania:
agenda_title: {title}
voting_topic: {topic}
"""


def _safe_float(value: Any, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _safe_bool(value: Any, default: bool) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"true", "1", "yes", "y"}:
            return True
        if lowered in {"false", "0", "no", "n"}:
            return False
    if isinstance(value, (int, float)):
        return bool(value)
    return default


def _format_duration(seconds: float) -> str:
    seconds_int = max(0, int(seconds))
    hours, remainder = divmod(seconds_int, 3600)
    minutes, secs = divmod(remainder, 60)
    if hours:
        return f"{hours:02d}:{minutes:02d}:{secs:02d}"
    return f"{minutes:02d}:{secs:02d}"


def call_ollama(
    prompt: str,
    model: str,
    ollama_url: str,
    temperature: float,
    timeout_seconds: int = 120,
    max_retries: int = 2,
) -> dict[str, Any] | None:
    payload = {
        "model": model,
        "prompt": prompt,
        "stream": False,
        "format": "json",
        "keep_alive": "30m",
        "options": {"temperature": temperature},
    }

    req = urllib.request.Request(
        url=f"{ollama_url.rstrip('/')}/api/generate",
        method="POST",
        headers={"Content-Type": "application/json"},
        data=json.dumps(payload).encode("utf-8"),
    )

    for attempt in range(max_retries + 1):
        try:
            with urllib.request.urlopen(req, timeout=timeout_seconds) as response:
                body = response.read().decode("utf-8")
        except (urllib.error.URLError, TimeoutError) as exc:
            if attempt == max_retries:
                logger.warning("Ollama request failed after retries: %s", exc)
                return None
            continue

        try:
            parsed = json.loads(body)
        except json.JSONDecodeError:
            logger.warning("Ollama returned non-JSON response envelope.")
            return None

        raw_response = parsed.get("response", "")
        if not isinstance(raw_response, str):
            return None

        raw_response = raw_response.strip().strip("`")
        if raw_response.startswith("json"):
            raw_response = raw_response[4:].strip()

        try:
            result = json.loads(raw_response)
        except json.JSONDecodeError:
            if attempt == max_retries:
                logger.warning("Model output is not valid JSON: %s", raw_response[:180])
                return None
            continue

        return result if isinstance(result, dict) else None

    return None


def classify_with_llm(
    title: str,
    topic: str,
    model: str,
    ollama_url: str,
    temperature: float,
) -> Classification:
    fallback = Classification("other", "unknown", False, 0.2, 1, False)
    result = call_ollama(
        prompt=build_prompt(title=title, topic=topic),
        model=model,
        ollama_url=ollama_url,
        temperature=temperature,
    )

    if result is None:
        return fallback

    label = str(result.get("label", fallback.label)).strip().lower() or fallback.label
    if label not in ALLOWED_LABELS:
        label = fallback.label

    sub_label = str(result.get("sub_label", fallback.sub_label)).strip().lower() or fallback.sub_label
    is_procedural = _safe_bool(result.get("is_procedural"), fallback.is_procedural)
    confidence = max(0.0, min(1.0, _safe_float(result.get("confidence"), fallback.confidence)))
    salience = max(1, min(5, _safe_int(result.get("salience"), fallback.salience)))

    return Classification(label, sub_label, is_procedural, confidence, salience, True)


def compute_contestedness(yes: Any, no: Any, total_voted: Any) -> float:
    yes_f = _safe_float(yes, 0.0)
    no_f = _safe_float(no, 0.0)
    total_f = _safe_float(total_voted, 0.0)
    if total_f <= 0:
        return 0.0
    return max(0.0, min(1.0, 1.0 - abs(yes_f - no_f) / total_f))


def compute_compatibility(yes: Any, no: Any, total_voted: Any) -> float:
    yes_f = _safe_float(yes, 0.0)
    no_f = _safe_float(no, 0.0)
    total_f = _safe_float(total_voted, 0.0)
    if total_f <= 0:
        return 0.0
    return max(0.0, min(1.0, max(yes_f, no_f) / total_f))


def final_weight(contestedness: float, confidence: float, salience: int) -> float:
    return salience * confidence * (0.25 + 0.75 * contestedness)


def process_votings(
    input_path: Path,
    output_path: Path,
    review_output_path: Path,
    model: str,
    ollama_url: str,
    temperature: float,
    review_threshold: float,
    limit: int | None,
    workers: int,
    batch_size: int,
    progress_every: int,
    skip_compatibility_above: float,
) -> None:
    df = pd.read_parquet(input_path)
    if limit is not None:
        df = df.head(limit).copy()

    records = df.to_dict(orient="records")
    filtered_records = []
    skipped_for_compatibility = 0

    for record in records:
        compatibility = compute_compatibility(
            yes=record.get("yes", 0),
            no=record.get("no", 0),
            total_voted=record.get("totalVoted", 0),
        )
        if compatibility > skip_compatibility_above:
            skipped_for_compatibility += 1
            continue
        record["compatibility"] = compatibility
        filtered_records.append(record)

    if skipped_for_compatibility:
        logger.info(
            "Skipped %d votings with compatibility > %.2f",
            skipped_for_compatibility,
            skip_compatibility_above,
        )

    total = len(filtered_records)
    if total == 0:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        review_output_path.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame([]).to_parquet(output_path, index=False)
        pd.DataFrame([]).to_parquet(review_output_path, index=False)
        logger.info("No rows left after filtering; wrote empty outputs.")
        return

    rows_by_position: list[dict[str, Any] | None] = [None] * total
    review_rows: list[dict[str, Any]] = []

    classification_cache: dict[str, Classification] = {}
    cache_lock = Lock()

    def process_single(position: int, record: dict[str, Any]) -> dict[str, Any]:
        title = record.get("title", "") or ""
        topic = record.get("topic", "") or ""
        cache_key = f"{normalize_text(title)}||{normalize_text(topic)}"

        with cache_lock:
            cached = classification_cache.get(cache_key)

        if cached is None:
            cls = classify_with_llm(
                title=title,
                topic=topic,
                model=model,
                ollama_url=ollama_url,
                temperature=temperature,
            )
            with cache_lock:
                classification_cache[cache_key] = cls
        else:
            cls = cached

        contestedness = compute_contestedness(
            yes=record.get("yes", 0),
            no=record.get("no", 0),
            total_voted=record.get("totalVoted", 0),
        )
        vote_weight = final_weight(contestedness, cls.confidence, cls.salience)

        return {
            **record,
            "label": cls.label,
            "sub_label": cls.sub_label,
            "is_procedural": cls.is_procedural,
            "confidence": cls.confidence,
            "salience": cls.salience,
            "contestedness": contestedness,
            "vote_weight": vote_weight,
            "llm_used": cls.llm_used,
            "_position": position,
        }

    started_at = time.perf_counter()
    processed = 0

    with ThreadPoolExecutor(max_workers=max(1, workers)) as executor:
        for batch_start in range(0, total, max(1, batch_size)):
            batch = filtered_records[batch_start : batch_start + max(1, batch_size)]
            future_map = {
                executor.submit(process_single, batch_start + offset, record): batch_start + offset
                for offset, record in enumerate(batch)
            }

            for future in as_completed(future_map):
                row_out = future.result()
                pos = row_out.pop("_position")
                rows_by_position[pos] = row_out
                if row_out["confidence"] < review_threshold:
                    review_rows.append(row_out)

                processed += 1
                if processed % max(1, progress_every) == 0 or processed == total:
                    elapsed = time.perf_counter() - started_at
                    rate = processed / elapsed if elapsed > 0 else 0.0
                    remaining = total - processed
                    eta_seconds = remaining / rate if rate > 0 else 0.0
                    logger.info(
                        "Processed %d/%d (%.1f%%) | %.2f rows/s | ETA %s",
                        processed,
                        total,
                        (processed / total) * 100,
                        rate,
                        _format_duration(eta_seconds),
                    )

    rows = [row for row in rows_by_position if row is not None]

    output_path.parent.mkdir(parents=True, exist_ok=True)
    review_output_path.parent.mkdir(parents=True, exist_ok=True)

    pd.DataFrame(rows).to_parquet(output_path, index=False)
    pd.DataFrame(review_rows).to_parquet(review_output_path, index=False)

    logger.info("Saved processed votings to %s", output_path)
    logger.info("Saved review candidates (%d rows) to %s", len(review_rows), review_output_path)
