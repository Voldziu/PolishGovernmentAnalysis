import json
import re
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from utils.logger import get_logger

logger = get_logger(__name__)


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


def classify_with_rules(title: str, topic: str) -> Classification:
    text = normalize_text(f"{title} || {topic}")

    if re.search(r"\bkandydatur|\bwyb[oó]r|wicemarsza|marsza", text):
        return Classification("personnel", "election", False, 0.95, 2)
    if re.search(r"\bpoprawk", text):
        return Classification("legislative", "amendment", False, 0.93, 3)
    if re.search(r"ca[lł]o[śs]ci[ąa] projektu|ca[lł][śs][ćc] projektu", text):
        return Classification("legislative", "final_passage", False, 0.96, 5)
    if re.search(r"\bsprawozdani", text):
        return Classification("legislative", "report_stage", False, 0.82, 3)
    if re.search(r"\bwniosek", text):
        return Classification("procedural", "motion", True, 0.88, 2)
    if re.search(r"\bkomisj", text):
        return Classification("procedural", "committee", True, 0.75, 2)
    if re.search(r"porz[ąa]dku dziennego|\bpkt\.", text):
        return Classification("procedural", "agenda", True, 0.7, 1)

    return Classification("other", "unknown", False, 0.4, 2)


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


def call_ollama(
    prompt: str,
    model: str,
    ollama_url: str,
    temperature: float,
    timeout_seconds: int = 45,
) -> dict[str, Any] | None:
    payload = {
        "model": model,
        "prompt": prompt,
        "stream": False,
        "options": {"temperature": temperature},
    }

    req = urllib.request.Request(
        url=f"{ollama_url.rstrip('/')}/api/generate",
        method="POST",
        headers={"Content-Type": "application/json"},
        data=json.dumps(payload).encode("utf-8"),
    )

    try:
        with urllib.request.urlopen(req, timeout=timeout_seconds) as response:
            body = response.read().decode("utf-8")
    except (urllib.error.URLError, TimeoutError) as exc:
        logger.warning("Ollama request failed: %s", exc)
        return None

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
        logger.warning("Model output is not valid JSON: %s", raw_response[:180])
        return None

    return result if isinstance(result, dict) else None


def classify_with_llm(
    title: str,
    topic: str,
    fallback: Classification,
    model: str,
    ollama_url: str,
    temperature: float,
) -> Classification:
    result = call_ollama(
        prompt=build_prompt(title=title, topic=topic),
        model=model,
        ollama_url=ollama_url,
        temperature=temperature,
    )

    if result is None:
        return fallback

    label = str(result.get("label", fallback.label)).strip().lower() or fallback.label
    sub_label = str(result.get("sub_label", fallback.sub_label)).strip().lower() or fallback.sub_label
    is_procedural = bool(result.get("is_procedural", fallback.is_procedural))
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


def final_weight(contestedness: float, confidence: float, salience: int) -> float:
    return salience * confidence * (0.25 + 0.75 * contestedness)


def process_votings(
    input_path: Path,
    output_path: Path,
    review_output_path: Path,
    use_llm: bool,
    model: str,
    ollama_url: str,
    temperature: float,
    review_threshold: float,
    limit: int | None,
) -> None:
    df = pd.read_parquet(input_path)
    if limit is not None:
        df = df.head(limit).copy()

    rows: list[dict[str, Any]] = []
    review_rows: list[dict[str, Any]] = []

    total = len(df)
    for index, record in enumerate(df.to_dict(orient="records"), start=1):
        title = record.get("title", "") or ""
        topic = record.get("topic", "") or ""

        rule_cls = classify_with_rules(title=title, topic=topic)
        final_cls = rule_cls

        if use_llm and (rule_cls.label == "other" or rule_cls.confidence < 0.8):
            final_cls = classify_with_llm(
                title=title,
                topic=topic,
                fallback=rule_cls,
                model=model,
                ollama_url=ollama_url,
                temperature=temperature,
            )

        contestedness = compute_contestedness(
            yes=record.get("yes", 0),
            no=record.get("no", 0),
            total_voted=record.get("totalVoted", 0),
        )
        vote_weight = final_weight(contestedness, final_cls.confidence, final_cls.salience)

        row_out = {
            **record,
            "label": final_cls.label,
            "sub_label": final_cls.sub_label,
            "is_procedural": final_cls.is_procedural,
            "confidence": final_cls.confidence,
            "salience": final_cls.salience,
            "contestedness": contestedness,
            "vote_weight": vote_weight,
            "llm_used": final_cls.llm_used,
        }
        rows.append(row_out)

        if final_cls.confidence < review_threshold:
            review_rows.append(row_out)

        if index % 50 == 0 or index == total:
            logger.info("Processed %d/%d votings", index, total)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    review_output_path.parent.mkdir(parents=True, exist_ok=True)

    pd.DataFrame(rows).to_parquet(output_path, index=False)
    pd.DataFrame(review_rows).to_parquet(review_output_path, index=False)

    logger.info("Saved processed votings to %s", output_path)
    logger.info("Saved review candidates (%d rows) to %s", len(review_rows), review_output_path)

