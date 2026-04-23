# PolishParliamentAnalysis
This repo contains an analysis of votings from Term X  (2023-2027) of the Sejm of the Republic of Poland

The data was obtained by scraping the official website of the Sejm, and then processed and analyzed using Python. The analysis includes various aspects of the votings, such as voting patterns, party cohesion, and individual MP behavior.


# How to run the code

For fetching the data, run the following commands in the terminal:
```bash
uv sync
uv run src/scrapper/main.py
```

## Processing votings for network weighting

Run the processing pipeline to classify vote titles with Ollama, score confidence and compute per-vote weight:
```bash
uv run src/processing/process_votings.py
```

Useful options:
```bash
uv run src/processing/process_votings.py \
  --model SpeakLeash/bielik-11b-v2.2-instruct:Q4_K_M \
  --workers 4 \
  --batch-size 128 \
  --progress-every 50 \
  --skip-compatibility-above 0.9
```

To install and start Ollama:
```bash
brew install ollama
ollama serve
ollama pull SpeakLeash/bielik-11b-v2.2-instruct:Q4_K_M
```

Notes:
- Votings with compatibility above `0.9` are skipped by default (`max(yes, no) / totalVoted`).
- Rows with confidence below `--review-threshold` are saved for manual review.

Main outputs:
- `data/votings_processed.parquet` - original voting rows plus `label`, `sub_label`, `is_procedural`, `confidence`, `salience`, `compatibility`, `contestedness`, `vote_weight`, `llm_used`
- `data/votings_review.parquet` - low-confidence rows for manual review
