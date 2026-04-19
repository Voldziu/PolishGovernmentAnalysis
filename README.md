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

Run the processing pipeline to classify vote titles, score confidence, and compute a per-vote weight:
```bash
uv run src/processing/process_votings.py
```

Optional: enable local Ollama for ambiguous titles:
```bash
uv run src/processing/process_votings.py --use-llm --model qwen2.5:3b-instruct
```
To install the model, run:
```bash
brew install ollama
ollama serve
ollama pull qwen2.5:3b-instruct
ollama run qwen2.5:3b-instruct

```

Main outputs:
- `data/votings_processed.parquet` - original voting rows plus `label`, `confidence`, `salience`, `contestedness`, `vote_weight`
- `data/votings_review.parquet` - low-confidence rows for manual review
