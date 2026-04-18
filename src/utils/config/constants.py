from pathlib import Path

BASE_URL = "https://api.sejm.gov.pl/sejm"
TERM = 10
CONCURRENCY = 5  # max simultaneous requests
RETRY_ATTEMPTS = 3
RETRY_BACKOFF = 2.0  # seconds, doubles on each retry
REQUEST_TIMEOUT = 30  # seconds per request
OUT_DIR = Path("data")
PARQUET_COMPRESSION = "zstd"
