"""
Kafka producer for Wikimedia pageviews JSON logs.

Usage (examples):
    python kafka_producer.py --bootstrap localhost:9092 --topic wm_pageviews
    python kafka_producer.py --source https://dumps.wikimedia.org/other/pageviews/2024/2024-01/pageviews-20240101-000000.gz
    python kafka_producer.py --source data/pageviews-20240101-000000.gz
"""

import argparse
import gzip
import json
import os
import tempfile
from pathlib import Path
from typing import Iterable, Iterator

import requests
from kafka import KafkaProducer

DEFAULT_SOURCE = "https://dumps.wikimedia.org/other/pageviews/2024/2024-01/pageviews-20240101-000000.gz"


def download_to_temp(url: str) -> Path:
    """Download a file to a temporary location and return the path."""
    resp = requests.get(url, stream=True, timeout=90)
    resp.raise_for_status()
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".gz")
    with tmp as fh:
        for chunk in resp.iter_content(chunk_size=8192):
            if chunk:
                fh.write(chunk)
    return Path(tmp.name)


def iter_lines(path: Path) -> Iterator[str]:
    """Yield decompressed lines from a local gz file."""
    with gzip.open(path, "rt", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                yield line


def load_source(source: str) -> Iterable[str]:
    """Yield lines from a URL (downloaded) or a local file path."""
    path: Path
    if source.startswith("http"):
        path = download_to_temp(source)
    else:
        path = Path(source)
        if not path.exists():
            raise FileNotFoundError(f"Source not found: {source}")
    return iter_lines(path)


def build_producer(bootstrap_servers: str) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda v: v.encode("utf-8") if v else None,
        linger_ms=50,
        acks="all",
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Kafka producer for Wikimedia pageviews JSON.")
    parser.add_argument("--bootstrap", dest="bootstrap", default=os.getenv("KAFKA_BOOTSTRAP", "localhost:9092"))
    parser.add_argument("--topic", dest="topic", default=os.getenv("KAFKA_TOPIC", "wm_pageviews"))
    parser.add_argument(
        "--source",
        dest="source",
        default=os.getenv("PAGEVIEWS_SOURCE", DEFAULT_SOURCE),
        help="URL or local path to a pageviews-YYYYMMDD-HH0000.gz file.",
    )
    parser.add_argument(
        "--limit",
        dest="limit",
        type=int,
        default=None,
        help="Optional max number of records to send (for quick tests).",
    )
    args = parser.parse_args()

    producer = build_producer(args.bootstrap)
    sent = 0
    for raw_line in load_source(args.source):
        try:
            payload = json.loads(raw_line)
        except json.JSONDecodeError:
            continue
        key = payload.get("page") or payload.get("title") or ""
        producer.send(args.topic, value=payload, key=key)
        sent += 1
        if args.limit and sent >= args.limit:
            break

    producer.flush()
    print(f"Sent {sent} records to topic '{args.topic}' on {args.bootstrap}")


if __name__ == "__main__":
    main()
