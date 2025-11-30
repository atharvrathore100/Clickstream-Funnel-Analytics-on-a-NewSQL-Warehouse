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
    parser.add_argument(
        "--verbose",
        dest="verbose",
        action="store_true",
        help="Show debug output.",
    )
    args = parser.parse_args()

    producer = build_producer(args.bootstrap)
    sent = 0
    total_lines = 0
    parse_errors = 0
    sample_errors = []
    
    print(f"Loading source: {args.source}")
    for raw_line in load_source(args.source):
        total_lines += 1
        try:
            # Try JSON first (for JSON-formatted sources)
            payload = json.loads(raw_line)
        except json.JSONDecodeError:
            # If not JSON, try space-separated format (Wikimedia pageviews format)
            # Format: domain_code page_title count_views total_response_size
            # Note: Page titles may contain spaces, so we need to split carefully
            # The format is: first field (domain), last two fields (views, bytes), 
            # and everything in between is the page title
            parts = raw_line.strip().split()
            if len(parts) >= 4:
                try:
                    # Parse space-separated format
                    domain_code = parts[0].strip('"') if parts[0].startswith('"') else parts[0]
                    # Last two fields are views and bytes
                    views = int(parts[-2])
                    bytes_val = int(parts[-1])
                    # Everything in between is the page title (handles spaces in titles)
                    if len(parts) > 4:
                        page_title = ' '.join(parts[1:-2])
                    else:
                        page_title = parts[1] if len(parts) > 1 else ""
                    
                    # Extract project from domain_code (e.g., "en.wikipedia" -> "en.wikipedia")
                    # Handle empty domain
                    project = domain_code if domain_code and domain_code != '""' else None
                    
                    payload = {
                        "project": project,
                        "page": page_title,
                        "views": views,
                        "bytes": bytes_val,
                    }
                except (ValueError, IndexError) as e:
                    parse_errors += 1
                    if args.verbose and len(sample_errors) < 3:
                        sample_errors.append((raw_line[:100], f"Parse error: {e}"))
                    continue
            else:
                parse_errors += 1
                if args.verbose and len(sample_errors) < 3:
                    sample_errors.append((raw_line[:100], f"Not enough fields (got {len(parts)}, need 4+)"))
                continue
        
        key = payload.get("page") or payload.get("title") or ""
        producer.send(args.topic, value=payload, key=key)
        sent += 1
        if args.limit and sent >= args.limit:
            break

    producer.flush()
    print(f"Sent {sent} records to topic '{args.topic}' on {args.bootstrap}")
    if args.verbose or sent == 0:
        print(f"Total lines read: {total_lines:,}")
        print(f"Parse errors: {parse_errors:,}")
        if sample_errors:
            print("\nSample parse errors:")
            for line_sample, error in sample_errors:
                print(f"  Line: {line_sample}...")
                print(f"  Error: {error}")


if __name__ == "__main__":
    main()
