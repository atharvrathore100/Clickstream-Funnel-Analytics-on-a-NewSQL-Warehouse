"""
Lightweight profiling for Wikimedia pageviews JSON logs.

Usage:
    python pageviews_profile.py --source https://dumps.wikimedia.org/other/pageviews/2024/2024-01/pageviews-20240101-000000.gz --limit 20000
    python pageviews_profile.py --source data/pageviews-20240101-000000.gz
"""

import argparse
import gzip
import json
import tempfile
from collections import Counter
from pathlib import Path
from typing import Iterable, Iterator

import requests

DEFAULT_SOURCE = "https://dumps.wikimedia.org/other/pageviews/2024/2024-01/pageviews-20240101-000000.gz"


def download_to_temp(url: str) -> Path:
    resp = requests.get(url, stream=True, timeout=90)
    resp.raise_for_status()
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".gz")
    with tmp as fh:
        for chunk in resp.iter_content(chunk_size=8192):
            if chunk:
                fh.write(chunk)
    return Path(tmp.name)


def iter_lines(path: Path) -> Iterator[str]:
    with gzip.open(path, "rt", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                yield line


def load_source(source: str) -> Iterable[str]:
    path: Path
    if source.startswith("http"):
        path = download_to_temp(source)
    else:
        path = Path(source)
        if not path.exists():
            raise FileNotFoundError(f"Source not found: {source}")
    return iter_lines(path)


def profile(source: str, limit: int | None) -> None:
    field_presence = Counter()
    page_counts = Counter()
    project_counts = Counter()
    bytes_sum = 0
    total = 0

    for raw_line in load_source(source):
        try:
            rec = json.loads(raw_line)
        except json.JSONDecodeError:
            continue

        total += 1
        for field in ("project", "page", "views", "bytes"):
            if field in rec:
                field_presence[field] += 1
        page_counts[rec.get("page")] += 1
        project_counts[rec.get("project")] += 1
        if "bytes" in rec and isinstance(rec["bytes"], int):
            bytes_sum += rec["bytes"]

        if limit and total >= limit:
            break

    print(f"Profiled {total:,} records from {source}")
    print("\nField completeness:")
    for field in ("project", "page", "views", "bytes"):
        pct = (field_presence[field] / total * 100) if total else 0
        print(f"  {field:7s}: {pct:6.2f}% present")

    print("\nTop projects:")
    for proj, count in project_counts.most_common(5):
        print(f"  {proj or 'None':10s} {count:,}")

    print("\nTop pages:")
    for page, count in page_counts.most_common(10):
        if page is None:
            continue
        print(f"  {page[:40]:40s} {count:,}")

    if total:
        avg_bytes = bytes_sum / total
        print(f"\nAverage bytes field: {avg_bytes:,.2f}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Profile Wikimedia pageviews JSON logs.")
    parser.add_argument("--source", default=DEFAULT_SOURCE, help="URL or local gz file.")
    parser.add_argument("--limit", type=int, default=50000, help="Max rows to scan (None for all).")
    args = parser.parse_args()

    profile(args.source, args.limit)


if __name__ == "__main__":
    main()
