"""
Load Wikimedia pageviews into a Snowflake VARIANT staging table.

Examples:
    python snowflake_stage_loader.py --database ANALYTICS --schema RAW --table PAGEVIEWS_RAW
    python snowflake_stage_loader.py --source data/pageviews-20240101-000000.gz --table PAGEVIEWS_STAGE
"""

from __future__ import annotations

import argparse
import getpass
import gzip
import json
import os
import tempfile
from pathlib import Path
from typing import Iterable, Iterator, List, Sequence

import requests
import snowflake.connector

DEFAULT_SOURCE = "https://dumps.wikimedia.org/other/pageviews/2024/2024-01/pageviews-20240101-000000.gz"
INSERT_BATCH_SIZE = 5000


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
    if source.startswith("http"):
        path = download_to_temp(source)
    else:
        path = Path(source)
        if not path.exists():
            raise FileNotFoundError(f"Source not found: {source}")
    return iter_lines(path)


def parse_record(raw_line: str) -> dict | None:
    try:
        return json.loads(raw_line)
    except json.JSONDecodeError:
        parts = raw_line.split()
        if len(parts) < 4:
            return None
        try:
            domain_code = parts[0].strip('"') if parts[0].startswith('"') else parts[0]
            views = int(parts[-2])
            bytes_val = int(parts[-1])
            page_title = " ".join(parts[1:-2]) if len(parts) > 4 else parts[1]
        except (IndexError, ValueError):
            return None
        project = domain_code if domain_code and domain_code != '""' else None
        return {
            "project": project,
            "page": page_title,
            "views": views,
            "bytes": bytes_val,
        }


def quote_ident(name: str) -> str:
    escaped = name.replace('"', '""')
    return f'"{escaped}"'


def qualified_table(database: str, schema: str, table: str) -> str:
    return ".".join(quote_ident(part) for part in (database, schema, table))


def ensure_table(conn: snowflake.connector.SnowflakeConnection, fq_table: str) -> None:
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {fq_table} (
        raw VARIANT,
        source_file STRING,
        line_number NUMBER,
        ingested_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
    )
    """
    with conn.cursor() as cur:
        cur.execute(create_sql)


def connect(args: argparse.Namespace) -> snowflake.connector.SnowflakeConnection:
    password = args.password or os.getenv("SNOWFLAKE_PASSWORD")
    if not password:
        password = getpass.getpass("Snowflake password: ")
    kwargs = {
        "account": args.account,
        "user": args.user,
        "password": password,
        "warehouse": args.warehouse,
        "database": args.database,
        "schema": args.schema,
    }
    if args.role:
        kwargs["role"] = args.role
    if args.authenticator:
        kwargs["authenticator"] = args.authenticator
    return snowflake.connector.connect(**kwargs)


def insert_batch(
    conn: snowflake.connector.SnowflakeConnection,
    fq_table: str,
    source_name: str,
    batch: Sequence[dict],
) -> int:
    rows = [
        (
            json.dumps(entry["record"]),
            source_name,
            entry["line_number"],
        )
        for entry in batch
    ]
    sql = f"""
        INSERT INTO {fq_table} (raw, source_file, line_number)
        SELECT PARSE_JSON(column1), column2, column3
        FROM VALUES (%s, %s, %s)
    """
    with conn.cursor() as cur:
        cur.executemany(sql, rows)
    return len(rows)


def ingest(args: argparse.Namespace) -> None:
    lines = load_source(args.source)
    fq_table = qualified_table(args.database, args.schema, args.table)
    with connect(args) as conn:
        ensure_table(conn, fq_table)
        total_inserted = 0
        buffer: List[dict] = []
        line_number = 0
        for raw_line in lines:
            line_number += 1
            rec = parse_record(raw_line)
            if rec is None:
                continue
            buffer.append({"line_number": line_number, "record": rec})
            if len(buffer) >= INSERT_BATCH_SIZE:
                inserted = insert_batch(conn, fq_table, args.source, buffer)
                total_inserted += inserted
                buffer.clear()
                print(f"Inserted {total_inserted:,} rows...", end="\r")
        if buffer:
            inserted = insert_batch(conn, fq_table, args.source, buffer)
            total_inserted += inserted
        conn.commit()
    print(f"\nLoaded {total_inserted:,} rows into {fq_table}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Stage Wikimedia pageviews JSON into Snowflake VARIANT tables.",
    )
    parser.add_argument(
        "--source",
        default=os.getenv("PAGEVIEWS_SOURCE", DEFAULT_SOURCE),
        help="URL or local gz file (default Wikimedia snapshot).",
    )
    parser.add_argument("--account", default=os.getenv("SNOWFLAKE_ACCOUNT"))
    parser.add_argument("--user", default=os.getenv("SNOWFLAKE_USER"))
    parser.add_argument("--password", default=os.getenv("SNOWFLAKE_PASSWORD"))
    parser.add_argument("--warehouse", default=os.getenv("SNOWFLAKE_WAREHOUSE", "LOAD_WH"))
    parser.add_argument("--database", default=os.getenv("SNOWFLAKE_DATABASE", "ANALYTICS"))
    parser.add_argument("--schema", default=os.getenv("SNOWFLAKE_SCHEMA", "RAW"))
    parser.add_argument("--table", default=os.getenv("SNOWFLAKE_TABLE", "PAGEVIEWS_RAW"))
    parser.add_argument("--role", default=os.getenv("SNOWFLAKE_ROLE"))
    parser.add_argument("--authenticator", default=os.getenv("SNOWFLAKE_AUTHENTICATOR"))
    args = parser.parse_args()
    required_flags = {
        "account": "--account / SNOWFLAKE_ACCOUNT",
        "user": "--user / SNOWFLAKE_USER",
        "database": "--database / SNOWFLAKE_DATABASE",
        "schema": "--schema / SNOWFLAKE_SCHEMA",
        "table": "--table / SNOWFLAKE_TABLE",
    }
    missing = [hint for field, hint in required_flags.items() if not getattr(args, field)]
    if missing:
        parser.error(f"Missing required Snowflake options: {', '.join(missing)}")
    return args


def main() -> None:
    args = parse_args()
    ingest(args)


if __name__ == "__main__":
    main()
