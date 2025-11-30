"""
Bridge Kafka clickstream events into Snowflake VARIANT staging tables.

The producer from Milestone 1 can emit Wikimedia events into Kafka. This script
consumes them in batches and streams them into Snowflake so Milestone 2 stays in sync.

Example:
    python kafka_to_snowflake.py \\
        --bootstrap localhost:9092 \\
        --topic wm_pageviews \\
        --account xy12345.us-east-1 \\
        --user demo_user \\
        --password ***** \\
        --warehouse LOAD_WH \\
        --database ANALYTICS \\
        --schema RAW \\
        --table PAGEVIEWS_RAW
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Optional

from kafka import KafkaConsumer
from kafka.errors import KafkaError

from snowflake_stage_loader import (
    connect,
    ensure_table,
    insert_batch,
    qualified_table,
)


def build_consumer(
    bootstrap_servers: str,
    topic: str,
    group_id: Optional[str],
    auto_offset_reset: str,
    enable_auto_commit: bool,
    consumer_timeout_ms: int,
) -> KafkaConsumer:
    config = {
        "bootstrap_servers": bootstrap_servers,
        "value_deserializer": lambda m: json.loads(m.decode("utf-8")),
        "key_deserializer": lambda m: m.decode("utf-8") if m else None,
        "auto_offset_reset": auto_offset_reset,
        "enable_auto_commit": enable_auto_commit,
        "consumer_timeout_ms": consumer_timeout_ms,
    }
    if group_id:
        config["group_id"] = group_id
    return KafkaConsumer(topic, **config)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Consume Kafka pageviews and load them into Snowflake.",
    )
    # Kafka options
    parser.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP", "localhost:9092"))
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "wm_pageviews"))
    parser.add_argument("--group-id", default=os.getenv("KAFKA_GROUP_ID"))
    parser.add_argument("--from-beginning", action="store_true", help="Consume from earliest offset.")
    parser.add_argument("--limit", type=int, default=None, help="Max messages to consume.")
    parser.add_argument("--batch-size", type=int, default=500, help="Snowflake insert batch size.")
    parser.add_argument("--timeout", type=int, default=5000, help="Consumer timeout (ms).")
    parser.add_argument("--no-auto-commit", action="store_true", help="Disable auto offset commits.")

    # Snowflake options
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

    required = {
        "account": "--account / SNOWFLAKE_ACCOUNT",
        "user": "--user / SNOWFLAKE_USER",
        "database": "--database / SNOWFLAKE_DATABASE",
        "schema": "--schema / SNOWFLAKE_SCHEMA",
        "table": "--table / SNOWFLAKE_TABLE",
    }
    missing = [hint for field, hint in required.items() if not getattr(args, field)]
    if missing:
        parser.error(f"Missing required Snowflake options: {', '.join(missing)}")
    return args


def main() -> None:
    args = parse_args()
    auto_offset_reset = "earliest" if args.from_beginning else "latest"
    group_id = args.group_id if args.group_id else None
    consumer = build_consumer(
        bootstrap_servers=args.bootstrap,
        topic=args.topic,
        group_id=group_id,
        auto_offset_reset=auto_offset_reset,
        enable_auto_commit=not args.no_auto_commit,
        consumer_timeout_ms=args.timeout,
    )

    fq_table = qualified_table(args.database, args.schema, args.table)
    source_name = f"kafka:{args.topic}"
    buffer = []
    consumed = 0

    print(f"Streaming from Kafka topic '{args.topic}' into Snowflake table {fq_table}")
    with connect(args) as conn:
        ensure_table(conn, fq_table)
        try:
            for message in consumer:
                record = message.value
                if not isinstance(record, dict):
                    # Attempt to parse str payloads
                    try:
                        record = json.loads(record)
                    except Exception:
                        print(f"Skipping non-JSON message at offset {message.offset}")
                        continue
                buffer.append({"record": record, "line_number": message.offset})
                consumed += 1
                if len(buffer) >= args.batch_size:
                    insert_batch(conn, fq_table, source_name, buffer)
                    buffer.clear()
                    conn.commit()
                    print(f"Inserted {consumed:,} messages...", end="\r")
                if args.limit and consumed >= args.limit:
                    break
        except KeyboardInterrupt:
            print("\nInterrupted by user.")
        except KafkaError as err:
            print(f"\nKafka error: {err}")
            sys.exit(1)
        finally:
            if buffer:
                insert_batch(conn, fq_table, source_name, buffer)
                conn.commit()
            consumer.close()

    print(f"\nFinished streaming {consumed:,} messages into {fq_table}")


if __name__ == "__main__":
    main()

