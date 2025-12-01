"""
Structured Streaming sessionization that consumes Kafka pageview events and writes
aggregated sessions into Snowflake.

Usage example:
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \\
        spark_sessionizer.py \\
        --bootstrap localhost:9092 \\
        --topic wm_pageviews \\
        --from-beginning \\
        --session-gap-minutes 10 \\
        --account xy12345.us-east-1 \\
        --user demo_user \\
        --password ***** \\
        --warehouse LOAD_WH \\
        --database ANALYTICS \\
        --raw-schema RAW \\
        --raw-table PAGEVIEWS_RAW \\
        --target-schema MODELLED \\
        --target-table SESSION_METRICS \\
        --seed-from-raw --truncate-target
"""

from __future__ import annotations

import argparse
import getpass
import os
from functools import partial
from typing import Dict, List, Tuple

import snowflake.connector
from pyspark.sql import DataFrame, SparkSession, functions as F, types as T

from snowflake_stage_loader import qualified_table, quote_ident

KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"


def build_conn_options(args: argparse.Namespace) -> Dict[str, str]:
    password = args.password or os.getenv("SNOWFLAKE_PASSWORD")
    if not password:
        password = getpass.getpass("Snowflake password: ")
    options = {
        "account": args.account,
        "user": args.user,
        "password": password,
        "warehouse": args.warehouse,
        "database": args.database,
        "schema": args.target_schema,
    }
    if args.role:
        options["role"] = args.role
    if args.authenticator:
        options["authenticator"] = args.authenticator
    return options


def ensure_target_table(
    conn: snowflake.connector.SnowflakeConnection,
    database: str,
    schema: str,
    table: str,
) -> None:
    schema_qual = f"{quote_ident(database)}.{quote_ident(schema)}"
    fq_table = qualified_table(database, schema, table)
    with conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_qual}")
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {fq_table} (
                project STRING,
                session_start TIMESTAMP_LTZ,
                session_end TIMESTAMP_LTZ,
                events NUMBER,
                total_views NUMBER,
                total_bytes NUMBER,
                loaded_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
            )
            """
        )


def truncate_table(conn: snowflake.connector.SnowflakeConnection, fq_table: str) -> None:
    with conn.cursor() as cur:
        cur.execute(f"TRUNCATE TABLE {fq_table}")


def seed_from_raw(
    conn: snowflake.connector.SnowflakeConnection,
    raw_table: str,
    target_table: str,
    gap_minutes: int,
) -> None:
    seed_sql = f"""
        INSERT INTO {target_table}
            (project, session_start, session_end, events, total_views, total_bytes, loaded_at)
        SELECT
            COALESCE(raw:project::string, 'unknown') AS project,
            DATE_TRUNC('minute', ingested_at) AS session_start,
            DATEADD('minute', %(gap)s, DATE_TRUNC('minute', ingested_at)) AS session_end,
            COUNT(*) AS events,
            SUM(TRY_TO_NUMBER(raw:views::string)) AS total_views,
            SUM(TRY_TO_NUMBER(raw:bytes::string)) AS total_bytes,
            CURRENT_TIMESTAMP()
        FROM {raw_table}
        GROUP BY 1, 2, 3
    """
    with conn.cursor() as cur:
        cur.execute(seed_sql, {"gap": gap_minutes})


def write_batch(
    df: DataFrame,
    epoch_id: int,
    conn_options: Dict[str, str],
    target_table: str,
) -> None:
    if df.rdd.isEmpty():
        return
    flattened = (
        df.withColumn("session_start", F.col("session_window.start"))
        .withColumn("session_end", F.col("session_window.end"))
        .drop("session_window")
    )
    rows: List[Dict[str, object]] = []
    for row in flattened.collect():
        rows.append(
            {
                "project": row["project"],
                "session_start": row["session_start"],
                "session_end": row["session_end"],
                "events": int(row["event_count"]) if row["event_count"] is not None else None,
                "total_views": int(row["total_views"]) if row["total_views"] is not None else None,
                "total_bytes": int(row["total_bytes"]) if row["total_bytes"] is not None else None,
            }
        )
    if not rows:
        return
    insert_sql = f"""
        INSERT INTO {target_table}
            (project, session_start, session_end, events, total_views, total_bytes, loaded_at)
        VALUES (%(project)s, %(session_start)s, %(session_end)s, %(events)s, %(total_views)s, %(total_bytes)s, CURRENT_TIMESTAMP())
    """
    with snowflake.connector.connect(**conn_options) as conn:
        with conn.cursor() as cur:
            cur.executemany(insert_sql, rows)
        conn.commit()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Stream Kafka pageviews, perform sessionization, and load Snowflake.",
    )
    parser.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP", "localhost:9092"))
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "wm_pageviews"))
    parser.add_argument("--group-id", default=os.getenv("KAFKA_GROUP_ID"))
    parser.add_argument("--from-beginning", action="store_true", help="Start from earliest offsets.")
    parser.add_argument("--session-gap-minutes", type=int, default=10, help="Gap duration defining a session.")
    parser.add_argument("--watermark-minutes", type=int, default=15, help="Late data watermark.")
    parser.add_argument("--batch-size", type=int, default=2000, help="Unused placeholder for parity.")
    parser.add_argument(
        "--checkpoint-dir",
        default="data/checkpoints/sessionizer",
        help="Checkpoint directory for streaming state.",
    )

    # Snowflake config
    parser.add_argument("--account", default=os.getenv("SNOWFLAKE_ACCOUNT"))
    parser.add_argument("--user", default=os.getenv("SNOWFLAKE_USER"))
    parser.add_argument("--password", default=os.getenv("SNOWFLAKE_PASSWORD"))
    parser.add_argument("--warehouse", default=os.getenv("SNOWFLAKE_WAREHOUSE", "LOAD_WH"))
    parser.add_argument("--database", default=os.getenv("SNOWFLAKE_DATABASE", "ANALYTICS"))
    parser.add_argument("--raw-schema", default=os.getenv("SNOWFLAKE_RAW_SCHEMA", "RAW"))
    parser.add_argument("--raw-table", default=os.getenv("SNOWFLAKE_RAW_TABLE", "PAGEVIEWS_RAW"))
    parser.add_argument("--target-schema", default=os.getenv("SNOWFLAKE_TARGET_SCHEMA", "MODELLED"))
    parser.add_argument("--target-table", default=os.getenv("SNOWFLAKE_TARGET_TABLE", "SESSION_METRICS"))
    parser.add_argument("--role", default=os.getenv("SNOWFLAKE_ROLE"))
    parser.add_argument("--authenticator", default=os.getenv("SNOWFLAKE_AUTHENTICATOR"))
    parser.add_argument("--seed-from-raw", action="store_true", help="Backfill sessions from staged Snowflake data.")
    parser.add_argument("--truncate-target", action="store_true", help="Truncate target table before streaming.")
    args = parser.parse_args()

    required = {
        "account": "--account / SNOWFLAKE_ACCOUNT",
        "user": "--user / SNOWFLAKE_USER",
    }
    missing = [hint for field, hint in required.items() if not getattr(args, field)]
    if missing:
        parser.error(f"Missing required Snowflake options: {', '.join(missing)}")
    return args


def main() -> None:
    args = parse_args()
    conn_options = build_conn_options(args)
    target_table = qualified_table(args.database, args.target_schema, args.target_table)
    raw_table = qualified_table(args.database, args.raw_schema, args.raw_table)

    with snowflake.connector.connect(**conn_options) as conn:
        ensure_target_table(conn, args.database, args.target_schema, args.target_table)
        if args.truncate_target:
            truncate_table(conn, target_table)
        if args.seed_from_raw:
            seed_from_raw(conn, raw_table, target_table, args.session_gap_minutes)

    session_gap = f"{args.session_gap_minutes} minutes"
    watermark = f"{args.watermark_minutes} minutes"

    spark = (
        SparkSession.builder.appName("ClickstreamSessionizer")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.jars.packages", KAFKA_PACKAGE)
        .getOrCreate()
    )

    schema = T.StructType(
        [
            T.StructField("project", T.StringType()),
            T.StructField("page", T.StringType()),
            T.StructField("views", T.LongType()),
            T.StructField("bytes", T.LongType()),
        ]
    )

    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap)
        .option("subscribe", args.topic)
        .option("startingOffsets", "earliest" if args.from_beginning else "latest")
        .load()
    )

    parsed_df = (
        kafka_df.selectExpr("CAST(value AS STRING) AS json_value", "timestamp")
        .select(F.from_json("json_value", schema).alias("payload"), "timestamp")
        .select("payload.*", "timestamp")
        .na.drop(subset=["project"])
        .fillna({"views": 0, "bytes": 0})
    )

    events_with_ts = parsed_df.withColumn("event_ts", F.col("timestamp")).drop("timestamp")

    sessionized = (
        events_with_ts.withWatermark("event_ts", watermark)
        .groupBy(
            F.session_window("event_ts", session_gap),
            F.col("project"),
        )
        .agg(
            F.count("*").alias("event_count"),
            F.sum("views").alias("total_views"),
            F.sum("bytes").alias("total_bytes"),
        )
    )

    writer = sessionized.writeStream.outputMode("append").option("checkpointLocation", args.checkpoint_dir).foreachBatch(
        partial(write_batch, conn_options=conn_options, target_table=target_table)
    )

    query = writer.start()
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        query.stop()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
