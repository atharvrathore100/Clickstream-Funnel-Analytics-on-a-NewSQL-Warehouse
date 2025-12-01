"""
Helpers to fetch aggregated data from Snowflake for the Streamlit dashboard.

The module expects the same SNOWFLAKE_* environment variables used by the
ingestion/streaming scripts so that Milestones 1-3 feed the dashboard seamlessly.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Dict, Tuple

import pandas as pd
import snowflake.connector

from snowflake_stage_loader import qualified_table


@dataclass
class SnowflakeConfig:
    account: str
    user: str
    password: str
    warehouse: str
    database: str
    schema: str
    table: str
    role: str | None = None
    authenticator: str | None = None


def load_config() -> SnowflakeConfig:
    env = os.environ
    account = env.get("SNOWFLAKE_ACCOUNT")
    user = env.get("SNOWFLAKE_USER")
    password = env.get("SNOWFLAKE_PASSWORD")
    warehouse = env.get("SNOWFLAKE_WAREHOUSE", "LOAD_WH")
    database = env.get("SNOWFLAKE_DATABASE", "ANALYTICS")
    schema = env.get("SNOWFLAKE_TARGET_SCHEMA", env.get("SNOWFLAKE_SCHEMA", "MODELLED"))
    table = env.get("SNOWFLAKE_TARGET_TABLE", "SESSION_METRICS")
    role = env.get("SNOWFLAKE_ROLE")
    authenticator = env.get("SNOWFLAKE_AUTHENTICATOR")

    missing = [name for name, value in (("SNOWFLAKE_ACCOUNT", account), ("SNOWFLAKE_USER", user), ("SNOWFLAKE_PASSWORD", password)) if not value]
    if missing:
        raise RuntimeError(f"Missing Snowflake configuration: {', '.join(missing)}")
    return SnowflakeConfig(
        account=account,
        user=user,
        password=password,
        warehouse=warehouse,
        database=database,
        schema=schema,
        table=table,
        role=role,
        authenticator=authenticator,
    )


def _conn_kwargs(cfg: SnowflakeConfig) -> Dict[str, str]:
    opts = {
        "account": cfg.account,
        "user": cfg.user,
        "password": cfg.password,
        "warehouse": cfg.warehouse,
        "database": cfg.database,
        "schema": cfg.schema,
    }
    if cfg.role:
        opts["role"] = cfg.role
    if cfg.authenticator:
        opts["authenticator"] = cfg.authenticator
    return opts


def fetch_session_metrics(hours: int = 24, limit: int = 500) -> Tuple[pd.DataFrame, Dict[str, float]]:
    """
    Return session aggregates from Snowflake for the dashboard.

    Args:
        hours: Lookback window in hours.
        limit: Maximum number of session rows to return.
    """
    cfg = load_config()
    table = qualified_table(cfg.database, cfg.schema, cfg.table)
    query = f"""
        SELECT
            project,
            session_start,
            session_end,
            events,
            total_views,
            total_bytes
        FROM {table}
        WHERE session_start >= DATEADD('hour', -%(hours)s, CURRENT_TIMESTAMP())
        ORDER BY session_start DESC
        LIMIT %(limit)s
    """
    with snowflake.connector.connect(**_conn_kwargs(cfg)) as conn:
        df = pd.read_sql(query, conn, params={"hours": hours, "limit": limit})

    if df.empty:
        summary = {"sessions": 0, "events": 0, "views": 0, "bytes": 0}
    else:
        summary = {
            "sessions": int(len(df)),
            "events": int(df["EVENTS"].sum()),
            "views": int(df["TOTAL_VIEWS"].fillna(0).sum()),
            "bytes": int(df["TOTAL_BYTES"].fillna(0).sum()),
        }

    df.columns = [col.lower() for col in df.columns]
    return df, summary

