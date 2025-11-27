# Clickstream Funnel Analytics

An interactive Streamlit demo that ingests Wikimedia clickstream data, surfaces entry/drop-off pages, and lets you build quick funnels without standing up full MSK/Spark/Snowflake infrastructure.

## Quickstart
- Use Python 3.10 or 3.11 (pandas wheels are not yet published for 3.13 on Windows).
- Install deps: `python -m pip install -r requirements.txt`
- Run the app: `python -m streamlit run streamlit_app.py`
- Open the URL Streamlit prints (default http://localhost:8501) and explore.

## How it works
- Downloads a Wikimedia clickstream TSV.gz (defaults to the smaller German dataset for speed; switch to English in the UI).
- Caches the data locally under `data/` and reloads via parquet for fast restarts.
- Shows top entry pages, high-volume navigation paths, and biggest drop-offs.
- Funnel builder lets you pick ordered pages and estimates completions/drop-offs from aggregated edge counts.

## Configuration
- Switch datasets in the sidebar between German (smaller), English (full), or a custom URL.
- To force a different default without the UI: set `CLICKSTREAM_URL` before running Streamlit.
- Use the sidebar row cap to keep the demo responsive; uncheck to process the full file.

## Milestone 1: Data Profiling & Ingestion
Goal: Set up a Kafka producer, profile Wikimedia pageviews JSON, and validate schema quality.

### 1) Create/verify Kafka topic
- Ensure Kafka is reachable (MSK/localhost) and create a topic, e.g. `wm_pageviews`.
  - Local example: `kafka-topics --bootstrap-server localhost:9092 --create --topic wm_pageviews --partitions 3 --replication-factor 1`

### 2) Produce pageview events into Kafka
- Command: `python kafka_producer.py --bootstrap localhost:9092 --topic wm_pageviews --limit 20000`
- Flags:
  - `--source`: URL or local `pageviews-YYYYMMDD-HH0000.gz` (defaults to 2024-01-01-00).
  - `--limit`: optional cap to keep tests fast.
  - Env overrides: `KAFKA_BOOTSTRAP`, `KAFKA_TOPIC`, `PAGEVIEWS_SOURCE`.

### 3) Profile the raw pageview schema/quality
- Command: `python pageviews_profile.py --limit 50000`
- Flags:
  - `--source`: same URL/path as above; defaults to 2024-01-01-00.
  - `--limit`: rows to scan; set to `0` or omit to scan all (can be large).
- Output includes field completeness, top projects/pages, and a quick bytes sanity check.

### 4) What remains for Milestone 1
- Run the producer against your Kafka endpoint and confirm events land in `wm_pageviews`.
- Run the profiler on the same file/stream to validate schema and spot anomalies.
- Optional: add alerting/validation rules (e.g., drop records missing `page` or `project` before producing).
