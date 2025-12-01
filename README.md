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

#### Kafka Setup (Local Development)

Kafka requires Java 11 or later. This project uses Java 17.

##### Prerequisites
- Java 17 installed (Kafka requires Java 11+)
- Homebrew (for macOS installation)

##### Installation Steps

1. **Install Kafka and Zookeeper**
   ```bash
   brew install kafka
   brew install zookeeper
   ```
   Note: This installs both Kafka and Zookeeper.

2. **Start Services**
   ```bash
   brew services start zookeeper
   brew services start kafka
   ```

3. **Create Kafka Topic**
   ```bash
   kafka-topics --bootstrap-server localhost:9092 --create --topic wm_pageviews --partitions 3 --replication-factor 1
   ```

##### Verify Installation
- Check if services are running: `brew services list`
- List topics: `kafka-topics --bootstrap-server localhost:9092 --list`
- Describe topic: `kafka-topics --bootstrap-server localhost:9092 --describe --topic wm_pageviews`


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

### 4) Consume pageview events from Kafka
- Command: `python kafka_consumer.py --bootstrap localhost:9092 --topic wm_pageviews --limit 100`
- Flags:
  - `--bootstrap`: Kafka bootstrap servers (default: localhost:9092)
  - `--topic`: Topic to consume from (default: wm_pageviews)
  - `--group-id`: Consumer group ID (default: clickstream-consumer-group)
  - `--from-beginning`: Start from earliest available offset (default: latest)
  - `--limit`: Maximum number of messages to consume (default: unlimited)
  - `--output`: Output file path for messages in JSONL format
  - `--verbose`: Print detailed information about each message
  - `--no-auto-commit`: Disable auto-commit of offsets
  - `--timeout`: Consumer timeout in milliseconds (default: 5000)
- Env overrides: `KAFKA_BOOTSTRAP`, `KAFKA_TOPIC`, `KAFKA_GROUP_ID`

**Examples:**
```bash
# Consume from latest offset with a consumer group
python kafka_consumer.py --bootstrap localhost:9092 --topic wm_pageviews --group-id my-group

# Consume from beginning (all messages)
python kafka_consumer.py --bootstrap localhost:9092 --topic wm_pageviews --from-beginning

# Consume 100 messages and save to file
python kafka_consumer.py --bootstrap localhost:9092 --topic wm_pageviews --limit 100 --output messages.jsonl

# Verbose mode
python kafka_consumer.py --bootstrap localhost:9092 --topic wm_pageviews --verbose
```

<!-- ### 5) What remains for Milestone 1
- Run the producer against your Kafka endpoint and confirm events land in `wm_pageviews`.
- Run the profiler on the same file/stream to validate schema and spot anomalies.
- Optional: add alerting/validation rules (e.g., drop records missing `page` or `project` before producing). -->

## Milestone 2: Warehouse Setup & Raw Staging
Goal: Create a Snowflake warehouse/database/schema and land the raw Wikimedia pageviews into a VARIANT table for downstream modeling.

### Prerequisites (WSL friendly)
```bash
sudo apt update
sudo apt install -y build-essential python3-dev libssl-dev
python3 -m pip install -r requirements.txt          # installs snowflake-connector
python3 -m pip install snowflake-cli-labs           # optional CLI helper
```

### Bootstrap Snowflake objects
Run once with an admin role (Snowsight, SnowSQL, or the CLI):
```sql
USE ROLE ACCOUNTADMIN;
CREATE WAREHOUSE IF NOT EXISTS LOAD_WH
  WITH WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE;

CREATE DATABASE IF NOT EXISTS ANALYTICS;
CREATE SCHEMA IF NOT EXISTS ANALYTICS.RAW;
GRANT USAGE ON WAREHOUSE LOAD_WH TO ROLE SYSADMIN;
GRANT USAGE ON DATABASE ANALYTICS TO ROLE SYSADMIN;
GRANT USAGE, CREATE TABLE ON SCHEMA ANALYTICS.RAW TO ROLE SYSADMIN;
```

### Load raw clickstream/pageview rows
Set env vars (or pass flags) for Snowflake connection info:
```bash
export SNOWFLAKE_ACCOUNT=xy12345.us-east-1
export SNOWFLAKE_USER=demo_user
export SNOWFLAKE_PASSWORD='*****'
export SNOWFLAKE_WAREHOUSE=LOAD_WH
export SNOWFLAKE_DATABASE=ANALYTICS
export SNOWFLAKE_SCHEMA=RAW
export SNOWFLAKE_TABLE=PAGEVIEWS_RAW

python3 snowflake_stage_loader.py --source data/pageviews.tsv.gz
python3 snowflake_stage_loader.py --source data/clickstream.tsv --table CLICKSTREAM_RAW
```

The script auto-creates the VARIANT table:
```sql
CREATE TABLE IF NOT EXISTS ANALYTICS.RAW.PAGEVIEWS_RAW (
  raw VARIANT,
  source_file STRING,
  line_number NUMBER,
  ingested_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);
```

### Validate the load
```sql
USE ANALYTICS.RAW;
SELECT COUNT(*) FROM PAGEVIEWS_RAW;
SELECT raw:project::string AS project,
       raw:page::string AS page,
       raw:views::int AS views
FROM PAGEVIEWS_RAW
LIMIT 20;
```

You now have Milestone 2 staged and ready for Spark/Snowflake transformations.

## Linking Milestones 1 & 2 (Kafka → Snowflake bridge)
The `kafka_to_snowflake.py` helper drains the Kafka topic from Milestone 1 and continuously loads the events into the Snowflake table created in Milestone 2.

### 1) Produce events into Kafka
```bash
python kafka_producer.py \
  --bootstrap localhost:9092 \
  --topic wm_pageviews \
  --limit 20000 \
  --source https://dumps.wikimedia.org/other/pageviews/2024/2024-01/pageviews-20240101-000000.gz
```

### 2) Stream Kafka into Snowflake
Reuse the env vars above or pass them inline:
```bash
python kafka_to_snowflake.py \
  --bootstrap localhost:9092 \
  --topic wm_pageviews \
  --from-beginning \
  --batch-size 1000 \
  --account "$SNOWFLAKE_ACCOUNT" \
  --user "$SNOWFLAKE_USER" \
  --password "$SNOWFLAKE_PASSWORD" \
  --warehouse "$SNOWFLAKE_WAREHOUSE" \
  --database "$SNOWFLAKE_DATABASE" \
  --schema "$SNOWFLAKE_SCHEMA" \
  --table "$SNOWFLAKE_TABLE"
```

Offsets are stored in `line_number`, making replays traceable. Hit `Ctrl+C` once caught up; pending batches flush automatically.

### 3) Confirm counts
```sql
SELECT COUNT(*) AS rows_loaded,
       MAX(line_number) AS max_offset
FROM ANALYTICS.RAW.PAGEVIEWS_RAW;
```

At this point Milestones 1 and 2 operate as a single pipeline: raw dumps → Kafka topic → Snowflake VARIANT staging.
