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

## Milestone 3: Sessionization & Transformation Layer
Goal: Use Spark Structured Streaming to read the Kafka topic, build session aggregates, and land the modeled results beside the Milestone 2 RAW tables.

### Prerequisites
```bash
sudo apt install -y openjdk-17-jdk
python3 -m pip install pyspark==3.5.1
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64  # adjust for your distro
```
Snowflake prep (run once):
```sql
CREATE SCHEMA IF NOT EXISTS ANALYTICS.MODELLED;
GRANT USAGE, CREATE TABLE ON SCHEMA ANALYTICS.MODELLED TO ROLE SYSADMIN;
```

### Stream Kafka into modeled Snowflake tables
1. Ensure Milestones 1 & 2 are producing data (Kafka topic populated, Snowflake RAW table filling via `snowflake_stage_loader.py` or `kafka_to_snowflake.py`).
2. Start the sessionizer (run from your project root). `spark-submit` downloads the Kafka connector automatically via `--packages`.
```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  spark_sessionizer.py \
  --bootstrap localhost:9092 \
  --topic wm_pageviews \
  --from-beginning \
  --session-gap-minutes 10 \
  --account "$SNOWFLAKE_ACCOUNT" \
  --user "$SNOWFLAKE_USER" \
  --password "$SNOWFLAKE_PASSWORD" \
  --warehouse "$SNOWFLAKE_WAREHOUSE" \
  --database "$SNOWFLAKE_DATABASE" \
  --raw-schema RAW \
  --raw-table PAGEVIEWS_RAW \
  --target-schema MODELLED \
  --target-table SESSION_METRICS \
  --seed-from-raw \
  --truncate-target
```

What happens:
- The script optionally backfills (`--seed-from-raw`) using the staged RAW data (Milestone 2), proving both layers communicate.
- Structured Streaming reads live Kafka events, groups them by project + session window (gap configurable via `--session-gap-minutes`), and writes aggregates to `ANALYTICS.MODELLED.SESSION_METRICS`.
- State is checkpointed under `data/checkpoints/sessionizer` so restarts resume automatically.

### Validate the modeled layer
```sql
SELECT project,
       session_start,
       session_end,
       events,
       total_views,
       total_bytes
FROM ANALYTICS.MODELLED.SESSION_METRICS
ORDER BY session_start DESC
LIMIT 20;
```

If you need to regenerate sessions, rerun the command with `--truncate-target --seed-from-raw` before letting the stream catch up on Kafka offsets.

## Milestone 4: Dashboard (Streamlit) Connected to the Pipeline
The Streamlit app now supports two views:
1. **Local Clickstream Demo** – the existing TSV-based experience for fast iteration.
2. **Snowflake Streaming Sessions** – a live dashboard powered by the Kafka ➜ Snowflake ➜ Spark pipeline from Milestones 1-3.

### Prerequisites
- Python deps already installed (`streamlit`, `snowflake-connector`, `pyspark`).
- Snowflake environment variables exported (reuse the ones from Milestones 2-3):
  ```bash
  export SNOWFLAKE_ACCOUNT=xy12345.us-east-1
  export SNOWFLAKE_USER=demo_user
  export SNOWFLAKE_PASSWORD='*****'
  export SNOWFLAKE_WAREHOUSE=LOAD_WH
  export SNOWFLAKE_DATABASE=ANALYTICS
  export SNOWFLAKE_TARGET_SCHEMA=MODELLED
  export SNOWFLAKE_TARGET_TABLE=SESSION_METRICS
  ```
- Milestone 1-3 services running (Kafka producer or bridge populating Snowflake, Spark sessionizer streaming results into `MODELLED.SESSION_METRICS`).

### Run the dashboard
```bash
python -m streamlit run streamlit_app.py
```

Use the sidebar selector:
- **Local Clickstream Demo** – identical to the original view (top entries, funnels, etc.).
- **Snowflake Streaming Sessions** – queries the modeled Snowflake table, displays session metrics, trends, and the current contents of `ANALYTICS.MODELLED.SESSION_METRICS`.

If the Snowflake view shows a configuration error, verify the environment variables and that the streaming job is writing rows. Once it connects, the metrics confirm end-to-end connectivity across all four milestones.
