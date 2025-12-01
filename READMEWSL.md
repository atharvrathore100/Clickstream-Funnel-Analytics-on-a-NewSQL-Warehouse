# WSL Quickstart (Kafka + Python + Profiling + Producer + Consumer)

This project runs fully inside WSL Ubuntu, not Windows.  
Use these commands exactly as written.

---
# Milestone 1: 

## 1) Create and activate your Python venv
```bash
cd ~/Clickstream-Funnel-Analytics-on-a-NewSQL-Warehouse
python3 -m venv BDT
source BDT/bin/activate
pip install -r requirements.txt
```

---

## 2) Install and run Kafka (WSL, Kafka KRaft mode — no Zookeeper)

### Download & extract Kafka  
(Example: Kafka 3.9.1 — the version you know exists)
```bash
cd ~
wget https://downloads.apache.org/kafka/3.9.1/kafka_2.13-3.9.1.tgz
tar -xzf kafka_2.13-3.9.1.tgz
mv kafka_2.13-3.9.1 kafka
```

### Initialize Kafka storage (first time only)
```bash
cd ~/kafka
bin/kafka-storage.sh random-uuid
```

Copy the UUID it prints, then run:
```bash
bin/kafka-storage.sh format -t <the-uuid> -c config/kraft/server.properties
```

### Start Kafka (every time you reboot WSL)
```bash
cd ~/kafka
bin/kafka-server-start.sh config/kraft/server.properties
```

Leave this terminal open.

---

## 3) Open a NEW WSL terminal and verify/create the topic
```bash
cd ~/kafka
bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

If `wm_pageviews` is missing, create it:
```bash
bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic wm_pageviews \
  --partitions 3 --replication-factor 1
```

---

## 4) Activate your Python venv again in the project folder
```bash
cd ~/Clickstream-Funnel-Analytics-on-a-NewSQL-Warehouse
source BDT/bin/activate
```

---

## 5) Profile the data (Milestone 1 validation)

### Default file:
```bash
python pageviews_profile.py --limit 50000
```

### Or explicit source:
```bash
python pageviews_profile.py \
  --source https://dumps.wikimedia.org/other/pageviews/2024/2024-01/pageviews-20240101-000000.gz \
  --limit 50000
```

---

## 6) Run the Kafka producer
```bash
python kafka_producer.py \
    --bootstrap localhost:9092 \
    --topic wm_pageviews \
    --limit 20000
```

This downloads Wikimedia pageviews → parses them → sends messages into Kafka.

---

## 7) Run the Kafka consumer

### Consume from the beginning:
```bash
python kafka_consumer.py \
    --bootstrap localhost:9092 \
    --topic wm_pageviews \
    --from-beginning \
    --limit 100 \
    --verbose
```

This verifies that your messages actually landed in the topic.

---

## WSL Milestone 1 Summary (Final Checklist)

✓ Kafka installed in WSL  
✓ Kafka initialized in KRaft mode  
✓ Kafka running at localhost:9092  
✓ Topic `wm_pageviews` created  
✓ Virtualenv active (`BDT`)  
✓ Profiling script executed  
✓ Producer sent events  
✓ Consumer read events  

Everything is now clean, reproducible, and ready for Milestone 2.

---

# Milestone 2: Load Raw Datasets into Snowflake (WSL)

Goal: Ingest Wikimedia Pageviews and Clickstream TSV files into Snowflake using the provided `snowflake_stage_loader.py`.

---

## 1) Make sure your venv is active
```bash
cd ~/Clickstream-Funnel-Analytics-on-a-NewSQL-Warehouse
source BDT/bin/activate
```

Snowflake dependencies are already installed because they're in `requirements.txt`.

---

## 2) Ingest `pageviews.tsv.gz` into Snowflake
Use the exact command:

```bash
python3 snowflake_stage_loader.py \
  --source data/pageviews.tsv.gz \
  --account <Your_account> \
  --user <your_username> \
  --password <your_Password> \
  --warehouse BDT_warehouse \
  --database ANALYTICS \
  --schema PUBLIC \
  --role ACCOUNTADMIN
```

This will load the **pageviews** dataset into your Snowflake table `ANALYTICS.PUBLIC.PAGEVIEWS_RAW`.

---

## 3) Ingest `clickstream.tsv` into Snowflake
If your extracted TSV file is named `clickstream.tsv`, run:

```bash
python3 snowflake_stage_loader.py \
  --source data/clickstream.tsv \
  --account <Your_account> \
  --user <your_username> \
  --password <your_Password> \
  --warehouse BDT_warehouse \
  --database ANALYTICS \
  --schema PUBLIC \
  --role ACCOUNTADMIN
```

This loads the **clickstream edge data** into `ANALYTICS.PUBLIC.CLICKSTREAM_RAW` (auto-created table via script).

---

# Milestone 2 Summary (Final Checklist)

✓ Snowflake CLI not required (we use Python connector)  
✓ Warehouse `BDT_warehouse` exists  
✓ Database `ANALYTICS` and schema `PUBLIC` exist  
✓ `pageviews.tsv.gz` ingested successfully  
✓ `clickstream.tsv` ingested successfully  
✓ Raw VARIANT tables created if not present  
✓ Ready for Milestone 3 (Transform layer + Funnels + Aggregation)

---

## Bonus: Stream Kafka directly into Snowflake
Once producers are running, bridge Milestone 1 and 2 in real time:

```bash
python kafka_producer.py --bootstrap localhost:9092 --topic wm_pageviews --limit 50000

python kafka_to_snowflake.py \
  --bootstrap localhost:9092 \
  --topic wm_pageviews \
  --from-beginning \
  --batch-size 1000 \
  --account <Your_account> \
  --user <your_username> \
  --password <your_Password> \
  --warehouse BDT_warehouse \
  --database ANALYTICS \
  --schema PUBLIC \
  --role ACCOUNTADMIN
```

This consumes Kafka offsets, writes them to Snowflake VARIANT rows, and prints progress as it flushes each batch.

---

## Milestone 3: Spark Sessionization (Kafka ➜ Snowflake MODELED layer)

### Install Java + PySpark inside WSL
```bash
sudo apt install -y openjdk-17-jdk
python3 -m pip install pyspark==3.5.1
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
```

### Prepare Snowflake MODELLED schema
```sql
CREATE SCHEMA IF NOT EXISTS ANALYTICS.MODELLED;
GRANT USAGE, CREATE TABLE ON SCHEMA ANALYTICS.MODELLED TO ROLE ACCOUNTADMIN;
```

### Run the sessionizer (connects Kafka ⇄ Snowflake)
Keep Kafka + producers running, then execute:
```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  spark_sessionizer.py \
  --bootstrap localhost:9092 \
  --topic wm_pageviews \
  --from-beginning \
  --session-gap-minutes 10 \
  --account <Your_account> \
  --user <your_username> \
  --password <your_Password> \
  --warehouse BDT_warehouse \
  --database ANALYTICS \
  --raw-schema PUBLIC \
  --raw-table PAGEVIEWS_RAW \
  --target-schema MODELLED \
  --target-table SESSION_METRICS \
  --seed-from-raw \
  --truncate-target
```

This backfills sessions using the Snowflake RAW table (Milestone 2) and keeps streaming Kafka events into the modeled table. Checkpoint files live under `data/checkpoints/sessionizer`.

### Verify modeled data
```sql
SELECT project, session_start, session_end, events
FROM ANALYTICS.MODELLED.SESSION_METRICS
ORDER BY session_start DESC
LIMIT 20;
```

---

## Milestone 4: Streamlit Dashboard wired to Snowflake

### Export Snowflake env vars inside WSL
```bash
export SNOWFLAKE_ACCOUNT=mawsfhr-wb66764
export SNOWFLAKE_USER=atharvrathore
export SNOWFLAKE_PASSWORD=Atharvrathore@06
export SNOWFLAKE_WAREHOUSE=BDT_warehouse
export SNOWFLAKE_DATABASE=ANALYTICS
export SNOWFLAKE_TARGET_SCHEMA=MODELLED
export SNOWFLAKE_TARGET_TABLE=SESSION_METRICS
```

### Keep Milestones 1-3 running
- Kafka producer + topic with live data
- Snowflake ingestion (loader or Kafka bridge)
- Spark sessionizer streaming into `MODELLED.SESSION_METRICS`

### Launch Streamlit
```bash
cd ~/Clickstream-Funnel-Analytics-on-a-NewSQL-Warehouse
source BDT/bin/activate
python -m streamlit run streamlit_app.py
```

Choose the desired view in the sidebar:
- **Local Clickstream Demo** keeps the TSV-based exploration for quick testing.
- **Snowflake Streaming Sessions** queries the Snowflake modeled table (lookback window + record limit controls available). If it errors, re-check the env vars and ensure Spark is still writing rows.

---

## Steps to run full project

1. **Project setup**
   ```bash
   cd ~/Clickstream-Funnel-Analytics-on-a-NewSQL-Warehouse
   python3 -m venv BDT && source BDT/bin/activate
   python3 -m pip install --upgrade pip
   python3 -m pip install -r requirements.txt
   ```

2. **Kafka (Milestone 1)**
   - Download + format (first time):
     ```bash
     cd ~
     wget https://downloads.apache.org/kafka/3.9.1/kafka_2.13-3.9.1.tgz
     tar -xzf kafka_2.13-3.9.1.tgz && mv kafka_2.13-3.9.1 kafka
     cd ~/kafka
     bin/kafka-storage.sh random-uuid
     bin/kafka-storage.sh format -t <uuid> -c config/kraft/server.properties
     ```
   - Start broker (keep terminal open):
     ```bash
     cd ~/kafka
     bin/kafka-server-start.sh config/kraft/server.properties
     ```
   - Create/check topic:
     ```bash
     cd ~/kafka
     bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic wm_pageviews --partitions 3 --replication-factor 1
     ```

3. **Milestone 1 scripts**
   ```bash
   cd ~/Clickstream-Funnel-Analytics-on-a-NewSQL-Warehouse
   source BDT/bin/activate
   python pageviews_profile.py --limit 50000
   python kafka_producer.py --bootstrap localhost:9092 --topic wm_pageviews --limit 50000
   python kafka_consumer.py --bootstrap localhost:9092 --topic wm_pageviews --from-beginning --limit 100 --verbose
   ```

4. **Snowflake raw ingestion (Milestone 2)**
   - Export env vars:
     ```bash
     export SNOWFLAKE_ACCOUNT=mawsfhr-wb66764
     export SNOWFLAKE_USER=atharvrathore
     export SNOWFLAKE_PASSWORD='Atharvrathore@06'
     export SNOWFLAKE_WAREHOUSE=BDT_warehouse
     export SNOWFLAKE_DATABASE=ANALYTICS
     export SNOWFLAKE_SCHEMA=PUBLIC
     ```
   - Snowflake SQL (run once as ACCOUNTADMIN):
     ```sql
     CREATE WAREHOUSE IF NOT EXISTS BDT_warehouse WAREHOUSE_SIZE='XSMALL' AUTO_SUSPEND=60 AUTO_RESUME=TRUE;
     CREATE DATABASE IF NOT EXISTS ANALYTICS;
     CREATE SCHEMA IF NOT EXISTS ANALYTICS.PUBLIC;
     GRANT USAGE ON WAREHOUSE BDT_warehouse TO ROLE SYSADMIN;
     GRANT USAGE ON DATABASE ANALYTICS TO ROLE SYSADMIN;
     GRANT USAGE, CREATE TABLE ON SCHEMA ANALYTICS.PUBLIC TO ROLE SYSADMIN;
     ```
   - Load data:
     ```bash
     python3 snowflake_stage_loader.py --source data/pageviews.tsv.gz --role ACCOUNTADMIN
     python3 snowflake_stage_loader.py --source data/clickstream.tsv --table CLICKSTREAM_RAW --role ACCOUNTADMIN
     ```
   - Optional Kafka → Snowflake bridge:
     ```bash
     python kafka_to_snowflake.py --bootstrap localhost:9092 --topic wm_pageviews --from-beginning --batch-size 1000 --role ACCOUNTADMIN
     ```

5. **Spark sessionizer (Milestone 3)**
   ```bash
   sudo apt install -y openjdk-17-jdk
   python3 -m pip install pyspark==3.5.1
   export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
   ```
   - Prepare modeled schema:
     ```sql
     CREATE SCHEMA IF NOT EXISTS ANALYTICS.MODELLED;
     GRANT USAGE, CREATE TABLE ON SCHEMA ANALYTICS.MODELLED TO ROLE ACCOUNTADMIN;
     ```
   - Run streaming job (new terminal with env vars):
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
       --raw-schema PUBLIC \
       --raw-table PAGEVIEWS_RAW \
       --target-schema MODELLED \
       --target-table SESSION_METRICS \
       --seed-from-raw \
       --truncate-target
     ```

6. **Streamlit dashboard (Milestone 4)**
   ```bash
   export SNOWFLAKE_TARGET_SCHEMA=MODELLED
   export SNOWFLAKE_TARGET_TABLE=SESSION_METRICS
   python -m streamlit run streamlit_app.py
   ```
   - Sidebar → choose “Snowflake Streaming Sessions” for live data; “Local Clickstream Demo” for TSV exploration.

7. **Verify pipeline**
   - Kafka offsets: `~/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group clickstream-consumer-group`
   - Snowflake RAW: `SELECT COUNT(*) FROM ANALYTICS.PUBLIC.PAGEVIEWS_RAW;`
   - Snowflake MODELED: `SELECT COUNT(*) FROM ANALYTICS.MODELLED.SESSION_METRICS;`
   - Streamlit shows session metrics without errors.
