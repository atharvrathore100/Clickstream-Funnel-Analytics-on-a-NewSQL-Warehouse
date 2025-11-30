# WSL Quickstart (Kafka + Python + Profiling + Producer + Consumer)

This project runs fully inside WSL Ubuntu, not Windows.  
Use these commands exactly as written.

---

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
  --account mawsfhr-wb66764 \
  --user atharvrathore \
  --password Atharvrathore@06 \
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
  --account mawsfhr-wb66764 \
  --user atharvrathore \
  --password Atharvrathore@06 \
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

This is the complete unrendered Markdown, ready to paste into your repo.
