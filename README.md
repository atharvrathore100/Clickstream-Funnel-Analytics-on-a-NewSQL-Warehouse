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
- Switch datasets in the sidebar between Simple English (small), English (full), or a custom URL.
- To force a different default without the UI: set `CLICKSTREAM_URL` before running Streamlit.
- Use the sidebar row cap to keep the demo responsive; uncheck to process the full file.

## Next steps
- Wire the loader into a Kafka producer to replay events into MSK.
- Replace the cached parquet with a Snowflake table via the Snowflake connector.
- Add Spark Structured Streaming on top of Kafka for sessionization and richer funnel math.
