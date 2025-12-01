import altair as alt
import pandas as pd
import streamlit as st

from clickstream_loader import (
    DEFAULT_CLICKSTREAM_URL,
    available_pages,
    compute_funnel,
    load_clickstream,
    top_dropoffs,
    top_entries,
    top_transitions,
)
from snowflake_dashboard import fetch_session_metrics


st.set_page_config(page_title="Clickstream Funnel Analytics", layout="wide")


@st.cache_data(show_spinner=False)
def _load_data(url: str, limit_rows: int | None):
    return load_clickstream(url=url, limit_rows=limit_rows)


@st.cache_data(ttl=60)
def _load_sessions(hours: int, limit: int):
    return fetch_session_metrics(hours=hours, limit=limit)


def render_local_demo() -> None:
    st.sidebar.header("Local clickstream data")
    dataset_choice = st.sidebar.radio(
        "Clickstream source",
        options=[
            "German (smaller, good for demos)",
            "English (full, larger download)",
            "Custom URL",
        ],
        index=0,
    )

    if dataset_choice == "English (full, larger download)":
        selected_url = (
            "https://dumps.wikimedia.org/other/clickstream/2023-10/clickstream-enwiki-2023-10.tsv.gz"
        )
    elif dataset_choice == "Custom URL":
        selected_url = st.sidebar.text_input(
            "URL", value=DEFAULT_CLICKSTREAM_URL, help="Point to any clickstream TSV.gz file."
        )
    else:
        selected_url = DEFAULT_CLICKSTREAM_URL

    sample_rows = st.sidebar.slider(
        "Row cap (keeps the demo responsive)",
        min_value=5000,
        max_value=200000,
        step=5000,
        value=50000,
        help="Set to the maximum to process more of the dataset.",
    )
    use_full = st.sidebar.checkbox("Load full dataset (may take longer)", value=False)
    limit_rows = None if use_full else sample_rows

    with st.spinner("Loading clickstream data..."):
        df = _load_data(selected_url, limit_rows)

    st.success(f"Loaded {len(df):,} edges from {selected_url}")

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Unique pages", f"{df['curr'].nunique():,}")
    with col2:
        st.metric("Unique referrers", f"{df['prev'].nunique():,}")
    with col3:
        st.metric("Total transitions", f"{int(df['n'].sum()):,}")

    st.divider()
    st.subheader("Top Entry Pages")
    entry_df = top_entries(df)
    if entry_df.empty:
        st.info("No entry pages found. Try loading more rows or a larger dataset.")
    else:
        chart = (
            alt.Chart(entry_df)
            .mark_bar(color="#3b82f6")
            .encode(
                x=alt.X("n:Q", title="Visits"),
                y=alt.Y("curr:N", sort="-x", title="Page"),
                tooltip=["curr", "n"],
            )
        )
        st.altair_chart(chart, use_container_width=True)

    st.subheader("Top Navigation Paths")
    trans_df = top_transitions(df)
    if not trans_df.empty:
        chart = (
            alt.Chart(trans_df)
            .mark_bar(color="#22c55e")
            .encode(
                x=alt.X("n:Q", title="Transitions"),
                y=alt.Y("prev:N", sort="-x", title="From"),
                color=alt.Color("curr:N", legend=None),
                tooltip=["prev", "curr", "n"],
            )
        )
        st.altair_chart(chart, use_container_width=True)

    st.subheader("Top Drop-offs")
    drop_df = top_dropoffs(df)
    if not drop_df.empty:
        chart = (
            alt.Chart(drop_df)
            .mark_bar(color="#ef4444")
            .encode(
                x=alt.X("n:Q", title="Exits"),
                y=alt.Y("prev:N", sort="-x", title="Page"),
                tooltip=["prev", "n"],
            )
        )
        st.altair_chart(chart, use_container_width=True)

    st.divider()
    st.subheader("Funnel Builder")
    options = available_pages(df)
    default_steps = options[:3] if len(options) >= 3 else options
    steps = st.multiselect(
        "Choose ordered funnel steps",
        options=options,
        default=default_steps,
        help="Pick at least two pages to evaluate drop-offs.",
    )
    edges, volume = compute_funnel(df, steps)
    if len(steps) < 2:
        st.info("Select two or more steps to compute a funnel.")
    elif volume == 0:
        st.warning("No traffic detected for this funnel path in the loaded data.")
    else:
        st.metric("Funnel completions (min edge volume)", f"{volume:,}")
        st.table(edges)

        edge_df = pd.DataFrame(edges)
        chart = (
            alt.Chart(edge_df)
            .mark_bar(color="#6366f1")
            .encode(
                x=alt.X("count:Q", title="Transitions"),
                y=alt.Y("to:N", sort="-x", title="Step"),
                tooltip=["from", "to", "count"],
            )
        )
        st.altair_chart(chart, use_container_width=True)

    st.divider()
    st.subheader("Raw Sample")
    st.dataframe(df.head(20))


def render_snowflake_sessions() -> None:
    st.sidebar.header("Snowflake streaming data")
    lookback_hours = st.sidebar.slider(
        "Lookback window (hours)",
        min_value=1,
        max_value=72,
        value=24,
        help="Controls how far back to query sessionized data from Snowflake.",
    )
    limit = st.sidebar.slider("Max sessions to fetch", min_value=100, max_value=2000, step=100, value=500)
    try:
        with st.spinner("Querying Snowflake MODELLED tables..."):
            df, summary = _load_sessions(lookback_hours, limit)
    except RuntimeError as exc:
        st.error(
            f"{exc}\n\nSet SNOWFLAKE_ACCOUNT / USER / PASSWORD (and optional WAREHOUSE/DATABASE) before using this view."
        )
        return
    except Exception as exc:  # pragma: no cover
        st.error(f"Failed to query Snowflake: {exc}")
        return

    st.success(
        "Connected to Snowflake and reading sessionized data driven by Milestones 1-3 (Kafka ➜ Snowflake ➜ Spark)."
    )

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Sessions", f"{summary['sessions']:,}")
    with col2:
        st.metric("Events", f"{summary['events']:,}")
    with col3:
        st.metric("Views", f"{summary['views']:,}")
    with col4:
        st.metric("Bytes", f"{summary['bytes']:,}")

    if df.empty:
        st.info("No session data found in the selected window. Let the streaming job run longer or widen the window.")
        return

    df["session_start"] = pd.to_datetime(df["session_start"])
    df["session_end"] = pd.to_datetime(df["session_end"])

    st.subheader("Sessions over time")
    trend = (
        df.groupby(pd.Grouper(key="session_start", freq="1H"))
        .agg({"events": "sum"})
        .reset_index()
        .rename(columns={"events": "total_events"})
    )
    chart = (
        alt.Chart(trend)
        .mark_line(color="#3b82f6")
        .encode(
            x=alt.X("session_start:T", title="Hour"),
            y=alt.Y("total_events:Q", title="Events"),
            tooltip=["session_start", "total_events"],
        )
    )
    st.altair_chart(chart, use_container_width=True)

    st.subheader("Top projects by session volume")
    proj_df = (
        df.groupby("project", as_index=False)["events"]
        .sum()
        .sort_values("events", ascending=False)
        .head(10)
    )
    proj_chart = (
        alt.Chart(proj_df)
        .mark_bar(color="#10b981")
        .encode(
            x=alt.X("events:Q", title="Events"),
            y=alt.Y("project:N", sort="-x", title="Project"),
            tooltip=["project", "events"],
        )
    )
    st.altair_chart(proj_chart, use_container_width=True)

    st.subheader("Session detail (Snowflake MODELLED.SESSION_METRICS)")
    st.dataframe(df.head(limit))


def main() -> None:
    st.title("Clickstream Funnel Analytics")
    st.caption("Milestones 1-4: Kafka ➜ Snowflake ➜ Spark ➜ Streamlit.")

    mode = st.sidebar.selectbox(
        "Data view",
        options=[
            "Local Clickstream Demo",
            "Snowflake Streaming Sessions",
        ],
        help="Local view uses cached TSVs; Snowflake view reads Milestones 1-3 output.",
    )

    if mode == "Snowflake Streaming Sessions":
        render_snowflake_sessions()
    else:
        render_local_demo()


if __name__ == "__main__":
    main()
