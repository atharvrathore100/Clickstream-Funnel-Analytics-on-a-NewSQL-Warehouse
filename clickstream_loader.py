import os
from pathlib import Path
from typing import List, Optional, Tuple

import pandas as pd

# Default to a smaller dataset (German wiki) to keep download size reasonable.
DEFAULT_CLICKSTREAM_URL = os.environ.get(
    "CLICKSTREAM_URL",
    "https://dumps.wikimedia.org/other/clickstream/2023-10/clickstream-dewiki-2023-10.tsv.gz",
)

DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

PARQUET_CACHE = DATA_DIR / "clickstream.parquet"
URL_CACHE = DATA_DIR / "clickstream.url"


def _download_clickstream(url: str, destination: Path) -> Path:
    """Download the clickstream file if it is not already cached."""
    import requests

    destination.parent.mkdir(parents=True, exist_ok=True)
    response = requests.get(url, stream=True, timeout=90)
    response.raise_for_status()
    with destination.open("wb") as fout:
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                fout.write(chunk)
    return destination


def _read_tsv(path: Path, limit_rows: Optional[int]) -> pd.DataFrame:
    """Read clickstream TSV into a DataFrame."""
    columns = ["prev", "curr", "type", "n"]
    return pd.read_csv(
        path,
        sep="\t",
        names=columns,
        compression="gzip",
        dtype={"prev": "string", "curr": "string", "type": "string", "n": "int64"},
        nrows=limit_rows,
    )


def load_clickstream(url: Optional[str] = None, limit_rows: Optional[int] = None) -> pd.DataFrame:
    """
    Load clickstream data, caching it locally as parquet for fast reloads.

    Args:
        url: Source URL for the clickstream gz file.
        limit_rows: Optional row cap to keep demos light.
    """
    chosen_url = url or DEFAULT_CLICKSTREAM_URL

    # Reuse cached parquet only if it came from the same URL and row limit is not set.
    if PARQUET_CACHE.exists() and URL_CACHE.exists() and limit_rows is None:
        cached_url = URL_CACHE.read_text().strip()
        if cached_url == chosen_url:
            return pd.read_parquet(PARQUET_CACHE)

    gz_path = DATA_DIR / "clickstream.tsv.gz"
    if not gz_path.exists() or not URL_CACHE.exists() or URL_CACHE.read_text().strip() != chosen_url:
        _download_clickstream(chosen_url, gz_path)
        URL_CACHE.write_text(chosen_url)

    df = _read_tsv(gz_path, limit_rows)
    # Only persist the full dataset to parquet to avoid confusing caches when sampling.
    if limit_rows is None:
        df.to_parquet(PARQUET_CACHE, index=False)
    return df


def top_entries(df: pd.DataFrame, top_k: int = 15) -> pd.DataFrame:
    """Top landing pages based on referrers that start outside the wiki."""
    entry_refs = {"other-search", "other-empty", "other-external", "other-internal"}
    entries = df[df["prev"].isin(entry_refs)]
    return (
        entries.groupby("curr", as_index=False)["n"]
        .sum()
        .sort_values("n", ascending=False)
        .head(top_k)
    )


def top_transitions(df: pd.DataFrame, top_k: int = 20) -> pd.DataFrame:
    """Top navigation edges by volume."""
    return df.sort_values("n", ascending=False).head(top_k)


def top_dropoffs(df: pd.DataFrame, top_k: int = 15) -> pd.DataFrame:
    """
    Pages that most often lead to exits. In the dataset, 'other-other'
    represents leaving the wiki.
    """
    exit_rows = df[df["curr"] == "other-other"]
    return (
        exit_rows.groupby("prev", as_index=False)["n"]
        .sum()
        .sort_values("n", ascending=False)
        .head(top_k)
    )


def available_pages(df: pd.DataFrame, top_k: int = 1000) -> List[str]:
    """List of candidate pages for funnel builder."""
    freq = (
        df.groupby("curr", as_index=False)["n"]
        .sum()
        .sort_values("n", ascending=False)
        .head(top_k)
    )
    return freq["curr"].tolist()


def compute_funnel(df: pd.DataFrame, steps: List[str]) -> Tuple[List[dict], int]:
    """
    Compute a simple funnel using edge counts as a proxy for progression.

    The dataset stores aggregated edges, so we approximate funnel volume by
    looking at consecutive transitions and using the minimum observed count
    along the path.
    """
    if len(steps) < 2:
        return [], 0

    edges = []
    for i in range(len(steps) - 1):
        prev_step = steps[i]
        next_step = steps[i + 1]
        count = (
            df[(df["prev"] == prev_step) & (df["curr"] == next_step)]["n"]
            .sum()
        )
        edges.append({"from": prev_step, "to": next_step, "count": int(count)})

    if not edges:
        return [], 0

    # Funnel volume is constrained by the smallest transition count.
    min_count = min(edge["count"] for edge in edges)
    return edges, min_count
