import os, json
from datetime import datetime
import pandas as pd
import numpy as np

from google.oauth2 import service_account
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import DateRange, Dimension, Metric, RunReportRequest
from googleapiclient.discovery import build

from plots import plot_top_queries


# =========================
# Auth
# =========================
SCOPES = [
    "https://www.googleapis.com/auth/webmasters.readonly",
    "https://www.googleapis.com/auth/analytics.readonly",
]

def get_creds():
    sa = json.loads(os.environ["GOOGLE_SA_JSON"])
    return service_account.Credentials.from_service_account_info(sa, scopes=SCOPES)


# =========================
# Fetch GA4
# =========================
def fetch_ga4(creds, property_id: str, start_date: str, end_date: str) -> pd.DataFrame:
    client = BetaAnalyticsDataClient(credentials=creds)
    req = RunReportRequest(
        property=f"properties/{property_id}",
        date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
        dimensions=[Dimension(name="sessionDefaultChannelGroup")],
        metrics=[
            Metric(name="sessions"),
            Metric(name="totalUsers"),
        ],
    )
    resp = client.run_report(req)

    rows = []
    for r in resp.rows:
        rows.append({
            "channel_group": r.dimension_values[0].value,
            "sessions": int(r.metric_values[0].value),
            "total_users": int(r.metric_values[1].value),
        })

    return pd.DataFrame(rows).sort_values("sessions", ascending=False)


# =========================
# Fetch GSC
# =========================
def fetch_gsc(creds, site_url: str, start_date: str, end_date: str) -> pd.DataFrame:
    svc = build("searchconsole", "v1", credentials=creds)
    body = {
        "startDate": start_date,
        "endDate": end_date,
        "dimensions": ["query"],
        "rowLimit": 50,
    }
    res = svc.searchanalytics().query(siteUrl=site_url, body=body).execute()

    rows = []
    for r in res.get("rows", []):
        rows.append({
            "query": r["keys"][0],
            "clicks": r.get("clicks", 0),
            "impressions": r.get("impressions", 0),
            "ctr": r.get("ctr", 0),
            "position": r.get("position", 0),
        })

    return pd.DataFrame(rows).sort_values(["clicks", "impressions"], ascending=False)


# =========================
# WoW helper
# =========================
def add_wow(current: pd.DataFrame, previous: pd.DataFrame, key: str, metrics: list[str]) -> pd.DataFrame:
    cur = current.copy()
    prev = previous.copy()

    prev = prev.rename(columns={m: f"{m}_prev" for m in metrics})
    df = cur.merge(prev[[key] + [f"{m}_prev" for m in metrics]], on=key, how="left")

    for m in metrics:
        df[f"{m}_delta"] = df[m] - df[f"{m}_prev"].fillna(0)
        df[f"{m}_pct"] = np.where(
            df[f"{m}_prev"].fillna(0) == 0,
            np.nan,
            df[f"{m}_delta"] / df[f"{m}_prev"]
        )

    return df


# =========================
# Markdown helper
# =========================
def to_md_table(df: pd.DataFrame, max_rows=20) -> str:
    if df is None or df.empty:
        return "_No data_"

    view = df.head(max_rows).copy()

    # %表示を整形
    for c in view.columns:
        if c.endswith("_pct"):
            view[c] = view[c].apply(lambda x: "—" if pd.isna(x) else f"{x*100:.1f}%")

    return view.to_markdown(index=False)


# =========================
# Main
# =========================
def main():
    start = os.environ["START_DATE"]
    end = os.environ["END_DATE"]
    prev_start = os.environ["PREV_START_DATE"]
    prev_end = os.environ["PREV_END_DATE"]

    site_url = os.environ["GSC_SITE_URL"]
    prop = os.environ["GA4_PROPERTY_ID"]

    creds = get_creds()

    # 今週
    gsc_df = fetch_gsc(creds, site_url, start, end)
    ga4_df = fetch_ga4(creds, prop, start, end)

    # 前週
    gsc_prev_df = fetch_gsc(creds, site_url, prev_start, prev_end)
    ga4_prev_df = fetch_ga4(creds, prop, prev_start, prev_end)

    # WoW
    gsc_wow = add_wow(
        gsc_df, gsc_prev_df,
        key="query",
        metrics=["clicks", "impressions", "ctr", "position"]
    )

    ga4_wow = add_wow(
        ga4_df, ga4_prev_df,
        key="channel_group",
        metrics=["sessions", "total_users"]
    )

    out_dir = "reports/weekly"
    img_dir = f"{out_dir}/images"
    os.makedirs(img_dir, exist_ok=True)

    # CSV（検算・再利用用）
    gsc_wow.to_csv(f"{out_dir}/gsc_top_queries_wow.csv", index=False)
    ga4_wow.to_csv(f"{out_dir}/ga4_channels_wow.csv", index=False)

    # グラフ（今週クリック上位）
    plot_top_queries(gsc_df, f"{img_dir}/top_queries.png")

    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    md = f"""# Weekly SEO Report

- Current: **{start} → {end}**
- Previous: **{prev_start} → {prev_end}**
- Generated: {now}

## Google Search Console – Top Queries (WoW)
{to_md_table(
    gsc_wow[
        ["query", "clicks", "clicks_prev", "clicks_delta", "clicks_pct",
         "impressions", "position"]
    ],
    20
)}

## Google Analytics (GA4) – Sessions by Channel (WoW)
{to_md_table(
    ga4_wow[
        ["channel_group", "sessions", "sessions_prev", "sessions_delta", "sessions_pct"]
    ],
    20
)}

## Visuals
![Top Queries](images/top_queries.png)
"""

    with open(f"{out_dir}/README.md", "w", encoding="utf-8") as f:
        f.write(md)

    print("✅ Weekly WoW report generated")


if __name__ == "__main__":
    main()
