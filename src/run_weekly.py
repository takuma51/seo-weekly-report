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
        metrics=[Metric(name="sessions"), Metric(name="totalUsers")],
    )
    resp = client.run_report(req)

    rows = []
    for r in resp.rows:
        rows.append({
            "channel_group": r.dimension_values[0].value,
            "sessions": int(r.metric_values[0].value),
            "total_users": int(r.metric_values[1].value),
        })
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    return df.sort_values("sessions", ascending=False)


# =========================
# Fetch GSC
# =========================
def fetch_gsc(creds, site_url: str, start_date: str, end_date: str) -> pd.DataFrame:
    svc = build("searchconsole", "v1", credentials=creds)
    body = {
        "startDate": start_date,
        "endDate": end_date,
        "dimensions": ["query"],
        "rowLimit": 250,  # 50だとWoW比較が薄くなるので増やすのがおすすめ
    }
    res = svc.searchanalytics().query(siteUrl=site_url, body=body).execute()

    rows = []
    for r in res.get("rows", []):
        rows.append({
            "query": r["keys"][0],
            "clicks": float(r.get("clicks", 0)),
            "impressions": float(r.get("impressions", 0)),
            "ctr": float(r.get("ctr", 0)),         # 0-1
            "position": float(r.get("position", 0))
        })

    df = pd.DataFrame(rows)
    if df.empty:
        return df
    return df.sort_values(["clicks", "impressions"], ascending=False)


# =========================
# WoW helper
# =========================
def add_wow(current: pd.DataFrame, previous: pd.DataFrame, key: str, metrics: list[str]) -> pd.DataFrame:
    cur = current.copy()
    prev = previous.copy()

    if cur is None or cur.empty:
        return cur

    if prev is None:
        prev = pd.DataFrame(columns=[key] + metrics)

    prev = prev.rename(columns={m: f"{m}_prev" for m in metrics})

    keep_cols = [key] + [f"{m}_prev" for m in metrics if f"{m}_prev" in prev.columns]
    df = cur.merge(prev[keep_cols], on=key, how="left")

    for m in metrics:
        prev_col = f"{m}_prev"
        if prev_col not in df.columns:
            df[prev_col] = np.nan

        df[f"{m}_delta"] = df[m] - df[prev_col].fillna(0)

        # pct: prev==0 は NaN（"—" 表示）
        denom = df[prev_col].fillna(0)
        df[f"{m}_pct"] = np.where(denom == 0, np.nan, df[f"{m}_delta"] / denom)

    return df


# =========================
# Formatting helpers
# =========================
def fmt_pct(x) -> str:
    return "—" if pd.isna(x) else f"{x*100:.1f}%"

def fmt_int(x) -> str:
    try:
        return f"{int(round(float(x))):,}"
    except Exception:
        return "0"

def to_md_table(df: pd.DataFrame, max_rows=20) -> str:
    if df is None or df.empty:
        return "_No data_"

    view = df.head(max_rows).copy()

    for c in view.columns:
        if c.endswith("_pct"):
            view[c] = view[c].apply(fmt_pct)
        if c in ["clicks", "clicks_prev", "clicks_delta", "impressions", "sessions", "sessions_prev", "sessions_delta", "total_users"]:
            view[c] = view[c].apply(lambda v: fmt_int(v))

    return view.to_markdown(index=False)


# =========================
# Executive summary & actions (rule-based)
# =========================
def build_exec_summary(gsc_wow: pd.DataFrame, ga4_wow: pd.DataFrame) -> tuple[str, list[str]]:
    lines = []
    actions = []

    # --- GSC overall clicks WoW ---
    total_clicks = float(gsc_wow["clicks"].fillna(0).sum()) if gsc_wow is not None and not gsc_wow.empty else 0.0
    total_prev = float(gsc_wow["clicks_prev"].fillna(0).sum()) if gsc_wow is not None and not gsc_wow.empty else 0.0
    if total_prev > 0:
        wow_pct = (total_clicks - total_prev) / total_prev
        lines.append(f"Overall search clicks changed by {wow_pct*100:.1f}% week over week.")
    else:
        lines.append("Overall search clicks were generated for this week (no prior-week baseline).")

    # --- Top gain / loss query by clicks delta ---
    if gsc_wow is not None and not gsc_wow.empty and "clicks_delta" in gsc_wow.columns:
        tmp = gsc_wow.copy()
        tmp["clicks_delta"] = tmp["clicks_delta"].fillna(0)

        top_gain = tmp.sort_values("clicks_delta", ascending=False).head(1)
        top_loss = tmp.sort_values("clicks_delta", ascending=True).head(1)

        if not top_gain.empty and float(top_gain.iloc[0]["clicks_delta"]) > 0:
            q = top_gain.iloc[0]["query"]
            d = int(round(float(top_gain.iloc[0]["clicks_delta"])))
            lines.append(f'The top growing query was "{q}" (+{d} clicks).')

        if not top_loss.empty and float(top_loss.iloc[0]["clicks_delta"]) < 0:
            q = top_loss.iloc[0]["query"]
            d = int(round(float(top_loss.iloc[0]["clicks_delta"])))
            lines.append(f'The largest decline was observed for "{q}" ({d} clicks).')

        # Actions from CTR / position movement (simple heuristics)
        # If many queries have negative clicks delta -> investigate
        neg_ratio = (tmp["clicks_delta"] < 0).mean()
        if neg_ratio >= 0.6:
            actions.append("Investigate queries with the largest WoW click drops and validate ranking/CTR changes (GSC: Queries + Pages).")

        # If average position got worse noticeably (higher number is worse)
        if "position" in tmp.columns and "position_prev" in tmp.columns:
            pos_cur = tmp["position"].replace([np.inf, -np.inf], np.nan).dropna()
            pos_prev = tmp["position_prev"].replace([np.inf, -np.inf], np.nan).dropna()
            if len(pos_cur) and len(pos_prev):
                avg_pos_cur = float(pos_cur.mean())
                avg_pos_prev = float(pos_prev.mean())
                if avg_pos_cur - avg_pos_prev > 0.5:
                    actions.append("Average position worsened WoW—review pages losing rankings and check for intent mismatch or internal linking gaps.")

    # --- GA4 channel callout ---
    if ga4_wow is not None and not ga4_wow.empty:
        top_channel = ga4_wow.sort_values("sessions", ascending=False).head(1)
        if not top_channel.empty:
            ch = top_channel.iloc[0]["channel_group"]
            lines.append(f"{ch} was the strongest traffic channel this week.")

        # Organic Search sessions drop -> action
        org = ga4_wow[ga4_wow["channel_group"] == "Organic Search"].copy()
        if not org.empty and "sessions_prev" in org.columns:
            s = float(org.iloc[0]["sessions"])
            sp = float(org.iloc[0]["sessions_prev"]) if not pd.isna(org.iloc[0]["sessions_prev"]) else 0.0
            if sp > 0:
                pct = (s - sp) / sp
                if pct <= -0.10:
                    actions.append("Organic Search sessions dropped WoW—cross-check GSC clicks/impressions vs GA4 landing pages to identify the root cause.")
                elif pct >= 0.10:
                    actions.append("Organic Search sessions grew WoW—double down on the top winning pages/queries (update content, strengthen internal links).")

    # Fallback actions
    if not actions:
        actions = [
            "Review top gaining/declining queries and map them to landing pages for quick wins.",
            "Optimize titles/meta descriptions for queries with high impressions but low CTR.",
            "Add internal links to pages that rank but are not converting clicks efficiently.",
        ]

    summary = " ".join(lines)
    return summary, actions[:5]


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

    # Current week
    gsc_df = fetch_gsc(creds, site_url, start, end)
    ga4_df = fetch_ga4(creds, prop, start, end)

    # Previous week
    gsc_prev_df = fetch_gsc(creds, site_url, prev_start, prev_end)
    ga4_prev_df = fetch_ga4(creds, prop, prev_start, prev_end)

    # WoW tables
    gsc_wow = add_wow(gsc_df, gsc_prev_df, key="query", metrics=["clicks", "impressions", "ctr", "position"])
    ga4_wow = add_wow(ga4_df, ga4_prev_df, key="channel_group", metrics=["sessions", "total_users"])

    # Executive Summary + Next Actions (English)
    exec_summary, next_actions = build_exec_summary(gsc_wow, ga4_wow)

    out_dir = "reports/weekly"
    img_dir = f"{out_dir}/images"
    os.makedirs(img_dir, exist_ok=True)

    # Save CSVs
    if gsc_df is not None and not gsc_df.empty:
        gsc_df.to_csv(f"{out_dir}/gsc_top_queries.csv", index=False)
    if ga4_df is not None and not ga4_df.empty:
        ga4_df.to_csv(f"{out_dir}/ga4_channels.csv", index=False)

    if gsc_wow is not None and not gsc_wow.empty:
        gsc_wow.to_csv(f"{out_dir}/gsc_top_queries_wow.csv", index=False)
    if ga4_wow is not None and not ga4_wow.empty:
        ga4_wow.to_csv(f"{out_dir}/ga4_channels_wow.csv", index=False)

    # Plot (current week top queries)
    if gsc_df is not None and not gsc_df.empty:
        plot_top_queries(gsc_df, f"{img_dir}/top_queries.png")

    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")

    md = f"""# Weekly SEO Report

## Executive Summary

{exec_summary}

- Current: **{start} → {end}**
- Previous: **{prev_start} → {prev_end}**
- Generated: {now}

## Google Search Console – Top Queries (WoW)
{to_md_table(
    gsc_wow[
        ["query", "clicks", "clicks_prev", "clicks_delta", "clicks_pct",
         "impressions", "position"]
    ] if gsc_wow is not None and not gsc_wow.empty else gsc_wow,
    20
)}

## Google Analytics (GA4) – Sessions by Channel (WoW)
{to_md_table(
    ga4_wow[
        ["channel_group", "sessions", "sessions_prev", "sessions_delta", "sessions_pct"]
    ] if ga4_wow is not None and not ga4_wow.empty else ga4_wow,
    20
)}

## Visuals
![Top Queries](images/top_queries.png)

## Notes / Next Actions
{chr(10).join([f"- {a}" for a in next_actions])}
"""

    with open(f"{out_dir}/README.md", "w", encoding="utf-8") as f:
        f.write(md)

    print("✅ Weekly report generated:", f"{out_dir}/README.md")


if __name__ == "__main__":
    main()
