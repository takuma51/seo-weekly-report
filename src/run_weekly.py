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
            "sessions": float(r.metric_values[0].value),
            "total_users": float(r.metric_values[1].value),
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
        "rowLimit": 250,
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
    if current is None or current.empty:
        return current

    cur = current.copy()
    prev = previous.copy() if previous is not None else pd.DataFrame(columns=[key] + metrics)

    prev = prev.rename(columns={m: f"{m}_prev" for m in metrics})

    keep_cols = [key] + [f"{m}_prev" for m in metrics if f"{m}_prev" in prev.columns]
    if not keep_cols:
        keep_cols = [key]

    df = cur.merge(prev[keep_cols], on=key, how="left")

    for m in metrics:
        prev_col = f"{m}_prev"
        if prev_col not in df.columns:
            df[prev_col] = np.nan

        df[f"{m}_delta"] = df[m] - df[prev_col].fillna(0)

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

def fmt_float(x, digits=2) -> str:
    try:
        return f"{float(x):.{digits}f}"
    except Exception:
        return "0"

def safe_div(n, d):
    try:
        if d is None or float(d) == 0:
            return np.nan
        return float(n) / float(d)
    except Exception:
        return np.nan

def to_md_table(df: pd.DataFrame, max_rows=20) -> str:
    if df is None or df.empty:
        return "_No data_"

    view = df.head(max_rows).copy()

    for c in view.columns:
        if c.endswith("_pct"):
            view[c] = view[c].apply(fmt_pct)

    int_like = {
        "clicks", "clicks_prev", "clicks_delta",
        "impressions", "impressions_prev", "impressions_delta",
        "sessions", "sessions_prev", "sessions_delta",
        "total_users", "total_users_prev", "total_users_delta",
    }
    for c in view.columns:
        if c in int_like:
            view[c] = view[c].apply(fmt_int)

    for c in ["position", "position_prev", "position_delta"]:
        if c in view.columns:
            view[c] = view[c].apply(lambda v: fmt_float(v, 2))

    for c in ["ctr", "ctr_prev"]:
        if c in view.columns:
            view[c] = view[c].apply(lambda v: "—" if pd.isna(v) else f"{float(v)*100:.1f}%")

    if "ctr_delta" in view.columns:
        view["ctr_delta"] = view["ctr_delta"].apply(lambda v: "—" if pd.isna(v) else f"{float(v)*100:.1f}pt")

    return view.to_markdown(index=False)


# =========================
# Executive summary & actions (rule-based)
# =========================
def build_exec_summary(
    gsc_cur: pd.DataFrame, gsc_prev: pd.DataFrame, gsc_wow: pd.DataFrame,
    ga4_cur: pd.DataFrame, ga4_prev: pd.DataFrame, ga4_wow: pd.DataFrame
) -> tuple[str, list[str]]:

    lines: list[str] = []
    actions: list[str] = []

    cur_clicks = float(gsc_cur["clicks"].sum()) if gsc_cur is not None and not gsc_cur.empty else 0.0
    prev_clicks = float(gsc_prev["clicks"].sum()) if gsc_prev is not None and not gsc_prev.empty else 0.0

    cur_impr = float(gsc_cur["impressions"].sum()) if gsc_cur is not None and not gsc_cur.empty else 0.0
    prev_impr = float(gsc_prev["impressions"].sum()) if gsc_prev is not None and not gsc_prev.empty else 0.0

    cur_ctr = safe_div(cur_clicks, cur_impr)
    prev_ctr = safe_div(prev_clicks, prev_impr)

    def weighted_pos(df: pd.DataFrame) -> float:
        if df is None or df.empty:
            return np.nan
        w = df["impressions"].fillna(0).astype(float)
        p = df["position"].replace([np.inf, -np.inf], np.nan).astype(float)
        if w.sum() == 0:
            return float(p.mean()) if len(p.dropna()) else np.nan
        return float((p.fillna(0) * w).sum() / w.sum())

    cur_pos = weighted_pos(gsc_cur)
    prev_pos = weighted_pos(gsc_prev)

    if prev_clicks > 0:
        wow_clicks_pct = (cur_clicks - prev_clicks) / prev_clicks
        lines.append(f"Search clicks changed by {wow_clicks_pct*100:.1f}% WoW ({int(cur_clicks):,} vs {int(prev_clicks):,}).")
    else:
        lines.append(f"Search clicks for the current period were {int(cur_clicks):,} (no prior-week baseline).")

    if prev_impr > 0:
        wow_impr_pct = (cur_impr - prev_impr) / prev_impr
        lines.append(f"Impressions changed by {wow_impr_pct*100:.1f}% WoW ({int(cur_impr):,} vs {int(prev_impr):,}).")

    if not pd.isna(cur_ctr):
        if not pd.isna(prev_ctr):
            ctr_delta_pt = (cur_ctr - prev_ctr) * 100
            lines.append(f"CTR is {cur_ctr*100:.1f}% ({ctr_delta_pt:+.1f}pt WoW).")
        else:
            lines.append(f"CTR is {cur_ctr*100:.1f}%.")

    if not pd.isna(cur_pos):
        if not pd.isna(prev_pos):
            pos_delta = cur_pos - prev_pos
            lines.append(f"Avg position is {cur_pos:.2f} ({pos_delta:+.2f} WoW).")
        else:
            lines.append(f"Avg position is {cur_pos:.2f}.")

    if gsc_wow is not None and not gsc_wow.empty and "clicks_delta" in gsc_wow.columns:
        tmp = gsc_wow.copy()
        tmp["clicks_delta"] = tmp["clicks_delta"].fillna(0)

        gain = tmp.sort_values("clicks_delta", ascending=False).head(1)
        loss = tmp.sort_values("clicks_delta", ascending=True).head(1)

        if not gain.empty and float(gain.iloc[0]["clicks_delta"]) > 0:
            q = gain.iloc[0]["query"]
            d = int(round(float(gain.iloc[0]["clicks_delta"])))
            lines.append(f'Top gaining query: "{q}" (+{d} clicks).')

        if not loss.empty and float(loss.iloc[0]["clicks_delta"]) < 0:
            q = loss.iloc[0]["query"]
            d = int(round(float(loss.iloc[0]["clicks_delta"])))
            lines.append(f'Top losing query: "{q}" ({d} clicks).')

        if not pd.isna(cur_pos) and not pd.isna(prev_pos) and (cur_pos - prev_pos) > 0.30:
            actions.append("Rankings slightly weakened WoW—review pages losing positions and strengthen internal links around those topics.")

        if not pd.isna(cur_ctr) and not pd.isna(prev_ctr) and (cur_ctr - prev_ctr) < -0.005:
            actions.append("CTR decreased WoW—test title/meta updates for high-impression queries and validate SERP intent alignment.")

        big_drops = tmp[tmp["clicks_delta"] <= -3].head(5)
        if len(big_drops) > 0:
            actions.append("Investigate the largest click drops (Queries → Pages) and check indexability/canonical/internal-link changes.")

        clicks_prev_series = tmp["clicks_prev"] if "clicks_prev" in tmp.columns else pd.Series([0]*len(tmp))
        new_winners = tmp[(clicks_prev_series.fillna(0) == 0) & (tmp["clicks"] > 0)].head(5)
        if len(new_winners) > 0:
            actions.append("New queries appeared WoW—map them to landing pages and expand content clusters to capture more long-tail demand.")

    cur_sessions = float(ga4_cur["sessions"].sum()) if ga4_cur is not None and not ga4_cur.empty else 0.0
    prev_sessions = float(ga4_prev["sessions"].sum()) if ga4_prev is not None and not ga4_prev.empty else 0.0

    if prev_sessions > 0:
        wow_sessions_pct = (cur_sessions - prev_sessions) / prev_sessions
        lines.append(f"GA4 sessions changed by {wow_sessions_pct*100:.1f}% WoW ({int(cur_sessions):,} vs {int(prev_sessions):,}).")
    elif cur_sessions > 0:
        lines.append(f"GA4 sessions for the current period were {int(cur_sessions):,} (no prior-week baseline).")

    if ga4_wow is not None and not ga4_wow.empty:
        top_channel = ga4_wow.sort_values("sessions", ascending=False).head(1)
        if not top_channel.empty:
            ch = top_channel.iloc[0]["channel_group"]
            lines.append(f"Top traffic channel: {ch}.")

        org = ga4_wow[ga4_wow["channel_group"] == "Organic Search"].copy()
        if not org.empty and "sessions_prev" in org.columns:
            s = float(org.iloc[0]["sessions"])
            sp = float(org.iloc[0]["sessions_prev"]) if not pd.isna(org.iloc[0]["sessions_prev"]) else 0.0
            if sp > 0:
                pct = (s - sp) / sp
                if pct <= -0.10:
                    actions.append("Organic Search sessions dropped WoW—compare GSC clicks vs GA4 landing pages to locate the main decline pages.")
                elif pct >= 0.10:
                    actions.append("Organic Search sessions grew WoW—double down on winning pages (refresh content + add internal links).")

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

    gsc_df = fetch_gsc(creds, site_url, start, end)
    ga4_df = fetch_ga4(creds, prop, start, end)

    gsc_prev_df = fetch_gsc(creds, site_url, prev_start, prev_end)
    ga4_prev_df = fetch_ga4(creds, prop, prev_start, prev_end)

    gsc_wow = add_wow(gsc_df, gsc_prev_df, key="query", metrics=["clicks", "impressions", "ctr", "position"])
    ga4_wow = add_wow(ga4_df, ga4_prev_df, key="channel_group", metrics=["sessions", "total_users"])

    exec_summary, next_actions = build_exec_summary(
        gsc_df, gsc_prev_df, gsc_wow,
        ga4_df, ga4_prev_df, ga4_wow
    )

    out_dir = "reports/weekly"
    img_dir = f"{out_dir}/images"
    os.makedirs(img_dir, exist_ok=True)

    if gsc_df is not None and not gsc_df.empty:
        gsc_df.to_csv(f"{out_dir}/gsc_top_queries.csv", index=False)
    if ga4_df is not None and not ga4_df.empty:
        ga4_df.to_csv(f"{out_dir}/ga4_channels.csv", index=False)

    if gsc_wow is not None and not gsc_wow.empty:
        gsc_wow.to_csv(f"{out_dir}/gsc_top_queries_wow.csv", index=False)
    if ga4_wow is not None and not ga4_wow.empty:
        ga4_wow.to_csv(f"{out_dir}/ga4_channels_wow.csv", index=False)

    if gsc_df is not None and not gsc_df.empty:
        plot_top_queries(gsc_df, f"{img_dir}/top_queries.png")

    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")

    gsc_cols = ["query", "clicks", "clicks_prev", "clicks_delta", "clicks_pct", "impressions", "ctr", "position"]
    ga4_cols = ["channel_group", "sessions", "sessions_prev", "sessions_delta", "sessions_pct", "total_users"]

    gsc_view = gsc_wow[gsc_cols] if gsc_wow is not None and not gsc_wow.empty else gsc_wow
    ga4_view = ga4_wow[ga4_cols] if ga4_wow is not None and not ga4_wow.empty else ga4_wow

    # ✅ Pagesで確実にMarkdown→HTML化させるため front matter を入れる
    md = f"""---
layout: default
title: Weekly SEO Report
---

# Weekly SEO Report

## Executive Summary

{exec_summary}

- Current: **{start} → {end}**
- Previous: **{prev_start} → {prev_end}**
- Generated: {now}

## Google Search Console – Top Queries (WoW)
{to_md_table(gsc_view, 20)}

## Google Analytics (GA4) – Sessions by Channel (WoW)
{to_md_table(ga4_view, 20)}

## Visuals
![Top Queries](images/top_queries.png)

## Notes / Next Actions
{chr(10).join([f"- {a}" for a in next_actions])}
"""

    # ✅ README.md ではなく index.md に出力する
    with open(f"{out_dir}/index.md", "w", encoding="utf-8") as f:
        f.write(md)

    print("✅ Weekly report generated:", f"{out_dir}/index.md")


if __name__ == "__main__":
    main()
