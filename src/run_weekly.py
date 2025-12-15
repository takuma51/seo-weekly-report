import os, json
from datetime import datetime
import pandas as pd

from google.oauth2 import service_account
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import DateRange, Dimension, Metric, RunReportRequest

from googleapiclient.discovery import build


SCOPES = [
    "https://www.googleapis.com/auth/webmasters.readonly",
    "https://www.googleapis.com/auth/analytics.readonly",
]


def get_creds():
    sa = json.loads(os.environ["GOOGLE_SA_JSON"])
    return service_account.Credentials.from_service_account_info(sa, scopes=SCOPES)


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
    df = pd.DataFrame(rows).sort_values("sessions", ascending=False)
    return df


def fetch_gsc(creds, site_url: str, start_date: str, end_date: str) -> pd.DataFrame:
    # Search Console API: webmasters v3
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
    df = pd.DataFrame(rows).sort_values(["clicks", "impressions"], ascending=False)
    return df


def to_md_table(df: pd.DataFrame, max_rows=20) -> str:
    if df is None or df.empty:
        return "_No data_"
    return df.head(max_rows).to_markdown(index=False)


def main():
    start = os.environ["START_DATE"]
    end = os.environ["END_DATE"]
    site_url = os.environ["GSC_SITE_URL"]
    prop = os.environ["GA4_PROPERTY_ID"]

    creds = get_creds()

    gsc_df = fetch_gsc(creds, site_url, start, end)
    ga4_df = fetch_ga4(creds, prop, start, end)

    out_dir = "reports/weekly"
    os.makedirs(out_dir, exist_ok=True)

    # 生データも保存（あとで加工しやすい）
    gsc_df.to_csv(f"{out_dir}/gsc_top_queries.csv", index=False)
    ga4_df.to_csv(f"{out_dir}/ga4_channels.csv", index=False)

    # レポート（Markdown）
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    md = f"""# Weekly SEO Report

- Range: **{start} → {end}**
- Generated: {now}

## GSC: Top queries (by clicks)
{to_md_table(gsc_df, 20)}

## GA4: Sessions by channel group
{to_md_table(ga4_df, 20)}
"""
    with open(f"{out_dir}/README.md", "w", encoding="utf-8") as f:
        f.write(md)

    print("✅ Report generated:", f"{out_dir}/README.md")


if __name__ == "__main__":
    main()
