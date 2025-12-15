import os, json
import pandas as pd
from google.oauth2 import service_account
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import DateRange, Dimension, Metric, RunReportRequest

SCOPES = ["https://www.googleapis.com/auth/analytics.readonly"]

def fetch_ga4(property_id: str, start_date: str, end_date: str) -> pd.DataFrame:
    creds = service_account.Credentials.from_service_account_info(
        json.loads(os.environ["GOOGLE_SA_JSON"]),
        scopes=SCOPES,
    )
    client = BetaAnalyticsDataClient(credentials=creds)

    request = RunReportRequest(
        property=f"properties/{property_id}",
        date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
        dimensions=[Dimension(name="sessionDefaultChannelGroup")],
        metrics=[Metric(name="sessions"), Metric(name="totalUsers")],
    )
    resp = client.run_report(request)

    rows = []
    for r in resp.rows:
        rows.append({
            "channel_group": r.dimension_values[0].value,
            "sessions": int(r.metric_values[0].value),
            "total_users": int(r.metric_values[1].value),
        })
    return pd.DataFrame(rows)

if __name__ == "__main__":
    prop = os.environ["GA4_PROPERTY_ID"]
    start = os.environ["START_DATE"]
    end = os.environ["END_DATE"]
    df = fetch_ga4(prop, start, end)
    out = os.environ.get("GA4_OUT", "reports/weekly/data/ga4.csv")
    os.makedirs(os.path.dirname(out), exist_ok=True)
    df.to_csv(out, index=False)
