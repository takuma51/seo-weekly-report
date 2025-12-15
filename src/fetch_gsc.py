import os, json
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build

SCOPES = ["https://www.googleapis.com/auth/webmasters.readonly"]

def fetch_gsc(site_url: str, start_date: str, end_date: str) -> pd.DataFrame:
    creds = service_account.Credentials.from_service_account_info(
        json.loads(os.environ["GOOGLE_SA_JSON"]),
        scopes=SCOPES,
    )
    service = build("searchconsole", "v1", credentials=creds)

    request = {
        "startDate": start_date,
        "endDate": end_date,
        "dimensions": ["query", "page"],
        "rowLimit": 25000,
    }
    resp = service.searchanalytics().query(siteUrl=site_url, body=request).execute()
    rows = resp.get("rows", [])

    data = []
    for r in rows:
        keys = r.get("keys", ["", ""])
        data.append({
            "query": keys[0],
            "page": keys[1],
            "clicks": r.get("clicks", 0),
            "impressions": r.get("impressions", 0),
            "ctr": r.get("ctr", 0),
            "position": r.get("position", 0),
        })
    return pd.DataFrame(data)

if __name__ == "__main__":
    site_url = os.environ["GSC_SITE_URL"]
    start = os.environ["START_DATE"]
    end = os.environ["END_DATE"]
    df = fetch_gsc(site_url, start, end)
    out = os.environ.get("GSC_OUT", "reports/weekly/data/gsc.csv")
    os.makedirs(os.path.dirname(out), exist_ok=True)
    df.to_csv(out, index=False)
