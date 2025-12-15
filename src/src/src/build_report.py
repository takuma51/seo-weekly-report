import os
import pandas as pd

def main():
    start = os.environ["START_DATE"]
    end = os.environ["END_DATE"]

    gsc = pd.read_csv("reports/weekly/data/gsc.csv")
    ga4 = pd.read_csv("reports/weekly/data/ga4.csv")

    gsc_sum = gsc[["clicks","impressions"]].sum(numeric_only=True)
    ctr = (gsc_sum["clicks"] / gsc_sum["impressions"]) if gsc_sum["impressions"] else 0

    organic = ga4[ga4["channel_group"].str.contains("Organic", na=False)]
    organic_sessions = int(organic["sessions"].sum()) if len(organic) else 0
    users = int(ga4["total_users"].sum())

    top_queries = (gsc.groupby("query", as_index=False)["clicks"].sum()
                     .sort_values("clicks", ascending=False).head(10))
    top_pages = (gsc.groupby("page", as_index=False)["clicks"].sum()
                   .sort_values("clicks", ascending=False).head(10))

    md = []
    md.append(f"# Weekly SEO Report (Mon–Sun) [{start} → {end}]\n")
    md.append("## Summary\n")
    md.append(f"- GSC Clicks: **{int(gsc_sum['clicks'])}**\n")
    md.append(f"- GSC Impressions: **{int(gsc_sum['impressions'])}**\n")
    md.append(f"- GSC CTR: **{ctr:.2%}**\n")
    md.append(f"- GA4 Users (All): **{users}**\n")
    md.append(f"- GA4 Sessions (Organic): **{organic_sessions}**\n")

    md.append("\n## Top Queries (Clicks)\n")
    md.append(top_queries.to_markdown(index=False))
    md.append("\n\n## Top Pages (Clicks)\n")
    md.append(top_pages.to_markdown(index=False))

    out_path = f"reports/weekly/{start}_to_{end}.md"
    os.makedirs("reports/weekly", exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(md))

if __name__ == "__main__":
    main()
