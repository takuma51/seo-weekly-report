import matplotlib.pyplot as plt
from pathlib import Path

def plot_top_queries(df, out_path):
    if df.empty:
        return

    Path(out_path).parent.mkdir(parents=True, exist_ok=True)

    top = df.head(10)
    plt.figure(figsize=(10, 5))
    plt.barh(top["query"], top["clicks"])
    plt.gca().invert_yaxis()
    plt.title("Top Queries by Clicks")
    plt.tight_layout()
    plt.savefig(out_path)
    plt.close()
