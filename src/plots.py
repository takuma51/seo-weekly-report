import matplotlib.pyplot as plt

def plot_top_queries(df, out_path: str):
    if df is None or df.empty:
        return None

    top = df.head(10)
    plt.figure(figsize=(10, 5))
    plt.barh(top["query"], top["clicks"])
    plt.gca().invert_yaxis()
    plt.title("Top Queries by Clicks")
    plt.tight_layout()
    plt.savefig(out_path)
    plt.close()
    return out_path
