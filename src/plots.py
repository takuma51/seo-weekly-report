import matplotlib.pyplot as plt

# 日本語フォント（Actions 側で fonts-noto-cjk が入っている前提）
plt.rcParams["font.family"] = "Noto Sans CJK JP"

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
