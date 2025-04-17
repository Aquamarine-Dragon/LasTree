import pandas as pd
import matplotlib.pyplot as plt

# Load CSV data
csv_file = "build/btree_benchmark.csv"  # Set default path
data = pd.read_csv(csv_file)

# Add derived column for display
data['Sortedness_Percent'] = data['Sortedness'] * 100

# Plot metric vs. sortedness for each read ratio and tree type
def plot_metric_by_read_ratio(data, metric, ylabel, title, filename):
    read_ratios = sorted(data['ReadRatio'].unique())
    tree_types = sorted(data['TreeType'].unique())

    plt.figure(figsize=(12, 4 * len(read_ratios)))

    for i, ratio in enumerate(read_ratios, 1):
        plt.subplot(len(read_ratios), 1, i)
        for tree in tree_types:
            subset = data[(data['ReadRatio'] == ratio) & (data['TreeType'] == tree)]
            plt.plot(subset['Sortedness_Percent'], subset[metric], marker='o', label=tree)
        plt.title(f"{title} (ReadRatio={ratio:.1f})")
        plt.xlabel('Sortedness (%)')
        plt.ylabel(ylabel)
        plt.grid(True)
        plt.legend()

    plt.tight_layout()
    plt.savefig(filename)
    print(f"Saved plot to: {filename}")
    plt.show()

# Generate plots
plot_metric_by_read_ratio(data, 'InsertTime', 'Insert Time (ms)', 'Insert Performance', 'insert_performance.png')
plot_metric_by_read_ratio(data, 'SearchTime', 'Search Time (ms)', 'Search Performance', 'search_performance.png')
plot_metric_by_read_ratio(data, 'NodeCount', 'Node Count', 'Node Usage', 'node_count.png')

if 'FastPathHits' in data.columns:
    plot_metric_by_read_ratio(data, 'FastPathHits', 'Fast Path Hits', 'Fast Path Usage', 'fast_path_hits.png')
