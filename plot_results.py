import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

# Load CSV
plt.rcParams.update({'font.size': 14})
csv_file = "build/btree_benchmark_cleaned.csv"
data = pd.read_csv(csv_file)

# Add percentage column
data['Sortedness_Percent'] = data['Sortedness'] * 100

colors = {
    "LoggedBTree": '#E0E26E',
    "OptimizedBTree": '#74B488',
    "SimpleBTree": '#6AA4B4',
    "LasTree": '#B494AB',
}

def plot_metric_by_read_ratio(data, metric, ylabel, title, filename, bar_unit_scale=1.0):
    read_ratios = sorted(data['ReadRatio'].unique())
    tree_types = sorted(data['TreeType'].unique())
    sortedness = sorted(data['Sortedness_Percent'].unique())

    x = np.arange(len(sortedness))
    width = 0.15

    plt.figure(figsize=(14, 5 * len(read_ratios)))

    for i, ratio in enumerate(read_ratios, 1):
        ax = plt.subplot(len(read_ratios), 1, i)
        for j, tree in enumerate(tree_types):
            subset = data[(data['ReadRatio'] == ratio) & (data['TreeType'] == tree)].sort_values("Sortedness_Percent")
            bar_vals = subset[metric].values / bar_unit_scale
            bars = plt.bar(
                x + j * width,
                bar_vals,
                width,
                label=tree,
                edgecolor='black',
                color=colors.get(tree, 'gray')
            )

        ax.set_xticks(x + width * (len(tree_types) - 1) / 2)
        ax.set_xticklabels([f"{int(s)}" for s in sortedness])
        ax.set_title(f"{title}")
        ax.set_xlabel('% Sortedness')
        ax.set_ylabel(ylabel)
        ax.legend(loc='center left', bbox_to_anchor=(1.0, 0.5), frameon=False)
        ax.grid(True, axis='y')

    plt.tight_layout()
    plt.savefig(filename)
    plt.savefig(filename + ".pdf", bbox_inches='tight')
    print(f"Saved plot to: {filename}")
    # plt.show()

# Adjust FastPathHits to percentage (given 100000 inserts)
data['FastPathHits'] = data['FastPathHits'] / 1000.0

# Plot
plot_metric_by_read_ratio(data, 'InsertTime', 'Insert Time (ms)', 'Insert Performance', 'insert_performance.png')
plot_metric_by_read_ratio(data, 'PointLookupTime', 'Search Time (ms)', 'Point Query Performance', 'point_performance.png')
plot_metric_by_read_ratio(data, 'RangeQueryTime', 'Search Time (ms)', 'Range Query Performance', 'range_performance.png')
plot_metric_by_read_ratio(data, 'FastPathHits', 'Fast Path Hit (%)', 'Fast Path Usage', 'fast_path_hits.png', bar_unit_scale=1.0)
plot_metric_by_read_ratio(data, 'MixedWorkloadTime', 'Mixed Workload Time (ms)', 'Mixed Workload Performance (70% insert, 30% lookup)', 'mixed_workload.png')
plot_metric_by_read_ratio(data, 'SortedLeafSearch', 'count', 'Search Count on Sorted Leaf', 'sorted_leaf_search_count.png')

# Plot Leaf Count
plot_metric_by_read_ratio(
    data,
    'LeafCount',
    'Leaf Count',
    'Leaf Node Usage',
    'styled_leaf_count.png'
)

# Plot Leaf Utilization (as a line plot instead of bar)
def plot_utilization(data, filename):
    read_ratios = sorted(data['ReadRatio'].unique())
    tree_types = sorted(data['TreeType'].unique())
    sortedness = sorted(data['Sortedness_Percent'].unique())

    plt.figure(figsize=(14, 5 * len(read_ratios)))

    for i, ratio in enumerate(read_ratios, 1):
        ax = plt.subplot(len(read_ratios), 1, i)
        for tree in tree_types:
            subset = data[(data['ReadRatio'] == ratio) & (data['TreeType'] == tree)].sort_values("Sortedness_Percent")
            y = subset['LeafUtilization'].values
            x = subset['Sortedness_Percent'].values
            ax.plot(x, y, marker='o', label=tree, color=colors.get(tree, 'gray'))

        ax.set_ylim(0, 1)
        ax.set_xticks(sortedness)
        ax.set_title(f"Leaf Utilization")
        # ax.set_title(f"Leaf Utilization (ReadRatio={ratio:.1f})")
        ax.set_xlabel('% Sortedness')
        ax.set_ylabel('Utilization Ratio')
        ax.grid(True)
        ax.legend(loc='center left', bbox_to_anchor=(1.0, 0.5), frameon=False)

    plt.tight_layout()
    plt.savefig(filename)
    plt.savefig(filename + ".pdf", bbox_inches='tight')
    print(f"Saved plot to: {filename}")
    # plt.show()

# Rename column to match plotting function
data['LeafUtilization'] = data['NodeCount.1'] if 'NodeCount.1' in data.columns else data['LeafUtilization']
plot_utilization(data, 'leaf_utilization.png')