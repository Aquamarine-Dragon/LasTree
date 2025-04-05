#!/usr/bin/env python3
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import sys

# Check if CSV file is provided as argument
csv_file = "build/btree_benchmark_results.csv"
if len(sys.argv) > 1:
    csv_file = sys.argv[1]

# Load the data
try:
    data = pd.read_csv(csv_file)
except FileNotFoundError:
    print(f"Error: Could not find {csv_file}")
    print("Run the benchmark first to generate results.")
    sys.exit(1)

# Convert sortedness to percentage for display
data['Sortedness_Percent'] = data['Sortedness'] * 100

# Create figure and subplots
plt.figure(figsize=(12, 10))

# Plot 1: Insert Time Comparison
plt.subplot(2, 2, 1)
plt.plot(data['Sortedness_Percent'], data['SimpleInsertTime'], 'b-o', label='Simple B+Tree')
plt.plot(data['Sortedness_Percent'], data['OptimizedInsertTime'], 'r-s', label='Optimized B+Tree')
plt.xlabel('Sortedness (%)')
plt.ylabel('Insert Time (ms)')
plt.title('Insert Performance vs. Sortedness')
plt.legend()
plt.grid(True)
plt.ylim(bottom=0)  # Start y-axis at 0

# Plot 2: Search Time Comparison
plt.subplot(2, 2, 2)
plt.plot(data['Sortedness_Percent'], data['SimpleSearchTime'], 'b-o', label='Simple B+Tree')
plt.plot(data['Sortedness_Percent'], data['OptimizedSearchTime'], 'r-s', label='Optimized B+Tree')
plt.xlabel('Sortedness (%)')
plt.ylabel('Search Time (ms)')
plt.title('Search Performance vs. Sortedness')
plt.legend()
plt.grid(True)
plt.ylim(bottom=0)  # Start y-axis at 0

# Plot 3: Speedup
plt.subplot(2, 2, 3)
plt.plot(data['Sortedness_Percent'], data['InsertSpeedup'], 'g-o', label='Insert Speedup')
plt.plot(data['Sortedness_Percent'], data['SearchSpeedup'], 'm-s', label='Search Speedup')
plt.axhline(y=1.0, color='k', linestyle='--', alpha=0.7)  # Reference line for no speedup
plt.xlabel('Sortedness (%)')
plt.ylabel('Speedup Factor')
plt.title('Performance Speedup vs. Sortedness')
plt.legend()
plt.grid(True)
plt.ylim(bottom=0)  # Start y-axis at 0

# Plot 4: Fast Path Hits
plt.subplot(2, 2, 4)
plt.plot(data['Sortedness_Percent'], data['OptimizedFastPathHits'], 'r-o')
# Add percentage labels
for i, row in data.iterrows():
    if row['OptimizedNodes'] > 0:
        hit_percent = row['OptimizedFastPathHits'] / row['OptimizedTotalInserts'] * 100
        plt.annotate(f"{hit_percent:.1f}%",
                     (row['Sortedness_Percent'], row['OptimizedFastPathHits']),
                     textcoords="offset points",
                     xytext=(0,10),
                     ha='center')
plt.xlabel('Sortedness (%)')
plt.ylabel('Fast Path Hits')
plt.title('Fast Path Usage vs. Sortedness')
plt.grid(True)
plt.ylim(bottom=0)  # Start y-axis at 0

# Add overall title
plt.suptitle('B+Tree Performance Analysis', fontsize=16)
plt.tight_layout(rect=[0, 0, 1, 0.96])  # Adjust layout to make room for suptitle

# Save figure
plt.savefig('btree_performance.png', dpi=150)
print("Generated plot: btree_performance.png")

# Show plot
plt.show()