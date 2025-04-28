# LasTree

This project implements and benchmarks several B+ Tree variants optimized for different data ingestion and query patterns, including:
- **SimpleBPlusTree**: Classical B+ Tree baseline
- **OptimizedBTree**: Fast-path insertion with sorted leaf nodes
- **LoggedBTree**: Fast-path insertion with append-only leaf nodes
- **LaSTree**: LoggedBTree plus background lazy sorting for cold nodes

## Build Instructions

Make sure you have **CMake** and a **C++17-compatible compiler** (e.g., `g++`, `clang++`) installed.

```bash
# Step 1: Create a build directory
mkdir build

# Step 2: Enter the build directory
cd build

# Step 3: Generate build system files with CMake
cmake ..

# Step 4: Compile the project
make
```

## Running the Benchmark
```bash
# Step 5: Run the benchmark binary
./BPlusTreeOpti [dataSize]

# Optional argument:
# dataSize: Number of records to insert (default is 100000 if not provided)
```

### Plotting Results
```aiignore
# Step 6: Go back to the project root directory
cd ..

# Step 7: Generate plots using the provided script
python3 plot_result.py
```

The generated plots will visualize the performance of different trees across various metrics such as insertion latency, point query latency, range query latency, and leaf utilization.


