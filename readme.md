# BPlusTreeOpti

This project implements several B+ Tree variants optimized for different write/read patterns, including:
- SimpleBPlusTree
- OptimizedBTree (with sorted leaf or LSM-style leaf nodes)

## Build Instructions

Make sure you have **CMake** and a **C++17-compatible compiler** (like `g++`, `clang++`) installed.

```bash
# Step 1: Create a build directory
mkdir build

# Step 2: Enter the build directory
cd build

# Step 3: Generate build system files with CMake
cmake ..

# Step 4: Compile the project
make

# Step 5: run the benchmark binary:
./BPlusTreeOpti [dataSize]
# dataSize (optional): Number of records to insert. Default is 1000 if not provided.
