cmake_minimum_required(VERSION 3.10)
project(BPlusTreeOpti)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Define block size (can be changed at compile time)
add_definitions(-DBLOCK_SIZE_BYTES=4096)

# Add include directory
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/trees)

# Benchmark executable
add_executable(BPlusTreeOpti
        BTreeBenchmark.cpp
        trees/Tuple.hpp
        trees/Types.hpp
        trees/LeafNode.hpp
        trees/InternalNode.hpp
        trees/LeafNodeLSM.hpp
        trees/PageLayout.hpp
        trees/BufferPool.hpp
        trees/Database.hpp
        trees/BaseFile.hpp
)

# Test executable
add_executable(TreeTest
        test_trees.cpp
)

# Add optional secondary index test program if desired
# add_executable(btree_secondary_index secondary_index.cpp)

# Find optional Python for plotting
find_package(Python3 COMPONENTS Interpreter)
if(Python3_FOUND)
    add_custom_target(plot
            COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/plot_results.py
            DEPENDS BTreeBenchmark.cpp
            WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
            COMMENT "Plotting benchmark results"
    )
    message(STATUS "Python found: ${Python3_EXECUTABLE}. 'plot' target available.")
else()
    message(STATUS "Python not found. Install Python with matplotlib and pandas to enable plotting.")
endif()

# Testing and benchmarking
enable_testing()
add_test(NAME BenchmarkTest COMMAND btree_benchmark 10000)