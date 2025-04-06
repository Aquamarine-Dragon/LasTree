#include <iostream>
#include <iomanip>
#include <vector>
#include <random>
#include <chrono>
#include <string>
#include <algorithm>
#include <fstream>
#include <memory>
#include <cmath>

#include "SimpleBPlusTree.hpp"
#include "OptimizedBTree.hpp"
#include "MemoryBlockManager.hpp"

// Enable verbose logging
bool VERBOSE_LOGGING = false;

// Progress indicator
class ProgressIndicator {
public:
    ProgressIndicator(int total, const std::string& description, int report_interval = 10)
        : total(total), current(0), description(description), report_interval(report_interval) {
        start_time = std::chrono::high_resolution_clock::now();
        if (VERBOSE_LOGGING) {
            std::cout << "Starting " << description << " with " << total << " items..." << std::endl;
        }
    }

    void update(int progress = 1) {
        current += progress;

        // Report progress at intervals
        if (current % report_interval == 0 || current == total) {
            auto now = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(now - start_time).count();

            if (VERBOSE_LOGGING) {
                double percent = (current * 100.0) / total;
                std::cout << "\r" << description << ": " << current << "/" << total
                          << " (" << std::fixed << std::setprecision(1) << percent << "%) - "
                          << duration << "ms";

                // Add estimated time remaining
                if (current > 0 && current < total) {
                    double items_per_ms = current / (double)duration;
                    int estimated_remaining = (total - current) / items_per_ms;
                    std::cout << " - ETA: " << estimated_remaining << "ms";
                }

                std::cout << std::flush;

                // Move to next line when complete
                if (current == total) {
                    std::cout << std::endl;
                }
            }
        }
    }

    int current;
    int total;
    std::string description;
    int report_interval;
    std::chrono::time_point<std::chrono::high_resolution_clock> start_time;
};

// Data generation utilities
class DataGenerator {
public:
    // Generate integers with varying degrees of sortedness
    template<typename T = int>
    static std::vector<T> generateData(int size, double sortedness, int seed = 42) {
        std::vector<T> data(size);

        if (VERBOSE_LOGGING) {
            std::cout << "Generating " << size << " elements with "
                      << (sortedness * 100) << "% sortedness..." << std::endl;
        }

        // Generate sorted data
        for (int i = 0; i < size; i++) {
            data[i] = static_cast<T>(i);
        }

        // If fully sorted, return as is
        if (sortedness >= 0.999) {
            return data;
        }

        // If fully random, shuffle everything
        if (sortedness <= 0.001) {
            std::mt19937 gen(seed);
            std::shuffle(data.begin(), data.end(), gen);
            return data;
        }

        // For partially sorted data:
        // 1. Keep 'sortedness' fraction of elements in their sorted positions
        // 2. Randomly shuffle the rest

        int preserveCount = static_cast<int>(size * sortedness);
        int shuffleCount = size - preserveCount;

        std::mt19937 gen(seed);

        // Randomly select positions to shuffle
        std::vector<int> positions(size);
        for (int i = 0; i < size; i++) {
            positions[i] = i;
        }

        std::shuffle(positions.begin(), positions.end(), gen);
        positions.resize(shuffleCount);
        std::sort(positions.begin(), positions.end());

        // Extract values to shuffle
        std::vector<T> valuesToShuffle;
        for (int pos : positions) {
            valuesToShuffle.push_back(data[pos]);
        }

        // Shuffle these values
        std::shuffle(valuesToShuffle.begin(), valuesToShuffle.end(), gen);

        // Put shuffled values back
        for (size_t i = 0; i < positions.size(); i++) {
            data[positions[i]] = valuesToShuffle[i];
        }

        if (VERBOSE_LOGGING) {
            std::cout << "Data generation complete." << std::endl;
        }

        return data;
    }

    // Create key-value pairs
    template<typename K, typename V = std::string>
    static std::vector<std::pair<K, V>> createPairs(const std::vector<K>& keys) {
        std::vector<std::pair<K, V>> pairs;
        pairs.reserve(keys.size());

        for (const auto& key : keys) {
            // Convert key to string for value
            pairs.emplace_back(key, "value-" + std::to_string(key));
        }

        return pairs;
    }
};

// Performance metrics
struct PerformanceMetrics {
    double insertTime = 0;  // in milliseconds
    double searchTime = 0;  // in milliseconds
    double memoryUsage = 0; // in bytes (if available)
    size_t nodeCount = 0;   // number of nodes allocated
    size_t totalInserts = 0; // number of inserts
    size_t fastPathHits = 0;// number of fast path insertions (for optimized tree)

    // Print metrics in a formatted way
    void print(const std::string& label) const {
        std::cout << std::left << std::setw(20) << label << ": "
                  << std::right << std::setw(8) << std::fixed << std::setprecision(2) << insertTime << " ms insert, "
                  << std::setw(8) << std::fixed << std::setprecision(2) << searchTime << " ms search";

        if (memoryUsage > 0) {
            std::cout << ", " << std::setw(8) << std::fixed << std::setprecision(2)
                      << (memoryUsage / 1024.0) << " KB memory";
        }

        if (nodeCount > 0) {
            std::cout << ", " << nodeCount << " nodes";
        }

        if (fastPathHits > 0) {
            std::cout << ", " << fastPathHits << " fast path hits ("
                      << std::fixed << std::setprecision(1)
                      << (fastPathHits * 100.0 / nodeCount) << "%)";
        }

        std::cout << std::endl;
    }
};

// Benchmark class
class BTreeBenchmark {
public:
    BTreeBenchmark(size_t blockSize = 4096, size_t blockCount = 1000000)
        : blockManager(blockCount) {
        std::cout << "Block size: " << blockSize << " bytes\n";
        std::cout << "Max blocks: " << blockCount << "\n";
    }

    // Run the complete benchmark
    void runBenchmark(int dataSize, const std::vector<double>& sortednessLevels) {
        std::cout << "\n===== B+Tree Performance Benchmark =====\n";
        std::cout << "Data size: " << dataSize << " elements\n\n";

        // Results containers
        std::vector<PerformanceMetrics> simpleMetrics;
        std::vector<PerformanceMetrics> optimizedMetrics;

        // Header for results
        std::cout << std::left << std::setw(15) << "Sortedness"
                  << std::setw(35) << "Simple B+Tree"
                  << std::setw(35) << "Optimized B+Tree"
                  << "Speedup" << std::endl;
        std::cout << std::string(100, '-') << std::endl;

        // Test each sortedness level
        for (double sortedness : sortednessLevels) {
            // Generate data
            auto keys = DataGenerator::generateData<int>(dataSize, sortedness);
            auto pairs = DataGenerator::createPairs<int>(keys);

            // Create search keys (random 10% of data)
            std::vector<int> searchKeys;
            std::mt19937 gen(42);
            std::sample(keys.begin(), keys.end(), std::back_inserter(searchKeys),
                       dataSize / 10, gen);

            // Format sortedness as percentage
            std::string sortednessStr = std::to_string(static_cast<int>(sortedness * 100)) + "%";

            // Reset block manager
            blockManager.reset();

            // Test simple tree
            try {
                if (VERBOSE_LOGGING) std::cout << "\nTesting Simple B+Tree with " << sortednessStr << " sortedness\n";
                auto simpleMetric = benchmarkSimpleTree(pairs, searchKeys);
                simpleMetrics.push_back(simpleMetric);
            } catch (const std::exception& e) {
                std::cerr << "ERROR in Simple B+Tree test: " << e.what() << std::endl;
                PerformanceMetrics errorMetric;
                errorMetric.insertTime = -1;
                errorMetric.searchTime = -1;
                simpleMetrics.push_back(errorMetric);
            }

            // Reset block manager
            blockManager.reset();

            // Test optimized tree
            try {
                if (VERBOSE_LOGGING) std::cout << "\nTesting Optimized B+Tree with " << sortednessStr << " sortedness\n";
                auto optimizedMetric = benchmarkOptimizedTree(pairs, searchKeys);
                optimizedMetrics.push_back(optimizedMetric);
            } catch (const std::exception& e) {
                std::cerr << "ERROR in Optimized B+Tree test: " << e.what() << std::endl;
                PerformanceMetrics errorMetric;
                errorMetric.insertTime = -1;
                errorMetric.searchTime = -1;
                optimizedMetrics.push_back(errorMetric);
                // todo: remove
                std::exit(EXIT_FAILURE);
            }

            // Calculate speedup if no errors
            if (simpleMetrics.back().insertTime > 0 && optimizedMetrics.back().insertTime > 0) {
                double insertSpeedup = simpleMetrics.back().insertTime / optimizedMetrics.back().insertTime;
                double searchSpeedup = simpleMetrics.back().searchTime / optimizedMetrics.back().searchTime;

                // Print results
                std::cout << std::left << std::setw(15) << sortednessStr;
                std::cout << std::setw(9) << std::fixed << std::setprecision(2) << simpleMetrics.back().insertTime << " ms, ";
                std::cout << std::setw(9) << std::fixed << std::setprecision(2) << simpleMetrics.back().searchTime << " ms | ";
                std::cout << std::setw(9) << std::fixed << std::setprecision(2) << optimizedMetrics.back().insertTime << " ms, ";
                std::cout << std::setw(9) << std::fixed << std::setprecision(2) << optimizedMetrics.back().searchTime << " ms | ";
                std::cout << std::fixed << std::setprecision(2) << insertSpeedup << "x insert, ";
                std::cout << std::fixed << std::setprecision(2) << searchSpeedup << "x search" << std::endl;
            } else {
                std::cout << std::left << std::setw(15) << sortednessStr;
                std::cout << "ERROR - See above for details" << std::endl;
            }
        }

        // Save results to CSV
        saveResultsToCSV(sortednessLevels, simpleMetrics, optimizedMetrics);
    }

private:
    InMemoryBlockManager<uint32_t> blockManager;

    // Benchmark the simple B+Tree
    PerformanceMetrics benchmarkSimpleTree(
            const std::vector<std::pair<int, std::string>>& data,
            const std::vector<int>& searchKeys) {

        PerformanceMetrics metrics;
        SimpleBPlusTree<int, std::string> tree(blockManager);

        // Measure insert performance
        auto startInsert = std::chrono::high_resolution_clock::now();

        // Use progress indicator for inserts
        ProgressIndicator progress(data.size(), "Simple B+Tree inserts");

        for (const auto& pair : data) {
            if (VERBOSE_LOGGING && progress.current % 100 == 0) {
                std::cout << "Inserting key: " << pair.first << std::endl;
            }
            try {
                tree.insert(pair.first, pair.second);
                progress.update();
            } catch (const std::exception& e) {
                std::cerr << "Error inserting key " << pair.first << ": " << e.what() << std::endl;
                throw; // Rethrow to handle in caller
            }
        }

        auto endInsert = std::chrono::high_resolution_clock::now();
        metrics.insertTime = std::chrono::duration<double, std::milli>(endInsert - startInsert).count();

        // Measure search performance
        auto startSearch = std::chrono::high_resolution_clock::now();

        // Use progress indicator for searches
        ProgressIndicator searchProgress(searchKeys.size(), "Simple B+Tree searches");

        int hits = 0;
        for (const auto& key : searchKeys) {
            try {
                auto result = tree.get(key);
                if (result) {
                    hits++;
                }
                searchProgress.update();
            } catch (const std::exception& e) {
                std::cerr << "Error searching for key " << key << ": " << e.what() << std::endl;
                throw; // Rethrow to handle in caller
            }
        }

        auto endSearch = std::chrono::high_resolution_clock::now();
        metrics.searchTime = std::chrono::duration<double, std::milli>(endSearch - startSearch).count();

        // Get memory usage
        metrics.nodeCount = blockManager.get_allocated_count();

        if (VERBOSE_LOGGING) {
            std::cout << "Simple B+Tree test complete: "
                      << metrics.insertTime << "ms insert, "
                      << metrics.searchTime << "ms search, "
                      << hits << "/" << searchKeys.size() << " hits"
                      << std::endl;
        }

        return metrics;
    }

    // Benchmark the optimized B+Tree
    PerformanceMetrics benchmarkOptimizedTree(
            const std::vector<std::pair<int, std::string>>& data,
            const std::vector<int>& searchKeys) {

        PerformanceMetrics metrics;
        OptimizedBTree<int, std::string> tree(blockManager);

        // Measure insert performance
        auto startInsert = std::chrono::high_resolution_clock::now();

        // Use progress indicator for inserts
        ProgressIndicator progress(data.size(), "Optimized B+Tree inserts");

        for (const auto& pair : data) {
            if (VERBOSE_LOGGING && progress.current % 100 == 0) {
                std::cout << "Inserting key: " << pair.first << std::endl;
            }
            try {
                tree.insert(pair.first, pair.second);
                progress.update();
            } catch (const std::exception& e) {
                std::cerr << "Error inserting key " << pair.first << ": " << e.what() << std::endl;
                throw; // Rethrow to handle in caller
            }
        }

        auto endInsert = std::chrono::high_resolution_clock::now();
        metrics.insertTime = std::chrono::duration<double, std::milli>(endInsert - startInsert).count();

        // Measure search performance
        auto startSearch = std::chrono::high_resolution_clock::now();

        // Use progress indicator for searches
        ProgressIndicator searchProgress(searchKeys.size(), "Optimized B+Tree searches");

        int hits = 0;
        for (const auto& key : searchKeys) {
            try {
                auto result = tree.get(key);
                if (result) {
                    hits++;
                }
                searchProgress.update();
            } catch (const std::exception& e) {
                std::cerr << "Error searching for key " << key << ": " << e.what() << std::endl;
                throw; // Rethrow to handle in caller
            }
        }

        auto endSearch = std::chrono::high_resolution_clock::now();
        metrics.searchTime = std::chrono::duration<double, std::milli>(endSearch - startSearch).count();

        // Get memory usage and fast path hits
        metrics.nodeCount = blockManager.get_allocated_count();
        metrics.fastPathHits = tree.get_fast_path_hits();
        metrics.totalInserts = data.size();

        if (VERBOSE_LOGGING) {
            std::cout << "Optimized B+Tree test complete: "
                      << metrics.insertTime << "ms insert, "
                      << metrics.searchTime << "ms search, "
                      << hits << "/" << searchKeys.size() << " hits, "
                      << metrics.fastPathHits << " fast path hits"
                      << std::endl;
        }

        return metrics;
    }

    // Save results to CSV for easy graphing
    void saveResultsToCSV(
            const std::vector<double>& sortednessLevels,
            const std::vector<PerformanceMetrics>& simpleMetrics,
            const std::vector<PerformanceMetrics>& optimizedMetrics) {

        std::ofstream file("btree_benchmark_results.csv");

        // Header
        file << "Sortedness,SimpleInsertTime,SimpleSearchTime,SimpleNodes,"
             << "OptimizedInsertTime,OptimizedSearchTime,OptimizedNodes,OptimizedFastPathHits,"
             << "OptimizedTotalInserts,InsertSpeedup,SearchSpeedup\n";

        // Data
        for (size_t i = 0; i < sortednessLevels.size(); i++) {
            // Skip rows with errors
            if (simpleMetrics[i].insertTime <= 0 || optimizedMetrics[i].insertTime <= 0) {
                continue;
            }

            double insertSpeedup = simpleMetrics[i].insertTime / optimizedMetrics[i].insertTime;
            double searchSpeedup = simpleMetrics[i].searchTime / optimizedMetrics[i].searchTime;

            file << sortednessLevels[i] << ","
                 << simpleMetrics[i].insertTime << ","
                 << simpleMetrics[i].searchTime << ","
                 << simpleMetrics[i].nodeCount << ","
                 << optimizedMetrics[i].insertTime << ","
                 << optimizedMetrics[i].searchTime << ","
                 << optimizedMetrics[i].nodeCount << ","
                 << optimizedMetrics[i].totalInserts << ","
                 << optimizedMetrics[i].fastPathHits << ","
                 << insertSpeedup << ","
                 << searchSpeedup << "\n";
        }

        file.close();
        std::cout << "\nResults saved to btree_benchmark_results.csv\n";
    }
};

int main(int argc, char* argv[]) {
    // Default values
    int dataSize = 100;

    // Parse command line arguments
    if (argc > 1) {
        dataSize = std::stoi(argv[1]);
    }

    // Define sortedness levels to test
    std::vector<double> sortednessLevels = {
        1.0,   // Fully sorted
        0.99,  // 99% sorted
        0.95,  // 95% sorted
        0.9,   // 90% sorted
        0.8,   // 80% sorted
        0.7,   // 70% sorted
        0.5,   // 50% sorted
        0.3,   // 30% sorted
        0.1,   // 10% sorted
        0.0    // Completely random
    };

    // std::vector<double> sortednessLevels = {
    //     1.0,   // Fully sorted
    //     0.0    // Completely random
    // };

    // Create and run benchmark
    BTreeBenchmark benchmark;
    benchmark.runBenchmark(dataSize, sortednessLevels);

    // Additional small verification test
    // std::cout << "\n===== Verification Test =====\n";
    //
    // // Test data
    // std::vector<std::pair<int, std::string>> testData = {
    //     {5, "five"}, {8, "eight"}, {1, "one"}, {7, "seven"},
    //     {3, "three"}, {12, "twelve"}, {9, "nine"}, {6, "six"}
    // };

    // Reset block manager
    // InMemoryBlockManager<uint32_t> verifyManager(100);

    // Test simple tree
    // {
    //     SimpleBPlusTree<int, std::string> tree(verifyManager);
    //
    //     std::cout << "Testing SimpleBPlusTree:\n";
    //     for (const auto& pair : testData) {
    //         tree.insert(pair.first, pair.second);
    //     }
    //
    //     // Test search
    //     for (int i = 1; i <= 12; i++) {
    //         auto result = tree.get(i);
    //         if (result) {
    //             std::cout << "Found " << i << ": " << *result << std::endl;
    //         } else {
    //             std::cout << "Did not find " << i << std::endl;
    //         }
    //     }
    //     std::cout << std::endl;
    // }

    // Reset block manager
    // verifyManager.reset();

    // Test optimized tree
    // {
    //     OptimizedBTree<int, std::string> tree(verifyManager);
    //
    //     std::cout << "Testing OptimizedBTree:\n";
    //     for (const auto& pair : testData) {
    //         tree.insert(pair.first, pair.second);
    //     }
    //
    //     // Test search
    //     for (int i = 1; i <= 12; i++) {
    //         auto result = tree.get(i);
    //         if (result) {
    //             std::cout << "Found " << i << ": " << *result << std::endl;
    //         } else {
    //             std::cout << "Did not find " << i << std::endl;
    //         }
    //     }
    //     std::cout << std::endl;
    // }

    return 0;
}