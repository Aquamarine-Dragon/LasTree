#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <tuple>
#include <chrono>
#include <random>
#include <optional>
#include <algorithm>

#include "Tuple.hpp"
#include "SimpleBPlusTree.hpp"
#include "OptimizedBTree.hpp"
#include "LeafNode.hpp"
#include "AppendOnlyLeafNode.hpp"
#include "LasTree.hpp"

constexpr size_t POOL_SIZE = 64;
constexpr size_t PAGE_SIZE = 4096;

using key_type = int;
using value_type = std::string;
using TupleT = db::Tuple;
using Field = db::field_t;
using db::PageId;

struct ResultRow {
    std::string tree_name;
    double sortedness;
    double read_ratio;
    double insert_time_ms;
    double search_time_ms;
    double range_query_time_ms;
    double mixed_workload_time_ms;
    size_t leaf_count;
    double leaf_utilization;
    size_t fast_path_hits;
    size_t sorted_leaf_search;
};

template<typename Tree>
std::optional<value_type> get(Tree &tree, const key_type &key) {
    return tree.get(key);
}

template<typename Tree>
std::vector<std::pair<key_type, value_type>> range_scan(Tree &tree, const key_type &start_key, const key_type &end_key) {
    return tree.range_scan(start_key, end_key);
}


void run_benchmark(size_t dataSize) {

    std::vector<double> sortedness_levels = {1.0, 0.95, 0.8, 0.5, 0.2, 0.0};
    // std::vector<double> sortedness_levels = {0.8};
    std::vector<double> read_ratios = {0.5};

    std::vector<ResultRow> results;

    for (double sortedness: sortedness_levels) {
        std::cout << "Benchmarking: sortedness=" << sortedness << "\n";

        // Generate keys
        std::vector<key_type> keys(dataSize);
        std::iota(keys.begin(), keys.end(), 0);

        if (sortedness < 1.0) {
            size_t shuffle_count = static_cast<size_t>(dataSize * (1.0 - sortedness));
            std::mt19937 rng(42);
            std::shuffle(keys.begin(), keys.begin() + shuffle_count, rng);
        }

        // Generate tuples
        db::TupleDesc td({db::type_t::INT, db::type_t::CHAR}, {"key", "val"});
        std::vector<Tuple> tuples;
        for (key_type k: keys) {
            std::vector<db::field_t> fields = {
                db::field_t(k),
                db::field_t("val-" + std::to_string(k))
            };
            tuples.emplace_back(fields, td.get_types());
        }

        // Generate range query bounds
        const size_t NUM_RANGES = 100;
        std::vector<std::pair<key_type, key_type>> range_queries;
        std::mt19937 rng(42);
        std::uniform_int_distribution<key_type> dist(0, dataSize - 1);
        for (size_t i = 0; i < NUM_RANGES; ++i) {
            key_type start = dist(rng);
            key_type end = std::min(start + 100, static_cast<key_type>(dataSize - 1));
            range_queries.push_back({start, end});
        }



        // === Benchmark 1: SimpleBPlusTree ===
        {
            const char *name = "simple.db";
            std::remove(name);
            getDatabase().add(std::make_unique<SimpleBPlusTree<key_type, 2> >(name, td, 0));
            auto &tree = db::getDatabase().get(name);
            tree.init();

            // Measure insert time
            auto t0 = std::chrono::high_resolution_clock::now();
            for (const auto &tup: tuples) {
                tree.insert(tup);
            }
            auto t1 = std::chrono::high_resolution_clock::now();
            auto insert_time = std::chrono::duration<double, std::milli>(t1 - t0).count();

            // Measure point lookup time for different read ratios
            std::vector<double> search_times;
            for (double read_ratio: read_ratios) {
                std::vector<key_type> read_keys;
                std::sample(keys.begin(), keys.end(), std::back_inserter(read_keys),
                           static_cast<size_t>(dataSize * read_ratio), std::mt19937(42));

                t0 = std::chrono::high_resolution_clock::now();
                for (key_type k: read_keys) {
                    auto val = tree.get(k);
                    if (!val.has_value()) throw std::runtime_error("Missing key in simple tree");
                }
                t1 = std::chrono::high_resolution_clock::now();
                double avg_search_time_per_op = std::chrono::duration<double, std::milli>(t1 - t0).count() / read_keys.size();
                search_times.push_back(avg_search_time_per_op);
            }

            // Measure range query time
            t0 = std::chrono::high_resolution_clock::now();
            for (const auto &[start, end]: range_queries) {
                auto results = tree.range(start, end);
                // if (results.empty() && end >= start) throw std::runtime_error("Empty range result in simple tree");
            }
            t1 = std::chrono::high_resolution_clock::now();
            auto range_query_time = std::chrono::duration<double, std::milli>(t1 - t0).count();
            auto range_query_time_per_op = range_query_time / range_queries.size();

            // construct a new tree for mixed workload
            const char *mix_name = "simple_mix.db";
            std::remove(mix_name);
            getDatabase().add(std::make_unique<SimpleBPlusTree<key_type, 2> >(mix_name, td, 0));
            auto &mix_tree = db::getDatabase().get(mix_name);
            mix_tree.init();

            // Mixed workload (70% insert, 30% lookup)
            std::vector<key_type> mixed_keys(dataSize);
            std::iota(mixed_keys.begin(), mixed_keys.end(), dataSize); // New keys starting from dataSize

            std::vector<Tuple> mixed_tuples;
            for (key_type k: mixed_keys) {
                std::vector<db::field_t> fields = {
                    db::field_t(k),
                    db::field_t("val-" + std::to_string(k))
                };
                mixed_tuples.emplace_back(fields, td.get_types());
            }

            // Sample 30% from the inserted keys as lookup keys
            std::vector<key_type> lookup_keys;
            std::sample(mixed_keys.begin(), mixed_keys.end(), std::back_inserter(lookup_keys),
                        static_cast<size_t>(dataSize * 0.3), std::mt19937(42));

            t0 = std::chrono::high_resolution_clock::now();
            size_t lookup_idx = 0;
            for (size_t i = 0; i < dataSize; ++i) {
                if (i % 10 < 7) {  // 70% inserts
                    mix_tree.insert(mixed_tuples[i]);
                } else {  // 30% lookups
                    if (lookup_idx < lookup_keys.size()) {
                        auto val = mix_tree.get(lookup_keys[lookup_idx++]);
                        // if (!val.has_value()) throw std::runtime_error("Missing key in mixed workload");
                    }
                }
            }
            t1 = std::chrono::high_resolution_clock::now();
            auto mixed_workload_time = std::chrono::duration<double, std::milli>(t1 - t0).count();
            auto mixed_workload_time_per_op = mixed_workload_time / dataSize;

            auto *tree_ptr = dynamic_cast<SimpleBPlusTree<key_type, 2> *>(&tree);
            if (!tree_ptr) throw std::runtime_error("Failed to cast BaseFile to SimpleBPlusTree");
            auto [leaf_count, utilization] = tree_ptr->get_leaf_stats();

            auto *mix_tree_ptr = dynamic_cast<SimpleBPlusTree<key_type, 2> *>(&mix_tree);
            if (!mix_tree_ptr) throw std::runtime_error("Failed to cast BaseFile to SimpleBPlusTree");
            size_t sorted_leaf_search = mix_tree_ptr->get_sorted_leaf_search();
            auto insert_time_per_op = insert_time / dataSize;

            for (size_t i = 0; i < read_ratios.size(); ++i) {
                results.push_back({"SimpleBTree", sortedness, read_ratios[i], insert_time_per_op,
                                  search_times[i], range_query_time_per_op, mixed_workload_time_per_op,
                                  leaf_count, utilization, 0, sorted_leaf_search});
            }
        }

        // === Benchmark 2: OptimizedBTree with LeafNode ===
        {
            const char *name = "opt.db";
            std::remove(name);
            db::getDatabase().add(
                std::make_unique<OptimizedBTree<key_type, LeafNode, 4>>(
                    SplitPolicy::SORT, 0, name, td));
            auto &tree = db::getDatabase().get(name);
            tree.init();

            // Measure insert time
            auto t0 = std::chrono::high_resolution_clock::now();
            for (const auto &tup: tuples) {
                tree.insert(tup);
            }
            auto t1 = std::chrono::high_resolution_clock::now();
            auto insert_time = std::chrono::duration<double, std::milli>(t1 - t0).count();

            // Measure point lookup time for different read ratios
            std::vector<double> search_times;
            for (double read_ratio: read_ratios) {
                std::vector<key_type> read_keys;
                std::sample(keys.begin(), keys.end(), std::back_inserter(read_keys),
                           static_cast<size_t>(dataSize * read_ratio), std::mt19937(42));

                t0 = std::chrono::high_resolution_clock::now();
                for (key_type k: read_keys) {
                    auto val = tree.get(k);
                    if (!val.has_value()) throw std::runtime_error("Missing key in optimized tree");
                }
                t1 = std::chrono::high_resolution_clock::now();
                double avg_search_time_per_op = std::chrono::duration<double, std::milli>(t1 - t0).count() / read_keys.size();
                search_times.push_back(avg_search_time_per_op);
                // search_times.push_back(std::chrono::duration<double, std::milli>(t1 - t0).count());
            }

            // Measure range query time
            t0 = std::chrono::high_resolution_clock::now();
            for (const auto &[start, end]: range_queries) {
                auto results = tree.range(start, end);
                // if (results.empty() && end >= start) throw std::runtime_error("Empty range result in optimized tree");
            }
            t1 = std::chrono::high_resolution_clock::now();
            auto range_query_time = std::chrono::duration<double, std::milli>(t1 - t0).count();
            auto range_query_time_per_op = range_query_time / range_queries.size();

            // Mixed workload (70% insert, 30% lookup)
            std::vector<key_type> mixed_keys(dataSize);
            std::iota(mixed_keys.begin(), mixed_keys.end(), dataSize); // New keys starting from dataSize

            const char *mix_name = "opt_mix.db";
            std::remove(mix_name);
            db::getDatabase().add(
                std::make_unique<OptimizedBTree<key_type, LeafNode, 4>>(
                    SplitPolicy::SORT, 0, mix_name, td));
            auto &mix_tree = db::getDatabase().get(mix_name);
            mix_tree.init();

            std::vector<Tuple> mixed_tuples;
            for (key_type k: mixed_keys) {
                std::vector<db::field_t> fields = {
                    db::field_t(k),
                    db::field_t("val-" + std::to_string(k))
                };
                mixed_tuples.emplace_back(fields, td.get_types());
            }

            std::vector<key_type> lookup_keys;
            std::sample(keys.begin(), keys.end(), std::back_inserter(lookup_keys),
                        static_cast<size_t>(dataSize * 0.3), std::mt19937(42));

            t0 = std::chrono::high_resolution_clock::now();
            size_t lookup_idx = 0;
            for (size_t i = 0; i < dataSize; ++i) {
                if (i % 10 < 7) {  // 70% inserts
                    mix_tree.insert(mixed_tuples[i]);
                } else {  // 30% lookups
                    if (lookup_idx < lookup_keys.size()) {
                        auto val = mix_tree.get(lookup_keys[lookup_idx++]);
                        // if (!val.has_value()) throw std::runtime_error("Missing key in mixed workload");
                    }
                }
            }
            t1 = std::chrono::high_resolution_clock::now();
            auto mixed_workload_time = std::chrono::duration<double, std::milli>(t1 - t0).count();
            auto mixed_workload_time_per_op = mixed_workload_time / dataSize;

            auto *tree_ptr = dynamic_cast<OptimizedBTree<key_type, LeafNode, 4> *>(&tree);
            if (!tree_ptr) throw std::runtime_error("Failed to cast BaseFile to OptimizedBTree");
            auto [leaf_count, utilization] = tree_ptr->get_leaf_stats();

            auto *mix_tree_ptr = dynamic_cast<OptimizedBTree<key_type, LeafNode, 4> *>(&mix_tree);
            if (!mix_tree_ptr) throw std::runtime_error("Failed to cast BaseFile to OptimizedBTree");
            size_t sorted_leaf_search = mix_tree_ptr->get_sorted_leaf_search();
            auto insert_time_per_op = insert_time / dataSize;

            for (size_t i = 0; i < read_ratios.size(); ++i) {
                results.push_back({"OptimizedBTree", sortedness, read_ratios[i], insert_time_per_op,
                                  search_times[i], range_query_time_per_op, mixed_workload_time_per_op,
                                  leaf_count, utilization, tree_ptr->get_fast_path_hits(), sorted_leaf_search});
            }
        }

        // === Benchmark 3: OptimizedBTree with AppendLeafNode ===
        {
            const char *name = "lsm.db";
            std::remove(name);
            db::getDatabase().add(
                std::make_unique<OptimizedBTree<key_type, AppendOnlyLeafNode, 4> >(
                    SplitPolicy::SORT, 0, name, td));
            auto &tree = db::getDatabase().get(name);
            tree.init();

            // Measure insert time
            auto t0 = std::chrono::high_resolution_clock::now();
            for (const auto &tup: tuples) {
                tree.insert(tup);
            }
            auto t1 = std::chrono::high_resolution_clock::now();
            auto insert_time = std::chrono::duration<double, std::milli>(t1 - t0).count();

            // Measure point lookup time for different read ratios
            std::vector<double> search_times;
            for (double read_ratio: read_ratios) {
                std::vector<key_type> read_keys;
                std::sample(keys.begin(), keys.end(), std::back_inserter(read_keys),
                           static_cast<size_t>(dataSize * read_ratio), std::mt19937(42));

                t0 = std::chrono::high_resolution_clock::now();
                for (key_type k: read_keys) {
                    auto val = tree.get(k);
                    if (!val.has_value()) throw std::runtime_error("Missing key in LSM tree");
                }
                t1 = std::chrono::high_resolution_clock::now();
                double avg_search_time_per_op = std::chrono::duration<double, std::milli>(t1 - t0).count() / read_keys.size();
                search_times.push_back(avg_search_time_per_op);
                // search_times.push_back(std::chrono::duration<double, std::milli>(t1 - t0).count());
            }

            // Measure range query time
            t0 = std::chrono::high_resolution_clock::now();
            for (const auto &[start, end]: range_queries) {
                auto results = tree.range(start, end);
                // if (results.empty() && end >= start) throw std::runtime_error("Empty range result in LSM tree");
            }
            t1 = std::chrono::high_resolution_clock::now();
            auto range_query_time = std::chrono::duration<double, std::milli>(t1 - t0).count();
            auto range_query_time_per_op = range_query_time / range_queries.size();

            // Mixed workload (70% insert, 30% lookup)
            std::vector<key_type> mixed_keys(dataSize);
            std::iota(mixed_keys.begin(), mixed_keys.end(), dataSize); // New keys starting from dataSize

            // construct a new tree for mixed workload
            const char *mix_name = "lsm_mix.db";
            std::remove(mix_name);
            db::getDatabase().add(
                std::make_unique<OptimizedBTree<key_type, AppendOnlyLeafNode, 4> >(
                    SplitPolicy::SORT, 0, mix_name, td));
            auto &mix_tree = db::getDatabase().get(mix_name);
            mix_tree.init();

            std::vector<Tuple> mixed_tuples;
            for (key_type k: mixed_keys) {
                std::vector<db::field_t> fields = {
                    db::field_t(k),
                    db::field_t("val-" + std::to_string(k))
                };
                mixed_tuples.emplace_back(fields, td.get_types());
            }

            std::vector<key_type> lookup_keys;
            std::sample(keys.begin(), keys.end(), std::back_inserter(lookup_keys),
                        static_cast<size_t>(dataSize * 0.3), std::mt19937(42));

            t0 = std::chrono::high_resolution_clock::now();
            size_t lookup_idx = 0;
            for (size_t i = 0; i < dataSize; ++i) {
                if (i % 10 < 7) {  // 70% inserts
                    mix_tree.insert(mixed_tuples[i]);
                } else {  // 30% lookups
                    if (lookup_idx < lookup_keys.size()) {
                        auto val = mix_tree.get(lookup_keys[lookup_idx++]);
                        // if (!val.has_value()) throw std::runtime_error("Missing key in mixed workload");
                    }
                }
            }
            t1 = std::chrono::high_resolution_clock::now();
            auto mixed_workload_time = std::chrono::duration<double, std::milli>(t1 - t0).count();
            auto mixed_workload_time_per_op = mixed_workload_time / dataSize;

            auto *tree_ptr = dynamic_cast<OptimizedBTree<key_type, AppendOnlyLeafNode, 4> *>(&tree);
            if (!tree_ptr) throw std::runtime_error("Failed to cast BaseFile to OptimizedBTree");
            auto [leaf_count, utilization] = tree_ptr->get_leaf_stats();

            auto *mix_tree_ptr = dynamic_cast<OptimizedBTree<key_type, AppendOnlyLeafNode, 4> *>(&mix_tree);
            if (!mix_tree_ptr) throw std::runtime_error("Failed to cast BaseFile to OptimizedBTree");
            size_t sorted_leaf_search = mix_tree_ptr->get_sorted_leaf_search();
            auto insert_time_per_op = insert_time / dataSize;
            for (size_t i = 0; i < read_ratios.size(); ++i) {
                results.push_back({"LoggedBTree", sortedness, read_ratios[i], insert_time_per_op,
                                  search_times[i], range_query_time_per_op, mixed_workload_time_per_op,
                                  leaf_count, utilization, tree_ptr->get_fast_path_hits(), sorted_leaf_search});
            }
        }

        // === Benchmark 4: LasTree ===
        {
            const char *name = "las.db";
            std::remove(name);
            db::getDatabase().add(
                std::make_unique<LasTree<key_type, 4> >(0, name, td));
            auto &tree = db::getDatabase().get(name);
            tree.init();

            // Measure insert time
            auto t0 = std::chrono::high_resolution_clock::now();
            for (const auto &tup: tuples) {
                tree.insert(tup);
            }
            auto t1 = std::chrono::high_resolution_clock::now();
            auto insert_time = std::chrono::duration<double, std::milli>(t1 - t0).count();

            // Measure point lookup time for different read ratios
            std::vector<double> search_times;
            for (double read_ratio: read_ratios) {
                std::vector<key_type> read_keys;
                std::sample(keys.begin(), keys.end(), std::back_inserter(read_keys),
                           static_cast<size_t>(dataSize * read_ratio), std::mt19937(42));

                t0 = std::chrono::high_resolution_clock::now();
                for (key_type k: read_keys) {
                    auto val = tree.get(k);
                    if (!val.has_value()) throw std::runtime_error("Missing key in LaS tree");
                }
                t1 = std::chrono::high_resolution_clock::now();
                double avg_search_time_per_op = std::chrono::duration<double, std::milli>(t1 - t0).count() / read_keys.size();
                search_times.push_back(avg_search_time_per_op);
                // search_times.push_back(std::chrono::duration<double, std::milli>(t1 - t0).count());
            }

            // Measure range query time
            t0 = std::chrono::high_resolution_clock::now();
            for (const auto &[start, end]: range_queries) {
                auto results = tree.range(start, end);
                // if (results.empty() && end >= start) throw std::runtime_error("Empty range result in LaS tree");
            }
            t1 = std::chrono::high_resolution_clock::now();
            auto range_query_time = std::chrono::duration<double, std::milli>(t1 - t0).count();
            auto range_query_time_per_op = range_query_time / range_queries.size();

            // Mixed workload (70% insert, 30% lookup)
            std::vector<key_type> mixed_keys(dataSize);
            std::iota(mixed_keys.begin(), mixed_keys.end(), dataSize); // New keys starting from dataSize

            // construct a new tree for mixed workload
            const char *mix_name = "las_mix.db";
            std::remove(mix_name);
            getDatabase().add(
                std::make_unique<LasTree<key_type, 4> >(0, mix_name, td));
            auto &mix_tree = db::getDatabase().get(mix_name);
            mix_tree.init();


            std::vector<Tuple> mixed_tuples;
            for (key_type k: mixed_keys) {
                std::vector<db::field_t> fields = {
                    db::field_t(k),
                    db::field_t("val-" + std::to_string(k))
                };
                mixed_tuples.emplace_back(fields, td.get_types());
            }

            std::vector<key_type> lookup_keys;
            std::sample(keys.begin(), keys.end(), std::back_inserter(lookup_keys),
                        static_cast<size_t>(dataSize * 0.3), std::mt19937(42));

            t0 = std::chrono::high_resolution_clock::now();
            size_t lookup_idx = 0;
            for (size_t i = 0; i < dataSize; ++i) {
                if (i % 10 < 7) {  // 70% inserts
                    mix_tree.insert(mixed_tuples[i]);
                } else {  // 30% lookups
                    if (lookup_idx < lookup_keys.size()) {
                        auto val = mix_tree.get(lookup_keys[lookup_idx++]);
                        // if (!val.has_value()) throw std::runtime_error("Missing key in mixed workload");
                    }
                }
            }
            t1 = std::chrono::high_resolution_clock::now();
            auto mixed_workload_time = std::chrono::duration<double, std::milli>(t1 - t0).count();
            auto mixed_workload_time_per_op = mixed_workload_time / dataSize;

            auto *tree_ptr = dynamic_cast<LasTree<key_type, 4> *>(&tree);
            if (!tree_ptr) throw std::runtime_error("Failed to cast BaseFile to LasTree");
            auto [leaf_count, utilization] = tree_ptr->get_leaf_stats();

            auto *mix_tree_ptr = dynamic_cast<LasTree<key_type, 4> *>(&mix_tree);
            if (!mix_tree_ptr) throw std::runtime_error("Failed to cast BaseFile to LasTree");
            size_t sorted_leaf_search = mix_tree_ptr->get_sorted_leaf_search();
            auto insert_time_per_op = insert_time / dataSize;
            for (size_t i = 0; i < read_ratios.size(); ++i) {
                results.push_back({"LasTree", sortedness, read_ratios[i], insert_time_per_op,
                                  search_times[i], range_query_time_per_op, mixed_workload_time_per_op,
                                  leaf_count, utilization, tree_ptr->get_fast_path_hits(), sorted_leaf_search});
            }
        }
    }

    // Export to CSV
    std::ofstream out("btree_benchmark.csv");
    out << "TreeType,Sortedness,ReadRatio,InsertTime,PointLookupTime,RangeQueryTime,MixedWorkloadTime,LeafCount,LeafUtilization,FastPathHits,SortedLeafSearch\n";
    for (const auto &r: results) {
        out << r.tree_name << "," << r.sortedness << "," << r.read_ratio << ","
                << r.insert_time_ms << "," << r.search_time_ms << "," << r.range_query_time_ms << ","
                << r.mixed_workload_time_ms << "," << r.leaf_count << "," << r.leaf_utilization << ","
                << r.fast_path_hits << "," << r.sorted_leaf_search << "\n";
    }
    out.close();
    std::cout << "CSV written to btree_benchmark.csv\n";
}

int main(int argc, char *argv[]) {
    size_t dataSize = 100000;
    if (argc > 1) dataSize = std::stoi(argv[1]);

    run_benchmark(dataSize);
    return 0;
}