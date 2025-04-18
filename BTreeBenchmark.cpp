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
#include "LeafNodeLSM.hpp"

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
    size_t node_count;
    size_t fast_path_hits;
};

template<typename Tree>
std::optional<value_type> get(Tree &tree, const key_type &key) {
    return tree.get(key);
}

void run_benchmark(size_t dataSize) {
    using key_type = int;
    using TupleT = db::Tuple;
    using Buffer = BufferPool;

    // std::vector<double> sortedness_levels = {1.0, 0,0};
    std::vector<double> sortedness_levels = {1.0, 0.95, 0.8, 0.5, 0.2, 0.0};
    std::vector<double> read_ratios = {0.0, 0.1, 0.5};

    std::vector<ResultRow> results;

    for (double sortedness: sortedness_levels) {
        for (double read_ratio: read_ratios) {
            std::cout << "Benchmarking: sortedness=" << sortedness << ", read_ratio=" << read_ratio << "\n";

            // generate keys
            std::vector<key_type> keys(dataSize);
            std::iota(keys.begin(), keys.end(), 0);


            if (sortedness < 1.0) {
                size_t shuffle_count = static_cast<size_t>(dataSize * (1.0 - sortedness));
                std::mt19937 rng(42);
                std::shuffle(keys.begin(), keys.begin() + shuffle_count, rng);
            }

            // generate tuples
            db::TupleDesc td({db::type_t::INT, db::type_t::CHAR}, {"key", "val"});
            std::vector<TupleT> tuples;
            for (key_type k: keys) {
                std::vector<db::field_t> fields = {
                    db::field_t(k),
                    db::field_t("val-" + std::to_string(k))
                };
                tuples.emplace_back(fields, td.get_types());
            }

            // sample read keys
            std::vector<key_type> read_keys;
            std::sample(keys.begin(), keys.end(), std::back_inserter(read_keys),
                        static_cast<size_t>(dataSize * read_ratio), std::mt19937(42));

            // === Benchmark 1: SimpleBPlusTree ===
            {
                const char *name = "simple.db";
                std::remove(name);
                getDatabase().add(std::make_unique<SimpleBPlusTree<key_type, 2> >(name, td, 0));
                auto &tree = db::getDatabase().get(name);
                tree.init();

                auto t0 = std::chrono::high_resolution_clock::now();
                for (const auto &tup: tuples) {
                    tree.insert(tup);
                }

                auto t1 = std::chrono::high_resolution_clock::now();
                auto insert_time = std::chrono::duration<double, std::milli>(t1 - t0).count();

                t0 = std::chrono::high_resolution_clock::now();
                for (key_type k: read_keys) {
                    auto val = tree.get(k);
                    if (!val.has_value()) throw std::runtime_error("Missing key in simple tree");
                }
                t1 = std::chrono::high_resolution_clock::now();
                auto search_time = std::chrono::duration<double, std::milli>(t1 - t0).count();

                results.push_back({"SimpleBPlusTree", sortedness, read_ratio, insert_time, search_time, 0, 0});
            }

            // === Benchmark 2: OptimizedBTree with LeafNode ===
            {
                const char *name = "opt.db";
                // getDatabase().remove(name);
                std::remove(name);
                db::getDatabase().add(
                    std::make_unique<OptimizedBTree<key_type, LeafNode, 4>>(
                        SplitPolicy::SORT, 0, name, td));
                auto &tree = db::getDatabase().get(name);
                tree.init();

                auto t0 = std::chrono::high_resolution_clock::now();
                for (const auto &tup: tuples) {
                    tree.insert(tup);
                }
                auto t1 = std::chrono::high_resolution_clock::now();
                auto insert_time = std::chrono::duration<double, std::milli>(t1 - t0).count();

                t0 = std::chrono::high_resolution_clock::now();
                for (key_type k: read_keys) {

                    auto val = tree.get(k);
                    if (!val.has_value()) throw std::runtime_error("Missing key in optimized tree");
                }
                t1 = std::chrono::high_resolution_clock::now();
                auto search_time = std::chrono::duration<double, std::milli>(t1 - t0).count();

                auto *tree_ptr = dynamic_cast<OptimizedBTree<key_type, LeafNode, 4> *>(&tree);
                if (!tree_ptr) throw std::runtime_error("Failed to cast BaseFile to OptimizedBTree");
                results.push_back({
                    "OptimizedBTree", sortedness, read_ratio, insert_time, search_time, 0,
                    tree_ptr->get_fast_path_hits()
                });
            }

            // === Benchmark 3: OptimizedBTree with LeafNodeLSM ===
            {
                const char *name = "lsm.db";
                std::remove(name);
                db::getDatabase().add(
                    std::make_unique<OptimizedBTree<key_type, LeafNodeLSM, 4> >(
                        SplitPolicy::SORT, 0, name, td));
                auto &tree = db::getDatabase().get(name);
                tree.init();

                auto t0 = std::chrono::high_resolution_clock::now();
                for (const auto &tup: tuples) {
                    tree.insert(tup);
                }
                auto t1 = std::chrono::high_resolution_clock::now();
                auto insert_time = std::chrono::duration<double, std::milli>(t1 - t0).count();

                t0 = std::chrono::high_resolution_clock::now();
                for (key_type k: read_keys) {
                    auto val = tree.get(k);
                    if (!val.has_value()) throw std::runtime_error("Missing key in LSM tree");
                }
                t1 = std::chrono::high_resolution_clock::now();
                auto search_time = std::chrono::duration<double, std::milli>(t1 - t0).count();

                auto *tree_ptr = dynamic_cast<OptimizedBTree<key_type, LeafNodeLSM, 4> *>(&tree);
                if (!tree_ptr) throw std::runtime_error("Failed to cast BaseFile to OptimizedBTree");
                results.push_back({
                    "LSMSORTTree", sortedness, read_ratio, insert_time, search_time, 0, tree_ptr->get_fast_path_hits()
                });
            }

            // === Benchmark 4: OptimizedBTree with LeafNodeLSM (quick partition) ===
            {
                const char *name = "lsm_qp.db";
                std::remove(name);
                db::getDatabase().add(
                    std::make_unique<OptimizedBTree<key_type, LeafNodeLSM, 4> >(
                        SplitPolicy::QUICK_PARTITION, 0, name, td));
                auto &tree = db::getDatabase().get(name);
                tree.init();

                auto t0 = std::chrono::high_resolution_clock::now();
                for (const auto &tup: tuples) {
                    tree.insert(tup);
                }
                auto t1 = std::chrono::high_resolution_clock::now();
                auto insert_time = std::chrono::duration<double, std::milli>(t1 - t0).count();

                t0 = std::chrono::high_resolution_clock::now();
                for (key_type k: read_keys) {
                    auto val = tree.get(k);
                    if (!val.has_value()) throw std::runtime_error("Missing key in LSM tree");
                }
                t1 = std::chrono::high_resolution_clock::now();
                auto search_time = std::chrono::duration<double, std::milli>(t1 - t0).count();

                auto *tree_ptr = dynamic_cast<OptimizedBTree<key_type, LeafNodeLSM, 4> *>(&tree);
                if (!tree_ptr) throw std::runtime_error("Failed to cast BaseFile to OptimizedBTree");
                results.push_back({
                    "LSMQPTree", sortedness, read_ratio, insert_time, search_time, 0, tree_ptr->get_fast_path_hits()
                });
            }
        }
    }

    // Export to CSV
    std::ofstream out("btree_benchmark.csv");
    out << "TreeType,Sortedness,ReadRatio,InsertTime,SearchTime,NodeCount,FastPathHits\n";
    for (const auto &r: results) {
        out << r.tree_name << "," << r.sortedness << "," << r.read_ratio << ","
                << r.insert_time_ms << "," << r.search_time_ms << ","
                << r.node_count << "," << r.fast_path_hits << "\n";
    }
    out.close();
    std::cout << "CSV written to btree_benchmark.csv\n";
}


int main(int argc, char *argv[]) {
    size_t dataSize = 1000;
    if (argc > 1) dataSize = std::stoi(argv[1]);

    run_benchmark(dataSize);
    return 0;
}
