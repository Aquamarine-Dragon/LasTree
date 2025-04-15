#include <iostream>
#include <vector>
#include <string>
#include <optional>

#include "LeafNodeLSM.hpp"
#include "SimpleBPlusTree.hpp"
#include "OptimizedBTree.hpp"

// Helper function to test a tree
template <typename TreeType>
void test_tree(TreeType& tree, const std::vector<std::pair<int, std::string>>& data, const std::string& label) {
    std::cout << "=== Testing " << label << " ===" << std::endl;

    db::TupleDesc desc({db::type_t::INT, db::type_t::CHAR}, {"key", "val"});
    size_t key_index = 0;

    // Insert data
    for (const auto& [key, value] : data) {
        db::Tuple tuple({key, value}, desc.get_types());
        tree.insert(tuple);
    }

    // Test search for inserted keys
    std::cout << "-> Verifying inserted keys..." << std::endl;
    for (const auto& [key, expected] : data) {
        auto result = tree.get(key);
        if (result.has_value()) {
            const db::Tuple& tuple = *result;
            std::string val = std::get<std::string>(tuple.get_field(1));
            if (val == expected) {
                std::cout << "PASS: Found key " << key << " with value '" << val << "'\n";
            } else {
                std::cout << "FAIL: Key " << key << " has wrong value '" << val << "' (expected '" << expected << "')\n";
            }
        } else {
            std::cout << "FAIL: Key " << key << " not found\n";
        }
    }

    // Test search for non-existent keys
    std::cout << "-> Testing non-existent keys..." << std::endl;
    for (int key = 100; key < 105; ++key) {
        auto result = tree.get(key);
        if (!result) {
            std::cout << "PASS: Key " << key << " correctly not found\n";
        } else {
            std::cout << "FAIL: Unexpectedly found tuple for key " << key << "\n";
        }
    }

    std::cout << std::endl;
}

int main() {
    using key_type = int;
    using value_type = std::string;
    using TupleT = db::Tuple;

    db::TupleDesc td({db::type_t::INT, db::type_t::CHAR}, {"key", "val"});

    std::vector<TupleT> tuples;
    for (int i : {10, 5, 20, 15, 8, 12}) {
        std::vector<db::field_t> fields = {
            db::field_t(i),
            db::field_t("val-" + std::to_string(i))
        };
        tuples.emplace_back(fields, td.get_types());
    }

    // test keys not in tree
    std::vector<int> not_found = {100, 101, 102};

    // === SimpleBPlusTree ===
    {
        const char *name = "simple.db";
        std::remove(name);
        db::getDatabase().add(std::make_unique<SimpleBPlusTree<key_type>>(name, td, 0));
        auto &tree = db::getDatabase().get(name);
        tree.init();

        std::cout << "===== SimpleBPlusTree Test =====\n";
        for (const auto& t : tuples)
            tree.insert(t);

        for (const auto& t : tuples) {
            auto key = std::get<int>(t.get_field(0));
            auto result = tree.get(key);
            if (!result.has_value()) {
                std::cerr << "FAIL: key " << key << " not found\n";
                continue;
            }

            const auto& actual_tuple = result.value();
            const auto& expected = std::get<std::string>(t.get_field(1));
            const auto& actual = std::get<std::string>(actual_tuple.get_field(1));

            if (actual != expected) {
                std::cerr << "FAIL: key " << key << " value mismatch. Expected: " << expected << ", Got: " << actual << "\n";
            } else {
                std::cout << "PASS: key " << key << " -> " << actual << "\n";
            }
        }

        for (auto k : not_found) {
            if (tree.get(k)) {
                std::cerr << "FAIL: unexpected hit for key " << k << "\n";
            } else {
                std::cout << "PASS: key " << k << " correctly not found\n";
            }
        }
    }

    // getDatabase().remove("simple.db");

    // === OptimizedBTree with LeafNode ===
    {
        const char *name = "opt.db";
        std::remove(name);
        db::getDatabase().add(std::make_unique<OptimizedBTree<key_type, LeafNode>>(OptimizedBTree<key_type, LeafNode>::SORT_ON_SPLIT, 0, name, td));
        auto &tree = db::getDatabase().get(name);
        tree.init();
        // OptimizedBTree<key_type, LeafNode, Buffer> tree(pool, OptimizedBTree<key_type, LeafNode, Buffer>::SORT_ON_SPLIT, 0, "opt", td);

        std::cout << "\n===== OptimizedBTree (LeafNode) Test =====\n";
        for (const auto& t : tuples) tree.insert(t);

        for (const auto& t : tuples) {
            auto key = std::get<int>(t.get_field(0));
            auto result = tree.get(key);
            if (!result.has_value()) {
                std::cerr << "FAIL: key " << key << " not found\n";
                continue;
            }

            const auto& actual_tuple = result.value();
            const auto& expected = std::get<std::string>(t.get_field(1));
            const auto& actual = std::get<std::string>(actual_tuple.get_field(1));

            if (actual != expected) {
                std::cerr << "FAIL: key " << key << " value mismatch. Expected: " << expected << ", Got: " << actual << "\n";
            } else {
                std::cout << "PASS: key " << key << " -> " << actual << "\n";
            }
        }

        for (auto k : not_found) {
            if (tree.get(k)) {
                std::cerr << "FAIL: unexpected hit for key " << k << "\n";
            } else {
                std::cout << "PASS: key " << k << " correctly not found\n";
            }
        }
    }

    // getDatabase().remove("opt.db");

    // === OptimizedBTree with LeafNodeLSM ===
    // {
    //     const char *name = "lsm.db";
    //     std::remove(name);
    //     db::getDatabase().add(std::make_unique<OptimizedBTree<key_type, LeafNodeLSM>>(OptimizedBTree<key_type, LeafNodeLSM>::SORT_ON_SPLIT, 0, name, td));
    //     auto &tree = db::getDatabase().get(name);
    //     tree.init();
    //     std::cout << "\n===== OptimizedBTree (LeafNodeLSM) Test =====\n";
    //     for (const auto& t : tuples) tree.insert(t);
    //
    //     for (const auto& t : tuples) {
    //         auto key = std::get<int>(t.get_field(0));
    //         auto result = tree.get(key);
    //         if (!result.has_value()) {
    //             std::cerr << "FAIL: key " << key << " not found\n";
    //             continue;
    //         }
    //
    //         const auto& actual_tuple = result.value();
    //         const auto& expected = std::get<std::string>(t.get_field(1));
    //         const auto& actual = std::get<std::string>(actual_tuple.get_field(1));
    //
    //         if (actual != expected) {
    //             std::cerr << "FAIL: key " << key << " value mismatch. Expected: " << expected << ", Got: " << actual << "\n";
    //         } else {
    //             std::cout << "PASS: key " << key << " -> " << actual << "\n";
    //         }
    //     }
    //
    //
    //     for (auto k : not_found) {
    //         if (tree.get(k)) {
    //             std::cerr << "FAIL: unexpected hit for key " << k << "\n";
    //         } else {
    //             std::cout << "PASS: key " << k << " correctly not found\n";
    //         }
    //     }
    // }

    // getDatabase().remove("lsm.db");

    return 0;
}



