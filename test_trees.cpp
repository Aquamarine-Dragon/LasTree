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
    using Buffer = BufferPool<64>;

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
        Buffer pool;
        SimpleBPlusTree<key_type, Buffer> tree(pool, "simple", td, 0);

        std::cout << "===== SimpleBPlusTree Test =====\n";
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

    // === OptimizedBTree with LeafNode ===
    {
        Buffer pool;
        OptimizedBTree<key_type, LeafNode, Buffer> tree(pool, OptimizedBTree<key_type, LeafNode, Buffer>::SORT_ON_SPLIT, 0, "opt", td);

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

    // === OptimizedBTree with LeafNodeLSM ===
    {
        Buffer pool;
        OptimizedBTree<key_type, LeafNodeLSM, Buffer> tree(pool, OptimizedBTree<key_type, LeafNodeLSM, Buffer>::SORT_ON_SPLIT, 0, "lsm", td);

        std::cout << "\n===== OptimizedBTree (LeafNodeLSM) Test =====\n";
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

    return 0;
}



