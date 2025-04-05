#include <iostream>
#include <vector>
#include <string>
#include <optional>
#include "SimpleBPlusTree.hpp"
#include "OptimizedBTree.hpp"
#include "MemoryBlockManager.hpp"

// Helper function to test a tree
template <typename TreeType>
void test_tree(TreeType& tree, const std::vector<std::pair<int, std::string>>& data, const std::string& label) {
    std::cout << "=== Testing " << label << " ===" << std::endl;

    // Insert data
    for (const auto& [key, value] : data) {
        tree.insert(key, value);
    }

    // Test search for inserted keys
    std::cout << "-> Verifying inserted keys..." << std::endl;
    for (const auto& [key, expected] : data) {
        auto result = tree.get(key);
        if (result && *result == expected) {
            std::cout << "PASS: Found key " << key << " with value '" << *result << "'\n";
        } else {
            std::cout << "FAIL: Key " << key << " not found or incorrect value\n";
        }
    }

    // Test search for non-existent keys
    std::cout << "-> Testing non-existent keys..." << std::endl;
    for (int key = 100; key < 105; ++key) {
        auto result = tree.get(key);
        if (!result) {
            std::cout << "PASS: Key " << key << " correctly not found\n";
        } else {
            std::cout << "FAIL: Unexpectedly found value '" << *result << "' for key " << key << "\n";
        }
    }

    std::cout << std::endl;
}

int main() {
    // Test data
    std::vector<std::pair<int, std::string>> testData = {
        {10, "ten"}, {5, "five"}, {20, "twenty"},
        {15, "fifteen"}, {8, "eight"}, {12, "twelve"}
    };

    // Memory manager with small block count for testing
    InMemoryBlockManager<uint32_t> blockManager(100);

    // Test SimpleBPlusTree
    {
        SimpleBPlusTree<int, std::string> simpleTree(blockManager);
        test_tree(simpleTree, testData, "SimpleBPlusTree");
    }

    // Reset block manager
    blockManager.reset();

    // Test OptimizedBTree
    {
        OptimizedBTree<int, std::string> optimizedTree(blockManager);
        test_tree(optimizedTree, testData, "OptimizedBTree");
    }

    return 0;
}

