#ifndef BTREENODE_HPP
#define BTREENODE_HPP

#pragma once

#include <cstdint>
#include <algorithm>
#include <memory>

#include "NodeTypes.hpp"

// BTreeNode class for a memory-efficient B+tree implementation
template <typename node_id_type, typename key_type, typename value_type, size_t block_size>
class BTreeNode {
private:
    // Node information structure at the beginning of each block
    struct node_info {
        node_id_type id;        // Node identifier
        node_id_type next_id;   // Next node ID (for leaf nodes in linked list)
        uint16_t size;          // Number of keys currently in the node
        uint16_t type;          // Node type (LEAF or INTERNAL)
        bool isSorted;          // Flag to indicate if the node is sorted
        bool isCold;            // Flag to indicate if this is a cold node
    };

    // todo: currently unused
    struct internal_info {
        node_id_type id;
        uint16_t type;
        uint16_t size;
    };

public:
    // Calculate maximum capacities based on block size and types
    static constexpr uint16_t leaf_capacity =
        (block_size - sizeof(node_info)) /
        (sizeof(key_type) + sizeof(value_type));

    static constexpr uint16_t internal_capacity =
        (block_size - sizeof(node_info) - sizeof(node_id_type)) /
        (sizeof(key_type) + sizeof(node_id_type));

    // Pointers to in-memory block components
    node_info* info;
    key_type* keys;

    // Union to differentiate between leaf and internal nodes
    union {
        node_id_type* children; // Children pointers for internal nodes
        value_type* values;     // Values for leaf nodes
    };

    // Default constructor
    BTreeNode() = default;

    // Constructor that loads from an existing buffer
    explicit BTreeNode(void* buf) {
        load(buf);
    }

    // Constructor that initializes a new node of specific type
    BTreeNode(void* buf, const bp_node_type& type) {
        info = static_cast<node_info*>(buf);
        keys = reinterpret_cast<key_type*>(info + 1);
        info->type = type;
        info->size = 0;
        info->isSorted = true;
        info->isCold = false;

        if (info->type == bp_node_type::LEAF) {
            values = reinterpret_cast<value_type*>(keys + leaf_capacity);
        } else {
            children = reinterpret_cast<node_id_type*>(keys + internal_capacity);
        }
    }

    // Load node from an existing memory block
    void load(void* buf) {
        info = static_cast<node_info*>(buf);
        keys = reinterpret_cast<key_type*>(info + 1);

        if (info->type == bp_node_type::LEAF) {
            values = reinterpret_cast<value_type*>(keys + leaf_capacity);
        } else {
            children = reinterpret_cast<node_id_type*>(keys + internal_capacity);
        }
    }

    // Find the slot where a key should be inserted (using binary search)
    uint16_t value_slot(const key_type& key) const {
        // If the node is sorted, use binary search
        if (info->isSorted) {
            auto it = std::lower_bound(keys, keys + info->size, key);
            return std::distance(keys, it);
        } else {
            // Linear search for unsorted nodes
            uint16_t i;
            for (i = 0; i < info->size; i++) {
                if (keys[i] >= key) break;
            }
            return i;
        }
    }

    // Find child slot for internal nodes
    // if keys = [10, 20, 30] which points to 4 child slots , child_slot(8) will return slot 0
    uint16_t child_slot(const key_type& key) const {
        auto it = std::upper_bound(keys, keys + info->size, key);
        return std::distance(keys, it);
    }

    // branchless binary search for better cache behavior
    uint16_t branchless_binary_search(const key_type& key) const {
        int left = 0;
        int right = info->size - 1;

        while (left <= right) {
            int mid = (left + right) / 2;
            // branchless version of if/else
            left = (keys[mid] < key) ? mid + 1 : left;
            right = (keys[mid] >= key) ? mid - 1 : right;
        }

        return left;
    }

    // Insert a key-value pair in sorted position (for leaf nodes)
    bool insert_sorted(const key_type& key, const value_type& value) {
        uint16_t index = value_slot(key);

        // Key already exists, update value
        if (index < info->size && keys[index] == key) {
            values[index] = value;
            return true;
        }

        // Check if node is full
        if (info->size >= leaf_capacity) {
            return false;
        }

        // Shift elements to make space for new key-value pair
        std::memmove(keys + index + 1, keys + index,
                    (info->size - index) * sizeof(key_type));
        std::memmove(values + index + 1, values + index,
                    (info->size - index) * sizeof(value_type));

        // Insert new key-value pair
        keys[index] = key;
        values[index] = value;
        info->size++;
        info->isSorted = true;

        return true;
    }

    // Append a key-value pair at the end (for fast path in leaf nodes)
    bool append(const key_type& key, const value_type& value) {
        // Check if node is full
        if (info->size >= leaf_capacity) {
            return false;
        }

        // Append at the end
        keys[info->size] = key;
        values[info->size] = value;

        // Check if this violates sorting
        if (info->size > 0 && key < keys[info->size - 1]) {
            info->isSorted = false;
        }

        info->size++;
        return true;
    }

    // Sort the node if it's not sorted
    void sort() {
        if (!info->isSorted && info->size > 1) {
            // Create temporary array for sorting
            std::vector<std::pair<key_type, value_type>> pairs;
            pairs.reserve(info->size);

            for (uint16_t i = 0; i < info->size; i++) {
                pairs.push_back(std::make_pair(keys[i], values[i]));
            }

            // Sort by key
            std::sort(pairs.begin(), pairs.end(),
                [](const auto& a, const auto& b) { return a.first < b.first; });

            // Copy back to node
            for (uint16_t i = 0; i < info->size; i++) {
                keys[i] = pairs[i].first;
                values[i] = pairs[i].second;
            }

            info->isSorted = true;
        }
    }

    // Quick partition for faster splitting (O(n) instead of O(n log n))
    int quick_partition() {
        if (info->size <= 1) {
            return 0;
        }

        // Find median using nth_element which is O(n)
        std::vector<key_type> keysCopy(keys, keys + info->size);
        size_t medianPos = keysCopy.size() / 2;
        std::nth_element(keysCopy.begin(), keysCopy.begin() + medianPos, keysCopy.end());
        key_type medianValue = keysCopy[medianPos];

        // Partition the keys and values around the median
        std::vector<key_type> leftKeys, rightKeys;
        std::vector<value_type> leftValues, rightValues;

        for (size_t i = 0; i < info->size; i++) {
            if (keys[i] < medianValue) {
                leftKeys.push_back(keys[i]);
                leftValues.push_back(values[i]);
            } else {
                rightKeys.push_back(keys[i]);
                rightValues.push_back(values[i]);
            }
        }

        // Replace current keys and values with the left partition
        for (size_t i = 0; i < leftKeys.size(); i++) {
            keys[i] = leftKeys[i];
            values[i] = leftValues[i];
        }

        info->size = leftKeys.size();
        info->isSorted = true;

        // Return the number of elements in the left partition
        return leftKeys.size();
    }

    // Check if the node is nearly full (e.g., 90% capacity)
    bool is_nearly_full() const {
        // currently only leaf uses this function
        return info->size >= (leaf_capacity * 0.9);
        // if (info->type == bp_node_type::LEAF) {
        //     return info->size >= (leaf_capacity * 0.9);
        // } else {
        //     return info->size >= (internal_capacity * 0.9);
        // }
    }

    // Mark this node as a "cold" node (not in fast path)
    void mark_as_cold() {
        info->isCold = true;
    }

    // Check if this node is a "cold" node
    bool is_cold() const {
        return info->isCold;
    }

    // Get minimum key in the leaf
    key_type min_key() const {
        if (info->size == 0) {
            throw std::runtime_error("Empty node");
        }

        if (info->isSorted) {
            return keys[0];
        } else {
            return *std::min_element(keys, keys + info->size);
        }
    }

    // Get maximum key in the leaf
    key_type max_key() const {
        if (info->size == 0) {
            throw std::runtime_error("Empty node");
        }

        if (info->isSorted) {
            return keys[info->size - 1];
        } else {
            return *std::max_element(keys, keys + info->size);
        }
    }
    void copyInfoFrom(const BTreeNode& other) {
        this->info->id = other.info->id;
        this->info->next_id = other.info->next_id;
        this->info->size = other.info->size;
        this->info->type = other.info->type;
        this->info->isSorted = other.info->isSorted;
        this->info->isCold = other.info->isCold;
    }
};


#endif //BTREENODE_HPP
