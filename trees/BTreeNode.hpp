#ifndef BTREENODE_HPP
#define BTREENODE_HPP

#pragma once

#include <cstdint>
#include <algorithm>
#include <memory>

#include "NodeTypes.hpp"
#include "Tuple.hpp"

using namespace db;

// BTreeNode class for a memory-efficient B+tree implementation
template <typename node_id_type, typename tuple_type, size_t block_size, size_t key_index>
class BTreeNode {
private:
    using key_type = typename std::tuple_element<key_index, tuple_type>::type;

    // Shared info for both internal and leaf nodes
    struct node_info {
        node_id_type id;
        uint16_t size;
        uint16_t type; // LEAF or INTERNAL
    };

    struct leaf_info {
        node_id_type next_id;   // For leaf node chaining
        bool isSorted;
        bool isCold;
    };

public:
    // Calculate maximum capacities based on block size and types
    static constexpr uint16_t leaf_capacity =
        (block_size - sizeof(node_info) - sizeof(leaf_info)) / sizeof(tuple_type);

    static constexpr uint16_t internal_capacity =
        (block_size - sizeof(node_info) - sizeof(node_id_type)) /
        (sizeof(key_type) + sizeof(node_id_type));

    // Layout
    node_info* info;
    union {
        struct {
            leaf_info* leafMeta;
            tuple_type* tuples;
        } leaf;
        struct {
            key_type* keys;
            node_id_type* children;
        } internal;
    } u;

    // Default constructor
    BTreeNode() = default;

    // Constructor that loads from an existing buffer
    explicit BTreeNode(void* buf) {
        load(buf);
    }

    // Constructor that initializes a new node of specific type
    BTreeNode(void* buf, const bp_node_type& type) {
        info = static_cast<node_info*>(buf);
        info->type = type;
        info->size = 0;

        if (type == bp_node_type::LEAF) {
            u.leaf.leafMeta = reinterpret_cast<leaf_info*>(info + 1);
            u.leaf.tuples = reinterpret_cast<tuple_type*>(u.leaf.leafMeta + 1);
            u.leaf.leafMeta->isSorted = true;
            u.leaf.leafMeta->isCold = false;
            u.leaf.leafMeta->next_id = 0;
        } else {
            u.internal.keys = reinterpret_cast<key_type*>(info + 1);
            u.internal.children = reinterpret_cast<node_id_type*>(u.internal.keys + internal_capacity);
        }
    }

    // Load node from an existing memory block
    void load(void* buf) {
        info = static_cast<node_info*>(buf);

        if (info->type == bp_node_type::LEAF) {
            u.leaf.leafMeta = reinterpret_cast<leaf_info*>(info + 1);
            u.leaf.tuples = reinterpret_cast<tuple_type*>(u.leaf.leafMeta + 1);
        } else {
            u.internal.keys = reinterpret_cast<key_type*>(info + 1);
            u.internal.children = reinterpret_cast<node_id_type*>(u.internal.keys + internal_capacity);
        }
    }

    key_type extract_key(const tuple_type& t) const {
        return std::get<key_index>(t);
    }

    // Find the slot where a key should be inserted (using binary search)
    uint16_t value_slot(const key_type& key) const {
        if (u.leaf.leafMeta->isSorted) {
            uint16_t left = 0, right = info->size;
            while (left < right) {
                uint16_t mid = (left + right) / 2;
                if (extract_key(u.leaf.tuples[mid]) < key) left = mid + 1;
                else right = mid;
            }
            return left;
        } else {
            for (uint16_t i = 0; i < info->size; i++) {
                if (extract_key(u.leaf.tuples[i]) >= key) return i;
            }
            return info->size;
        }
    }

    bool is_leaf() const { return info->type == bp_node_type::LEAF; }
    uint16_t size() const { return info->size; }
    node_id_type id() const { return info->id; }

    // Find child slot for internal nodes
    // if keys = [10, 20, 30] which points to 4 child slots , child_slot(8) will return slot 0
    uint16_t child_slot(const key_type& key) const {
        auto it = std::upper_bound(u.internal.keys, u.internal.keys + info->size, key);
        return std::distance(u.internal.keys, it);
    }

    // Insert a key-value pair in sorted position (for leaf nodes)
    bool insert_sorted(const tuple_type& t) {
        key_type key = extract_key(t);
        uint16_t index = value_slot(key);

        if (index < info->size && extract_key(u.leaf.tuples[index]) == key) {
            u.leaf.tuples[index] = t;
            return true;
        }

        if (info->size >= leaf_capacity) return false;

        std::memmove(u.leaf.tuples + index + 1, u.leaf.tuples + index,
                     (info->size - index) * sizeof(tuple_type));
        u.leaf.tuples[index] = t;
        info->size++;
        u.leaf.leafMeta->isSorted = true;
        return true;
    }

    // Append a key-value pair at the end (for fast path in leaf nodes)
    bool append(const tuple_type& t) {
        if (info->size >= leaf_capacity) return false;

        key_type key = extract_key(t);
        if (info->size > 0 && key < extract_key(u.leaf.tuples[info->size - 1])) {
            u.leaf.leafMeta->isSorted = false;
        }
        u.leaf.tuples[info->size++] = t;
        return true;
    }

    // Check if the node is nearly full (e.g., 90% capacity)
    bool is_nearly_full() const {
        return info->size >= (leaf_capacity * 0.9);
    }

    // Sort the node if it's not sorted
    void sort() {
        if (!u.leaf.leafMeta->isSorted && info->size > 1) {
            std::sort(u.leaf.tuples, u.leaf.tuples + info->size, [&](const tuple_type& a, const tuple_type& b) {
                return extract_key(a) < extract_key(b);
            });
            u.leaf.leafMeta->isSorted = true;
        }
    }


    // Quick partition for faster splitting (O(n) instead of O(n log n))
    int quick_partition() {
        if (info->size <= 1) return 0;

        std::vector<tuple_type> temp(u.leaf.tuples, u.leaf.tuples + info->size);

        size_t median_index = temp.size() / 2;
        std::nth_element(temp.begin(), temp.begin() + median_index, temp.end(),
                         [&](const tuple_type& a, const tuple_type& b) {
                             return extract_key(a) < extract_key(b);
                         });

        key_type median_key = extract_key(temp[median_index]);

        std::vector<tuple_type> left, right;
        for (uint16_t i = 0; i < info->size; ++i) {
            if (extract_key(u.leaf.tuples[i]) < median_key) {
                left.push_back(u.leaf.tuples[i]);
            } else {
                right.push_back(u.leaf.tuples[i]);
            }
        }

        // Copy left side back to current node
        for (uint16_t i = 0; i < left.size(); ++i) {
            u.leaf.tuples[i] = left[i];
        }

        info->size = left.size();
        u.leaf.leafMeta->isSorted = true;
        return left.size();
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

        if (info->type != bp_node_type::LEAF) {
            throw std::logic_error("min_key is only valid for leaf nodes");
        }

        if (u.leaf.leafMeta->isSorted) {
            return extract_key(u.leaf.tuples[0]);
        }
        return extract_key(*std::min_element(
        u.leaf.tuples,
        u.leaf.tuples + info->size,
        [this](const Tuple &a, const Tuple &b) {
            return a.get_field(key_index) < b.get_field(key_index);
        }));
    }

    // Get maximum key in the leaf
    key_type max_key() const {
        if (info->size == 0) {
            throw std::runtime_error("Empty node");
        }

        if (info->type != bp_node_type::LEAF) {
            throw std::logic_error("max_key is only valid for leaf nodes");
        }

        if (u.leaf.leafMeta->isSorted) {
            return extract_key(u.leaf.tuples[info->size - 1]);
        }
        // not sorted
        return extract_key(*std::max_element(
        u.leaf.tuples,
        u.leaf.tuples + info->size,
        [this](const Tuple &a, const Tuple &b) {
            return a.get_field(key_index) < b.get_field(key_index);
        }));
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
