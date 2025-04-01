#ifndef BASIC_BTREENODE_HPP
#define BASIC_BTREENODE_HPP
#pragma once

#include <cstdint>
#include <algorithm>
#include <memory>

#include "NodeTypes.hpp"

// SimpleBTreeNode class - Basic version without optimization features
template <typename node_id_type, typename key_type, typename value_type, size_t block_size>
class BasicBTreeNode {
private:
    // Node information structure at the beginning of each block
    struct node_info {
        node_id_type id;        // Node identifier
        node_id_type next_id;   // Next node ID (for leaf nodes in linked list)
        uint16_t size;          // Number of keys currently in the node
        uint16_t type;          // Node type (LEAF or INTERNAL)
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
    // ptrs to sorted keys arr
    key_type* keys;

    // Union to differentiate between leaf and internal nodes
    union {
        node_id_type* children; // Children pointers for internal nodes
        value_type* values;     // Values for leaf nodes
    };

    // Default constructor
    BasicBTreeNode() = default;

    // Constructor that loads from an existing buffer
    explicit BasicBTreeNode(void* buf) {
        load(buf);
    }

    // Constructor that initializes a new node of specific type
    BasicBTreeNode(void* buf, const bp_node_type& type) {
        // 把整块内存 buf 的开头解释为一个 node_info 结构体 (强制转换）
        info = static_cast<node_info*>(buf);
        // info + 1 实际上是 “跳过一个 node_info 的大小”，也就是指向这个结构之后的下一块地址。
        // 所以这一步是：将 keys 指向在 node_info 后面存储的 key 数组起始位置。
        keys = reinterpret_cast<key_type*>(info + 1);
        info->type = type;
        info->size = 0;

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
        // Always use binary search since we maintain sorted order
        auto it = std::lower_bound(keys, keys + info->size, key);
        return std::distance(keys, it);
    }

    // Find child slot for internal nodes
    uint16_t child_slot(const key_type& key) const {
        // find the first index from [0..info.size - 1] where keys[index] > key
        auto it = std::upper_bound(keys, keys + info->size, key);
        // slot = index of first key > target
        return std::distance(keys, it);
    }

    // Insert a key-value pair in sorted position (for leaf nodes)
    // returns true if insert success
    bool insert_sorted(const key_type& key, const value_type& value) {
        // find the index to insert
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
        // len: how many byte to move
        // info->size - index: the number of ele from index to last ele
        std::memmove(keys + index + 1, keys + index,
                    (info->size - index) * sizeof(key_type));
        std::memmove(values + index + 1, values + index,
                    (info->size - index) * sizeof(value_type));

        // Insert new key-value pair
        keys[index] = key;
        values[index] = value;
        ++info->size;

        return true;
    }

    // Check if the node is full
    bool is_full() const {
        if (info->type == bp_node_type::LEAF) {
            return info->size >= leaf_capacity;
        } else {
            return info->size >= internal_capacity;
        }
    }

    // Get minimum key in the leaf
    key_type min_key() const {
        if (info->size == 0) {
            throw std::runtime_error("Empty node");
        }
        return keys[0];
    }

    // Get maximum key in the leaf
    key_type max_key() const {
        if (info->size == 0) {
            throw std::runtime_error("Empty node");
        }
        return keys[info->size - 1];
    }

    void copyInfoFrom(const BasicBTreeNode& other) {
        std::memcpy(this->info, other.info, sizeof(node_info));
    }
};


#endif //BTREENODE_HPP
