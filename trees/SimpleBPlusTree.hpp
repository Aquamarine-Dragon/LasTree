#ifndef SIMPLEBPLUSTREE_HPP
#define SIMPLEBPLUSTREE_HPP

#pragma once

#include <cstdint>
#include <vector>
#include <iostream>
#include <memory>
#include <limits>
#include <optional>
#include <mutex>

#include "NodeTypes.hpp"
#include "BasicBTreeNode.hpp"
#include "BufferPool.hpp"
#include "Types.hpp"

// SimpleBPlusTree - Basic B+tree implementation using memory blocks
template <typename key_type, typename value_type, template <size_t> class BufferPoolType>
class SimpleBPlusTree : public BaseFile {
public:
    // Type aliases for readability
    using node_id_t = uint32_t;
    using node_t = BasicBTreeNode<node_id_t, key_type, value_type, db::DEFAULT_PAGE_SIZE>;
    using buffer_pool_t = BufferPoolType<DEFAULT_PAGE_SIZE>;
    using path_t = std::vector<node_id_t>;
    
    // Constants
    static constexpr const char* name = "SimpleBPlusTree";
    static constexpr bool concurrent = false;
    static constexpr uint16_t SPLIT_INTERNAL_POS = node_t::internal_capacity / 2;
    static constexpr uint16_t SPLIT_LEAF_POS = (node_t::leaf_capacity + 1) / 2;
    static constexpr node_id_t INVALID_NODE_ID = std::numeric_limits<node_id_t>::max();
    
    explicit SimpleBPlusTree(buffer_pool_t &buffer_pool,  const std::string& filename)
        : BaseFile(filename), buffer_pool(buffer_pool) {
        // Allocate leaf node
        head_id = num_pages.fetch_add(1);
        PageId leaf_pid{filename, head_id};
        Page& leaf_page = buffer_pool.get_mut_block(leaf_pid);
        node_t leaf(leaf_page.data(), bp_node_type::LEAF);
        buffer_pool.mark_dirty(leaf_pid);
        leaf.info->id = head_id;
        leaf.info->next_id = INVALID_NODE_ID;
        leaf.info->size = 0;

        // Allocate root node
        root_id = num_pages.fetch_add(1);
        PageId root_pid{filename, root_id};
        Page& root_page = buffer_pool.get_mut_block(root_pid);
        node_t root(root_page.data(), bp_node_type::INTERNAL);
        buffer_pool.mark_dirty(root_pid);
        root.info->id = root_id;
        root.info->next_id = INVALID_NODE_ID;
        root.info->size = 0;
        root.children[0] = head_id;
    }
    
    // Insert a key-value pair into the tree
    void insert(const Tuple &tuple) {
        // Find the leaf node where the key belongs
        node_t leaf;
        path_t path;
        find_leaf_with_path(leaf, path, key);
        
        // Try to insert in the leaf
        if (leaf.insert_sorted(key, value)) {
            block_manager.mark_dirty(leaf.info->id);
            size++;
            return;
        }
        
        // Leaf is full, need to split
        node_id_t new_id;
        key_type separator_key;
        split_leaf(leaf, key, value, separator_key, new_id);
        
        // Insert the separator key into the parent
        internal_insert(path, separator_key, new_id);
        
        size++;
    }
    
    // Update an existing key with a new value
    bool update(const key_type& key, const value_type& value) {
        node_t leaf;
        find_leaf(leaf, key);
        
        uint16_t index = leaf.value_slot(key);
        if (index >= leaf.info->size || leaf.keys[index] != key) {
            return false;
        }
        
        block_manager.mark_dirty(leaf.info->id);
        leaf.values[index] = value;
        return true;
    }
    
    // Search for a key and return its value if found
    std::optional<value_type> get(const key_type& key) const {
        node_t leaf;
        find_leaf(leaf, key);
        
        uint16_t index = leaf.value_slot(key);
        if (index < leaf.info->size && leaf.keys[index] == key) {
            return std::make_optional(leaf.values[index]);
        }
        
        return std::nullopt;
    }
    
    // Check if a key exists in the tree
    bool contains(const key_type& key) const {
        node_t leaf;
        find_leaf(leaf, key);
        
        uint16_t index = leaf.value_slot(key);
        return index < leaf.info->size && leaf.keys[index] == key;
    }
    
    // Print the tree structure (for debugging)
    void print() const {
        std::cout << "B+Tree Structure:" << std::endl;
        print_node(root_id, 0);
        
        std::cout << "\nLeaf Node Chain:" << std::endl;
        node_id_t curr_id = head_id;
        while (curr_id != INVALID_NODE_ID) {
            node_t node(block_manager.open_block(curr_id));
            std::cout << "Leaf [";
            for (uint16_t i = 0; i < node.info->size; i++) {
                std::cout << "(" << node.keys[i] << ":" << node.values[i] << ")";
                if (i < node.info->size - 1) std::cout << ", ";
            }
            std::cout << "]" << std::endl;
            
            curr_id = node.info->next_id;
        }
    }
    
    // Get the number of elements in the tree
    size_t get_size() const {
        return size;
    }
    
    // Get the height of the tree
    uint8_t get_height() const {
        return height;
    }
    
private:
    buffer_pool_t buffer_pool;
    
    // Tree structure identifiers
    std::atomic<node_id_t> num_pages{0};
    node_id_t root_id;
    node_id_t head_id;
    
    // Tree metrics
    uint8_t height = 1;
    size_t size = 0;

    // Find the leaf node that should contain the given key
    void find_leaf(node_t& node, const key_type& key) const {
        node_id_t node_id = root_id;
        PageId pid{filename, node_id};
        Page& page = buffer_pool.get_mut_block(pid);
        node.load(page.data());

        while (node.info->type == bp_node_type::INTERNAL) {
            uint16_t slot = node.child_slot(key);
            node_id = node.children[slot];
            pid = {filename, node_id};
            Page& page2 = buffer_pool.get_mut_block(pid);
            node.load(page2.data());
        }
    }

    
    // Find the leaf and collect the path from root to leaf
    void find_leaf_with_path(node_t& node, path_t& path, const key_type& key) const {
        node_id_t node_id = root_id;
        path.reserve(height);
        
        node.load(block_manager.open_block(node_id)); //
        
        // Traverse the tree and build path
        while (node.info->type == bp_node_type::INTERNAL) {
            path.push_back(node_id);
            uint16_t slot = node.child_slot(key);
            node_id = node.children[slot];
            node.load(block_manager.open_block(node_id));
        }
    }
    
    // Split a leaf node during insertion
    void split_leaf(node_t& leaf, const key_type& key, const value_type& value, 
                   key_type& separator_key, node_id_t& new_id) {
        block_manager.mark_dirty(leaf.info->id);
        
        // Create a new leaf node
        node_id_t new_leaf_id = block_manager.allocate();
        node_t new_leaf(block_manager.open_block(new_leaf_id), bp_node_type::LEAF);
        block_manager.mark_dirty(new_leaf_id);
        
        // Split position
        uint16_t split_pos = SPLIT_LEAF_POS;
        
        // First insert the new key-value pair into a temporary array
        std::vector<std::pair<key_type, value_type>> all_pairs;
        all_pairs.reserve(leaf.info->size + 1);
        
        // Copy existing keys and values
        for (uint16_t i = 0; i < leaf.info->size; i++) {
            all_pairs.push_back(std::make_pair(leaf.keys[i], leaf.values[i]));
        }
        
        // Add the new key-value pair
        all_pairs.push_back(std::make_pair(key, value));
        
        // Sort all pairs
        std::sort(all_pairs.begin(), all_pairs.end(), 
            [](const auto& a, const auto& b) { return a.first < b.first; });
        
        // Set up the new leaf
        new_leaf.info->id = new_leaf_id;
        new_leaf.info->next_id = leaf.info->next_id;
        leaf.info->next_id = new_leaf_id;
        
        // Distribute keys and values
        leaf.info->size = split_pos;
        for (uint16_t i = 0; i < split_pos; i++) {
            leaf.keys[i] = all_pairs[i].first;
            leaf.values[i] = all_pairs[i].second;
        }
        
        new_leaf.info->size = all_pairs.size() - split_pos;
        for (uint16_t i = 0; i < new_leaf.info->size; i++) {
            new_leaf.keys[i] = all_pairs[split_pos + i].first;
            new_leaf.values[i] = all_pairs[split_pos + i].second;
        }
        
        // Return the first key of the new leaf as separator
        separator_key = new_leaf.keys[0];
        new_id = new_leaf_id;
    }
    
    void internal_insert(const path_t &path, key_type key, node_id_t child_id) {
        // Process path in reverse (from leaf's parent up to root)
        for (auto it = path.rbegin(); it != path.rend(); ++it) {
            node_id_t node_id = *it;
            node_t node(block_manager.open_block(node_id));
            block_manager.mark_dirty(node_id);

            // Find the position where key should be inserted
            uint16_t index = node.child_slot(key);

            // If there's room in the node, insert and we're done
            if (node.info->size < node_t::internal_capacity) {
                // Shift existing keys and children to make room
                std::memmove(node.keys + index + 1, node.keys + index,
                             (node.info->size - index) * sizeof(key_type));
                std::memmove(node.children + index + 2, node.children + index + 1,
                             (node.info->size - index) * sizeof(node_id_t));

                // Insert new key and child
                node.keys[index] = key;
                node.children[index + 1] = child_id;
                ++node.info->size;
                return;
            }

            // Save original size
            uint16_t original_size = node.info->size;

            node_id_t new_node_id = block_manager.allocate();
            node_t new_node(block_manager.open_block(new_node_id), bp_node_type::INTERNAL);
            block_manager.mark_dirty(new_node_id);

            // Prepare split position
            uint16_t split_pos = SPLIT_INTERNAL_POS; // the key at split_pos will be propagated up to parent node
            new_node.info->id = new_node_id;
            new_node.info->next_id = node.info->next_id;
            node.info->next_id = new_node_id;

            // update node sizes
            new_node.info->size = node_t::internal_capacity - split_pos - 1; // new node get latter half keys
            node.info->size = split_pos;// original node get first half keys

            // Handle the split based on where the new key goes
            if (index < split_pos) {
                // New key goes in left node

                // Copy keys and children to new node from (split_pos + 1)
                std::memcpy(new_node.keys, node.keys + split_pos + 1,
                        new_node.info->size * sizeof(key_type));
                std::memcpy(new_node.children, node.children + split_pos + 1,
                            (new_node.info->size + 1) * sizeof(node_id_t));

                // Shift to make room for new key in original node
                std::memmove(node.keys + index + 1, node.keys + index,
                             (split_pos - index) * sizeof(key_type));
                std::memmove(node.children + index + 2, node.children + index + 1,
                             (split_pos - index) * sizeof(node_id_t));

                // Insert new key and child
                node.keys[index] = key;
                node.children[index + 1] = child_id;
                ++node.info->size;

                // Key to promote to parent
                key = node.keys[split_pos];
            } else if (index == split_pos) {
                // New key becomes the separator/promoted key

                // Copy keys and children to new node
                std::memcpy(new_node.keys, node.keys + split_pos + 1,
                        new_node.info->size * sizeof(key_type));
                std::memcpy(new_node.children + 1, node.children + split_pos + 1,
                            new_node.info->size * sizeof(node_id_t));

                // Set up the new node's first child to be the new child
                new_node.children[0] = child_id;

                // Key to promote is already correct
            } else {
                // New key goes in right (new) node
                uint16_t new_index = index - split_pos - 1;

                // Copy keys before new key (skip promote key)
                std::memcpy(new_node.keys, node.keys + split_pos + 1,
                            new_index * sizeof(key_type));

                // insert new key
                new_node.keys[new_index] = key;

                // Copy keys after insertion point
                std::memcpy(new_node.keys + new_index + 1, node.keys + index,
                            (original_size - index) * sizeof(key_type));

                // Copy children before insertion point
                std::memcpy(new_node.children, node.children + split_pos + 1,
                            (new_index + 1) * sizeof(node_id_t));

                // Insert new child
                new_node.children[new_index + 1] = child_id;

                // Copy children after insertion point
                std::memcpy(new_node.children + new_index + 2, node.children + index + 1,
                            (original_size - index) * sizeof(node_id_t));

                // Key to promote to parent
                ++new_node.info->size;
                key = node.keys[split_pos];
            }

            // Continue upward with new key and right node
            child_id = new_node_id;
        }
        create_new_root(key, child_id);
    }
    
    // create a new root when the tree height increases
    void create_new_root(const key_type& key, node_id_t right_child_id) {
        // allocate a new left node, this will contains our original root's content
        node_id_t left_child_id = block_manager.allocate();
        
        // Get current root
        node_t old_root(block_manager.open_block(root_id));
        
        // Create new left child by copying current root
        node_t left_child(block_manager.open_block(left_child_id), static_cast<bp_node_type>(old_root.info->type));
        block_manager.mark_dirty(left_child_id);
        
        // Copy contents of old root to left child
        left_child.copyInfoFrom(old_root);
        left_child.info->id = left_child_id;
        
        if (old_root.info->type == bp_node_type::INTERNAL) {
            // Copy keys and children
            std::memcpy(left_child.keys, old_root.keys, 
                       old_root.info->size * sizeof(key_type));
            std::memcpy(left_child.children, old_root.children, 
                       (old_root.info->size + 1) * sizeof(node_id_t));
        } else {
            // Copy keys and values for leaf nodes
            std::memcpy(left_child.keys, old_root.keys, 
                       old_root.info->size * sizeof(key_type));
            std::memcpy(left_child.values, old_root.values, 
                       old_root.info->size * sizeof(value_type));
        }

        // if original tree's height is 1 (root is leaf),
        // convert old root to internal node
        if (old_root.info->type == bp_node_type::LEAF) {
            old_root.load(block_manager.open_block(root_id));
            old_root.info->type = bp_node_type::INTERNAL;
        }
        
        // Update old root to become a new root
        block_manager.mark_dirty(root_id);
        old_root.info->size = 1;
        old_root.keys[0] = key;
        // new root only has one key and two children
        old_root.children[0] = left_child_id;
        old_root.children[1] = right_child_id;
        
        // Increase tree height
        height++;
    }
    
    // Print a node and its children recursively (for debugging)
    void print_node(node_id_t node_id, int level) const {
        for (int i = 0; i < level; i++) {
            std::cout << "  ";
        }
        
        node_t node(block_manager.open_block(node_id));
        
        if (node.info->type == bp_node_type::LEAF) {
            std::cout << "Leaf " << node_id << ": [";
            for (uint16_t i = 0; i < node.info->size; i++) {
                std::cout << node.keys[i];
                if (i < node.info->size - 1) std::cout << ", ";
            }
            std::cout << "]" << std::endl;
        } else {
            std::cout << "Internal " << node_id << ": [";
            for (uint16_t i = 0; i < node.info->size; i++) {
                std::cout << node.keys[i];
                if (i < node.info->size - 1) std::cout << ", ";
            }
            std::cout << "]" << std::endl;
            
            for (uint16_t i = 0; i <= node.info->size; i++) {
                print_node(node.children[i], level + 1);
            }
        }
    }
};

#endif //SIMPLEBPLUSTREE_HPP