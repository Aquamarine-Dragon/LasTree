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
#include "LeafNode.hpp"
#include "Types.hpp"

// SimpleBPlusTree - Basic B+tree implementation using memory blocks
template <typename key_type, typename buffer_pool_t>
class SimpleBPlusTree : public BaseFile {
public:
    // Type aliases for readability
    using node_id_t = uint32_t;
    using leaf_t = LeafNode<node_id_t, key_type, db::DEFAULT_PAGE_SIZE>;
    using internal_t = InternalNode<node_id_t, key_type, db::DEFAULT_PAGE_SIZE>;
    // using buffer_pool_t = BufferPoolType<64>;
    using path_t = std::vector<node_id_t>;
    
    // Constants
    static constexpr const char* name = "SimpleBPlusTree";
    static constexpr bool concurrent = false;
    static constexpr uint16_t SPLIT_INTERNAL_POS = internal_t::internal_capacity / 2;
    static constexpr node_id_t INVALID_NODE_ID = std::numeric_limits<node_id_t>::max();
    
    explicit SimpleBPlusTree(buffer_pool_t &buffer_pool,
                const std::string& filename,
                const TupleDesc& td,
                size_t key_index)
        : BaseFile(filename),
          buffer_pool(buffer_pool),
          td(td),
          key_index(key_index) {
        // Allocate leaf node
        head_id = num_pages.fetch_add(1);
        PageId leaf_pid{filename, head_id};
        Page& leaf_page = buffer_pool.get_mut_page(leaf_pid);
        leaf_t leaf(leaf_page, td, key_index, head_id, INVALID_NODE_ID, /*isSorted=*/true, /*isCold=*/false);
        buffer_pool.mark_dirty(leaf_pid);

        // Allocate root node
        root_id = num_pages.fetch_add(1);
        PageId root_pid{filename, root_id};
        Page& root_page = buffer_pool.get_mut_page(root_pid);
        internal_t root(root_page, bp_node_type::INTERNAL);
        buffer_pool.mark_dirty(root_pid);
        root.header->size = 1;
        root.children[0] = head_id;
    }
    
    // Insert a key-value pair into the tree
    void insert(const Tuple &tuple) {
        // Find the leaf node where the key belongs
        path_t path;
        key_type key = std::get<key_type>(tuple.get_field(key_index));
        node_id_t leaf_id = find_leaf_with_path(path, key);
        PageId leaf_pid{filename, leaf_id};
        Page &leaf_page = buffer_pool.get_mut_page(leaf_pid);
        leaf_t leaf(leaf_page, td, key_index);
        
        // Try to insert in the leaf
        if (leaf.insert(tuple)) {
            buffer_pool.mark_dirty(leaf_pid);
            size++;
            return;
        }
        
        insert_into_leaf(leaf_pid, tuple, path);
        size++;
    }

    std::optional<db::Tuple> get(const key_type& key) const {
        // Find leaf containing key
        path_t path;
        node_id_t leaf_id = find_leaf_with_path(path, key);
        PageId page_id{filename, leaf_id};
        Page& page = buffer_pool.get_mut_page(page_id);
        leaf_t leaf(page, td, key_index);

        return leaf.get(key);
    }

    
    // Update an existing key with a new value
    // bool update(const key_type& key, const value_type& value) {
    //     node_t leaf;
    //     find_leaf(leaf, key);
    //
    //     uint16_t index = leaf.value_slot(key);
    //     if (index >= leaf.info->size || leaf.keys[index] != key) {
    //         return false;
    //     }
    //
    //     block_manager.mark_dirty(leaf.info->id);
    //     leaf.values[index] = value;
    //     return true;
    // }
    
    // Print the tree structure (for debugging)

    
    // Get the number of elements in the tree
    size_t get_size() const {
        return size;
    }
    
    // Get the height of the tree
    uint8_t get_height() const {
        return height;
    }
    
private:
    buffer_pool_t &buffer_pool;
    const TupleDesc& td;
    size_t key_index;
    
    // Tree structure identifiers
    std::atomic<node_id_t> num_pages{0};
    node_id_t root_id;
    node_id_t head_id;
    
    // Tree metrics
    uint8_t height = 1;
    size_t size = 0;

    // Find the leaf and collect the path from root to leaf
    node_id_t find_leaf_with_path(path_t& path, const key_type& key) const {
        path.reserve(height);
        node_id_t node_id = root_id;

        while (true) {
            path.push_back(node_id);
            PageId pid{filename, node_id};
            Page& page = buffer_pool.get_mut_page(pid);

            const auto* base_header = reinterpret_cast<const BaseHeader*>(page.data());
            if (base_header->type == bp_node_type::LEAF) break;

            internal_t node(page);
            // node.load(page);
            uint16_t slot = node.child_slot(key);
            node_id = node.children[slot];
        }

        return node_id;
    }

    /**
     * insert tuple into leaf by path (splits required)
     */
    void insert_into_leaf(const PageId& pid, const Tuple& t, const path_t& path) {
        Page& page = buffer_pool.get_mut_page(pid);
        leaf_t leaf(page, td, key_index);

        // split
        node_id_t new_leaf_id = num_pages.fetch_add(1);
        PageId new_leaf_pid{filename, new_leaf_id};
        Page& new_leaf_page = buffer_pool.get_mut_page(new_leaf_pid);
        leaf_t new_leaf(new_leaf_page, td, key_index, new_leaf_id, INVALID_NODE_ID, true, false);
        buffer_pool.mark_dirty(new_leaf_pid);

        auto [split_key, new_id] = leaf.split_into(new_leaf);
        buffer_pool.mark_dirty(pid);

        // Insert again (must succeed)
        new_leaf.insert(t);

        internal_insert(path, split_key, new_id);
    }

    // Insert a key and child into internal nodes along the path
    void internal_insert(const path_t &path, key_type key, node_id_t child_id) {
        // Process path in reverse (from leaf's parent up to root)
        for (auto it = path.rbegin(); it != path.rend(); ++it) {
            node_id_t node_id = *it;
            PageId page_id{filename, node_id};
            Page &page = buffer_pool.get_mut_page(page_id);
            internal_t node(page, node_id); // load internal node from buffer
            buffer_pool.mark_dirty(page_id);

            // Find the position where key should be inserted
            uint16_t index = node.child_slot(key);

            // If there's room in the node, insert and we're done
            if (node.header->size < internal_t::internal_capacity) {
                // Shift existing keys and children to make room
                std::memmove(node.keys + index + 1, node.keys + index,
                             (node.header->size - index) * sizeof(key_type));
                std::memmove(node.children + index + 2, node.children + index + 1,
                             (node.header->size - index) * sizeof(node_id_t));

                // Insert new key and child
                node.keys[index] = key;
                node.children[index + 1] = child_id;
                ++node.header->size;
                return;
            }

            // Node is full, need to split it
            // std::cout << "Before internal split: " << std::endl;
            // print();

            // Save original size
            uint16_t original_size = node.header->size;

            node_id_t new_node_id = num_pages.fetch_add(1);
            PageId new_page_id{filename, new_node_id};

            Page &new_page = buffer_pool.get_mut_page(new_page_id);
            internal_t new_node(new_page, new_node_id);
            buffer_pool.mark_dirty(new_page_id);

            // Prepare split position
            uint16_t split_pos = SPLIT_INTERNAL_POS; // the key at split_pos will be propagated up to parent node
            new_node.header->id = new_node_id;

            // update node sizes
            new_node.header->size = internal_t::internal_capacity - split_pos - 1; // new node get latter half keys
            node.header->size = split_pos; // original node get first half keys

            // Handle the split based on where the new key goes
            if (index < split_pos) {
                // New key goes in left node

                // Copy keys and children to new node from (split_pos + 1)
                std::memcpy(new_node.keys, node.keys + split_pos + 1,
                            new_node.header->size * sizeof(key_type));
                std::memcpy(new_node.children, node.children + split_pos + 1,
                            (new_node.header->size + 1) * sizeof(node_id_t));

                // Shift to make room for new key in original node
                std::memmove(node.keys + index + 1, node.keys + index,
                             (split_pos - index) * sizeof(key_type));
                std::memmove(node.children + index + 2, node.children + index + 1,
                             (split_pos - index) * sizeof(node_id_t));

                // Insert new key and child
                node.keys[index] = key;
                node.children[index + 1] = child_id;
                ++node.header->size;

                // Key to promote to parent
                key = node.keys[split_pos];
            } else if (index == split_pos) {
                // New key becomes the separator/promoted key

                // Copy keys and children to new node
                std::memcpy(new_node.keys, node.keys + split_pos + 1,
                            new_node.header->size * sizeof(key_type));
                std::memcpy(new_node.children + 1, node.children + split_pos + 1,
                            new_node.header->size * sizeof(node_id_t));

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
                ++new_node.header->size;
                key = node.keys[split_pos];
            }

            // Continue upward with new key and right node
            child_id = new_node_id;
        }

        // If we've processed the entire path and still have a key to insert,
        // we need to create a new root
        create_new_root(key, child_id);

        // std::cout << "After internal split: " << std::endl;
        // print();
        // std::cout << std::endl;
    }
    void create_new_root(const key_type &key, node_id_t right_child_id) {
        node_id_t left_child_id = num_pages.fetch_add(1);

        // Get current root
        PageId root_pid{filename, root_id};
        Page &old_root_page = buffer_pool.get_mut_page(root_pid);
        internal_t old_root(old_root_page, root_id);

        // Create new left child by copying current root
        PageId left_pid{filename, left_child_id};
        Page &left_page = buffer_pool.get_mut_page(left_pid);
        internal_t left_child(left_page, left_child_id);
        buffer_pool.mark_dirty(left_pid);

        // Copy contents of old root to left child
        left_child.copyInfoFrom(old_root);
        left_child.header->id = left_child_id;

        if (old_root.base_header->type == bp_node_type::INTERNAL) {
            // Copy keys and children
            std::memcpy(left_child.keys, old_root.keys,
                        old_root.header->size * sizeof(key_type));
            std::memcpy(left_child.children, old_root.children,
                        (old_root.header->size + 1) * sizeof(node_id_t));
        } else {
            throw std::logic_error("create_new_root: root was a leaf, which is unsupported in this configuration.");
        }

        // Update old root to become a new root
        buffer_pool.mark_dirty(root_pid);
        old_root.header->size = 1;
        old_root.keys[0] = key;
        old_root.children[0] = left_child_id;
        old_root.children[1] = right_child_id;

        // Increase tree height
        height++;
    }
    
    // Print a node and its children recursively (for debugging)
    // void print_node(node_id_t node_id, int level, bool show_all_values) const {
    //     for (int i = 0; i < level; i++) {
    //         std::cout << "  ";
    //     }
    //
    //     PageId pid{filename, node_id};
    //     Page page = buffer_pool.get_page(pid);
    //     uint8_t* raw = page.data();
    //     auto* base = reinterpret_cast<BaseHeader*>(raw);
    //
    //     if (base->type == bp_node_type::LEAF) {
    //         leaf_t node(raw, td, key_index);  // load leaf
    //
    //         std::cout << "Leaf ID " << node_id << ": [";
    //
    //         if (show_all_values) {
    //             for (uint16_t i = 0; i < node.info->size; i++) {
    //                 std::optional<Tuple> t = node.get_by_index(i);
    //                 if (t.has_value()) {
    //                     std::cout << node.extract_key(*t);
    //                     if (i < node.get_size() - 1) std::cout << ", ";
    //                 }
    //             }
    //         } else {
    //             if (node.get_size() > 0) {
    //                 std::cout << "min=" << node.min_key();
    //
    //                 if (node.get_size() > 1) {
    //                     std::cout << ", max=" << node.max_key();
    //                     std::cout << ", size=" << node.get_size();
    //                 }
    //             } else {
    //                 std::cout << "empty";
    //             }
    //         }
    //
    //         std::cout << "]" << std::endl;
    //     } else {
    //         internal_t node(raw, node_id);
    //         std::cout << "Internal ID " << node_id << ": [";
    //
    //         if (show_all_values) {
    //             for (uint16_t i = 0; i < node.header->size; i++) {
    //                 std::cout << node.keys[i];
    //                 if (i < node.header->size - 1) std::cout << ", ";
    //             }
    //         } else {
    //             if (node.header->size > 0) {
    //                 std::cout << "min=" << node.keys[0];
    //                 if (node.header->size > 1) {
    //                     std::cout << ", max=" << node.keys[node.header->size - 1];
    //                     std::cout << ", size=" << node.header->size;
    //                 }
    //             } else {
    //                 std::cout << "empty";
    //             }
    //         }
    //
    //         std::cout << "]" << std::endl;
    //
    //         for (uint16_t i = 0; i <= node.header->size; i++) {
    //             print_node(node.children[i], level + 1, show_all_values);
    //         }
    //     }
    // }
};

#endif //SIMPLEBPLUSTREE_HPP