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
#include "LeafNode.hpp"
#include "Types.hpp"
#include <BaseFile.hpp>
#include <Database.hpp>
#include <InternalNode.hpp>

using namespace db;

// SimpleBPlusTree - Basic B+tree implementation using memory blocks
template<typename key_type, size_t split_percentage>
class SimpleBPlusTree : public BaseFile {
public:
    // Type aliases for readability
    using node_id_t = uint32_t;
    using leaf_t = LeafNode<node_id_t, key_type, split_percentage,  db::DEFAULT_PAGE_SIZE>;
    using internal_t = InternalNode<node_id_t, key_type, db::DEFAULT_PAGE_SIZE>;
    // using buffer_pool_t = BufferPoolType<64>;
    using path_t = std::vector<node_id_t>;

    // Constants
    static constexpr const char *name = "SimpleBPlusTree";
    static constexpr bool concurrent = false;
    static constexpr uint16_t SPLIT_INTERNAL_POS = internal_t::internal_capacity / 2;
    static constexpr node_id_t INVALID_NODE_ID = std::numeric_limits<node_id_t>::max();

    explicit SimpleBPlusTree(
        const std::string &filename,
        const TupleDesc &td,
        size_t key_index)
        : BaseFile(filename),
          td(td),
          key_index(key_index),
          root_id(INVALID_NODE_ID),
          head_id(INVALID_NODE_ID) {
    }

    void init() override {
        BufferPool &buffer_pool = getDatabase().getBufferPool();

        // Allocate leaf node
        head_id = num_pages.fetch_add(1);
        PageId leaf_pid{filename, head_id};
        Page &leaf_page = buffer_pool.get_mut_page(leaf_pid);
        leaf_t leaf(leaf_page, td, key_index, head_id, INVALID_NODE_ID, SplitPolicy::SORT, /*isCold=*/false);
        buffer_pool.mark_dirty(leaf_pid);
        buffer_pool.unpin_page(leaf_pid);

        // Allocate root node
        root_id = num_pages.fetch_add(1);
        PageId root_pid{filename, root_id};
        Page &root_page = buffer_pool.get_mut_page(root_pid);
        internal_t root(root_page, root_id);
        buffer_pool.mark_dirty(root_pid);
        root.header->size = 0;
        root.children[0] = head_id;
        buffer_pool.unpin_page(root_pid);
    }

    // Insert a key-value pair into the tree
    void insert(const Tuple &tuple) override {
        // Find the leaf node where the key belongs
        BufferPool &buffer_pool = getDatabase().getBufferPool();
        path_t path;
        key_type key = std::get<key_type>(tuple.get_field(key_index));

        // std::cout << "inserting " << key << std::endl;

        node_id_t leaf_id = find_leaf(path, key);
        PageId leaf_pid{filename, leaf_id};
        Page &leaf_page = buffer_pool.get_mut_page(leaf_pid);
        leaf_t leaf(leaf_page, td, key_index);

        // Try to insert in the leaf
        if (leaf.insert(tuple)) {
            buffer_pool.mark_dirty(leaf_pid);
            size++;
            buffer_pool.unpin_page(leaf_pid);
            return;
        }
        buffer_pool.unpin_page(leaf_pid);
        insert_into_leaf(leaf_pid, tuple, path);
        size++;
    }

    std::optional<db::Tuple> get(const field_t &key) override {
        // Find leaf containing key
        BufferPool &buffer_pool = getDatabase().getBufferPool();
        key_type actual_key = std::get<key_type>(key);
        path_t path;
        node_id_t leaf_id = find_leaf(path, actual_key);
        PageId page_id{filename, leaf_id};
        Page &page = buffer_pool.get_mut_page(page_id);
        leaf_t leaf(page, td, key_index);
        if (leaf.is_sorted()) {
            ++sorted_leaf_search;
        }
        std::optional<Tuple> opt_tuple = leaf.get(actual_key);
        buffer_pool.unpin_page(page_id);
        return opt_tuple;
    }

    std::vector<Tuple> range(const field_t &min_key, const field_t &max_key) override {
        BufferPool &buffer_pool = getDatabase().getBufferPool();
        path_t path;
        std::vector<Tuple> result;

        key_type actual_min_key = std::get<key_type>(min_key);
        key_type actual_max_key = std::get<key_type>(max_key);
        node_id_t leaf_id = find_leaf(path, actual_min_key);
        while (leaf_id != INVALID_NODE_ID) {
            PageId page_id{filename, leaf_id};
            Page &page = buffer_pool.get_mut_page(page_id);
            leaf_t leaf(page, td, key_index);
            std::vector<Tuple> rangeTuples = leaf.get_range(actual_min_key, actual_max_key);
            if (rangeTuples.empty()) {
                buffer_pool.unpin_page(page_id);
                return result;
            }
            result.insert(result.end(), rangeTuples.begin(), rangeTuples.end());

            buffer_pool.unpin_page(page_id);
            leaf_id = leaf.page_header->meta.next_id;
        }
        return result;
    }



    // Get the number of elements in the tree
    size_t get_size() const {
        return size;
    }

    // Get the height of the tree
    uint8_t get_height() const {
        return height;
    }

    std::pair<size_t, double> get_leaf_stats() const {
        size_t leaf_count = 0;
        size_t total_used = 0;
        size_t total_available = 0;

        node_id_t curr = head_id;
        auto &buffer_pool = getDatabase().getBufferPool();

        while (curr != INVALID_NODE_ID) {
            PageId pid{filename, curr};
            Page &page = buffer_pool.get_mut_page(pid);
            leaf_t leaf(page, td, key_index);

            ++leaf_count;
            total_used += leaf.used_space();
            total_available += leaf_t::available_space;

            curr = leaf.page_header->meta.next_id;
            buffer_pool.unpin_page(pid);
        }

        double utilization = (total_available > 0) ? (double)total_used / total_available : 0.0;
        return {leaf_count, utilization};
    }

    size_t get_sorted_leaf_search() const {
        return sorted_leaf_search;
    }

private:
    // buffer_pool_t &buffer_pool;
    const TupleDesc &td;
    size_t key_index;

    // Tree structure identifiers
    std::atomic<node_id_t> num_pages{0};
    node_id_t root_id;
    node_id_t head_id;

    // Tree metrics
    uint8_t height = 1;
    size_t size = 0;
    size_t sorted_leaf_search = 0;

    // Find the leaf and collect the path from root to leaf
    node_id_t find_leaf(path_t &path, const key_type &key) const {
        auto &buffer_pool = getDatabase().getBufferPool();
        path.reserve(height);

        node_id_t node_id = root_id;

        while (true) {
            PageId pid{filename, node_id};
            Page &page = buffer_pool.get_mut_page(pid);

            auto *base = reinterpret_cast<BaseHeader *>(page.data());

            if (base->type == bp_node_type::LEAF) {
                buffer_pool.unpin_page(pid);
                break;
            }

            internal_t node(page);
            path.push_back(node_id);

            uint16_t slot = node.child_slot(key);
            node_id = node.children[slot];
            buffer_pool.unpin_page(pid);
        }
        return node_id;
    }

    /**
     * insert tuple into leaf by path (splits required)
     */
    void insert_into_leaf(const PageId &pid, const Tuple &t, const path_t &path) {
        BufferPool &buffer_pool = getDatabase().getBufferPool();
        Page &page = buffer_pool.get_mut_page(pid);
        leaf_t leaf(page, td, key_index);

        // split
        node_id_t new_leaf_id = num_pages.fetch_add(1);
        PageId new_leaf_pid{filename, new_leaf_id};
        Page &new_leaf_page = buffer_pool.get_mut_page(new_leaf_pid);
        leaf_t new_leaf(new_leaf_page, td, key_index, new_leaf_id, INVALID_NODE_ID, SplitPolicy::SORT, false);

        key_type split_key = leaf.split_into(new_leaf);
        buffer_pool.mark_dirty(pid);
        buffer_pool.mark_dirty(new_leaf_pid);

        // Insert again (must succeed)
        key_type key = std::get<key_type>(t.get_field(key_index));
        if (key < split_key) {
            leaf.insert(t);
        }else {
            new_leaf.insert(t);
        }
        buffer_pool.unpin_page(pid);
        buffer_pool.unpin_page(new_leaf_pid);

        internal_insert(path, split_key, new_leaf_id);
    }

    // Insert a key and child into internal nodes along the path
    void internal_insert(const path_t &path, key_type key, node_id_t child_id) {
        BufferPool &buffer_pool = getDatabase().getBufferPool();
        // Process path in reverse (from leaf's parent up to root)
        for (auto it = path.rbegin(); it != path.rend(); ++it) {
            node_id_t node_id = *it;
            PageId page_id{filename, node_id};
            Page &page = buffer_pool.get_mut_page(page_id);
            internal_t node(page); // load internal node from buffer
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

                buffer_pool.unpin_page(page_id);
                return;
            }
            // Save original size
            uint16_t original_size = node.header->size;

            node_id_t new_node_id = num_pages.fetch_add(1);
            PageId new_page_id{filename, new_node_id};

            Page &new_page = buffer_pool.get_mut_page(new_page_id);
            internal_t new_node(new_page, new_node_id);
            buffer_pool.mark_dirty(new_page_id);

            // Prepare split position
            uint16_t split_pos = SPLIT_INTERNAL_POS; // the key at split_pos will be propagated up to parent node

            // update node sizes
            node.header->size = split_pos;
            new_node.header->size = internal_t::internal_capacity - node.header->size; // new node get latter half keys

            // Handle the split based on where the new key goes
            if (index < split_pos) {
                // New key goes in left node

                // Copy keys and children to new node from (split_pos + 1)
                std::memcpy(new_node.keys, node.keys + split_pos,
                            new_node.header->size * sizeof(key_type));
                std::memcpy(new_node.children, node.children + split_pos,
                            (new_node.header->size + 1) * sizeof(node_id_t));

                // Shift to make room for new key in original node
                std::memmove(node.keys + index + 1, node.keys + index,
                             (split_pos - index) * sizeof(key_type));
                std::memmove(node.children + index + 2, node.children + index + 1,
                             (split_pos - index + 1) * sizeof(node_id_t));

                // Insert new key and child
                node.keys[index] = key;
                node.children[index + 1] = child_id;

                // Key to promote to parent
                key = node.keys[split_pos];
            } else if (index == split_pos) {
                // New key becomes the separator/promoted key

                // Copy keys and children to new node
                std::memcpy(new_node.keys, node.keys + split_pos,
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
                key = node.keys[split_pos];
            }

            // Continue upward with new key and right node
            child_id = new_node_id;
            buffer_pool.unpin_page(page_id);
            buffer_pool.unpin_page(new_page_id);
        }

        // If we've processed the entire path and still have a key to insert,
        // we need to create a new root
        create_new_root(key, child_id);
    }

    void create_new_root(const key_type &key, node_id_t right_child_id) {
        BufferPool &buffer_pool = getDatabase().getBufferPool();

        // Get current root
        PageId root_pid{filename, root_id};
        Page &old_root_page = buffer_pool.get_mut_page(root_pid);
        internal_t old_root(old_root_page);

        // Create new left child by copying current root
        node_id_t left_child_id = num_pages.fetch_add(1);
        PageId left_pid{filename, left_child_id};
        Page &left_page = buffer_pool.get_mut_page(left_pid);
        internal_t left_child(left_page, left_child_id);
        buffer_pool.mark_dirty(left_pid);

        // Copy contents of old root to left child
        left_child.copyInfoFrom(old_root);

        // Copy keys and children
        std::memcpy(left_child.keys, old_root.keys,
                    old_root.header->size * sizeof(key_type));
        std::memcpy(left_child.children, old_root.children,
                    (old_root.header->size + 1) * sizeof(node_id_t));

        // Update old root to become a new root
        buffer_pool.mark_dirty(root_pid);
        old_root.header->size = 1;
        old_root.keys[0] = key;
        old_root.children[0] = left_child_id;
        old_root.children[1] = right_child_id;

        // Increase tree height
        height++;
        buffer_pool.unpin_page(root_pid);
        buffer_pool.unpin_page(left_pid);
    }
};

#endif //SIMPLEBPLUSTREE_HPP
