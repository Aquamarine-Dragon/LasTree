#pragma once

#include <cstdint>
#include <vector>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <queue>
#include <functional>
#include <iostream>
#include <memory>
#include <limits>
#include <optional>

#include "NodeTypes.hpp"
#include "BTreeNode.hpp"
#include "MemoryBlockManager.hpp"

// OptimizedBTree: B+tree optimized for near-sorted data insertion
template<typename key_type, typename value_type>
class OptimizedBTree {
public:
    // Type aliases for readability
    using node_id_t = uint32_t;
    using BlockManager = InMemoryBlockManager<node_id_t>;
    using node_t = BTreeNode<node_id_t, key_type, value_type, BlockManager::block_size>;
    using path_t = std::vector<node_id_t>;

    // Constants
    static constexpr const char *name = "OptimizedBTree";
    static constexpr bool concurrent = true;
    static constexpr uint16_t SPLIT_INTERNAL_POS = node_t::internal_capacity / 2;
    // static constexpr uint16_t SPLIT_LEAF_POS = (node_t::leaf_capacity + 1) / 2;
    static constexpr uint16_t SPLIT_LEAF_POS = node_t::leaf_capacity * 9 / 10;
    static constexpr node_id_t INVALID_NODE_ID = std::numeric_limits<node_id_t>::max();

    // Node policies for handling unsorted leaves
    enum SortPolicy { SORT_ON_SPLIT, SORT_ON_MOVE, ALWAYS_SORTED };

    // Constructor initializes the tree with an empty root
    explicit OptimizedBTree(BlockManager &manager, SortPolicy policy = SORT_ON_SPLIT)
        : block_manager(manager),
          root_id(manager.allocate()),
          head_id(manager.allocate()),
          sort_policy(ALWAYS_SORTED), // todo: test on sorted (policy)
          height(1),
          size(0),
          stop_background_thread(true) {
        // todo : test on background (false)

        // initialize fast path tracking
        fast_path_min_key = std::numeric_limits<key_type>::min();
        fast_path_max_key = std::numeric_limits<key_type>::max();
        fast_path_leaf_id = head_id;

        // initialize leaf node
        node_t leaf(manager.open_block(head_id), bp_node_type::LEAF);
        manager.mark_dirty(head_id);
        leaf.info->id = head_id;
        leaf.info->next_id = INVALID_NODE_ID;
        leaf.info->size = 0;

        // Initialize root node (internal node pointing to the leaf)
        node_t root(manager.open_block(root_id), bp_node_type::INTERNAL);
        manager.mark_dirty(root_id);
        root.info->id = root_id;
        // root.info->next_id = INVALID_NODE_ID;
        root.info->size = 0;
        root.children[0] = head_id;

        // todo: Start background thread for sorting cold nodes
        // background_sort_thread = std::thread(&OptimizedBTree::background_sort_worker, this);
    }

    // Destructor ensures background thread is properly terminated
    ~OptimizedBTree() { {
            std::unique_lock<std::timed_mutex> lock(tree_mutex);
            stop_background_thread = true;
        }
        cold_nodes_cv.notify_one();
        if (background_sort_thread.joinable()) {
            background_sort_thread.join();
        }
    }

    // Insert a key-value pair into the tree
    void insert(const key_type &key, const value_type &value) {
        // std::cout << "inserting " << key << std::endl;
        std::unique_lock<std::timed_mutex> lock(tree_mutex);

        // try fast path insertion if key is in the current fast path range
        if (can_use_fast_path(key)) {
            // std::cout << "using fast path" << std::endl;
            insert_fast_path(key, value);
            return;
        }

        // Regular path insertion
        node_t leaf;
        path_t path;
        key_type leaf_max;

        // Find the appropriate leaf and collect path information
        find_leaf(leaf, path, key, leaf_max);

        // Update fast path to this leaf for future insertions
        fast_path_leaf_id = leaf.info->id;
        if (leaf.info->size > 0) {
            fast_path_min_key = leaf.keys[0];
        } else {
            // empty leaf
            fast_path_min_key = key;
        }
        fast_path_max_key = leaf_max;

        // Insert based on the sort policy
        if (sort_policy == ALWAYS_SORTED) {
            leaf.insert_sorted(key, value);
        } else {
            leaf.append(key, value);
        }
        block_manager.mark_dirty(leaf.info->id);

        // Check if leaf needs to be split
        if (leaf.is_nearly_full()) {
            if (sort_policy == SORT_ON_SPLIT) {
                leaf.sort();
            }

            // Find split position
            uint16_t index = leaf.value_slot(key);
            if (index < SPLIT_LEAF_POS) {
                node_id_t new_id;
                split_leaf(leaf, index, key, value, fast_path_max_key, new_id);
                internal_insert(path, fast_path_max_key, new_id);
            } else {
                split_leaf(leaf, index, key, value, fast_path_min_key, fast_path_leaf_id);
                internal_insert(path, fast_path_min_key, fast_path_leaf_id);
            }
        }
        size++;
    }

    // Update an existing key with a new value
    bool update(const key_type &key, const value_type &value) {
        std::unique_lock<std::timed_mutex> lock(tree_mutex);

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
    std::optional<value_type> get(const key_type &key) const {
        std::unique_lock<std::timed_mutex> lock(tree_mutex);

        node_t leaf;
        find_leaf(leaf, key);

        uint16_t index = leaf.value_slot(key);
        if (index < leaf.info->size && leaf.keys[index] == key) {
            return std::make_optional(leaf.values[index]);
        }

        return std::nullopt;
    }

    // Check if a key exists in the tree
    bool contains(const key_type &key) const {
        std::unique_lock<std::timed_mutex> lock(tree_mutex);

        node_t leaf;
        find_leaf(leaf, key);

        uint16_t index = leaf.value_slot(key);
        return index < leaf.info->size && leaf.keys[index] == key;
    }

    // Print the tree structure (for debugging)
    void print(bool show_all_values = false) const {
        // todo lock not working?
        // std::unique_lock<std::mutex> lock(tree_mutex);

        std::cout << "B+Tree Structure:" << std::endl;
        print_node(root_id, 0, show_all_values);

        std::cout << "\nLeaf Node Chain:" << std::endl;
        node_id_t curr_id = head_id;
        while (curr_id != INVALID_NODE_ID) {
            node_t node(block_manager.open_block(curr_id));

            std::cout << "Leaf ID " << curr_id << ": [";

            if (show_all_values) {
                // display all values
                for (uint16_t i = 0; i < node.info->size; i++) {
                    std::cout << "(" << node.keys[i] << ":" << node.values[i] << ")";
                    if (i < node.info->size - 1) std::cout << ", ";
                }
            } else {
                if (node.info->size > 0) {
                    std::cout << "min=(" << node.keys[0] << ":" << node.values[0] << ")";

                    if (node.info->size > 1) {
                        std::cout << ", max=(" << node.keys[node.info->size - 1]
                                << ":" << node.values[node.info->size - 1] << ")";
                        std::cout << ", size=" << node.info->size;
                    }
                } else {
                    std::cout << "empty";
                }
            }

            std::cout << "] ";

            // extra info
            std::cout << (node.info->isSorted ? "sorted" : "unsorted");
            if (curr_id == fast_path_leaf_id) std::cout << " (fast path)";
            std::cout << std::endl;

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

    // Get the fast path hit count (for performance analysis)
    size_t get_fast_path_hits() const {
        return fast_path_hits;
    }

private:
    // Memory manager for allocating and tracking node blocks
    BlockManager &block_manager;

    // Tree structure identifiers
    node_id_t root_id;
    node_id_t head_id;

    // Fast path tracking
    node_id_t fast_path_leaf_id;
    key_type fast_path_min_key;
    key_type fast_path_max_key;

    // Policy for handling unsorted leaves
    SortPolicy sort_policy;

    // Tree metrics
    uint8_t height;
    size_t size;
    size_t fast_path_hits = 0;

    // Thread for background sorting of cold nodes
    std::thread background_sort_thread;
    mutable std::timed_mutex tree_mutex;
    std::condition_variable cold_nodes_cv;
    std::queue<node_id_t> cold_nodes_queue;
    bool stop_background_thread;

    // Check if a key can use the fast path for insertion
    bool can_use_fast_path(const key_type &key) const {
        if (fast_path_leaf_id == INVALID_NODE_ID) {
            return false;
        }
        return key >= fast_path_min_key && key < fast_path_max_key;
    }

    // Insert using the fast path (O(1) insertion for sequential data)
    void insert_fast_path(const key_type &key, const value_type &value) {
        node_t leaf(block_manager.open_block(fast_path_leaf_id));
        block_manager.mark_dirty(fast_path_leaf_id);

        if (sort_policy == ALWAYS_SORTED) {
            if (leaf.insert_sorted(key, value)) {
                fast_path_hits++;
                size++;
                return;
            }
        } else {
            if (leaf.append(key, value)) {
                fast_path_hits++;
                size++;
                return;
            }
        }

        // If we couldn't insert, we need to split the leaf
        // std::cout << "Before leaf split: " << std::endl;
        // print();
        path_t path;
        key_type leaf_max;
        find_path_to_node(path, key, leaf_max);

        if (sort_policy == SORT_ON_SPLIT) {
            leaf.sort();
        }

        uint16_t index = leaf.value_slot(key);
        if (index < SPLIT_LEAF_POS) {// insert into old leaf
            node_id_t new_id;
            split_leaf(leaf, index, key, value, fast_path_max_key, new_id);
            internal_insert(path, fast_path_max_key, new_id);
            // std::cout << "Inserting: " << key << " to old leaf" << std::endl;
        } else { // insert into new leaf
            split_leaf(leaf, index, key, value, fast_path_min_key, fast_path_leaf_id);
            internal_insert(path, fast_path_min_key, fast_path_leaf_id);
            // std::cout << "Inserting: " << key << " to new leaf" << std::endl;
        }
        // todo
        // std::cout << "After leaf split: " << std::endl;
        // print();
        // std::cout << std::endl;

        size++;
    }

    // Find the leaf node that should contain the given key
    void find_leaf(node_t &node, const key_type &key) const {
        node_id_t node_id = root_id;
        node.load(block_manager.open_block(node_id));

        // Traverse the tree from root to leaf
        while (node.info->type == bp_node_type::INTERNAL) {
            uint16_t slot = node.child_slot(key);
            node_id = node.children[slot];
            node.load(block_manager.open_block(node_id));
        }
    }

    // Find the leaf and collect the path from root to leaf
    void find_leaf(node_t &node, path_t &path, const key_type &key, key_type &leaf_max) const {
        // initialize leaf max value as max possible value
        leaf_max = std::numeric_limits<key_type>::max();
        node_id_t node_id = root_id;
        path.reserve(height);

        node.load(block_manager.open_block(node_id));

        // Traverse the tree and build path
        while (node.info->type == bp_node_type::INTERNAL) {
            path.push_back(node_id);
            uint16_t slot = node.child_slot(key);
            node_id = node.children[slot];

            // Record the upper bound key for this slot
            if (slot < node.info->size) {
                leaf_max = node.keys[slot];
            }

            node.load(block_manager.open_block(node_id));
        }
    }

    void find_path_to_node(path_t &path, const key_type &key, key_type &leaf_max) const {
        // std::cout << "target: " << target_id << ", leaf max: " << leaf_max << std::endl;
        leaf_max = std::numeric_limits<key_type>::max();
        path.reserve(height);

        node_id_t node_id = root_id;
        node_t node(block_manager.open_block(node_id));

        while (node.info->type == bp_node_type::INTERNAL) {
            path.push_back(node_id);

            uint16_t slot = node.child_slot(key);
            node_id = node.children[slot];

            if (slot < node.info->size) {
                leaf_max = node.keys[slot];
            }

            node.load(block_manager.open_block(node_id));
        }

    }

    // Split a leaf node during insertion
    void split_leaf(node_t &leaf, uint16_t index, const key_type &key,
                    const value_type &value, key_type &new_key, node_id_t &new_id) {

        // std::cout << "Splitting leaf at index " << index << " with key " << key << std::endl;

        block_manager.mark_dirty(leaf.info->id);

        // Create a new leaf node
        node_id_t new_leaf_id = block_manager.allocate();
        node_t new_leaf(block_manager.open_block(new_leaf_id), bp_node_type::LEAF);
        block_manager.mark_dirty(new_leaf_id);

        if (sort_policy == ALWAYS_SORTED) {

            // Traditional split with full sorting
            uint16_t split_pos = SPLIT_LEAF_POS;

            new_leaf.info->id = new_leaf_id;
            new_leaf.info->next_id = leaf.info->next_id;
            new_leaf.info->size = node_t::leaf_capacity + 1 - split_pos;
            leaf.info->next_id = new_leaf_id;
            leaf.info->size = split_pos;

            // Handle the split based on where the new key goes
            if (index < leaf.info->size) {
                // New key goes in the original leaf
                std::memcpy(new_leaf.keys, leaf.keys + leaf.info->size - 1,
                            new_leaf.info->size * sizeof(key_type));
                std::memmove(leaf.keys + index + 1, leaf.keys + index,
                            (leaf.info->size - index - 1) * sizeof(key_type));
                leaf.keys[index] = key;

                std::memcpy(new_leaf.values, leaf.values + leaf.info->size - 1,
                            new_leaf.info->size * sizeof(value_type));
                std::memmove(leaf.values + index + 1, leaf.values + index,
                            (leaf.info->size - index - 1) * sizeof(value_type));
                leaf.values[index] = value;
            } else {
                // New key goes in the new leaf
                uint16_t new_index = index - leaf.info->size;

                std::memcpy(new_leaf.keys, leaf.keys + leaf.info->size,
                            new_index * sizeof(key_type));
                new_leaf.keys[new_index] = key;
                std::memcpy(new_leaf.keys + new_index + 1, leaf.keys + index,
                            (node_t::leaf_capacity - index) * sizeof(key_type));

                std::memcpy(new_leaf.values, leaf.values + leaf.info->size,
                            new_index * sizeof(value_type));
                new_leaf.values[new_index] = value;
                std::memcpy(new_leaf.values + new_index + 1, leaf.values + index,
                            (node_t::leaf_capacity - index) * sizeof(value_type));
            }
        } else {
            // Use quick partition for faster splitting
            int partition_pos = leaf.quick_partition();
            new_leaf.info->id = new_leaf_id;
            new_leaf.info->next_id = leaf.info->next_id;
            leaf.info->next_id = new_leaf_id;

            // Create temporary vector for the right partition plus the new key-value
            std::vector<std::pair<key_type, value_type> > right_partition;
            right_partition.reserve(node_t::leaf_capacity - partition_pos + 1);

            // Insert the new key-value at the right position in the right partition
            bool inserted = false;
            for (uint16_t i = partition_pos; i < leaf.info->size; i++) {
                if (!inserted && key < leaf.keys[i]) {
                    right_partition.push_back(std::make_pair(key, value));
                    inserted = true;
                }
                right_partition.push_back(std::make_pair(leaf.keys[i], leaf.values[i]));
            }

            // If not inserted yet, it goes at the end
            if (!inserted) {
                right_partition.push_back(std::make_pair(key, value));
            }

            // Update the new leaf with the right partition
            new_leaf.info->size = right_partition.size();
            for (uint16_t i = 0; i < right_partition.size(); i++) {
                new_leaf.keys[i] = right_partition[i].first;
                new_leaf.values[i] = right_partition[i].second;
            }

            // If we're splitting the last leaf, update the fast path
            if (fast_path_leaf_id == leaf.info->id) {
                if (sort_policy == SORT_ON_MOVE) {
                    leaf.mark_as_cold();

                    // Add it to the cold nodes queue for background sorting
                    cold_nodes_queue.push(leaf.info->id);
                    cold_nodes_cv.notify_one();
                }

                fast_path_leaf_id = new_leaf_id;
            }
        }

        new_key = new_leaf.keys[0];
        new_id = new_leaf_id;
    }

    // Insert a key and child into internal nodes along the path
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

            // Node is full, need to split it
            // std::cout << "Before internal split: " << std::endl;
            // print();

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

        // If we've processed the entire path and still have a key to insert,
        // we need to create a new root
        create_new_root(key, child_id);

        // std::cout << "After internal split: " << std::endl;
        // print();
        // std::cout << std::endl;
    }

    // Create a new root when the height of the tree increases
    // key: key that we want to elect to new node
    // right_child_id: the right sub tree id of this key
    void create_new_root(const key_type &key, node_id_t right_child_id) {
        node_id_t left_child_id = block_manager.allocate();

        // Get current root
        node_t old_root(block_manager.open_block(root_id));

        // Create new left child by copying current root
        node_t left_child(block_manager.open_block(left_child_id), static_cast<bp_node_type>(old_root.info->type));
        block_manager.mark_dirty(left_child_id);

        // Copy contents of old root to left child
        // std::memcpy(left_child.info, old_root.info, sizeof(typename node_t::node_info));
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

        // Convert old root to internal node (if it was a leaf)
        if (old_root.info->type == bp_node_type::LEAF) {
            old_root.load(block_manager.open_block(root_id));
            old_root.info->type = bp_node_type::INTERNAL;
        }

        // Update old root to become a new root
        block_manager.mark_dirty(root_id);
        old_root.info->size = 1;
        old_root.keys[0] = key;
        old_root.children[0] = left_child_id;
        old_root.children[1] = right_child_id;

        // Increase tree height
        height++;
    }

    // Worker method for background thread that sorts cold nodes
    void background_sort_worker() {
        while (true) {
            node_id_t node_id_to_sort;
            bool has_work = false; {
                std::unique_lock<std::timed_mutex> lock(tree_mutex, std::chrono::milliseconds(100));
                if (!lock.owns_lock()) {
                    std::cout << "background_sort_worker: failed to acquire lock" << std::endl;
                    continue;
                }

                if (stop_background_thread) {
                    break;
                }

                if (!cold_nodes_queue.empty()) {
                    node_id_to_sort = cold_nodes_queue.front();
                    cold_nodes_queue.pop();
                    has_work = true;
                }
            }

            // Sort the cold node in the background
            if (has_work) {
                try {
                    node_t node(block_manager.open_block(node_id_to_sort));
                    if (!node.info->isSorted) {
                        block_manager.mark_dirty(node_id_to_sort);
                        node.sort();
                    }
                } catch (const std::exception &e) {
                    std::cerr << "Error in background sort: " << e.what() << std::endl;
                }
            } else {
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            }
        }
    }

    // Print a node and its children recursively (for debugging)
    void print_node(node_id_t node_id, int level, bool show_all_values) const {
        for (int i = 0; i < level; i++) {
            std::cout << "  ";
        }

        node_t node(block_manager.open_block(node_id));

        if (node.info->type == bp_node_type::LEAF) {
            std::cout << "Leaf ID " << node_id << ": [";

            if (show_all_values) {
                for (uint16_t i = 0; i < node.info->size; i++) {
                    std::cout << node.keys[i];
                    if (i < node.info->size - 1) std::cout << ", ";
                }
            } else {
                if (node.info->size > 0) {
                    std::cout << "min=" << node.keys[0];

                    if (node.info->size > 1) {
                        std::cout << ", max=" << node.keys[node.info->size - 1];
                        std::cout << ", size=" << node.info->size;
                    }
                } else {
                    std::cout << "empty";
                }
            }

            std::cout << "]" << std::endl;
        } else {
            std::cout << "Internal ID " << node_id << ": [";

            if (show_all_values) {
                for (uint16_t i = 0; i < node.info->size; i++) {
                    std::cout << node.keys[i];
                    if (i < node.info->size - 1) std::cout << ", ";
                }
            } else {
                for (uint16_t i = 0; i < node.info->size; i++) {
                    std::cout << node.keys[i];
                    if (i < node.info->size - 1) std::cout << ", ";
                }
                // if (node.info->size > 0) {
                //     std::cout << "min=" << node.keys[0];
                //
                //     if (node.info->size > 1) {
                //         std::cout << ", max=" << node.keys[node.info->size - 1];
                //         std::cout << ", size=" << node.info->size;
                //     }
                // } else {
                //     std::cout << "empty";
                // }
            }

            std::cout << "]" << std::endl;

            for (uint16_t i = 0; i <= node.info->size; i++) {
                print_node(node.children[i], level + 1, show_all_values);
            }
        }
    }
};
