#pragma once

#include <cstdint>
#include <vector>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <queue>
#include <iostream>
#include <unistd.h>
#include <limits>
#include <optional>

#include "BaseFile.hpp"
#include "NodeTypes.hpp"
#include "BufferPool.hpp"
#include "InternalNode.hpp"
#include "Tuple.hpp"

using namespace db;

// OptimizedBTree: B+tree optimized for near-sorted data insertion
template<
    typename key_type,
    template <typename, typename, size_t> class LeafNodeType,
    typename buffer_pool_t>
class OptimizedBTree: public BaseFile {
public:
    // Type aliases for readability
    using BaseFile::BaseFile;
    using node_id_t = uint32_t;
    using leaf_t = LeafNodeType<node_id_t, key_type, DEFAULT_PAGE_SIZE>;
    using internal_t = InternalNode<node_id_t, key_type, DEFAULT_PAGE_SIZE>;
    // using buffer_pool_t = BufferPoolType<DEFAULT_PAGE_SIZE>;
    using path_t = std::vector<node_id_t>;

    // Constants
    static constexpr bool concurrent = true;
    static constexpr uint16_t SPLIT_INTERNAL_POS = internal_t::internal_capacity / 2;
    static constexpr node_id_t INVALID_NODE_ID = std::numeric_limits<node_id_t>::max();

    // Node policies for handling unsorted leaves
    enum SortPolicy { SORT_ON_SPLIT, SORT_ON_MOVE, ALWAYS_SORTED };

    // Constructor initializes the tree with an empty root
    explicit OptimizedBTree(buffer_pool_t &buffer_pool, SortPolicy policy = SORT_ON_SPLIT, size_t key_index = 0,
                            const std::string &name = "", const TupleDesc &td = {})
        : BaseFile(name),
        buffer_pool(buffer_pool),
          td(td),
          key_index(key_index),
          sort_policy(policy),
          height(1),
          size(0),
          stop_background_thread(true) {
        // todo : test on background (false)

        // initialize fast path tracking
        fast_path_min_key = std::numeric_limits<key_type>::min();
        fast_path_max_key = std::numeric_limits<key_type>::max();
        fast_path_leaf_id = head_id;

        // initialize leaf node
        head_id = numPages.fetch_add(1);
        PageId leaf_pid{filename, head_id};
        leaf_t leaf(buffer_pool.get_mut_page(leaf_pid), td, key_index, head_id, SplitPolicy::SORT, INVALID_NODE_ID,  false);
        buffer_pool.mark_dirty(leaf_pid);

        // Initialize root node (internal node pointing to the leaf)
        root_id = numPages.fetch_add(1);
        PageId root_pid{filename, root_id};
        internal_t root(buffer_pool.get_mut_page(root_pid), root_id);
        buffer_pool.mark_dirty(root_pid);
        root.header->size = 1;
        root.children[0] = head_id;

        // todo: Start background thread for sorting cold nodes
        // background_sort_thread = std::thread(&OptimizedBTree::background_sort_worker, this);
    }

    // Destructor ensures background thread is properly terminated
    ~OptimizedBTree() override { {
            std::unique_lock<std::timed_mutex> lock(tree_mutex);
            stop_background_thread = true;
        }
        close(fd);
        cold_nodes_cv.notify_one();
        if (background_sort_thread.joinable()) {
            background_sort_thread.join();
        }
    }

    // Insert a tuple into the tree
    void insert(const Tuple &tuple) {
        // std::cout << "inserting " << key << std::endl;
        std::unique_lock<std::timed_mutex> lock(tree_mutex);
        key_type key = std::get<key_type>(tuple.get_field(key_index));

        // try fast path insertion if key is in the current fast path range
        if (can_use_fast_path(key)) {
            // std::cout << "using fast path" << std::endl;
            insert_fast_path(tuple, key);
            return;
        }

        // Regular path insertion
        path_t path;
        key_type leaf_max;
        node_id_t leaf_id = find_leaf(path, key, leaf_max);

        PageId leaf_pid{filename, leaf_id};
        Page& page = buffer_pool.get_mut_page(leaf_pid);
        leaf_t leaf(page, td, key_index);

        if (leaf.insert(tuple)) {
            buffer_pool.mark_dirty(leaf_pid);
            fast_path_leaf_id = leaf_id;
            fast_path_min_key = leaf.min_key();
            fast_path_max_key = leaf_max;
            size++;
            return;
        }

        insert_into_leaf(leaf_pid, tuple, path);
        size++;
    }

    std::optional<db::Tuple> get(const key_type& key) {
        path_t _;
        key_type __;
        node_id_t leaf_id = find_leaf(_, key, __);

        PageId pid{filename, leaf_id};
        Page& page = buffer_pool.get_mut_page(pid);
        leaf_t leaf(page, td, key_index);

        return leaf.get(key);
    }


    // Update an existing key with a new value
    bool update(const key_type& key, const std::vector<std::pair<size_t, db::field_t>>& updates) {
        std::unique_lock<std::timed_mutex> lock(tree_mutex);

        path_t path;
        key_type _;
        node_id_t leaf_id = find_leaf(path, key, _);

        PageId leaf_pid{filename, leaf_id};
        Page& leaf_page = buffer_pool.get_mut_page(leaf_pid);
        leaf_t leaf(leaf_page, td, key_index);
        buffer_pool.mark_dirty(leaf_pid);

        auto opt = leaf.get(key);
        if (!opt.has_value()) return false;

        db::Tuple updated = opt.value();

        for (const auto& [idx, val] : updates) {
            updated.set_field(idx, val);
        }

        // 假设 leaf.update() 是支持替换的，否则你需要 remove + insert
        return leaf.update(updated);
    }


    // Check if a key exists in the tree
    bool contains(const key_type &key) const {
        std::unique_lock<std::timed_mutex> lock(tree_mutex);

        leaf_t leaf;
        find_leaf(leaf, key);

        return leaf.get(key) != std::nullopt;
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
            // fetch from buffer pool and construct
            Page &page = buffer_pool.get_mut_page({filename, curr_id});
            leaf_t node(page, td, key_index);

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
    buffer_pool_t buffer_pool;

    // Tree structure identifiers
    const TupleDesc td; // tuple schema of this page
    size_t key_index; // index of key in each tuple

    node_id_t root_id; // root id
    node_id_t head_id; // id of first leaf
    std::atomic<size_t> numPages{0}; // keep track of current pages

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
    void insert_fast_path(const Tuple& t, key_type key) {
        PageId fast_pid{filename, fast_path_leaf_id};
        Page& leaf_page = buffer_pool.get_mut_page(fast_pid);
        leaf_t leaf(leaf_page, td, key_index);

        if (leaf.insert(t)) {
            // different insert impl based on leaf type
            buffer_pool.mark_dirty(fast_pid);
            fast_path_hits++;
            size++;
            return;
        }

        // If we couldn't insert, we need to split the leaf
        // std::cout << "Before leaf split: " << std::endl;
        // print();
        path_t path;
        key_type leaf_max;
        find_path_to_node(path, key, leaf_max);

        fast_path_leaf_id = path.back();
        fast_path_max_key = leaf_max;

        insert_into_leaf(fast_pid, t, path);
        size++;

        // todo
        // std::cout << "After leaf split: " << std::endl;
        // print();
        // std::cout << std::endl;
    }

    /**
     * insert tuple into leaf by path (splits required)
     */
    void insert_into_leaf(const PageId& pid, const Tuple& t, const path_t& path) {
        Page& page = buffer_pool.get_mut_page(pid);
        leaf_t leaf(page, td, key_index);

        // split
        node_id_t new_leaf_id = numPages.fetch_add(1);
        PageId new_leaf_pid{filename, new_leaf_id};
        Page& new_leaf_page = buffer_pool.get_mut_page(new_leaf_pid);
        leaf_t new_leaf(new_leaf_page, td, key_index, new_leaf_id, SplitPolicy::SORT,INVALID_NODE_ID,  false);
        buffer_pool.mark_dirty(new_leaf_pid);

        auto [split_key, new_id] = leaf.split_into(new_leaf);
        buffer_pool.mark_dirty(pid);

        // Insert again (must succeed)
        new_leaf.insert(t);

        internal_insert(path, split_key, new_id);
    }


    // Find the leaf node that should contain the given key
    // void find_leaf(internal_t &node, const key_type &key) const {
    //     node_id_t node_id = root_id;
    //     while (true) {
    //         PageId pid{filename, node_id};
    //         Page &page = buffer_pool.get_mut_page(pid);
    //         node.load(page);
    //
    //         if (node.base_header->type != bp_node_type::INTERNAL) {
    //             break;  // reached leaf
    //         }
    //
    //         uint16_t slot = node.child_slot(key);
    //         node_id = node.children[slot];
    //     }
    // }

    // Only returns the leaf node id and path to it
    node_id_t find_leaf(path_t &path, const key_type &key, key_type &leaf_max) {
        leaf_max = std::numeric_limits<key_type>::max();
        path.reserve(height);

        node_id_t node_id = root_id;

        while (true) {
            path.push_back(node_id);

            PageId pid{filename, node_id};
            Page &page = buffer_pool.get_mut_page(pid);
            internal_t node(page);

            if (node.base_header->type != bp_node_type::INTERNAL) {
                break;
            }

            uint16_t slot = node.child_slot(key);
            node_id = node.children[slot];

            if (slot < node.header->size) {
                leaf_max = node.keys[slot];
            }
        }

        return node_id;
    }


    void find_path_to_node(path_t &path, key_type &key, key_type &leaf_max) {
        // std::cout << "target: " << target_id << ", leaf max: " << leaf_max << std::endl;
        leaf_max = std::numeric_limits<key_type>::max();
        path.reserve(height);

        node_id_t node_id = root_id;

        while (true) {
            PageId pid{filename, node_id};
            Page &page = buffer_pool.get_mut_page(pid);

            auto* base = reinterpret_cast<BaseHeader*>(page.data());

            if (base->type == bp_node_type::LEAF) break;

            internal_t node(page);
            path.push_back(node_id);

            uint16_t slot = node.child_slot(key);
            node_id = node.children[slot];

            if (slot < node.header->size) {
                leaf_max = node.keys[slot];
            }
        }
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

            node_id_t new_node_id = numPages.fetch_add(1);
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

    // Create a new root when the height of the tree increases
    // key: key that we want to elect to new node
    // right_child_id: the right subtree id of this key
    void create_new_root(const key_type &key, node_id_t right_child_id) {
        node_id_t left_child_id = numPages.fetch_add(1);

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
                // try {
                //     node_t node(block_manager.open_block(node_id_to_sort));
                //     if (!node.info->isSorted) {
                //         block_manager.mark_dirty(node_id_to_sort);
                //         node.sort();
                //     }
                // } catch (const std::exception &e) {
                //     std::cerr << "Error in background sort: " << e.what() << std::endl;
                // }
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

        PageId pid{filename, node_id};
        Page page = buffer_pool.get_mut_page(pid);
        uint8_t* raw = page.data();
        auto* base = reinterpret_cast<BaseHeader*>(raw);

        if (base->type == bp_node_type::LEAF) {
            leaf_t node(raw, td, key_index);  // load leaf

            std::cout << "Leaf ID " << node_id << ": [";

            if (show_all_values) {
                for (uint16_t i = 0; i < node.info->size; i++) {
                    std::optional<Tuple> t = node.get_by_index(i);
                    if (t.has_value()) {
                        std::cout << node.extract_key(*t);
                        if (i < node.get_size() - 1) std::cout << ", ";
                    }
                }
            } else {
                if (node.get_size() > 0) {
                    std::cout << "min=" << node.min_key();

                    if (node.get_size() > 1) {
                        std::cout << ", max=" << node.max_key();
                        std::cout << ", size=" << node.get_size();
                    }
                } else {
                    std::cout << "empty";
                }
            }

            std::cout << "]" << std::endl;
        } else {
            internal_t node(raw, node_id);
            std::cout << "Internal ID " << node_id << ": [";

            if (show_all_values) {
                for (uint16_t i = 0; i < node.header->size; i++) {
                    std::cout << node.keys[i];
                    if (i < node.header->size - 1) std::cout << ", ";
                }
            } else {
                if (node.header->size > 0) {
                    std::cout << "min=" << node.keys[0];
                    if (node.header->size > 1) {
                        std::cout << ", max=" << node.keys[node.header->size - 1];
                        std::cout << ", size=" << node.header->size;
                    }
                } else {
                    std::cout << "empty";
                }
            }

            std::cout << "]" << std::endl;

            for (uint16_t i = 0; i <= node.header->size; i++) {
                print_node(node.children[i], level + 1, show_all_values);
            }
        }
    }
};
