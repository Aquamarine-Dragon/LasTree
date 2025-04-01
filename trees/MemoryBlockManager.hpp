//
// Created by Kelly Yan on 3/22/25.
//

#ifndef MEMORYBLOCKMANAGER_HPP
#define MEMORYBLOCKMANAGER_HPP

#pragma once

#include <cstdint>
#include <unordered_set>
#include <vector>
#include <array>
#include <atomic>
#include <limits>
#include <stdexcept>

// Default block size: 4KB (common page size)
#ifndef BLOCK_SIZE_BYTES
#define BLOCK_SIZE_BYTES 4096
#endif

// Memory block manager for B+Tree nodes
template <typename node_id_type>
class InMemoryBlockManager {
public:
    // Block size is configurable via BLOCK_SIZE_BYTES define
    static constexpr size_t block_size = BLOCK_SIZE_BYTES;

    // Constructor creates a pool of memory blocks
    explicit InMemoryBlockManager(const uint32_t capacity)
        : internal_memory(capacity) {}

    // Reset the block manager state
    void reset() {
        next_block_id.store(0, std::memory_order_relaxed);
        dirty_blocks.clear();
    }

    // Allocate a new block and return its ID
    node_id_type allocate() {
        node_id_type id = next_block_id.fetch_add(1, std::memory_order_acq_rel);
        if (id >= internal_memory.size()) {
            throw std::runtime_error("Memory block manager out of capacity");
        }
        return id;
    }

    // Mark a block as dirty (needs to be written to persistent storage)
    void mark_dirty(node_id_type id) {
        dirty_blocks.insert(id);
    }

    // Get a pointer to the block data
    void* open_block(const node_id_type id) {
        if (id >= internal_memory.size()) {
            throw std::runtime_error("Invalid block ID");
        }
        return internal_memory[id].data();
    }

    // Get total capacity
    node_id_type get_capacity() const {
        return internal_memory.size();
    }

    // Get the number of allocated blocks
    node_id_type get_allocated_count() const {
        return next_block_id.load(std::memory_order_relaxed);
    }

    // Get the set of dirty blocks
    const std::unordered_set<node_id_type>& get_dirty_blocks() const {
        return dirty_blocks;
    }

private:
    // Define a memory block as a fixed-size array of bytes
    using Block = std::array<uint8_t, block_size>;

    // Storage for all memory blocks
    std::vector<Block> internal_memory;

    // Next block ID to allocate
    std::atomic<node_id_type> next_block_id{0};

    // Set of blocks that have been modified
    std::unordered_set<node_id_type> dirty_blocks;
};

#endif //MEMORYBLOCKMANAGER_HPP
