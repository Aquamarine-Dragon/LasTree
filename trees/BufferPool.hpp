#pragma once

#include <unordered_map>
#include <unordered_set>
#include <list>
#include <vector>
#include <array>
#include <numeric>

#include "Types.hpp"
#include "Database.hpp"

// A generic BufferPool supporting LRU and dirty tracking
template <size_t pool_size>
class BufferPool {

private:
    std::array<Page, pool_size> pages;
    std::vector<size_t> free_list;
    std::unordered_map<PageId, size_t> pid_to_slot;
    std::unordered_map<size_t, PageId> slot_to_id;
    std::unordered_set<size_t> dirty_slots;

    std::list<size_t> lru;
    std::unordered_map<size_t, std::list<size_t>::iterator> slot_lru_map;


    size_t fetch_slot() {
        if (!free_list.empty()) {
            size_t slot = free_list.back();
            free_list.pop_back();
            return slot;
        }

        // evict from LRU
        size_t slot = lru.back();
        const PageId& old_id = slot_to_id[slot];
        evict(old_id);
        return slot;
    }

    void touch(size_t slot) {
        lru.erase(slot_lru_map[slot]);
        lru.push_front(slot);
        slot_lru_map[slot] = lru.begin();
    }
public:

    BufferPool() {
        std::iota(free_list.rbegin(), free_list.rend(), 0);
    }

    // Read-only view
    const Page& get_page(const PageId& id) {
        return get_mut_block(id); // reuse logic
    }

    // Writable view
    Page& get_mut_block(const PageId& pid) {
        // Case 1: already in buffer
        if (pid_to_slot.contains(pid)) {
            size_t slot = pid_to_slot.at(pid);
            touch(slot);
            return pages[slot];
        }

        // Case 2: allocate from free list or evict
        size_t slot = fetch_slot();
        pid_to_slot[pid] = slot;
        slot_to_id[slot] = pid;

        // Load from disk
        Page &page = pages[slot];
        getDatabase().get(pid.file).readPage(page, pid.page);

        lru.push_front(slot);
        slot_lru_map[slot] = lru.begin();

        return pages[slot];
    }

    void mark_dirty(const PageId& id) {
        size_t slot = pid_to_slot.at(id);
        dirty_slots.insert(slot);
    }

    void flush(const PageId& id) {
        size_t slot = pid_to_slot.at(id);
        if (!dirty_slots.contains(slot)) return;
        write_block_to_disk(id, pages[slot]);
        dirty_slots.erase(slot);
    }

    void flush_all() {
        for (size_t slot : dirty_slots) {
            const PageId& id = slot_to_id[slot];
            write_block_to_disk(id, pages[slot]);
        }
        dirty_slots.clear();
    }

    void evict(const PageId& id) {
        size_t slot = pid_to_slot.at(id);
        flush(id);
        pid_to_slot.erase(id);
        slot_to_id.erase(slot);
        lru.erase(slot_lru_map[slot]);
        slot_lru_map.erase(slot);
        free_list.push_back(slot);
    }

    bool contains(const PageId& id) const {
        return pid_to_slot.contains(id);
    }
};

