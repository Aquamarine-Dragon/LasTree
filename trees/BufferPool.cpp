#include "BufferPool.hpp"
#include "Database.hpp"  // Only needed in cpp, not in hpp

#include <numeric>
#include <stdexcept>

namespace db {

BufferPool::BufferPool() : free_list(POOL_SIZE) {
    std::iota(free_list.rbegin(), free_list.rend(), 0);
}

BufferPool::~BufferPool() {
    flush_all();
}

size_t BufferPool::fetch_slot() {
    if (!free_list.empty()) {
        size_t slot = free_list.back();
        free_list.pop_back();
        return slot;
    }

    size_t slot = lru.back();
    if (!slot_to_id.contains(slot)) {
        throw std::runtime_error("fetch_slot: slot not found in slot_to_id!");
    }
    const PageId& old_id = slot_to_id.at(slot);
    evict(old_id);
    return slot;
}

void BufferPool::touch(size_t slot) {
    lru.erase(slot_lru_map[slot]);
    lru.push_front(slot);
    slot_lru_map[slot] = lru.begin();
}

const Page& BufferPool::get_page(const PageId& id) {
    return get_mut_page(id);
}

Page& BufferPool::get_mut_page(const PageId& pid) {
    if (pid_to_slot.contains(pid)) {
        size_t slot = pid_to_slot.at(pid);
        touch(slot);
        return pages[slot];
    }

    size_t slot = fetch_slot();
    Page &page = pages[slot];

    getDatabase().get(pid.file).readPage(page, pid.page);
    pid_to_slot[pid] = slot;
    slot_to_id[slot] = pid;

    lru.push_front(slot);
    slot_lru_map[slot] = lru.begin();

    return page;
}

void BufferPool::mark_dirty(const PageId& id) {
    if (!pid_to_slot.contains(id)) {
        throw std::runtime_error("mark_dirty: PageId not found in pid_to_slot!");
    }
    size_t slot = pid_to_slot.at(id);
    dirty_slots.insert(slot);
}

void BufferPool::flush(const PageId& id) {
    if (!pid_to_slot.contains(id)) {
        throw std::runtime_error("flush: PageId not found in pid_to_slot!");
    }
    size_t slot = pid_to_slot.at(id);
    if (!dirty_slots.contains(slot)) return;
    const Page &page = pages[slot];
    getDatabase().get(id.file).writePage(page, id.page);
    dirty_slots.erase(slot);
}

void BufferPool::flush_all() {
    for (const size_t &slot : dirty_slots) {
        const Page &page = pages[slot];
        const PageId &pid = slot_to_id.at(slot);
        getDatabase().get(pid.file).writePage(page, pid.page);
    }
    dirty_slots.clear();
}

void BufferPool::flushFile(const std::string &file) {
    std::vector<size_t> to_flush;
    for (const size_t &pos: dirty_slots) {
        const PageId &pid = slot_to_id[pos];
        if (pid.file == file) {
            to_flush.emplace_back(pid.page);
        }
    }
    for (const auto &page: to_flush) {
        flush({file, page});
    }
}

void BufferPool::evict(const PageId& id) {
    if (!pid_to_slot.contains(id)) {
        throw std::runtime_error("evict: PageId not found in pid_to_slot!");
    }
    size_t slot = pid_to_slot.at(id);
    if (dirty_slots.contains(slot)) {
        flush(id);
    }
    pid_to_slot.erase(id);
    slot_to_id.erase(slot);
    lru.erase(slot_lru_map[slot]);
    slot_lru_map.erase(slot);
    free_list.push_back(slot);
}

bool BufferPool::contains(const PageId& id) const {
    return pid_to_slot.contains(id);
}

} // namespace db
