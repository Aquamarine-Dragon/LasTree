#include "BufferPool.hpp"
#include "Database.hpp"  // Only needed in cpp, not in hpp

#include <numeric>
#include <sstream>
#include <stdexcept>
#include <thread>

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
        // std::cout << "[Thread " << std::this_thread::get_id() << " remove slot " << slot << " from free list"<<std::endl;
        // pin_count[slot] = 0;
        // std::cout << "[Thread " << std::this_thread::get_id() << " get slot " << slot << " from free list"<<std::endl;
        return slot;
    }

    // no available slots, evict
    // for (auto it = lru.rbegin(); it != lru.rend(); ++it) {
    //     if (pin_count[*it] == 0) {
    //         size_t slot = *it;
    //         const PageId& old_id = slot_to_id.at(slot);
    //         evict(old_id);
    //         free_list.pop_back();
    //         std::cout << "[Thread " << std::this_thread::get_id() << " get slot " << slot << " by evict"<<std::endl;
    //         return slot;
    //     }
    // }
    //
    // throw std::runtime_error("BufferPool::fetch_slot: No available slot to evict!");

    size_t slot = lru.back();
    const PageId& old_id = slot_to_id.at(slot);
    evict(old_id);
    free_list.pop_back();// reserve the slot
    return slot;
}

void BufferPool::touch(size_t slot) {
    if (!slot_lru_map.contains(slot)) {
        throw std::runtime_error("touch: slot not found in slot_lru_map!");
    }

    lru.erase(slot_lru_map[slot]);
    lru.push_front(slot);
    slot_lru_map[slot] = lru.begin();
}

const Page& BufferPool::get_page(const PageId& id) {
    return get_mut_page(id);
}

Page& BufferPool::get_mut_page(const PageId& pid) {
    std::lock_guard<std::mutex> lock(pool_mutex);
    if (pid_to_slot.contains(pid)) {
        std::cout << "[Thread " << std::this_thread::get_id() << " contains " << pid.page << std::endl;
        size_t slot = pid_to_slot.at(pid);
        touch(slot);
        // pin_page(pid);
        // pin_count[slot]++;
        // std::cout << "[Thread " << std::this_thread::get_id()
        //           << "] get_mut_page added 1 to pinned slot " << slot
        //           << " (PageId=" << pid.page << "), pin count now = " << pin_count[slot]
        //           << std::endl;
        return pages[slot];
    }

    size_t slot = fetch_slot();
    std::cout << "[Thread " << std::this_thread::get_id() << " fetched slot " << slot << std::endl;
    Page &page = pages[slot];

    getDatabase().get(pid.file).readPage(page, pid.page);
    pid_to_slot[pid] = slot;
    slot_to_id[slot] = pid;
    // pin_count[slot] = 1;
    // std::cout << "[Thread " << std::this_thread::get_id()
    //               << "] get_mut_page set pinned slot to 1 =" << slot
    //               << " (PageId=" << pid.page << "), pin count now = " << pin_count[slot]
    //               << std::endl;
    lru.push_front(slot);
    slot_lru_map[slot] = lru.begin();
    return page;
}

void BufferPool::mark_dirty(const PageId& id) {
    std::lock_guard<std::mutex> lock(pool_mutex);
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
    if (dirty_slots.erase(slot) == 0) return;
    const Page &page = pages[slot];
    getDatabase().get(id.file).writePage(page, id.page);
}

void BufferPool::flush_all() {
    std::lock_guard<std::mutex> lock(pool_mutex);
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
    // if (pin_count[slot] > 0) {
    //     throw std::runtime_error("evict: Trying to evict a pinned page!");
    // }
    if (dirty_slots.contains(slot)) {
        flush(id);
    }
    // clean page, discard
    pid_to_slot.erase(id);
    slot_to_id.erase(slot);
    lru.erase(slot_lru_map[slot]);
    slot_lru_map.erase(slot);
    free_list.push_back(slot);
    std::cout << "[Thread " << std::this_thread::get_id() << " added slot " << slot << " to free list"<<std::endl;
    // pin_count.erase(slot);
}

    void BufferPool::pin_page(const PageId& id) {
    // if (!pid_to_slot.contains(id)) return;
    // size_t slot = pid_to_slot.at(id);
    // // pin_count[slot]++;
    // std::cout << "[Thread " << std::this_thread::get_id()
    //               << "] Pinned slot=" << slot
    //               << " (PageId=" << id.page << "), pin count now = " << pin_count[slot]
    //               << std::endl;

}

    void BufferPool::unpin_page(const PageId& id) {
    // if (!pid_to_slot.contains(id)) return;
    // size_t slot = pid_to_slot.at(id);
    // if (--pin_count[slot] <= 0) {
    //     pin_count.erase(slot);
    //     std::cout << "[Thread " << std::this_thread::get_id()
    //               << "] Unpinned slot=" << slot
    //               << " (PageId=" << id.page << ") — removed from pin_count"
    //               << std::endl;
    // } else {
    //     std::cout << "[Thread " << std::this_thread::get_id()
    //               << "] Unpinned slot=" << slot
    //               << " (PageId=" << id.page << "), pin count now = " << pin_count[slot]
    //               << std::endl;
    // }
}

bool BufferPool::contains(const PageId& id) const {
    return pid_to_slot.contains(id);
}

} // namespace db
