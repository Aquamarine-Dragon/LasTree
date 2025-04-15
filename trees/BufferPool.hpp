#pragma once

#include <unordered_map>
#include <unordered_set>
#include <list>
#include <vector>
#include <array>
#include "Types.hpp"

namespace db {

    class BufferPool {
    private:
        std::array<Page, POOL_SIZE> pages;
        std::vector<size_t> free_list;
        std::unordered_map<PageId, size_t> pid_to_slot;
        std::unordered_map<size_t, PageId> slot_to_id;
        std::unordered_set<size_t> dirty_slots;

        std::list<size_t> lru;
        std::unordered_map<size_t, std::list<size_t>::iterator> slot_lru_map;

        size_t fetch_slot();
        void touch(size_t slot);

    public:
        BufferPool();
        ~BufferPool();

        const Page& get_page(const PageId& id);
        Page& get_mut_page(const PageId& pid);

        void mark_dirty(const PageId& id);
        void flush(const PageId& id);
        void flush_all();
        void flushFile(const std::string &file);
        void evict(const PageId& id);
        bool contains(const PageId& id) const;
    };

} // namespace db
