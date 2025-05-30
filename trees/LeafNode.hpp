#pragma once

#include <cstdint>
#include <algorithm>
#include <vector>
#include <stdexcept>
#include "NodeTypes.hpp"
#include "Tuple.hpp"

using namespace db;

/**
 * A LeafNode implementation for sorted tree node
 */
template<typename node_id_type, typename key_type, size_t split_per, size_t block_size>
class LeafNode {
public:
    static constexpr size_t MAX_SLOTS = 256;

    struct Slot {
        uint16_t offset; // offset of this tuple in heap
        uint16_t length; // length of tuple
        bool valid;
    };

    struct leaf_info {
        node_id_type next_id; // next leaf id
        bool isSorted;
        bool isCold;
    };

    struct PageHeader {
        node_id_type id; // node id
        uint16_t size; // number of tuples
        leaf_info meta;
        size_t slot_count;
        size_t heap_end;
    };

    static constexpr uint16_t available_space = block_size - (sizeof(BaseHeader) + sizeof(PageHeader));

    uint8_t *buffer; // address of the page that stores this page
    const TupleDesc &td; // tuple schema of this page
    size_t key_index{}; // index of key in each tuple

    BaseHeader* base_header;
    PageHeader* page_header;
    Slot* slots;

    LeafNode() = default;

    // constructor that loads from an existing buffer
    explicit LeafNode(Page &page, const TupleDesc &td, size_t key_index)
        : buffer(page.data()),
        td(td),
          key_index(key_index) {
        base_header = reinterpret_cast<BaseHeader*>(buffer);
        page_header = reinterpret_cast<PageHeader*>(buffer + sizeof(BaseHeader));
        slots = reinterpret_cast<Slot*>(buffer + sizeof(BaseHeader) + sizeof(PageHeader));
    }

    // constructor: divide page into multiples parts
    LeafNode(Page &page, const TupleDesc &desc, size_t key, node_id_type id, node_id_type next_id,  SplitPolicy policy,
             bool isCold)
        :buffer(page.data()),
          td(desc),
          key_index(key) {
        base_header = reinterpret_cast<BaseHeader*>(buffer);
        page_header = reinterpret_cast<PageHeader*>(buffer + sizeof(BaseHeader));
        slots = reinterpret_cast<Slot*>(buffer + sizeof(BaseHeader) + sizeof(PageHeader));

        base_header->type = 0;
        page_header->id = id;
        page_header->meta.next_id = next_id;
        page_header->meta.isSorted = true;
        page_header->meta.isCold = isCold;
        page_header->size = 0;
        page_header->slot_count = 0;
        page_header->heap_end = block_size;
    }

    node_id_type get_id() {
        return page_header->id;
    }

    uint16_t get_size() {
        return page_header->size;
    }

    bool is_sorted() {
        return page_header->meta.isSorted;
    }

    key_type extract_key(const Tuple &t) const {
        return std::get<key_type>(t.get_field(key_index));
    }

    key_type min_key() const {
        for (size_t i = 0; i < page_header->slot_count; ++i) {
            if (slots[i].valid) {
                Tuple t = td.deserialize(buffer + slots[i].offset);
                return extract_key(t);
            }
        }
        throw std::runtime_error("Empty node");
    }

    key_type max_key() const {
        for (int i = page_header->slot_count - 1; i >= 0; --i) {
            if (slots[i].valid) {
                Tuple t = td.deserialize(buffer + slots[i].offset);
                return extract_key(t);
            }
        }
        throw std::runtime_error("Empty node");
    }


    // Binary search based on keys in slots
    uint16_t value_slot(const key_type &key) const {
        // Binary search if sorted
        uint16_t left = 0, right = page_header->slot_count;
        while (left < right) {
            uint16_t mid = (left + right) / 2;
            Slot cur = slots[mid];
            if (!slots[mid].valid) {
                ++left;
                continue;
            }
            Tuple mid_tuple = td.deserialize(buffer + slots[mid].offset);
            if (extract_key(mid_tuple) < key) left = mid + 1;
            else right = mid;
        }
        return left;
    }

    std::optional<Tuple> get(const key_type &key) const {
        uint16_t index = value_slot(key);

        if (index < page_header->slot_count) {
            const Slot &slot = slots[index];
            if (!slot.valid) return std::nullopt;

            Tuple t = td.deserialize(buffer + slot.offset);
            if (extract_key(t) == key) {
                return t;
            }
        }
        return std::nullopt;
    }

    std::vector<Tuple> get_range(const key_type &minkey, const key_type &maxkey) const {
        std::vector<Tuple> result;
        if (this->min_key() > maxkey) // stop if no result
            return result;
        if (page_header->meta.isSorted) {
            for (uint16_t i = 0; i < page_header->slot_count; ++i) {
                const Slot &slot = slots[i];
                if (!slot.valid) {
                    continue;
                }
                Tuple t = td.deserialize(buffer + slot.offset);
                // sorted data allows early cut
                if (extract_key(t) < minkey) {
                    continue;
                }
                if (extract_key(t) > maxkey) {
                    return result;
                }
                result.push_back(t);
            }
        }else { // scan
            for (uint16_t i = 0; i < page_header->slot_count; ++i) {
                const Slot &slot = slots[i];
                if (!slot.valid) continue;

                Tuple t = td.deserialize(buffer + slot.offset);
                key_type k = extract_key(t);
                if (k < minkey) continue;
                if (k > maxkey) continue;
                result.push_back(t);
            }
        }
        return result;
    }

    Tuple get_tuple(size_t i) const {
        const Slot &slot = slots[i];
        return td.deserialize(buffer + slot.offset);
    }

    size_t used_space() const {
        return block_size - page_header->heap_end + sizeof(Slot) * (page_header->slot_count);
    }

    size_t free_space() const {
        return block_size - sizeof(BaseHeader) + sizeof(PageHeader) - sizeof(Slot) * (page_header->slot_count + 1);
    }

    bool can_insert(size_t tuple_len) const {
        size_t new_offset = page_header->heap_end - tuple_len;
        size_t end_offset = sizeof(BaseHeader) + sizeof(PageHeader) + sizeof(Slot) * (page_header->slot_count + 1);

        // std::cout << "[new heap offset]: " << new_offset
        //       << ", [metadata end offset]: " << end_offset << std::endl;
        return new_offset >= end_offset;
    }

    void print_page_debug() const {
        std::cout << "  Slots (" << page_header->slot_count << "):" << std::endl;
        for (size_t i = 0; i < page_header->slot_count; ++i) {
            const auto& slot = slots[i];
            std::cout << "    [" << i << "]: offset=" << slot.offset
                      << ", length=" << slot.length
                      << ", valid=" << slot.valid << std::endl;
        }

        std::cout << "  Heap content:" << std::endl;
        for (size_t i = 0; i < page_header->slot_count; ++i) {
            const auto& slot = slots[i];
            if (slot.valid) {
                Tuple t = td.deserialize(buffer + slot.offset);
                std::cout << "    [" << i << "] " << td.to_string(t) << std::endl;
            }
        }
    }



    bool insert(const Tuple &t) {
        const size_t len = td.length(t);

        if (!can_insert(len)) {
            return false;
        }

        key_type key = extract_key(t);
        uint16_t insert_pos = value_slot(key);

        page_header->heap_end -= len;
        td.serialize(buffer + page_header->heap_end, t);

        if (insert_pos < page_header->slot_count) {
            std::memmove(slots + insert_pos + 1, slots + insert_pos,
                     (page_header->slot_count - insert_pos) * sizeof(Slot));
        }

        slots[insert_pos] = {static_cast<uint16_t>(page_header->heap_end), static_cast<uint16_t>(len), true};
        ++(page_header->slot_count);
        ++page_header->size;

        // std::cout << "[DEBUG] After inserting key=" << extract_key(t) << std::endl;
        // std::cout << "[DEBUG] After heap end=" << page_header->heap_end << std::endl;
        // print_page_debug();
        return true;
    }

    bool update(const Tuple &t) {
        key_type key = extract_key(t);
        uint16_t index = value_slot(key);

        if (index < page_header->slot_count) {
            const Slot &slot = slots[index];
            if (slot.valid) {
                Tuple existing = td.deserialize(buffer + slot.offset);
                if (extract_key(existing) == key) {
                    // Overwrite slot: mark old invalid, insert new
                    slots[index].valid = false;
                    --page_header->size;
                    return insert(t);
                }
            }
        }

        // Fallback insert if not found
        return insert(t);
    }

    key_type split_into(LeafNode &new_leaf) {
        // Decide how much to move
        size_t total_bytes = block_size - page_header->heap_end;
        size_t moved = 0;
        int i = static_cast<int>(page_header->slot_count - 1);

        // find key index which makes moved >= 25%
        for (; i >= 0; --i) {
            const auto &slot = slots[i];
            if (!slot.valid) continue;
            moved += slot.length;
            if (moved >= total_bytes / split_per) break;
        }

        // move those slots to new_leaf
        for (size_t j = i + 1; j < page_header->slot_count; ++j) {
            const auto &slot = slots[j];
            if (!slot.valid) continue;

            Tuple t = td.deserialize(buffer + slot.offset);
            new_leaf.insert(t);
        }

        page_header->size = i + 1;
        page_header->slot_count = i + 1;

        // Step 1: collect tuples to keep (i.e., slots[0..i])
        std::vector<Tuple> to_keep;
        for (size_t k = 0; k <= static_cast<size_t>(i); ++k) {
            if (!slots[k].valid) continue;
            Tuple t = td.deserialize(buffer + slots[k].offset);
            to_keep.push_back(t);
        }

        // Step 2: reset heap/slot
        page_header->slot_count = 0;
        page_header->size = 0;
        page_header->heap_end = block_size;

        // Step 3: re-insert
        for (const auto& t : to_keep) {
            insert(t); // this will update heap_end properly
        }

        // update next pointers
        new_leaf.page_header->meta.next_id = page_header->meta.next_id;
        page_header->meta.next_id = new_leaf.page_header->id;

        return new_leaf.min_key();
    }


    bool is_full(const Tuple &t) const {
        return free_space() < td.length(t) + sizeof(Slot);
    }


};
