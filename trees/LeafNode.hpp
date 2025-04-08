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
template <typename node_id_type, size_t block_size>
class LeafNode {
private:
    static constexpr size_t BLOCK_SIZE = 4096;
    static constexpr size_t HEADER_SIZE = 16;
    static constexpr size_t MAX_SLOTS = 256;

    struct Slot {
        uint16_t offset; // offset of this tuple in heap
        uint16_t length; // length of tuple
        bool valid;
    };

    struct node_info {
        node_id_type id; // node id
        uint16_t size; // number of tuples
        uint16_t type; // 0 = leaf
    };

    struct leaf_info {
        node_id_type next_id; // next leaf id
        bool isSorted;
        bool isCold;
    };

    struct PageHeader {
        node_info info;
        leaf_info meta;
        size_t slot_count;
    };

    uint8_t* buffer; // address of the page that stores this page
    const TupleDesc& td; // tuple schema of this page
    size_t key_index; // index of key in each tuple

public:
    using key_type = field_t;

    PageHeader* header;
    Slot* slots; // slot directory, each record the address and length of each tuple in heap
    size_t* heap_end; // top of heap
    uint8_t* heap_base; // bottom of heap


    LeafNode() = default;

    // constructor: divide page into multiples parts
    LeafNode(void* data, const TupleDesc& desc, size_t key) : buffer(reinterpret_cast<uint8_t*>(data)), td(desc), key_index(key) {
        header = reinterpret_cast<PageHeader*>(buffer);
        slots = reinterpret_cast<Slot*>(buffer + sizeof(size_t));
        heap_end = reinterpret_cast<size_t*>(buffer + HEADER_SIZE + sizeof(Slot) * MAX_SLOTS);
        heap_base = buffer + BLOCK_SIZE;
    }

    void init(node_id_type id, node_id_type next_id, bool sorted) {
        header->info.id = id;
        header->info.size = 0;
        header->info.type = 0;
        header->meta.next_id = next_id;
        header->meta.isSorted = sorted;
        header->meta.isCold = false;
        header->slot_count = 0;
    }

    node_id_type get_id() {
        return header->info.id;
    }

    uint16_t get_size() {
        return header->info.size;
    }


    size_t free_space() const {
        size_t slot_bytes = sizeof(Slot) * header->slot_count;
        size_t used_heap = BLOCK_SIZE - *heap_end;
        return BLOCK_SIZE - HEADER_SIZE - slot_bytes - used_heap;
    }

    key_type extract_key(const Tuple& t) const {
        return t.get_field(key_index);
    }

    // Binary search based on keys in slots
    uint16_t value_slot(const key_type& key) const {
        if (!header->meta.isSorted) {
            for (uint16_t i = 0; i < header->slot_count; ++i) {
                if (!slots[i].valid) continue;
                Tuple t = td.deserialize(buffer + slots[i].offset);
                if (extract_key(t) >= key) return i;
            }
            return header->slot_count;
        }

        // Binary search if sorted
        uint16_t left = 0, right = header->slot_count;
        while (left < right) {
            uint16_t mid = (left + right) / 2;
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

    std::optional<Tuple> get(const key_type& key) const {
        uint16_t index = value_slot(key);

        if (index < header->slot_count) {
            const Slot& slot = slots[index];
            if (!slot.valid) return std::nullopt;

            Tuple t = td.deserialize(buffer + slot.offset);
            if (extract_key(t) == key) {
                return t;
            }
        }
        return std::nullopt;
    }

    bool insert(const Tuple& t) {
        const size_t len = td.length(t);
        if (free_space() < len + sizeof(Slot)) return false;

        key_type key = extract_key(t);
        uint16_t insert_pos = value_slot(key);

        *heap_end -= len;
        td.serialize(buffer + *heap_end, t);

        std::memmove(slots + insert_pos + 1, slots + insert_pos,
                     (header->slot_count - insert_pos) * sizeof(Slot));

        slots[insert_pos] = { static_cast<uint16_t>(*heap_end), static_cast<uint16_t>(len), true };
        ++(header->slot_count);
        ++header->info.size;
        return true;
    }

    std::pair<key_type, node_id_type> split_into(LeafNode& new_leaf) {
        // 2. Decide how much to move
        size_t total_bytes = block_size - free_space();
        size_t moved = 0;
        size_t i = 0;

        // find key index which makes moved >= 25%
        // todo modify percentage
        for (; i < header->slot_count; ++i) {
            const auto &slot = slots[i];
            if (!slot.valid) continue;
            moved += slot.length + sizeof(Slot);
            if (moved >= total_bytes / 4) break;
        }

        // move those slots to new_leaf
        for (size_t j = i + 1; j < header->slot_count; ++j) {
            const auto &slot = slots[j];
            if (!slot.valid) continue;

            Tuple t = td.deserialize(buffer + slot.offset);
            new_leaf.insert(t);
            slots[j].valid = false;
            --header->info.size;
        }

        // update next pointers
        new_leaf.header->meta.next_id = header->meta.next_id;
        header->meta.next_id = new_leaf.header->meta.next_id;

        return { new_leaf.min_key(), new_leaf.header->info.id };
    }


    bool is_nearly_full() const {
        return free_space() < 0.1 * block_size;
    }

    bool is_full(const Tuple& t) const {
        return free_space() < td.length(t) + sizeof(Slot);
    }

    key_type min_key() const {
        for (size_t i = 0; i < header->slot_count; ++i) {
            if (slots[i].valid) {
                Tuple t = td.deserialize(buffer + slots[i].offset);
                return extract_key(t);
            }
        }
        throw std::runtime_error("Empty node");
    }

    key_type max_key() const {
        for (int i = header->slot_count - 1; i >= 0; --i) {
            if (slots[i].valid) {
                Tuple t = td.deserialize(buffer + slots[i].offset);
                return extract_key(t);
            }
        }
        throw std::runtime_error("Empty node");
    }
};