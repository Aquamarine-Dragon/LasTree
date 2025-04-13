#pragma once

#include <cstdint>
#include <algorithm>
#include <vector>
#include <stdexcept>
#include "NodeTypes.hpp"
#include "PageLayout.hpp"
#include "Tuple.hpp"

using namespace db;

/**
 * A LeafNode implementation for sorted tree node
 */
template<typename node_id_type, typename key_type, size_t block_size>
class LeafNode {
public:
    static constexpr size_t BLOCK_SIZE = 4096;
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
    };

    using Layout = PageLayout<BaseHeader, PageHeader, Slot, MAX_SLOTS, block_size>;

    uint8_t *buffer; // address of the page that stores this page
    const TupleDesc &td; // tuple schema of this page
    size_t key_index{}; // index of key in each tuple
    Layout layout;

    LeafNode() = default;

    // constructor that loads from an existing buffer
    explicit LeafNode(Page &page, const TupleDesc &td, size_t key_index)
        : buffer(page.data()),
          layout(buffer),
          td(td),
          key_index(key_index) {
    }

    // constructor: divide page into multiples parts
    LeafNode(Page &page, const TupleDesc &desc, size_t key, node_id_type id, node_id_type next_id, bool sorted,
             bool isCold)
        : buffer(page.data()),
          layout(Layout(buffer)),
          td(desc),
          key_index(key) {
        layout.base_header->type = 0;
        layout.page_header->id = id;
        layout.page_header->meta.next_id = next_id;
        layout.page_header->meta.isSorted = sorted;
        layout.page_header->meta.isCold = isCold;
        layout.page_header->size = 0;
        layout.page_header->slot_count = 0;
        layout.heap_end[0] = block_size; // heap grows down
    }

    node_id_type get_id() {
        return layout.page_header->info.id;
    }

    uint16_t get_size() {
        return layout.page_header->info.size;
    }

    size_t free_space() const {
        return layout.free_space();
    }

    key_type extract_key(const Tuple &t) const {
        return std::get<key_type>(t.get_field(key_index));
    }

    // Binary search based on keys in slots
    uint16_t value_slot(const key_type &key) const {
        if (!layout.page_header->meta.isSorted) {
            for (uint16_t i = 0; i < layout.page_header->slot_count; ++i) {
                if (!layout.slots[i].valid) continue;
                Tuple t = td.deserialize(buffer + layout.slots[i].offset);
                if (extract_key(t) >= key) return i;
            }
            return layout.page_header->slot_count;
        }


        // Binary search if sorted
        uint16_t left = 0, right = layout.page_header->slot_count;
        while (left < right) {
            uint16_t mid = (left + right) / 2;
            if (!layout.slots[mid].valid) {
                ++left;
                continue;
            }
            Tuple mid_tuple = td.deserialize(buffer + layout.slots[mid].offset);
            if (extract_key(mid_tuple) < key) left = mid + 1;
            else right = mid;
        }
        return left;
    }

    std::optional<Tuple> get(const key_type &key) const {
        uint16_t index = value_slot(key);

        if (index < layout.page_header->slot_count) {
            const Slot &slot = layout.slots[index];
            if (!slot.valid) return std::nullopt;

            Tuple t = td.deserialize(buffer + slot.offset);
            if (extract_key(t) == key) {
                return t;
            }
        }
        return std::nullopt;
    }

    bool insert(const Tuple &t) {
        const size_t len = td.length(t);
        if (free_space() < len + sizeof(Slot)) return false;

        key_type key = extract_key(t);
        uint16_t insert_pos = value_slot(key);

        *layout.heap_end -= len;
        td.serialize(buffer + *layout.heap_end, t);

        std::memmove(layout.slots + insert_pos + 1, layout.slots + insert_pos,
                     (layout.page_header->slot_count - insert_pos) * sizeof(Slot));

        layout.slots[insert_pos] = {static_cast<uint16_t>(*layout.heap_end), static_cast<uint16_t>(len), true};
        ++(layout.page_header->slot_count);
        ++layout.page_header->size;
        return true;
    }

    bool update(const Tuple &t) {
        key_type key = extract_key(t);
        uint16_t index = value_slot(key);

        if (index < layout.page_header->slot_count) {
            const Slot &slot = layout.slots[index];
            if (slot.valid) {
                Tuple existing = td.deserialize(buffer + slot.offset);
                if (extract_key(existing) == key) {
                    // Overwrite slot: mark old invalid, insert new
                    layout.slots[index].valid = false;
                    --layout.page_header->size;
                    return insert(t);
                }
            }
        }

        // Fallback insert if not found
        return insert(t);
    }

    std::pair<key_type, node_id_type> split_into(LeafNode &new_leaf) {
        // 2. Decide how much to move
        size_t total_bytes = block_size - free_space();
        size_t moved = 0;
        size_t i = 0;

        // find key index which makes moved >= 25%
        // todo modify percentage
        for (; i < layout.page_header->slot_count; ++i) {
            const auto &slot = layout.slots[i];
            if (!slot.valid) continue;
            moved += slot.length + sizeof(Slot);
            if (moved >= total_bytes / 4) break;
        }

        // move those slots to new_leaf
        for (size_t j = i + 1; j < layout.page_header->slot_count; ++j) {
            const auto &slot = layout.slots[j];
            if (!slot.valid) continue;

            Tuple t = td.deserialize(buffer + slot.offset);
            new_leaf.insert(t);
            layout.slots[j].valid = false;
            --layout.page_header->size;
        }

        // update next pointers
        new_leaf.layout.page_header->meta.next_id = layout.page_header->meta.next_id;
        layout.page_header->meta.next_id = new_leaf.layout.page_header->meta.next_id;

        return {new_leaf.min_key(), new_leaf.layout.page_header->id};
    }

    bool is_nearly_full() const {
        return free_space() < 0.1 * block_size;
    }

    bool is_full(const Tuple &t) const {
        return free_space() < td.length(t) + sizeof(Slot);
    }

    key_type min_key() const {
        for (size_t i = 0; i < layout.page_header->slot_count; ++i) {
            if (layout.slots[i].valid) {
                Tuple t = td.deserialize(buffer + layout.slots[i].offset);
                return extract_key(t);
            }
        }
        throw std::runtime_error("Empty node");
    }

    key_type max_key() const {
        for (int i = layout.page_header->slot_count - 1; i >= 0; --i) {
            if (layout.slots[i].valid) {
                Tuple t = td.deserialize(buffer + layout.slots[i].offset);
                return extract_key(t);
            }
        }
        throw std::runtime_error("Empty node");
    }
};
