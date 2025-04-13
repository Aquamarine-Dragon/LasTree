#pragma once

#include <cstdint>
#include <algorithm>
#include <vector>
#include <stdexcept>
#include <unordered_set>
#include "NodeTypes.hpp"
#include "PageLayout.hpp"
#include "Tuple.hpp"

using namespace db;

/**
 * A LeafNode implementation for an append-only, LSM-like B+Tree node.
 * Supports insert and delete by appending new tuples with OpType.
 */
template <typename node_id_type, typename key_type, size_t block_size>
class LeafNodeLSM {
public:
    static constexpr size_t BLOCK_SIZE = block_size;
    static constexpr size_t MAX_SLOTS = 256;

    enum class OpType : uint8_t {
        Insert,
        Delete
    };

    struct Slot {
        uint16_t offset;
        uint16_t length;
    };

    struct leaf_info {
        node_id_type next_id;
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

    uint8_t* buffer;
    const TupleDesc& td;
    size_t key_index{};
    SplitPolicy split_strategy;
    Layout layout;

    LeafNodeLSM() = default;

    // constructor that loads from an existing buffer
    explicit LeafNodeLSM(Page &page, const TupleDesc &td, size_t key_index)
    : buffer(page.data()),
    layout(buffer),
    td(td),
          key_index(key_index) {
    }

    LeafNodeLSM(Page &page, const TupleDesc& desc, size_t key, node_id_type id, SplitPolicy policy,  node_id_type next_id, bool isCold)
    : buffer(page.data()),
    layout(Layout(buffer)),
    td(desc),
    key_index(key),
    split_strategy(policy) {
        layout.base_header->type = 0;
        layout.page_header->id = id;
        layout.page_header->size = 0;
        layout.page_header->meta.next_id = next_id;
        layout.page_header->meta.isCold = isCold;
        layout.page_header->slot_count = 0;
        layout.page_header->meta = {0, false, false};
        layout.heap_end[0] = block_size;
    }

    size_t free_space() const {
        return layout.free_space();
    }

    key_type extract_key(const Tuple& t) const {
        return std::get<key_type>(t.get_field(key_index));
    }

    // Append an insert operation
    bool insert(const Tuple& t) {
        const size_t len = td.length(t) + sizeof(OpType);
        if (free_space() < len + sizeof(Slot)) return false;

        *layout.heap_end -= len;
        buffer[*layout.heap_end] = static_cast<uint8_t>(OpType::Insert);
        td.serialize(buffer + *layout.heap_end + sizeof(OpType), t);

        layout.slots[layout.page_header->slot_count++] = {
            static_cast<uint16_t>(*layout.heap_end),
            static_cast<uint16_t>(len)
        };
        ++layout.page_header->size;
        return true;
    }

    bool update(const Tuple& t) {
        return insert(t); // Simply append a new version
    }

    // Append a delete marker
    bool erase(const key_type& key) {
        Tuple tombstone(td.size());
        // Fill only key field (others don't matter)
        tombstone.set_field(key_index, key);

        const size_t len = td.length(tombstone) + sizeof(OpType);
        if (free_space() < len + sizeof(Slot)) return false;

        *layout.heap_end -= len;
        buffer[*layout.heap_end] = static_cast<uint8_t>(OpType::Delete);
        td.serialize(buffer + *layout.heap_end + sizeof(OpType), tombstone);

        layout.slots[layout.page_header->slot_count++] = {
            static_cast<uint16_t>(*layout.heap_end),
            static_cast<uint16_t>(len)
        };
        ++layout.page_header->size;
        return true;
    }

    // Find the most recent value for key (or tombstone)
    std::optional<Tuple> get(const key_type& key) const {
        for (int i = static_cast<int>(layout.page_header->slot_count) - 1; i >= 0; --i) {
            const Slot& slot = layout.slots[i];
            OpType op = static_cast<OpType>(buffer[slot.offset]);
            Tuple t = td.deserialize(buffer + slot.offset + sizeof(OpType));
            if (extract_key(t) == key) {
                if (op == OpType::Delete) return std::nullopt;
                return t;
            }
        }
        return std::nullopt;
    }

    bool is_nearly_full() const {
        return free_space() < BLOCK_SIZE * 0.1;
    }

    std::vector<Tuple> compact() {
        std::vector<Tuple> compacted;
        std::unordered_set<key_type> tombstones;

        for (int i = layout.page_header->slot_count - 1; i >= 0; --i) {
            const Slot& slot = layout.slots[i];
            OpType op = static_cast<OpType>(buffer[slot.offset]);
            Tuple t = td.deserialize(buffer + slot.offset + sizeof(OpType));
            key_type k = extract_key(t);

            if (op == OpType::Delete) {
                tombstones.insert(k);
            } else if (!tombstones.contains(k)) {
                tombstones.insert(k);
                compacted.push_back(t);
            }
        }
        std::ranges::reverse(compacted);
        return compacted;
    }

    key_type choose_split_key(const std::vector<Tuple>& tuples) {
        std::vector<key_type> keys;
        for (const auto& t : tuples) keys.push_back(extract_key(t));
        size_t mid = keys.size() * 3 / 4;
        std::nth_element(keys.begin(), keys.begin() + mid, keys.end());
        return keys[mid];
    }

    std::pair<key_type, node_id_type> split_into(LeafNodeLSM& new_leaf) {
        // compact
        std::vector<Tuple> compacted = compact();

        // Before re-inserting into old page, clear all records
        layout.page_header->slot_count = 0;
        *layout.heap_end = BLOCK_SIZE;
        layout.page_header->size = 0;

        // split
        if (split_strategy == SplitPolicy::QUICK_PARTITION) {
            // policy 1: quick partition by split key
            key_type split_key = choose_split_key(compacted);
            for (const Tuple& t : compacted) {
                if (extract_key(t) < split_key)
                    this->insert(t); // back to old page
                else
                    new_leaf.insert(t); // to new page
            }
        }else {
            // policy 2: sort on split
            std::sort(compacted.begin(), compacted.end(), [&](const Tuple& a, const Tuple& b) {
                return extract_key(a) < extract_key(b);
            });
            // todo modify percentage
            size_t half = compacted.size() * 3 / 4;
            for (size_t i = 0; i < half; ++i) insert(compacted[i]);
            for (size_t i = half; i < compacted.size(); ++i) new_leaf.insert(compacted[i]);
        }
        // restore linked list
        new_leaf.layout.page_header->meta.next_id = layout.page_header->meta.next_id;
        layout.page_header->meta.next_id = new_leaf.layout.page_header->id;

        return { new_leaf.min_key(), new_leaf.layout.page_header->id };
    }

    key_type min_key() const {
        std::optional<key_type> min;

        for (int i = static_cast<int>(layout.page_header->slot_count) - 1; i >= 0; --i) {
            const Slot& slot = layout.slots[i];
            OpType op = static_cast<OpType>(buffer[slot.offset]);
            if (op == OpType::Delete) continue;
            Tuple t = td.deserialize(buffer + slot.offset + sizeof(OpType));
            key_type k = extract_key(t);
            if (!min.has_value() || k < min.value()) min = k;
        }
        if (!min.has_value()) throw std::runtime_error("Empty node");
        return min.value();
    }


    key_type max_key() const {
        std::optional<key_type> max;

        for (int i = static_cast<int>(layout.page_header->slot_count) - 1; i >= 0; --i) {
            const Slot& slot = layout.slots[i];
            OpType op = static_cast<OpType>(buffer[slot.offset]);
            if (op == OpType::Delete) continue;
            Tuple t = td.deserialize(buffer + slot.offset + sizeof(OpType));
            key_type k = extract_key(t);
            if (!max.has_value() || k > max.value()) max = k;
        }
        if (!max.has_value()) throw std::runtime_error("Empty node");
        return max.value();
    }

};
