#pragma once

#include <cstdint>
#include <algorithm>
#include <random>
#include <vector>
#include <unordered_set>
#include "NodeTypes.hpp"
#include "Tuple.hpp"

using namespace db;

/**
 * A LeafNode implementation for an append-only, LSM-like B+Tree node.
 * Supports insert and delete by appending new tuples with OpType.
 */
template <typename node_id_type, typename key_type, size_t split_per, size_t block_size>
class AppendOnlyLeafNode {
public:
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
        SplitPolicy split_strategy;
        key_type min_key;
        key_type max_key;
        size_t heap_end;
    };

    static constexpr uint16_t available_space = block_size - (sizeof(BaseHeader) + sizeof(PageHeader));

    uint8_t* buffer;
    const TupleDesc& td;
    size_t key_index{};

    BaseHeader* base_header;
    PageHeader* page_header;
    Slot* slots;

    AppendOnlyLeafNode() = default;

    // constructor that loads from an existing buffer
    explicit AppendOnlyLeafNode(Page &page, const TupleDesc &td, size_t key_index)
    : buffer(page.data()),
        td(td),
          key_index(key_index) {
        base_header = reinterpret_cast<BaseHeader*>(buffer);
        page_header = reinterpret_cast<PageHeader*>(buffer + sizeof(BaseHeader));
        slots = reinterpret_cast<Slot*>(buffer + sizeof(BaseHeader) + sizeof(PageHeader));
    }

    AppendOnlyLeafNode(Page &page, const TupleDesc& desc, size_t key, node_id_type id, node_id_type next_id,  SplitPolicy policy, bool isCold)
    : buffer(page.data()),
    td(desc),
    key_index(key)
     {
        base_header = reinterpret_cast<BaseHeader*>(buffer);
        page_header = reinterpret_cast<PageHeader*>(buffer + sizeof(BaseHeader));
        slots = reinterpret_cast<Slot*>(buffer + sizeof(BaseHeader) + sizeof(PageHeader));

        base_header->type = 0;
        page_header->id = id;
        page_header->meta.next_id = next_id;
        page_header->meta.isSorted = false;
        page_header->meta.isCold = isCold;
        page_header->size = 0;
        page_header->slot_count = 0;
        page_header->split_strategy = policy;
        page_header->min_key = std::numeric_limits<key_type>::max();
        page_header->max_key = std::numeric_limits<key_type>::min();
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

    key_type extract_key(const Tuple& t) const {
        return std::get<key_type>(t.get_field(key_index));
    }

    OpType get_op_type(size_t i) const {
        return static_cast<OpType>(buffer[slots[i].offset]);
    }

    size_t used_space() const {
        return block_size - page_header->heap_end + sizeof(Slot) * (page_header->slot_count);
    }

    bool can_insert(size_t tuple_len) const {
        size_t new_offset = page_header->heap_end - tuple_len;
        size_t end_offset = sizeof(BaseHeader) + sizeof(PageHeader) + sizeof(Slot) * (page_header->slot_count + 1);

        return new_offset >= end_offset;
    }

    void print_page_debug() const {
        std::cout << "  Slots (" << page_header->slot_count << "):" << std::endl;
        for (size_t i = 0; i < page_header->slot_count; ++i) {
            const auto& slot = slots[i];
            OpType op = static_cast<OpType>(buffer[slot.offset]);

            std::cout << "    [" << i << "]: offset=" << slot.offset
                      << ", length=" << slot.length
                      << ", op=" << (op == OpType::Insert ? "Insert" : "Delete")
                      << std::endl;
        }

        std::cout << "  Heap content:" << std::endl;
        for (size_t i = 0; i < page_header->slot_count; ++i) {
            const auto& slot = slots[i];
            OpType op = static_cast<OpType>(buffer[slot.offset]);
            Tuple t = td.deserialize(buffer + slot.offset + sizeof(OpType));
            std::cout << "    [" << i << "] "
                      << "(" << (op == OpType::Insert ? "Insert" : "Delete") << ") "
                      << td.to_string(t)
                      << std::endl;
        }
    }


    // Append an insert operation
    bool insert(const Tuple& t) {
        const size_t len = td.length(t) + sizeof(OpType);

        if (!can_insert(len)) {
            return false;
        }

        page_header->heap_end -= len;

        // write operation type
        buffer[page_header->heap_end] = static_cast<uint8_t>(OpType::Insert);
        // write tuple immediately after
        td.serialize(buffer + page_header->heap_end + sizeof(OpType), t);
        // append slot
        slots[page_header->slot_count] = {
            static_cast<uint16_t>(page_header->heap_end),
            static_cast<uint16_t>(len)
        };
        ++(page_header->slot_count);
        ++page_header->size;

        // update min max key
        key_type key = extract_key(t);
        if (key < page_header->min_key) {
            page_header->min_key = key;
        }
        if (key > page_header->max_key) {
            page_header->max_key = key;
        }
        // update to be unsorted
        page_header->meta.isSorted = false;
        return true;
    }

    bool update(const Tuple& t) {
        if (insert(t)) {// Simply append a new version
            ++page_header->size; // update does not count for size
            return true;
        }
        return false;
    }

    // Append a delete marker
    bool erase(const key_type& key) {
        Tuple tombstone(td.size());
        // Fill only key field (others don't matter)
        tombstone.set_field(key_index, key);

        const size_t len = td.length(tombstone) + sizeof(OpType);
        if (!can_insert(len)) return false;

        page_header->heap_end -= len;
        buffer[page_header->heap_end] = static_cast<uint8_t>(OpType::Delete);
        td.serialize(buffer + page_header->heap_end + sizeof(OpType), tombstone);

        slots[page_header->slot_count] = {
            static_cast<uint16_t>(page_header->heap_end),
            static_cast<uint16_t>(len)
        };
        --page_header->size;
        ++page_header->slot_count;

        if (key == page_header->min_key || key == page_header->max_key) {
            compute_min_max();
        }
        return true;
    }

    // Binary search based on keys in slots (only usable when leaf is sorted!!!)
    uint16_t value_slot(const key_type &key) const {
        // Binary search if sorted
        uint16_t left = 0, right = page_header->slot_count;
        while (left < right) {
            uint16_t mid = (left + right) / 2;
            Slot cur = slots[mid];
            Tuple mid_tuple = td.deserialize(buffer + slots[mid].offset+ sizeof(OpType));
            if (extract_key(mid_tuple) < key) left = mid + 1;
            else right = mid;
        }
        return left;
    }

    // Find the most recent value for key (or tombstone)
    std::optional<Tuple> get(const key_type& key) const {

        // sorted and deduped, no delete, O(log n)
        if (page_header->meta.isSorted){
            uint16_t index = value_slot(key);

            if (index < page_header->slot_count) {
                const Slot &slot = slots[index];
                Tuple t = td.deserialize(buffer + slot.offset+ sizeof(OpType));
                if (extract_key(t) == key) {
                    return t;
                }
            }
            return std::nullopt;
        }

        // unsorted, O(n)
        for (int i = static_cast<int>(page_header->slot_count) - 1; i >= 0; --i) {
            const Slot& slot = slots[i];
            OpType op = static_cast<OpType>(buffer[slot.offset]);
            Tuple t = td.deserialize(buffer + slot.offset + sizeof(OpType));
            if (extract_key(t) == key) {
                if (op == OpType::Delete) return std::nullopt;
                return t;
            }
        }
        return std::nullopt;
    }

    Tuple get_tuple(size_t i) const {
        const Slot &slot = slots[i];
        return td.deserialize(buffer + slot.offset + sizeof(OpType));
    }

    std::vector<Tuple> compact() {
        std::vector<Tuple> compacted;
        std::unordered_set<key_type> tombstones;

        for (int i = page_header->slot_count - 1; i >= 0; --i) {
            const Slot& slot = slots[i];
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

    key_type split_into(AppendOnlyLeafNode& new_leaf) {
        // compact
        std::vector<Tuple> compacted = compact();

        // Before re-inserting into old page, clear all records
        page_header->slot_count = 0;
        page_header->heap_end = block_size;
        page_header->size = 0;
        page_header->min_key = std::numeric_limits<key_type>::max();
        page_header->max_key = std::numeric_limits<key_type>::min();

        // split
        if (page_header->split_strategy == SplitPolicy::QUICK_PARTITION) {
            // policy 1: quick partition by split key
            size_t idx = compacted.size() * 3 / 4;
            key_type split_key = extract_key(compacted[idx]);
            for (const Tuple& t : compacted) {
                if (extract_key(t) < split_key)
                    this->insert(t); // back to old page
                else
                    new_leaf.insert(t); // to new page
            }
            // restore linked list
            new_leaf.page_header->meta.next_id = page_header->meta.next_id;
            page_header->meta.next_id = new_leaf.page_header->id;
            return split_key;
        }else {
            // policy 2: sort on split
            std::sort(compacted.begin(), compacted.end(), [&](const Tuple& a, const Tuple& b) {
                return extract_key(a) < extract_key(b);
            });
            // todo modify percentage
            size_t half = compacted.size() * 3 / 4;
            for (size_t i = 0; i < half; ++i) insert(compacted[i]);
            for (size_t i = half; i < compacted.size(); ++i) new_leaf.insert(compacted[i]);
            page_header->meta.isSorted = true;
            new_leaf.page_header->meta.isSorted = true;

            // restore linked list
            new_leaf.page_header->meta.next_id = page_header->meta.next_id;
            page_header->meta.next_id = new_leaf.page_header->id;

            return new_leaf.min_key();
        }
    }

    void compute_min_max() {
        key_type new_min = std::numeric_limits<key_type>::max();
        key_type new_max = std::numeric_limits<key_type>::min();
        std::unordered_set<key_type> deleted;

        for (int i = static_cast<int>(page_header->slot_count) - 1; i >= 0; --i) {
            OpType op = static_cast<OpType>(buffer[slots[i].offset]);
            Tuple t = td.deserialize(buffer + slots[i].offset + sizeof(OpType));
            key_type k = extract_key(t);

            if (op == OpType::Delete) {
                deleted.insert(k);
            } else if (!deleted.contains(k)) {
                new_min = std::min(new_min, k);
                new_max = std::max(new_max, k);
            }
        }

        page_header->min_key = new_min;
        page_header->max_key = new_max;
    }


    key_type min_key() const {
        return page_header->min_key;
    }

    key_type max_key() const {
        return page_header->max_key;
    }

};
