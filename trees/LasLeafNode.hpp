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
 * Lazy Sorted LeafNode
 */
template <typename node_id_type, typename key_type, size_t split_per, size_t block_size>
class LasLeafNode {
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
        // SplitPolicy split_strategy;
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

    LasLeafNode() = default;

    // constructor that loads from an existing buffer
    explicit LasLeafNode(Page &page, const TupleDesc &td, size_t key_index)
    : buffer(page.data()),
        td(td),
          key_index(key_index) {
        base_header = reinterpret_cast<BaseHeader*>(buffer);
        page_header = reinterpret_cast<PageHeader*>(buffer + sizeof(BaseHeader));
        slots = reinterpret_cast<Slot*>(buffer + sizeof(BaseHeader) + sizeof(PageHeader));
    }

    LasLeafNode(Page &page, const TupleDesc& desc, size_t key, node_id_type id, node_id_type next_id, bool isCold)
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

    uint16_t get_slot_count() {
        return page_header->slot_count;
    }

    bool is_sorted() {
        return page_header->meta.isSorted;
    }

    key_type extract_key(const Tuple& t) const {
        return std::get<key_type>(t.get_field(key_index));
    }

    key_type min_key() const {
        return page_header->min_key;
    }

    key_type max_key() const {
        return page_header->max_key;
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

            if (op == OpType::Delete) {
                key_type tombstone_key = *reinterpret_cast<const key_type*>(buffer + slot.offset + sizeof(OpType));
                std::cout
                      << "Tombstone: key= " << tombstone_key
                      << std::endl;
                continue;
            }
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
            --page_header->size; // update does not count for size
            return true;
        }
        return false;
    }

    bool remove(const key_type& key) {
        // Size calculation for tombstone: OpType + key only
        const size_t tombstone_len = sizeof(OpType) + sizeof(key_type);

        if (!can_insert(tombstone_len)) return false;

        page_header->heap_end -= tombstone_len;
        buffer[page_header->heap_end] = static_cast<uint8_t>(OpType::Delete);

        // Write only the key directly after the OpType
        *reinterpret_cast<key_type*>(buffer + page_header->heap_end + sizeof(OpType)) = key;

        slots[page_header->slot_count] = {
            static_cast<uint16_t>(page_header->heap_end),
            static_cast<uint16_t>(tombstone_len)
        };

        --page_header->size;
        ++page_header->slot_count;

        return true;
    }

    // Append a delete marker
    bool erase(const key_type& key) {
        // Size calculation for tombstone: OpType + key only
        const size_t tombstone_len = sizeof(OpType) + sizeof(key_type);

        if (!can_insert(tombstone_len)) return false;

        page_header->heap_end -= tombstone_len;
        buffer[page_header->heap_end] = static_cast<uint8_t>(OpType::Delete);

        // Write only the key directly after the OpType
        *reinterpret_cast<key_type*>(buffer + page_header->heap_end + sizeof(OpType)) = key;

        slots[page_header->slot_count] = {
            static_cast<uint16_t>(page_header->heap_end),
            static_cast<uint16_t>(tombstone_len)
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

        // sorted and deduped, no tombstones, O(log n)
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

            if (op == OpType::Delete) {
                key_type tombstone_key = *reinterpret_cast<const key_type*>(buffer + slot.offset + sizeof(OpType));
                if (tombstone_key == key) {
                    // Tombstone with the same key found, return nullopt (key deleted)
                    return std::nullopt;
                }
                continue;  // Skip tombstones
            }
            Tuple t = td.deserialize(buffer + slot.offset + sizeof(OpType));
            if (extract_key(t) == key) {
                return t;
            }
        }
        return std::nullopt;
    }

    std::optional<Tuple> get_tuple(size_t i) const {
        const Slot &slot = slots[i];
        OpType op = static_cast<OpType>(buffer[slot.offset]);
        // std::unordered_set<key_type> tombstone_keys;

        if (op == OpType::Delete) {
            key_type tombstone_key = *reinterpret_cast<const key_type*>(buffer + slot.offset + sizeof(OpType));
            // tombstone_keys.insert(tombstone_key);
            return std::nullopt;  // Skip tombstones
        }
        Tuple t = td.deserialize(buffer + slot.offset + sizeof(OpType));
        // if (tombstone_keys.contains(extract_key(t)))
        //     return std::nullopt;
        return t;
    }

    std::vector<Tuple> get_range(const key_type &min_key, const key_type &max_key) const {
        std::vector<Tuple> result;
        if (this->min_key() > max_key) // stop if no result
            return result;
        if (page_header->meta.isSorted) {
            for (uint16_t i = 0; i < page_header->slot_count; ++i) {
                const Slot &slot = slots[i];
                Tuple t = td.deserialize(buffer + slot.offset + sizeof(OpType));
                // sorted data allows early cut
                if (extract_key(t) < min_key) {
                    continue;
                }
                if (extract_key(t) > max_key) {
                    return result;
                }
                result.push_back(t);
            }
        }else { // scan
            std::unordered_set<key_type> seen;          // To store keys we've already seen
            std::unordered_set<key_type> tombstones;    // To store keys that have been deleted

            for (int i = page_header->slot_count - 1; i >= 0; --i) {
                const Slot &slot = slots[i];

                OpType op = static_cast<OpType>(buffer[slot.offset]);
                if (op == OpType::Delete) {
                    key_type tombstone_key = *reinterpret_cast<const key_type*>(buffer + slot.offset + sizeof(OpType));
                    tombstones.insert(tombstone_key);
                    continue;
                }

                Tuple t = td.deserialize(buffer + slot.offset + sizeof(OpType));
                key_type k = extract_key(t);

                if (k < min_key || k > max_key) continue;

                if (seen.contains(k) || tombstones.contains(k)) continue;
                seen.insert(k);

                result.push_back(t);
            }
            std::ranges::reverse(result); // maintain sorted order
        }
        return result;
    }

    std::vector<Tuple> compact() {
        std::vector<Tuple> compacted;
        std::unordered_set<key_type> seen;
        std::unordered_set<key_type> tombstones;

        for (int i = page_header->slot_count - 1; i >= 0; --i) {
            const Slot& slot = slots[i];
            OpType op = static_cast<OpType>(buffer[slot.offset]);
            if (op == OpType::Delete) {
                key_type tombstone_key = *reinterpret_cast<const key_type*>(buffer + slot.offset + sizeof(OpType));
                tombstones.insert(tombstone_key);
                continue;  // Skip tombstones
            }

            Tuple t = td.deserialize(buffer + slot.offset + sizeof(OpType));
            key_type k = extract_key(t);

            if (!tombstones.contains(k) && !seen.contains(k)) {
                tombstones.insert(k);
                compacted.push_back(t);
            }
            seen.insert(k);
        }
        std::ranges::reverse(compacted);
        return compacted;
    }

    void sort() {
        // compact
        std::vector<Tuple> compacted = compact();

        // Before re-inserting into old page, clear all records
        page_header->slot_count = 0;
        page_header->heap_end = block_size;
        page_header->size = 0;
        page_header->min_key = std::numeric_limits<key_type>::max();
        page_header->max_key = std::numeric_limits<key_type>::min();

        std::sort(compacted.begin(), compacted.end(), [&](const Tuple& a, const Tuple& b) {
                return extract_key(a) < extract_key(b);
            });
        for (size_t i = 0; i < compacted.size(); ++i) insert(compacted[i]);
        page_header->meta.isSorted = true;
    }

    key_type split_into(LasLeafNode& new_leaf) {
        // compact
        std::vector<Tuple> compacted = compact();

        // Before re-inserting into old page, clear all records
        page_header->slot_count = 0;
        page_header->heap_end = block_size;
        page_header->size = 0;
        page_header->min_key = std::numeric_limits<key_type>::max();
        page_header->max_key = std::numeric_limits<key_type>::min();

        // quick partition by split key
        size_t idx = compacted.size() * 3 / 4;
        key_type split_key = extract_key(compacted[idx]);

        for (size_t i = 0; i < compacted.size(); ++i) {
            Tuple t = compacted[i];
            key_type key = extract_key(t);
            if (key < split_key) {
                this->insert(t);
            }else {
                new_leaf.insert(t);
            }
        }

        // restore linked list
        new_leaf.page_header->meta.next_id = page_header->meta.next_id;
        page_header->meta.next_id = new_leaf.page_header->id;

        return split_key;
    }

    void compute_min_max() {
        key_type new_min = std::numeric_limits<key_type>::max();
        key_type new_max = std::numeric_limits<key_type>::min();
        std::unordered_set<key_type> deleted;
        std::unordered_set<key_type> seen;          // To store keys we've already seen

        for (int i = static_cast<int>(page_header->slot_count) - 1; i >= 0; --i) {
            const Slot& slot = slots[i];
            OpType op = static_cast<OpType>(buffer[slots[i].offset]);
            if (op == OpType::Delete) {
                key_type tombstone_key = *reinterpret_cast<const key_type*>(buffer + slot.offset + sizeof(OpType));
                deleted.insert(tombstone_key);
                continue;  // Skip tombstones
            }

            Tuple t = td.deserialize(buffer + slot.offset + sizeof(OpType));
            key_type k = extract_key(t);
            if (seen.contains(k) || deleted.contains(k)) continue;
            new_min = std::min(new_min, k);
            new_max = std::max(new_max, k);
            seen.insert(k);

        }
        page_header->min_key = new_min;
        page_header->max_key = new_max;
    }
};
