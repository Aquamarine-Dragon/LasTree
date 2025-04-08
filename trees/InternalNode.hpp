#pragma once

#include <cstdint>
#include <algorithm>
#include <vector>
#include <stdexcept>
#include "NodeTypes.hpp"

template <typename node_id_type, typename key_type, size_t block_size>
class InternalNode {
private:
    struct PageHeader {
        node_id_type id;
        uint16_t size;  // number of keys
        uint16_t type;  // 1 for internal
    };

public:
    PageHeader* header;
    key_type* keys;
    node_id_type* children;
    static constexpr uint16_t internal_capacity = (block_size - sizeof(header) - sizeof(node_id_type)) / (sizeof(key_type) + sizeof(node_id_type));

    InternalNode() = default;

    InternalNode(void* buf) {
        header = static_cast<PageHeader*>(buf);
        header->type = 1;
        header->size = 0;
        keys = reinterpret_cast<key_type*>(header + 1);
        children = reinterpret_cast<node_id_type*>(keys + internal_capacity);
    }

    uint16_t child_slot(const key_type& key) const {
        auto it = std::upper_bound(keys, keys + header->size, key);
        return std::distance(keys, it);
    }
};

