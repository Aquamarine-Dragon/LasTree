#pragma once

#include <cstdint>
#include <algorithm>
#include <vector>
#include <stdexcept>
#include "NodeTypes.hpp"

using namespace db;

template <typename node_id_type, typename key_type, size_t block_size>
class InternalNode {
private:
    struct PageHeader {
        node_id_type id;
        uint16_t size;  // number of keys
    };

public:
    uint8_t *buffer; // address of the page that stores this page
    BaseHeader* base_header;
    PageHeader* header;
    key_type* keys;
    node_id_type* children;
    static constexpr uint16_t internal_capacity = (block_size - sizeof(BaseHeader) - sizeof(PageHeader) - sizeof(node_id_type)) / (sizeof(key_type) + sizeof(node_id_type));

    InternalNode() = default;

    explicit InternalNode(Page &page) {
        load(page);
    }

    InternalNode(Page &page,  node_id_type id) : buffer(page.data()){
        base_header = reinterpret_cast<BaseHeader*>(buffer);
        header = reinterpret_cast<PageHeader*>(base_header + 1);
        base_header->type = 1;
        header->id = id;
        header->size = 0;
        keys = reinterpret_cast<key_type*>(header + 1);
        children = reinterpret_cast<node_id_type*>(keys + internal_capacity);
    }

    void load(Page &page){
        buffer = page.data();
        base_header = reinterpret_cast<BaseHeader*>(buffer);
        header = reinterpret_cast<PageHeader*>(base_header + 1);
        keys = reinterpret_cast<key_type*>(header + 1);
        children = reinterpret_cast<node_id_type*>(keys + internal_capacity);
    }


    uint16_t child_slot(const key_type& key) const {
        auto it = std::upper_bound(keys, keys + header->size, key);
        return std::distance(keys, it);
    }

    void copyInfoFrom(const InternalNode& other) {
        this->header->size = other.header->size;
        std::memcpy(this->keys, other.keys, other.header->size * sizeof(key_type));
        std::memcpy(this->children, other.children, (other.header->size + 1) * sizeof(node_id_type));
    }

};

