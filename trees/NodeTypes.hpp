#ifndef NODETYPES_HPP
#define NODETYPES_HPP

#pragma once

// Node types used in B+Tree
enum bp_node_type {
    LEAF, // 0
    INTERNAL // 1
};

// Shared base header to access `type` without knowing actual node structure
struct BaseHeader {
    uint16_t type;
};

#endif //NODETYPES_HPP
