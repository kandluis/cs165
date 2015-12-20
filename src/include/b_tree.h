// hash_map.h
// CS165 Fall 2015
//
// Copyright 2015 <Luis Perez>
//
// Provides access to a (string, ptr) value pairing mapping strings to pointers.
// We implement a hash map with chaining (modified for sequential access)

#ifndef SRC_INCLUDE_B_TREE_H_
#define SRC_INCLUDE_B_TREE_H_

#include <stdlib.h>

#include "cs165_api.h"

// Fanout is large enough so that we can fit an entire node into a single cache line.
// Using:
// getconf LEVEL1_DCACHE_LINESIZE
// We determine that the cache line size is 64 bytes (on my machine).
// Running the command lscpu gives the below results.
/**
    Architecture:          x86_64
    CPU op-mode(s):        32-bit, 64-bit
    Byte Order:            Little Endian
    CPU(s):                4
    On-line CPU(s) list:   0-3
    Thread(s) per core:    2
    Core(s) per socket:    2
    Socket(s):             1
    NUMA node(s):          1
    Vendor ID:             GenuineIntel
    CPU family:            6
    Model:                 58
    Model name:            Intel(R) Core(TM) i7-3517U CPU @ 1.90GHz
    Stepping:              9
    CPU MHz:               1094.343
    CPU max MHz:           3000.0000
    CPU min MHz:           800.0000
    BogoMIPS:              4788.99
    Virtualization:        VT-x
    L1d cache:             32K
    L1i cache:             32K
    L2 cache:              256K
    L3 cache:              4096K
    NUMA node0 CPU(s):     0-3
**/
// With the above, and given that Data type is of size 64 bits (8 bytes),
// the size of a node is going to be FANOUT * 8 + 4*8 (upper estimate)
// This gives an optimal FANOUT of
// So we can fit it into L1 cash.
#define FANOUT 4 * 1023

// The load capacity ratio for each node in the tree.
#define CAPACITY 0.8

typedef enum NodeType {
  Internal,
  Leaf,
  Position
} NodeType;

/**
 * A Node for A B+ tree
 * Data keys[NODE_SIZE] is just an array of keys used for searching.
 * size_t count is the current number of keys in the array.
 * NodeType type is the NodeType of this node.
 * Node* children points to
 *  : the first node in the contiguous chunk of memory with the children
 *  : a node corresponding to this node's keys which contains their values in the keys array.
 * Node* next_link points to:
 *  : NULL if the node is an internal type (we link at every level)
 *  : Link to the next node if a leave node (we're at the leave level)

**/

typedef struct Node {
  Data keys[FANOUT];
  size_t count;
  NodeType type;
  struct Node* children;
  struct Node* next_link;
} Node;

// Stores a pointer to the leaf node containing the
// desired element in node. If the element if not found, we find the
// element <= to it that is in the tree.
// The function returns the index of the retrieved element in the stored node.
// Root is the root of the binary tree we're searching for.
size_t find_element_tree(Data el, Node* root, Node** node);


// Bulk load.
// Given a sorted Data array and corresponding pos array and a size of the array,
// bulk loads it into a B+ tree. We assume that root points to the root of
// the tree into which we want to load the data! It creates copies of the input data,
// so the given arrays can be freed after use.
// The tree is rooted at the location pointed to by root, which we
// assume is already allocated. No error checking is performed.
void bulk_load(Data* data, Data* pos, size_t n, Node* root);


// Return the minumum key in the tree.
Data get_min_key(Node* root);

// Returns the maximum key in the tree.
Data get_max_key(Node* root);

// Return the minumum value in the tree.
Data get_min_value(Node* root);

// Returns the maximum value in the tree.
Data get_max_value(Node* root);

// Extract the data from the btree -- they keys are placed in the
// array pointed to by keys and the values into that pointed to
// by values! Note that keys and values are assumed to be pre-allocated
// arrays with enough space to fit the data.
void extract_data(Node* root, Data* keys, Data* values);

// Frees a btree!
void free_btree(Node* root);

// Insert a key-value pair into a b-tree. Try to keep it balanced!
void insert_tree(Node* root, Data key, Data value);

#endif  // SRC_INCLUDE_B_TREE_H_