// hash_map.h
// CS165 Fall 2015
//
// Copyright 2015 <Luis Perez>
//
// Provides access to a (string, ptr) value pairing mapping strings to pointers.
// We implement a hash map with chaining (modified for sequential access)

#ifndef SRC_INCLUDE_HASH_MAP_H_
#define SRC_INCLUDE_HASH_MAP_H_

#include <stdbool.h>
#include <stdlib.h>

#define LINK_SIZE 1024  // Size of a continuous link.
#define BUCKETS 1024  // Size of the hashmap.

/**
* A kv_pair is a (key,value) pair.
* The key is  a string and the value is a generic pointer to the data associated
* with the key. The data must persist the data_structure.
**/
typedef struct kv_pair {
  char* key;
  void* value;
} kv_pair;

/**
* A cont_node is similar to a node in a linked list but stores a sequence of
* kv_pairs contiguously.
**/
typedef struct cont_node {
  size_t size;  // Maximum number of kv_pairs.
  size_t count; // Filled lsots.
  kv_pair link[LINK_SIZE];
  struct cont_node* next;  // Next data structure.
} cont_node;

/**
* A hash_map structure hashes the keys into buckets.
**/
typedef struct hash_map {
  size_t size;
  cont_node* buckets[BUCKETS];
} hash_map;

/**
* Given a map and a key,value pair, associates the key with the value.
* Returns true on success, false if they key is already present.
**/
bool insert_into_map(hash_map* map, char* key, void* value);

/**
* Given a map and a key, returns a pointer to the associated value if found in
* the hash_map. Returns null if not found.
*/
void* find_in_map(hash_map* map, char* key);

#endif  // SRC_INCLUDE_HASH_MAP_H_
