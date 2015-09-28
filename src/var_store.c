// Copyright 2015 <Luis Perez>
//
// General Note: Uses hash_maps for storage.

#include "include/hash_map.h"
#include "include/utils.h"
#include "include/var_store.h"

// Global data structure to keep track of variables.
hash_map var_mapping;

void* get_var(char* var)
{
  return find_in_map(&var_mapping, var);
}

// Could fail catastrophically if we run out of memory.
void set_var(char* var, void* value)
{
  // We log failures.
  if (!insert_into_map(&var_mapping, var, value)) {
     log_err("Failed to set variable %s. Likely causes are low memory.", var);
  }
}