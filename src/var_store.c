// Copyright 2015 <Luis Perez>
//
// General Note: Uses hash_maps for storage.

#include "include/hash_map.h"
#include "include/utils.h"
#include "include/var_store.h"

// Global data structure to keep track of variables.
hash_map var_mapping;

// Global data structure to keep track of resources.
hash_map resource_mapping;

void* get_var(const char* var)
{
  return find_in_map(&var_mapping, var);
}

// Could fail catastrophically if we run out of memory.
void set_var(const char* var, void* value)
{
  // We log failures.
  if (!insert_into_map(&var_mapping, var, value)) {
     log_err("Failed to set variable %s. Likely causes are low memory.", var);
  }
}

void* get_resource(const char* var)
{
  return find_in_map(&resource_mapping, var);
}

void set_resource(const char* var, void* value)
{
  // We log failures.
  if (!insert_into_map(&resource_mapping, var, value)) {
     log_err("Failed to set resource %s. Likely causes are low memory.", var);
  }
}

// TODO -- Determine what needs to be freed...
void clear_vars(void) {
  clear_map(&var_mapping);
}

void clear_resources(void) {
  clear_map(&resource_mapping);
}
