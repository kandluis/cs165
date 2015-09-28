// var_store.h
// CS165 Fall 2015
//
// Copyright 2015 <Luis Perez>
//
// Data structure to track created variables.
#ifndef SRC_INCLUDE_VAR_STORE_H_
#define SRC_INCLUDE_VAR_STORE_H_

// Looks up the pointer to the value for var. Returns NULL if not set.
void* get_var(const char* var);

// Sets the var to the pointer for value. Failures occur only catastrophically.
void set_var(const char* var, void* value);

#endif  // SRC_INCLUDE_VAR_STORE_H_
