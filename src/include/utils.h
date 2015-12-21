// utils.h
// CS165 Fall 2015
//
// Provides utility and helper functions that may be useful throughout.
// Includes debugging tools.

#ifndef __UTILS_H__
#define __UTILS_H__

#include <stdarg.h>
#include <stdio.h>

#include "cs165_api.h"

#define TERMINATE_LOAD "EOF"
#define SHUTDOWN_MESSAGE "SHUTDOWN"



// cs165_log(out, format, ...)
// Writes the string from @format to the @out pointer, extendable for
// additional parameters.
//
// Usage: cs165_log(stderr, "%s: error at line: %d", __func__, __LINE__);
void cs165_log(FILE* out, const char *format, ...);

// log_err(format, ...)
// Writes the string from @format to stderr, extendable for
// additional parameters. Like cs165_log, but specifically to stderr.
//
// Usage: log_err("%s: error at line: %d", __func__, __LINE__);
void log_err(const char *format, ...);

// log_info(format, ...)
// Writes the string from @format to stdout, extendable for
// additional parameters. Like cs165_log, but specifically to stdout.
// Only use this when appropriate (e.g., denoting a specific checkpoint),
// else defer to using printf.
//
// Usage: log_info("Command received: %s", command_string);
void log_info(const char *format, ...);

// Takes a pointer to data and resizes it to a new contiguous section of memory.
// osize is the old size (in bytes) of the data to be copied and nsize is the size
// of the new space. Returns a pointer to the new section of memory.
// data must not be NULL.
void* resize(void* data, size_t osize, size_t nsize);

// Takes a pointer to a string and creates a copy. The caller is responsible for
// freeing the memory at the return pointer location.
char* copystr(const char* src);

// Binary searches the array for the given element and returns
// the index at which it should be inserted to maintain sorted order.
// The array is restricted to [start,end] (inclusive).
// Last parameter is the total size of th ENTIRE array
// Returns the smallest index possible for insertion to mainted sortedness.
size_t find_index(Data* array, size_t start, size_t end, Data el, size_t size);

// Recursive function to sort a column along with an array of positions.
// Ignores pos if it is NULL and then behaves as normal mergesort.
void mergesort(Data* A, Data* pos, int start, int end);

// Similar to the xrange function in python, allocates a
// column with 0...n-1 integers.
column* xrange(size_t n);

#endif /* __UTILS_H__ */
