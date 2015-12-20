// Copyright (2015) - Luis Perez

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "utils.h"

#define ANSI_COLOR_RED     "\x1b[31m"
#define ANSI_COLOR_GREEN   "\x1b[32m"
#define ANSI_COLOR_RESET   "\x1b[0m"

void cs165_log(FILE* out, const char *format, ...) {
#ifdef LOG
    va_list v;
    va_start(v, format);
    vfprintf(out, format, v);
    va_end(v);
#else
    (void) out;
    (void) format;
#endif
}

void log_err(const char *format, ...) {
#ifdef LOG_ERR
    va_list v;
    va_start(v, format);
    fprintf(stderr, ANSI_COLOR_RED);
    vfprintf(stderr, format, v);
    fprintf(stderr, ANSI_COLOR_RESET);
    va_end(v);
#else
    (void) format;
#endif
}

void log_info(const char *format, ...) {
#ifdef LOG_INFO
    va_list v;
    va_start(v, format);
    fprintf(stdout, ANSI_COLOR_GREEN);
    vfprintf(stdout, format, v);
    fprintf(stdout, ANSI_COLOR_RESET);
    fflush(stdout);
    va_end(v);
#else
    (void) format;
#endif
}

void* resize(void* data, size_t osize, size_t nsize) {
    assert(osize <= nsize);
    void* ndata = malloc(sizeof(char) * nsize);
    memcpy(ndata, data, osize);
    return ndata;
}

// Copies a string. Returns pointer to new string.
char* copystr(const char* src) {
    char* tmp = malloc(sizeof(char) * (strlen(src) + 1));
    return strcpy(tmp, src);
}

size_t find_index(Data* array, size_t start, size_t end, Data el, size_t size) {
    // If only one element or if we've found the element.
    size_t mid = (start + end) / 2;
    if (start == end || el.i == array[mid].i) {
        // We can insert here because they are equal.
        if (array[start].i == el.i) {
            return start;
        }
        // Search before until we hit bottom or find element smaller.
        else if (el.i < array[start].i) {
            while (start != 0) {
                if (array[--start].i <= el.i) {
                    return start + 1;
                }
            }

            // Insert at the beginning of the array (so BAD!)
            return start;
        }
        // Search after until we hit top or find element larger
        else {
            while (start != size) {
                if (array[++start].i >= el.i) {
                    return start;
                }
            }
            // Insert at the end!
            return size;
        }
    }

    // Now we handle the recursive case!
    if (el.i < array[mid].i) {
        return find_index(array, start, (mid - 1 > start) ? mid - 1 : start, el, size);
    }
    else { // el.i > array[mid].i
        return find_index(array, (mid + 1 < end) ? mid + 1 : end, end, el, size);
    }
}

