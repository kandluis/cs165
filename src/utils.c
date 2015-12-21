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
    void* ndata = calloc(nsize, sizeof(char));
    memcpy(ndata, data, osize);
    return ndata;
}

// Copies a string. Returns pointer to new string.
char* copystr(const char* src) {
    char* tmp = calloc(strlen(src) + 1, sizeof(char));
    return strcpy(tmp, src);
}

size_t find_index(Data* array, size_t start, size_t end, Data el, size_t size) {
    // In the case of an empty array, automatically return.
    if (size <= 0 || end < start) {
        return size;
    }

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
        return find_index(array, start, (mid > start + 1) ? mid - 1 : start, el, size);
    }
    else { // el.i > array[mid].i
        return find_index(array, (mid + 1 < end) ? mid + 1 : end, end, el, size);
    }
}

// Merges the subsections of the input data array.
// start - the starting index of the left side
// end - the final (not-inclusive) index of the right
// mid - the starting index of the right.
// Keeps track of swapping positions using pos, which is
//      assumed to mirror A.
// Assumes data contains integers.
void merge(Data* A, Data* pos, int start, int mid, int end) {
    // Assume that [start, mid] sorted and [mid + 1,end] sorted.
    int i = start;
    int j = mid + 1;
    int k = 0;

    // Allocate space to temporarily hold the results.
    Data* tpos;
    Data* tmp = calloc((end - start + 1), sizeof(Data));
    while (i <= mid && j <= end) {
        if (A[i].i < A[j].i) {
            tmp[k++] = A[i++];
        }
        else {
            tmp[k++] = A[j++];
        }
    }

    // Iterate over the i
    while (i <= mid) {
        tmp[k++] = A[i++];
    }

    // Iterate over the j
    while (j <= end) {
        tmp[k++] = A[j++];
    }

    int total = k;

    // Merge the positions if they exist!
    if (pos) {
        i = start;
        j = mid + 1;
        k = 0;
        tpos = calloc((end - start + 1), sizeof(Data));

        while (i <= mid && j <= end) {
            if (A[i].i < A[j].i) {
                tpos[k++] = pos[i++];
            }
            else {
                tpos[k++] = pos[j++];
            }
        }

        // Iterate over the i
        while (i <= mid) {
            tpos[k++] = pos[i++];
        }

        // Iterate over the j
        while (j <= end) {
            tpos[k++] = pos[j++];
        }

        // Now we copy the results back into A assuming the spaces are continguous.
        while (--k >= 0) {
            pos[start + k] = tpos[k];
        }

        free(tpos);
    }

    // Now we copy the results back into A assuming the spaces are continguous.
    while (--total >= 0) {
        A[start + total] = tmp[total];
    }

    free(tmp);
}

void mergesort(Data* A, Data* pos, int start, int end) {
    // Array of length 1 is already sorted.
    if (end - start + 1 < 2) {
        return;
    }

    // Otherwise split in half.
    int mid = (end + start) / 2;

    // Mergesort each half
    mergesort(A, pos, start, mid);
    mergesort(A, pos, mid + 1, end);

    // And merge the two
    merge(A, pos, start, mid, end);
}

column* xrange(size_t n){
    column* res = calloc(1, sizeof(struct column));
    res->data = calloc(n, sizeof(Data));
    for (size_t i = 0; i < n; i++) {
        res->data[i].i = i;
    }
    res->type = INT;
    res->count = n;
    res->size = n;

    return res;
}

