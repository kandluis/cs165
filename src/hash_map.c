#include "hash_map.h"

// The has function used by this particular implementation.
// Source (http://stackoverflow.com/questions/7666509/hash-function-for-string)
unsigned long hash(unsigned char* str) {
    unsigned long hash = 5381;
    int c;

    while (c = *str++)
        hash = ((hash << 5) + hash) + c; /* hash * 33 + c */

    return hash;
}

//
bool add_element(cont_node* node, const) {

}

bool insert(hash_map* map, const char* key, const void* value) {

}