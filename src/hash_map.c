// Copyright 2015 <Luis Perez>

// As a general note, everything is implemented iteratively to avoid function call
// overhead whenever possible. If needed, we use tail recursion.

#include <string.h>

#include "include/utils.h"
#include "include/hash_map.h"
#include "include/cs165_api.h"

// The has function used by this particular implementation.
// Source (http://stackoverflow.com/questions/7666509/hash-function-for-string)
unsigned long hash_function(const char* str)
{
    unsigned long hash = 5381;
    int c;
    while ((c = *str++)) {
        hash = ((hash << 5) + hash) + c; /* hash * 33 + c */
    }
    return hash;
}

// Adds an element to our modified linked list. Returns new head of list.
cont_node* add_element(cont_node* head, const char* key, void* value)
{
    // Check if current node is full or non-existent.
    cont_node* new_head = NULL;
    if (!head || head->count == head->size) {
        new_head = malloc(sizeof(struct cont_node));
        new_head->next = head;
        new_head->size = LINK_SIZE;
        new_head->count = 0;
    }
    else {
        new_head = head;
    }

    new_head->link[new_head->count].key = copystr(key);
    new_head->link[new_head->count++].value = value;
    return new_head;
}

// Returns pointer to the key,value pair with key. Returns null if not found.
kv_pair* find_element(cont_node* head, const char* key)
{
    // Empty.
    if (!head) {
        return NULL;
    }

    // Search this link.
    for (size_t i = 0; i < head->count; i++) {
        if (strcmp(head->link[i].key, key) == 0) {
            return &head->link[i];
        }
    }

    // Search next link.
    return find_element(head->next, key);
}

bool insert_into_map(hash_map* map, const char* key, void* value)
{
    if (find_in_map(map, key)) {
        return false;
    }
    unsigned int index = hash_function(key) % BUCKETS;
    map->buckets[index] = add_element(map->buckets[index], key, value);
    return true;
}

void* find_in_map(hash_map* map, const char* key)
{
    kv_pair* el = find_element(map->buckets[hash_function(key) % BUCKETS], key);
    return (el) ? el->value : el;
}

// Frees the linked list starting at head.
void clear_element(cont_node* head) {
    column* c;
    if (head) {
        for (size_t i = 0; i < head->count; i++) {
            free(head->link[i].key);

            // We cast all to columns and see if the name is set.
            c = (column *) head->link[i].value;
            if (!c->name) {
                // We also assume it is a column type, as all of our variables
                // are stored as columns.
                free(c->data);
            }

            free(head->link[i].value);
        }
        head->count = 0;

        clear_element(head->next);

        free(head);
    }
}

void clear_map(hash_map* map) {
    for (int i = 0; i < BUCKETS; i++) {
        clear_element(map->buckets[i]);
        map->buckets[i] = NULL;
    }
    map->size = 0;
}