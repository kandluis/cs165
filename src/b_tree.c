// Copyright 2015 <Luis Perez>

// As a general note, everything is implemented iteratively to avoid function call
// overhead whenever possible. If needed, we use tail recursion.

#include "include/b_tree.h"
#include "include/utils.h"

void bulk_load(Data* data, Data* pos, size_t n, Node* root) {
  // If we can fit into a single node, just do that.
  size_t capacity = CAPACITY * FANOUT;
  if (n <= capacity) {
    root->count = n;
    root->type = Leaf;
    root->next_link = NULL;
    root->children = calloc(1, sizeof(Node));
    root->children->count = n;
    root->children->type = Position;
    root->children->children = NULL;
    root->children->next_link = NULL;

    for(size_t i = 0; i < n; i++){
      root->children->keys[i] = pos[i];
      root->keys[i] = data[i];
    }

    return;
  }

  // Otherwise split the data by children and bulk load them first.
  // Note that our tree will be left leaning.
  size_t split_size = n / (capacity - 1);
  root->children = calloc(FANOUT, sizeof(Node));
  for (size_t i = 0; i < capacity && split_size * i < n; i++) {
    // Bulk load the first child!
    size_t start_index = split_size * i;
    size_t length = (n - start_index) ? n - start_index : split_size;
    bulk_load(&data[start_index], &pos[start_index], length, &root->children[i]);

    // Set-up the keys to the tree (note that equal values might be split.
    // we take care of this in search?
    // Note that this node contains all values < the key (if unique)
    // Otherwise it might contain duplicate <= values (screws with us!)
    if (start_index + length == n) {
      root->keys[i] = data[start_index + length - 1];
    }
    else {
      root->keys[i] = data[start_index + length];
    }


    // Add a link from the previous child (unless first child)
    if (i != 0 ) {
      // This links all the children internally together.
      root->children[i-1].next_link = &root->children[i];

      // Node link the contiguous chunks together by linking the last from the
      // previous group to the first of this group!
      if (root->children[i-1].children) {

        // Get the last one in the previous contiguous block!
        root->children[i-1].children[root->children[i-1].count - 1].next_link = root->children[i].children;
      }
    }
  }

  // Note that the root node has now become an internal node.
  root->type = Internal;
}


// Iterate over the root tree.
size_t find_element_tree(Data el, Node* root, Node** node) {
  // If we got a null pointer, something went very wrong!
  if (!root) {
    log_err("Root pointer cannot be null! %s: line %d\n", __func__, __LINE__);
    return 0;
  }

  // Find the index of our value in either the internal node or leaf node!
  size_t data_idx = find_index(root->keys, 0, root->count - 1, el, root->count);

  if (root->type == Leaf) {
    // If the data_idx is the col count, we return that index.
    if (data_idx == root->count) {
      *node = root;
      return data_idx;
    }
    // We return the value! For a leaf node, we must have
    // children set!
    if (!root->children) {
      log_err("Error. Children null in leaf node! %s: line %d\n", __func__, __LINE__);
      *node = NULL;
      return 0;
    }

    // Return the clustered index for the base data
    *node = root;
    return root->children->keys[data_idx].i;
  }


  // Otherwise, we're at an internal node, so we climb down the tree based on the
  // key values! (hopefully none are repeated?) by comparing the key to the value
  // we're looking for!
  // We found a key which is larger than all of our elements.
  if (data_idx >= root->count) {
    // This should not happen internally, but we return n.
    log_err("Found key larger than all elements.");
    *node = root;
    return data_idx;
  }
  Data key = root->keys[data_idx];

  // Look left. This should almost always be the case unless the values are equal!
  if (el.i < key.i) {
    return find_element_tree(el, &root->children[data_idx], node);
  }

  // Equal values, so we actually look in both directions.
  else if (el.i == key.i){
    log_info("Column contains duplicate values!");

    // Look right first because this is where we expect to find it.
    size_t res = find_element_tree(el, &root->children[data_idx], node);

    // Did not find in the left, so look right!
    if (!(*node)) {
      if (data_idx + 1 == root->count) {
        // This really should never happen...
        log_err("Internal node -- accessing pointer too far?");
      }
      res = find_element_tree(el, &root->children[data_idx + 1], node);
    }

    return res;
  }
  // With our current structure, this really should never happen.
  else {
    log_err("Found a value that was strictly larger than our data. %s, line %d\n",
      __func__, __LINE__);
    *node = NULL;
    return 0;
  }
}

Data get_min_key(Node* root) {
  // If at leaf, we're done.
  if (root->type == Leaf) {
    return root->keys[0];
  }

  // Traverse the left half!
  return get_min_key(&root->children[0]);
}

Data get_max_key(Node* root) {
  // If at leaf, we're done
  if (root->type == Leaf) {
    return root->keys[root->count - 1];
  }

  // Traverse the right half
  return get_max_key(&root->children[root->count - 1]);
}

Data get_min_value(Node* root) {
  // Leaf, so we're done
  if (root->type == Leaf) {
    return root->children->keys[0];
  }

  return get_min_value(&root->children[0]);
}

Data get_max_value(Node* root) {
  if (root->type == Leaf) {
    return root->children->keys[root->count - 1];
  }

  return get_max_value(&root->children[root->count - 1]);
}

void extract_data_leaf(Node* leaf, Data* keys, Data* values) {
  // When the leaf is null, we're done
  if (!leaf || leaf->count == 0) {
    return;
  }
  // We assume arrays are pre-allocated with enough space to fit data
  for(size_t i = 0; i < leaf->count; i++) {
    keys[i] = leaf->keys[i];
    values[i] = leaf->children->keys[i];
  }

  extract_data_leaf(leaf->next_link, keys + leaf->count, values + leaf->count);
}

void extract_data(Node* root, Data* keys, Data* values) {
  // Go all the way down until the bottom left!
  if (root->type == Leaf) {
    extract_data_leaf(root, keys, values);
  }
  else {
    extract_data(&root->children[0], keys, values);
  }
}

void free_btree(Node* root) {
  // If NULL, we're done!
  if (!root || root->count <= 0) {
    return;
  }

  // Free all of the children if we have any!
  if (root->children) {
    for(size_t i = 0; i < FANOUT; i++) {
      free_btree(&root->children[i]);
    }
    // Now we free our children (the contiguous block we had allocated)
    free(root->children);
  }

  // Only the root needs to free itself, so we do this outside the function.
}

void insert_tree(Node* root, Data key, Data value) {
  // Find the node where we need to insert this value!
  Node* node;
  size_t pos = find_element_tree(key, root, &node);

  // Then we just insert it.
  // TODO(luisperez): We need to fix this. We need to filter up
  // the values if the result doesn't fit into the node!

  // Now we insert into the specified position if we have enough space.
  if (node->count < FANOUT) {
    Data tmpKey;
    Data tmpValue;
    while (pos < node->count) {
      tmpKey = node->keys[pos];
      tmpValue = node->children->keys[pos];

      // Now copy over the current key/value.
      node->keys[pos] = key;
      node->children->keys[pos++] = value;

      // Now copy over.
      key = tmpKey;
      value = tmpValue;
    }

    // Insert final item
    node->keys[node->count++] = key;
    node->children->keys[node->children->count++] = value;

    // Now we're done!
  }
  else {
    log_err("Unimplemented!");
    return;
  }

}

void write_tree(FILE* fp, Node* root) {
  // Write out everything in binary as a struct!
  if (1 != fwrite(root, sizeof(Node), 1, fp)) {
    log_err("Failed at writing out node!!");
  }

  // Write out the pointers depending on type!
  if (root->type == Leaf) {
    if (1 != fwrite(root, sizeof(Node), 1, fp)) {
      log_err("Failed writing out values for leaf!");
    }
  }
  // Type is Internal!
  else if (root->type == Internal) {
    for (size_t i = 0; i < root->count; i++) {
      write_tree(fp, &root->children[i]);
    }
  }
  else {
    log_err("Node type not supported!");
  }
}

void read_tree(FILE* fp, Node* root) {
  // Read in the node
  if (1 != fread(root, sizeof(Node), 1, fp)) {
    log_err("Unable to read root node!");
  }

  // Check to see if it's a leaf node
  if (root->type == Leaf) {
    root->children = calloc(1, sizeof(Node));
    if (1 != fread(root->children, sizeof(Node*), 1, fp)) {
      log_err("Unable to read children node!");
    }
  }
  else if (root->type == Internal) {
    root->children = calloc(FANOUT, sizeof(Node));
    for (size_t i = 0; i < root->count; i++) {
      read_tree(fp, &root->children[i]);

      // Let's link all the children together!
      if (i != 0) {
        root->children[i-1].next_link = root->children;
      }

      // And let's link contiguous groups together
      if (root->children[i-1].children) {
        // Get the last one in the previous continguous chunk
        root->children[i-1].children[root->children[i-1].count - 1].next_link = root->children[i].children;
      }
    }
  }

  else {
    log_err("Node type is unsupported.");
  }
}