// Copyright 2015 <Luis Perez>

#include <assert.h>

#include "db.h"
#include "include/b_tree.h"
#include "include/common.h"
#include "include/utils.h"
#include "include/var_store.h"

status global;

// TODO(USER): Here we provide an incomplete implementation of the create_db.
// There will be changes that you will need to include here.
status create_db(const char* db_name, db** db) {
    // Store status for return.
    status s;

    if (*db == NULL) {
        int attempts = 0;
        while (!(*db = calloc(1, sizeof(struct db))) && attempts < MAX_ATTEMPTS) {
          log_info("Database %s creation failed. Attempt %i.\n", db_name, attempts);
          attempts++;
        }
        // We have failed all attempts
        if (attempts == MAX_ATTEMPTS) {
            log_err("Database %s creation failed after repeated attempts! %s: error at line %d\n",
                db_name, __func__, __LINE__);
            s.code = ERROR;
            return s;
        }
    }

    // Set up the database.
    (*db)->name = copystr(db_name);
    (*db)->table_count = 0;
    (*db)->tables_available = 0;
    (*db)->tables = NULL;

    s.code = OK;
    return s;
}

// Loads the data belong to tbl into table. tbl must have all parameters set
// except the columns, which are loaded here.
// Metadata is a file ponter to the metadata file.
status load_table(FILE* metadata, table* tbl) {
    // Allocate space for the columns
    tbl->col = calloc(1, sizeof(struct column*) * tbl->table_size);

    char buffer[DEFAULT_ARRAY_SIZE];

    // Read in each column.
    FILE* fp;
    status ret;
    for (size_t i = 0; i < tbl->table_size; i++) {
        column* col = calloc(1, sizeof(struct column));
        if (fscanf(metadata,
            (i < tbl->table_size - 1) ? " %s %zu " : " %s %zu\n",
            buffer, &col->count) != 2) {
            log_err("Could not read from metadata file for column loading");
            ret.code = ERROR;
            ret.error_message = "Could read file.\n";
            return ret;
        }
        col->size = col->count;
        col->name = copystr(buffer);

        // Read in the data!
        col->data = calloc(col->size, sizeof(Data));
        sprintf(buffer, "%s/%s.data", DATA_FOLDER, col->name);
        fp = fopen(buffer, "rb");
        if (!fp) {
            log_err("Could not open columnar file %s.", buffer);
            ret.code = ERROR;
            ret.error_message = "Could not open columnar file.\n";
            return ret;
        }
        if (col->count != fread(col->data, sizeof(Data), col->count, fp)) {
            log_err("Could not read columnar file data %s.\n", buffer);
            ret.code = ERROR;
            ret.error_message = "Could not read data.\n";
            fclose(fp);
            return ret;
        }

        // Add the column to the variable pool
        set_resource(col->name, col);

        // Add column to the table
        tbl->col[i] = col;

        fclose(fp);
    }


    ret.code = OK;
    return ret;
}

status open_db(const char* filename, db** db, OpenFlags flags)
{
    status ret;
    if (flags == CREATE || !db) {
        ret.error_message = "CREATE flag not supported.\n";
        ret.code = ERROR;
        return ret;
    }

    // Open the file with the data
    FILE* mfile = fopen(filename, "r");
    if (!mfile) {
        ret.error_message = "Open failed.\n";
        ret.code = ERROR;
        return ret;
    }

    // Allocate space for the tables
    (*db)->tables = calloc((*db)->table_count, sizeof(struct table*));
    if (!(*db)->tables) {
        ret.error_message = "Could not allocate space.\n";
        ret.code = ERROR;
        fclose(mfile);
        return ret;
    }

    // Iterate over the tables and load each one
    char buffer[DEFAULT_ARRAY_SIZE];
    for(size_t i = 0; i < (*db)->table_count; i++) {
        table* tbl = calloc(1, sizeof(struct table));

        // Read in the table name and column count
        if (fscanf(mfile,
            (i < (*db)->table_count - 1) ? " %s %zu " : " %s %zu\n",
            buffer, &tbl->col_count) != 2) {
            ret.code = ERROR;
            ret.error_message = "Could not read metadata.";
            log_err(ret.error_message);
            fclose(mfile);
            return ret;
        }
        tbl->table_size = tbl->col_count;
        tbl->name = copystr(buffer);

        // Read the rest of the line for this table!
        status s = load_table(mfile, tbl);
        if (s.code != OK) {
            fclose(mfile);
            free(tbl);
            return s;
        }

        tbl->length = 0;  // unused

        // Add the table to the variable pool
        set_resource(tbl->name, tbl);

        // Add the table to the database
        (*db)->tables[i] = tbl;
    }

    fclose(mfile);

    ret.code = OK;
    return ret;
}

status drop_db(db* db)
{
    (void) db;
    return global;
}

// Writes out a table.
status sync_table(table* tbl){
    char fname[DEFAULT_ARRAY_SIZE];
    status s;
    for (size_t i = 0; i < tbl->col_count; i++) {
        sprintf(fname, "%s/%s.data", DATA_FOLDER, tbl->col[i]->name);
        FILE* data = fopen(fname, "wb");
        if (!data) {
            s.code = ERROR;
            s.error_message = "Could not open file.";
            log_err("Could not open %s\n", fname);
            return s;
        }
        if (tbl->col[i]->count != fwrite(tbl->col[i]->data, sizeof(Data), tbl->col[i]->count, data)) {
            s.code = ERROR;
            s.error_message = "Could not write data to file!";
            log_err("Could not write data to file!");
            fclose(data);
            return s;
        }

        // Free the column.
        free(tbl->col[i]->name);
        free(tbl->col[i]->data);
        if (tbl->col[i]->index) {
            if (tbl->col[i]->index->type == B_PLUS_TREE) {
                // TODO (FREE THE B_PLUS TREE) -- also, write it out to
                free(tbl->col[i]->index->index);
            }
            free(tbl->col[i]->index);
        }
        free(tbl->col[i]->index);
        free(tbl->col[i]);
        fclose(data);
    }

    s.code = OK;
    return s;
}

status sync_db(db* db) {
    // We need to create a metadata file with the information in the database
    char fname[DEFAULT_ARRAY_SIZE];
    status s;
    sprintf(fname, "%s/%s.meta", DATA_FOLDER, db->name);
    FILE* mfile = fopen(fname, "w");
    if (!mfile) {
        s.code = ERROR;
        s.error_message = "Could not open file.\n";
        log_err("Could not open file %s", fname);
        return s;
    }

    // Iterate over the tables!
    for (size_t i = 0; i < db->table_count; i++){
        if (fprintf(mfile, "%s %zu ", db->tables[i]->name, db->tables[i]->col_count) < 0) {
            log_err("Unable write to file %s\n", fname);
            s.code = ERROR;
            s.error_message = "Unable to write to file";
            fclose(mfile);
            return s;
        }
        // Write out the column names for the table
        for(size_t j = 0; j < db->tables[i]->col_count - 1; j++) {
            if (fprintf(mfile, "%s %zu ", db->tables[i]->col[j]->name,
                db->tables[i]->col[j]->count) < 0) {
                log_err("Unable to write to file %s\n", fname);
                s.code = ERROR;
                s.error_message = "Unable to write to file!\n";
                fclose(mfile);
                return s;
            }
        }
        // Last one is written with a newline
        if (fprintf(mfile, "%s %zu\n",
            db->tables[i]->col[db->tables[i]->col_count - 1]->name,
            db->tables[i]->col[db->tables[i]->col_count - 1]->count) < 0) {
            log_err("Unable to write to file %s\n", fname);
            s.error_message = "Unable to write to file";
            s.code = ERROR;
            fclose(mfile);
            return s;
        }
        s = sync_table(db->tables[i]);

        // Free what we can!
        free(db->tables[i]->name);
        free(db->tables[i]->col);
        free(db->tables[i]);
    }
    fclose(mfile);
    return s;
}

status create_table(db* db, const char* name, size_t num_columns, table** table)
{
    // TODO(luisperez): Need to check to see if the table already exists.
    // Note: A table would already be in our variable pool, so this function
    // won't be called with a duplicate table. We ignore this case for
    // performance reasons as linearly searching over the tables seems
    // inefficient.

    // Store results.
    status s;

    // Need to allocate space for the table.
    if (*table == NULL) {
        *table = calloc(1, sizeof(struct table));
        if (!(*table)) {
            log_err("Table %s creation failed! %s: error at line %d\n",
                name, __func__, __LINE__);
            s.code = ERROR;
            return s;
        }
    }
    // We have no space.
    if (db->tables_available <= 0) {
        log_info("No space for table %s in db:%s available! Creating more space.\n", name, db->name);
        size_t max_table_count = 2 * db->table_count + 1;
        size_t newsize = sizeof(struct table*) * max_table_count;
        size_t oldsize = sizeof(struct table*) * db->table_count;
        // Had no tables.
        if (oldsize == 0) {
            assert(newsize > 0);
            db->tables = calloc(newsize, sizeof(char));
        }
        // Have tables, need to resize.
        else {
            void* tmp = resize(db->tables, oldsize, newsize);
            free(db->tables);
            db->tables = tmp;
        }
        if (!db->tables) {
            log_err("Failure creating space for table %s in db %s. %s: error at line %d\n",
                name, db->name, __func__, __LINE__);
            s.code = ERROR;
            free(*table);
            return s;
        }

        // Update tables
        db->tables_available = max_table_count - db->table_count;
    }

    // Assign the table to the next available space in the database.
    db->tables[db->table_count++] = *table;
    db->tables_available--;

    // Populate table.
    (*table)->name = copystr(name);
    (*table)->col_count = 0;
    (*table)->col = calloc(num_columns, sizeof(struct column*));
    (*table)->length = 0;
    (*table)->table_size = num_columns;
    (*table)->cluster_column = NULL;

    s.code = OK;
    return s;
}

status drop_table(db* db, table* table)
{
    (void) db;
    (void) table;
    return global;
}

status create_column(table* table, const char* name, column** col)
{
    // TODO(luisperez): Need to check if the column already exists.
    // Again, no need to do this because we check in var_pool before getting here.
    // Removing the check for the sake of performance.

    // Need to allocate space for the column.
    status s;
    if (*col == NULL) {
        // Allocate space for the column!
        *col = calloc(1, sizeof(struct column));
        if (!(*col)) {
            log_err("Column %s creation failed! %s: error at line %d\n",
                    name, __func__, __LINE__);
            s.code = ERROR;
            return s;
        }
    }
    // We have no space left in the table
    if (table->col_count >= table->table_size) {
        log_info("No space for column %s in table: %s available! Creating more space.\n",
            name, table->name);
        size_t new_table_size = 2 * table->table_size + 1;
        size_t newsize = sizeof(struct column*) * new_table_size;
        size_t oldsize = sizeof(struct column*) * table->table_size;
        // Had no columns.
        if (oldsize == 0) {
            assert(newsize > 0);
            table->col = calloc(newsize, sizeof(char));
        }
        // Have tables, need to resize.
        else {
            void* tmp = resize(table->col, oldsize, newsize);
            free(table->col);
            table->col = tmp;
        }
        if (!table->col) {
            log_err("Failure creating space for column %s in table %s. %s: error at line %d\n",
                name, table->name, __func__, __LINE__);
            s.code = ERROR;
            free(*col);
            return s;
        }

        // Update the table_size
        table->table_size = new_table_size;
    }

    // Assign the column to the next available space in the table.
    table->col[table->col_count++] = *col;


    (*col)->name = copystr(name);
    (*col)->size = 0;
    (*col)->count = 0;
    (*col)->type = INT;

    // TODO (data, index)
    (*col)->data = NULL;
    (*col)->index = NULL;

    s.code = OK;
    return s;
}

status create_index(column* col, IndexType type)
{
    if (type == SORTED) {
        // We assume the column has no data.
    }
    (void) col;
    (void) type;
    return global;
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
    Data* tmp = malloc(sizeof(Data) * (end - start + 1));
    Data* tpos = malloc(sizeof(Data) * (end - start + 1));
    while (i <= mid && j <= end) {
        if (A[i].i < A[j].i) {
            tpos[k] = pos[i];
            tmp[k++] = A[i++];
        }
        else {
            tpos[k] = pos[j];
            tmp[k++] = A[j++];
        }
    }

    // Iterate over the i
    while (i <= mid) {
        tpos[k] = pos[i];
        tmp[k++] = A[i++];
    }

    // Iterate over the j
    while (j <= end) {
        tpos[k] = pos[j];
        tmp[k++] = A[j++];
    }

    // Now we copy the results back into A assuming the spaces are continguous.
    while (--k >= 0) {
        A[start + k] = tmp[k];
        pos[start + k] = tpos[k];
    }
}

// Recursive function to sort a column along with an array of positions.
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


// Similar to the xrange function in python, allocates a
// column with 0...n-1 integers.
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

// Copies the column data in c into a new column.
column* copycolumn(column* col) {
    column* res = calloc(1 ,sizeof(struct column));
    res->name = col->name;
    // Copy data (TODO- size or count)
    res->data = calloc(col->count, sizeof(Data));
    for (size_t i = 0; i < col->count; i++) {
        res->data[i] = col->data[i];
    }
    res->size = col->count;
    res->count = col->count;
    res->type = col->type;
    return res;
}

status recluster(table* tbl, IndexType newtype) {
    // Let's recluster only if the current type does not match the new type.
    status ret;
    if (tbl->cluster_column->index->type != newtype) {
        if (newtype == SORTED) {
            // We had a btree before and now we want... sorted data?
            log_err("Unimplemented btree to sorted conversion. %s: line %d.\n",
                __func__, __LINE__);
            ret.code = ERROR;
            ret.error_message = "Unimplemented.\n";
            return ret;
        }
        else if (newtype == B_PLUS_TREE) {
            // TODO create a bplus tree!
            log_err("Unimplemented sorted to btree conversion.");
            ret.code = ERROR;
            ret.error_message = "Unimplemented.\n";
            return ret;
        }
        else {
            log_err("Unsupported index type! Cannot recluster");
            ret.code = ERROR;
            ret.error_message = "Unsupported index type!\n";
            return ret;
        }
    }

    ret.code = OK;
    return ret;
}

// We add secondary index to the column.
status create_secondary_index(column* col, IndexType type) {
    status ret;
    // Free any previous indexes based on type.
    if (col->index) {
        if (col->index->type == SORTED) {
            SortedIndex* idx = col->index->index;
            free(idx->data);
            free(idx->pos);
            free(idx);
        }
        else if (col->index->type == B_PLUS_TREE) {
            // TODO(luisperez): Free a btree
            free(col->index->index);
        }
        else {
            // We still free it, but don't know it's type.
            free(col->index->index);
            log_err("Invalid index detected. %s, line %d\n", __func__, __LINE__);
        }
    }

    // Allocate space for the index if non-existent
    if (!col->index) {
        col->index = calloc(1, sizeof(struct column_index));
    }

    // We create copies of the data
    column* pos = xrange(col->count);
    column* data = copycolumn(col);

    // Then we sort them!
    mergesort(data->data, pos->data, 0, col->count - 1);

    if (type == SORTED) {
        // An now we have an index!
        col->index->type = SORTED;
        col->index->index = malloc(sizeof(SortedIndex));
        ((SortedIndex*) (col->index->index))->data = data;
        ((SortedIndex*) (col->index->index))->pos = pos;

        // We are done!
        ret.code = OK;
        return ret;
    }
    else if (type == B_PLUS_TREE) {
        // And now we bulk load into a B_Tree!
        col->index->type = B_PLUS_TREE;
        col->index->index = bulk_load(data->data, pos->data, data->count);

        // We can free because the data was copied into the btree
        free(pos->data);
        free(pos);
        free(data->data);
        free(data);

        ret.code = OK;
        return ret;
    }
    else {
        log_err("Unsupported index. %s: line %d\n", __func__, __LINE__);
        ret.code = ERROR;
        ret.error_message = "Unsupported index type.";
        return ret;
    }
}

status cluster_table(table* tbl) {
    status s;
    if (!tbl) {
        s.code = ERROR;
        s.error_message = "Invalid table pointer!";
        return s;
    }

    // A table with no clusters is already clustered
    if (!tbl->cluster_column) {
        s.code = OK;
        return s;
    }

    // Now we need to cluster, so we first sort the cluster_column
    column* pcol = tbl->cluster_column;
    column* pos = xrange(pcol->count);

    // After this call, pos is sorted in the order specified by pcol.
    mergesort(pcol->data, pos->data, 0, pcol->count - 1);

    // Now, for each column, we fetch based on positions.
    column* column;
    result* r = NULL;
    for (size_t col = 0; col < tbl->col_count; col++) {
        // Only the non-leading columns
        column = tbl->col[col];
        if (column != pcol) {
            if (fetch(column, pos, &r).code != OK) {
                log_err("Failed to sort column %s\n", column->name);
            }

            // The results are stored in r.
            free(column->data);
            column->data = r->payload;
            free(r);
            r = NULL;
        }
    }

    free(pos);
    s.code = OK;
    return s;
}


// Binary searches the array for the given element and returns
// the index at which it should be inserted to maintain sorted order.
// The array is restricted to [start,end] (inclusive).
// Last parameter is the total size of th ENTIRE array
// Returns the smallest index possible for insertion to mainted sortedness.
size_t find_index(Data* array, size_t start, size_t end, Data el, size_t size) {
    // If only one element or if we've found the element.
    size_t mid = start + end / 2;
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

// Find the position on which we insert the value. Column must be indexed.
size_t find_pos(column* col, Data data) {
    if (col->index->type == SORTED) {
        return find_index(col->data, 0, col->count - 1, data, col->count - 1);
    }
    else if (col->index->type == B_PLUS_TREE) {
        // TOOD(luisperez): Find the position to insert into using the
        // tree.
        return col->count - 1;
    }
    else {
        log_err("Unsupported index type on cluster column.");
        return col->count - 1;
    }
}

// Inserts datume into arr at the specified location. Returns size array.
status insert_into_column(column* col, Data datum, size_t pos) {
    status ret;
    if (col->count >= col->size) {
        log_info("No space for data in column. Creating more space.\n", col->name);
        size_t newcount = 2 * col->count + 1;
        if (newcount == 1) {
            newcount = DEFAULT_ARRAY_SIZE;
            col->data = calloc(sizeof(Data), newcount);
            if (!col->data) {
                ret.code = ERROR;
                ret.error_message = "Failed allocating space for data";
                log_err(ret.error_message);
                return ret;
            }
        }
        else {
            size_t newsize = newcount * sizeof(Data);
            size_t oldsize = col->count * sizeof(Data);
            void* tmp = resize(col->data, oldsize, newsize);
            free(col->data);
            col->data = tmp;
        }

        // Update tables
        col->size = newcount;
    }

    Data tmp;
    while (pos < col->count) {
        tmp = col->data[pos];
        col->data[pos++] = datum;
        datum = tmp;
    }
    col->data[col->count++] = datum;
    ret.code = OK;
    return ret;
}

// Inserts the given value into positions specified by pos.
status insert_pos(column *col, size_t pos, Data data) {
    status ret = insert_into_column(col, data, pos);
    if (ret.code != OK){
        log_err("Failed to insert into column. %s: line %s.\n",
            __func__, __LINE__);
        return ret;
    }

    // We've added a new element, now update the index if necessary
    // We have an index with allocated index space and we are not a cluster column.
    if (col->index && col->index->index && col) {
        // TODO(luisperez): Currently only support sorted indexes
        if (col->index->type == SORTED) {
            SortedIndex* idx = (SortedIndex*) col->index->index;
            // We only need to do stuff for an unclustered column
            if (idx->data != col &&
                idx->pos != NULL) {
                // The first step is inserting the value into the sorted array.
                size_t sorted_pos = find_pos(idx->data, data);
                ret = insert_into_column(idx->data, data, sorted_pos);
                if (ret.code != OK) {
                    ret.error_message = "Failed to insert into sorted column index.";
                    log_err("Failed to insert into column index");
                    return ret;
                }
                // Insert the position into the pos array.
                Data d;
                d.i = pos;
                ret = insert_into_column(idx->pos, d, sorted_pos);
            }
        }
    }
    return ret;

}

status insert(column* col, Data data)
{
    return insert_pos(col, col->count, data);
}

status delete(column* col, int* pos)
{
    (void) col;
    (void) pos;
    return global;
}
status update(column* col, int* pos, int new_val)
{
    (void) col;
    (void) pos;
    (void) new_val;
    return global;
}


int check(comparator* f, int value){
    comparator* cur = f;
    int success = 1; // 1 is True
    Junction mode = AND; // Start with True && ...
    while (cur) {
        if (mode == AND) {
            if (cur->type == LESS_THAN) {
                success = success && (value < cur->p_val);
            }
            else if (cur->type == GREATER_THAN) {
                success = success && (value > cur->p_val);
            }
            else if (cur->type == EQUAL) {
                success = success && (value == cur->p_val);
            }
            else if (cur->type == (EQUAL | LESS_THAN)) {
                success = success && (value <= cur->p_val);
            }
            else if (cur->type == (EQUAL | GREATER_THAN)) {
                success = success && (value >= cur->p_val);
            }
            else {
                log_err("Unsupported comparator type!");
                return 0;
            }
        }
        else if (mode == OR) {
            if (cur->type == LESS_THAN) {
                success = success || (value < cur->p_val);
            }
            else if (cur->type == GREATER_THAN) {
                success = success || (value > cur->p_val);
            }
            else if (cur->type == EQUAL) {
                success = success || (value == cur->p_val);
            }
            else if (cur->type == (EQUAL | LESS_THAN)) {
                success = success || (value <= cur->p_val);
            }
            else if (cur->type == (EQUAL | GREATER_THAN)) {
                success = success || (value >= cur->p_val);
            }
            else {
                log_err("Unsupported comparator type!");
                return 0;
            }
        }
        // We don't use j == NONE because we assume next_compartor is NULL.
        mode = cur->mode;
        cur = cur->next_comparator;
    }
    return success;
}

// Fetches the values specified by pos from col and stores in r. Allocates
// space for r if not already existent.
status fetch(column* col, column* pos,  result** r){
    status ret;
    if (!(*r)){
        *r = calloc(1, sizeof(struct result));
        if (!(*r)) {
            log_err("Low on memory! Could not allocate more space.");
            ret.code = ERROR;
            ret.error_message = "Low on memory";
            return ret;
        }
    }

    // Allocate space for result
    (*r)->payload = calloc(pos->size, sizeof(Data));
    (*r)->num_tuples = pos->size;

    // Copy out to the results
    for (size_t i = 0; i < pos->size; i++) {
        (*r)->payload[i] = col->data[pos->data[i].i];
    }

    ret.code = OK;
    return ret;
}

// These functions are not exposed as they assume work from col_scan
status index_scan(comparator* f, column* col, result** r, Data* pos)
{
    // This function IS ONLY called from col_scan. Assume input parameters.
    status ret;
    // size_t size = (*r)->num_tuples;
    size_t res_pos = 0;

    // Extract the min/max from the operator, if possible, so we can find their indexes
    size_t min_index = 0;
    size_t max_index = col->count - 1;

    // Sorted column
    // TODO(luisperez: Deal with BTree)
    // We assume that we have a sorted index column.
    SortedIndex* sorted = (SortedIndex*) col->index->index;

    // We assume only the first two relevant matter.
    while (f) {
        if (f->type == GREATER_THAN && min_index == 0) {
            Data min;
            min.i = f->p_val;
            min_index = find_index(sorted->data->data, 0, sorted->data->count - 1,
                min, sorted->data->count);

            // This is the index for the sorted data!
        }
        else if (f->type == (LESS_THAN | EQUAL) && max_index == col->count - 1) {
            Data max;
            max.i = f->p_val;
            max_index = find_index(sorted->data->data, 0, sorted->data->count - 1, max,
                sorted->data->count);
        }
        f = f->next_comparator;
    }

    // This is a sequential scan, we just need to find a good starting point.
    if (!pos) {
        // Clustered.
        if (!sorted->pos || sorted->data == col) {
            for(size_t i = min_index; i < max_index; i++) {
                // Return the clustered position index.
                (*r)->payload[res_pos++] = sorted->pos->data[i];
            }
        }
    }
    // TODO this does not get run, as we maintain normal shit.
    // for the case when we're given a pos vector!
    // This is mentioned in the TODO for the SortedIndex.
    // Currently, we simply don't deal with this issue?
    // This code here is never run because we do a normal sequential
    // scan in the case where pos is given. See col_scan for details.
    else {
        // Let's assume that pos is also sorted so we can make this efficiently
        // TODO(luisperes): Deal with the case where pos isn't sorted?
        Data max_index_data;
        Data min_index_data;
        max_index_data.i = max_index;
        min_index_data.i = min_index;
        size_t min_pos_index = find_index(pos, 0, (*r)->num_tuples - 1,
            min_index_data, (*r)->num_tuples);
        size_t max_pos_index = find_index(pos, 0, (*r)->num_tuples - 1,
            max_index_data, (*r)->num_tuples);
        for (size_t ii = min_pos_index; ii < max_pos_index; ii++) {
            // We index pos, which
            (*r)->payload[res_pos++] = pos[ii];
        }

    }

    (*r)->num_tuples = res_pos;
    ret.code = OK;
    return ret;

}

status col_scan(comparator* f, column* col, result** r)
{
    // The positions we need to scan are stored in *r->payload.
    // We assume r has been pre-allocated.
    status ret;
    if (!(*r)) {
        ret.code = ERROR;
        ret.error_message = "Result not-allocated for col_scan.";
        return ret;
    }

    Data* pos = (*r)->payload;
    size_t size = (*r)->num_tuples;
    size_t res_pos = 0;

    // We override with a new array because this data will be saved too!
    (*r)->payload = calloc(size, sizeof(Data));


    // Check if we have an index on this column.
    // TODO(we are only dealing with full column scans!)
    if (!pos && col->index && col->index->index) {
        return index_scan(f, col, r, pos);
    }

    // Otherwise do a dumb scan on the data.
    // This is a full column scan.
    if (!pos){
        for(size_t i = 0; i < col->count; i++) {
            if (check(f, col->data[i].i)) {
                (*r)->payload[res_pos++].i = i;
            }
        }
    }

    // We use the pos indicated in the res.
    else {
        for(size_t ii = 0; ii < (*r)->num_tuples; ii++) {
            if (check(f, col->data[pos[ii].i].i)) {
                (*r)->payload[res_pos++] = pos[ii];
            }
        }
    }
    (*r)->num_tuples = res_pos;
    ret.code = OK;
    return ret;
}


// TODO(luisperez): Figure out what to do with these!
status query_prepare(const char* query, db_operator** op)
{
    (void) query;
    (void) op;
    return global;
}
status query_execute(db_operator* op, result** results)
{
    (void) op;
    (void) results;
    return global;
}
