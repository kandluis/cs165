// Copyright 2015 <Luis Perez>

#include <assert.h>
#include <string.h>

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
status load_table(FILE* metadata, table* tbl, char* cluster_column) {
    // Allocate space for the columns
    tbl->col = calloc(1, sizeof(struct column*) * tbl->table_size);

    char buffer[DEFAULT_ARRAY_SIZE];
    char buffer2[DEFAULT_ARRAY_SIZE];

    // Read in each column.
    FILE* fp;
    status ret;
    for (size_t i = 0; i < tbl->table_size; i++) {
        column* col = calloc(1, sizeof(struct column));
        if (fscanf(metadata,
            // Read column_name size index
            (i < tbl->table_size - 1) ? " %s %zu %s " : " %s %zu %s\n",
            buffer, &col->count, buffer2) != 3) {
            log_err("Could not read from metadata file for column loading");
            ret.code = ERROR;
            ret.error_message = "Could read file.\n";
            return ret;
        }
        col->size = col->count;
        col->name = copystr(buffer);

        // Read in the data! We do this for all indexes!
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

        // Read in the index data!
        if (strcmp(buffer2, "sorted" ) == 0) {
            col->index = calloc(1, sizeof(column_index));
            col->index->type = SORTED;
            SortedIndex* idx = calloc(1, sizeof(SortedIndex));
            // Read sorted values only when not the cluster
            if (strcmp(col->name, cluster_column) != 0) {
                idx->data = calloc(col->count, sizeof(Data));
                if (col->count != fread(idx->data, sizeof(Data), col->count, fp)) {
                    log_err("Could not read columnar file data %s.\n", buffer);
                    ret.code = ERROR;
                    ret.error_message = "Could not read data.\n";
                    fclose(fp);
                    free(idx);
                    return ret;
                }

                // Read positions mappings
                idx->pos = calloc(col->count, sizeof(Data));
                if (col->count != fread(idx->pos, sizeof(Data), col->count, fp)) {
                    log_err("Could not read columnar file data %s.\n", buffer);
                    ret.code = ERROR;
                    ret.error_message = "Could not read data.\n";
                    fclose(fp);
                    free(idx);
                    return ret;
                }
            } else {
                // This a clustered column so index just point to data
                idx->data = col;
                idx->pos = NULL;
            }
            col->index->index = idx;

        }
        else if (strcmp(buffer2, "btree") == 0) {
            col->index = calloc(1, sizeof(column_index));
            col->index->type = B_PLUS_TREE;
            col->index->index = calloc(1, sizeof(Node));
            read_tree(fp, col->index->index);
        }
        // No index!
        else {
            col->index = NULL;
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
        if (fscanf(mfile, " %s %zu ",
            buffer, &tbl->col_count) != 2) {
            ret.code = ERROR;
            ret.error_message = "Could not read metadata.";
            log_err(ret.error_message);
            fclose(mfile);
            return ret;
        }
        tbl->table_size = tbl->col_count;
        tbl->name = copystr(buffer);

        // Read in the cluster index name!
        if (fscanf(mfile, " %s ", buffer) != 1) {
            ret.code = ERROR;
            ret.error_message = "Could not read metadata.";
            log_err(ret.error_message);
            free(tbl);
            fclose(mfile);
            return ret;
        }

        // Read the rest of the line for this table!
        status s = load_table(mfile, tbl, buffer);
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

        // If we have an index, write it out!
        if (tbl->col[i]->index) {
            if (tbl->col[i]->index->type == B_PLUS_TREE) {
                Node* idx = tbl->col[i]->index->index;
                write_tree(data, idx);

                free_btree(idx);
            }
            else if (tbl->col[i]->index->type == SORTED) {
                // Write out the SortedIndex only if you're not clustered
                SortedIndex* idx = tbl->col[i]->index->index;
                if (idx->data != tbl->cluster_column) {

                    if (tbl->col[i]->count != fwrite(idx->data, sizeof(Data), tbl->col[i]->count, data)) {
                        s.code = ERROR;
                        s.error_message = "Could not write data to file!";
                        log_err("Could not write data to file!");
                        fclose(data);
                        return s;
                    }

                    if (tbl->col[i]->count != fwrite(idx->pos, sizeof(Data), tbl->col[i]->count, data)) {
                        s.code = ERROR;
                        s.error_message = "Could not write data to file!";
                        log_err("Could not write data to file!");
                        fclose(data);
                        return s;
                    }

                    // Free the structure.
                    free(idx->pos);
                    free(idx->data);
                }


            }
            else {
                log_err("Unsupported index type for permission");
            }


            free(tbl->col[i]->index);
        }

        // Free the column.
        free(tbl->col[i]->name);
        free(tbl->col[i]->data);
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
        // Write out the table name and the column count
        if (fprintf(mfile, "%s %zu ", db->tables[i]->name, db->tables[i]->col_count) < 0) {
            log_err("Unable write to file %s\n", fname);
            s.code = ERROR;
            s.error_message = "Unable to write to file";
            fclose(mfile);
            return s;
        }
        // Write out the name of the cluster column, if any
        if (db->tables[i]->cluster_column) {
            if (fprintf(mfile, "%s ", db->tables[i]->cluster_column->name) < 0) {
                log_err("Unable write to file %s\n", fname);
                s.code = ERROR;
                s.error_message = "Unable to write to file";
                fclose(mfile);
                return s;
            }
        }
        else {
            if (fprintf(mfile, "%s ", "null") < 0) {
                log_err("Unable write to file %s\n", fname);
                s.code = ERROR;
                s.error_message = "Unable to write to file";
                fclose(mfile);
                return s;
            }
        }

        // Write out the column names for the table
        for(size_t j = 0; j < db->tables[i]->col_count - 1; j++) {
            char* index;
            if (!db->tables[i]->col[j]->index) {
                index = "unsorted";
            }
            else {
                if (db->tables[i]->col[j]->index->type == SORTED) {
                    index = "sorted";
                }
                else if (db->tables[i]->col[j]->index->type == B_PLUS_TREE) {
                    index = "btree";
                }
                else {
                    log_err("Unsupported persistence type for index!.");
                }
            }
            // Write out column_name size index
            if (fprintf(mfile, "%s %zu %s ", db->tables[i]->col[j]->name,
                db->tables[i]->col[j]->count, index) < 0) {
                log_err("Unable to write to file %s\n", fname);
                s.code = ERROR;
                s.error_message = "Unable to write to file!\n";
                fclose(mfile);
                return s;
            }
        }
        char* index = "";
        if (!db->tables[i]->col[db->tables[i]->col_count - 1]->index) {
            index = "unsorted";
        }
        else {
            if (db->tables[i]->col[db->tables[i]->col_count - 1]->index->type == SORTED) {
                index = "sorted";
            }
            else if (db->tables[i]->col[db->tables[i]->col_count - 1]->index->type == B_PLUS_TREE) {
                index = "btree";
            }
            else {
                log_err("Unsupported persistence type for index!.");
            }
        }
        // Last one is written with a newline
        if (fprintf(mfile, "%s %zu %s\n",
            db->tables[i]->col[db->tables[i]->col_count - 1]->name,
            db->tables[i]->col[db->tables[i]->col_count - 1]->count, index) < 0) {
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

// TOOD(luisperez): Currently unused!
status create_index(column* col, IndexType type)
{
    if (type == SORTED) {
        // We assume the column has no data.
    }
    (void) col;
    (void) type;
    return global;
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

// Reclusters a single column.
status recluster_col(column* col, IndexType newtype) {
    status ret;
    // Only if we're changing types
    if (newtype != col->index->type) {

        if (newtype == SORTED) {
            Node* idx = col->index->index;

            // Create the new SortedIndex
            SortedIndex* idx2 = calloc(1, sizeof(SortedIndex));
            idx2->data = calloc(1, sizeof(struct column));
            idx2->data->data = calloc(col->count, sizeof(Data));
            idx2->pos = calloc(1, sizeof(struct column));
            idx2->pos->data = calloc(col->count, sizeof(Data));

            extract_data(idx, idx2->data->data, idx2->pos->data);

            // Free the tree
            free_btree(idx);
            free(idx);

            // Set the new index
            col->index->index = idx2;
            col->index->type = SORTED;

            ret.code = OK;
            return ret;
        }
        else if (newtype == B_PLUS_TREE) {
            SortedIndex* idx = col->index->index;

            Node* root = calloc(1, sizeof(Node));

            // Especial case the cluster node. If pos is null, range it.
            if (!idx->pos) {
                idx->pos = xrange(idx->data->count);
            }
            bulk_load(idx->data->data, idx->pos->data, idx->data->count, root);

            // Free the results (don't free everything if we're a cluster!)
            if (idx->pos) {
                free(idx->data->data);
                // free(idx->data);
                free(idx->pos->data);
                free(idx->pos);
            }
            free(idx);

            // Reset the parameters.
            col->index->index = root;
            col->index->type = B_PLUS_TREE;
            ret.code = OK;
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

// TODO: reclustering if the index already exists sorted doesn't mean much, just changing
// types.
status recluster(table* tbl, IndexType newtype) {
    // Let's recluster only if the current type does not match the new type.
    if (tbl->cluster_column->index->type != newtype) {
        return recluster_col(tbl->cluster_column, newtype);
    }
    status s;
    s.code = OK;
    log_info("Reclustering to the same index!");
    return s;
}

// We add secondary index to the column.
status create_secondary_index(column* col, IndexType type) {
    status ret;
    // If we have a previous index that's different!
    if (col->index) {
        return recluster_col(col, type);
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
        Node* root = calloc(1, sizeof(Node));
        bulk_load(data->data, pos->data, data->count, root);
        col->index->index = root;

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

        // We need to reindex columns with indexes.
        if(column->index) {
            // We free the index depending on type and recreate it
            IndexType type = column->index->type;
            if (column->index->type == SORTED) {
                SortedIndex* idx = column->index->index;
                if (idx->pos) {
                    free(idx->data->data);
                    free(idx->data);
                    free(idx->pos->data);
                    free(idx->pos);
                }
            }
            else if (column->index->type == B_PLUS_TREE) {
                Node* idx = column->index->index;
                free_btree(idx);
            }
            else {
                log_err("Unsupported index type");
                free(pos);
                s.code = ERROR;
                return s;
            }
            free(column->index);
            column->index = NULL;
            s = create_secondary_index(column, type);
            if (s.code != OK) {
                log_err("Could not create secondary index!");
                free(pos);
                return s;
            }
        }
    }

    free(pos);
    s.code = OK;
    return s;
}

// Find the position on which we insert the value. Column must be indexed.
// Column is also expected to be the clustered index, so we return the
// index for the clustered table!
size_t find_pos(column* col, Data data) {
    if (!col->index || !col->index->index) {
        log_err("No index exists for col %s. %s, line %d\n",
            col->name, __func__, __LINE__);
        return -1;
    }
    if (col->index->type == SORTED) {
        SortedIndex* idx = col->index->index;
        return find_index(idx->data->data, 0, (col->count == 0) ? 0 : col->count - 1,
            data, col->count - 1);
    }
    else if (col->index->type == B_PLUS_TREE) {
        Node* tmp;
        return find_element_tree(data, col->index->index, &tmp);
    }
    else {
        log_err("Unsupported index type on cluster column.");
        return col->count - 1;
    }
}

// Inserts datum into arr at the specified location. Returns size array.
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
    if (col->index && col->index->index) {
        if (col->index->type == SORTED) {
            SortedIndex* idx = (SortedIndex*) col->index->index;
            // If the index data is null and pos in null, we have a clustered column
            // to which a new element has been added. Repoint to it.
            if (!idx->data && !idx->pos) {
                log_info("Column %s is a primary sorted column. New element added.",
                    col->name);
                idx->data = col;
            }
            // We only need to do stuff for an unclustered column
            if (idx->data != col &&
                idx->pos != NULL) {
                // The first step is inserting the value into the sorted array.
                size_t sorted_pos = find_index(idx->data->data, 0,
                    (idx->data->count == 0) ? 0 : idx->data->count - 1,
                    data, idx->data->count);
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
        else if (col->index->type == B_PLUS_TREE) {
            Node* idx = (Node*) col->index->index;
            // TODO(luisperez): Be efficient and only keep one copy of the
            // data in the btree for the cluster column on the table.
            // However, we currently do not do this!
            Data d;
            d.i = pos;
            insert_tree(idx, data, d);
        }
        else {
            log_err("Index type is not supported! %s: line %d.\n",
                __func__, __LINE__);
            ret.code = ERROR;
            ret.error_message = "Unsupported index type";
            return ret;
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
    Data* new_pos = calloc(col->count, sizeof(Data));
    size_t new_pos_count = 0;

    // Sorted column so extract new_pos
    if (col->index->type == SORTED) {
        // Extract the min/max from the operator, if possible, so we can find their indexes
        size_t min_index = 0;
        size_t max_index = col->count - 1;
        SortedIndex* sorted = (SortedIndex*) col->index->index;

        // We assume only the first two relevant matter.
        while (f) {
            if (f->type == LESS_THAN && max_index == col->count - 1) {
                Data max;
                max.i = f->p_val;
                max_index = find_index(sorted->data->data, 0,
                    (sorted->data->count == 0) ? 0 : sorted->data->count - 1,
                    max, sorted->data->count);

                // This is the index for the sorted data!
            }
            else if (f->type == (GREATER_THAN | EQUAL) && min_index == 0) {
                Data min;
                min.i = f->p_val;
                min_index = find_index(sorted->data->data, 0,
                    (sorted->data->count == 0) ? 0 : sorted->data->count - 1,
                    min, sorted->data->count);
            }
            f = f->next_comparator;
        }

        // Clustered.
        if (!sorted->pos && sorted->data == col) {
            // Special case when we are running a scan over the clustered column.
            for(size_t i = min_index; i < max_index; i++) {
                // Return the clustered position index.
                new_pos[new_pos_count++].i = i;
            }
        }
        // Secondary index!
        else {
            for(size_t i = min_index; i < max_index; i++) {
                // Return the clustered position index.
                new_pos[new_pos_count++] = sorted->pos->data[i];
            }
        }
    }
    // TOOD(luisperez): Deal with btree scan to extract new_pos
    else if (col->index->type == B_PLUS_TREE) {

        size_t min_index = 0;
        size_t max_index = col->count - 1;
        Node* root = (Node*) col->index->index;
        Node* min_leaf = NULL;
        Node* max_leaf = NULL;

        // We assume only the first two relevant matter.
        while (f) {
            if (f->type == LESS_THAN && max_index == col->count - 1) {
                Data max;
                max.i = f->p_val;
                max_index = find_element_tree(max, root, &max_leaf);

                // This is the index for the sorted data!
            }
            else if (f->type == (GREATER_THAN | EQUAL) && min_index == 0) {
                Data min;
                min.i = f->p_val;
                min_index = find_element_tree(min, root, &min_leaf);
            }
            f = f->next_comparator;
        }

        // We don't differentiate between clustered and unclustered indexes?
        while (min_leaf != max_leaf) {
            while (min_index < min_leaf->count) {
                new_pos[new_pos_count++] = min_leaf->children->keys[min_index++];
            }
            min_leaf = min_leaf->next_link;
            min_index = 0;
        }

        // We reached the final node!
        while (min_index < max_index) {
            new_pos[new_pos_count++] = min_leaf->children->keys[min_index++];
        }

    }
    else {
        log_err("Index type not supported.");
        ret.code = ERROR;
        ret.error_message = "Do not support requested index.";
        return ret;
    }

    // We need to intersect the results because we've achieved good results.
    if (pos) {
        // Mergesort with respect to the other
        mergesort(pos, NULL, 0, (*r)->num_tuples - 1);
        mergesort(new_pos, NULL, 0, new_pos_count - 1);

        // Now we can run a modified version of merge!
        size_t i = 0;
        size_t j = 0;
        while (i < (*r)->num_tuples && j < new_pos_count) {
            if (pos[i].i > new_pos[j].i) {
                j++;
            }
            else if (pos[i].i < new_pos[j].i) {
                i++;
            }
            else {
                (*r)->payload[res_pos++] = pos[i];
                i++;
                j++;
            }
        }
        // We ran out of one so we're out!
    }
    // We just copy newpos over the payload!
    else {
        for(size_t i = 0; i < new_pos_count; i++) {
            (*r)->payload[res_pos++] = new_pos[i];
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
    if (col->index && col->index->index) {
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
    // We're note accessing a column directly!
    else if (!col->name) {
        for(size_t ii = 0; ii < (*r)->num_tuples; ii++) {
            if (check(f, col->data[ii].i)) {
                (*r)->payload[res_pos++] = pos[ii];
            }
        }
    }

    // We use the pos indicated in the res to access the column.
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
