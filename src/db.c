// Copyright 2015 <Luis Perez>

#include <assert.h>

#include "db.h"
#include "include/utils.h"

status global;

// TODO(USER): Here we provide an incomplete implementation of the create_db.
// There will be changes that you will need to include here.
status create_db(const char* db_name, db** db) {
    // Store status for return.
    status s;

    if (*db == NULL) {
        int attempts = 0;
        while (!(*db = malloc(sizeof(struct db))) && attempts < MAX_ATTEMPTS) {
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

status open_db(const char* filename, db** db, OpenFlags flags)
{
    (void) filename;
    (void) db;
    (void) flags;
    return global;
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
        sprintf(fname, "%s.data", tbl->col[i]->name);
        FILE* data = fopen(fname, "wb");
        if (!data) {
            s.code = ERROR;
            s.error_message = "Could not open file.";
            log_err("Could not open %s\n", fname);
            return s;
        }
        if (tbl->col[i]->count != fwrite(tbl->col[i]->data, sizeof(int64_t), tbl->col[i]->count, data)) {
            s.code = ERROR;
            s.error_message = "Could not write data to file!";
            log_err("Could not write data to file!");
            return s;
        }
    }

    s.code = OK;
    return s;
}

status sync_db(db* db) {
    // We need to create a metadata file with the information in the database
    char fname[DEFAULT_ARRAY_SIZE];
    status s;
    sprintf(fname, "%s.meta", db->name);
    FILE* mfile = fopen(fname, "w");
    if (!mfile) {
        s.code = ERROR;
        s.error_message = "Could not open file.\n";
        log_err("Could not open file %s", fname);
        return s;
    }

    // Iterate over the tables!
    for (size_t i = 0; i < db->table_count; i++){
        if (fprintf(mfile, "%s,%zd,", db->tables[i]->name, db->tables[i]->col_count) < 0) {
            log_err("Unable write to file %s\n", fname);
            s.code = ERROR;
            s.error_message = "Unable to write to file";
            fclose(mfile);
            return s;
        }
        // Write out the column names for the table
        for(size_t j = 0; j < db->tables[i]->col_count - 1; j++) {
            if (fprintf(mfile, "%s,%zd,", db->tables[i]->col[j]->name,
                db->tables[i]->col[j]->count) < 0) {
                log_err("Unable to write to file %s\n", fname);
                s.code = ERROR;
                s.error_message = "Unable to write to file!\n";
                fclose(mfile);
                return s;
            }
        }
        // Last one is writtne with a newline
        if (fprintf(mfile, "%s,%zd\n",
            db->tables[i]->col[db->tables[i]->col_count - 1]->name,
            db->tables[i]->col[db->tables[i]->col_count - 1]->count) < 0) {
            log_err("Unable to write to file %s\n", fname);
            s.error_message = "Unable to write to file";
            s.code = ERROR;
            fclose(mfile);
            return s;
        }
        s = sync_table(db->tables[i]);
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
        *table = malloc(sizeof(struct table));
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
            db->tables = malloc(sizeof(char) * newsize);
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
    (*table)->col = malloc(sizeof(struct column*) * num_columns);
    (*table)->length = 0;
    (*table)->table_size = num_columns;

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
        *col = malloc(sizeof(struct column));
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
            table->col = malloc(sizeof(char) * newsize);
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

    // TODO (data, index)
    (*col)->data = NULL;
    (*col)->index = NULL;

    s.code = OK;
    return s;
}

status create_index(column* col, IndexType type)
{
    (void) col;
    (void) type;
    return global;
}

status insert(column* col, int64_t data)
{
    // Check the data size in case we need to resize
    status ret;
    if (col->count >= col->size) {
        log_info("No space for data in column %s. Creating more space.\n", col->name);
        size_t newcount = 2 * col->count + 1;
        if (newcount == 1) {
            newcount = DEFAULT_ARRAY_SIZE;
            col->data = malloc(sizeof(int64_t) * newcount);
            if (!col->data) {
                ret.code = ERROR;
                ret.error_message = "Failed allocating space for data";
                log_err(ret.error_message);
                return ret;
            }
        }
        else {
            size_t newsize = newcount * sizeof(int64_t);
            size_t oldsize = col->count * sizeof(int64_t);
            void* tmp = resize(col->data, oldsize, newsize);
            free(col->data);
            col->data = tmp;
        }

        // Update tables
        col->size = newcount;
    }

    // Set the value into the column
    col->data[col->count++] = data;
    ret.code = OK;

    return ret;
}
status delete(column* col, int64_t* pos)
{
    (void) col;
    (void) pos;
    return global;
}
status update(column* col, int64_t* pos, int64_t new_val)
{
    (void) col;
    (void) pos;
    (void) new_val;
    return global;
}


int check(comparator* f, int64_t value){
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
        *r = malloc(sizeof(struct result));
        if (!(*r)) {
            log_err("Low on memory! Could not allocate more space.");
            ret.code = ERROR;
            ret.error_message = "Low on memory";
            return ret;
        }
    }

    // Allocate space for result
    (*r)->payload = malloc(pos->size * sizeof(int64_t));
    (*r)->num_tuples = pos->size;

    // Copy out to the results
    for (size_t i = 0; i < pos->size; i++) {
        (*r)->payload[i] = col->data[pos->data[i]];
    }

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

    int64_t* pos = (*r)->payload;
    size_t size = (*r)->num_tuples;
    size_t res_pos = 0;

    // We override with a new array because this data will be saved too!
    (*r)->payload = calloc(size, sizeof(int64_t));

    // This is a full column scan.
    if (!pos){
        for(size_t i = 0; i < col->count; i++) {
            if (check(f, col->data[i])) {
                (*r)->payload[res_pos++] = i;
            }
        }
        // Now we have a new result stored in r.
        (*r)->num_tuples = res_pos;
    }

    // We use the pos indicated in the res.
    else {
        for(size_t ii = 0; ii < (*r)->num_tuples; ii++) {
            if (check(f, col->data[pos[ii]])) {
                (*r)->payload[res_pos++] = pos[ii];
            }
        (*r)->num_tuples = res_pos;
        }
    }

    ret.code = OK;
    return ret;
}

status index_scan(comparator* f, column* col, result** r)
{
    (void) f;
    (void) col;
    (void) r;
    return global;
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
