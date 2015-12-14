// Copyright 2015 <Luis Perez>

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

status sync_db(db* db)
{
    (void) db;
    return global;
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
        // We have no space.
        if (db->tables_available <= 0) {
            log_info("No space for table %s in db:%s available! Creating more space.\n", name, db->name);
            size_t max_table_count = 2 * db->table_count + 1;
            db->tables_available = max_table_count - db->table_count;
            db->tables = resize(db->tables, sizeof(struct table) * db->table_count,
                sizeof(struct table) * max_table_count);
            if (!db->tables) {
                log_err("Failure creating space for table %s in db %s. %s: error at line %d\n",
                    name, db->name, __func__, __LINE__);
                s.code = ERROR;
                return s;
            }
        }
        // Assign the table to the next available space in the database.
        *table = (db->tables[db->table_count++]);
    }

    // Populate table.
    (*table)->name = copystr(name);
    (*table)->col_count = 0;
    (*table)->col = malloc(sizeof(struct column) * num_columns);
    (*table)->length = 0;

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
    if (*col == NULL) {
        // Assign the column to the next available space in the database.
        *col = (table->col[table->col_count++]);
    }
    (*col)->name = copystr(name);
    return global;
}

status create_index(column* col, IndexType type)
{
    (void) col;
    (void) type;
    return global;
}

status insert(column* col, int data)
{
    (void) col;
    (void) data;
    return global;
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
status col_scan(comparator* f, column* col, result** r)
{
    (void) f;
    (void) col;
    (void) r;
    return global;
}
status index_scan(comparator* f, column* col, result** r)
{
    (void) f;
    (void) col;
    (void) r;
    return global;
}

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
