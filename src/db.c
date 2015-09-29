// Copyright 2015 <Luis Perez>

#include "db.h"
#include "include/utils.h"

status global;

// TODO(USER): Here we provide an incomplete implementation of the create_db.
// There will be changes that you will need to include here.
status create_db(const char* db_name, db** db) {
    if (*db == NULL) {
        int attempts = 0;
        while (!(*db = malloc(sizeof(db))) && attempts < MAX_ATTEMPTS) {
          log_info("Database creation failed. Attempt %i.\n", attempts);
        }
    }

    (*db)->name = db_name;
    (*db)->table_count = 0;
    (*db)->tables_available = 0;
    (*db)->tables = NULL;

    status s;
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

    // Need to allocate space for the table.
    if (*table == NULL) {
        // We have no space.
        if (db->tables_available <= 0) {
            size_t max_table_count = 2 * db->table_count + 1;
            db->tables_available = max_table_count - db->table_count;
            db->tables = resize(db->tables, sizeof(table) * db->table_count,
                sizeof(table) * max_table_count);
        }
        // Assign the table to the next available space in the database.
        *table = &db->tables[db->table_count++];
    }

    // Populate table.
    (*table)->name = name;
    (*table)->col_count = 0;
    (*table)->col = malloc(sizeof(column) * num_columns);
    (*table)->length = 0;

    return global;
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

    // Need to allocate space for the column.
    if (*col == NULL) {
        // Assign the column to the next available space in the database.
        *col = &table->col[table->col_count++];
    }
    (*col)->name = name;
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
