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
    (void) db;
    (void) name;
    (void) num_columns;
    (void) table;
    return global;
}

status drop_table(db* db, table* table)
{
    (void) db;
    (void) table;
    return global;
}

status create_column(table *table, const char* name, column** col)
{
    (void) table;
    (void) name;
    (void) col;
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
