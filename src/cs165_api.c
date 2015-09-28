// Copyright 2015 <Luis Perez>

#include "include/cs165_api.h"

status open_db(const char* filename, db** db, OpenFlags flags);

status drop_db(db* db);

status sync_db(db* db);

status create_db(const char* db_name, db** db);

status create_table(db* db, const char* name, size_t num_columns, table** table);

status drop_table(db* db, table* table);

status create_column(table *table, const char* name, column** col);

status create_index(column* col, IndexType type);

status insert(column *col, int data);
status delete(column *col, int *pos);
status update(column *col, int *pos, int new_val);
status col_scan(comparator *f, column *col, result **r);
status index_scan(comparator *f, column *col, result **r);

status query_prepare(const char* query, db_operator** op);
status query_execute(db_operator* op, result** results);
