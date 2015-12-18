#ifndef DB_H__
#define DB_H__

#include "cs165_api.h"

#define MAX_ATTEMPTS 3 // Maximum attempts before failing the execution of a command.

// We assume that the able is already clustered and we're just changing
// it's type. No need to verify the existence of an index as this function
// should only be called on a column that has already been clustered.
// In our case, we only support BTrees and SortedIndex.
status recluster(table* tbl, IndexType type);

#endif // DB_H__
