// dsl.h
//
// CS165, Fall 2015
//
// This file defines some of the regular expressions to match the incoming DSL.
// For now, we implement regular expressions to match the create functions for
// you, and you are free to update this for all the other DSL commands.

#ifndef DSL_H__
#define DSL_H__

#include <stdlib.h>

// Currently we have 4 DSL commands to parse.
// TODO(USER): you will need to increase this to track the commands you support.
#define NUM_DSL_COMMANDS (16)

// This helps group similar DSL commands together.
// For example, some queries can be parsed together:
//
// create(col, <col_name>, <tbl_name>, unsorted)
// create(col, <col_name>, <tbl_name>, sorted)
//
// and
//
// p = select(col1, 5, 5) (point query)
// p = select(col1, 5, 10) (range query)
typedef enum DSLGroup {
    CREATE_DB,
    CREATE_TABLE,
    CREATE_COLUMN,
    RELATIONAL_INSERT,
    SELECT_COLUMN,
    SELECT_POS,
    FETCH,
    EXTREME,
    EXTREME_INDEX,
    AVERAGE,
    VECTOR_OPERATION,
    TUPLE,
    SHUTDOWNCOMMAND,
    LOADCOMMAND,
    CREATE_INDEX,
    // TODO(USER): Add more here...
} DSLGroup;

// A dsl is defined as the DSL listed on the project website.
// We use this to track the relevant string to parse, and its group.
typedef struct dsl {
    const char* c;
    DSLGroup g;
} dsl;

// This returns an array of all the DSL commands that you can match with.
dsl** dsl_commands_init(void);

// We define these in the dsl.c file.
// TODO(USER) Add more extern strings for new commands
extern const char* create_db_command;
extern const char* create_table_command;
extern const char* create_col_command_sorted;
extern const char* create_col_command_unsorted;
extern const char* relational_insert_command;
extern const char* select_column_command;
extern const char* select_pos_command;
extern const char* fetch_commad;
extern const char* extreme_value_command;
extern const char* extreme_index_command;
extern const char* average_command;
extern const char* vect_operation_command;
extern const char* tuple_command;
extern const char* load_command;
extern const char* shutdown_command;

#endif // DSL_H__
