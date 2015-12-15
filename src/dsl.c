#include "dsl.h"

// Create Commands
// Matches: create(db, <db_name>);
const char* create_db_command = "^create\\(db\\,\\\"[a-zA-Z0-9_]+\\\"\\)";

// Matches: create(tbl, <table_name>, <db_name>, <col_count>);
const char* create_table_command = "^create\\(tbl\\,\\\"[a-zA-Z0-9_\\.]+\\\"\\,[a-zA-Z0-9_\\.]+\\,[0-9]+\\)";

// Matches: create(col, <col_name>, <tbl_var>, sorted);
const char* create_col_command_sorted = "^create\\(col\\,\\\"[a-zA-Z0-9_\\.]+\\\"\\,[a-zA-Z0-9_\\.]+\\,sorted)";

// Matches: create(col, <col_name>, <tbl_var>, unsorted);
const char* create_col_command_unsorted = "^create\\(col\\,\\\"[a-zA-Z0-9_\\.]+\\\"\\,[a-zA-Z0-9_\\.]+\\,unsorted)";

// Matches: relational_insert(<tbl_var>,[INT1],[INT2],...);
// const char* relational_insert_command = "^relational_insert\\([a-zA-Z0-9_\\.]+\\,([0-9]+\\,)+[0-9]+\\)";
const char* relational_insert_command = "^relational_insert(*)";

// Matches: select from column
const char* select_column_command = "^[a-zA-Z0-9_\\.]+\\=select\\([a-zA-Z0-9_\\.]+\\,([0-9]+|null)\\,([0-9]+|null)\\)";

// Matches: select from positions in column
const char* select_pos_command = "^[a-zA-Z0-9_\\.]+\\=select\\([a-zA-Z0-9_\\.]+\\,[a-zA-Z0-9_\\.]+\\,([0-9]+|null)\\,([0-9]+|null)\\)";

// Matches: fetch for fetching values from a column
const char* fetch_commad = "^[a-zA-Z0-9_\\.]+\\=fetch\\([a-zA-Z0-9_\\.]+\\,[a-zA-Z0-9_\\.]+\\)";

// Matches: min/amx aggregate
const char* extreme_value_command = "^[a-zA-Z0-9_\\.]+\\=(min|max)\\([a-zA-Z0-9_\\.]+\\)";

// Matches: min/max index aggregate
const char* extreme_index_command = "^[a-zA-Z0-9_\\.]+\\,[a-zA-Z0-9_\\.]+\\=(min|max)\\((null|[a-zA-Z0-9_\\.]+)\\,[a-zA-Z0-9_\\.]+\\)";

// MAtches: average
const char* average_command = "^[a-zA-Z0-9_\\.]+\\=avg\\([a-zA-Z0-9_\\.]+\\)";

// Matches: vector operation to either add or subtract
const char* vect_operation_command = "^[a-zA-Z0-9_\\.]+\\=(add|sub)\\([a-zA-Z0-9_\\.]+\\,[a-zA-Z0-9_\\.]+\\)";

// Matches: tuple operation for printing
const char* tuple_command = "^tuple(*)";

// TODO(USER): You will need to update the commands here for every single command you add.
dsl** dsl_commands_init(void)
{
    dsl** commands = calloc(NUM_DSL_COMMANDS, sizeof(dsl*));

    for (int i = 0; i < NUM_DSL_COMMANDS; ++i) {
        commands[i] = malloc(sizeof(dsl));
    }

    // Assign the create commands
    commands[0]->c = create_db_command;
    commands[0]->g = CREATE_DB;

    commands[1]->c = create_table_command;
    commands[1]->g = CREATE_TABLE;

    commands[2]->c = create_col_command_sorted;
    commands[2]->g = CREATE_COLUMN;

    commands[3]->c = create_col_command_unsorted;
    commands[3]->g = CREATE_COLUMN;

    commands[4]->c = relational_insert_command;
    commands[4]->g = RELATIONAL_INSERT;

    commands[5]->c = select_column_command;
    commands[5]->g = SELECT_COLUMN;

    commands[6]->c = select_pos_command;
    commands[6]->g = SELECT_POS;

    commands[7]->c = fetch_commad;
    commands[7]->g = FETCH;

    commands[8]->c = extreme_value_command;
    commands[8]->g = EXTREME;

    commands[9]->c = extreme_index_command;
    commands[9]->g = EXTREME_INDEX;

    commands[10]->c = average_command;
    commands[10]->g = AVERAGE;

    commands[11]->c = vect_operation_command;
    commands[11]->g = VECTOR_OPERATION;

    commands[12]->c = tuple_command;
    commands[12]->g = TUPLE;

    return commands;
}
