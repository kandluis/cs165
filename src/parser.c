#include "parser.h"

#include <regex.h>
#include <string.h>
#include <ctype.h>

#include "db.h"
#include "include/var_store.h"

// Prototype for Helper function that executes that actual parsing after
// parse_command_string has found a matching regex.
status parse_dsl(char* str, dsl* d, db_operator* op);

// Finds a possible matching DSL command by using regular expressions.
// If it finds a match, it calls parse_command to actually process the dsl.
status parse_command_string(char* str, dsl** commands, db_operator* op)
{
    log_info("Parsing: %s", str);

    // Create a regular expression to parse the string
    regex_t regex;
    int ret;

    // Track the number of matches; a string must match all
    int n_matches = 1;
    regmatch_t m;

    for (int i = 0; i < NUM_DSL_COMMANDS; ++i) {
        dsl* d = commands[i];
        if (regcomp(&regex, d->c, REG_EXTENDED) != 0) {
            log_err("Could not compile regex\n");
        }

        // Bind regular expression associated with the string
        ret = regexec(&regex, str, n_matches, &m, 0);

        // If we have a match, then figure out which one it is!
        if (ret == 0) {
            log_info("Found Command: %d\n", i);
            // Here, we actually strip the command as appropriately
            // based on the DSL to get the variable names.
            return parse_dsl(str, d, op);
        }
    }

    // Nothing was found!
    status s;
    s.code = ERROR;
    return s;
}

status parse_dsl(char* str, dsl* d, db_operator* op)
{
    // Use the commas to parse out the string
    char open_paren[2] = "(";
    char close_paren[2] = ")";
    char comma[2] = ",";
    char quotes[2] = "\"";
    // char end_line[2] = "\n";
    // char eq_sign[2] = "=";

    if (d->g == CREATE_DB) {
        // Create a working copy, +1 for '\0'
        char* str_cpy = malloc(strlen(str) + 1);
        strncpy(str_cpy, str, strlen(str) + 1);

        // This gives us everything inside the (db, "<db_name>")
        strtok(str_cpy, open_paren);
        char* args = strtok(NULL, close_paren);

        // This gives us "db", but we don't need to use it
        char* db_indicator = strtok(args, comma);
        (void) db_indicator;

        // This gives us the , before the quote
        // char* delimiter = strtok(NULL, quotes);
        // (void) delimiter;

        // This gives us "<db_name>"
        char* db_name = strtok(NULL, quotes);

        log_info("create_db(%s)\n", db_name);

        // Here, we can create the DB using our parsed info!
        // First, we check to see if we've already created the db.
        db* db1 = get_var(db_name);
        if (!db1) {
            status s = create_db(db_name, &db1);
            if (s.code != OK) {
                log_err("Error creating database: %s\n", s.error_message);
                free(str_cpy);
                return s;
            }
            else {
                set_var(db_name, db1);
            }
        }

        // Free the str_cpy
        free(str_cpy);

        // No db_operator required, since no query plan
        (void) op;
        status ret;
        ret.code = OK;
        return ret;
    } else if (d->g == CREATE_TABLE) {
        // Create a working copy, +1 for '\0'
        char* str_cpy = malloc(strlen(str) + 1);
        strncpy(str_cpy, str, strlen(str) + 1);

        // This gives us everything inside the (table, <tbl_name>, <db_name>, <count>)
        strtok(str_cpy, open_paren);
        char* args = strtok(NULL, close_paren);

        // This gives us "table"
        char* tbl_indicator = strtok(args, comma);
        (void) tbl_indicator;

        // This gives us <tbl_name>, we will need this to create the full name
        char* tbl_name = strtok(NULL, quotes);

        // This gives us <db_name>, we will need this to create the full name
        char* db_name = strtok(NULL, comma);

        // Generate the full name using <db_name>.<tbl_name>
        char* full_name = (char*)malloc(sizeof(char)*(strlen(tbl_name) + strlen(db_name) + 2));

        strncat(full_name, db_name, strlen(db_name));
        strncat(full_name, ".", 1);
        strncat(full_name, tbl_name, strlen(tbl_name));

        // This gives us count
        char* count_str = strtok(NULL, comma);
        int count = 0;
        if (count_str != NULL) {
            count = atoi(count_str);
        }
        (void) count;

        log_info("create_table(%s, %s, %d)\n", full_name, db_name, count);

        // Find database.
        status ret;
        db* db1 = get_var(db_name);
        if (!db1) {
            log_err("No database found. %s: error at line: %d\n", __func__, __LINE__);
            ret.code = ERROR;
            ret.error_message = "Database does not exist.";
            return ret;
        }

        // TODO(luisperez): We only support one db for now, so no duplicate names allowed.
        table* tbl1 = get_var(tbl_name);
        if (!tbl1) {
            status s = create_table(db1, full_name, count, &tbl1);
            if (s.code != OK) {
                log_err("Table creation failed. %s: error at line: %d\n", __func__, __LINE__);
                free(str_cpy);
                return s;
            }
            else {
                set_var(tbl_name, tbl1);
            }
        }

        // Free the str_cpy
        free(str_cpy);
        free(full_name);
        str_cpy=NULL;
        full_name=NULL;

        // No db_operator required, since no query plan
        ret.code = OK;
        return ret;
    } else if (d->g == CREATE_COLUMN) {
        // Create a working copy, +1 for '\0'
        char* str_cpy = malloc(strlen(str) + 1);
        strncpy(str_cpy, str, strlen(str) + 1);

        // This gives us everything inside the (col, <col_name>, <tbl_name>, unsorted)
        strtok(str_cpy, open_paren);
        char* args = strtok(NULL, close_paren);

        // This gives us "col"
        char* col_indicator = strtok(args, comma);
        (void) col_indicator;

        // This gives us <col_name>, we will need this to create the full name
        char* col_name = strtok(NULL, quotes);

        // This gives us <tbl_name>, we will need this to create the full name
        char* tbl_name = strtok(NULL, comma);

        // Generate the full name using <db_name>.<tbl_name>
        char* full_name=(char*)malloc(sizeof(char)*(strlen(tbl_name) + strlen(col_name) + 2));

        strncat(full_name, tbl_name, strlen(tbl_name));
        strncat(full_name, ".", 1);
        strncat(full_name, col_name, strlen(col_name));

        // This gives us the "unsorted"
        char* sorting_str = strtok(NULL, comma);
        (void) sorting_str;

        log_info("create_column(%s, %s, %s)\n", full_name, tbl_name, sorting_str);

        // Grab the table, which should already exist.
        // TODO(luisperez) : Currently only support one database.
        status ret;
        table* tbl1 = get_var(tbl_name);
        if (!tbl1) {
            log_err("No table found. %s: error at line: %d\n", __func__, __LINE__);
            ret.code = ERROR;
            ret.error_message = "Table does not exist.";
            return ret;
        }

        column* col1 = get_var(full_name);
        if (!col1) {
            status s = create_column(tbl1, full_name, &col1);
            if (s.code != OK) {
                log_err("Column creation failed. %s: error at line: %d\n", __func__, __LINE__);
                free(str_cpy);
                return s;
            }
            else {
                set_var(full_name, tbl1);
            }
        }

        // Free the str_cpy
        free(str_cpy);
        free(full_name);
        str_cpy=NULL;
        full_name=NULL;

        // No db_operator required, since no query plan
        ret.code = OK;
        return ret;
    }

    // Should have been caught earlier...
    status fail;
    fail.code = ERROR;
    return fail;
}
