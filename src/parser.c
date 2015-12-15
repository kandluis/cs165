#include "parser.h"

#include <regex.h>
#include <string.h>
#include <ctype.h>

#include "db.h"
#include "include/var_store.h"

// TODO(luisperez): Are we supposed to return an error if the "thing" we are
// creating already exists?

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
    s.error_message = "No matching commands found.\n";
    return s;
}

status parse_dsl(char* str, dsl* d, db_operator* op)
{
    // Use the commas to parse out the string
    char open_paren[2] = "(";
    char close_paren[2] = ")";
    char comma[2] = ",";
    char quotes[2] = "\"";
    char eq_sign[2] = "=";
    // char end_line[2] = "\n";

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
        status ret;
        db* db1 = get_var(db_name);
        if (!db1) {
            status s = create_db(db_name, &db1);
            if (s.code != OK) {
                log_err("Error creating database.\n");

                ret.code = ERROR;
                ret.error_message = "Database creation failed.\n";
            }
            else {
                set_var(db_name, db1);
                ret.code = OK;
            }
        }
        else {
            log_err("Database %s already exists!", db_name);
            ret.code = ERROR;
            ret.error_message = "Database already exists!\n";
        }

        (void) op;
        free(str_cpy);
        str_cpy = NULL;
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
        char* full_name = (char*)calloc(strlen(tbl_name) + strlen(db_name) + 2, sizeof(char));

        strncat(full_name, db_name, strlen(db_name));
        strncat(full_name, ".", 1);
        strncat(full_name, tbl_name, strlen(tbl_name));

        // This gives us count
        char* count_str = strtok(NULL, comma);
        int count = 0;
        if (count_str != NULL) {
            count = atoi(count_str);
        }

        log_info("create_table(%s, %s, %d)\n", full_name, db_name, count);

        // Find database.
        status ret;
        db* db1 = get_var(db_name);
        if (!db1) {
            log_err("No database found. %s: error at line: %d\n", __func__, __LINE__);

            ret.code = ERROR;
            ret.error_message = "Database does not exist.\n";
        }
        else {
            // We now support multiple dbs.
            table* tbl1 = get_var(full_name);
            if (!tbl1) {
                status s = create_table(db1, full_name, count, &tbl1);
                if (s.code != OK) {
                    log_err("Table creation failed. %s: error at line: %d\n", __func__, __LINE__);

                    ret.code = ERROR;
                    ret.error_message = "Table creation failed.\n";
                }
                else {
                    set_var(full_name, tbl1);
                    ret.code = OK;
                }
            }
            else {
                log_err("Table %s already exists!", full_name);
                ret.code = ERROR;
                ret.error_message = "Table already exists!\n";
            }
        }

        // Free the str_cpy
        free(str_cpy);
        free(full_name);
        str_cpy=NULL;
        full_name=NULL;

        (void) op;
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
        char* full_name=(char*)calloc(strlen(tbl_name) + strlen(col_name) + 2, sizeof(char));

        strncat(full_name, tbl_name, strlen(tbl_name));
        strncat(full_name, ".", 1);
        strncat(full_name, col_name, strlen(col_name));

        // This gives us the "unsorted"
        char* sorting_str = strtok(NULL, comma);
        (void) sorting_str;

        log_info("create_column(%s, %s, %s)\n", full_name, tbl_name, sorting_str);

        // Grab the table, which should already exist.
        status ret;
        table* tbl1 = get_var(tbl_name);
        if (!tbl1) {
            log_err("No table found. %s: error at line: %d\n", __func__, __LINE__);
            ret.code = ERROR;
            ret.error_message = "Table does not exist.\n";
            return ret;
        }
        else {
            // Check to see if column already exists.
            column* col1 = get_var(full_name);
            if (!col1) {
                status s = create_column(tbl1, full_name, &col1);
                if (s.code != OK) {
                    log_err("Column creation failed. %s: error at line: %d\n", __func__, __LINE__);
                    ret.code = ERROR;
                    ret.error_message = "Column creation failed!\n";
                }
                else {
                    ret.code = OK;
                    set_var(full_name, col1);
                }
            }
            else {
                log_err("Column %s already exists!", full_name);
                ret.code = ERROR;
                ret.error_message = "Column already exists!\n";
            }
        }

        // Free the str_cpy
        free(str_cpy);
        free(full_name);
        str_cpy=NULL;
        full_name=NULL;

        // No db_operator required, since no query plan
        (void) op;

        ret.code = OK;
        return ret;
    } else if (d->g == RELATIONAL_INSERT) {
        // Create a working copy, +1 for '\0'
        char* str_cpy = malloc(strlen(str) + 1);
        strncpy(str_cpy, str, strlen(str) + 1);

        // This gives us everything inside the (<tbl_name>, [INT1], [INT2], ...)
        strtok(str_cpy, open_paren);
        char* args = strtok(NULL, close_paren);

        // This gives us <tlb_name>
        char* tbl_name = strtok(args, comma);
        status ret;

        // Get the pointer to the table.
        table* tbl1 = get_var(tbl_name);
        if (!tbl1) {
            log_err("Table %s does not exists. Cannot insert!", tbl_name);
            ret.code = ERROR;
            ret.error_message = "Table does not exists!";
        }
        else {
            // Now we need to create a query plan?
            op->type = INSERT;

            // Add table
            op->tables = malloc(sizeof(struct table*));
            op->tables[0] = tbl1;

            // Add column pointers
            op->columns = tbl1->col;

            // Add values extracted from input by users
            char* count_str;
            int count;
            op->value1 = malloc(sizeof(int) * tbl1->col_count);
            for (size_t i = 0; i < tbl1->col_count; i++) {
                count_str = strtok(NULL, comma);
                if (count_str) {
                    count = atoi(count_str);
                     op->value1[i] = count;
                } else {
                    log_err("Could not parse insert value into table %s\n", tbl_name);
                }
            }

            // All others are set to null
            op->var_name = NULL;
            op->pos1 = NULL;
            op->pos2 = NULL;
            op->value2 = NULL;
            op->c = NULL;
            // NOTE: agg is not set?
        }

        // Return the status
        free(str_cpy);

        ret.code = OK;
        return ret;


    } else if (d->g == SELECT_POS || d->g == SELECT_COLUMN) {
        // Create a working copy, +1 for '\0'
        char* str_cpy = malloc(strlen(str) + 1);
        strncpy(str_cpy, str, strlen(str) + 1);

        // We split on the equals sign <vec_pos>=select([<posn_vec>, ]<col_var>,<low>,<high>)
        char* vec_pos = strtok(str_cpy, eq_sign);
        op->var_name = copystr(vec_pos);

        // Now we extract everything inside ([<posn_vec>, ]<col_var>,<low>,<high>)
        strtok(NULL, open_paren);
        char* args = strtok(NULL, close_paren);

        status ret;
        if (d->g == SELECT_POS) {
            // This gives use <posn_vec>
            char* posn_name = strtok(args, comma);
            int* posn_vec = get_var(posn_name);
            if (!posn_vec) {
                log_err("Variable %s not defined. %s: error at line %d\n",
                    posn_name, __func__, __LINE__);
                ret.code = ERROR;
                ret.error_message = "Undefined variable";
                free(str_cpy);
                return ret;
            }
            op->pos1 = posn_vec;
        }
        else {
            op->pos1 = NULL;
        }

        // this gives us col_var
        char* col_name = strtok((d->g == SELECT_COLUMN ) ?  args : NULL, comma);
        column* col1 = get_var(col_name);
        if (!col1) {
            log_err("Column %s does not exists. Cannot select!", col_name);
            ret.code = ERROR;
            ret.error_message = "Column does not exist!";
            free(str_cpy);
            return ret;
        }

        // We now prepare the query operator
        op->type = SELECT;
        op->tables = NULL;

        // TODO: FREE THIS!
        op->columns = malloc(sizeof(struct column*));
        if (!(op->columns)) {
            log_err("Low memory. %s: error at line %d\n", __func__, __LINE__);
            ret.code = ERROR;
            ret.error_message = "Low memory\n";
            free(str_cpy);
            return ret;
        }
        op->columns[0] = col1;
        op->pos2 = NULL;
        op->value1 = NULL;
        op->value2 = NULL;

        // Note agg is not set

        // We extract the low and the high
        char* low_str = strtok(NULL, comma);
        char* high_str = strtok(NULL, comma);
        if (!low_str || !high_str) {
            log_err("Low memory. %s: error at line %d\n", __func__, __LINE__);
            ret.code = ERROR;
            ret.error_message = "Low memory\n";
            free(str_cpy);
            free(op->columns);
            return ret;
        }

        // Head of our comparator!
        comparator* c = NULL;
        // We have a query looking for values low <= x (x > low)
        if (strcmp(low_str, "null") != 0) {
            int low = atoi(low_str);
            // TODO FREE THIS
            comparator* tmp = malloc(sizeof(comparator));
            tmp->p_val = low;
            tmp->col = col1;
            tmp->type = GREATER_THAN;
            tmp->next_comparator = c;
            tmp->mode = NONE;

            // New head.
            c = tmp;
        }
        if (strcmp(high_str, "null") != 0) {
            int high = atoi(high_str);
            // TODO FREE THIS
            comparator* tmp = malloc(sizeof(comparator));
            tmp->p_val = high;
            tmp->col = col1;
            tmp->type = LESS_THAN;
            tmp->next_comparator = c;
            tmp->mode = AND;

            // New head.
            c = tmp;
        }

        // Add it to the query!
        op->c = c;

        free(str_cpy);

        ret.code = OK;
        return ret;

    }

    // Should have been caught earlier...
    status fail;
    fail.code = ERROR;
    fail.error_message = "No matching operation!\n";
    return fail;
}
