#include "parser.h"

#include <regex.h>
#include <string.h>
#include <ctype.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include "db.h"
#include "include/common.h"
#include "include/var_store.h"

// Global column to store pointers to databases we've created/loaded etc.
extern Storage databases;

// Global variable to keep track of whether the system has changed.
extern int changed;

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

    // RESET THE OPERATOR
    op->type = NOTAVAILABLE;

    if (d->g == CREATE_DB) {
        // Mark database as modified so we persist.
        changed = 1;

        // Create a working copy, +1 for '\0'
        char* str_cpy = malloc(strlen(str) + 1);
        strncpy(str_cpy, str, strlen(str) + 1);

        // This gives us everything inside the (db,"<db_name>")
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
        db* db1 = get_resource(db_name);
        if (!db1) {
            status s = create_db(db_name, &db1);
            if (s.code != OK) {
                log_err("Error creating database.\n");

                ret.code = ERROR;
                ret.error_message = "Database creation failed.\n";
            }
            else {
                set_resource(db_name, db1);
                // Store in our global pointer
                if (databases.count >= databases.size) {
                    // need to resize the array
                    databases.data = realloc(databases.data, (2 * databases.count + 1) * sizeof(struct db*));
                }
                databases.data[databases.count++] = db1;
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
        // Mark database as modified so we persist.
        changed = 1;

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
        db* db1 = get_resource(db_name);
        if (!db1) {
            log_err("No database found. %s: error at line: %d\n", __func__, __LINE__);

            ret.code = ERROR;
            ret.error_message = "Database does not exist.\n";
        }
        else {
            // We now support multiple dbs.
            table* tbl1 = get_resource(full_name);
            if (!tbl1) {
                status s = create_table(db1, full_name, count, &tbl1);
                if (s.code != OK) {
                    log_err("Table creation failed. %s: error at line: %d\n", __func__, __LINE__);

                    ret.code = ERROR;
                    ret.error_message = "Table creation failed.\n";
                }
                else {
                    set_resource(full_name, tbl1);
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
        // Mark database as modified so we persist.
        changed = 1;

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

        log_info("create_column(%s, %s, %s)\n", full_name, tbl_name, sorting_str);

        // Grab the table, which should already exist.
        status ret;
        table* tbl1 = get_resource(tbl_name);
        if (!tbl1) {
            log_err("No table found. %s: error at line: %d\n", __func__, __LINE__);
            ret.code = ERROR;
            ret.error_message = "Table does not exist.\n";
        }
        else {
            // Check to see if column already exists.
            column* col1 = get_resource(full_name);
            if (!col1) {
                status s = create_column(tbl1, full_name, &col1);
                if (s.code != OK) {
                    log_err("Column creation failed. %s: error at line: %d\n", __func__, __LINE__);
                    ret.code = ERROR;
                    ret.error_message = "Column creation failed!\n";
                }
                else {
                    // Set the correct index type
                    if (strcmp(sorting_str, "sorted") == 0) {
                        col1->index = calloc(1, sizeof(struct column_index));
                        if (!col1->index) {
                            log_err("Index creation failed. ^s: error at line %d\n",
                                __func__, __LINE__);
                            ret.code = ERROR;
                            ret.error_message = "Failed inserting sorted column";
                        }
                        col1->index->type = SORTED;
                        col1->index->index = (void*) col1->data;


                        // The table is clustered on this column
                        tbl1->cluster_column = col1;
                    }
                    set_resource(full_name, col1);
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
        // Mark database as modified so we persist.
        changed = 1;

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
        table* tbl1 = get_resource(tbl_name);
        if (!tbl1) {
            log_err("Table %s does not exists. Cannot insert!", tbl_name);
            ret.code = ERROR;
            ret.error_message = "Table does not exists!";
        }
        else {
            // Now we need to create a query plan?
            op->type = INSERT;

            // Add table
            op->tables = calloc(1, sizeof(struct table*));
            op->tables[0] = tbl1;

            // Add column pointers
            op->columns = tbl1->col;

            // Add values extracted from input by users
            char* count_str;
            int count;
            op->value1 = calloc(tbl1->col_count, sizeof(Data));
            for (size_t i = 0; i < tbl1->col_count; i++) {
                count_str = strtok(NULL, comma);
                if (count_str) {
                    count = atoi(count_str);
                     op->value1[i].i = count;
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
            column* posn_vec = get_var(posn_name);
            if (!posn_vec) {
                // Try the resource
                posn_vec = get_resource(posn_name);
                if (!posn_vec) {
                    log_err("Variable %s not defined. %s: error at line %d\n",
                        posn_name, __func__, __LINE__);
                    ret.code = ERROR;
                    ret.error_message = "Undefined variable";
                    free(str_cpy);
                    return ret;
                }
            }

            // We overload these operator fields.
            op->pos1 = posn_vec->data;
            op->pos1type = posn_vec->type;
            op->value1 = (Data*) &(posn_vec->count);
            op->value1type = LONGINT;
        }
        else {
            op->pos1 = NULL;
            op->value1 = NULL;
        }

        // this gives us col_var
        char* col_name = strtok((d->g == SELECT_COLUMN ) ?  args : NULL, comma);
        column* col1 = get_resource(col_name);
        if (!col1) {
            col1 = get_var(col_name);
            if (!col1) {
                log_err("Column %s does not exists. Cannot select!", col_name);
                ret.code = ERROR;
                ret.error_message = "Column does not exist!";
                free(str_cpy);
                return ret;
            }
        }

        // We now prepare the query operator
        op->type = SELECT;
        op->tables = NULL;

        op->columns = calloc(1, sizeof(struct column*));
        if (!(op->columns)) {
            log_err("Low memory. %s: error at line %d\n", __func__, __LINE__);
            ret.code = ERROR;
            ret.error_message = "Low memory\n";
            free(str_cpy);
            return ret;
        }
        op->columns[0] = col1;
        op->pos2 = NULL;
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
            comparator* tmp = calloc(1, sizeof(struct comparator));
            tmp->p_val = low;
            tmp->col = col1;
            tmp->type = GREATER_THAN | EQUAL;
            tmp->next_comparator = c;
            tmp->mode = NONE;

            // New head.
            c = tmp;
        }
        if (strcmp(high_str, "null") != 0) {
            int high = atoi(high_str);
            // TODO FREE THIS
            comparator* tmp = calloc(1, sizeof(struct comparator));
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
    else if (d->g == FETCH) {
        // Create a working copy, +1 for '\0'
        char* str_cpy = malloc(strlen(str) + 1);
        strncpy(str_cpy, str, strlen(str) + 1);

        // We split on the equals sign <vec_pos>=fetch(<col_var>,<vec_pos>)
        char* vec_pos = strtok(str_cpy, eq_sign);
        op->var_name = copystr(vec_pos);

        // Now we extract everything inside (<col_var>,<vec_pos>)
        strtok(NULL, open_paren);
        char* args = strtok(NULL, close_paren);

        status ret;
        // This gives us col_var
        char* col_name = strtok(args, comma);
        column* col1 = get_resource(col_name);
        if (!col1) {
            log_err("Column %s does not exists. Cannot select!", col_name);
            ret.code = ERROR;
            ret.error_message = "Column does not exist!";
            free(op->var_name);
            free(str_cpy);
            return ret;
        }
        // This gives use <posn_vec>
        char* posn_name = strtok(NULL, comma);
        column* posn_vec = get_var(posn_name);
        if (!posn_vec) {
            // Try the resource!
            posn_vec = get_resource(posn_name);
            if (!posn_vec) {
                log_err("Variable %s not defined. %s: error at line %d\n",
                    posn_name, __func__, __LINE__);
                ret.code = ERROR;
                ret.error_message = "Undefined variable";
                free(op->var_name);
                free(str_cpy);
                return ret;
            }
        }


        // We now execute the operation (TODO -- is this valid?)
        result* r = NULL;
        status s = fetch(col1, posn_vec, &r);
        if (s.code != OK) {
            log_err("Fetch operation failed %s. %s: error in line %d",
                s.error_message, __func__, __LINE__);
            free(str_cpy);
            free(op->var_name);
            ret.code = ERROR;
            ret.error_message = "Fetch failed";
            return ret;
        }

        // Store into the variable pool
        column* narray = calloc(1, sizeof(struct column));
        narray->data = r->payload;
        narray->type = r->type;
        narray->size = r->num_tuples;
        narray->count = r->num_tuples;
        set_var(op->var_name, narray);

        free(str_cpy);
        free(op->var_name);
        free(r);
        ret.code = OK;
        return ret;
    }
    else if (d->g == EXTREME) {
        // Create a working copy, +1 for '\0'
        char* str_cpy = malloc(strlen(str) + 1);
        strncpy(str_cpy, str, strlen(str) + 1);

        // We split on the equals sign <[min_val|max_val]>=min(<vec_val>)
        char* val_str = strtok(str_cpy, eq_sign);

        // Now we extract everything inside (<vec_val>)
        char* fun_str = strtok(NULL, open_paren);
        char* vec_val_str = strtok(NULL, close_paren);

        // Look up the vector in our variable pool
        status ret;
        column* vec_val = get_var(vec_val_str);
        if (!vec_val) {
            // Try the resource.
            vec_val = get_resource(vec_val_str);
            if (!vec_val) {
                log_err("Variable %s not defined. %s: error at line %d\n",
                    vec_val_str, __func__, __LINE__);
                ret.code = ERROR;
                ret.error_message = "Undefined variable";
                free(str_cpy);
                return ret;
            }
        }

        // Find the minimum or maximum (ONLY SUPPORT INTEGERS)
        Data* res = calloc(1, sizeof(Data));
        *res = vec_val->data[0];
        if (strcmp(fun_str, "min") == 0) {
            for (size_t i = 0; i < vec_val->count; i++) {
                if (vec_val->data[i].i < res->i) {
                    *res = vec_val->data[i];
                }
            }
        }
        else if (strcmp(fun_str, "max") == 0) {
            for (size_t i = 0; i < vec_val->count; i++) {
                if (vec_val->data[i].i > res->i) {
                    *res = vec_val->data[i];
                }
            }
        }
        else {
            ret.error_message = "Unsupported operation.\n";
            ret.code = ERROR;
            log_err(ret.error_message);
            free(str_cpy);
            free(res);
            return ret;
        }

        // Store the result the a vector of length 1
        column* col = calloc(1, sizeof(struct column));
        col->data = res;
        col->type = INT;
        col->size = 1;
        col->count = 1;
        set_var(val_str, col);

        free(str_cpy);

        ret.code = OK;
        return ret;
    }
    else if (d->g == EXTREME_INDEX) {
         // Create a working copy, +1 for '\0'
        char* str_cpy = malloc(strlen(str) + 1);
        strncpy(str_cpy, str, strlen(str) + 1);

        // We split on the equals sign <min_pos>,<min_val>=min(<vec_pos>,<vec_val>)
        // or max!
        char* str = strtok(str_cpy, eq_sign);

        // Now we extract everything inside (<vec_pos>, <vec_val>)
        char* fun_str = strtok(NULL, open_paren);
        char* args = strtok(NULL, close_paren);
        char* vec_pos_str = strtok(args, comma);
        char* vec_val_str = strtok(NULL, comma);

        // Now go back an extract the location where results will be stored
        char* pos_str = strtok(str, comma);
        char* val_str = strtok(NULL, comma);

        // Look up the vector in our variable pool
        status ret;
        column* vec_val = get_var(vec_val_str);
        if (!vec_val) {
            vec_val = get_resource(vec_val_str);
            if (!vec_val) {
                log_err("Variable %s not defined. %s: error at line %d\n",
                    vec_val_str, __func__, __LINE__);
                ret.code = ERROR;
                ret.error_message = "Undefined variable";
                free(str_cpy);
                return ret;
            }
        }
        column* vec_pos = get_var(vec_pos_str);
        if (!vec_pos && strcmp(vec_pos_str, "null") != 0) {
            // Try the resource pool
            vec_pos = get_resource(vec_pos_str);
            if (!vec_pos) {
                log_err("Variable %s not defined. %s: error at line %d\n",
                    vec_pos_str, __func__, __LINE__);
                ret.code = ERROR;
                ret.error_message = "Undefined variable";
                free(str_cpy);
                return ret;
            }
        }

        // Find the index of the minimum or maximum
        Data* res = calloc(1, sizeof(Data));
        *res = vec_val->data[0];
        if (strcmp(fun_str, "min") == 0) {
            for (size_t i = 0; i < vec_val->count; i++) {
                if (vec_val->data[i].i < res->i) {
                    res->i = i;
                }
            }
        }
        else if (strcmp(fun_str, "max") == 0) {
            for (size_t i = 0; i < vec_val->count; i++) {
                if (vec_val->data[i].i > res->i) {
                    res->i = i;
                }
            }
        }
        else {
            ret.error_message = "Unsupported operation.\n";
            ret.code = ERROR;
            log_err(ret.error_message);
            free(str_cpy);
            free(res);
            return ret;
        }

        // Store the value result
        Data* value = calloc(1, sizeof(Data));
        *value = vec_val->data[res->i];

        // Store in an array of length 1
        column* col = calloc(1, sizeof(struct column));
        col->data = value;
        col->type = INT;
        col->size = 1;
        col->count = 1;
        set_var(val_str, col);

        // Determine index to store
        if (vec_pos) {
            *res = vec_pos->data[res->i];
        }

        // Store in an array of length 1
        col = calloc(1, sizeof(struct column));
        col->data = res;
        col->type = INT;
        col->size = 1;
        col->count = 1;
        set_var(pos_str, col);

        ret.code = OK;
        free(str_cpy);
        return ret;
    }

    else if (d->g == AVERAGE) {
        // Create a working copy, +1 for '\0'
        char* str_cpy = malloc(strlen(str) + 1);
        strncpy(str_cpy, str, strlen(str) + 1);

        // We split on the equals sign <scl_val>=avg(<vec_val>)
        char* scl_str = strtok(str_cpy, eq_sign);

        // Now we extract everything inside (<vec_val>)
        strtok(NULL, open_paren);
        char* vec_val_str = strtok(NULL, close_paren);

        // Look up the vector in our variable pool
        status ret;
        column* vec_val = get_var(vec_val_str);
        if (!vec_val) {
            vec_val = get_resource(vec_val_str);
            if (!vec_val) {
                log_err("Variable %s not defined. %s: error at line %d\n",
                    vec_val_str, __func__, __LINE__);
                ret.code = ERROR;
                ret.error_message = "Undefined variable";
                free(str_cpy);
                return ret;
            }
        }

        // Find the average
        long int sum = 0.;
        if (vec_val->type == LONGINT) {
            for (size_t i = 0; i < vec_val->count; i++) {
                sum += vec_val->data[i].li;
            }
        }
        else if (vec_val->type == DOUBLE) {
            for (size_t i = 0; i < vec_val->count; i++) {
                sum += vec_val->data[i].f;
            }

        }
        else if (vec_val->type == INT) {
            for (size_t i = 0; i < vec_val->count; i++) {
                sum += vec_val->data[i].i;
            }

        }
        else {
            log_err("Unsupported data type. %s: error in line %d\n",
                __func__, __LINE__);
            ret.code = ERROR;
            ret.error_message = "Unsupported data type";
            free(str_cpy);
            return ret;

        }
        Data* res = calloc(1, sizeof(Data));
        res->f = (double) sum / ((double) vec_val->count);

        // Size of vector is 1
        column* col = calloc(1, sizeof(struct column));
        // TODO: Figure out how to deal with these!
        col->data = res;
        col->type = DOUBLE;
        col->size = 1;
        col->count = 1;
        set_var(scl_str, col);
        free(str_cpy);

        ret.code = OK;
        return ret;
    }
    else if (d->g == VECTOR_OPERATION) {
        // Create a working copy, +1 for '\0'
        char* str_cpy = malloc(strlen(str) + 1);
        strncpy(str_cpy, str, strlen(str) + 1);

        // We split on the equals sign <vec_val>=[add|sub](<vec_val1>,<vec_val2>)
        char* vec_val_str = strtok(str_cpy, eq_sign);

        // Now we extract everything inside (<vec_val>)
        char* fun_str = strtok(NULL, open_paren);
        char* args = strtok(NULL, close_paren);

        // Now extract each vector
        status ret;
        char* vec_val1_str = strtok(args, comma);
        column* vec_val1 = get_var(vec_val1_str);
        if (!vec_val1) {
            vec_val1 = get_resource(vec_val1_str);
            if (!vec_val1) {
                log_err("Variable %s not defined. %s: error at line %d\n",
                    vec_val1_str, __func__, __LINE__);
                ret.code = ERROR;
                ret.error_message = "Undefined variable";
                free(str_cpy);
                return ret;
            }
        }
        char* vec_val2_str = strtok(NULL, comma);
        column* vec_val2 = get_var(vec_val2_str);
        if (!vec_val2) {
            vec_val2 = get_resource(vec_val2_str);
            if (!vec_val2) {
                log_err("Variable %s not defined. %s: error at line %d\n",
                    vec_val2_str, __func__, __LINE__);
                ret.code = ERROR;
                ret.error_message = "Undefined variable";
                free(str_cpy);
                return ret;
            }
        }

        // Vectors must be the same size.
        if (vec_val1->count != vec_val2->count) {
            log_err("Vectors of different size: %d, %d",
                vec_val1->count, vec_val2->count);
            ret.code = ERROR;
            ret.error_message = "Incompatible operands";
            free(str_cpy);
            return ret;
        }
        size_t n = vec_val1->count;
        // Allocate space for the result.
        column* res = calloc(1, sizeof(struct column));
        res->size = n;
        res->count = n;
        res->data = calloc(res->size, sizeof(Data));
        res->type = LONGINT;

        // Determine operation to be performed!
        // TODO (What would be really cool would be to do all of this lazily!)
        if (strcmp(fun_str, "sub") == 0) {
            for (size_t i = 0; i < n; i++) {
                res->data[i].li = (long int) vec_val1->data[i].i - (long int) vec_val2->data[i].i;
            }
        }
        else if (strcmp(fun_str, "add") == 0) {
            for (size_t i = 0; i < n; i++) {
                res->data[i].li = (long int) vec_val1->data[i].i + (long int) vec_val2->data[i].i;
            }
        }
        else {
            ret.error_message = "Unsupported operation.\n";
            ret.code = ERROR;
            log_err(ret.error_message);
            free(str_cpy);
            free(res);
            return ret;
        }

        set_var(vec_val_str, res);
        free(str_cpy);
        ret.code = OK;
        return ret;
    }
    else if (d->g == TUPLE) {

        // Create a working copy, +1 for '\0'
        char* str_cpy = malloc(strlen(str) + 1);
        strncpy(str_cpy, str, strlen(str) + 1);

        // Now we extract everything inside tuple(<vec_val1>,...)
        strtok(str_cpy, open_paren);
        char* args = strtok(NULL, close_paren);

        // Set defaults to query plan.
        op->type = PRINT;
        op->tables = NULL;
        op->pos2 = NULL;
        op->value1 = NULL;
        op->value2 = NULL;
        op->c = NULL;

        // Count how many columns we're printing.
        int ncols = 1;
        int i = 0;
        while (args[i] != '\0') {
            if (args[i] == ',') {
                ncols++;
            }
            i++;
        }

        // Extract the first column!
        status ret;
        op->columns = calloc(ncols, sizeof(struct column*));
        op->pos1 = calloc(1, sizeof(Data));
        op->pos1->i = ncols;  // We override this to pass along the information  about ncols
        char* col_name = strtok(args, comma);
        column* col = get_resource(col_name);
        if (!col){
            col = get_var(col_name);
            if (!col) {
                log_err("Variable %s not defined. %s: error at line %d\n",
                    col_name, __func__, __LINE__);
                ret.code = ERROR;
                ret.error_message = "Undefined variable";
                free(str_cpy);
                free(op->columns);
                return ret;
            }
        }
        op->columns[0] = col;
        size_t col_count = col->count;

        // Grab the remaining columns!
        for (int i = 1; i < ncols; i++) {
            col_name = strtok(NULL, comma);
            col = get_resource(col_name);
            if (!col) {
                log_err("Variable %s not defined. %s: error at line %d\n",
                    col_name, __func__, __LINE__);
                ret.code = ERROR;
                ret.error_message = "Undefined variable";
                free(str_cpy);
                free(op->columns);
                return ret;
            }
            if (col->count != col_count) {
                log_err("Cannot reconstruct tuple with variable sized columns.\n");
                ret.code = ERROR;
                ret.error_message = "Undefined variable";
                free(str_cpy);
                free(op->columns);
                return ret;
            }
            op->columns[i] = col;
        }

        // Eveyrthing went well!
        free(str_cpy);
        ret.code = OK;

        return ret;
    }
    else if (d->g == CREATE_INDEX) {
        // Mark changes to database as an index modifies the underlying data
        changed = 1;
        status ret;

        // Crete a working copy
        char* str_cpy = malloc(strlen(str) + 1);
        strncpy(str_cpy, str, strlen(str) + 1);

        // This gives us everything inside the (idx,<col_name>,<type>)
        strtok(str_cpy, open_paren);
        char* args = strtok(NULL, close_paren);

        // This gives us "idx"
        strtok(args, comma);

        // Try to grab the column and type
        char* col_name = strtok(NULL, comma);
        char* type = strtok(NULL, comma);
        column* col = get_resource(col_name);
        if (!col) {
            log_err("No column found. %s: error at line: %d\n", __func__, __LINE__);
            ret.code = ERROR;
            ret.error_message = "Column does not exist.\n";
            free(str_cpy);
            return ret;
        }

        // Access the table name
        char* tbl_name = strtok(col_name, ".");
        char* period = strtok(NULL, ".");
        *(period - 1) = '.';

        // Try to grab the table
        table* tbl = get_resource(tbl_name);
        if (!tbl) {
            log_err("No table found. %s: error at line: %d\n", __func__, __LINE__);
            ret.code = ERROR;
            ret.error_message = "Table does not exist.\n";
            free(str_cpy);
            return ret;
        }

        // TODO(luisperez): Special case when we create index on a leading column;
        // Essentially, we just want to make sure that the column we're working
        // on isn't the leading column for the clustered index.
        if (!tbl->cluster_column || tbl->cluster_column != col) {

            // Let's deal first with creating a secondary sorted index
            if (strcmp(type, "sorted") == 0) {
                status s = create_secondary_index(col, SORTED);
                if (s.code != OK) {
                    log_err("Failed at creating secondary sorted index");
                    free(str_cpy);
                    return s;
                }
            }

            // Let's deal next with creating secondary btree index
            else if (strcmp(type, "btree") == 0) {
                status s = create_secondary_index(col, B_PLUS_TREE);
                if (s.code != OK) {
                    log_err("Failed at creating secondary index (btree)");
                    free(str_cpy);
                    return s;
                }
            }

            // We do not yet support other types! TODO
            else {
                log_err("Unsupported secondary index type %s. %s: line %d.\n",
                    type, __func__, __LINE__);
                ret.error_message = "Unsupported index type";
                ret.code = ERROR;
                free(str_cpy);
                return ret;
            }
        }
        else {
            // TODO (what if we want to re-cluster the leading column)
        }

        ret.code = OK;
        free(str_cpy);
        return ret;

    }
    else if (d->g == LOADCOMMAND) {
        // Need to stream data from the client to the server and insert it!
        status ret;
        op->type = LOADFILE;

        ret.code = OK;
        return ret;
    }
    else if (d->g == SHUTDOWNCOMMAND) {
        status ret;

        if (changed) {
            // Make our data directory if it doesn't already exist.
            struct stat st = {0};
            if (stat(DATA_FOLDER, &st) == -1) {
                mkdir(DATA_FOLDER, 0700);
            }

            // Our metadata file needs to be overwritten
            char filename[DEFAULT_ARRAY_SIZE];
            sprintf(filename, "%s/%s.meta", DATA_FOLDER, SYSTEM_META_FILE);
            FILE* mfile = fopen(filename, "w");
            if (!mfile) {
                log_err("Could not create metadata file %s for server.\n", filename);
                ret.code = ERROR;
                ret.error_message = "File creation failed.\n";
                return ret;
            }

            // Write out the number of databases
            fprintf(mfile, "%zu\n", databases.count);

            for (size_t i = 0; i < databases.count; i++) {
                db* db = (struct db*)(databases.data[i]);
                fprintf(mfile, " %s %zu\n", db->name, db->table_count);
                status s = sync_db(db);
                if (s.code != OK) {
                    log_err("Unable to store database %s", db->name);
                    fclose(mfile);
                    return s;
                }

                // Free some data
                free(db->name);
                free(db->tables);
                free(db);
            }
            fclose(mfile);
        }

        // Successfully persisted the data.
        op->type = SHUTDOWN;
        ret.code = OK;
        return ret;

    }

    // Should have been caught earlier...
    status fail;
    fail.code = ERROR;
    fail.error_message = "No matching operation!\n";
    return fail;
}
