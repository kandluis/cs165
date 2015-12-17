/** server.c
 * CS165 Fall 2015
 *
 * This file provides a basic unix socket implementation for a server
 * used in an interactive client-server database.
 * The server should allow for multiple concurrent connections from clients.
 * Each client should be able to send messages containing queries to the
 * server.  When the server receives a message, it must:
 * 1. Respond with a status based on the query (OK, UNKNOWN_QUERY, etc.)
 * 2. Process any appropriate queries, if applicable.
 * 3. Return the query response to the client.
 *
 * For more information on unix sockets, refer to:
 * http://beej.us/guide/bgipc/output/html/multipage/unixsock.html
 **/
#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/socket.h>
#include <unistd.h>
#include <string.h>

#include "common.h"
#include "cs165_api.h"
#include "include/var_store.h"
#include "message.h"
#include "parser.h"
#include "utils.h"

#define DEFAULT_QUERY_BUFFER_SIZE 1024

// Here, we allow for a global of DSL COMMANDS to be shared in the program
dsl** dsl_commands;


// Here, we allow for a global databases to be shared in the program
Storage databases;

// Here we allow for tracking changes.
int changed;

/**
 * parse_command takes as input the send_message from the client and then
 * parses it into the appropriate query. Stores into send_message the
 * status to send back.
 * Returns a db_operator.
 **/
db_operator* parse_command(message* recv_message, message* send_message) {
    send_message->status = OK_WAIT_FOR_RESPONSE;
    db_operator *dbo = calloc(1, sizeof(db_operator));
    // Here you parse the message and fill in the proper db_operator fields for
    // now we just log the payload
    cs165_log(stdout, recv_message->payload);

    // Here, we give you a default parser, you are welcome to replace it with anything you want
    status parse_status = parse_command_string(recv_message->payload, dsl_commands, dbo);
    if (parse_status.code != OK) {
        log_err("Error parsing command! %s", parse_status.error_message);
        return NULL;
    }

    return dbo;
}

/** execute_db_operator takes as input the db_operator and executes the query.
 * It should return the result (currently as a char*, although I'm not clear
 * on what the return type should be, maybe a result struct, and then have
 * a serialization into a string message).
 **/
char* execute_db_operator(db_operator* query) {
    if (!query){
        return "Failed!";
    }

    // Let's see what the query wants us to do!
    char* ret = "";
    if (query->type == INSERT) {
        // Extract the table.
        table* tbl = query->tables[0];

        // Iterate over the columns and insert the values
        for(size_t i = 0; i < tbl->col_count; i++) {
            status s = insert(query->columns[i], query->value1[i]);
            if (s.code == ERROR) {
                ret = s.error_message;
                break;
            }
        }

        // We still need to free the stuff allocated by parse.
        free(query->tables);
        free(query->value1);
    }
    else if (query->type == SELECT) {
        result* r = calloc(1, sizeof(struct result));
        r->payload = query->pos1;
        r->type = query->pos1type;
        r->num_tuples = (query->value1) ? (size_t) query->value1->li : query->columns[0]->count;

        status s = col_scan(query->c, query->columns[0], &r);
        if (s.code != OK) {
            log_err("Column scan failed %s. %s: error in line %d\n",
                s.error_message, __func__, __LINE__);
            ret = s.error_message;
        }

        // The results payload is NEW! Now we can store it as an Array.
        column* res = calloc(1, sizeof(struct column));
        res->data = r->payload;
        res->size = r->num_tuples;
        res->count = r->num_tuples;
        res->type = r->type;

        // Add it to the var_map so we can access it later!
        set_var(query->var_name, res);
        free(query->var_name);

        // Free everything we've malloced.
        free(query->columns);
        comparator* cur = query->c;
        comparator* tmp;
        while (cur) {
            tmp = cur;
            cur = cur->next_comparator;
            free(tmp);
        }
        free(r);
    }
    else if (query->type == PRINT) {

        // Need to construct a string with the result
        size_t rows = query->columns[0]->count;
        int ncols = query->pos1->i;

        // We allocate space for the result based on upper bound estimate.
        // TODO(luisperez): Dynamically resize to avoid buffer overflow problems!
        char* res = calloc(1 + ((MAX_STRING_LENGTH * ncols) + 1) * rows, sizeof(char));
        res[0] = '\0';
        ret = res; // Keep track of the start.

        column* column;
        for(size_t row = 0; row < rows; row++) {
            for (int col = 0; col < ncols - 1; col++) {
                // Grab the column
                column = query->columns[col];
                // Grab the value
                if (column->type == LONGINT) {
                    res += sprintf(res, "%ld,", column->data[row].li);
                }
                else if (column->type == DOUBLE) {
                    res += sprintf(res, "%.12f,", column->data[row].f);
                }
                else if (column->type == INT) {
                    res += sprintf(res, "%d,", column->data[row].i);
                }
                else {
                    log_err("Incompatible type!");
                    continue;
                }
                // Generate the string to hold a single digit

            }
            // For the last digit, don't add comma
            column = query->columns[ncols - 1];
            // Grab the value
            if (column->type == LONGINT) {
                res += sprintf(res, "%ld\n", column->data[row].li);
            }
            else if (column->type == DOUBLE) {
                res += sprintf(res, "%.12f\n", column->data[row].f);
            }
            else if (column->type == INT) {
                res += sprintf(res, "%d\n", column->data[row].i);
            }
            else {
                log_err("Incompatible type!");
                continue;
            }
        }

        // Drop the final newline.
        if (ret != res) {
            *(res - 1) = '\0';
        }

        // NEED TO FREE COLS AND POS1
        free(query->columns);
        free(query->pos1);
    }
    else if (query->type == SHUTDOWN) {
        // We send a special message to the client to shutdown gracefully!
        ret = SHUTDOWN_MESSAGE;
    }
    free(query);
    return ret;
}

// Load command executed, so synchronize with client and read incoming messages.
void load_data(int client_socket, message* recv_message){
    // Read the first line of the file (foo.t1.a,foo.t1.b) and get columns
    int length = recv(client_socket, recv_message, sizeof(struct message), 0);
    if (length <= 0) {
        log_err("Client connection closed!\n");
    }

    char buffer[recv_message->length];
    length = recv(client_socket, buffer, recv_message->length, 0);
    buffer[recv_message->length] = '\0';

    // Read the rest of the input line by line until payload TERMINATES.
    // First, let's count how many columns we have
    int ncol = 1;
    int i = 0;
    while (buffer[i] != '\0') {
        if (buffer[i] == ',') {
            ncol++;
        }
        i++;
    }

    // Now we can parse each col.
    column** cols = calloc(sizeof(struct column*), ncol);
    cols[0] = get_resource(strtok(buffer, ","));
    if (!cols[0]) {
        log_err("Resource not found. Column name invalid: %s.", buffer);
    }
    for (int i = 1; i < ncol; i++) {
        cols[i] = get_resource(strtok(NULL, ","));
        if (!cols[i]) {
            log_err("Resource not found.");
        }
    }

    // Now we read line by line and insert into the database
    while(recv(client_socket, recv_message, sizeof(message), 0) > 0) {
        recv(client_socket, buffer, recv_message->length, 0);
        buffer[recv_message->length] = '\0';

        // Break out of loop if we received the EOF message
        if (strcmp(buffer, TERMINATE_LOAD) == 0) {
            log_info("Received termination signal from client. Load completed.");
            break;
        }
        // Insert into respective columns
        char* str = NULL;
        for (int i = 0; i < ncol; i++){
            str = strtok((i == 0) ? buffer : NULL, ",");
            if (!str) {
                log_err("Could not parse. Error in load.\n", buffer);
            }
            Data datum;
            datum.i = atoi(str);
            status s = insert(cols[i], datum);
            if (s.code != OK) {
                log_err("Could not insert value %s into column during load.", str);
            }
        }
    }

    // Let's extract the table name.
    char* str_cpy = malloc(strlen(cols[0]->name) + 1);
    strncpy(str_cpy, cols[0]->name, strlen(cols[0]->name) + 1);

    // Now we split on periods to extract table name db.tbl.col
    strtok(str_cpy, ".");
    char* tbl_name = strtok(NULL, ".");

    // TOOD(luisperez): Do we really want to crash here?
    if (strcmp(strtok(NULL, "."), cols[0]->name) != 0) {
        log_err("%s does not match %s!\n", cols[0]->name);
        free(cols);
        free(str_cpy);
        return;
    }

    // Get the table from resource pool
    table* tbl = get_resource(tbl_name);
    if (!tbl) {
        log_err("Table %s not found. Could not cluster!\n", tbl_name);
        free(cols);
        free(str_cpy);
        return;
    }

    // Let's cluster the table!
    (void) cluster_table(tbl);

    free(cols);
    free(str_cpy);
}

/**
 * handle_client(client_socket)
 * This is the execution routine after a client has connected.
 * It will continually listen for messages from the client and execute queries.
 **/
 // Returns -1 on client error.
 // Returns 0 on successful handling
 // Returns 1 on shutdown.
int handle_client(int client_socket) {
    int done = 0;
    int length = 0;

    log_info("Connected to socket: %d.\n", client_socket);

    // Create two messages, one from which to read and one from which to receive
    message send_message;
    send_message.length = 0;
    send_message.payload = NULL;
    message recv_message;
    recv_message.length = 0;
    recv_message.payload = NULL;

    // Continually receive messages from client and execute queries.
    // 1. Parse the command
    // 2. Handle request if appropriate
    // 3. Send status of the received message (OK, UNKNOWN_QUERY, etc)
    // 4. Send response of request.
    int ret = 0;
    do {
        length = recv(client_socket, &recv_message, sizeof(message), 0);
        if (length < 0) {
            log_err("Client connection closed!\n");
            ret = -1;
            break;
        } else if (length == 0) {
            done = 1;
        }

        if (!done) {
            char recv_buffer[recv_message.length];
            length = recv(client_socket, recv_buffer, recv_message.length,0);
            recv_message.payload = recv_buffer;
            recv_message.payload[recv_message.length] = '\0';

            // 1. Parse command
            db_operator* query = parse_command(&recv_message, &send_message);

            // We have to special case LOAD! We just do a lot of logging on errors!
            if (query && query->type == LOADFILE) {
                changed = 1;
                load_data(client_socket, &recv_message);
            }

            // 2. Handle request
            char* result = execute_db_operator(query);
            send_message.length = strlen(result);

            // 3. Send status of the received message (OK, UNKNOWN_QUERY, etc)
            if (send(client_socket, &(send_message), sizeof(struct message), 0) == -1) {
                log_err("Failed to send message.");
                ret = -1;
                break;
            }

            // 4. Send response of request
            if (send(client_socket, result, send_message.length, 0) == -1) {
                log_err("Failed to send message.");
                ret = -1;
                break;
            }

            // If we sent a shutdown to the client, the server has persisted
            // and needs to exit.
            if (strcmp(result, SHUTDOWN_MESSAGE) == 0) {
                log_info("Client requested server shutdown. Closing socket %d!\n", client_socket);
                ret = 1;
                break;
            }

            // Free the message
            if (strlen(result) > 1) {
                free(result);
            }

        }
    } while (!done);

    log_info("Connection closed at socket %d!\n", client_socket);
    close(client_socket);

    // Successfully closed connection to client with no errors and no requests
    // to shutdown.
    return ret;
}

/**
 * setup_server()
 *
 * This sets up the connection on the server side using unix sockets.
 * Returns a valid server socket fd on success, else -1 on failure.
 **/
int setup_server() {
    int server_socket;
    size_t len;
    struct sockaddr_un local;

    log_info("Attempting to setup server...\n");

    if ((server_socket = socket(AF_UNIX, SOCK_STREAM, 0)) == -1) {
        log_err("L%d: Failed to create socket.\n", __LINE__);
        return -1;
    }

    local.sun_family = AF_UNIX;
    strncpy(local.sun_path, SOCK_PATH, strlen(SOCK_PATH) + 1);
    unlink(local.sun_path);

    /*
    int on = 1;
    if (setsockopt(server_socket, SOL_SOCKET, SO_REUSEADDR, (char *)&on, sizeof(on)) < 0)
    {
        log_err("L%d: Failed to set socket as reusable.\n", __LINE__);
        return -1;
    }
    */

    len = strlen(local.sun_path) + sizeof(local.sun_family) + 1;
    if (bind(server_socket, (struct sockaddr *)&local, len) == -1) {
        log_err("L%d: Socket failed to bind.\n", __LINE__);
        return -1;
    }

    if (listen(server_socket, 5) == -1) {
        log_err("L%d: Failed to listen on socket.\n", __LINE__);
        return -1;
    }

    return server_socket;
}


// Function to load the metadata file from the server.
// Returns True/False depending on whether we successfully loaded the persisted data.
void load_server(void) {
    // Open the metadata system file
    char tmp[DEFAULT_QUERY_BUFFER_SIZE];
    sprintf(tmp, "%s/%s.meta", DATA_FOLDER, SYSTEM_META_FILE);
    FILE* mfile = fopen(tmp, "r");
    if (!mfile) {
        log_info("No persisted data found.\n");
        return;
    }

    // The first line tells us the number of databases
    size_t ndbs = 0;
    if (fscanf(mfile, "%zu\n", &ndbs) != 1) {
        log_err("Unable to read metadata file.");
        fclose(mfile);
        return;
    }

    // Allocate space for the databases
    databases.count = ndbs;
    databases.size = ndbs;
    databases.data = calloc(sizeof(struct db*), ndbs);
    if (!databases.data) {
        log_err("Unable to allocate space for persisted data!");
        fclose(mfile);
        return;
    }

    // Read in each database!
    for (size_t i = 0; i < ndbs; i++) {
        db* db =  calloc(sizeof(struct db), 1);
        db->tables_available = 0;

        // Store the name and table count
        if (fscanf(mfile, "%s %zu\n", tmp, &db->table_count) != 2) {
            log_err("Unable to read database metadata.");
            return;
        }
        // Copy it to the database.
        db->name = copystr(tmp);

        sprintf(tmp, "%s/%s.meta", DATA_FOLDER, db->name);
        status s = open_db(tmp, &db, LOAD);
        if (s.code != OK) {
            log_err("Unable to load db: %s\n", db->name);
            return;
        }

        // Store the loaded db in the global pool
        databases.data[i] = db;

        // Store it in our variable storage
        set_resource(db->name, db);
    }

    log_info("Successfully loaded persisted data!");
}

// Currently this main will setup the socket and accept a single client.
// After handling the client, it will exit.
// You will need to extend this to handle multiple concurrent clients
// and remain running until it receives a shut-down command.
int main(void)
{
    int server_socket = setup_server();
    if (server_socket < 0) {
        exit(1);
    }

    // No modification to the data have been made.
    changed = 0;

    // Load persisted data!
    load_server();

    // Populate the global dsl commands
    dsl_commands = dsl_commands_init();

    log_info("Waiting for a connection %d ...\n", server_socket);

    struct sockaddr_un remote;
    socklen_t t = sizeof(remote);
    int client_socket = 0;

    // Continually accept clients.
    while (1) {
        if ((client_socket = accept(server_socket, (struct sockaddr *)&remote, &t)) == -1) {
            log_err("L%d: Failed to accept a new connection.\n", __LINE__);
            exit(1);
        }

        // Shutdown of server requested.
        int ret = handle_client(client_socket);
        if (ret == 1) {
            log_info("Shutdown!");
            break;
        }
        else if (ret < 0) {
            log_info("Client error.");
            clear_vars();
        }
        else {
            log_info("Client success.");
            clear_vars();
        }
    }

    return 0;
}

