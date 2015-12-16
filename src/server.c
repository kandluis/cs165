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

/**
 * parse_command takes as input the send_message from the client and then
 * parses it into the appropriate query. Stores into send_message the
 * status to send back.
 * Returns a db_operator.
 **/
db_operator* parse_command(message* recv_message, message* send_message) {
    send_message->status = OK_WAIT_FOR_RESPONSE;
    db_operator *dbo = malloc(sizeof(db_operator));
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
        result* r = malloc(sizeof(struct result));
        r->payload = query->pos1;
        r->num_tuples = (query->value1) ? *(query->value1) : (int) query->columns[0]->count;

        status s = col_scan(query->c, query->columns[0], &r);
        if (s.code != OK) {
            log_err("Column scan failed %s. %s: error in line %d\n",
                s.error_message, __func__, __LINE__);
            ret = s.error_message;
        }

        // The results payload is NEW! Now we can store it as an Array.
        column* res = malloc(sizeof(struct column));
        res->data = r->payload;
        res->size = r->num_tuples;
        res->count = r->num_tuples;

        // Add it to the var_map so we can access it later!
        set_var(query->var_name, res);

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
        int64_t ncols = *query->pos1;

        // We allocate space for the result based on upper bound estimate.
        // TODO(luisperez): Dynamically resize to avoid buffer overflow problems!
        char* res = malloc(sizeof(char) * ((10 * ncols) + 1) * rows);
        res[0] = '\0';
        ret = res; // Keep track of the start.
        for(size_t row = 0; row < rows; row++) {
            for (int64_t col = 0; col < ncols - 1; col++) {
                // Generate the string to hold a single digit
                res += sprintf(res, "%ld,", query->columns[col]->data[row]);
            }
            // For the last digit, don't add comma instead add a newline
            res +=  sprintf(res, "%ld\n", query->columns[ncols - 1]->data[row]);
        }

        // NEED TO FREE COLS AND POS1
        free(query->columns);
        free(query->pos1);
    }
    free(query);
    return ret;
}

/**
 * handle_client(client_socket)
 * This is the execution routine after a client has connected.
 * It will continually listen for messages from the client and execute queries.
 **/
void handle_client(int client_socket) {
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
    do {
        length = recv(client_socket, &recv_message, sizeof(message), 0);
        if (length < 0) {
            log_err("Client connection closed!\n");
            exit(1);
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

            // We have to special case LOAD!
            if (query->type == LOADFILE) {
                // Read the first line of the file (foo.t1.a,foo.t1.b) and get columns
                length = recv(client_socket, &recv_message, sizeof(message), 0);
                if (length <= 0) {
                    log_err("Client connection closed!\n");
                    exit(1);
                }

                char buffer[recv_message.length];
                length = recv(client_socket, buffer, recv_message.length, 0);
                buffer[recv_message.length] = '\0';

                // Read the rest of the input line by line until payload TERMINATES.
                // First, let's count how many columns we have
                int64_t ncol = 1;
                int64_t i = 0;
                while (buffer[i] != '\0') {
                    if (buffer[i] == ',') {
                        ncol++;
                    }
                    i++;
                }

                // Now we can parse each col.
                column** cols = malloc(sizeof(struct column*) * ncol);
                cols[0] = get_var(strtok(buffer, ","));
                for (int64_t i = 1; i < ncol; i++) {
                    cols[i] = get_var(strtok(NULL, ","));
                }

                // Now we read line by line and insert into the database
                while(recv(client_socket, &recv_message, sizeof(message), 0) > 0) {
                    recv(client_socket, buffer, recv_message.length, 0);
                    buffer[recv_message.length] = '\0';

                    // Break out of loop if we received the EOF message
                    if (strcmp(buffer, TERMINATE_LOAD) == 0) {
                        log_info("Received termination signal from client. Load completed.");
                        break;
                    }
                    // Insert into respective columns
                    char* str = NULL;
                    for (int64_t i = 0; i < ncol; i++){
                        str = strtok((i == 0) ? buffer : NULL, ",");
                        if (!str) {
                            log_err("Could not parse. Error in load.\n", buffer);
                        }
                        status s = insert(cols[i], atoi(str));
                        if (s.code != OK) {
                            log_err("Could not insert value %s into column during load.", str);
                        }
                    }
                }
                /// Stuff
            }

            // 2. Handle request
            char* result = execute_db_operator(query);
            send_message.length = strlen(result);

            // 3. Send status of the received message (OK, UNKNOWN_QUERY, etc)
            if (send(client_socket, &(send_message), sizeof(struct message), 0) == -1) {
                log_err("Failed to send message.");
                exit(1);
            }

            // 4. Send response of request
            if (send(client_socket, result, send_message.length, 0) == -1) {
                log_err("Failed to send message.");
                exit(1);
            }
        }
    } while (!done);

    log_info("Connection closed at socket %d!\n", client_socket);
    close(client_socket);
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

    // Populate the global dsl commands
    dsl_commands = dsl_commands_init();

    log_info("Waiting for a connection %d ...\n", server_socket);

    struct sockaddr_un remote;
    socklen_t t = sizeof(remote);
    int client_socket = 0;

    if ((client_socket = accept(server_socket, (struct sockaddr *)&remote, &t)) == -1) {
        log_err("L%d: Failed to accept a new connection.\n", __LINE__);
        exit(1);
    }

    handle_client(client_socket);

    return 0;
}

