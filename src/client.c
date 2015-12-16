#define _XOPEN_SOURCE
/**
 * client.c
 *  CS165 Fall 2015
 *
 * This file provides a basic unix socket implementation for a client
 * used in an interactive client-server database.
 * The client receives input from stdin and sends it to the server.
 * No pre-processing is done on the client-side.
 *
 * For more information on unix sockets, refer to:
 * http://beej.us/guide/bgipc/output/html/multipage/unixsock.html
 **/
#include <stdlib.h>
#include <unistd.h>
#include <regex.h>
#include <string.h>
#include <stdio.h>

#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>

#include "common.h"
#include "message.h"
#include "utils.h"
#include "dsl.h"

#define DEFAULT_STDIN_BUFFER_SIZE 1024

const char* load_command = "^load\\(\\\"[a-zA-Z0-9_\\.\\/]+\\\"\\)";
const char* comment = "^--";


/**
 * connect_client()
 *
 * This sets up the connection on the client side using unix sockets.
 * Returns a valid client socket fd on success, else -1 on failure.
 *
 **/
int connect_client() {
    int client_socket;
    size_t len;
    struct sockaddr_un remote;

    log_info("Attempting to connect...\n");

    if ((client_socket = socket(AF_UNIX, SOCK_STREAM, 0)) == -1) {
        log_err("L%d: Failed to create socket.\n", __LINE__);
        return -1;
    }

    remote.sun_family = AF_UNIX;
    strncpy(remote.sun_path, SOCK_PATH, strlen(SOCK_PATH) + 1);
    len = strlen(remote.sun_path) + sizeof(remote.sun_family) + 1;
    if (connect(client_socket, (struct sockaddr *)&remote, len) == -1) {
        perror("client connect failed: ");
        return -1;
    }

    log_info("Client connected at socket: %d.\n", client_socket);
    return client_socket;
}

void send_to_server(int socket, message* msg) {
    // Send the message_header, which tells server payload size
    if (send(socket, msg, sizeof(message), 0) == -1) {
        log_err("Failed to send message header.");
        exit(1);
    }
    if (send(socket, msg->payload, msg->length, 0) == -1) {
        log_err("Failed to send query payload.");
        exit(1);
    }
}

void process_load_command(char* cmd, int client_socket, message* send_message) {
    // This gives use everything inside ("load.txt")
    strtok(cmd, "(");
    char* filename = strtok(NULL, ")");
    filename += 1;
    filename = strtok(filename, "\"");

    // Open the file
    FILE* fp = fopen(filename, "r");
    char* read = NULL;
    if (fp) {
        while ((read = fgets(send_message->payload, DEFAULT_STDIN_BUFFER_SIZE, fp))) {
            send_message->length = strlen(send_message->payload);
            send_message->payload[send_message->length - 1] = '\0';
            send_to_server(client_socket, send_message);
        }
        fclose(fp);
    }

    // Send a final message (EOF) that we've reached end of file!
    // This trigger the server to respond!
    send_message->length = strlen(TERMINATE_LOAD);
    strcpy(send_message->payload, TERMINATE_LOAD);
    send_to_server(client_socket, send_message);
}

int main(void)
{
    int client_socket = connect_client();
    if (client_socket < 0) {
        exit(1);
    }

    message send_message;
    message recv_message;

    // Always output an interactive marker at the start of each command if the
    // input is from stdin. Do not output if piped in from file or from other fd
    char* prefix = "";
    if (isatty(fileno(stdin))) {
        prefix = "db_client > ";
    }

    char *output_str = NULL;

    // Continuously loop and wait for input. At each iteration:
    // 1. output interactive marker
    // 2. read from stdin until eof.
    char read_buffer[DEFAULT_STDIN_BUFFER_SIZE];
    send_message.payload = read_buffer;
    int len = 0;

    // To check the message is a load_command. We need to stream a file in that case.
    regex_t regex1, regex2;
    int ret;
    int n_matches = 1;
    regmatch_t m;
    if (regcomp(&regex1, load_command, REG_EXTENDED) != 0 ||
        regcomp(&regex2, comment, REG_EXTENDED) != 0 ) {
        log_err("Could not compile regex\n");
        exit(1);
    }

    while (printf("%s", prefix), output_str = fgets(read_buffer,
           DEFAULT_STDIN_BUFFER_SIZE, stdin), !feof(stdin)) {
        if (output_str == NULL) {
            log_err("fgets failed.\n");
            break;
        }

        // Only process input that is greater than 1 character.
        // Ignore things such as new lines.
        // Otherwise, convert to message and send the message and the
        // payload directly to the server.
        send_message.length = strlen(read_buffer);

        // Ignore comments.
        ret = regexec(&regex2, read_buffer, n_matches, &m, 0);
        if (send_message.length > 1 && ret != 0) {
            send_to_server(client_socket, &send_message);

            // Check to see if the message is a load message. In this case,
            // don't wait for response, send the file!
            ret = regexec(&regex1, read_buffer, n_matches, &m, 0);
            // If we have  match, we want to open the file and stream it to the server.
            if (ret == 0) {
                char* cmd = malloc(strlen(read_buffer) + 1);
                strncpy(cmd, read_buffer, strlen(read_buffer) + 1);

                process_load_command(cmd, client_socket, &send_message);

                free(cmd);
            }

            // Always wait for server response (even if it is just an OK message)
            if ((len = recv(client_socket, &(recv_message), sizeof(message), 0)) > 0) {
                if (recv_message.status == OK_WAIT_FOR_RESPONSE &&
                    (int) recv_message.length > 0) {
                    // Calculate number of bytes in response package
                    int num_bytes = (int) recv_message.length;
                    char payload[num_bytes + 1];

                    // Receive the payload and print it out
                    if ((len = recv(client_socket, payload, num_bytes, 0)) > 0) {
                        payload[num_bytes] = '\0';
                        printf("%s\n", payload);
                    }
                }
            }
            else {
                if (len < 0) {
                    log_err("Failed to receive message.");
                }
                else {
                    log_info("Server closed connection\n");
                }
                exit(1);
            }
        }
    }
    close(client_socket);
    return 0;
}
