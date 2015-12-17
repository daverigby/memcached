/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2014 Couchbase, Inc
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

/* mcctl - Utility program to perform IOCTL-style operations on a memcached
 *         process.
 */

#include "config.h"

#include <memcached/protocol_binary.h>
#include <memcached/openssl.h>
#include <platform/platform.h>

#include <getopt.h>
#include <stdlib.h>
#include <stdio.h>
#include <strings.h>

#include <memcached/util.h>
#include <utilities/protocol2text.h>
#include "programs/utilities.h"

struct statistic {
    char *key;
    char *value;
};

/**
 * Allocate a chunk of memory (and add a room for a termination byte)
 */
static char *allocate(size_t size)
{
    if (size == 0) {
        return NULL;
    } else {
        char *ret = malloc(size + 1);
        if (ret == NULL) {
            fprintf(stderr, "Failed to allocate %lu bytes of memory\n",
                    (unsigned long)size);
            exit(EXIT_FAILURE);
        }
        ret[size] = '\0';
        return ret;
    }
}

/**
 * Receive the response packet from a stats call and split it up
 * into the key/value pair.
 *
 * @param bio the connection to read the packet from
 * @param st where to stash the result
 */
static void receive_stat_response(BIO *bio, struct statistic *st) {
    protocol_binary_response_no_extras response;
    ensure_recv(bio, &response, sizeof(response.bytes));

    st->key = st->value = NULL;
    uint16_t keylen = ntohs(response.message.header.response.keylen);
    uint32_t vallen = ntohl(response.message.header.response.bodylen) - keylen;

    st->key = allocate(keylen);
    ensure_recv(bio, st->key, keylen);
    st->value = allocate(vallen);
    ensure_recv(bio, st->value, vallen);

    protocol_binary_response_status status;
    status = ntohs(response.message.header.response.status);

    if (status != PROTOCOL_BINARY_RESPONSE_SUCCESS) {
        fprintf(stderr, "Error from server requesting stats: %s\n",
                memcached_status_2_text(status));
        /* Just terminate.. we might have multiple packets in the
         * pipeline and this makes the error handling easier and
         * safer
         */
        exit(EXIT_FAILURE);
    }
}

/**
 * Get the verbosity level on the server.
 *
 * There isn't a single command to retrieve the current verbosity level,
 * but it is available through the settings stats...
 *
 * @param bio connection to the server.
 */
static int get_verbosity(BIO *bio)
{
    const char *settings = "settings";
    const uint16_t settingslen = (uint16_t)strlen(settings);

    protocol_binary_request_stats request = {
        .message.header.request.magic = PROTOCOL_BINARY_REQ,
        .message.header.request.opcode = PROTOCOL_BINARY_CMD_STAT,
        .message.header.request.keylen = htons(settingslen),
        .message.header.request.bodylen = htonl(settingslen)
    };

    ensure_send(bio, &request, sizeof(request));
    ensure_send(bio, settings, settingslen);

    // loop and receive the result and print the verbosity when we get it
    struct statistic st;
    do {
        receive_stat_response(bio, &st);
        if (st.key != NULL && strcasecmp(st.key, "verbosity") == 0) {
            uint32_t level;
            if (safe_strtoul(st.value, &level)) {
                const char *levels[] = { "warning",
                                         "info",
                                         "debug",
                                         "detail",
                                         "unknown" };
                const char *ptr = levels[4];

                if (level < 4) {
                    ptr = levels[level];
                }
                fprintf(stderr, "%s\n", ptr);
            } else {
                fprintf(stderr, "%s\n", st.value);
            }
        }
        free(st.key);
        free(st.value);
    } while (st.key != NULL);

    return EXIT_SUCCESS;
}

/**
 * Sets the verbosity level on the server
 *
 * @param bio connection to the server.
 * @param value value to set the property to.
 */
static int set_verbosity(BIO *bio, const char* value)
{
    protocol_binary_request_verbosity request = {
        .message.header.request.magic = PROTOCOL_BINARY_REQ,
        .message.header.request.opcode = PROTOCOL_BINARY_CMD_VERBOSITY,
        .message.header.request.extlen = 4,
        .message.header.request.bodylen = htonl(4)
    };
    uint32_t level;

    if (!safe_strtoul(value, &level)) {
        // Try to map it...
        if (strcasecmp("warning", value) == 0) {
            level = 0;
        } else if (strcasecmp("info", value) == 0) {
            level = 1;
        } else if (strcasecmp("debug", value) == 0) {
            level = 2;
        } else if (strcasecmp("detail", value) == 0) {
            level = 3;
        } else {
            fprintf(stderr, "Unknown verbosity level \"%s\". "
                    "Use warning/info/debug/detail\n",
                    value);
            return EXIT_FAILURE;
        }
    }

    // Fix byte order
    request.message.body.level = htonl(level);
    ensure_send(bio, &request, sizeof(request.bytes));

    // Read the response
    protocol_binary_response_no_extras response;
    ensure_recv(bio, &response, sizeof(response.bytes));

    if (response.message.header.response.bodylen != 0) {
        char *buffer = NULL;
        uint32_t valuelen = ntohl(response.message.header.response.bodylen);
        buffer = malloc(valuelen);
        if (buffer == NULL) {
            fprintf(stderr, "Failed to allocate memory for set response\n");
            exit(EXIT_FAILURE);
        }
        ensure_recv(bio, buffer, valuelen);
        free(buffer);
    }

    protocol_binary_response_status status;
    status = htons(response.message.header.response.status);
    if (status == PROTOCOL_BINARY_RESPONSE_SUCCESS) {
        return EXIT_SUCCESS;
    }

    fprintf(stderr, "Error: %s\n", memcached_status_2_text(status));
    return EXIT_FAILURE;
}

/**
 * Sets a property (to the specified value).
 * @param bio connection to the server.
 * @param property the name of the property to set.
 * @param value value to set the property to (NULL == no value).
 */
static int ioctl_set(BIO *bio, const char *property, const char* value)
{
    char *buffer = NULL;
    uint16_t keylen = 0;
    uint32_t valuelen = 0;
    int result;
    protocol_binary_request_ioctl_set request;
    protocol_binary_response_no_extras response;
    protocol_binary_response_status status;

    if (property != NULL) {
        keylen = (uint16_t)strlen(property);
    }
    if (value != NULL) {
        valuelen = (uint32_t)strlen(value);
    }

    memset(&request, 0, sizeof(request));
    request.message.header.request.magic = PROTOCOL_BINARY_REQ;
    request.message.header.request.opcode = PROTOCOL_BINARY_CMD_IOCTL_SET;
    request.message.header.request.keylen = htons(keylen);
    request.message.header.request.bodylen = htonl(keylen + valuelen);

    ensure_send(bio, &request, sizeof(request));
    if (keylen > 0) {
        ensure_send(bio, property, keylen);
    }
    if (valuelen > 0) {
        ensure_send(bio, value, valuelen);
    }

    ensure_recv(bio, &response, sizeof(response.bytes));
    if (response.message.header.response.bodylen != 0) {
        valuelen = ntohl(response.message.header.response.bodylen);
        buffer = malloc(valuelen);
        if (buffer == NULL) {
            fprintf(stderr, "Failed to allocate memory for set response\n");
            exit(EXIT_FAILURE);
        }
        ensure_recv(bio, buffer, valuelen);
    }
    status = htons(response.message.header.response.status);
    if (status == PROTOCOL_BINARY_RESPONSE_SUCCESS) {
        result = 0;
    } else {
        fprintf(stderr, "Error from server: %s\n",
                memcached_status_2_text(status));
        result = 1;
    }

    if (buffer != NULL) {
        fwrite(buffer, valuelen, 1, stdout);
        fputs("\n", stdout);
        fflush(stdout);
        free(buffer);
    }
    return result;
}

/**
 * Gets a property
 * @param bio connection to the server.
 * @param property the name of the property to get.
 */
static int ioctl_get(BIO *bio, const char *property)
{
    char *buffer = NULL;
    uint16_t keylen = 0;
    uint32_t valuelen = 0;
    int result;
    protocol_binary_request_ioctl_get request;
    protocol_binary_response_no_extras response;
    protocol_binary_response_status status;

    if (property == NULL) {
        return EXIT_FAILURE;
    }
    keylen = (uint16_t)strlen(property);

    memset(&request, 0, sizeof(request));
    request.message.header.request.magic = PROTOCOL_BINARY_REQ;
    request.message.header.request.opcode = PROTOCOL_BINARY_CMD_IOCTL_GET;
    request.message.header.request.keylen = htons(keylen);
    request.message.header.request.bodylen = htonl(keylen);

    ensure_send(bio, &request, sizeof(request));
    if (keylen > 0) {
        ensure_send(bio, property, keylen);
    }

    ensure_recv(bio, &response, sizeof(response.bytes));
    if (response.message.header.response.bodylen != 0) {
        valuelen = ntohl(response.message.header.response.bodylen);
        buffer = malloc(valuelen);
        if (buffer == NULL) {
            fprintf(stderr, "Failed to allocate memory for get response\n");
            exit(EXIT_FAILURE);
        }
        ensure_recv(bio, buffer, valuelen);
    }
    status = htons(response.message.header.response.status);
    if (status == PROTOCOL_BINARY_RESPONSE_SUCCESS) {
        result = 0;
    } else {
        fprintf(stderr, "Error from server for get request: %s\n",
                memcached_status_2_text(status));
        result = 1;
    }

    if (buffer != NULL) {
        fwrite(buffer, valuelen, 1, stdout);
        fputs("\n", stdout);
        fflush(stdout);
        free(buffer);
    }
    return result;
}

static int usage() {
    fprintf(stderr,
            "Usage: mcctl [-h host[:port]] [-p port] [-u user] [-P pass] [-s] <get|set> property [value]\n"
            "\n"
            "    get <property>           Returns the value of the given property.\n"
            "    set <property> [value]   Sets `property` to the given value.\n");
    return EXIT_FAILURE;
}

int main(int argc, char** argv) {
    int cmd;
    const char *port = "11210";
    const char *host = "localhost";
    const char *user = NULL;
    const char *pass = NULL;
    int secure = 0;
    char *ptr;
    SSL_CTX* ctx;
    BIO* bio;
    int result = EXIT_FAILURE;

    /* Initialize the socket subsystem */
    cb_initialize_sockets();

    while ((cmd = getopt(argc, argv, "h:p:u:P:s")) != EOF) {
        switch (cmd) {
        case 'h' :
            host = optarg;
            ptr = strchr(optarg, ':');
            if (ptr != NULL) {
                *ptr = '\0';
                port = ptr + 1;
            }
            break;
        case 'p':
            port = optarg;
            break;
        case 'u' :
            user = optarg;
            break;
        case 'P':
            pass = optarg;
            break;
        case 's':
            secure = 1;
            break;
        default:
            return usage();
        }
    }

    /* Need at least two more arguments: get/set and a property name. */
    if (optind + 1 >= argc) {
        return usage();
    } else {
        if (strcmp(argv[optind], "get") == 0 || strcmp(argv[optind], "set") == 0) {
            const char* property = argv[optind+1];
            if (create_ssl_connection(&ctx, &bio, host, port, user,
                                      pass, secure) != 0) {
                return 1;
            }

            if (strcmp(argv[optind], "get") == 0) {
                if (strcmp(property, "verbosity") == 0) {
                    result = get_verbosity(bio);
                } else {
                    result = ioctl_get(bio, property);
                }
            } else if (strcmp(argv[optind], "set") == 0) {
                const char* value = (optind + 2 < argc) ? argv[optind+2]
                                                        : NULL;
                if (strcmp(property, "verbosity") == 0) {
                    if (value == NULL) {
                        fprintf(stderr,
                                "Error: 'set verbosity' requires a value argument.");
                        result = usage();
                    } else {
                        result = set_verbosity(bio, value);
                    }
                } else {
                    result = ioctl_set(bio, property, value);
                }
            }

            BIO_free_all(bio);
            if (secure) {
                SSL_CTX_free(ctx);
            }
        } else {
            fprintf(stderr, "Unknown subcommand \"%s\"\n", argv[optind]);
            result = usage();
        }
    }

    return result;
}
