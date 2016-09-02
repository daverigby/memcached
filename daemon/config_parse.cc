/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"
#include "memcached.h"

#include "config_parse.h"

#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <strings.h>
#include <sys/stat.h>
#include <ctype.h>
#include <stdarg.h>
#include <limits>
#include <sstream>

#include "breakpad.h"
#include "cmdline.h"
#include "config_util.h"
#include "config_parse.h"
#include "connections.h"
#include "runtime.h"
#include <platform/cb_malloc.h>
#include "settings.h"
#include "ssl_utils.h"

static void do_asprintf(char **strp, const char *fmt, ...)
{
    va_list ap;

    va_start(ap, fmt);
    if (vasprintf(strp, fmt, ap) < 0){
        if (settings.extensions.logger) {
            LOG_WARNING(NULL, "vasprintf failed: %s", strerror(errno));
        } else {
            fprintf(stderr, "vasprintf failed: %s", strerror(errno));
        }
    }
    va_end(ap);
}

#ifdef WIN32
static int isDrive(const char *file) {
    if ((isupper(file[0]) || islower(file[0])) && file[1] == ':') {
        return 1;
    }
    return 0;
}
#endif

static bool get_absolute_file(const char *file, const char **value,
                              char **error_msg) {
    char buffer[1024];
    size_t len;
#ifdef WIN32
    const char sep = '\\';
#else
    const char sep = '/';
#endif

    if (file[0] == '/') {
        *value = cb_strdup(file);
        return true;
    }

#ifdef WIN32
    if (file[0] == '\\' || isDrive(file)) {
        *value = cb_strdup(file);
        return true;
    }

    if (GetCurrentDirectory(sizeof(buffer), buffer) == 0) {
        do_asprintf(error_msg, "Failed to determine current working directory");
        return false;
    }
#else
    if (getcwd(buffer, sizeof(buffer)) == NULL) {
        do_asprintf(error_msg, "Failed to determine current working directory: "
                    "%s\n", strerror(errno));
        return false;
    }
#endif

    len = strlen(buffer);
    snprintf(buffer + len, sizeof(buffer) - len, "%c%s", sep, file);

    fprintf(stderr, "WARNING: workaround for https://www.couchbase.com/"
            "issues/browse/MB-10305 to convert from \"%s\" to \"%s\"\n",
            file, buffer);

    *value = cb_strdup(buffer);
    return true;
}


/** Given a JSON element {i} with the name {key}, attempt to convert
 *  it's value to an integer and store the result in {value}. Returns
 *  true on success; else returns false and sets {error_msg} to point
 *  to message describing the any error.
 */
static bool get_int_value(cJSON *i, const char *key, int* value,
                          char **error_msg) {
    switch (i->type) {
    case cJSON_Number:
        if (i->valueint != i->valuedouble) {
            char *json = cJSON_Print(i);
            do_asprintf(error_msg, "Non-integer value specified for %s: %s\n", key,
                        json);
            cJSON_Free(json);
            return false;
        } else {
            *value = i->valueint;
            return true;
        }
    case cJSON_String:
        if (!safe_strtol(i->valuestring, value)) {
            char *json = cJSON_Print(i);
            do_asprintf(error_msg, "Invalid value specified for %s: %s\n", key,
                        json);
            cJSON_Free(json);
            return false;
        }
        return true;
    default:
        {
            char *json = cJSON_Print(i);
            do_asprintf(error_msg, "Invalid value specified for %s: %s\n", key,
                        json);
            cJSON_Free(json);
            return false;
        }
    }
}

static bool get_in_port_value(cJSON *i, const char *key, in_port_t* value,
                              char **error_msg) {
    int int_value;
    if (!get_int_value(i, key, &int_value, error_msg)) {
        return false;
    }
    if (int_value < 0 || int_value > std::numeric_limits<uint16_t>::max()) {
        do_asprintf(error_msg, "port must be in the range: [0,%u] for %s\n",
                    std::numeric_limits<uint16_t>::max(), key);
        return false;
    }

    *value =  (in_port_t)int_value;
    return true;
}

static bool get_bool_value(cJSON *i, const char *key, bool *value,
                           char **error_msg) {
    switch (i->type) {
    case cJSON_False:
        *value = false;
        return true;
    case cJSON_True:
        *value = true;
        return true;
    default:
        {
            char *json = cJSON_Print(i);
            do_asprintf(error_msg, "Invalid value specified for %s: %s\n", key,
                        json);
            cJSON_Free(json);
            return false;
        }
    }
}

/* Gets a string value from the specified JSON object. Returns true, and sets
 * value to the string value on success; else returns false and sets
 * error_msg to a string describing the error.
 * Caller is responsible for cb_free()ing *value.
 * @param i JSON object.
 * @param value the pointer to store the string value into if return value is
 *              true.
 * @param error_msg the pointer to store a string describing any error
 *                  encountered, if return value is false. Note: if non-null
 *                  this string should be free'd by the caller.
 * @return true if JSON object is a string, else false.
 */
static bool get_string_value(cJSON *i, const char* key, const char **value,
                             char **error_msg) {
    switch (i->type) {
    case cJSON_String:
        *value = cb_strdup(i->valuestring);
        return true;
    default:
        {
            char *json = cJSON_Print(i);
            do_asprintf(error_msg, "Invalid value specified for %s: %s\n", key,
                        json);
            cJSON_Free(json);
            return false;
        }
    }
}

static bool get_host_value(cJSON *i, const char *key, const char **value,
                           char **error_msg) {
    /* @todo add validation */
    return get_string_value(i, key, value, error_msg);
}

static bool get_protocol_value(cJSON *i, const char *key,
                               Protocol *protocol, char **error_msg) {
    const char *string = NULL;
    if (!get_string_value(i, key, &string, error_msg)) {
        return false;
    }

    bool ret = true;
    if (strcasecmp(string, "memcached") == 0) {
        *protocol = Protocol::Memcached;
    } else if (strcasecmp(string, "greenstack") == 0) {
        *protocol = Protocol::Greenstack;
    } else {
        char *json = cJSON_Print(i);
        do_asprintf(error_msg, "Invalid protocol specified for %s: %s\n", key,
                    json);
        cJSON_Free(json);
        ret = false;
    }

    cb_free((void*)string);
    return ret;
}

static bool get_file_value(cJSON *i, const char *key, const char **value,
                           char **error_msg) {
    struct stat st;
    if (i->type != cJSON_String) {
        char *json = cJSON_Print(i);
        do_asprintf(error_msg, "Invalid value specified for %s (not a string): %s\n",
                    key, json);
        cJSON_Free(json);
        return false;
    }

    if (stat(i->valuestring, &st) == -1) {
        do_asprintf(error_msg, "Cannot access \"%s\" specified for \"%s\"\n",
                    i->valuestring, i->string);
        return false;
    }

    *value = i->valuestring;
    return true;
}

/**
 * The callback function for a single configuration attribute
 * @param obj the object containing the configuration value
 * @param settings The settings object to update.
 * @param error_msg If return false is false, message describing why the
 *                  attribute was incorrect. Note caller is responsible for
 *                  cb_free()ing this.
 * @return true if attribute was successfully parsed, else false and error_msg
 *         is set to a string describing the error.
 */
typedef bool (*config_handler)(cJSON *obj, struct settings *settings,
                               char **error_msg);

/**************************************************************************
 **********************  configuration callback  **************************
 *************************************************************************/
static bool get_admin(cJSON *o, struct settings *settings, char **error_msg) {
    const char *ptr = NULL;
    if (!get_string_value(o, o->string, &ptr, error_msg)) {
        return false;
    }
    cb_free((char*)settings->admin);
    if (strlen(ptr) == 0) {
        settings->disable_admin = true;
        settings->admin = NULL;
        cb_free((char*)ptr);
    } else {
        settings->disable_admin = false;
        settings->admin = ptr;
    }
    settings->has.admin = true;
    return true;
}

static bool get_rbac_file(cJSON *o, struct settings *settings, char **error_msg) {
    const char *ptr = NULL;
    if (!get_file_value(o, "RBAC file", &ptr, error_msg)) {
        return false;
    }

    if (!get_absolute_file(ptr, &settings->rbac_file, error_msg)) {
        return false;
    }

    settings->has.rbac = true;
    return true;
}

static bool get_rbac_privilege_debug(cJSON *o, struct settings *settings, char **error_msg) {
    if (!get_bool_value(o, "rbac_privilege_debug", &settings->rbac_privilege_debug, error_msg)) {
        return false;
    }

    settings->has.rbac_privilege_debug = true;
    return true;
}

static bool get_audit_file(cJSON *o, struct settings *settings, char **error_msg) {
    const char *ptr = NULL;
    if (!get_file_value(o, "audit file", &ptr, error_msg)) {
        return false;
    }

    if (!get_absolute_file(ptr, &settings->audit_file, error_msg)) {
        return false;
    }

    settings->has.audit = true;
    return true;
}

static bool get_root(cJSON *o, struct settings *settings, char **error_msg) {
    const char *ptr = NULL;
    if (!get_file_value(o, "root", &ptr, error_msg)) {
        return false;
    }

    if (!get_absolute_file(ptr, &settings->root, error_msg)) {
        return false;
    }

    settings->has.root = true;
    return true;
}

static bool get_ssl_cipher_list(cJSON *o, struct settings *settings, char **error_msg) {
    const char *ptr = NULL;
    if (!get_string_value(o, o->string, &ptr, error_msg)) {
        return false;
    }

    if (strlen(ptr) == 0) {
        settings->ssl_cipher_list = NULL;
        cb_free((void*)ptr);
    } else {
        settings->ssl_cipher_list = ptr;
    }

    settings->has.ssl_cipher_list = true;
    return true;
}

static bool get_ssl_minimum_protocol(cJSON* o, struct settings* settings,
                                     char** error_msg) {
    const char *ptr = nullptr;
    if (!get_string_value(o, o->string, &ptr, error_msg)) {
        return false;
    }

    if (strlen(ptr) == 0) {
        settings->ssl_minimum_protocol = nullptr;
        cb_free((void*)ptr);
    } else {
        settings->ssl_minimum_protocol = ptr;
    }

    settings->has.ssl_minimum_protocol = true;
    return true;
}

static bool get_threads(cJSON *o, struct settings *settings,
                        char **error_msg) {
    if (get_int_value(o, o->string, &settings->num_threads, error_msg)) {
        settings->has.threads = true;
        return true;
    } else {
        return false;
    }
}

static bool get_max_packet_size(cJSON *o, struct settings *settings,
                                char **error_msg) {
    int max_packet_size;
    if (get_int_value(o, o->string, &max_packet_size, error_msg)) {
        settings->has.max_packet_size = true;
        settings->max_packet_size = (uint32_t)max_packet_size * 1024 * 1024;
        return true;
    } else {
        return false;
    }
}

static bool get_verbosity(cJSON *o, struct settings *settings,
                          char **error_msg) {
    int verbosity;
    if (get_int_value(o, o->string, &verbosity, error_msg)) {
        settings->verbose.store(verbosity);
        settings->has.verbose = true;
        return true;
    } else {
        return false;
    }
}

static bool get_connection_idle_time(cJSON *o, struct settings *settings,
                          char **error_msg) {
    int value;
    if (get_int_value(o, o->string, &value, error_msg)) {
        settings->connection_idle_time = (size_t)value;
        settings->has.connection_idle_time = true;
        return true;
    } else {
        return false;
    }
}

static bool get_default_reqs_per_event(cJSON *o, struct settings *settings,
                                       char **error_msg) {
    if (get_int_value(o, o->string, &settings->default_reqs_per_event, error_msg)) {
        settings->has.default_reqs_per_event = true;
        return true;
    } else {
        return false;
    }
}

static bool get_reqs_per_event_high_priority(cJSON *o, struct settings *settings,
                                             char **error_msg) {
    if (get_int_value(o, o->string, &settings->reqs_per_event_high_priority, error_msg)) {
        settings->has.reqs_per_event_high_priority = true;
        return true;
    } else {
        return false;
    }
}

static bool get_reqs_per_event_med_priority(cJSON *o, struct settings *settings,
                                            char **error_msg) {
    if (get_int_value(o, o->string, &settings->reqs_per_event_med_priority, error_msg)) {
        settings->has.reqs_per_event_med_priority = true;
        return true;
    } else {
        return false;
    }
}

static bool get_reqs_per_event_low_priority(cJSON *o, struct settings *settings,
                                            char **error_msg) {
    if (get_int_value(o, o->string, &settings->reqs_per_event_low_priority, error_msg)) {
        settings->has.reqs_per_event_low_priority = true;
        return true;
    } else {
        return false;
    }
}

static bool get_require_init(cJSON *o, struct settings *settings,
                             char **error_msg) {
    if (get_bool_value(o, o->string, &settings->require_init, error_msg)) {
        settings->has.require_init = true;
        return true;
    } else {
        return false;
    }
}

static bool get_require_sasl(cJSON *o, struct settings *settings,
                             char **error_msg) {
    if (get_bool_value(o, o->string, &settings->require_sasl, error_msg)) {
        settings->has.require_sasl = true;
        return true;
    } else {
        return false;
    }
}

static bool get_extension(cJSON *r, struct extension_settings *ext_settings,
                          char **error_msg) {
    if (r->type == cJSON_Object) {
        cJSON *p = r->child;
        while (p != NULL) {
            if (strcasecmp("module", p->string) == 0) {
                if (!get_string_value(p, "extension module",
                                      &ext_settings->soname, error_msg)) {
                    return false;
                }
            } else if (strcasecmp("config", p->string) == 0) {
                if (!get_string_value(p, "extension config",
                                      &ext_settings->config, error_msg)) {
                    return false;
                }
            } else {
                do_asprintf(error_msg, "Unknown attribute for extension: %s\n",
                            p->string);
                return false;
            }
            p = p->next;
        }
        return true;
    } else {
        do_asprintf(error_msg, "Invalid entry for extension\n");
        return false;
    }
}

static bool get_extensions(cJSON *o, struct settings *settings,
                           char **error_msg) {
    /* extensions is supposed to be a sub group */
    settings->num_pending_extensions = cJSON_GetArraySize(o);
    cJSON *e = o->child;
    int ii = 0;

    settings->pending_extensions = reinterpret_cast<struct extension_settings *>(cb_calloc(settings->num_pending_extensions,
        sizeof(struct extension_settings)));

    while (e != NULL) {
        if (!get_extension(e, &settings->pending_extensions[ii], error_msg)) {
            return false;
        }
        ++ii;
        e = e->next;
    }
    settings->has.extensions = true;
    return true;
}

static bool get_engine(cJSON *, struct settings *, char **) {
    LOG_NOTICE(nullptr,
               "Ignoring obsolete engine parameter specified in config.");
    return true;
}

/**
 * The callback function for an interface configuration section.
 * @param ii the interface index.
 * @param r the object containing the configuration value
 * @param iface The interface settings object to update.
 * @error_msg if parsing failed, set to a string describing the error. Note: it
 *            is the caller's responsibility to free() this.
 * @return true if parsed successfully, else false.
 */
typedef bool (*interface_handler)(int ii, cJSON *r, struct interface* iface,
                                  char **erro_msg);

static bool get_interface_maxconn(int idx, cJSON *r, struct interface* iface,
                                  char **error_msg) {
    return get_int_value(r, "interface maxconn", &iface->maxconn, error_msg);
}

static bool get_interface_port(int idx, cJSON *r, struct interface* iface,
                               char **error_msg) {
    return get_in_port_value(r, "interface port", &iface->port, error_msg);
}

static bool get_interface_backlog(int idx, cJSON *r, struct interface* iface,
                                  char **error_msg) {
    return get_int_value(r, "interface backlog", &iface->backlog, error_msg);
}

static bool get_interface_tcp_nodelay(int idx, cJSON *o,
                                      struct interface* iface,
                                      char **error_msg) {
    return get_bool_value(o, o->string, &iface->tcp_nodelay, error_msg);
}

static bool get_interface_ipv4(int idx, cJSON *r, struct interface* iface,
                               char **error_msg) {
    return get_bool_value(r, r->string, &iface->ipv4, error_msg);
}

static bool get_interface_ipv6(int idx, cJSON *r, struct interface* iface,
                               char **error_msg) {
    return get_bool_value(r, r->string, &iface->ipv6, error_msg);
}

static bool get_interface_host(int idx, cJSON *r, struct interface* iface,
                               char **error_msg) {
    const char* host;
    if (!get_host_value(r, "interface host", &host, error_msg)) {
        return false;
    }

    iface->host = host;
    return true;
}

static bool get_interface_protocol(int idx, cJSON *r, struct interface* iface,
                                   char **error_msg) {
    if (!get_protocol_value(r, "interface protocol",
                            &iface->protocol, error_msg)) {
        return false;
    }
    return true;
}

static bool get_interface_ssl(int idx, cJSON *r, struct interface* iface,
                              char **error_msg) {
    const char *cert = NULL;
    const char *key = NULL;
    if (r->type == cJSON_Object) {
        cJSON *p = r->child;
        while (p != NULL) {
            if (strcasecmp("key", p->string) == 0) {
                if (!get_file_value(p, "interface key file", &key, error_msg)) {
                    return false;
                }
            } else if (strcasecmp("cert", p->string) == 0) {
                if (!get_file_value(p, "interface ssl certificate", &cert,
                                    error_msg)) {
                    return false;
                }
            } else {
                do_asprintf(error_msg, "Unknown attribute for ssl: %s\n",
                            p->string);
                return false;
            }
            p = p->next;
        }

        if (key && cert) {
            if (!get_absolute_file(key, &iface->ssl.key, error_msg)) {
                return false;
            }
            if (!get_absolute_file(cert, &iface->ssl.cert, error_msg)) {
                return false;
            }
        } else if (key || cert) {
            do_asprintf(error_msg, "You need to specify a value for cert and key\n");
            return false;
        }
    } else if (r->type != cJSON_False) {
        do_asprintf(error_msg, "Invalid entry for ssl\n");
        return false;
    }
    return true;
}

static bool get_interface_management(int idx, cJSON *r, struct interface* iface,
                               char **error_msg) {
    return get_bool_value(r, r->string, &iface->management, error_msg);
}


static bool handle_interface(int idx, cJSON *r, struct interface* iface_list,
                             char **error_msg) {
    /* set default values */
    struct interface* iface = &iface_list[idx];
    iface->backlog = 1024;
    iface->ipv4 = true;
    iface->ipv6 = true;
    iface->tcp_nodelay = true;
    iface->management = false;

    if (r->type == cJSON_Object) {
        struct {
            const char *key;
            interface_handler handler;
        } handlers[] = {
            { "maxconn", get_interface_maxconn },
            { "port", get_interface_port },
            { "host", get_interface_host },
            { "backlog", get_interface_backlog },
            { "ipv4", get_interface_ipv4 },
            { "ipv6", get_interface_ipv6 },
            { "tcp_nodelay", get_interface_tcp_nodelay },
            { "ssl", get_interface_ssl },
            { "management", get_interface_management },
            { "protocol", get_interface_protocol },
            { NULL, NULL }
        };
        cJSON *obj = r->child;
        while (obj != NULL) {
            int ii = 0;
            while (handlers[ii].key != NULL) {
                if (strcasecmp(handlers[ii].key, obj->string) == 0) {
                    break;
                }
                ++ii;
            }

            if (handlers[ii].key == NULL) {
                do_asprintf(error_msg,
                            "Unknown token \"%s\" for interface #%u ignored.\n",
                            obj->string, idx);
            } else {
                if (!handlers[ii].handler(idx, obj, iface, error_msg)) {
                    return false;
                }
            }

            obj = obj->next;
        }

        /* Perform additional checks on inter-related attributes */
        if (!iface->ipv4 && !iface->ipv6) {
            do_asprintf(error_msg,
                        "IPv4 and IPv6 cannot be disabled at the same time\n");
            return false;
        }
        for (int ii = 0; ii < idx; ++ii) {
            if (iface_list[ii].port == iface->port && iface->port != 0) {
                /* port numbers are used as a unique identified inside memcached
                 * (see for example: get_listening_port_instance(). Check user
                 * doesn't try to use the same number twice.
                 */
                do_asprintf(error_msg,
                            "Port %d is already in use by interface[%d].\n",
                            iface_list[ii].port, ii);
                return false;
            }
        }
        /* validate !!! */
        return true;
    } else {
        do_asprintf(error_msg, "Invalid entry for interface #%u\n", idx);
        return false;
    }
}

static bool get_interfaces(cJSON *o, struct settings *settings,
                           char **error_msg) {
    int total = cJSON_GetArraySize(o);
    cJSON *c = o->child;
    int ii = 0;

    settings->interfaces = reinterpret_cast<struct interface*>(cb_calloc(total, sizeof(struct interface)));
    settings->num_interfaces = total;
    while (c != NULL) {
        if (!handle_interface(ii, c, settings->interfaces, error_msg)) {
            return false;
        }
        ++ii;
        c = c->next;
    }
    settings->has.interfaces = true;
    return true;
}

static bool get_bio_drain_sz(cJSON *i, struct settings *settings,
                                    char **error_msg) {
    int buffer_sz;
    if (!get_int_value(i, "bio_drain_buffer_sz", &buffer_sz, error_msg)) {
        return false;
    } else {
        settings->bio_drain_buffer_sz = (size_t)buffer_sz;
        settings->has.bio_drain_buffer_sz = true;
        return true;
    }
}

static bool get_datatype(cJSON *o, struct settings *settings,
                         char **error_msg) {
    if (get_bool_value(o, o->string, &settings->datatype, error_msg)) {
        settings->has.datatype = true;
        return true;
    } else {
        return false;
    }
}

static bool get_dedupe_nmvb_maps(cJSON* o, struct settings* settings,
                                 char** error_msg) {
    bool value;
    if (get_bool_value(o, o->string, &value, error_msg)) {
        settings->dedupe_nmvb_maps.store(value);
        settings->has.dedupe_nmvb_maps = true;
        return true;
    } else {
        return false;
    }
}

static bool get_stdin_listen(cJSON *o, struct settings *settings,
                                 char **error_msg) {
    if (get_bool_value(o, o->string, &settings->stdin_listen, error_msg)) {
        settings->has.stdin_listen = true;
        return true;
    } else {
        return false;
    }
}

static bool dyna_reconfig_stdin_listen(const struct settings *new_settings,
                                       cJSON* errors) {
    /* stdin_listen isn't dynamic */
    if (!new_settings->has.stdin_listen) {
        return true;
    }
    if (new_settings->stdin_listen == settings.stdin_listen) {
        return true;
    } else {
        cJSON_AddItemToArray(errors,
                             cJSON_CreateString("'stdin_listen' is not a dynamic setting."));
        return false;
    }
}

static bool get_exit_on_connection_close(cJSON *o, struct settings *settings,
                                         char **error_msg) {
    if (get_bool_value(o, o->string, &settings->exit_on_connection_close, error_msg)) {
        settings->has.exit_on_connection_close = true;
        return true;
    } else {
        return false;
    }
}

static bool get_sasl_mechanisms(cJSON *o, struct settings *settings,
                                char **error_msg) {
    if (get_string_value(o, o->string, &settings->sasl_mechanisms, error_msg)) {
        settings->has.sasl_mechanisms = true;
        return true;
    } else {
        return false;
    }
}

static bool dyna_reconfig_exit_on_connection_close(const struct settings *new_settings,
                                                   cJSON* errors) {
    /* exit_on_connection_close isn't dynamic */
    if (!new_settings->has.exit_on_connection_close) {
        return true;
    }
    if (new_settings->exit_on_connection_close == settings.exit_on_connection_close) {
        return true;
    } else {
        cJSON_AddItemToArray(errors,
                             cJSON_CreateString("'exit_on_connection_close' is not a dynamic setting."));
        return false;
    }
}

static bool dyna_reconfig_sasl_mechanisms(const struct settings *new_settings,
                                          cJSON* errors) {
    /* exit_on_connection_close isn't dynamic */
    if (!new_settings->has.sasl_mechanisms) {
        return true;
    }

    if (settings.sasl_mechanisms == nullptr) {
        cJSON_AddItemToArray(errors,
                             cJSON_CreateString("'sasl_mechanisms' is not a "
                                                    "dynamic setting."));
        return false;
    }

    if (strcmp(new_settings->sasl_mechanisms, settings.sasl_mechanisms) == 0) {
        return true;
    } else {
        cJSON_AddItemToArray(errors,
                             cJSON_CreateString("'sasl_mechanisms' is not a "
                                                    "dynamic setting."));
        return false;
    }
}

static bool parse_breakpad(cJSON *o, struct settings *settings,
                           char** error_msg) {
    if (o->type != cJSON_Object) {
        do_asprintf(error_msg, "Invalid entry for breakpad - expected object.\n");
        return false;
    }

    // Breakpad config defaults:
    bool enabled = false;
    const char* minidump_dir = NULL;
    breakpad_content_t content = CONTENT_DEFAULT;

    const char* content_str = NULL;
    bool error = false;


    for(cJSON *p = o->child; p != NULL && !error; p = p->next) {
        if (strcasecmp("enabled", p->string) == 0) {
            if (!get_bool_value(p, "breapad enabled", &enabled,
                                  error_msg)) {
                error = true;
            }
        } else if (strcasecmp("minidump_dir", p->string) == 0) {
            if (!get_string_value(p, "breakpad minidump_dir", &minidump_dir,
                                  error_msg)) {
                error = true;
            }
        } else if (strcasecmp("content", p->string) == 0) {
            if (!get_string_value(p, "breakpad content", &content_str,
                                  error_msg)) {
                error = true;
            }
        } else {
            do_asprintf(error_msg, "Unknown attribute for breakpad: %s\n",
                        p->string);
            error = true;
        }
    }
    if (error) {
        cb_free((char*)minidump_dir);
        cb_free((char*)content_str);
        return false;
    }

    /* Validate parameters */
    if (enabled) {
        /* If 'enabled' was set, 'minidump_dir' must also be set. */
        if (minidump_dir == NULL) {
            do_asprintf(error_msg,
                        "breakpad.enabled==true but minidump_dir not specified.\n");
            cb_free((char*)minidump_dir);
            cb_free((char*)content_str);
            return false;
        }
    }

    if (content_str) {
        /* Only valid value is 'default' currently. */
        if (strcmp(content_str, "default") == 0) {
            content = CONTENT_DEFAULT;
        } else {
            do_asprintf(error_msg, "Invalid value for breakpad.content: %s\n",
                        content_str);
            error = true;
        }
        /* String converted to enum, no longer needed. */
        cb_free((char*)content_str);
    }
    if (error) {
        cb_free((char*)minidump_dir);
        return false;
    }

    // Allow runtime-disabling of Breakpad if CB_DISABLE_BREAKPAD is set.
    if (getenv("CB_DISABLE_BREAKPAD") != NULL) {
        enabled = false;
    }

    /* Validated, update settings. */
    settings->breakpad.enabled = enabled;
    /* Empty string (as opposed to NULL string) used here to simplify compare
       logic when checking for differences in breakpad config. */
    settings->breakpad.minidump_dir = minidump_dir ? minidump_dir
                                                   : cb_strdup("");
    settings->breakpad.content = content;
    settings->has.breakpad = true;
    return true;
}

/* reconfig (dynamic config update) handlers *********************************/

typedef bool (*dynamic_validate_handler)(const struct settings *new_settings,
                                         cJSON* errors);

typedef void (*dynamic_reconfig_handler)(const struct settings *new_settings);

static bool dyna_validate_admin(const struct settings *new_settings,
                                cJSON* errors) {
    if (!new_settings->has.admin) {
        return true;
    }
    if (settings.admin != NULL &&
        new_settings->admin != NULL &&
        strcmp(new_settings->admin, settings.admin) == 0) {
        return true;
    } else if (settings.admin == NULL && new_settings->admin == NULL) {
        return true;
    } else {
        cJSON_AddItemToArray(errors,
                             cJSON_CreateString("'admin' is not a dynamic setting."));
        return false;
    }
}

static bool dyna_validate_rbac_file(const struct settings *new_settings,
                                    cJSON* errors) {
    if (!new_settings->has.rbac) {
        return true;
    }

    if (settings.rbac_file != NULL &&
        new_settings->rbac_file != NULL &&
        strcmp(new_settings->rbac_file, settings.rbac_file) == 0) {
        return true;
    } else if (settings.rbac_file == NULL && new_settings->rbac_file == NULL) {
        return true;
    } else {
        cJSON_AddItemToArray(errors,
                             cJSON_CreateString("'rbac_file' is not a dynamic setting."));
        return false;
    }
}

static bool dyna_validate_rbac_privilege_debug(const struct settings *new_settings,
                                               cJSON* errors) {
    return true;
}

static bool dyna_validate_audit_file(const struct settings *new_settings,
                                    cJSON* errors) {
    if (!new_settings->has.audit) {
        return true;
    }

    if (settings.audit_file != NULL &&
        new_settings->audit_file != NULL &&
        strcmp(new_settings->audit_file, settings.audit_file) == 0) {
        return true;
    } else if (settings.audit_file == NULL && new_settings->audit_file == NULL) {
        return true;
    } else {
        cJSON_AddItemToArray(errors,
                             cJSON_CreateString("'audit_file' is not a dynamic setting."));
        return false;
    }
}

static bool dyna_validate_root(const struct settings *new_settings,
                               cJSON* errors) {
    if (!new_settings->has.root) {
        return true;
    }

    if (settings.root != NULL &&
        new_settings->root != NULL &&
        strcmp(new_settings->root, settings.root) == 0) {
        return true;
    } else if (settings.root == NULL && new_settings->root == NULL) {
        return true;
    } else {
        cJSON_AddItemToArray(errors,
                             cJSON_CreateString("'root' is not a dynamic setting."));
        return false;
    }
}

static bool dyna_validate_ssl_cipher_list(const struct settings *new_settings,
                                          cJSON* errors) {
    /* Its dynamic :-) */
    return true;
}

static bool dyna_validate_ssl_minimum_protocol(const struct settings* ns,
                                               cJSON* errors) {
    if (!ns->has.ssl_minimum_protocol) {
        return true;
    }

    try {
        decode_ssl_protocol(ns->ssl_minimum_protocol);
    } catch (std::invalid_argument& e) {
        std::stringstream ss;
        ss << "'" << ns->ssl_minimum_protocol << "' is not a supported "
           << "value for 'ssl_minimum_protocol";
        cJSON_AddItemToArray(errors,
                             cJSON_CreateString(ss.str().c_str()));
        return false;
    }
    return true;
}

static bool dyna_validate_threads(const struct settings *new_settings,
                                  cJSON* errors) {
    if (!new_settings->has.threads) {
        return true;
    }
    if (new_settings->num_threads == settings.num_threads) {
        return true;
    } else {
        cJSON_AddItemToArray(errors,
                             cJSON_CreateString("'num_threads' is not a dynamic setting."));
        return false;
    }
}

static bool dyna_validate_max_packet_size(const struct settings *new_settings,
                                  cJSON* errors) {
    if (!new_settings->has.max_packet_size) {
        return true;
    }
    if (new_settings->max_packet_size == settings.max_packet_size) {
        return true;
    } else {
        cJSON_AddItemToArray(errors,
                             cJSON_CreateString("'max_packet_size' is not a dynamic setting."));
        return false;
    }
}

static bool dyna_validate_interfaces(const struct settings *new_settings,
                                     cJSON* errors) {
    bool valid = false;
    if (!new_settings->has.interfaces) {
        return true;
    }

    /* parts of interface are dynamic, but not the overall number or name... */
    if (new_settings->num_interfaces == settings.num_interfaces) {
        char* tempstr = NULL;
        valid = true;
        int ii = 0;
        for (ii = 0; ii < settings.num_interfaces; ii++) {
            struct interface *cur_if = &settings.interfaces[ii];
            struct interface *new_if = &new_settings->interfaces[ii];

            // we can't validate dynamic ports...
            if (cur_if->port == 0 || new_if->port == 0) {
                continue;
            }

            /* These settings cannot change: */
            if (strcmp(new_if->host, cur_if->host) != 0) {
                do_asprintf(&tempstr,
                            "interface '%d' cannot change host dynamically.",
                            ii);
                cJSON_AddItemToArray(errors, cJSON_CreateString(tempstr));
                free(tempstr);
                valid = false;
            }
            if (new_if->port != cur_if->port) {
                do_asprintf(&tempstr,
                            "interface '%d' cannot change port dynamically.",
                            ii);
                cJSON_AddItemToArray(errors, cJSON_CreateString(tempstr));
                free(tempstr);
                valid = false;
            }
            if (new_if->ipv4 != cur_if->ipv4) {
                do_asprintf(&tempstr,
                            "interface '%d' cannot change IPv4 dynamically.",
                            ii);
                cJSON_AddItemToArray(errors, cJSON_CreateString(tempstr));
                free(tempstr);
                valid = false;
            }
            if (new_if->ipv6 != cur_if->ipv6) {
                do_asprintf(&tempstr,
                            "interface '%d' cannot change IPv6 dynamically.",
                            ii);
                cJSON_AddItemToArray(errors, cJSON_CreateString(tempstr));
                free(tempstr);
                valid = false;
            }
        }
    } else {
        cJSON_AddItemToArray(errors,
                             cJSON_CreateString("Number of interfaces cannot change dynamically."));
    }
    return valid;
}

static bool dyna_validate_extensions(const struct settings *new_settings,
                                     cJSON* errors)
{
    if (!new_settings->has.extensions) {
        return true;
    }

    /* extensions is not dynamic - validate it hasn't changed.*/
    bool valid = false;
    if (new_settings->num_pending_extensions ==
        settings.num_pending_extensions) {
        valid = true;
        int ii = 0;
        for (ii = 0; ii < settings.num_pending_extensions; ii++) {
            /* soname must be non-NULL and equal */
            valid &= new_settings->pending_extensions[ii].soname != NULL &&
                     strcmp(new_settings->pending_extensions[ii].soname,
                            settings.pending_extensions[ii].soname) == 0;

            /* new 'config' should either be NULL or equal to to the old one. */
            valid &= settings.pending_extensions[ii].config == NULL ||
                    (new_settings->pending_extensions[ii].config != NULL &&
                     strcmp(new_settings->pending_extensions[ii].config,
                            settings.pending_extensions[ii].config) == 0);
        }
    }

    if (valid) {
        return true;
    } else {
        cJSON_AddItemToArray(errors,
                             cJSON_CreateString("'extensions' is not a dynamic setting."));
        return false;
    }
}

static bool dyna_validate_engine(const struct settings *new_settings,
                                 cJSON* errors)
{
    return true;
}

static bool dyna_validate_require_init(const struct settings *new_settings,
                                       cJSON* errors)
{
    if (!new_settings->has.require_init) {
        return true;
    }

    if (new_settings->require_init == settings.require_init) {
        return true;
    } else {
        cJSON_AddItemToArray(errors,
                             cJSON_CreateString("'require_init' is not a dynamic setting."));
        return false;
    }
}

static bool dyna_validate_require_sasl(const struct settings *new_settings,
                                       cJSON* errors)
{
    if (!new_settings->has.require_sasl) {
        return true;
    }

    if (new_settings->require_sasl == settings.require_sasl) {
        return true;
    } else {
        cJSON_AddItemToArray(errors,
                             cJSON_CreateString("'require_sasl' is not a dynamic setting."));
        return false;
    }
}

static bool dyna_validate_default_reqs_per_event(const struct settings *new_settings,
                                                 cJSON* errors)
{
    /* default_reqs_per_event *is* dynamic */
    return true;
}

static bool dyna_validate_reqs_per_event_high_priority(const struct settings *new_settings,
                                                       cJSON* errors)
{
    /* reqs_per_event_high_priority *is* dynamic */
    return true;
}

static bool dyna_validate_reqs_per_event_med_priority(const struct settings *new_settings,
                                                      cJSON* errors)
{
    /* reqs_per_event_med_priority *is* dynamic */
    return true;
}

static bool dyna_validate_reqs_per_event_low_priority(const struct settings *new_settings,
                                                      cJSON* errors)
{
    /* reqs_per_event_low_priority *is* dynamic */
    return true;
}

static bool dyna_validate_verbosity(const struct settings *new_settings,
                                    cJSON* errors)
{
    /* verbosity *is* dynamic */
    return true;
}

static bool dyna_validate_connection_idle_time(const struct settings *new_settings,
                                               cJSON* errors)
{
    /* connection_idle_time *is* dynamic */
    return true;
}

static bool dyna_validate_bio_drain_sz(const struct settings *new_settings,
                                              cJSON* errors)
{
    /* bio_drain_buffer_sz isn't dynamic */
    if (!new_settings->has.bio_drain_buffer_sz) {
        return true;
    }
    if (new_settings->bio_drain_buffer_sz == settings.bio_drain_buffer_sz) {
        return true;
    } else {
        cJSON_AddItemToArray(errors,
                             cJSON_CreateString("'bio_drain_buffer_sz' is not a dynamic setting."));
        return false;
    }
}

static bool dyna_validate_datatype(const struct settings *new_settings,
                                   cJSON* errors)
{
    /* datatype isn't dynamic */
    if (!new_settings->has.datatype) {
        return true;
    }
    if (new_settings->datatype == settings.datatype) {
        return true;
    } else {
        cJSON_AddItemToArray(errors,
                             cJSON_CreateString("'datatype_support' is not a dynamic setting."));
        return false;
    }
}

static bool dyna_validate_breakpad(const struct settings *new_settings,
                                   cJSON* errors)
{
    /* breakpad settings are all dynamic. */
    return true;
}

static bool dyna_validate_always_true(const struct settings *, cJSON*) {
    /* Its dynamic :-) */
    return true;
}

/* dynamic reconfiguration handlers ******************************************/

static void dyna_reconfig_iface_maxconns(const struct interface *new_if,
                                         struct interface *cur_if) {
    if (new_if->maxconn != cur_if->maxconn) {
        struct listening_port *port = get_listening_port_instance(cur_if->port);
        int old_maxconns = cur_if->maxconn;
        cur_if->maxconn = new_if->maxconn;
        port->maxconns = new_if->maxconn;
        calculate_maxconns();

        LOG_NOTICE(NULL, "Changed maxconns for interface %s:%hu from %d to %d",
                   cur_if->host, cur_if->port, old_maxconns, cur_if->maxconn);
    }
}

static void dyna_reconfig_iface_backlog(const struct interface *new_if,
                                       struct interface *cur_if) {
    if (new_if->backlog != cur_if->backlog) {
        int old_backlog = cur_if->backlog;
        cur_if->backlog = new_if->backlog;

        LOG_NOTICE(NULL, "Changed backlog for interface %s:%hu from %d to %d",
                   cur_if->host, cur_if->port, old_backlog, cur_if->backlog);
    }
}

static void dyna_reconfig_iface_nodelay(const struct interface *new_if,
                                       struct interface *cur_if) {
    if (new_if->tcp_nodelay != cur_if->tcp_nodelay) {
        Connection * c = NULL;
        bool old_tcp_nodelay = cur_if->tcp_nodelay;
        cur_if->tcp_nodelay = new_if->tcp_nodelay;

        /* find all sockets for this connection, and update TCP_NODELAY sockopt */
        for (c = listen_conn; c != NULL; c = c->getNext()) {
            auto port = c->getParentPort();
            if (port == cur_if->port) {
                int nodelay_flag = cur_if->tcp_nodelay;
#if defined(WIN32)
                char* ptr = reinterpret_cast<char*>(&nodelay_flag);
#else
                void* ptr = reinterpret_cast<void*>(&nodelay_flag);
#endif
                int error = setsockopt(c->getSocketDescriptor(), IPPROTO_TCP, TCP_NODELAY,
                                       ptr, sizeof(nodelay_flag));
                if (error != 0) {
                    LOG_WARNING(NULL,
                                "Failed to set TCP_NODELAY for FD %d, interface %s:%hu to %d: %s",
                                c->getSocketDescriptor(), cur_if->host,
                                cur_if->port, nodelay_flag,
                                strerror(errno));
                } else {
                    LOG_NOTICE(NULL,
                               "Changed tcp_nodelay for FD %d, interface %s:%hu from %d to %d",
                               c->getSocketDescriptor(), cur_if->host,
                               cur_if->port, old_tcp_nodelay,
                               cur_if->tcp_nodelay);
                }
            }
        }
    }
}

static void dyna_reconfig_iface_ssl(const struct interface *new_if,
                                    struct interface *cur_if) {
    if (cur_if->ssl.cert != NULL && strcmp(new_if->ssl.cert,
                                           cur_if->ssl.cert) != 0) {
        const char *old_cert = cur_if->ssl.cert;
        cur_if->ssl.cert = cb_strdup(new_if->ssl.cert);
        LOG_NOTICE(NULL, "Changed ssl.cert for interface %s:%hu from %s to %s",
                   cur_if->host, cur_if->port, old_cert, cur_if->ssl.cert);
        cb_free((char*)old_cert);
    }

    if (cur_if->ssl.key != NULL && strcmp(new_if->ssl.key,
                                           cur_if->ssl.key) != 0) {
        const char *old_key = cur_if->ssl.key;
        cur_if->ssl.key = cb_strdup(new_if->ssl.key);
        LOG_NOTICE(NULL, "Changed ssl.key for interface %s:%hu from %s to %s",
                   cur_if->host, cur_if->port, old_key, cur_if->ssl.key);
        cb_free((char*)old_key);
    }
}

static void dyna_reconfig_interfaces(const struct settings *new_settings) {
    for (int ii = 0; ii < settings.num_interfaces; ii++) {
        struct interface* cur_if = &settings.interfaces[ii];
        struct interface* new_if = &new_settings->interfaces[ii];

        // Skip wildcards
        if (new_if->port != 0) {
            dyna_reconfig_iface_maxconns(new_if, cur_if);
            dyna_reconfig_iface_backlog(new_if, cur_if);
            dyna_reconfig_iface_nodelay(new_if, cur_if);
            dyna_reconfig_iface_ssl(new_if, cur_if);
        }
    }
}

static void dyna_reconfig_default_reqs_per_event(const struct settings *new_settings) {
    if (new_settings->has.default_reqs_per_event &&
        new_settings->default_reqs_per_event != settings.default_reqs_per_event) {
        int old_reqs = settings.default_reqs_per_event;
        settings.default_reqs_per_event = new_settings->default_reqs_per_event;
        LOG_NOTICE(NULL, "Changed default reqs_per_event from %d to %d",
                   old_reqs, settings.default_reqs_per_event);
    }
}

static void dyna_reconfig_reqs_per_event_high_priority(const struct settings *new_settings) {
    if (new_settings->has.reqs_per_event_high_priority &&
        new_settings->reqs_per_event_high_priority != settings.reqs_per_event_high_priority) {
        int old_reqs = settings.reqs_per_event_high_priority;
        settings.reqs_per_event_high_priority = new_settings->reqs_per_event_high_priority;
        LOG_NOTICE(NULL, "Changed high priority reqs_per_event from %d to %d",
                   old_reqs, settings.reqs_per_event_high_priority);
    }
}

static void dyna_reconfig_reqs_per_event_med_priority(const struct settings *new_settings) {
    if (new_settings->has.reqs_per_event_med_priority &&
        new_settings->reqs_per_event_med_priority != settings.reqs_per_event_med_priority) {
        int old_reqs = settings.reqs_per_event_med_priority;
        settings.reqs_per_event_med_priority = new_settings->reqs_per_event_med_priority;
        LOG_NOTICE(NULL, "Changed medium priority reqs_per_event from %d to %d",
                   old_reqs, settings.reqs_per_event_med_priority);
    }
}

static void dyna_reconfig_reqs_per_event_low_priority(const struct settings *new_settings) {
    if (new_settings->has.reqs_per_event_low_priority &&
        new_settings->reqs_per_event_low_priority != settings.reqs_per_event_low_priority) {
        int old_reqs = settings.reqs_per_event_low_priority;
        settings.reqs_per_event_low_priority = new_settings->reqs_per_event_low_priority;
        LOG_NOTICE(NULL, "Changed low priority reqs_per_event from %d to %d",
                   old_reqs, settings.reqs_per_event_low_priority);
    }
}

static void dyna_reconfig_verbosity(const struct settings *new_settings) {
    if (new_settings->has.verbose &&
        new_settings->verbose != settings.verbose) {
        int old_verbose = settings.verbose;
        settings.verbose.store(new_settings->verbose);
        perform_callbacks(ON_LOG_LEVEL, NULL, NULL);
        LOG_NOTICE(NULL, "Changed verbosity from %d to %d", old_verbose,
                   settings.verbose.load());
    }
}

static void dyna_reconfig_connection_idle_time(
    const struct settings* new_settings) {
    if (new_settings->has.connection_idle_time) {
        if (new_settings->connection_idle_time !=
            settings.connection_idle_time) {
            LOG_NOTICE(nullptr, "Changed connection_idle_time from %us to %us",
                       (unsigned int)settings.connection_idle_time,
                       (unsigned int)new_settings->connection_idle_time);
            settings.connection_idle_time = new_settings->connection_idle_time;
        }
    }
}

static void dyna_reconfig_rbac_privilege_debug(const struct settings *new_settings) {
    if (new_settings->has.rbac_privilege_debug) {
        auth_set_privilege_debug(new_settings->rbac_privilege_debug);
        settings.has.rbac_privilege_debug = true;
        settings.rbac_privilege_debug = new_settings->rbac_privilege_debug;
    }
}

static void dyna_reconfig_breakpad(const struct settings *new_settings) {
    if (new_settings->has.breakpad) {
        bool reconfig = false;
        if (new_settings->breakpad.enabled != settings.breakpad.enabled) {
            reconfig = true;
            const bool old_enabled = settings.breakpad.enabled;
            settings.breakpad.enabled = new_settings->breakpad.enabled;
            LOG_NOTICE(NULL,
                "Changed breakpad.enabled from %d to %d", old_enabled,
                settings.breakpad.enabled);
        }

        if (strcmp(new_settings->breakpad.minidump_dir,
                   settings.breakpad.minidump_dir) != 0) {
            reconfig = true;
            const char* old_dir = settings.breakpad.minidump_dir;
            settings.breakpad.minidump_dir = cb_strdup(new_settings->breakpad.minidump_dir);
            LOG_NOTICE(NULL,
                "Changed breakpad.minidump_dir from %s to %s", old_dir,
                settings.breakpad.minidump_dir);
            cb_free((char*)old_dir);
        }

        if (new_settings->breakpad.content != settings.breakpad.content) {
            reconfig = true;
            const breakpad_content_t old_content = settings.breakpad.content;
            settings.breakpad.content = new_settings->breakpad.content;
            LOG_NOTICE(NULL,
                "Changed breakpad.content from %d to %d", old_content,
                settings.breakpad.content);
        }

        if (reconfig) {
            initialize_breakpad(&(settings.breakpad));
        }
    }
}

static void dyna_reconfig_ssl_cipher_list(const struct settings *new_settings) {
    if (new_settings->has.ssl_cipher_list) {
        set_ssl_cipher_list(new_settings->ssl_cipher_list);
        cb_free((void*)settings.ssl_cipher_list);
        settings.ssl_cipher_list = new_settings->ssl_cipher_list;
        settings.has.ssl_cipher_list = true;
    }
}

static void dyna_reconfig_ssl_minimum_protocol(const struct settings *ns) {
    if (ns->has.ssl_minimum_protocol) {
        set_ssl_protocol_mask(ns->ssl_minimum_protocol);
        cb_free((void*)settings.ssl_minimum_protocol);
        settings.ssl_minimum_protocol = ns->ssl_minimum_protocol;
        settings.has.ssl_minimum_protocol = true;
    }
}

static void dyna_reconfig_dedupe_nmvb_maps(const struct settings* new_settings) {
    if (new_settings->has.dedupe_nmvb_maps) {
        settings.dedupe_nmvb_maps.store(new_settings->dedupe_nmvb_maps.load());
        settings.has.dedupe_nmvb_maps = true;
    }
}

/* list of handlers for each setting */

struct {
    const char *key;
    config_handler handler;
    dynamic_validate_handler dynamic_validate;
    dynamic_reconfig_handler dyanamic_reconfig;
} handlers[] = {
    { "admin", get_admin, dyna_validate_admin, NULL},
    { "rbac_file", get_rbac_file, dyna_validate_rbac_file, NULL},
    { "rbac_privilege_debug", get_rbac_privilege_debug, dyna_validate_rbac_privilege_debug, dyna_reconfig_rbac_privilege_debug},
    { "audit_file", get_audit_file, dyna_validate_audit_file, NULL},
    { "threads", get_threads, dyna_validate_threads, NULL },
    { "interfaces", get_interfaces, dyna_validate_interfaces, dyna_reconfig_interfaces },
    { "extensions", get_extensions, dyna_validate_extensions, NULL },
    { "engine", get_engine, dyna_validate_engine, NULL },
    { "require_init", get_require_init, dyna_validate_require_init, NULL },
    { "require_sasl", get_require_sasl, dyna_validate_require_sasl, NULL },
    { "default_reqs_per_event", get_default_reqs_per_event,
      dyna_validate_default_reqs_per_event, dyna_reconfig_default_reqs_per_event },
    { "reqs_per_event_high_priority", get_reqs_per_event_high_priority,
      dyna_validate_reqs_per_event_high_priority, dyna_reconfig_reqs_per_event_high_priority },
    { "reqs_per_event_med_priority", get_reqs_per_event_med_priority,
      dyna_validate_reqs_per_event_med_priority, dyna_reconfig_reqs_per_event_med_priority },
    { "reqs_per_event_low_priority", get_reqs_per_event_low_priority,
      dyna_validate_reqs_per_event_low_priority, dyna_reconfig_reqs_per_event_low_priority },
    { "verbosity", get_verbosity, dyna_validate_verbosity, dyna_reconfig_verbosity },
    { "connection_idle_time", get_connection_idle_time,
        dyna_validate_connection_idle_time, dyna_reconfig_connection_idle_time },
    { "bio_drain_buffer_sz", get_bio_drain_sz, dyna_validate_bio_drain_sz, NULL },
    { "datatype_support", get_datatype, dyna_validate_datatype, NULL },
    { "root", get_root, dyna_validate_root, NULL},
    { "ssl_cipher_list", get_ssl_cipher_list, dyna_validate_ssl_cipher_list,
      dyna_reconfig_ssl_cipher_list },
    { "ssl_minimum_protocol",
        get_ssl_minimum_protocol,
        dyna_validate_ssl_minimum_protocol,
        dyna_reconfig_ssl_minimum_protocol },
    { "breakpad", parse_breakpad, dyna_validate_breakpad, dyna_reconfig_breakpad },
    { "max_packet_size", get_max_packet_size, dyna_validate_max_packet_size, NULL},
    { "stdin_listen", get_stdin_listen, dyna_reconfig_stdin_listen, NULL},
    { "exit_on_connection_close", get_exit_on_connection_close, dyna_reconfig_exit_on_connection_close, NULL},
    { "sasl_mechanisms", get_sasl_mechanisms, dyna_reconfig_sasl_mechanisms, NULL},
    { "dedupe_nmvb_maps", get_dedupe_nmvb_maps, dyna_validate_always_true,
        dyna_reconfig_dedupe_nmvb_maps},
    { NULL, NULL, NULL, NULL }
};

/* Parses the specified JSON object, updating settings with all found
 * parameters.
 * @param sys JSON object containing config options.
 * @param settings Settings struct to fill in.
 * @param error_msg pointer to char* which will upon error will be set to a
 *                  string describing why parse failed. If non-null caller is
 *                  responsible for free()ing it.
 * @return true if JSON was successfully parsed, else false.
 */
static bool parse_JSON_config(cJSON* sys, struct settings *s,
                       char **error_msg) {
    s->config = cJSON_PrintUnformatted(sys);

    cJSON *obj = sys->child;
    while (obj) {
        int ii = 0;
        while (handlers[ii].key != NULL) {
            if (strcasecmp(handlers[ii].key, obj->string) == 0) {
                break;
            }
            ++ii;
        }

        if (handlers[ii].key == NULL) {
            LOG_WARNING(NULL, "Unknown token \"%s\" in config ignored.\n",
                        obj->string);
        } else {
            if (!handlers[ii].handler(obj, s, error_msg)) {
                return false;
            }
        }

        obj = obj->next;
    }

    *error_msg = NULL;
    return true;
}

/* Reads the specified file and parses it, filling in settings with parsed
 * settings.
 * @param file path to config file to parse
 * @param settings Settings struct to fill in.
 * @param error_msg pointer to char* which will upon error will be set to a
 *                  string describing why parse failed. If non-null caller is
 *                  responsible for free()ing it.
 * @return true if file was successfully parsed, else false.
 */
static bool parse_config_file(const char* file, struct settings *settings,
                              char** error_msg) {
    cJSON *sys;
    bool result;
    config_error_t err = config_load_file(file, &sys);

    if (err != CONFIG_SUCCESS) {
        *error_msg = config_strerror(file, err);
        return false;
    }

    result = parse_JSON_config(sys, settings, error_msg);
    cJSON_Delete(sys);
    return result;
}

/******************************************************************************
 * Public functions
 *****************************************************************************/

void load_config_file(const char *file, struct settings *settings)
{
    char* error_msg = NULL;
    if (!parse_config_file(file, settings, &error_msg)) {
        fprintf(stderr, "%s\nTerminating\n", error_msg);
        free(error_msg);
        exit(EXIT_FAILURE);
    }
}

bool validate_proposed_config_changes(const char* new_cfg, cJSON* errors) {
    bool valid;
    struct settings new_settings = {0};
    char *error_msg = NULL;
    cJSON *config = cJSON_Parse(new_cfg);
    if (config == NULL) {
        cJSON_AddItemToArray(errors, cJSON_CreateString("JSON parse error"));
        return false;
    }

    if ((valid = parse_JSON_config(config, &new_settings, &error_msg))) {
        int i = 0;
        while (handlers[i].key != NULL) {
            valid &= handlers[i].dynamic_validate(&new_settings, errors);
            i++;
        }
    } else {
        cJSON_AddItemToArray(errors, cJSON_CreateString(error_msg));
        free(error_msg);
    }

    /* cleanup */
    free_settings(&new_settings);
    cJSON_Delete(config);
    return valid;
}

void reload_config_file(void) {
    struct settings new_settings = {0};
    char* error_msg;
    int ii;
    bool valid = true;

    LOG_NOTICE(NULL, "Reloading config file %s", get_config_file());

    /* parse config into a new settings structure */
    if (!parse_config_file(get_config_file(), &new_settings, &error_msg)) {
        free_settings(&new_settings);
        LOG_WARNING(NULL, "Failed to reload config file %s : %s\n",
                    get_config_file(), error_msg);
        free(error_msg);
        return;
    }

    /* Validate */
    cJSON* errors = cJSON_CreateArray();
    for (ii = 0; handlers[ii].key != NULL; ii++) {
        valid &= handlers[ii].dynamic_validate(&new_settings, errors);
    }

    if (valid) {
        /* for all dynamic options, apply any differences to the running config. */
        for (ii = 0; handlers[ii].key != NULL; ii++) {
            if (handlers[ii].dyanamic_reconfig != NULL) {
                handlers[ii].dyanamic_reconfig(&new_settings);
            }
        }
    } else {
        LOG_WARNING(NULL,
                    "Validation failed while reloading config file '%s'. Errors:",
                    get_config_file());
        for (ii = 0; ii < cJSON_GetArraySize(errors); ii++) {
            char* json = cJSON_Print(cJSON_GetArrayItem(errors, ii));
            LOG_WARNING(NULL, "\t%s", json);
            cb_free(json);
        }
    }
    free_settings(&new_settings);
    cJSON_Delete(errors);
}

/* Frees all dynamic memory associated with the given settings struct */
void free_settings(struct settings* s) {
    int ii;
    cb_free((char*)s->admin);
    for (ii = 0; ii < s->num_interfaces; ii++) {
        cb_free((char*)s->interfaces[ii].host);
        cb_free((char*)s->interfaces[ii].ssl.key);
        cb_free((char*)s->interfaces[ii].ssl.cert);
    }
    cb_free(s->interfaces);
    for (ii = 0; ii < s->num_pending_extensions; ii++) {
        cb_free((char*)s->pending_extensions[ii].soname);
        cb_free((char*)s->pending_extensions[ii].config);
    }
    cb_free(s->pending_extensions);
    cb_free((char*)s->rbac_file);
    cb_free((char*)s->config);
    cb_free((char*)s->root);
    cb_free((char*)s->breakpad.minidump_dir);
    cb_free((char*)s->ssl_cipher_list);
    cb_free((char*)s->ssl_minimum_protocol);
    cb_free((char*)s->audit_file);
    cb_free((char*)s->sasl_mechanisms);
}
