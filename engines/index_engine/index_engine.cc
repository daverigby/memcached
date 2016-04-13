/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc
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

/*
 *                "index_engine"
*/

#include <atomic>
#include <condition_variable>
#include <cstring>
#include <map>
#include <memory>
#include <mutex>
#include <queue>
#include <random>
#include <sstream>
#include <string>

#include <memcached/engine.h>
#include "utilities/engine_loader.h"


/* Public API declaration ****************************************************/

extern "C" {
    MEMCACHED_PUBLIC_API
    ENGINE_ERROR_CODE create_instance(uint64_t interface, GET_SERVER_API gsa,
                                      ENGINE_HANDLE **handle);

    MEMCACHED_PUBLIC_API
    void destroy_engine(void);
}

/** ewouldblock_engine class */
class Index_Engine : public ENGINE_HANDLE_V1 {

public:
    Index_Engine(GET_SERVER_API gsa_);

    ~Index_Engine();

    // Convert from a handle back to the read object.
    static Index_Engine* to_engine(ENGINE_HANDLE* handle) {
        return reinterpret_cast<Index_Engine*> (handle);
    }

    /* Implementation of all the engine functions. ***************************/

    static const engine_info* get_info(ENGINE_HANDLE* handle) {
        return &to_engine(handle)->info.eng_info;
    }

    static ENGINE_ERROR_CODE initialize(ENGINE_HANDLE* handle,
                                        const char* config_str) {
        Index_Engine* ixe = to_engine(handle);
        // TODO: parse the config string.

        return ENGINE_SUCCESS;
    }

    static void destroy(ENGINE_HANDLE* handle, const bool force) {
        Index_Engine* ixe = to_engine(handle);
        delete ixe;
    }

    static ENGINE_ERROR_CODE allocate(ENGINE_HANDLE* handle, const void* cookie,
                                      item **item, const void* key,
                                      const size_t nkey, const size_t nbytes,
                                      const int flags, const rel_time_t exptime,
                                      uint8_t datatype) {
        Index_Engine* ixe = to_engine(handle);
        abort();

    }

    static ENGINE_ERROR_CODE remove(ENGINE_HANDLE* handle, const void* cookie,
                                    const void* key, const size_t nkey,
                                    uint64_t* cas, uint16_t vbucket,
                                    mutation_descr_t* mut_info) {
        Index_Engine* ixe = to_engine(handle);
        abort();

    }

    static void release(ENGINE_HANDLE* handle, const void *cookie, item* item) {
        Index_Engine* ixe = to_engine(handle);
        abort();
    }

    static ENGINE_ERROR_CODE get(ENGINE_HANDLE* handle, const void* cookie,
                                 item** item, const void* key, const int nkey,
                                 uint16_t vbucket) {
        Index_Engine* ixe = to_engine(handle);
        abort();
    }

    static ENGINE_ERROR_CODE store(ENGINE_HANDLE* handle, const void *cookie,
                                   item* item, uint64_t *cas,
                                   ENGINE_STORE_OPERATION operation,
                                   uint16_t vbucket) {
        Index_Engine* ixe = to_engine(handle);
        abort();

    }

    static ENGINE_ERROR_CODE arithmetic(ENGINE_HANDLE* handle,
                                        const void* cookie, const void* key,
                                        const int nkey, const bool increment,
                                        const bool create, const uint64_t delta,
                                        const uint64_t initial,
                                        const rel_time_t exptime, item **item,
                                        uint8_t datatype, uint64_t *result,
                                        uint16_t vbucket) {
        Index_Engine* ixe = to_engine(handle);
        abort();
    }

    static ENGINE_ERROR_CODE flush(ENGINE_HANDLE* handle, const void* cookie,
                                   time_t when) {
        abort();
    }

    static ENGINE_ERROR_CODE get_stats(ENGINE_HANDLE* handle,
                                       const void* cookie, const char* stat_key,
                                       int nkey, ADD_STAT add_stat) {
        abort();
    }

    static void reset_stats(ENGINE_HANDLE* handle, const void* cookie) {
        abort();
    }

    static void* get_stats_struct(ENGINE_HANDLE* handle, const void* cookie) {
        abort();
    }

    /* Handle 'unknown_command'. In additional to wrapping calls to the
     * underlying real engine, this is also used to configure
     * ewouldblock_engine itself using he CMD_EWOULDBLOCK_CTL opcode.
     */
    static ENGINE_ERROR_CODE unknown_command(ENGINE_HANDLE* handle,
                                             const void* cookie,
                                             protocol_binary_request_header *request,
                                             ADD_RESPONSE response) {
        abort();
    }

    static void item_set_cas(ENGINE_HANDLE *handle, const void* cookie,
                             item* item, uint64_t cas) {
        abort();
    }

    static bool get_item_info(ENGINE_HANDLE *handle, const void *cookie,
                              const item* item, item_info *item_info) {
        abort();
    }
    static bool set_item_info(ENGINE_HANDLE *handle, const void *cookie,
                              item* item, const item_info *item_info) {
        abort();
    }

    static ENGINE_ERROR_CODE get_engine_vb_map(ENGINE_HANDLE* handle,
                                               const void * cookie,
                                               engine_get_vb_map_cb callback) {
        abort();
    }

    GET_SERVER_API gsa;
    union {
        engine_info eng_info;
        char buffer[sizeof(engine_info) +
                    (sizeof(feature_info) * LAST_REGISTERED_ENGINE_FEATURE)];
    } info;

private:

};


Index_Engine::Index_Engine(GET_SERVER_API gsa_)
  : gsa(gsa_)
{
    interface.interface = 1;
    ENGINE_HANDLE_V1::get_info = get_info;
    ENGINE_HANDLE_V1::initialize = initialize;
    ENGINE_HANDLE_V1::destroy = destroy;
    ENGINE_HANDLE_V1::allocate = allocate;
    ENGINE_HANDLE_V1::remove = remove;
    ENGINE_HANDLE_V1::release = release;
    ENGINE_HANDLE_V1::get = get;
    ENGINE_HANDLE_V1::store = store;
    ENGINE_HANDLE_V1::arithmetic = arithmetic;
    ENGINE_HANDLE_V1::flush = flush;
    ENGINE_HANDLE_V1::get_stats = get_stats;
    ENGINE_HANDLE_V1::reset_stats = reset_stats;
    ENGINE_HANDLE_V1::aggregate_stats = NULL;
    ENGINE_HANDLE_V1::unknown_command = unknown_command;
    ENGINE_HANDLE_V1::tap_notify = NULL;
    ENGINE_HANDLE_V1::get_tap_iterator = NULL;
    ENGINE_HANDLE_V1::item_set_cas = item_set_cas;
    ENGINE_HANDLE_V1::get_item_info = get_item_info;
    ENGINE_HANDLE_V1::set_item_info = set_item_info;
    ENGINE_HANDLE_V1::get_engine_vb_map = get_engine_vb_map;
    ENGINE_HANDLE_V1::get_stats_struct = NULL;
    ENGINE_HANDLE_V1::set_log_level = NULL;

    std::memset(&info, 0, sizeof(info.buffer));
    info.eng_info.description = "Index Engine";
}

Index_Engine::~Index_Engine() {

}

ENGINE_ERROR_CODE create_instance(uint64_t interface,
                                  GET_SERVER_API gsa,
                                  ENGINE_HANDLE **handle)
{
    if (interface != 1) {
        return ENGINE_ENOTSUP;
    }

    try {
        Index_Engine* engine = new Index_Engine(gsa);
        *handle = reinterpret_cast<ENGINE_HANDLE*> (engine);
        return ENGINE_SUCCESS;

    } catch (std::exception& e) {
        auto logger = gsa()->log->get_logger();
        logger->log(EXTENSION_LOG_WARNING, NULL,
                    "Index_Engine: failed to create engine: %s", e.what());
        return ENGINE_FAILED;
    }

}

void destroy_engine(void) {
    // nothing todo.
}
