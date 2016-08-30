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

/******************************************************************************
 * 'whynotboth' memory tracking support.
 *
 * Allows tracking memory for /two/ memory allocation libraries.
 *
 * Intent is to assist in migrating between implicit C memory tracking - i.e.
 * relying on chosen memory allocator to hook all application calls to
 * malloc/realloc/free etc, and explicit tracking - where the application
 * explicitly calls our own cb_XXX alloc functions.
 *
 *****************************************************************************/

#include "config.h"

#include "alloc_hooks_whynotboth.h"
#include <cstring>
#include <stdbool.h>

#include <platform/cb_malloc.h>

#include "alloc_hooks_tcmalloc.h"
#include "alloc_hooks_jemalloc.h"

/* Irrespective of how jemalloc was configured on this platform,
* don't rename je_FOO to FOO.
*/
#define JEMALLOC_NO_RENAME
#include <jemalloc/jemalloc.h>


void WhyNotBothHooks::initialize() {
    fprintf(stderr, "Installing whynotboth memory allocation API\n");
}

bool WhyNotBothHooks::add_new_hook(malloc_new_hook_t f) {
    // Install hook in both TCMalloc and our own cbmalloc callback.
    cb_add_new_hook((cb_malloc_new_hook_t)f);
    return TCMallocHooks::add_new_hook(f);
}

bool WhyNotBothHooks::remove_new_hook(malloc_new_hook_t f) {
    cb_remove_new_hook((cb_malloc_new_hook_t)f);
    return TCMallocHooks::remove_new_hook(f);
}

bool WhyNotBothHooks::add_delete_hook(malloc_delete_hook_t f) {
    // Install hook in both TCMalloc and our own callback.
    cb_add_delete_hook((cb_malloc_delete_hook_t)f);
    return TCMallocHooks::add_delete_hook(f);
}

bool WhyNotBothHooks::remove_delete_hook(malloc_delete_hook_t f) {
    cb_remove_delete_hook((cb_malloc_delete_hook_t)f);
    return TCMallocHooks::remove_delete_hook(f);
}

int WhyNotBothHooks::get_extra_stats_size() {
    return 0;
}

void WhyNotBothHooks::get_allocator_stats(allocator_stats* stats) {
}

size_t WhyNotBothHooks::get_allocation_size(const void* ptr) {
    // TCMalloc supports checking if it owns an allocation; so check with
    // it first.
    size_t result = TCMallocHooks::get_allocation_size(ptr);
    if (result != 0) {
        return result;
    }

    return JemallocHooks::get_allocation_size(ptr);
}

void WhyNotBothHooks::get_detailed_stats(char* buffer, int size) {
    // Return stats for both allocators.
    // Unfortunately TCMalloc doesn't give any indication on how much data
    // it wrote to the buffer, so we need to zero-fill it before calling
    // and then append after we find the first zero byte.
    std::memset(buffer, 0, size);
    TCMallocHooks::get_detailed_stats(buffer, size);

    int tcmalloc_len = strlen(buffer);
    fprintf(stderr, "TCMalloc wrote:%d out of %d\n", tcmalloc_len, size);
    if (size - tcmalloc_len > 0) {
        JemallocHooks::get_detailed_stats(buffer+tcmalloc_len, size - tcmalloc_len);
    }
}

void WhyNotBothHooks::release_free_memory() {
    // empty
}

bool WhyNotBothHooks::enable_thread_cache(bool enable) {
    // Only supported for jemalloc.
    return JemallocHooks::enable_thread_cache(enable);
}

bool WhyNotBothHooks::get_allocator_property(const char* name, size_t* value) {
    // Try TCMalloc first, if that fails try jemalloc.
    if (TCMallocHooks::get_allocator_property(name, value)) {
        return true;
    }
    return JemallocHooks::get_allocator_property(name, value);
}

bool WhyNotBothHooks::set_allocator_property(const char* name, size_t value) {
    // Try TCMalloc first, if that fails try jemalloc.
    if (TCMallocHooks::set_allocator_property(name, value)) {
        return true;
    }
    return JemallocHooks::set_allocator_property(name, value);
}
