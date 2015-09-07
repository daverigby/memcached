/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc
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
 * Simple shared library which can be used to interpose malloc and
 * inject failures.
 */

#include "config.h"

#include "alloc_hooks.h"

#include <platform/backtrace.h>
#include <platform/visibility.h>

#include <dlfcn.h>
#include <memory>
#include <random>

static int recursion_depth = 0;

struct BadMalloc {
    BadMalloc()
        : gen(rd())
    {
        float failure_ratio = 0.01;
        char* env = getenv("BADMALLOC_FAILURE_RATIO");
        if (env != nullptr) {
            try {
                failure_ratio = std::stof(env);
            } catch (std::exception& e) {
                fprintf(stderr, "badmalloc: Error parsing BADMALLOC_FAILURE_RATIO: %s\n",
                        e.what());
            }
        }
        distribution.reset(new std::bernoulli_distribution(failure_ratio));

        fprintf(stderr, "badmalloc: Loaded. Using failure liklihood of %f\n",
                distribution->p());
    }

    bool shouldFail() {
        // Allow the first N operations to always succeed (initialization, etc).
        static int grace_period = 1000;
        if (grace_period > 0) {
            grace_period--;
            return false;
        }
        // Also don't fail if we have been recursively been called.
        if (recursion_depth > 1) {
            return false;
        }
        return distribution->operator()(gen);
    }

    std::random_device rd;
    std::mt19937 gen;
    std::unique_ptr<std::bernoulli_distribution> distribution;
};

std::unique_ptr<BadMalloc> badMalloc;

typedef void* (*malloc_t)(size_t);
typedef void* (*realloc_t)(void*, size_t);

/* Create BadMalloc (and hence start returning malloc failures only when
 * init_alloc_hooks() is called - this ensures that anything pre-main()
 * - C++ static initialization is all completed successfully.
 */
void init_alloc_hooks() {
    badMalloc.reset(new BadMalloc());
}

extern "C" {

/* Exported malloc functions */
EXPORT_SYMBOL void* malloc(size_t size) throw() {
    static malloc_t real_malloc = (malloc_t)dlsym(RTLD_NEXT, "malloc");

    recursion_depth++;
    if (badMalloc != nullptr && badMalloc->shouldFail()) {
        fprintf(stderr, "badmalloc: Failing malloc of size %" PRIu64 "\n",
                size);
        print_backtrace_to_file(stderr);
        recursion_depth--;
        return nullptr;
    }
    recursion_depth--;
    return real_malloc(size);
}

EXPORT_SYMBOL void* realloc(void* ptr, size_t size) throw() {
    static realloc_t real_realloc = (realloc_t)dlsym(RTLD_NEXT, "realloc");

    recursion_depth++;
    if (badMalloc != nullptr && badMalloc->shouldFail()) {
        fprintf(stderr, "badmalloc: Failing realloc of size %" PRIu64 "\n",
                size);
        print_backtrace_to_file(stderr);
        recursion_depth--;
        return nullptr;
    }
    recursion_depth--;
    return real_realloc(ptr, size);
}

} // extern "C"

/*
 * Various alloc hooks. None of these are actually used in badmalloc.
*/
bool mc_add_new_hook(void (* hook)(const void* ptr, size_t size)) {
    return false;
}

bool mc_remove_new_hook(void (* hook)(const void* ptr, size_t size)) {
    return false;
}

bool mc_add_delete_hook(void (* hook)(const void* ptr)) {
    return false;
}

bool mc_remove_delete_hook(void (* hook)(const void* ptr)) {
    return false;
}

int mc_get_extra_stats_size() {
    return 0;
}

void mc_get_allocator_stats(allocator_stats* stats) {
}

size_t mc_get_allocation_size(const void* ptr) {
    return 0;
}

void mc_get_detailed_stats(char* buffer, int size) {
    // empty
}

void mc_release_free_memory() {
    // empty
}

bool mc_enable_thread_cache(bool enable) {
    return true;
}

bool mc_get_allocator_property(const char* name, size_t* value) {
    return false;
}

bool mc_set_allocator_property(const char* name, size_t value) {
    return false;
}
