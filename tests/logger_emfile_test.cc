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
 * Test how logger handles running out of file descriptors (EMFILE)
 */

#include "logger_test_common.h"

#include <cstdio>
#include <cstdlib>
#include <unistd.h>
#include <platform/cbassert.h>
#include <platform/dirutils.h>
#include <sys/resource.h>

int main() {

    // Clean out any old files.
    std::vector<std::string> files;
    files = CouchbaseDirectoryUtilities::findFilesWithPrefix("log_test_emfile");
    if (!files.empty()) {
        remove_files(files);
    }

    // Bring down out open file limit to a more conservative level (to
    // save using up a huge number of user / system FDs).
    struct rlimit rlim;
    if (getrlimit(RLIMIT_NOFILE, &rlim) != 0) {
        fprintf(stderr, "Failed to get getrlimit number of files\n");
        exit(1);
    }

    rlim.rlim_cur = 100;
    if (setrlimit(RLIMIT_NOFILE, &rlim) != 0) {
        fprintf(stderr, "Failed to setrlimit number of files\n");
        exit(2);
    }

    // Consume all available FD so we cannot open any more files.
    std::vector<FILE*> FDs;
    FILE* file;
    while ((file = std::fopen(".", "r")) != NULL) {
        FDs.emplace_back(file);
    }

    // Open the logger
    EXTENSION_ERROR_CODE ret = memcached_extensions_initialize(
            "unit_test=true;loglevel=warning;cyclesize=128;"
            "buffersize=128;sleeptime=1;filename=log_test_emfile",
            get_server_api);
    cb_assert(ret == EXTENSION_SUCCESS);

    // Print at least two lines to cause log rotation to occur.
    for (int ii = 0; ii < 2; ii++) {
        logger->log(EXTENSION_LOG_DETAIL, NULL,
                    "test_emfile: Should not be logged as we have run out of FDs");
    }

    // Close extra FDs so we can now print.
    for (auto f : FDs) {
        std::fclose(f);
    }

    // Print one more line to cause log rotation to occur.
    logger->log(EXTENSION_LOG_DETAIL, NULL,
                "test_emfile: Should log to to file now FDs are available.");

    logger->shutdown(false);
}
