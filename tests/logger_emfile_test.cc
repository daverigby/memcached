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

    // Open the logger
    EXTENSION_ERROR_CODE ret = memcached_extensions_initialize(
            "unit_test=true;loglevel=warning;cyclesize=200;"
            "buffersize=100;sleeptime=1;filename=log_test_emfile",
            get_server_api);
    cb_assert(ret == EXTENSION_SUCCESS);

    // Wait for first log file to be created, and open it
    FILE* initial_log_file;
    while ((initial_log_file = std::fopen("log_test_emfile.0.txt", "r")) == NULL) {
        usleep(10);
    }

    // Consume all available FD so we cannot open any more files
    // (i.e. rotation will fail).
    std::vector<FILE*> FDs;
    FILE* file;
    while ((file = std::fopen(".", "r")) != NULL) {
        FDs.emplace_back(file);
    }

    // Repeatedly print lines, waiting for the rotation failure message to show up.
    char* line_ptr = NULL;
    size_t line_sz = 0;
    while (true) {
        logger->log(EXTENSION_LOG_DETAIL, NULL,
                   "test_emfile: Log line which should be in log_test_emfile.0.log");

        if (getline(&line_ptr, &line_sz, initial_log_file) > 0) {
            fprintf(stderr, "Got line: %s", line_ptr);
            fflush(stderr);
            if (strstr(line_ptr, "Failed to open next logfile") != NULL) {
                break;
            } else {
                perror("getline() failed:");
            }
        }
        usleep(10);
    }
    free(line_ptr);

    // Close extra FDs so we can now print.
    for (auto f : FDs) {
        std::fclose(f);
    }

    // Print one more line to cause log rotation to occur.
    logger->log(EXTENSION_LOG_DETAIL, NULL,
                "test_emfile: Should log to to file now FDs are available.");

    logger->shutdown(false);
}
