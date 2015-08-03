/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc.
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
#include "config.h"

#include "mcbp_test.h"
#include "utilities/subdoc_encoder.h"

#include <memcached/protocol_binary.h>
#include <algorithm>
#include <vector>

/*
 * Sub-document API validator tests
 */

namespace BinaryProtocolValidator {

class SubdocMultiLookupTest : public ValidatorTest {
    virtual void SetUp() override {
        ValidatorTest::SetUp();

        // Setup basic, correct header.
        request.key = "multi_lookup";
        request.specs.push_back({protocol_binary_command(PROTOCOL_BINARY_CMD_SUBDOC_EXISTS),
                                 protocol_binary_subdoc_flag(0), "[0]"});
    }

protected:
    int validate(const std::vector<char>& request) {
        void* packet = const_cast<void*>
            (static_cast<const void*>(request.data()));
        return ValidatorTest::validate(PROTOCOL_BINARY_CMD_SUBDOC_MULTI_LOOKUP,
                                       packet);
    }

    SubdocMultiLookupCmd request;
};

TEST_F(SubdocMultiLookupTest, Baseline) {
    // Ensure that the initial request as formed by SetUp is valid.
    EXPECT_EQ(0, validate(request.encode()));
}

TEST_F(SubdocMultiLookupTest, InvalidMagic) {
    std::vector<char> payload = request.encode();
    auto* header = reinterpret_cast<protocol_binary_request_header*>
        (payload.data());
    header->request.magic = 0;
    EXPECT_EQ(-1, validate(payload));
}

TEST_F(SubdocMultiLookupTest, InvalidDatatype) {
    std::vector<char> payload = request.encode();
    auto* header = reinterpret_cast<protocol_binary_request_header*>
        (payload.data());
    header->request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(-1, validate(payload));
    header->request.datatype = PROTOCOL_BINARY_DATATYPE_COMPRESSED_JSON;
    EXPECT_EQ(-1, validate(payload));
    header->request.datatype = PROTOCOL_BINARY_DATATYPE_COMPRESSED;
    EXPECT_EQ(-1, validate(payload));
}

TEST_F(SubdocMultiLookupTest, InvalidKey) {
    request.key = "";
    EXPECT_EQ(-1, validate(request.encode()));
}

TEST_F(SubdocMultiLookupTest, InvalidExtras) {
    std::vector<char> payload = request.encode();
    auto* header = reinterpret_cast<protocol_binary_request_header*>
        (payload.data());
    header->request.extlen = 1;
    EXPECT_EQ(-1, validate(payload));
}


TEST_F(SubdocMultiLookupTest, NumPaths) {
    // Need at least one path.
    request.specs.clear();
    EXPECT_EQ(-1, validate(request.encode()));

    // Should handle total of 16 paths.
    request.specs.clear();
    // Add maximum number of paths.
    SubdocMultiLookupCmd::LookupSpec spec{protocol_binary_command(PROTOCOL_BINARY_CMD_SUBDOC_EXISTS),
                                          protocol_binary_subdoc_flag(0),
                                          "[0]"};
    for (unsigned int i = 0; i<16; i++) {
        request.specs.push_back(spec);
    }
    EXPECT_EQ(0, validate(request.encode()));

    // Add one more - should now fail.
    request.specs.push_back(spec);
    EXPECT_EQ(-1, validate(request.encode()));
}

TEST_F(SubdocMultiLookupTest, ValidLocationOpcodes) {
    // Check that GET is supported.
    request.specs.clear();
    request.specs.push_back({protocol_binary_command(PROTOCOL_BINARY_CMD_SUBDOC_GET),
                             protocol_binary_subdoc_flag(0),
                             "[0]"});
    EXPECT_EQ(0, validate(request.encode()));
}

TEST_F(SubdocMultiLookupTest, InvalidLocationOpcodes) {
    // Check that mutation opcodes are not.
    request.specs.push_back({protocol_binary_command(PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_ADD_UNIQUE),
                             protocol_binary_subdoc_flag(0),
                             "[0]"});
    EXPECT_EQ(-1, validate(request.encode()));

    request.specs[1].opcode = protocol_binary_command(PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_INSERT);
    EXPECT_EQ(-1, validate(request.encode()));

    request.specs[1].opcode = protocol_binary_command(PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_FIRST);
    EXPECT_EQ(-1, validate(request.encode()));

    request.specs[1].opcode = protocol_binary_command(PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_LAST);
    EXPECT_EQ(-1, validate(request.encode()));

    request.specs[1].opcode = protocol_binary_command(PROTOCOL_BINARY_CMD_SUBDOC_COUNTER);
    EXPECT_EQ(-1, validate(request.encode()));

    request.specs[1].opcode = protocol_binary_command(PROTOCOL_BINARY_CMD_SUBDOC_DELETE);
    EXPECT_EQ(-1, validate(request.encode()));

    request.specs[1].opcode = protocol_binary_command(PROTOCOL_BINARY_CMD_SUBDOC_DICT_ADD);
    EXPECT_EQ(-1, validate(request.encode()));

    request.specs[1].opcode = protocol_binary_command(PROTOCOL_BINARY_CMD_SUBDOC_DICT_UPSERT);
    EXPECT_EQ(-1, validate(request.encode()));

    request.specs[1].opcode = protocol_binary_command(PROTOCOL_BINARY_CMD_SUBDOC_REPLACE);
    EXPECT_EQ(-1, validate(request.encode()));

    // Yo dawg, can't have a multi in your multi...
    request.specs[1].opcode = protocol_binary_command(PROTOCOL_BINARY_CMD_SUBDOC_MULTI_LOOKUP);
    EXPECT_EQ(-1, validate(request.encode()));
    request.specs[1].opcode = protocol_binary_command(PROTOCOL_BINARY_CMD_SUBDOC_MULTI_MUTATION);
    EXPECT_EQ(-1, validate(request.encode()));
}

TEST_F(SubdocMultiLookupTest, InvalidLocationPaths) {
    // Path must not be zero length.
    request.specs[0].path.clear();
    EXPECT_EQ(-1, validate(request.encode()));

    // Maximum length should be accepted...
    request.specs[0].path.assign(1024, 'x');
    EXPECT_EQ(0, validate(request.encode()));

    // But any longer should be rejected.
    request.specs[0].path.push_back('x');
    EXPECT_EQ(-1, validate(request.encode()));
}

TEST_F(SubdocMultiLookupTest, InvalidLocationFlags) {
    // Both GET and EXISTS do not accept any flags.
    request.specs[0].opcode = PROTOCOL_BINARY_CMD_SUBDOC_EXISTS;
    request.specs[0].flags = SUBDOC_FLAG_MKDIR_P;
    EXPECT_EQ(-1, validate(request.encode()));

    request.specs[0].opcode = PROTOCOL_BINARY_CMD_SUBDOC_GET;
    EXPECT_EQ(-1, validate(request.encode()));
}

} // namespace BinaryProtocolValidator
