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
#pragma once

#include "config.h"

#include <cJSON.h>
#include <cJSON_utils.h>
#include <cstdlib>
#include <engines/ewouldblock_engine/ewouldblock_engine.h>
#include <libgreenstack/Greenstack.h>
#include <memcached/openssl.h>
#include <memcached/protocol_binary.h>
#include <memcached/types.h>
#include <stdexcept>
#include <string>
#include <vector>

enum class Protocol : uint8_t {
    Memcached,
    Greenstack
};

/**
 * The Frame class is used to represent all of the data included in the
 * protocol unit going over the wire. For the memcached binary protocol
 * this is either the full request or response as defined in
 * memcached/protocol_binary.h, and for greenstack this is the greenstack
 * frame as defined in libreenstack/Frame.h
 */
class Frame {
public:
    void reset() {
        payload.resize(0);
    }

    std::vector<uint8_t> payload;
    typedef std::vector<uint8_t>::size_type size_type;
};

class DocumentInfo {
public:
    std::string id;
    uint32_t flags;
    std::string expiration;
    Greenstack::Compression compression;
    Greenstack::Datatype datatype;
    uint64_t cas;
};

class Document {
public:
    DocumentInfo info;
    std::vector<uint8_t> value;
};

class MutationInfo {
public:
    uint64_t cas;
    size_t size;
    uint64_t seqno;
    uint64_t vbucketuuid;
};

class ConnectionError : public std::runtime_error {
public:
    explicit ConnectionError(const char* what_arg, const Protocol& protocol_,
                             uint16_t reason_)
        : std::runtime_error(what_arg),
          protocol(protocol_),
          reason(reason_) {

    }

    explicit ConnectionError(const std::string what_arg,
                             const Protocol& protocol_, uint16_t reason_)
        : std::runtime_error(what_arg),
          protocol(protocol_),
          reason(reason_) {

    }

#ifdef WIN32
#define NOEXCEPT
#else
#define NOEXCEPT noexcept
#endif

    virtual const char* what() const NOEXCEPT override {
        std::string msg(std::runtime_error::what());
        msg.append(" reason:");
        msg.append(std::to_string(reason));
        return msg.c_str();
    }

    uint16_t getReason() const {
        return reason;
    }

    Protocol getProtocol() const {
        return protocol;
    }

    bool isInvalidArguments() const {
        if (protocol == Protocol::Memcached) {
            return reason == PROTOCOL_BINARY_RESPONSE_EINVAL;
        } else {
            return reason == uint16_t(Greenstack::Status::InvalidArguments);
        }
    }

    bool isAlreadyExists() const {
        if (protocol == Protocol::Memcached) {
            return reason == PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS;
        } else {
            return reason == uint16_t(Greenstack::Status::AlreadyExists);
        }
    }

    bool isNotFound() const {
        if (protocol == Protocol::Memcached) {
            return reason == PROTOCOL_BINARY_RESPONSE_KEY_ENOENT;
        } else {
            return reason == uint16_t(Greenstack::Status::NotFound);
        }
    }

    bool isNotStored() const {
        if (protocol == Protocol::Memcached) {
            return reason == PROTOCOL_BINARY_RESPONSE_NOT_STORED;
        } else {
            return reason == uint16_t(Greenstack::Status::NotStored);
        }
    }

    bool isAccessDenied() const {
        if (protocol == Protocol::Memcached) {
#ifdef USE_EXTENDED_ERROR_CODES
            return reason == PROTOCOL_BINARY_RESPONSE_EACCESS;
#else
            return false;
#endif
        } else {
            return reason == uint16_t(Greenstack::Status::NoAccess);
        }
    }

private:
    Protocol protocol;
    uint16_t reason;
};

/**
 * The MemcachedConnection class is an abstract class representing a
 * connection to memcached. The concrete implementations of the class
 * implements the Memcached binary protocol and Greenstack.
 *
 * By default a connection is set into a synchronous mode.
 *
 * All methods is expeted to work, and all failures is reported through
 * exceptions. Unexpected packets / responses etc will use the ConnectionError,
 * and other problems (like network error etc) use std::runtime_error.
 *
 */
class MemcachedConnection {
public:
    MemcachedConnection() = delete;

    MemcachedConnection(const MemcachedConnection&) = delete;

    virtual ~MemcachedConnection();

    // Creates clone (copy) of the given connection - i.e. a second independent
    // channel to memcached. Used for multi-connection testing.
    virtual std::unique_ptr<MemcachedConnection> clone() = 0;

    in_port_t getPort() const {
        return port;
    }

    sa_family_t getFamily() const {
        return family;
    }

    bool isSsl() const {
        return ssl;
    }

    const Protocol& getProtocol() const {
        return protocol;
    }

    bool isSynchronous() const {
        return synchronous;
    }

    virtual void setSynchronous(bool enable) {
        if (!enable) {
            throw ConnectionError("Not implemented", Protocol::Memcached,
                                  PROTOCOL_BINARY_RESPONSE_EINVAL);
        }
    }

    /**
     * Perform a SASL authentication to memcached
     *
     * @param username the username to use in authentication
     * @param password the password to use in authentication
     * @param mech the SASL mech to use
     */
    virtual void authenticate(const std::string& username,
                              const std::string& password,
                              const std::string& mech) = 0;

    /**
     * Create a bucket
     *
     * @param name the name of the bucket
     * @param config the buckets configuration attributes
     * @param type the kind of bucket to create
     */
    virtual void createBucket(const std::string& name,
                              const std::string& config,
                              const Greenstack::BucketType& type) = 0;

    /**
     * Delete the named bucket
     *
     * @param name the name of the bucket
     */
    virtual void deleteBucket(const std::string& name) = 0;

    /**
     * Select the named bucket
     *
     * @param name the name of the bucket to select
     */
    virtual void selectBucket(const std::string& name) = 0;

    /**
     * List all of the buckets on the server
     *
     * @return a vector containing all of the buckets
     */
    virtual std::vector<std::string> listBuckets() = 0;

    /**
     * Fetch a document from the server
     *
     * @param id the name of the document
     * @param vbucket the vbucket the document resides in
     * @return a document object containg the information about the
     *         document.
     */
    virtual Document get(const std::string& id, uint16_t vbucket) = 0;

    /*
     * Form a Frame representing a CMD_GET
     */
    virtual Frame encodeCmdGet(const std::string& id, uint16_t vbucket) = 0;

    /*
     * Form a Frame representing a CMD_TAP_CONNECT
     */
    virtual Frame encode_cmd_tap_connect() = 0;

    /*
     * Form a Frame representing a CMD_DCP_OPEN
     */
    virtual Frame encode_cmd_dcp_open() = 0;

    /*
     * Form a Frame representing a CMD_DCP_STREAM_REQ
     */
    virtual Frame encode_cmd_dcp_stream_req() = 0;

    /**
     * Perform the mutation on the attached document.
     *
     * The method throws an exception upon errors
     *
     * @param doc the document to mutate
     * @param vbucket the vbucket to operate on
     * @param type the type of mutation to perform
     * @return the new cas value for success
     */
    virtual MutationInfo mutate(const Document& doc, uint16_t vbucket,
                                const Greenstack::mutation_type_t type) = 0;


    virtual unique_cJSON_ptr stats(const std::string& subcommand) = 0;

    /**
     * Sent the given frame over this connection
     *
     * @param frame the frame to send to the server
     */
    virtual void sendFrame(const Frame& frame);

    /** Send part of the given frame over this connection. Upon success,
     * the frame's payload will be modified such that the sent bytes are
     * deleted - i.e. after a successful call the frame object will only have
     * the remaining, unsent bytes left.
     *
     * @param frame The frame to partially send.
     * @param length The number of bytes to transmit. Must be less than or
     *               equal to the size of the frame.
     */
    void sendPartialFrame(Frame& frame, Frame::size_type length);

    /**
     * Receive the next frame on the connection
     *
     * @param frame the frame object to populate with the next frame
     */
    virtual void recvFrame(Frame& frame) = 0;

    /**
     * Get a textual representation of this connection
     *
     * @return a textual representation of the connection including the
     *         protocol and any special attributes
     */
    virtual std::string to_string() = 0;

    void reconnect();

    /**
     * Try to configure the ewouldblock engine
     *
     * See the header /engines/ewouldblock_engine/ewouldblock_engine.h
     * for a full description on the parameters.
     */
    virtual void configureEwouldBlockEngine(const EWBEngineMode& mode,
                                            ENGINE_ERROR_CODE err_code = ENGINE_EWOULDBLOCK,
                                            uint32_t value = 0) = 0;

protected:
    MemcachedConnection(in_port_t port, sa_family_t family, bool ssl,
                        const Protocol& protocol);

    void close();

    void connect();

    void read(Frame& frame, size_t bytes);

    void readPlain(Frame& frame, size_t bytes);

    void readSsl(Frame& frame, size_t bytes);

    void sendFramePlain(const Frame& frame);

    void sendFrameSsl(const Frame& frame);

    in_port_t port;
    sa_family_t family;
    bool ssl;
    Protocol protocol;
    SSL_CTX* context;
    BIO* bio;
    SOCKET sock;
    bool synchronous;
};

class ConnectionMap {
public:
    /**
     * Initialize the connection map with connections matching the ports
     * opened from Memcached
     */
    void initialize(cJSON* ports);

    /**
     * Invalidate all of the connections
     */
    void invalidate();

    /**
     * Get a connection object matching the given attributes
     *
     * @param protocol The requested protocol (Greenstack / Memcached)
     * @param ssl If ssl should be enabled or not
     * @param family the network family (IPv4 / IPv6)
     * @param port (optional) The specific port number to use..
     * @return A connection object to use
     * @throws std::runtime_error if the request can't be served
     */
    MemcachedConnection& getConnection(const Protocol& protocol,
                                       bool ssl,
                                       sa_family_t family = AF_INET,
                                       in_port_t port = 0);

    /**
     * Do we have a connection matching the requested attributes
     */
    bool contains(const Protocol& protocol, bool ssl, sa_family_t family);

private:
    std::vector<MemcachedConnection*> connections;
};
