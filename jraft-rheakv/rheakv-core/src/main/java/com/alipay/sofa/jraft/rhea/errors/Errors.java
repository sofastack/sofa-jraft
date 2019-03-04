/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alipay.sofa.jraft.rhea.errors;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.rhea.util.Maps;
import com.alipay.sofa.jraft.rhea.util.StackTraceUtil;

/**
 * This class contains all the client-server errors--those errors that must be sent from the server to the client. These
 * are thus part of the protocol. The names can be changed but the error code cannot.
 *
 * Note that client library will convert an unknown error code to the non-retriable UnknownServerException if the client library
 * version is old and does not recognize the newly-added error code. Therefore when a new server-side error is added,
 * we may need extra logic to convert the new error code to another existing error code before sending the response back to
 * the client if the request version suggests that the client may not recognize the new error code.
 *
 * Do not add exceptions that occur only on the client or only on the server here.
 */
public enum Errors {
    UNKNOWN_SERVER_ERROR(-1, "The server experienced an unexpected error when processing the request",
        UnknownServerException::new),

    NONE(0, null, message -> null),

    STORAGE_ERROR(1, "Disk error when trying to access log file on the disk.", StorageException::new),

    INVALID_REQUEST(2, "This most likely occurs because of a request being malformed by the "
                       + "client library or the message was sent to an incompatible server. See the server logs "
                       + "for more details.", InvalidRequestException::new),

    LEADER_NOT_AVAILABLE(3, "The leader is not available.", LeaderNotAvailableException::new),

    NOT_LEADER(4, "This is not the correct leader.", NotLeaderException::new),

    INVALID_PARAMETER(5, "Invalid parameter error, please check your input parameters. See the server logs for more "
                         + "details.", InvalidParameterException::new),

    NO_REGION_FOUND(6, "Can not find region error.", NoRegionFoundException::new),

    INVALID_REGION_MEMBERSHIP(7, "Invalid region membership config error (add or remove peer happened).",
        InvalidRegionMembershipException::new),

    INVALID_REGION_VERSION(8, "Invalid region version error (region split or merge happened).",
        InvalidRegionVersionException::new),

    INVALID_REGION_EPOCH(9, "Invalid region epoch (membership or version changed).", InvalidRegionEpochException::new),

    INVALID_STORE_STATS(10, "Placement driver: invalid store stats", InvalidStoreStatsException::new),

    INVALID_REGION_STATS(11, "Placement driver: invalid region stats", InvalidStoreStatsException::new),

    STORE_HEARTBEAT_OUT_OF_DATE(12, "The store heartbeat info is out of date", StoreHeartbeatOutOfDateException::new),

    REGION_HEARTBEAT_OUT_OF_DATE(13, "The region heartbeat info is out of date", RegionHeartbeatOutOfDateException::new),

    CALL_SELF_ENDPOINT_ERROR(14, "The usual reason is that the rpc call selected the self endpoint.",
        CallSelfEndpointException::new),

    SERVER_BUSY(15, "The server is busy now.", ServerBusyException::new),

    REGION_ENGINE_FAIL(16, "Fail to start region engine. See the server logs for more details.",
        RegionEngineFailException::new),

    CONFLICT_REGION_ID(17, "There is a conflict between the new region id and the existing ids. "
                            + "The new region cannot be created.", RangeSplitFailException::new),

    TOO_SMALL_TO_SPLIT(18, "The region size is too small to split. See the server logs for more details.",
        RangeSplitFailException::new);

    private interface ApiExceptionBuilder {
        ApiException build(final String message);
    }

    private static final Logger          LOG          = LoggerFactory.getLogger(Errors.class);

    private static Map<Class<?>, Errors> classToError = Maps.newHashMap();
    private static Map<Short, Errors>    codeToError  = Maps.newHashMap();

    static {
        for (final Errors error : Errors.values()) {
            codeToError.put(error.code(), error);
            if (error.exception != null) {
                classToError.put(error.exception.getClass(), error);
            }
        }
    }

    private final short                  code;
    private final ApiExceptionBuilder    builder;
    private final ApiException           exception;

    Errors(int code, String defaultExceptionString, ApiExceptionBuilder builder) {
        this.code = (short) code;
        this.builder = builder;
        this.exception = builder.build(defaultExceptionString);
    }

    /**
     * An instance of the exception
     */
    public ApiException exception() {
        return this.exception;
    }

    /**
     * Create an instance of the ApiException that contains the given error message.
     *
     * @param message The message string to set.
     * @return        The exception.
     */
    public ApiException exception(final String message) {
        if (message == null) {
            // If no error message was specified, return an exception with the default error message.
            return this.exception;
        }
        // Return an exception with the given error message.
        return this.builder.build(message);
    }

    /**
     * Returns the class name of the exception or null if this is {@code Errors.NONE}.
     */
    public String exceptionName() {
        return this.exception == null ? null : this.exception.getClass().getName();
    }

    /**
     * The error code for the exception
     */
    public short code() {
        return this.code;
    }

    /**
     * Throw the exception corresponding to this error if there is one
     */
    public void maybeThrow() {
        if (this.exception != null) {
            throw this.exception;
        }
    }

    /**
     * Get a friendly description of the error (if one is available).
     * @return the error message
     */
    public String message() {
        if (this.exception != null) {
            return this.exception.getMessage();
        }
        return toString();
    }

    /**
     * Throw the exception if there is one
     */
    public static Errors forCode(final short code) {
        final Errors error = codeToError.get(code);
        if (error != null) {
            return error;
        } else {
            LOG.error("Unexpected error code: {}.", code);
            return UNKNOWN_SERVER_ERROR;
        }
    }

    /**
     * Return the error instance associated with this exception or any of its superclasses (or UNKNOWN if there is none).
     * If there are multiple matches in the class hierarchy, the first match starting from the bottom is used.
     */
    public static Errors forException(final Throwable t) {
        Class<?> clazz = t.getClass();
        while (clazz != null) {
            final Errors error = classToError.get(clazz);
            if (error != null) {
                return error;
            }
            clazz = clazz.getSuperclass();
        }
        LOG.error("Unexpected error: {}.", StackTraceUtil.stackTrace(t));
        return UNKNOWN_SERVER_ERROR;
    }

    public boolean isSuccess() {
        return this == Errors.NONE;
    }
}
