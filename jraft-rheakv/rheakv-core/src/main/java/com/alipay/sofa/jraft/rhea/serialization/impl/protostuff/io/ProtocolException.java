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
package com.alipay.sofa.jraft.rhea.serialization.impl.protostuff.io;

import io.protostuff.ProtobufException;

/**
 *
 * @author jiachun.fjc
 */
public class ProtocolException extends ProtobufException {

    private static final long serialVersionUID = -1L;

    public ProtocolException(String description) {
        super(description);
    }

    public ProtocolException(String description, Throwable cause) {
        super(description, cause);
    }

    static ProtocolException misreportedSize() {
        return new ProtocolException("CodedInput encountered an embedded string or bytes "
                                     + "that misreported its size.");
    }

    static ProtocolException negativeSize() {
        return new ProtocolException("CodedInput encountered an embedded string or message "
                                     + "which claimed to have negative size.");
    }

    static ProtocolException malformedVarInt() {
        return new ProtocolException("CodedInput encountered a malformed varint.");
    }

    static ProtocolException invalidTag() {
        return new ProtocolException("Protocol message contained an invalid tag (zero).");
    }

    static ProtocolException invalidEndTag() {
        return new ProtocolException("Protocol message end-group tag did not match expected tag.");
    }

    static ProtocolException invalidWireType() {
        return new ProtocolException("Protocol message tag had invalid wire type.");
    }
}
