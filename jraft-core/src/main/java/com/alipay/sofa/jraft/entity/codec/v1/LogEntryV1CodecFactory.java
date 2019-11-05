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
package com.alipay.sofa.jraft.entity.codec.v1;

import com.alipay.sofa.jraft.entity.codec.LogEntryCodecFactory;
import com.alipay.sofa.jraft.entity.codec.LogEntryDecoder;
import com.alipay.sofa.jraft.entity.codec.LogEntryEncoder;

/**
 *  Old V1 log entry codec implementation.
 * @author boyan(boyan@antfin.com)
 *
 */
@Deprecated
public class LogEntryV1CodecFactory implements LogEntryCodecFactory {

    //"Beeep boop beep beep boop beeeeeep" -BB8
    public static final byte MAGIC = (byte) 0xB8;

    private LogEntryV1CodecFactory() {
    }

    private static final LogEntryV1CodecFactory INSTANCE = new LogEntryV1CodecFactory();

    /**
     * Returns a singleton instance of DefaultLogEntryCodecFactory.
     * @return a singleton instance
     */
    public static LogEntryV1CodecFactory getInstance() {
        return INSTANCE;
    }

    @Override
    public LogEntryEncoder encoder() {
        return V1Encoder.INSTANCE;
    }

    @Override
    public LogEntryDecoder decoder() {
        return V1Decoder.INSTANCE;
    }

}
