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
package com.alipay.sofa.jraft.entity.codec.v2;

import java.util.List;
import java.util.stream.Collectors;

import com.alipay.sofa.jraft.entity.LogEntry;
import com.alipay.sofa.jraft.entity.LogId;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.entity.codec.LogEntryEncoder;
import com.alipay.sofa.jraft.entity.codec.v2.LogOutter.PBLogEntry;
import com.alipay.sofa.jraft.util.Bits;
import com.alipay.sofa.jraft.util.Requires;
import com.google.protobuf.ByteString;
import com.google.protobuf.ZeroByteStringHelper;

/**
 * V2 log entry encoder based on protobuf, see src/main/resources/log.proto
 * @author boyan(boyan@antfin.com)
 *
 */
public class V2Encoder implements LogEntryEncoder {

    private V2Encoder() {

    }

    private static boolean hasPeers(final List<PeerId> peers) {
        return peers != null && !peers.isEmpty();
    }

    public static final V2Encoder INSTANCE = new V2Encoder();

    @Override
    public byte[] encode(final LogEntry log) {
        Requires.requireNonNull(log, "Null log");

        LogId logId = log.getId();
        PBLogEntry.Builder builder = PBLogEntry.newBuilder() //
                .setType(log.getType()) //
                .setIndex(logId.getIndex()) //
                .setTerm(logId.getTerm());

        List<PeerId> peers = log.getPeers();
        if (hasPeers(peers)) {
            builder.addAllPeers(peers.stream().map(PeerId::toString).collect(Collectors.toList()));
        }

        List<PeerId> oldPeers = log.getOldPeers();
        if (hasPeers(oldPeers)) {
            builder.addAllPeers(oldPeers.stream().map(PeerId::toString).collect(Collectors.toList()));
        }

        if (log.hasChecksum()) {
            builder.setChecksum(log.getChecksum());
        }

        builder.setData(log.getData() != null ? ZeroByteStringHelper.wrap(log.getData()) : ByteString.EMPTY);

        byte[] bs = builder.build().toByteArray();

        byte[] ret = new byte[LogEntryV2CodecFactory.HEADER_SIZE + bs.length];
        int i = 0;
        for (byte b : LogEntryV2CodecFactory.MAGIC_BYTES) {
            ret[i++] = b;
        }
        Bits.putShort(ret, i, LogEntryV2CodecFactory.VERSION);
        i += 2;
        Bits.putInt(ret, i, LogEntryV2CodecFactory.RESERVED);
        i += 4;
        System.arraycopy(bs, 0, ret, i, bs.length);
        return ret;
    }
}
