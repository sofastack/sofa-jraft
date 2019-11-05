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

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import com.alipay.sofa.jraft.entity.LogEntry;
import com.alipay.sofa.jraft.entity.LogId;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.entity.codec.LogEntryEncoder;
import com.alipay.sofa.jraft.entity.codec.v2.LogOutter.PBLogEntry;
import com.alipay.sofa.jraft.error.LogEntryCorruptedException;
import com.alipay.sofa.jraft.util.AsciiStringUtil;
import com.alipay.sofa.jraft.util.Requires;
import com.google.protobuf.ByteString;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.ZeroByteStringHelper;

/**
 * V2 log entry encoder based on protobuf, see src/main/resources/log.proto
 *
 * @author boyan(boyan@antfin.com)
 */
public class V2Encoder implements LogEntryEncoder {

    public static final V2Encoder INSTANCE = new V2Encoder();

    private static boolean hasPeers(final Collection<PeerId> peers) {
        return peers != null && !peers.isEmpty();
    }

    private void encodePeers(final PBLogEntry.Builder builder, final List<PeerId> peers) {
        final int size = peers.size();
        for (int i = 0; i < size; i++) {
            builder.addPeers(ZeroByteStringHelper.wrap(AsciiStringUtil.unsafeEncode(peers.get(i).toString())));
        }
    }

    private void encodeOldPeers(final PBLogEntry.Builder builder, final List<PeerId> peers) {
        final int size = peers.size();
        for (int i = 0; i < size; i++) {
            builder.addOldPeers(ZeroByteStringHelper.wrap(AsciiStringUtil.unsafeEncode(peers.get(i).toString())));
        }
    }

    private void encodeLearners(final PBLogEntry.Builder builder, final List<PeerId> learners) {
        final int size = learners.size();
        for (int i = 0; i < size; i++) {
            builder.addLearners(ZeroByteStringHelper.wrap(AsciiStringUtil.unsafeEncode(learners.get(i).toString())));
        }
    }

    private void encodeOldLearners(final PBLogEntry.Builder builder, final List<PeerId> learners) {
        final int size = learners.size();
        for (int i = 0; i < size; i++) {
            builder.addOldLearners(ZeroByteStringHelper.wrap(AsciiStringUtil.unsafeEncode(learners.get(i).toString())));
        }
    }

    @Override
    public byte[] encode(final LogEntry log) {
        Requires.requireNonNull(log, "Null log");

        final LogId logId = log.getId();
        final PBLogEntry.Builder builder = PBLogEntry.newBuilder() //
            .setType(log.getType()) //
            .setIndex(logId.getIndex()) //
            .setTerm(logId.getTerm());

        final List<PeerId> peers = log.getPeers();
        if (hasPeers(peers)) {
            encodePeers(builder, peers);
        }

        final List<PeerId> oldPeers = log.getOldPeers();
        if (hasPeers(oldPeers)) {
            encodeOldPeers(builder, oldPeers);
        }

        final List<PeerId> learners = log.getLearners();
        if (hasPeers(learners)) {
            encodeLearners(builder, learners);
        }
        final List<PeerId> oldLearners = log.getOldLearners();
        if (hasPeers(oldLearners)) {
            encodeOldLearners(builder, oldLearners);
        }

        if (log.hasChecksum()) {
            builder.setChecksum(log.getChecksum());
        }

        builder.setData(log.getData() != null ? ZeroByteStringHelper.wrap(log.getData()) : ByteString.EMPTY);

        final PBLogEntry pbLogEntry = builder.build();
        final int bodyLen = pbLogEntry.getSerializedSize();
        final byte[] ret = new byte[LogEntryV2CodecFactory.HEADER_SIZE + bodyLen];

        // write header
        int i = 0;
        for (; i < LogEntryV2CodecFactory.MAGIC_BYTES.length; i++) {
            ret[i] = LogEntryV2CodecFactory.MAGIC_BYTES[i];
        }
        ret[i++] = LogEntryV2CodecFactory.VERSION;
        // avoid memory copy for only 3 bytes
        for (; i < LogEntryV2CodecFactory.HEADER_SIZE; i++) {
            ret[i] = LogEntryV2CodecFactory.RESERVED[i - LogEntryV2CodecFactory.MAGIC_BYTES.length - 1];
        }

        // write body
        writeToByteArray(pbLogEntry, ret, i, bodyLen);

        return ret;
    }

    private void writeToByteArray(final PBLogEntry pbLogEntry, final byte[] array, final int offset, final int len) {
        final CodedOutputStream output = CodedOutputStream.newInstance(array, offset, len);
        try {
            pbLogEntry.writeTo(output);
            output.checkNoSpaceLeft();
        } catch (final IOException e) {
            throw new LogEntryCorruptedException(
                "Serializing PBLogEntry to a byte array threw an IOException (should never happen).", e);
        }
    }

    private V2Encoder() {
    }
}
