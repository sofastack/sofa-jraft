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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import com.alipay.sofa.jraft.entity.EnumOutter.EntryType;
import com.alipay.sofa.jraft.entity.LogEntry;
import com.alipay.sofa.jraft.entity.LogId;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.entity.codec.LogEntryEncoder;
import com.alipay.sofa.jraft.entity.codec.v2.LogOutter;
import com.alipay.sofa.jraft.util.AsciiStringUtil;
import com.alipay.sofa.jraft.util.Bits;

/**
 * V1 log entry encoder
 * @author boyan(boyan@antfin.com)
 *
 */
@Deprecated
public final class V1Encoder implements LogEntryEncoder {

    private V1Encoder() {
    }

    public static final LogEntryEncoder INSTANCE = new V1Encoder();

    @Override
    public byte[] encode(final LogEntry log) {
        if (log.hasLearners()) {
            throw new IllegalArgumentException("V1 log entry encoder doesn't support learners");
        }
        EntryType type = log.getType();
        LogId id = log.getId();
        List<PeerId> peers = log.getPeers();
        List<PeerId> oldPeers = log.getOldPeers();
        ByteBuffer data = log.getData();
        Boolean enableFlexible = log.getEnableFlexible();
        LogOutter.Quorum quorum = log.getQuorum();
        LogOutter.Quorum oldQuorum = log.getOldQuorum();
        final Integer readFactor = log.getReadFactor();
        final Integer writeFactor = log.getWriteFactor();
        final Integer oldReadFactor = log.getOldReadFactor();
        final Integer oldWriteFactor = log.getOldWriteFactor();
        // magic number 1 byte
        int totalLen = 1;
        final int iType = type.getNumber();
        final long index = id.getIndex();
        final long term = id.getTerm();
        // type(4) + index(8) + term(8)
        totalLen += 4 + 8 + 8;
        int peerCount = 0;
        // peer count
        totalLen += 4;
        final List<String> peerStrs = new ArrayList<>(peerCount);
        if (peers != null) {
            peerCount = peers.size();
            for (final PeerId peer : peers) {
                final String peerStr = peer.toString();
                // peer len (short in 2 bytes)
                // peer str
                totalLen += 2 + peerStr.length();
                peerStrs.add(peerStr);
            }
        }
        int oldPeerCount = 0;
        // old peer count
        totalLen += 4;
        final List<String> oldPeerStrs = new ArrayList<>(oldPeerCount);
        if (oldPeers != null) {
            oldPeerCount = oldPeers.size();
            for (final PeerId peer : oldPeers) {
                final String peerStr = peer.toString();
                // peer len (short in 2 bytes)
                // peer str
                totalLen += 2 + peerStr.length();
                oldPeerStrs.add(peerStr);
            }
        }

        final int bodyLen = data != null ? data.remaining() + 4 : 4;
        totalLen += bodyLen;
        // isEnableFlexible
        totalLen += 1;
        // quorum
        totalLen += quorum != null ? 8 + 1 : 1;
        // oldQuorum
        totalLen += oldQuorum != null ? 8 + 1 : 1;
        // factor
        totalLen += Objects.nonNull(readFactor) && Objects.nonNull(writeFactor) && readFactor != 0 && writeFactor != 0 ? 8 + 1
            : 1;
        // oldFactor
        totalLen += Objects.nonNull(oldReadFactor) && Objects.nonNull(oldWriteFactor) && oldReadFactor != 0
                    && oldWriteFactor != 0 ? 8 + 1 : 1;

        final byte[] content = new byte[totalLen];
        // {0} magic
        content[0] = LogEntryV1CodecFactory.MAGIC;
        // 1-5 type
        Bits.putInt(content, 1, iType);
        // 5-13 index
        Bits.putLong(content, 5, index);
        // 13-21 term
        Bits.putLong(content, 13, term);
        // peers
        // 21-25 peer count
        Bits.putInt(content, 21, peerCount);
        int pos = 25;
        for (final String peerStr : peerStrs) {
            final byte[] ps = AsciiStringUtil.unsafeEncode(peerStr);
            Bits.putShort(content, pos, (short) peerStr.length());
            System.arraycopy(ps, 0, content, pos + 2, ps.length);
            pos += 2 + ps.length;
        }
        // old peers
        // old peers count
        Bits.putInt(content, pos, oldPeerCount);
        pos += 4;
        for (final String peerStr : oldPeerStrs) {
            final byte[] ps = AsciiStringUtil.unsafeEncode(peerStr);
            Bits.putShort(content, pos, (short) peerStr.length());
            System.arraycopy(ps, 0, content, pos + 2, ps.length);
            pos += 2 + ps.length;
        }
        // data
        if (data != null) {
            int remaining = data.remaining();
            Bits.putInt(content, pos, remaining);
            System.arraycopy(data.array(), data.position(), content, pos + 4, data.remaining());
            pos += data.remaining() + 4;
        } else {
            Bits.putInt(content, pos, 0);
            pos += 4;
        }

        // Flexible Raft V1 Format:
        // isEnableFlexible  quorumExist  quorum  oldQuorumExist    oldQuorum  factorExist  readFactor+writeFactor oldFactorExist oldReadFactor+oldWriteFactor
        //   (Boolean)1      (Boolean)1   2*(int)4   (Boolean)1     2*(int)4     (Boolean)1       (int)8               (Boolean)1            (int)8

        // isEnableFlexible
        if (Objects.isNull(enableFlexible)) {
            Bits.putBoolean(content, pos, 0);
        } else {
            Bits.putBoolean(content, pos, enableFlexible ? 1 : 0);
        }
        pos += 1;
        // quorum
        byte quorumExist;
        if (Objects.nonNull(quorum)) {
            quorumExist = 1;
            int r = quorum.getR();
            int w = quorum.getW();
            Bits.putBoolean(content, pos, quorumExist);
            Bits.putInt(content, pos + 1, r);
            Bits.putInt(content, pos + 5, w);
            pos += 9;
        } else {
            quorumExist = 0;
            Bits.putBoolean(content, pos, quorumExist);
            pos += 1;
        }
        // oldQuorum
        byte oldQuorumExist;
        if (Objects.nonNull(oldQuorum)) {
            oldQuorumExist = 1;
            int r = oldQuorum.getR();
            int w = oldQuorum.getW();
            Bits.putBoolean(content, pos, oldQuorumExist);
            Bits.putInt(content, pos + 1, r);
            Bits.putInt(content, pos + 5, w);
            pos += 9;
        } else {
            oldQuorumExist = 0;
            Bits.putBoolean(content, pos, oldQuorumExist);
            pos += 1;
        }
        // readFactor and writeFactor
        byte factorExist;
        if (Objects.nonNull(readFactor) && Objects.nonNull(writeFactor) && readFactor != 0 && writeFactor != 0) {
            factorExist = 1;
            Bits.putBoolean(content, pos, factorExist);
            Bits.putInt(content, pos + 1, readFactor);
            Bits.putInt(content, pos + 5, writeFactor);
            pos += 9;
        } else {
            factorExist = 0;
            Bits.putBoolean(content, pos, factorExist);
            pos += 1;
        }
        // oldReadFactor and oldWriteFactor
        byte oldFactorExist;
        if (Objects.nonNull(oldReadFactor) && Objects.nonNull(oldWriteFactor) && oldReadFactor != 0
            && oldWriteFactor != 0) {
            oldFactorExist = 1;
            Bits.putInt(content, pos, oldFactorExist);
            Bits.putInt(content, pos + 1, oldReadFactor);
            Bits.putInt(content, pos + 5, oldWriteFactor);
            pos += 9;
        } else {
            oldFactorExist = 0;
            Bits.putBoolean(content, pos, oldFactorExist);
            pos += 1;
        }
        return content;
    }
}