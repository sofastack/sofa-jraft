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
package com.alipay.sofa.jraft.entity.codec;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import com.alipay.sofa.jraft.entity.EnumOutter;
import com.alipay.sofa.jraft.entity.EnumOutter.EntryType;
import com.alipay.sofa.jraft.entity.LogEntry;
import com.alipay.sofa.jraft.entity.LogId;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.util.AsciiStringUtil;
import com.alipay.sofa.jraft.util.Bits;

/**
 *  Old V1 log entry codec implementation.
 * @author boyan(boyan@antfin.com)
 *
 */
public class LogEntryV1CodecFactory implements LogEntryCodecFactory {

    /**
     * V1 log entry encoder
     * @author boyan(boyan@antfin.com)
     *
     */
    public static final class V1Encoder implements LogEntryEncoder {
        @Override
        public byte[] encode(final LogEntry log) {
            EntryType type = log.getType();
            LogId id = log.getId();
            List<PeerId> peers = log.getPeers();
            List<PeerId> oldPeers = log.getOldPeers();
            ByteBuffer data = log.getData();

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

            final int bodyLen = data != null ? data.remaining() : 0;
            totalLen += bodyLen;

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
                System.arraycopy(data.array(), data.position(), content, pos, data.remaining());
            }

            return content;
        }
    }

    /**
     * V1 log entry decoder
     * @author boyan(boyan@antfin.com)
     *
     */
    public static final class V1Decoder implements LogEntryDecoder {
        @Override
        public LogEntry decode(final byte[] content) {
            if (content == null || content.length == 0) {
                return null;
            }
            if (content[0] != LogEntryV1CodecFactory.MAGIC) {
                // Corrupted log
                return null;
            }
            LogEntry log = new LogEntry();
            decode(log, content);

            return log;
        }

        public void decode(final LogEntry log, final byte[] content) {
            // 1-5 type
            final int iType = Bits.getInt(content, 1);
            log.setType(EnumOutter.EntryType.forNumber(iType));
            // 5-13 index
            // 13-21 term
            final long index = Bits.getLong(content, 5);
            final long term = Bits.getLong(content, 13);
            log.setId(new LogId(index, term));
            // 21-25 peer count
            int peerCount = Bits.getInt(content, 21);
            // peers
            int pos = 25;
            if (peerCount > 0) {
                List<PeerId> peers = new ArrayList<>(peerCount);
                while (peerCount-- > 0) {
                    final short len = Bits.getShort(content, pos);
                    final byte[] bs = new byte[len];
                    System.arraycopy(content, pos + 2, bs, 0, len);
                    // peer len (short in 2 bytes)
                    // peer str
                    pos += 2 + len;
                    final PeerId peer = new PeerId();
                    peer.parse(AsciiStringUtil.unsafeDecode(bs));
                    peers.add(peer);
                }
                log.setPeers(peers);
            }
            // old peers
            int oldPeerCount = Bits.getInt(content, pos);
            pos += 4;
            if (oldPeerCount > 0) {
                List<PeerId> oldPeers = new ArrayList<>(oldPeerCount);
                while (oldPeerCount-- > 0) {
                    final short len = Bits.getShort(content, pos);
                    final byte[] bs = new byte[len];
                    System.arraycopy(content, pos + 2, bs, 0, len);
                    // peer len (short in 2 bytes)
                    // peer str
                    pos += 2 + len;
                    final PeerId peer = new PeerId();
                    peer.parse(AsciiStringUtil.unsafeDecode(bs));
                    oldPeers.add(peer);
                }
                log.setOldPeers(oldPeers);
            }

            // data
            if (content.length > pos) {
                final int len = content.length - pos;
                ByteBuffer data = ByteBuffer.allocate(len);
                data.put(content, pos, len);
                data.flip();
                log.setData(data);
            }
        }
    }

    //"Beeep boop beep beep boop beeeeeep" -BB8
    public static final byte MAGIC = (byte) 0xB8;

    private LogEntryV1CodecFactory() {
    }

    private static LogEntryV1CodecFactory INSTANCE = new LogEntryV1CodecFactory();

    /**
     * Returns a singleton instance of DefaultLogEntryCodecFactory.
     * @return
     */
    public static LogEntryV1CodecFactory getInstance() {
        return INSTANCE;
    }

    public static LogEntryEncoder V1_ENCODER = new V1Encoder();

    public static V1Decoder       V1_DECODER = new V1Decoder();

    @Override
    public LogEntryEncoder encoder() {
        return V1_ENCODER;
    }

    @Override
    public LogEntryDecoder decoder() {
        return V1_DECODER;
    }

}
