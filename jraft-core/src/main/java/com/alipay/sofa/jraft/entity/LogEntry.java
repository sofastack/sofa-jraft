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
package com.alipay.sofa.jraft.entity;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import com.alipay.sofa.jraft.util.AsciiStringUtil;
import com.alipay.sofa.jraft.util.Bits;

/**
 * A replica log entry.
 *
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Mar-12 3:13:02 PM
 */
public class LogEntry {

    /** entry type */
    private EnumOutter.EntryType type;
    /** log id with index/term */
    private LogId                id    = new LogId(0, 0);
    /** log entry current peers */
    private List<PeerId>         peers;
    /** log entry old peers */
    private List<PeerId>         oldPeers;
    /** entry data */
    private ByteBuffer           data;

    //"Beeep boop beep beep boop beeeeeep" -BB8
    public static final byte     MAGIC = (byte) 0xB8;

    public LogEntry() {
        super();
    }

    public LogEntry(EnumOutter.EntryType type) {
        super();
        this.type = type;
    }

    public byte[] encode() {
        // magic number 1 byte
        int totalLen = 1;
        final int iType = this.type.getNumber();
        final long index = this.id.getIndex();
        final long term = this.id.getTerm();
        // type(4) + index(8) + term(8)
        totalLen += 4 + 8 + 8;
        int peerCount = 0;
        // peer count
        totalLen += 4;
        final List<String> peerStrs = new ArrayList<>(peerCount);
        if (this.peers != null) {
            peerCount = this.peers.size();
            for (final PeerId peer : this.peers) {
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
        if (this.oldPeers != null) {
            oldPeerCount = this.oldPeers.size();
            for (final PeerId peer : this.oldPeers) {
                final String peerStr = peer.toString();
                // peer len (short in 2 bytes)
                // peer str
                totalLen += 2 + peerStr.length();
                oldPeerStrs.add(peerStr);
            }
        }

        final int bodyLen = this.data != null ? this.data.remaining() : 0;
        totalLen += bodyLen;

        final byte[] content = new byte[totalLen];
        // {0} magic
        content[0] = MAGIC;
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
        if (this.data != null) {
            System.arraycopy(this.data.array(), this.data.position(), content, pos, this.data.remaining());
        }

        return content;
    }

    public boolean decode(final byte[] content) {
        if (content == null || content.length == 0) {
            return false;
        }
        if (content[0] != MAGIC) {
            // Corrupted log
            return false;
        }
        // 1-5 type
        final int iType = Bits.getInt(content, 1);
        this.type = EnumOutter.EntryType.forNumber(iType);
        // 5-13 index
        // 13-21 term
        final long index = Bits.getLong(content, 5);
        final long term = Bits.getLong(content, 13);
        this.id = new LogId(index, term);
        // 21-25 peer count
        int peerCount = Bits.getInt(content, 21);
        // peers
        int pos = 25;
        if (peerCount > 0) {
            this.peers = new ArrayList<>(peerCount);
            while (peerCount-- > 0) {
                final short len = Bits.getShort(content, pos);
                final byte[] bs = new byte[len];
                System.arraycopy(content, pos + 2, bs, 0, len);
                // peer len (short in 2 bytes)
                // peer str
                pos += 2 + len;
                final PeerId peer = new PeerId();
                peer.parse(AsciiStringUtil.unsafeDecode(bs));
                this.peers.add(peer);
            }
        }
        // old peers
        int oldPeerCount = Bits.getInt(content, pos);
        pos += 4;
        if (oldPeerCount > 0) {
            this.oldPeers = new ArrayList<>(oldPeerCount);
            while (oldPeerCount-- > 0) {
                final short len = Bits.getShort(content, pos);
                final byte[] bs = new byte[len];
                System.arraycopy(content, pos + 2, bs, 0, len);
                // peer len (short in 2 bytes)
                // peer str
                pos += 2 + len;
                final PeerId peer = new PeerId();
                peer.parse(AsciiStringUtil.unsafeDecode(bs));
                this.oldPeers.add(peer);
            }
        }

        // data
        if (content.length > pos) {
            final int len = content.length - pos;
            this.data = ByteBuffer.allocate(len);
            this.data.put(content, pos, len);
            this.data.flip();
        }

        return true;
    }

    public EnumOutter.EntryType getType() {
        return this.type;
    }

    public void setType(EnumOutter.EntryType type) {
        this.type = type;
    }

    public LogId getId() {
        return this.id;
    }

    public void setId(LogId id) {
        this.id = id;
    }

    public List<PeerId> getPeers() {
        return this.peers;
    }

    public void setPeers(List<PeerId> peers) {
        this.peers = peers;
    }

    public List<PeerId> getOldPeers() {
        return this.oldPeers;
    }

    public void setOldPeers(List<PeerId> oldPeers) {
        this.oldPeers = oldPeers;
    }

    public ByteBuffer getData() {
        return this.data;
    }

    public void setData(ByteBuffer data) {
        this.data = data;
    }

    @Override
    public String toString() {
        return "LogEntry [type=" + this.type + ", id=" + this.id + ", peers=" + this.peers + ", oldPeers="
               + this.oldPeers + ", data=" + (this.data != null ? this.data.remaining() : 0) + "]";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (this.data == null ? 0 : this.data.hashCode());
        result = prime * result + (this.id == null ? 0 : this.id.hashCode());
        result = prime * result + (this.oldPeers == null ? 0 : this.oldPeers.hashCode());
        result = prime * result + (this.peers == null ? 0 : this.peers.hashCode());
        result = prime * result + (this.type == null ? 0 : this.type.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final LogEntry other = (LogEntry) obj;
        if (this.data == null) {
            if (other.data != null) {
                return false;
            }
        } else if (!this.data.equals(other.data)) {
            return false;
        }
        if (this.id == null) {
            if (other.id != null) {
                return false;
            }
        } else if (!this.id.equals(other.id)) {
            return false;
        }
        if (this.oldPeers == null) {
            if (other.oldPeers != null) {
                return false;
            }
        } else if (!this.oldPeers.equals(other.oldPeers)) {
            return false;
        }
        if (this.peers == null) {
            if (other.peers != null) {
                return false;
            }
        } else if (!this.peers.equals(other.peers)) {
            return false;
        }
        return this.type == other.type;
    }
}
