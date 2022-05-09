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
import java.util.List;

import com.alipay.sofa.jraft.entity.codec.LogEntryDecoder;
import com.alipay.sofa.jraft.entity.codec.LogEntryEncoder;
import com.alipay.sofa.jraft.entity.codec.v1.LogEntryV1CodecFactory;
import com.alipay.sofa.jraft.entity.codec.v1.V1Decoder;
import com.alipay.sofa.jraft.entity.codec.v1.V1Encoder;
import com.alipay.sofa.jraft.util.CrcUtil;

/**
 * A replica log entry.
 *
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Mar-12 3:13:02 PM
 */
public class LogEntry implements Checksum {

    public static final ByteBuffer EMPTY_DATA = ByteBuffer.wrap(new byte[0]);

    /** entry type */
    private EnumOutter.EntryType   type;
    /** log id with index/term */
    private LogId                  id         = new LogId(0, 0);
    /** log entry current peers */
    private List<PeerId>           peers;
    /** log entry old peers */
    private List<PeerId>           oldPeers;
    /** log entry current learners */
    private List<PeerId>           learners;
    /** log entry old learners */
    private List<PeerId>           oldLearners;
    /** entry data */
    private ByteBuffer             data       = EMPTY_DATA;
    /** checksum for log entry*/
    private long                   checksum;
    /** true when the log has checksum **/
    private boolean                hasChecksum;

    public List<PeerId> getLearners() {
        return this.learners;
    }

    public void setLearners(final List<PeerId> learners) {
        this.learners = learners;
    }

    public List<PeerId> getOldLearners() {
        return this.oldLearners;
    }

    public void setOldLearners(final List<PeerId> oldLearners) {
        this.oldLearners = oldLearners;
    }

    public LogEntry() {
        super();
    }

    public LogEntry(final EnumOutter.EntryType type) {
        super();
        this.type = type;
    }

    public boolean hasLearners() {
        return (this.learners != null && !this.learners.isEmpty())
               || (this.oldLearners != null && !this.oldLearners.isEmpty());
    }

    @Override
    public long checksum() {
        long c = checksum(this.type.getNumber(), this.id.checksum());
        c = checksum(this.peers, c);
        c = checksum(this.oldPeers, c);
        c = checksum(this.learners, c);
        c = checksum(this.oldLearners, c);
        if (this.data != null && this.data.hasRemaining()) {
            c = checksum(c, CrcUtil.crc64(this.data));
        }
        return c;
    }

    /**
     * Please use {@link LogEntryEncoder} instead.
     *
     * @deprecated
     * @return encoded byte array
     */
    @SuppressWarnings("DeprecatedIsStillUsed")
    @Deprecated
    public byte[] encode() {
        return V1Encoder.INSTANCE.encode(this);
    }

    /**
     * Please use {@link LogEntryDecoder} instead.
     *
     * @deprecated
     * @return whether success to decode
     */
    @SuppressWarnings("DeprecatedIsStillUsed")
    @Deprecated
    public boolean decode(final byte[] content) {
        if (content == null || content.length == 0) {
            return false;
        }
        if (content[0] != LogEntryV1CodecFactory.MAGIC) {
            // Corrupted log
            return false;
        }
        V1Decoder.INSTANCE.decode(this, content);
        return true;
    }

    /**
     * Returns whether the log entry has a checksum.
     * @return true when the log entry has checksum, otherwise returns false.
     * @since 1.2.26
     */
    public boolean hasChecksum() {
        return this.hasChecksum;
    }

    /**
     * Returns true when the log entry is corrupted, it means that the checksum is mismatch.
     * @since 1.2.6
     * @return true when the log entry is corrupted, otherwise returns false
     */
    public boolean isCorrupted() {
        return this.hasChecksum && this.checksum != checksum();
    }

    /**
     * Returns the checksum of the log entry. You should use {@link #hasChecksum} to check if
     * it has checksum.
     * @return checksum value
     */
    public long getChecksum() {
        return this.checksum;
    }

    public void setChecksum(final long checksum) {
        this.checksum = checksum;
        this.hasChecksum = true;
    }

    public EnumOutter.EntryType getType() {
        return this.type;
    }

    public void setType(final EnumOutter.EntryType type) {
        this.type = type;
    }

    public LogId getId() {
        return this.id;
    }

    public void setId(final LogId id) {
        this.id = id;
    }

    public List<PeerId> getPeers() {
        return this.peers;
    }

    public void setPeers(final List<PeerId> peers) {
        this.peers = peers;
    }

    public List<PeerId> getOldPeers() {
        return this.oldPeers;
    }

    public void setOldPeers(final List<PeerId> oldPeers) {
        this.oldPeers = oldPeers;
    }

    /**
     * Returns the log data, it's not read-only, you SHOULD take care it's modification and
     * thread-safety by yourself.
     *
     * @return the log data
     */
    public ByteBuffer getData() {
        return this.data;
    }

    /**
     * Creates a new byte buffer whose content is a shared subsequence of this log entry's data
     * buffer's content.
     *
     * @return The new byte buffer
     */
    public ByteBuffer sliceData() {
        return this.data != null ? this.data.slice() : null;
    }

    /**
     * Creates a new, read-only byte buffer that shares this log entry's data buffer's content.
     *
     * @return the new, read-only byte buffer
     */
    public ByteBuffer getReadOnlyData() {
        return this.data != null ? this.data.asReadOnlyBuffer() : null;
    }

    public void setData(final ByteBuffer data) {
        this.data = data;
    }

    @Override
    public String toString() {
        return "LogEntry [type=" + this.type + ", id=" + this.id + ", peers=" + this.peers + ", oldPeers="
               + this.oldPeers + ", learners=" + this.learners + ", oldLearners=" + this.oldLearners + ", data="
               + (this.data != null ? this.data.remaining() : 0) + "]";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((this.data == null) ? 0 : this.data.hashCode());
        result = prime * result + ((this.id == null) ? 0 : this.id.hashCode());
        result = prime * result + ((this.learners == null) ? 0 : this.learners.hashCode());
        result = prime * result + ((this.oldLearners == null) ? 0 : this.oldLearners.hashCode());
        result = prime * result + ((this.oldPeers == null) ? 0 : this.oldPeers.hashCode());
        result = prime * result + ((this.peers == null) ? 0 : this.peers.hashCode());
        result = prime * result + ((this.type == null) ? 0 : this.type.hashCode());
        return result;
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        LogEntry other = (LogEntry) obj;
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
        if (this.learners == null) {
            if (other.learners != null) {
                return false;
            }
        } else if (!this.learners.equals(other.learners)) {
            return false;
        }
        if (this.oldLearners == null) {
            if (other.oldLearners != null) {
                return false;
            }
        } else if (!this.oldLearners.equals(other.oldLearners)) {
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
