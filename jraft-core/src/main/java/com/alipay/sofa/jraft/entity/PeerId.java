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

import java.io.Serializable;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.core.ElectionPriorityType;
import com.alipay.sofa.jraft.util.AsciiStringUtil;
import com.alipay.sofa.jraft.util.Copiable;
import com.alipay.sofa.jraft.util.CrcUtil;
import com.alipay.sofa.jraft.util.Endpoint;
import com.alipay.sofa.jraft.util.Utils;

/**
 * Represent a participant in a replicating group.
 *
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Mar-12 3:27:37 PM
 */
public class PeerId implements Copiable<PeerId>, Serializable, Checksum {

    private static final long   serialVersionUID = 8083529734784884641L;

    private static final Logger LOG              = LoggerFactory.getLogger(PeerId.class);

    /** Peer address. */
    private Endpoint            endpoint         = new Endpoint(Utils.IP_ANY, 0);
    /** Index in same addr, default is 0. */
    private int                 idx;
    /** Cached toString result. */
    private String              str;

    /** Node's local priority value, if node don't support priority election, this value is -1. */
    private int                 priority         = ElectionPriorityType.NOT_SUPPORT;

    public static final PeerId  ANY_PEER         = new PeerId();

    private long                checksum;

    public PeerId() {
        super();
    }

    @Override
    public long checksum() {
        if (this.checksum == 0) {
            this.checksum = CrcUtil.crc64(AsciiStringUtil.unsafeEncode(toString()));
        }
        return this.checksum;
    }

    /**
     * Create an empty peer.
     * @return empty peer
     */
    public static PeerId emptyPeer() {
        return new PeerId();
    }

    @Override
    public PeerId copy() {
        return new PeerId(this.endpoint.copy(), this.idx, this.priority);
    }

    /**
     * Parse a peer from string in the format of "ip:port:idx",
     * returns null if fail to parse.
     *
     * @param s input string with the format of "ip:port:idx"
     * @return parsed peer
     */
    public static PeerId parsePeer(final String s) {
        final PeerId peer = new PeerId();
        if (peer.parse(s)) {
            return peer;
        }
        return null;
    }

    public PeerId(final Endpoint endpoint, final int idx) {
        super();
        this.endpoint = endpoint;
        this.idx = idx;
    }

    public PeerId(final String ip, final int port) {
        this(ip, port, 0);
    }

    public PeerId(final String ip, final int port, final int idx) {
        super();
        this.endpoint = new Endpoint(ip, port);
        this.idx = idx;
    }

    public PeerId(final Endpoint endpoint, final int idx, final int priority) {
        super();
        this.endpoint = endpoint;
        this.idx = idx;
        this.priority = priority;
    }

    public PeerId(final String ip, final int port, final int idx, final int priority) {
        super();
        this.endpoint = new Endpoint(ip, port);
        this.idx = idx;
        this.priority = priority;
    }

    public Endpoint getEndpoint() {
        return this.endpoint;
    }

    public String getIp() {
        return this.endpoint.getIp();
    }

    public int getPort() {
        return this.endpoint.getPort();
    }

    public int getIdx() {
        return this.idx;
    }

    public int getPriority() {
        return priority;
    }

    public void setPriority(int priority) {
        this.priority = priority;
    }

    /**
     * Returns true when ip is ANY_IP, port is zero and idx is zero too.
     */
    public boolean isEmpty() {
        return getIp().equals(Utils.IP_ANY) && getPort() == 0 && this.idx == 0;
    }

    @Override
    public String toString() {
        if (this.str == null) {
            this.str = this.endpoint.toString();

            // ignore idx when it is zero and ignore priority when it is -1.
            StringBuilder appendStr = new StringBuilder();
            appendStr.append(":");

            if (this.idx != 0) {
                appendStr.append(this.idx);
            }

            if (this.priority != ElectionPriorityType.NOT_SUPPORT) {
                appendStr.append(":").append(this.priority);
            }

            if (appendStr.length() > 1) {
                this.str += appendStr.toString();
            }
        }
        return this.str;
    }

    /**
     * Parse peerId from string that generated by {@link #toString()}
     * This method can support parameter string values are below:
     *
     * <pre>
     * PeerId.parse("a:b")          = new PeerId("a", "b", 0 , -1)
     * PeerId.parse("a:b:c")        = new PeerId("a", "b", "c", -1)
     * PeerId.parse("a:b::d")       = new PeerId("a", "b", 0, "d")
     * PeerId.parse("a:b:c:d")      = new PeerId("a", "b", "c", "d")
     * </pre>
     *
     */
    public boolean parse(final String s) {

        if (StringUtils.isEmpty(s)) {
            return false;
        }

        final String[] tmps = StringUtils.splitPreserveAllTokens(s, ':');
        if (tmps.length <= 1 || tmps.length > 4) {
            return false;
        }
        try {
            final int port = Integer.parseInt(tmps[1]);
            this.endpoint = new Endpoint(tmps[0], port);

            if (tmps.length == 3) {
                this.idx = Integer.parseInt(tmps[2]);
            }

            if (tmps.length == 4) {
                if (tmps[2].equals("")) {
                    this.idx = 0;
                } else {
                    this.idx = Integer.parseInt(tmps[2]);
                }

                this.priority = Integer.parseInt(tmps[3]);
            }

            this.str = null;
            return true;
        } catch (final Exception e) {
            LOG.error("Parse peer from string failed: {}", s, e);
            return false;
        }
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (this.endpoint == null ? 0 : this.endpoint.hashCode());
        result = prime * result + this.idx;
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
        final PeerId other = (PeerId) obj;
        if (this.endpoint == null) {
            if (other.endpoint != null) {
                return false;
            }
        } else if (!this.endpoint.equals(other.endpoint)) {
            return false;
        }
        return this.idx == other.idx;
    }
}
