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
package com.alipay.sofa.jraft.core;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.entity.PeerId;

/**
 * Trace event emitter for TLA+ trace validation.
 *
 * Emits NDJSON events compatible with Tracesofajraft.tla.
 * Enabled via system property: -Djraft.trace.enabled=true
 * Output file: -Djraft.trace.file=path/to/trace.ndjson
 *
 * Each event captures the node's state snapshot and optional message fields.
 * Thread-safe: multiple Replicator threads + NodeImpl thread may emit concurrently.
 */
public final class RaftTracer {

    private static final Logger              LOG       = LoggerFactory.getLogger(RaftTracer.class);
    private static final boolean             ENABLED   = Boolean.getBoolean("jraft.trace.enabled");
    private static final String              FILE_PATH = System.getProperty("jraft.trace.file", "trace.ndjson");
    private static final Map<String, String> ID_MAP    = new ConcurrentHashMap<>();
    private static final AtomicLong          SEQ       = new AtomicLong(0);
    private static volatile PrintWriter      WRITER;

    static {
        if (ENABLED) {
            try {
                WRITER = new PrintWriter(new BufferedWriter(new FileWriter(FILE_PATH, false)), true);
                LOG.info("RaftTracer enabled, writing to {}", FILE_PATH);
            } catch (final IOException e) {
                LOG.error("RaftTracer failed to open file {}", FILE_PATH, e);
            }
        }
    }

    private RaftTracer() {
    }

    /**
     * Register a PeerId -> simplified ID mapping.
     * Call this at test setup, e.g. registerMapping("127.0.0.1:8081:0", "s1")
     */
    public static void registerMapping(final String peerId, final String simpleId) {
        ID_MAP.put(peerId, simpleId);
    }

    /**
     * Register a PeerId -> simplified ID mapping.
     */
    public static void registerMapping(final PeerId peerId, final String simpleId) {
        ID_MAP.put(peerId.toString(), simpleId);
    }

    /**
     * Map a PeerId string to its simplified ID.
     * Returns the original string if no mapping is registered.
     */
    public static String mapId(final String peerId) {
        if (peerId == null || peerId.isEmpty()) {
            return "";
        }
        return ID_MAP.getOrDefault(peerId, peerId);
    }

    /**
     * Map a PeerId to its simplified ID.
     */
    public static String mapId(final PeerId peerId) {
        if (peerId == null || peerId.isEmpty()) {
            return "";
        }
        return mapId(peerId.toString());
    }

    public static boolean isEnabled() {
        return ENABLED && WRITER != null;
    }

    /**
     * Shutdown the tracer (flush and close the output file).
     */
    public static void shutdown() {
        final PrintWriter w = WRITER;
        if (w != null) {
            w.flush();
            w.close();
        }
    }

    // ---- Emit methods ----

    /**
     * Emit a node-only event (no message fields).
     * Caller must hold appropriate lock (writeLock or readLock).
     */
    public static void logNodeEvent(final String name, final NodeImpl node) {
        if (!isEnabled())
            return;
        final String nid = mapId(node.getServerId());
        final String stateStr = node.traceStateJson();
        final String line = new StringBuilder(300).append("{\"tag\":\"trace\",\"ts\":").append(SEQ.incrementAndGet())
            .append(",\"event\":{\"name\":\"").append(name).append('"').append(",\"nid\":\"").append(nid).append('"')
            .append(",\"state\":{").append(stateStr).append('}').append("}}").toString();
        writeLine(line);
    }

    /**
     * Emit a message event with explicit msg JSON.
     * Caller must hold appropriate lock (writeLock or readLock).
     */
    public static void logMsgEvent(final String name, final NodeImpl node, final String msgJson) {
        if (!isEnabled())
            return;
        final String nid = mapId(node.getServerId());
        final String stateStr = node.traceStateJson();
        final String line = new StringBuilder(400).append("{\"tag\":\"trace\",\"ts\":").append(SEQ.incrementAndGet())
            .append(",\"event\":{\"name\":\"").append(name).append('"').append(",\"nid\":\"").append(nid).append('"')
            .append(",\"state\":{").append(stateStr).append('}').append(",\"msg\":{").append(msgJson).append('}')
            .append("}}").toString();
        writeLine(line);
    }

    /**
     * Emit a message event from Replicator context.
     * Acquires NodeImpl.readLock for consistent state snapshot.
     */
    public static void logReplicatorEvent(final String name, final Replicator r, final String msgJson) {
        if (!isEnabled())
            return;
        final NodeImpl node = r.getOpts().getNode();
        node.readLock.lock();
        try {
            logMsgEvent(name, node, msgJson);
        } finally {
            node.readLock.unlock();
        }
    }

    /**
     * Emit an AdvanceCommitIndex event from BallotBox context.
     * Acquires NodeImpl.readLock for consistent state snapshot.
     * The nid is the leader (node) that owns this BallotBox.
     */
    public static void logAdvanceCommitIndex(final NodeImpl node, final long newCommitIndex) {
        if (!isEnabled())
            return;
        node.readLock.lock();
        try {
            logNodeEvent("AdvanceCommitIndex", node);
        } finally {
            node.readLock.unlock();
        }
    }

    // ---- Message JSON builders ----

    /** Build msg JSON for vote request: {from, to, term} */
    public static String voteRequestMsg(final String from, final String to, final long term) {
        return "\"from\":\"" + mapId(from) + "\",\"to\":\"" + mapId(to) + "\",\"term\":" + term;
    }

    /** Build msg JSON for vote response: {from, to, term, granted} */
    public static String voteResponseMsg(final String from, final String to, final long term, final boolean granted) {
        return "\"from\":\"" + mapId(from) + "\",\"to\":\"" + mapId(to) + "\",\"term\":" + term + ",\"granted\":"
               + granted;
    }

    /** Build msg JSON for AppendEntries send (replicate): {from, to, term, prevLogIndex} */
    public static String appendEntriesMsg(final String from, final String to, final long term, final long prevLogIndex) {
        return "\"from\":\"" + mapId(from) + "\",\"to\":\"" + mapId(to) + "\",\"term\":" + term + ",\"prevLogIndex\":"
               + prevLogIndex;
    }

    /** Build msg JSON for heartbeat send: {from, to, term} — no prevLogIndex */
    public static String heartbeatMsg(final String from, final String to, final long term) {
        return "\"from\":\"" + mapId(from) + "\",\"to\":\"" + mapId(to) + "\",\"term\":" + term;
    }

    /** Build msg JSON for AppendEntries request received: {from, to, term} or with prevLogIndex */
    public static String appendEntriesRequestMsg(final String from, final String to, final long term,
                                                 final boolean hasEntries, final long prevLogIndex) {
        final StringBuilder sb = new StringBuilder(100);
        sb.append("\"from\":\"").append(mapId(from)).append("\",\"to\":\"").append(mapId(to)).append("\",\"term\":")
            .append(term);
        if (hasEntries) {
            sb.append(",\"prevLogIndex\":").append(prevLogIndex);
        }
        return sb.toString();
    }

    /** Build msg JSON for AppendEntries response (success): {from, to, term, success, matchIndex} */
    public static String appendEntriesResponseSuccessMsg(final String from, final String to, final long term,
                                                         final long matchIndex) {
        return "\"from\":\"" + mapId(from) + "\",\"to\":\"" + mapId(to) + "\",\"term\":" + term
               + ",\"success\":true,\"matchIndex\":" + matchIndex;
    }

    /** Build msg JSON for AppendEntries response (failure): {from, to, term, success} */
    public static String appendEntriesResponseFailureMsg(final String from, final String to, final long term) {
        return "\"from\":\"" + mapId(from) + "\",\"to\":\"" + mapId(to) + "\",\"term\":" + term + ",\"success\":false";
    }

    /** Build msg JSON for heartbeat response: {from, to, term} */
    public static String heartbeatResponseMsg(final String from, final String to, final long term) {
        return "\"from\":\"" + mapId(from) + "\",\"to\":\"" + mapId(to) + "\",\"term\":" + term;
    }

    /** Build msg JSON for InstallSnapshot response: {from, to, term, success} */
    public static String installSnapshotResponseMsg(final String from, final String to, final long term,
                                                    final boolean success) {
        return "\"from\":\"" + mapId(from) + "\",\"to\":\"" + mapId(to) + "\",\"term\":" + term + ",\"success\":"
               + success;
    }

    /** Build msg JSON for config change: {newPeers: "s1,s2,s3"} */
    public static String configChangeMsg(final String newPeers) {
        return "\"newPeers\":\"" + newPeers + "\"";
    }

    // ---- Internal ----

    private static void writeLine(final String line) {
        final PrintWriter w = WRITER;
        if (w != null) {
            synchronized (w) {
                w.println(line);
            }
        }
    }
}
