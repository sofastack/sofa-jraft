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
package com.alipay.sofa.jraft.rhea.storage;

import java.io.Serializable;
import java.util.List;

import com.alipay.sofa.jraft.rhea.util.Pair;
import com.alipay.sofa.jraft.rhea.util.concurrent.DistributedLock;
import com.alipay.sofa.jraft.util.BytesUtil;
import com.alipay.sofa.jraft.util.Requires;

/**
 * The KV store operation
 *
 * @author jiachun.fjc
 */
public class KVOperation implements Serializable {

    private static final long   serialVersionUID = 1368415383186519279L;

    /** Encode magic number */
    public static final byte    MAGIC            = 0x00;

    /** Put operation */
    public static final byte    PUT              = 0x01;
    /** PutIfAbsent operation */
    public static final byte    PUT_IF_ABSENT    = 0x02;
    /** Delete operation */
    public static final byte    DELETE           = 0x03;
    /** Put list operation */
    public static final byte    PUT_LIST         = 0x04;
    /** Delete range operation */
    public static final byte    DELETE_RANGE     = 0x05;
    /** Get sequence operation */
    public static final byte    GET_SEQUENCE     = 0x06;
    /** Execute on every node operation */
    public static final byte    NODE_EXECUTE     = 0x07;
    /** Tries to lock the specified key */
    public static final byte    KEY_LOCK         = 0x08;
    /** Unlock the specified key */
    public static final byte    KEY_LOCK_RELEASE = 0x09;
    /** Get operation */
    public static final byte    GET              = 0x0a;
    /** MultiGet operation  */
    public static final byte    MULTI_GET        = 0x0b;
    /** Scan operation */
    public static final byte    SCAN             = 0x0c;
    /** Get and put operation */
    public static final byte    GET_PUT          = 0x0d;
    /** Merge operation */
    public static final byte    MERGE            = 0x0e;
    /** Reset sequence operation */
    public static final byte    RESET_SEQUENCE   = 0x0f;

    // split operation ***********************************
    /** Range split operation */
    public static final byte    RANGE_SPLIT      = 0x10;
    /** Compare and put operation */
    public static final byte    COMPARE_PUT      = 0x11;
    /** Delete list operation */
    public static final byte    DELETE_LIST      = 0x12;
    /** Contains key operation */
    public static final byte    CONTAINS_KEY     = 0x13;

    /** Reverse Scan operation */
    public static final byte    REVERSE_SCAN     = 0x14;
    /** Compare and put all */
    public static final byte    COMPARE_PUT_ALL  = 0x15;

    public static final byte    EOF              = 0x16;

    private static final byte[] VALID_OPS;

    static {
        VALID_OPS = new byte[EOF - 1];
        VALID_OPS[0] = PUT;
        VALID_OPS[1] = PUT_IF_ABSENT;
        VALID_OPS[2] = DELETE;
        VALID_OPS[3] = PUT_LIST;
        VALID_OPS[4] = DELETE_RANGE;
        VALID_OPS[5] = GET_SEQUENCE;
        VALID_OPS[6] = NODE_EXECUTE;
        VALID_OPS[7] = KEY_LOCK;
        VALID_OPS[8] = KEY_LOCK_RELEASE;
        VALID_OPS[9] = GET;
        VALID_OPS[10] = MULTI_GET;
        VALID_OPS[11] = SCAN;
        VALID_OPS[12] = GET_PUT;
        VALID_OPS[13] = MERGE;
        VALID_OPS[14] = RESET_SEQUENCE;
        VALID_OPS[15] = RANGE_SPLIT;
        VALID_OPS[16] = COMPARE_PUT;
        VALID_OPS[17] = DELETE_LIST;
        VALID_OPS[18] = CONTAINS_KEY;
        VALID_OPS[19] = REVERSE_SCAN;
        VALID_OPS[20] = COMPARE_PUT_ALL;
    }

    private byte[]              key;                                    // also startKey for DELETE_RANGE
    private byte[]              value;                                  // also endKey for DELETE_RANGE
    private Object              attach;

    private byte                op;

    public static boolean isValidOp(final byte op) {
        return op > MAGIC && op < EOF;
    }

    /**
     * The best practice is to call this method only once
     * and keep a copy of it yourself.
     */
    public static byte[] getValidOps() {
        final byte[] copy = new byte[VALID_OPS.length];
        System.arraycopy(VALID_OPS, 0, copy, 0, copy.length);
        return copy;
    }

    public static KVOperation createPut(final byte[] key, final byte[] value) {
        Requires.requireNonNull(key, "key");
        Requires.requireNonNull(value, "value");
        return new KVOperation(key, value, null, PUT);
    }

    public static KVOperation createPutIfAbsent(final byte[] key, final byte[] value) {
        Requires.requireNonNull(key, "key");
        Requires.requireNonNull(value, "value");
        return new KVOperation(key, value, null, PUT_IF_ABSENT);
    }

    public static KVOperation createDelete(final byte[] key) {
        Requires.requireNonNull(key, "key");
        return new KVOperation(key, BytesUtil.EMPTY_BYTES, null, DELETE);
    }

    public static KVOperation createPutList(final List<KVEntry> entries) {
        Requires.requireNonNull(entries, "entries");
        Requires.requireTrue(!entries.isEmpty(), "entries is empty");
        return new KVOperation(BytesUtil.EMPTY_BYTES, BytesUtil.EMPTY_BYTES, entries, PUT_LIST);
    }

    public static KVOperation createDeleteRange(final byte[] startKey, final byte[] endKey) {
        Requires.requireNonNull(startKey, "startKey");
        Requires.requireNonNull(endKey, "endKey");
        return new KVOperation(startKey, endKey, null, DELETE_RANGE);
    }

    public static KVOperation createDeleteList(final List<byte[]> keys) {
        Requires.requireNonNull(keys, "keys");
        Requires.requireTrue(!keys.isEmpty(), "keys is empty");
        return new KVOperation(BytesUtil.EMPTY_BYTES, BytesUtil.EMPTY_BYTES, keys, DELETE_LIST);
    }

    public static KVOperation createGetSequence(final byte[] seqKey, final int step) {
        Requires.requireNonNull(seqKey, "seqKey");
        Requires.requireTrue(step > 0, "step must > 0");
        return new KVOperation(seqKey, BytesUtil.EMPTY_BYTES, step, GET_SEQUENCE);
    }

    public static KVOperation createNodeExecutor(final NodeExecutor nodeExecutor) {
        return new KVOperation(BytesUtil.EMPTY_BYTES, BytesUtil.EMPTY_BYTES, nodeExecutor, NODE_EXECUTE);
    }

    public static KVOperation createKeyLockRequest(final byte[] key, final byte[] fencingKey,
                                                   final Pair<Boolean, DistributedLock.Acquirer> acquirerPair) {
        Requires.requireNonNull(key, "key");
        return new KVOperation(key, fencingKey, acquirerPair, KEY_LOCK);
    }

    public static KVOperation createKeyLockReleaseRequest(final byte[] key, final DistributedLock.Acquirer acquirer) {
        Requires.requireNonNull(key, "key");
        return new KVOperation(key, BytesUtil.EMPTY_BYTES, acquirer, KEY_LOCK_RELEASE);
    }

    public static KVOperation createGet(final byte[] key) {
        Requires.requireNonNull(key, "key");
        return new KVOperation(key, BytesUtil.EMPTY_BYTES, null, GET);
    }

    public static KVOperation createMultiGet(final List<byte[]> keys) {
        Requires.requireNonNull(keys, "keys");
        Requires.requireTrue(!keys.isEmpty(), "keys is empty");
        return new KVOperation(BytesUtil.EMPTY_BYTES, BytesUtil.EMPTY_BYTES, keys, MULTI_GET);
    }

    public static KVOperation createContainsKey(final byte[] key) {
        Requires.requireNonNull(key, "key");
        return new KVOperation(key, BytesUtil.EMPTY_BYTES, null, CONTAINS_KEY);
    }

    public static KVOperation createScan(final byte[] startKey, final byte[] endKey, final int limit,
                                         final boolean returnValue) {
        return new KVOperation(startKey, endKey, Pair.of(limit, returnValue), SCAN);
    }

    public static KVOperation createReverseScan(final byte[] startKey, final byte[] endKey, final int limit,
                                                final boolean returnValue) {
        return new KVOperation(startKey, endKey, Pair.of(limit, returnValue), REVERSE_SCAN);
    }

    public static KVOperation createGetAndPut(final byte[] key, final byte[] value) {
        Requires.requireNonNull(key, "key");
        Requires.requireNonNull(value, "value");
        return new KVOperation(key, value, null, GET_PUT);
    }

    public static KVOperation createCompareAndPut(final byte[] key, final byte[] expect, final byte[] update) {
        Requires.requireNonNull(key, "key");
        Requires.requireNonNull(expect, "expect");
        Requires.requireNonNull(update, "update");
        return new KVOperation(key, update, expect, COMPARE_PUT);
    }

    public static KVOperation createMerge(final byte[] key, final byte[] value) {
        Requires.requireNonNull(key, "key");
        Requires.requireNonNull(value, "value");
        return new KVOperation(key, value, null, MERGE);
    }

    public static KVOperation createResetSequence(final byte[] seqKey) {
        Requires.requireNonNull(seqKey, "seqKey");
        return new KVOperation(seqKey, BytesUtil.EMPTY_BYTES, null, RESET_SEQUENCE);
    }

    public static KVOperation createRangeSplit(final byte[] splitKey, final long currentRegionId, final long newRegionId) {
        Requires.requireNonNull(splitKey, "splitKey");
        return new KVOperation(splitKey, BytesUtil.EMPTY_BYTES, Pair.of(currentRegionId, newRegionId), RANGE_SPLIT);
    }

    public static KVOperation createCompareAndPutAll(final List<CASEntry> entries) {
        Requires.requireNonNull(entries, "entries");
        Requires.requireTrue(!entries.isEmpty(), "entries is empty");
        return new KVOperation(BytesUtil.EMPTY_BYTES, BytesUtil.EMPTY_BYTES, entries, COMPARE_PUT_ALL);
    }

    public KVOperation() {
    }

    public KVOperation(byte[] key, byte[] value, Object attach, byte op) {
        this.key = key;
        this.value = value;
        this.attach = attach;
        this.op = op;
    }

    public byte[] getKey() {
        return key;
    }

    public byte[] getStartKey() {
        return key;
    }

    public byte[] getSeqKey() {
        return key;
    }

    public byte[] getValue() {
        return value;
    }

    public byte[] getEndKey() {
        return value;
    }

    public byte[] getFencingKey() {
        return value;
    }

    public byte getOp() {
        return op;
    }

    public boolean isReadOp() {
        return GET == this.op || MULTI_GET == this.op || SCAN == this.op || CONTAINS_KEY == this.op
               || REVERSE_SCAN == this.op;
    }

    public int getStep() {
        return (Integer) this.attach;
    }

    public byte[] getExpect() {
        return (byte[]) this.attach;
    }

    @SuppressWarnings("unchecked")
    public List<KVEntry> getEntries() {
        return (List<KVEntry>) this.attach;
    }

    @SuppressWarnings("unchecked")
    public List<CASEntry> getCASEntries() {
        return (List<CASEntry>) this.attach;
    }

    @SuppressWarnings("unchecked")
    public List<byte[]> getKeys() {
        return (List<byte[]>) this.attach;
    }

    public NodeExecutor getNodeExecutor() {
        return (NodeExecutor) this.attach;
    }

    @SuppressWarnings("unchecked")
    public Pair<Boolean, DistributedLock.Acquirer> getAcquirerPair() {
        return (Pair<Boolean, DistributedLock.Acquirer>) this.attach;
    }

    public DistributedLock.Acquirer getAcquirer() {
        return (DistributedLock.Acquirer) this.attach;
    }

    @SuppressWarnings("unchecked")
    public List<byte[]> getKeyList() {
        return (List<byte[]>) this.attach;
    }

    @SuppressWarnings("unchecked")
    public long getCurrentRegionId() {
        return ((Pair<Long, Long>) this.attach).getKey();
    }

    @SuppressWarnings("unchecked")
    public long getNewRegionId() {
        return ((Pair<Long, Long>) this.attach).getValue();
    }

    @SuppressWarnings("unchecked")
    public int getLimit() {
        if (this.attach instanceof Pair) {
            return ((Pair<Integer, Boolean>) this.attach).getKey();
        } else {
            // forwards compatibility
            return (Integer) this.attach;
        }
    }

    @SuppressWarnings("unchecked")
    public boolean isReturnValue() {
        if (this.attach instanceof Pair) {
            return ((Pair<Integer, Boolean>) this.attach).getValue();
        } else {
            // forwards compatibility
            return true;
        }
    }

    public static String opName(KVOperation op) {
        return opName(op.op);
    }

    public static String opName(byte op) {
        switch (op) {
            case PUT:
                return "PUT";
            case PUT_IF_ABSENT:
                return "PUT_IF_ABSENT";
            case DELETE:
                return "DELETE";
            case PUT_LIST:
                return "PUT_LIST";
            case DELETE_RANGE:
                return "DELETE_RANGE";
            case GET_SEQUENCE:
                return "GET_SEQUENCE";
            case NODE_EXECUTE:
                return "NODE_EXECUTE";
            case KEY_LOCK:
                return "KEY_LOCK";
            case KEY_LOCK_RELEASE:
                return "KEY_LOCK_RELEASE";
            case GET:
                return "GET";
            case MULTI_GET:
                return "MULTI_GET";
            case SCAN:
                return "SCAN";
            case GET_PUT:
                return "GET_PUT";
            case COMPARE_PUT:
                return "COMPARE_PUT";
            case MERGE:
                return "MERGE";
            case RESET_SEQUENCE:
                return "RESET_SEQUENCE";
            case RANGE_SPLIT:
                return "RANGE_SPLIT";
            case DELETE_LIST:
                return "DELETE_LIST";
            case CONTAINS_KEY:
                return "CONTAINS_KEY";
            case REVERSE_SCAN:
                return "REVERSE_SCAN";
            case COMPARE_PUT_ALL:
                return "COMPARE_PUT_ALL";
            default:
                return "UNKNOWN" + op;
        }
    }

    @Override
    public String toString() {
        return "KVOperation{" + "key=" + BytesUtil.toHex(key) + ", value=" + BytesUtil.toHex(value) + ", attach="
               + attach + ", op=" + op + '}';
    }
}
