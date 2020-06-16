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
package com.alipay.sofa.jraft.rhea;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.rhea.cmd.proto.RheakvRpc;
import com.alipay.sofa.jraft.rhea.errors.Errors;
import com.alipay.sofa.jraft.rhea.metadata.RegionEpoch;
import com.alipay.sofa.jraft.rhea.serialization.JavaSerializer;
import com.alipay.sofa.jraft.rhea.storage.BaseKVStoreClosure;
import com.alipay.sofa.jraft.rhea.storage.KVEntry;
import com.alipay.sofa.jraft.rhea.storage.NodeExecutor;
import com.alipay.sofa.jraft.rhea.storage.RawKVStore;
import com.alipay.sofa.jraft.rhea.util.KVParameterRequires;
import com.alipay.sofa.jraft.rhea.util.StackTraceUtil;
import com.alipay.sofa.jraft.rhea.util.concurrent.DistributedLock;
import com.google.protobuf.ByteString;

/**
 * Rhea KV region RPC request processing service.
 *
 * @author jiachun.fjc
 */
public class DefaultRegionKVService implements RegionKVService {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultRegionKVService.class);

    private final RegionEngine  regionEngine;
    private final RawKVStore    rawKVStore;

    public DefaultRegionKVService(RegionEngine regionEngine) {
        this.regionEngine = regionEngine;
        this.rawKVStore = regionEngine.getMetricsRawKVStore();
    }

    @Override
    public long getRegionId() {
        return this.regionEngine.getRegion().getId();
    }

    @Override
    public RegionEpoch getRegionEpoch() {
        return this.regionEngine.getRegion().getRegionEpoch();
    }

    @Override
    public void handlePutRequest(final RheakvRpc.BaseRequest baseRequest, final RheakvRpc.PutRequest request,
                                 final RequestProcessClosure<RheakvRpc.BaseRequest, RheakvRpc.BaseResponse> closure) {
        final RheakvRpc.BaseResponse.Builder response = RheakvRpc.BaseResponse.newBuilder().setRegionId(getRegionId())
            .setConfVer(getRegionEpoch().getConfVer()).setVersion(getRegionEpoch().getVersion())
            .setError(JavaSerializer.serializeByteString(Errors.NONE));
        try {
            KVParameterRequires.requireSameEpoch(baseRequest, getRegionEpoch());
            final byte[] key = KVParameterRequires.requireNonNull(request.getKey().toByteArray(), "put.key");
            final byte[] value = KVParameterRequires.requireNonNull(request.getValue().toByteArray(), "put.value");
            this.rawKVStore.put(key, value, new BaseKVStoreClosure() {
                @Override
                public void run(final Status status) {
                    if (status.isOk()) {
                        response.setValue(JavaSerializer.serializeByteString(getData()));
                    } else {
                        setFailure(baseRequest, response, status, getError());
                    }
                    closure.sendResponse(response.build());
                }
            });
        } catch (final Throwable t) {
            LOG.error("Failed to handle: {}, {}.", baseRequest, StackTraceUtil.stackTrace(t));
            response.setError(JavaSerializer.serializeByteString(Errors.forException(t)));
            closure.sendResponse(response.build());
        }
    }

    @Override
    public void handleBatchPutRequest(final RheakvRpc.BaseRequest baseRequest, final RheakvRpc.BatchPutRequest request,
                                      final RequestProcessClosure<RheakvRpc.BaseRequest, RheakvRpc.BaseResponse> closure) {
        final RheakvRpc.BaseResponse.Builder response = RheakvRpc.BaseResponse.newBuilder().setRegionId(getRegionId())
            .setConfVer(getRegionEpoch().getConfVer()).setVersion(getRegionEpoch().getVersion())
            .setError(JavaSerializer.serializeByteString(Errors.NONE));
        try {
            KVParameterRequires.requireSameEpoch(baseRequest, getRegionEpoch());
            final List<ByteString> kvEntryBytes = KVParameterRequires.requireNonEmpty(request.getKvEntriesList(),
                "put.kvEntries");
            List<KVEntry> kvEntries = new ArrayList<>();
            kvEntryBytes.forEach(kvEntryByte -> kvEntries.add((KVEntry) JavaSerializer.deserialize(kvEntryByte.toByteArray())));
            this.rawKVStore.put(kvEntries, new BaseKVStoreClosure() {

                @Override
                public void run(final Status status) {
                    if (status.isOk()) {
                        response.setValue(JavaSerializer.serializeByteString(getData()));
                    } else {
                        setFailure(baseRequest, response, status, getError());
                    }
                    closure.sendResponse(response.build());
                }
            });
        } catch (final Throwable t) {
            LOG.error("Failed to handle: {}, {}.", baseRequest, StackTraceUtil.stackTrace(t));
            response.setError(JavaSerializer.serializeByteString(Errors.forException(t)));
            closure.sendResponse(response.build());
        }
    }

    @Override
    public void handlePutIfAbsentRequest(final RheakvRpc.BaseRequest baseRequest,
                                         final RheakvRpc.PutIfAbsentRequest request,
                                         final RequestProcessClosure<RheakvRpc.BaseRequest, RheakvRpc.BaseResponse> closure) {
        final RheakvRpc.BaseResponse.Builder response = RheakvRpc.BaseResponse.newBuilder().setRegionId(getRegionId())
            .setConfVer(getRegionEpoch().getConfVer()).setVersion(getRegionEpoch().getVersion())
            .setError(JavaSerializer.serializeByteString(Errors.NONE));
        try {
            KVParameterRequires.requireSameEpoch(baseRequest, getRegionEpoch());
            final byte[] key = KVParameterRequires.requireNonNull(request.getKey().toByteArray(), "putIfAbsent.key");
            final byte[] value = KVParameterRequires.requireNonNull(request.getValue().toByteArray(),
                "putIfAbsent.value");
            this.rawKVStore.putIfAbsent(key, value, new BaseKVStoreClosure() {

                @Override
                public void run(final Status status) {
                    if (status.isOk()) {
                        response.setValue(JavaSerializer.serializeByteString(getData()));
                    } else {
                        setFailure(baseRequest, response, status, getError());
                    }
                    closure.sendResponse(response.build());
                }
            });
        } catch (final Throwable t) {
            LOG.error("Failed to handle: {}, {}.", baseRequest, StackTraceUtil.stackTrace(t));
            response.setError(JavaSerializer.serializeByteString(Errors.forException(t)));
            closure.sendResponse(response.build());
        }
    }

    @Override
    public void handleGetAndPutRequest(final RheakvRpc.BaseRequest baseRequest,
                                       final RheakvRpc.GetAndPutRequest request,
                                       final RequestProcessClosure<RheakvRpc.BaseRequest, RheakvRpc.BaseResponse> closure) {
        final RheakvRpc.BaseResponse.Builder response = RheakvRpc.BaseResponse.newBuilder().setRegionId(getRegionId())
            .setConfVer(getRegionEpoch().getConfVer()).setVersion(getRegionEpoch().getVersion())
            .setError(JavaSerializer.serializeByteString(Errors.NONE));
        try {
            KVParameterRequires.requireSameEpoch(baseRequest, getRegionEpoch());
            final byte[] key = KVParameterRequires.requireNonNull(request.getKey().toByteArray(), "getAndPut.key");
            final byte[] value = KVParameterRequires
                .requireNonNull(request.getValue().toByteArray(), "getAndPut.value");
            this.rawKVStore.getAndPut(key, value, new BaseKVStoreClosure() {
                @Override
                public void run(final Status status) {
                    if (status.isOk()) {
                        response.setValue(JavaSerializer.serializeByteString(getData()));
                    } else {
                        setFailure(baseRequest, response, status, getError());
                    }
                    closure.sendResponse(response.build());
                }
            });
        } catch (final Throwable t) {
            LOG.error("Failed to handle: {}, {}.", request, StackTraceUtil.stackTrace(t));
            response.setError(JavaSerializer.serializeByteString(Errors.forException(t)));
            closure.sendResponse(response.build());
        }
    }

    @Override
    public void handleCompareAndPutRequest(final RheakvRpc.BaseRequest baseRequest,
                                           final RheakvRpc.CompareAndPutRequest request,
                                           final RequestProcessClosure<RheakvRpc.BaseRequest, RheakvRpc.BaseResponse> closure) {
        final RheakvRpc.BaseResponse.Builder response = RheakvRpc.BaseResponse.newBuilder().setRegionId(getRegionId())
            .setConfVer(getRegionEpoch().getConfVer()).setVersion(getRegionEpoch().getVersion())
            .setError(JavaSerializer.serializeByteString(Errors.NONE));
        try {
            KVParameterRequires.requireSameEpoch(baseRequest, getRegionEpoch());
            final byte[] key = KVParameterRequires.requireNonNull(request.getKey().toByteArray(), "compareAndPut.key");
            final byte[] expect = KVParameterRequires.requireNonNull(request.getExpect().toByteArray(),
                "compareAndPut.expect");
            final byte[] update = KVParameterRequires.requireNonNull(request.getUpdate().toByteArray(),
                "compareAndPut.update");
            this.rawKVStore.compareAndPut(key, expect, update, new BaseKVStoreClosure() {

                @Override
                public void run(final Status status) {
                    if (status.isOk()) {
                        response.setValue(JavaSerializer.serializeByteString(getData()));
                    } else {
                        setFailure(baseRequest, response, status, getError());
                    }
                    closure.sendResponse(response.build());
                }
            });
        } catch (final Throwable t) {
            LOG.error("Failed to handle: {}, {}.", request, StackTraceUtil.stackTrace(t));
            response.setError(JavaSerializer.serializeByteString(Errors.forException(t)));
            closure.sendResponse(response.build());
        }
    }

    @Override
    public void handleDeleteRequest(final RheakvRpc.BaseRequest baseRequest, final RheakvRpc.DeleteRequest request,
                                    final RequestProcessClosure<RheakvRpc.BaseRequest, RheakvRpc.BaseResponse> closure) {
        final RheakvRpc.BaseResponse.Builder response = RheakvRpc.BaseResponse.newBuilder().setRegionId(getRegionId())
            .setConfVer(getRegionEpoch().getConfVer()).setVersion(getRegionEpoch().getVersion())
            .setError(JavaSerializer.serializeByteString(Errors.NONE));
        try {
            KVParameterRequires.requireSameEpoch(baseRequest, getRegionEpoch());
            final byte[] key = KVParameterRequires.requireNonNull(request.getKey().toByteArray(), "delete.key");
            this.rawKVStore.delete(key, new BaseKVStoreClosure() {

                @Override
                public void run(final Status status) {
                    if (status.isOk()) {
                        response.setValue(JavaSerializer.serializeByteString(getData()));
                    } else {
                        setFailure(baseRequest, response, status, getError());
                    }
                    closure.sendResponse(response.build());
                }
            });
        } catch (final Throwable t) {
            LOG.error("Failed to handle: {}, {}.", request, StackTraceUtil.stackTrace(t));
            response.setError(JavaSerializer.serializeByteString(Errors.forException(t)));
            closure.sendResponse(response.build());
        }
    }

    @Override
    public void handleDeleteRangeRequest(final RheakvRpc.BaseRequest baseRequest,
                                         final RheakvRpc.DeleteRangeRequest request,
                                         final RequestProcessClosure<RheakvRpc.BaseRequest, RheakvRpc.BaseResponse> closure) {
        final RheakvRpc.BaseResponse.Builder response = RheakvRpc.BaseResponse.newBuilder().setRegionId(getRegionId())
            .setConfVer(getRegionEpoch().getConfVer()).setVersion(getRegionEpoch().getVersion())
            .setError(JavaSerializer.serializeByteString(Errors.NONE));
        try {
            KVParameterRequires.requireSameEpoch(baseRequest, getRegionEpoch());
            final byte[] startKey = KVParameterRequires.requireNonNull(request.getStartKey().toByteArray(),
                "deleteRange.startKey");
            final byte[] endKey = KVParameterRequires.requireNonNull(request.getEndKey().toByteArray(),
                "deleteRange.endKey");
            this.rawKVStore.deleteRange(startKey, endKey, new BaseKVStoreClosure() {

                @Override
                public void run(final Status status) {
                    if (status.isOk()) {
                        response.setValue(JavaSerializer.serializeByteString(getData()));
                    } else {
                        setFailure(baseRequest, response, status, getError());
                    }
                    closure.sendResponse(response.build());
                }
            });
        } catch (final Throwable t) {
            LOG.error("Failed to handle: {}, {}.", request, StackTraceUtil.stackTrace(t));
            response.setError(JavaSerializer.serializeByteString(Errors.forException(t)));
            closure.sendResponse(response.build());
        }
    }

    @Override
    public void handleBatchDeleteRequest(final RheakvRpc.BaseRequest baseRequest,
                                         final RheakvRpc.BatchDeleteRequest request,
                                         final RequestProcessClosure<RheakvRpc.BaseRequest, RheakvRpc.BaseResponse> closure) {
        final RheakvRpc.BaseResponse.Builder response = RheakvRpc.BaseResponse.newBuilder().setRegionId(getRegionId())
            .setConfVer(getRegionEpoch().getConfVer()).setVersion(getRegionEpoch().getVersion())
            .setError(JavaSerializer.serializeByteString(Errors.NONE));
        try {
            KVParameterRequires.requireSameEpoch(baseRequest, getRegionEpoch());
            final List<ByteString> keyBytes = KVParameterRequires.requireNonEmpty(request.getKeysList(), "delete.keys");
            List<byte[]> keys = new ArrayList<>();
            for (ByteString keyByte : keyBytes) {
                keys.add(keyByte.toByteArray());
            }
            this.rawKVStore.delete(keys, new BaseKVStoreClosure() {
                @Override
                public void run(final Status status) {
                    if (status.isOk()) {
                        response.setValue(JavaSerializer.serializeByteString(getData()));
                    } else {
                        setFailure(baseRequest, response, status, getError());
                    }
                    closure.sendResponse(response.build());
                }
            });
        } catch (final Throwable t) {
            LOG.error("Failed to handle: {}, {}.", request, StackTraceUtil.stackTrace(t));
            response.setError(JavaSerializer.serializeByteString(Errors.forException(t)));
            closure.sendResponse(response.build());
        }
    }

    @Override
    public void handleMergeRequest(final RheakvRpc.BaseRequest baseRequest, final RheakvRpc.MergeRequest request,
                                   final RequestProcessClosure<RheakvRpc.BaseRequest, RheakvRpc.BaseResponse> closure) {
        final RheakvRpc.BaseResponse.Builder response = RheakvRpc.BaseResponse.newBuilder().setRegionId(getRegionId())
            .setConfVer(getRegionEpoch().getConfVer()).setVersion(getRegionEpoch().getVersion())
            .setError(JavaSerializer.serializeByteString(Errors.NONE));
        try {
            KVParameterRequires.requireSameEpoch(baseRequest, getRegionEpoch());
            final byte[] key = KVParameterRequires.requireNonNull(request.getKey().toByteArray(), "merge.key");
            final byte[] value = KVParameterRequires.requireNonNull(request.getValue().toByteArray(), "merge.value");
            this.rawKVStore.merge(key, value, new BaseKVStoreClosure() {

                @Override
                public void run(final Status status) {
                    if (status.isOk()) {
                        response.setValue(JavaSerializer.serializeByteString(getData()));
                    } else {
                        setFailure(baseRequest, response, status, getError());
                    }
                    closure.sendResponse(response.build());
                }
            });
        } catch (final Throwable t) {
            LOG.error("Failed to handle: {}, {}.", request, StackTraceUtil.stackTrace(t));
            response.setError(JavaSerializer.serializeByteString(Errors.forException(t)));
            closure.sendResponse(response.build());
        }
    }

    @Override
    public void handleGetRequest(final RheakvRpc.BaseRequest baseRequest, final RheakvRpc.GetRequest request,
                                 final RequestProcessClosure<RheakvRpc.BaseRequest, RheakvRpc.BaseResponse> closure) {
        final RheakvRpc.BaseResponse.Builder response = RheakvRpc.BaseResponse.newBuilder().setRegionId(getRegionId())
            .setConfVer(getRegionEpoch().getConfVer()).setVersion(getRegionEpoch().getVersion())
            .setError(JavaSerializer.serializeByteString(Errors.NONE));
        try {
            KVParameterRequires.requireSameEpoch(baseRequest, getRegionEpoch());
            final byte[] key = KVParameterRequires.requireNonNull(request.getKey().toByteArray(), "get.key");
            this.rawKVStore.get(key, request.getReadOnlySafe(), new BaseKVStoreClosure() {
                @Override
                public void run(final Status status) {
                    if (status.isOk()) {
                        response.setValue(JavaSerializer.serializeByteString(getData()));
                    } else {
                        setFailure(baseRequest, response, status, getError());
                    }
                    closure.sendResponse(response.build());
                }
            });
        } catch (final Throwable t) {
            LOG.error("Failed to handle: {}, {}.", request, StackTraceUtil.stackTrace(t));
            response.setError(JavaSerializer.serializeByteString(Errors.forException(t)));
            closure.sendResponse(response.build());
        }
    }

    @Override
    public void handleMultiGetRequest(final RheakvRpc.BaseRequest baseRequest, final RheakvRpc.MultiGetRequest request,
                                      final RequestProcessClosure<RheakvRpc.BaseRequest, RheakvRpc.BaseResponse> closure) {
        final RheakvRpc.BaseResponse.Builder response = RheakvRpc.BaseResponse.newBuilder().setRegionId(getRegionId())
            .setConfVer(getRegionEpoch().getConfVer()).setVersion(getRegionEpoch().getVersion())
            .setError(JavaSerializer.serializeByteString(Errors.NONE));
        try {
            KVParameterRequires.requireSameEpoch(baseRequest, getRegionEpoch());
            final List<ByteString> keyBytes = KVParameterRequires.requireNonEmpty(request.getKeysList(),
                "multiGet.keys");
            List<byte[]> keys = new ArrayList<>();
            for (ByteString keyByte : keyBytes) {
                keys.add(keyByte.toByteArray());
            }
            this.rawKVStore.multiGet(keys, request.getReadOnlySafe(), new BaseKVStoreClosure() {

                @SuppressWarnings("unchecked")
                @Override
                public void run(final Status status) {
                    if (status.isOk()) {
                        response.setValue(JavaSerializer.serializeByteString(getData()));
                    } else {
                        setFailure(baseRequest, response, status, getError());
                    }
                    closure.sendResponse(response.build());
                }
            });
        } catch (final Throwable t) {
            LOG.error("Failed to handle: {}, {}.", request, StackTraceUtil.stackTrace(t));
            response.setError(JavaSerializer.serializeByteString(Errors.forException(t)));
            closure.sendResponse(response.build());
        }
    }

    @Override
    public void handleContainsKeyRequest(final RheakvRpc.BaseRequest baseRequest,
                                         final RheakvRpc.ContainsKeyRequest request,
                                         final RequestProcessClosure<RheakvRpc.BaseRequest, RheakvRpc.BaseResponse> closure) {
        final RheakvRpc.BaseResponse.Builder response = RheakvRpc.BaseResponse.newBuilder().setRegionId(getRegionId())
            .setConfVer(getRegionEpoch().getConfVer()).setVersion(getRegionEpoch().getVersion())
            .setError(JavaSerializer.serializeByteString(Errors.NONE));
        try {
            KVParameterRequires.requireSameEpoch(baseRequest, getRegionEpoch());
            final byte[] key = KVParameterRequires.requireNonNull(request.getKey().toByteArray(), "containsKey.key");
            this.rawKVStore.containsKey(key, new BaseKVStoreClosure() {

                @Override
                public void run(final Status status) {
                    if (status.isOk()) {
                        response.setValue(JavaSerializer.serializeByteString(getData()));
                    } else {
                        setFailure(baseRequest, response, status, getError());
                    }
                    closure.sendResponse(response.build());
                }
            });
        } catch (final Throwable t) {
            LOG.error("Failed to handle: {}, {}.", request, StackTraceUtil.stackTrace(t));
            response.setError(JavaSerializer.serializeByteString(Errors.forException(t)));
            closure.sendResponse(response.build());
        }
    }

    @Override
    public void handleScanRequest(final RheakvRpc.BaseRequest baseRequest, final RheakvRpc.ScanRequest request,
                                  final RequestProcessClosure<RheakvRpc.BaseRequest, RheakvRpc.BaseResponse> closure) {
        final RheakvRpc.BaseResponse.Builder response = RheakvRpc.BaseResponse.newBuilder().setRegionId(getRegionId())
            .setConfVer(getRegionEpoch().getConfVer()).setVersion(getRegionEpoch().getVersion())
            .setError(JavaSerializer.serializeByteString(Errors.NONE));
        try {
            KVParameterRequires.requireSameEpoch(baseRequest, getRegionEpoch());
            final BaseKVStoreClosure kvStoreClosure = new BaseKVStoreClosure() {

                @SuppressWarnings("unchecked")
                @Override
                public void run(final Status status) {
                    if (status.isOk()) {
                        response.setValue(JavaSerializer.serializeByteString(getData()));
                    } else {
                        setFailure(baseRequest, response, status, getError());
                    }
                    closure.sendResponse(response.build());
                }
            };
            if (request.getReverse()) {
                this.rawKVStore.reverseScan(request.getStartKey().toByteArray(), request.getEndKey().toByteArray(),
                    request.getLimit(), request.getReadOnlySafe(), request.getReturnValue(), kvStoreClosure);
            } else {
                this.rawKVStore.scan(request.getStartKey().toByteArray(), request.getEndKey().toByteArray(),
                    request.getLimit(), request.getReadOnlySafe(), request.getReturnValue(), kvStoreClosure);
            }
        } catch (final Throwable t) {
            LOG.error("Failed to handle: {}, {}.", request, StackTraceUtil.stackTrace(t));
            response.setError(JavaSerializer.serializeByteString(Errors.forException(t)));
            closure.sendResponse(response.build());
        }
    }

    @Override
    public void handleGetSequence(final RheakvRpc.BaseRequest baseRequest, final RheakvRpc.GetSequenceRequest request,
                                  final RequestProcessClosure<RheakvRpc.BaseRequest, RheakvRpc.BaseResponse> closure) {
        final RheakvRpc.BaseResponse.Builder response = RheakvRpc.BaseResponse.newBuilder().setRegionId(getRegionId())
            .setConfVer(getRegionEpoch().getConfVer()).setVersion(getRegionEpoch().getVersion())
            .setError(JavaSerializer.serializeByteString(Errors.NONE));
        try {
            KVParameterRequires.requireSameEpoch(baseRequest, getRegionEpoch());
            final byte[] seqKey = KVParameterRequires.requireNonNull(request.getSeqKey().toByteArray(),
                "sequence.seqKey");
            final int step = KVParameterRequires.requireNonNegative(request.getStep(), "sequence.step");
            this.rawKVStore.getSequence(seqKey, step, new BaseKVStoreClosure() {

                @Override
                public void run(final Status status) {
                    if (status.isOk()) {
                        response.setValue(JavaSerializer.serializeByteString(getData()));
                    } else {
                        setFailure(baseRequest, response, status, getError());
                    }
                    closure.sendResponse(response.build());
                }
            });
        } catch (final Throwable t) {
            LOG.error("Failed to handle: {}, {}.", request, StackTraceUtil.stackTrace(t));
            response.setError(JavaSerializer.serializeByteString(Errors.forException(t)));
            closure.sendResponse(response.build());
        }
    }

    @Override
    public void handleResetSequence(final RheakvRpc.BaseRequest baseRequest,
                                    final RheakvRpc.ResetSequenceRequest request,
                                    final RequestProcessClosure<RheakvRpc.BaseRequest, RheakvRpc.BaseResponse> closure) {
        final RheakvRpc.BaseResponse.Builder response = RheakvRpc.BaseResponse.newBuilder().setRegionId(getRegionId())
            .setConfVer(getRegionEpoch().getConfVer()).setVersion(getRegionEpoch().getVersion())
            .setError(JavaSerializer.serializeByteString(Errors.NONE));
        try {
            KVParameterRequires.requireSameEpoch(baseRequest, getRegionEpoch());
            final byte[] seqKey = KVParameterRequires.requireNonNull(request.getSeqKey().toByteArray(),
                "sequence.seqKey");
            this.rawKVStore.resetSequence(seqKey, new BaseKVStoreClosure() {

                @Override
                public void run(final Status status) {
                    if (status.isOk()) {
                        response.setValue(JavaSerializer.serializeByteString(getData()));
                    } else {
                        setFailure(baseRequest, response, status, getError());
                    }
                    closure.sendResponse(response.build());
                }
            });
        } catch (final Throwable t) {
            LOG.error("Failed to handle: {}, {}.", request, StackTraceUtil.stackTrace(t));
            response.setError(JavaSerializer.serializeByteString(Errors.forException(t)));
            closure.sendResponse(response.build());
        }
    }

    @Override
    public void handleKeyLockRequest(final RheakvRpc.BaseRequest baseRequest, final RheakvRpc.KeyLockRequest request,
                                     final RequestProcessClosure<RheakvRpc.BaseRequest, RheakvRpc.BaseResponse> closure) {
        final RheakvRpc.BaseResponse.Builder response = RheakvRpc.BaseResponse.newBuilder().setRegionId(getRegionId())
            .setConfVer(getRegionEpoch().getConfVer()).setVersion(getRegionEpoch().getVersion())
            .setError(JavaSerializer.serializeByteString(Errors.NONE));
        try {
            KVParameterRequires.requireSameEpoch(baseRequest, getRegionEpoch());
            final byte[] key = KVParameterRequires.requireNonNull(request.getKey().toByteArray(), "lock.key");
            final byte[] fencingKey = this.regionEngine.getRegion().getStartKey();
            final DistributedLock.Acquirer acquirer = KVParameterRequires.requireNonNull(
                (DistributedLock.Acquirer) JavaSerializer.deserialize(request.getAcquirer().toByteArray()),
                "lock.acquirer");
            KVParameterRequires.requireNonNull(acquirer.getId(), "lock.id");
            KVParameterRequires.requirePositive(acquirer.getLeaseMillis(), "lock.leaseMillis");
            this.rawKVStore.tryLockWith(key, fencingKey, request.getKeepLease(), acquirer, new BaseKVStoreClosure() {

                @Override
                public void run(final Status status) {
                    if (status.isOk()) {
                        response.setValue(JavaSerializer.serializeByteString(getData()));
                    } else {
                        setFailure(baseRequest, response, status, getError());
                    }
                    closure.sendResponse(response.build());
                }
            });
        } catch (final Throwable t) {
            LOG.error("Failed to handle: {}, {}.", request, StackTraceUtil.stackTrace(t));
            response.setError(JavaSerializer.serializeByteString(Errors.forException(t)));
            closure.sendResponse(response.build());
        }
    }

    @Override
    public void handleKeyUnlockRequest(final RheakvRpc.BaseRequest baseRequest,
                                       final RheakvRpc.KeyUnlockRequest request,
                                       final RequestProcessClosure<RheakvRpc.BaseRequest, RheakvRpc.BaseResponse> closure) {
        final RheakvRpc.BaseResponse.Builder response = RheakvRpc.BaseResponse.newBuilder().setRegionId(getRegionId())
            .setConfVer(getRegionEpoch().getConfVer()).setVersion(getRegionEpoch().getVersion())
            .setError(JavaSerializer.serializeByteString(Errors.NONE));
        try {
            KVParameterRequires.requireSameEpoch(baseRequest, getRegionEpoch());
            final byte[] key = KVParameterRequires.requireNonNull(request.getKey().toByteArray(), "unlock.key");
            final DistributedLock.Acquirer acquirer = KVParameterRequires.requireNonNull(
                (DistributedLock.Acquirer) JavaSerializer.deserialize(request.getAcquirer().toByteArray()),
                "lock.acquirer");
            KVParameterRequires.requireNonNull(acquirer.getId(), "lock.id");
            this.rawKVStore.releaseLockWith(key, acquirer, new BaseKVStoreClosure() {

                @Override
                public void run(final Status status) {
                    if (status.isOk()) {
                        response.setValue(JavaSerializer.serializeByteString(getData()));
                    } else {
                        setFailure(baseRequest, response, status, getError());
                    }
                    closure.sendResponse(response.build());
                }
            });
        } catch (final Throwable t) {
            LOG.error("Failed to handle: {}, {}.", request, StackTraceUtil.stackTrace(t));
            response.setError(JavaSerializer.serializeByteString(Errors.forException(t)));
            closure.sendResponse(response.build());
        }
    }

    @Override
    public void handleNodeExecuteRequest(final RheakvRpc.BaseRequest baseRequest,
                                         final RheakvRpc.NodeExecuteRequest request,
                                         final RequestProcessClosure<RheakvRpc.BaseRequest, RheakvRpc.BaseResponse> closure) {
        final RheakvRpc.BaseResponse.Builder response = RheakvRpc.BaseResponse.newBuilder().setRegionId(getRegionId())
            .setConfVer(getRegionEpoch().getConfVer()).setVersion(getRegionEpoch().getVersion())
            .setError(JavaSerializer.serializeByteString(Errors.NONE));
        try {
            KVParameterRequires.requireSameEpoch(baseRequest, getRegionEpoch());
            final NodeExecutor executor = KVParameterRequires.requireNonNull(
                (NodeExecutor) JavaSerializer.deserialize(request.getNodeExecutor().toByteArray()), "node.executor");
            this.rawKVStore.execute(executor, true, new BaseKVStoreClosure() {

                @Override
                public void run(final Status status) {
                    if (status.isOk()) {
                        response.setValue(JavaSerializer.serializeByteString(getData()));
                    } else {
                        setFailure(baseRequest, response, status, getError());
                    }
                    closure.sendResponse(response.build());
                }
            });
        } catch (final Throwable t) {
            LOG.error("Failed to handle: {}, {}.", request, StackTraceUtil.stackTrace(t));
            response.setError(JavaSerializer.serializeByteString(Errors.forException(t)));
            closure.sendResponse(response.build());
        }
    }

    @Override
    public void handleRangeSplitRequest(final RheakvRpc.BaseRequest baseRequest,
                                        final RheakvRpc.RangeSplitRequest request,
                                        final RequestProcessClosure<RheakvRpc.BaseRequest, RheakvRpc.BaseResponse> closure) {
        final RheakvRpc.BaseResponse.Builder response = RheakvRpc.BaseResponse.newBuilder().setRegionId(getRegionId())
            .setConfVer(getRegionEpoch().getConfVer()).setVersion(getRegionEpoch().getVersion())
            .setError(JavaSerializer.serializeByteString(Errors.NONE));
        try {
            // do not need to check the region epoch
            final Long newRegionId = KVParameterRequires.requireNonNull(request.getNewRegionId(),
                "rangeSplit.newRegionId");
            this.regionEngine.getStoreEngine().applySplit(baseRequest.getRegionId(), newRegionId,
                new BaseKVStoreClosure() {

                    @Override
                    public void run(final Status status) {
                        if (status.isOk()) {
                            response.setValue(JavaSerializer.serializeByteString(getData()));
                        } else {
                            setFailure(baseRequest, response, status, getError());
                        }
                        closure.sendResponse(response.build());
                    }
                });
        } catch (final Throwable t) {
            LOG.error("Failed to handle: {}, {}.", request, StackTraceUtil.stackTrace(t));
            response.setError(JavaSerializer.serializeByteString(Errors.forException(t)));
            closure.sendResponse(response.build());
        }
    }

    private static void setFailure(final RheakvRpc.BaseRequest request, final RheakvRpc.BaseResponse.Builder response,
                                   final Status status, final Errors error) {
        response.setError(JavaSerializer.serializeByteString(error == null ? Errors.STORAGE_ERROR : error));
        LOG.error("Failed to handle: {}, status: {}, error: {}.", request, status, error);
    }
}
