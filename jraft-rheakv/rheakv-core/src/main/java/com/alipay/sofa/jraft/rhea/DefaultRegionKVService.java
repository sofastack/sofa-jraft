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

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.rhea.cmd.store.BaseRequest;
import com.alipay.sofa.jraft.rhea.cmd.store.BaseResponse;
import com.alipay.sofa.jraft.rhea.cmd.store.CASAllRequest;
import com.alipay.sofa.jraft.rhea.cmd.store.CASAllResponse;
import com.alipay.sofa.jraft.rhea.cmd.store.BatchDeleteRequest;
import com.alipay.sofa.jraft.rhea.cmd.store.BatchDeleteResponse;
import com.alipay.sofa.jraft.rhea.cmd.store.BatchPutRequest;
import com.alipay.sofa.jraft.rhea.cmd.store.BatchPutResponse;
import com.alipay.sofa.jraft.rhea.cmd.store.CompareAndPutRequest;
import com.alipay.sofa.jraft.rhea.cmd.store.CompareAndPutResponse;
import com.alipay.sofa.jraft.rhea.cmd.store.ContainsKeyRequest;
import com.alipay.sofa.jraft.rhea.cmd.store.ContainsKeyResponse;
import com.alipay.sofa.jraft.rhea.cmd.store.DeleteRangeRequest;
import com.alipay.sofa.jraft.rhea.cmd.store.DeleteRangeResponse;
import com.alipay.sofa.jraft.rhea.cmd.store.DeleteRequest;
import com.alipay.sofa.jraft.rhea.cmd.store.DeleteResponse;
import com.alipay.sofa.jraft.rhea.cmd.store.GetAndPutRequest;
import com.alipay.sofa.jraft.rhea.cmd.store.GetAndPutResponse;
import com.alipay.sofa.jraft.rhea.cmd.store.GetRequest;
import com.alipay.sofa.jraft.rhea.cmd.store.GetResponse;
import com.alipay.sofa.jraft.rhea.cmd.store.GetSequenceRequest;
import com.alipay.sofa.jraft.rhea.cmd.store.GetSequenceResponse;
import com.alipay.sofa.jraft.rhea.cmd.store.KeyLockRequest;
import com.alipay.sofa.jraft.rhea.cmd.store.KeyLockResponse;
import com.alipay.sofa.jraft.rhea.cmd.store.KeyUnlockRequest;
import com.alipay.sofa.jraft.rhea.cmd.store.KeyUnlockResponse;
import com.alipay.sofa.jraft.rhea.cmd.store.MergeRequest;
import com.alipay.sofa.jraft.rhea.cmd.store.MergeResponse;
import com.alipay.sofa.jraft.rhea.cmd.store.MultiGetRequest;
import com.alipay.sofa.jraft.rhea.cmd.store.MultiGetResponse;
import com.alipay.sofa.jraft.rhea.cmd.store.NodeExecuteRequest;
import com.alipay.sofa.jraft.rhea.cmd.store.NodeExecuteResponse;
import com.alipay.sofa.jraft.rhea.cmd.store.PutIfAbsentRequest;
import com.alipay.sofa.jraft.rhea.cmd.store.PutIfAbsentResponse;
import com.alipay.sofa.jraft.rhea.cmd.store.PutRequest;
import com.alipay.sofa.jraft.rhea.cmd.store.PutResponse;
import com.alipay.sofa.jraft.rhea.cmd.store.RangeSplitRequest;
import com.alipay.sofa.jraft.rhea.cmd.store.RangeSplitResponse;
import com.alipay.sofa.jraft.rhea.cmd.store.ResetSequenceRequest;
import com.alipay.sofa.jraft.rhea.cmd.store.ResetSequenceResponse;
import com.alipay.sofa.jraft.rhea.cmd.store.ScanRequest;
import com.alipay.sofa.jraft.rhea.cmd.store.ScanResponse;
import com.alipay.sofa.jraft.rhea.errors.Errors;
import com.alipay.sofa.jraft.rhea.metadata.RegionEpoch;
import com.alipay.sofa.jraft.rhea.storage.BaseKVStoreClosure;
import com.alipay.sofa.jraft.rhea.storage.CASEntry;
import com.alipay.sofa.jraft.rhea.storage.KVEntry;
import com.alipay.sofa.jraft.rhea.storage.NodeExecutor;
import com.alipay.sofa.jraft.rhea.storage.RawKVStore;
import com.alipay.sofa.jraft.rhea.storage.Sequence;
import com.alipay.sofa.jraft.rhea.util.ByteArray;
import com.alipay.sofa.jraft.rhea.util.KVParameterRequires;
import com.alipay.sofa.jraft.rhea.util.StackTraceUtil;
import com.alipay.sofa.jraft.rhea.util.concurrent.DistributedLock;

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
    public void handlePutRequest(final PutRequest request,
                                 final RequestProcessClosure<BaseRequest, BaseResponse<?>> closure) {
        final PutResponse response = new PutResponse();
        response.setRegionId(getRegionId());
        response.setRegionEpoch(getRegionEpoch());
        try {
            KVParameterRequires.requireSameEpoch(request, getRegionEpoch());
            final byte[] key = KVParameterRequires.requireNonNull(request.getKey(), "put.key");
            final byte[] value = KVParameterRequires.requireNonNull(request.getValue(), "put.value");
            this.rawKVStore.put(key, value, new BaseKVStoreClosure() {

                @Override
                public void run(final Status status) {
                    if (status.isOk()) {
                        response.setValue((Boolean) getData());
                    } else {
                        setFailure(request, response, status, getError());
                    }
                    closure.sendResponse(response);
                }
            });
        } catch (final Throwable t) {
            LOG.error("Failed to handle: {}, {}.", request, StackTraceUtil.stackTrace(t));
            response.setError(Errors.forException(t));
            closure.sendResponse(response);
        }
    }

    @Override
    public void handleBatchPutRequest(final BatchPutRequest request,
                                      final RequestProcessClosure<BaseRequest, BaseResponse<?>> closure) {
        final BatchPutResponse response = new BatchPutResponse();
        response.setRegionId(getRegionId());
        response.setRegionEpoch(getRegionEpoch());
        try {
            KVParameterRequires.requireSameEpoch(request, getRegionEpoch());
            final List<KVEntry> kvEntries = KVParameterRequires
                .requireNonEmpty(request.getKvEntries(), "put.kvEntries");
            this.rawKVStore.put(kvEntries, new BaseKVStoreClosure() {

                @Override
                public void run(final Status status) {
                    if (status.isOk()) {
                        response.setValue((Boolean) getData());
                    } else {
                        setFailure(request, response, status, getError());
                    }
                    closure.sendResponse(response);
                }
            });
        } catch (final Throwable t) {
            LOG.error("Failed to handle: {}, {}.", request, StackTraceUtil.stackTrace(t));
            response.setError(Errors.forException(t));
            closure.sendResponse(response);
        }
    }

    @Override
    public void handlePutIfAbsentRequest(final PutIfAbsentRequest request,
                                         final RequestProcessClosure<BaseRequest, BaseResponse<?>> closure) {
        final PutIfAbsentResponse response = new PutIfAbsentResponse();
        response.setRegionId(getRegionId());
        response.setRegionEpoch(getRegionEpoch());
        try {
            KVParameterRequires.requireSameEpoch(request, getRegionEpoch());
            final byte[] key = KVParameterRequires.requireNonNull(request.getKey(), "putIfAbsent.key");
            final byte[] value = KVParameterRequires.requireNonNull(request.getValue(), "putIfAbsent.value");
            this.rawKVStore.putIfAbsent(key, value, new BaseKVStoreClosure() {

                @Override
                public void run(final Status status) {
                    if (status.isOk()) {
                        response.setValue((byte[]) getData());
                    } else {
                        setFailure(request, response, status, getError());
                    }
                    closure.sendResponse(response);
                }
            });
        } catch (final Throwable t) {
            LOG.error("Failed to handle: {}, {}.", request, StackTraceUtil.stackTrace(t));
            response.setError(Errors.forException(t));
            closure.sendResponse(response);
        }
    }

    @Override
    public void handleGetAndPutRequest(final GetAndPutRequest request,
                                       final RequestProcessClosure<BaseRequest, BaseResponse<?>> closure) {
        final GetAndPutResponse response = new GetAndPutResponse();
        response.setRegionId(getRegionId());
        response.setRegionEpoch(getRegionEpoch());
        try {
            KVParameterRequires.requireSameEpoch(request, getRegionEpoch());
            final byte[] key = KVParameterRequires.requireNonNull(request.getKey(), "getAndPut.key");
            final byte[] value = KVParameterRequires.requireNonNull(request.getValue(), "getAndPut.value");
            this.rawKVStore.getAndPut(key, value, new BaseKVStoreClosure() {

                @Override
                public void run(final Status status) {
                    if (status.isOk()) {
                        response.setValue((byte[]) getData());
                    } else {
                        setFailure(request, response, status, getError());
                    }
                    closure.sendResponse(response);
                }
            });
        } catch (final Throwable t) {
            LOG.error("Failed to handle: {}, {}.", request, StackTraceUtil.stackTrace(t));
            response.setError(Errors.forException(t));
            closure.sendResponse(response);
        }
    }

    @Override
    public void handleCompareAndPutRequest(final CompareAndPutRequest request,
                                           final RequestProcessClosure<BaseRequest, BaseResponse<?>> closure) {
        final CompareAndPutResponse response = new CompareAndPutResponse();
        response.setRegionId(getRegionId());
        response.setRegionEpoch(getRegionEpoch());
        try {
            KVParameterRequires.requireSameEpoch(request, getRegionEpoch());
            final byte[] key = KVParameterRequires.requireNonNull(request.getKey(), "compareAndPut.key");
            final byte[] expect = KVParameterRequires.requireNonNull(request.getExpect(), "compareAndPut.expect");
            final byte[] update = KVParameterRequires.requireNonNull(request.getUpdate(), "compareAndPut.update");
            this.rawKVStore.compareAndPut(key, expect, update, new BaseKVStoreClosure() {

                @Override
                public void run(final Status status) {
                    if (status.isOk()) {
                        response.setValue((Boolean) getData());
                    } else {
                        setFailure(request, response, status, getError());
                    }
                    closure.sendResponse(response);
                }
            });
        } catch (final Throwable t) {
            LOG.error("Failed to handle: {}, {}.", request, StackTraceUtil.stackTrace(t));
            response.setError(Errors.forException(t));
            closure.sendResponse(response);
        }
    }

    @Override
    public void handleDeleteRequest(final DeleteRequest request,
                                    final RequestProcessClosure<BaseRequest, BaseResponse<?>> closure) {
        final DeleteResponse response = new DeleteResponse();
        response.setRegionId(getRegionId());
        response.setRegionEpoch(getRegionEpoch());
        try {
            KVParameterRequires.requireSameEpoch(request, getRegionEpoch());
            final byte[] key = KVParameterRequires.requireNonNull(request.getKey(), "delete.key");
            this.rawKVStore.delete(key, new BaseKVStoreClosure() {

                @Override
                public void run(final Status status) {
                    if (status.isOk()) {
                        response.setValue((Boolean) getData());
                    } else {
                        setFailure(request, response, status, getError());
                    }
                    closure.sendResponse(response);
                }
            });
        } catch (final Throwable t) {
            LOG.error("Failed to handle: {}, {}.", request, StackTraceUtil.stackTrace(t));
            response.setError(Errors.forException(t));
            closure.sendResponse(response);
        }
    }

    @Override
    public void handleDeleteRangeRequest(final DeleteRangeRequest request,
                                         final RequestProcessClosure<BaseRequest, BaseResponse<?>> closure) {
        final DeleteRangeResponse response = new DeleteRangeResponse();
        response.setRegionId(getRegionId());
        response.setRegionEpoch(getRegionEpoch());
        try {
            KVParameterRequires.requireSameEpoch(request, getRegionEpoch());
            final byte[] startKey = KVParameterRequires.requireNonNull(request.getStartKey(), "deleteRange.startKey");
            final byte[] endKey = KVParameterRequires.requireNonNull(request.getEndKey(), "deleteRange.endKey");
            this.rawKVStore.deleteRange(startKey, endKey, new BaseKVStoreClosure() {

                @Override
                public void run(final Status status) {
                    if (status.isOk()) {
                        response.setValue((Boolean) getData());
                    } else {
                        setFailure(request, response, status, getError());
                    }
                    closure.sendResponse(response);
                }
            });
        } catch (final Throwable t) {
            LOG.error("Failed to handle: {}, {}.", request, StackTraceUtil.stackTrace(t));
            response.setError(Errors.forException(t));
            closure.sendResponse(response);
        }
    }

    @Override
    public void handleBatchDeleteRequest(final BatchDeleteRequest request,
                                         final RequestProcessClosure<BaseRequest, BaseResponse<?>> closure) {
        final BatchDeleteResponse response = new BatchDeleteResponse();
        response.setRegionId(getRegionId());
        response.setRegionEpoch(getRegionEpoch());
        try {
            KVParameterRequires.requireSameEpoch(request, getRegionEpoch());
            final List<byte[]> keys = KVParameterRequires.requireNonEmpty(request.getKeys(), "delete.keys");
            this.rawKVStore.delete(keys, new BaseKVStoreClosure() {

                @Override
                public void run(final Status status) {
                    if (status.isOk()) {
                        response.setValue((Boolean) getData());
                    } else {
                        setFailure(request, response, status, getError());
                    }
                    closure.sendResponse(response);
                }
            });
        } catch (final Throwable t) {
            LOG.error("Failed to handle: {}, {}.", request, StackTraceUtil.stackTrace(t));
            response.setError(Errors.forException(t));
            closure.sendResponse(response);
        }
    }

    @Override
    public void handleMergeRequest(final MergeRequest request,
                                   final RequestProcessClosure<BaseRequest, BaseResponse<?>> closure) {
        final MergeResponse response = new MergeResponse();
        response.setRegionId(getRegionId());
        response.setRegionEpoch(getRegionEpoch());
        try {
            KVParameterRequires.requireSameEpoch(request, getRegionEpoch());
            final byte[] key = KVParameterRequires.requireNonNull(request.getKey(), "merge.key");
            final byte[] value = KVParameterRequires.requireNonNull(request.getValue(), "merge.value");
            this.rawKVStore.merge(key, value, new BaseKVStoreClosure() {

                @Override
                public void run(final Status status) {
                    if (status.isOk()) {
                        response.setValue((Boolean) getData());
                    } else {
                        setFailure(request, response, status, getError());
                    }
                    closure.sendResponse(response);
                }
            });
        } catch (final Throwable t) {
            LOG.error("Failed to handle: {}, {}.", request, StackTraceUtil.stackTrace(t));
            response.setError(Errors.forException(t));
            closure.sendResponse(response);
        }
    }

    @Override
    public void handleGetRequest(final GetRequest request,
                                 final RequestProcessClosure<BaseRequest, BaseResponse<?>> closure) {
        final GetResponse response = new GetResponse();
        response.setRegionId(getRegionId());
        response.setRegionEpoch(getRegionEpoch());
        try {
            KVParameterRequires.requireSameEpoch(request, getRegionEpoch());
            final byte[] key = KVParameterRequires.requireNonNull(request.getKey(), "get.key");
            this.rawKVStore.get(key, request.isReadOnlySafe(), new BaseKVStoreClosure() {

                @Override
                public void run(final Status status) {
                    if (status.isOk()) {
                        response.setValue((byte[]) getData());
                    } else {
                        setFailure(request, response, status, getError());
                    }
                    closure.sendResponse(response);
                }
            });
        } catch (final Throwable t) {
            LOG.error("Failed to handle: {}, {}.", request, StackTraceUtil.stackTrace(t));
            response.setError(Errors.forException(t));
            closure.sendResponse(response);
        }
    }

    @Override
    public void handleMultiGetRequest(final MultiGetRequest request,
                                      final RequestProcessClosure<BaseRequest, BaseResponse<?>> closure) {
        final MultiGetResponse response = new MultiGetResponse();
        response.setRegionId(getRegionId());
        response.setRegionEpoch(getRegionEpoch());
        try {
            KVParameterRequires.requireSameEpoch(request, getRegionEpoch());
            final List<byte[]> keys = KVParameterRequires.requireNonEmpty(request.getKeys(), "multiGet.keys");
            this.rawKVStore.multiGet(keys, request.isReadOnlySafe(), new BaseKVStoreClosure() {

                @SuppressWarnings("unchecked")
                @Override
                public void run(final Status status) {
                    if (status.isOk()) {
                        response.setValue((Map<ByteArray, byte[]>) getData());
                    } else {
                        setFailure(request, response, status, getError());
                    }
                    closure.sendResponse(response);
                }
            });
        } catch (final Throwable t) {
            LOG.error("Failed to handle: {}, {}.", request, StackTraceUtil.stackTrace(t));
            response.setError(Errors.forException(t));
            closure.sendResponse(response);
        }
    }

    @Override
    public void handleContainsKeyRequest(final ContainsKeyRequest request,
                                         final RequestProcessClosure<BaseRequest, BaseResponse<?>> closure) {
        final ContainsKeyResponse response = new ContainsKeyResponse();
        response.setRegionId(getRegionId());
        response.setRegionEpoch(getRegionEpoch());
        try {
            KVParameterRequires.requireSameEpoch(request, getRegionEpoch());
            final byte[] key = KVParameterRequires.requireNonNull(request.getKey(), "containsKey.key");
            this.rawKVStore.containsKey(key, new BaseKVStoreClosure() {

                @Override
                public void run(final Status status) {
                    if (status.isOk()) {
                        response.setValue((Boolean) getData());
                    } else {
                        setFailure(request, response, status, getError());
                    }
                    closure.sendResponse(response);
                }
            });
        } catch (final Throwable t) {
            LOG.error("Failed to handle: {}, {}.", request, StackTraceUtil.stackTrace(t));
            response.setError(Errors.forException(t));
            closure.sendResponse(response);
        }
    }

    @Override
    public void handleScanRequest(final ScanRequest request,
                                  final RequestProcessClosure<BaseRequest, BaseResponse<?>> closure) {
        final ScanResponse response = new ScanResponse();
        response.setRegionId(getRegionId());
        response.setRegionEpoch(getRegionEpoch());
        try {
            KVParameterRequires.requireSameEpoch(request, getRegionEpoch());
            final BaseKVStoreClosure kvStoreClosure = new BaseKVStoreClosure() {

                @SuppressWarnings("unchecked")
                @Override
                public void run(final Status status) {
                    if (status.isOk()) {
                        response.setValue((List<KVEntry>) getData());
                    } else {
                        setFailure(request, response, status, getError());
                    }
                    closure.sendResponse(response);
                }
            };
            if (request.isReverse()) {
                this.rawKVStore.reverseScan(request.getStartKey(), request.getEndKey(), request.getLimit(),
                    request.isReadOnlySafe(), request.isReturnValue(), kvStoreClosure);
            } else {
                this.rawKVStore.scan(request.getStartKey(), request.getEndKey(), request.getLimit(),
                    request.isReadOnlySafe(), request.isReturnValue(), kvStoreClosure);
            }
        } catch (final Throwable t) {
            LOG.error("Failed to handle: {}, {}.", request, StackTraceUtil.stackTrace(t));
            response.setError(Errors.forException(t));
            closure.sendResponse(response);
        }
    }

    @Override
    public void handleGetSequence(final GetSequenceRequest request,
                                  final RequestProcessClosure<BaseRequest, BaseResponse<?>> closure) {
        final GetSequenceResponse response = new GetSequenceResponse();
        response.setRegionId(getRegionId());
        response.setRegionEpoch(getRegionEpoch());
        try {
            KVParameterRequires.requireSameEpoch(request, getRegionEpoch());
            final byte[] seqKey = KVParameterRequires.requireNonNull(request.getSeqKey(), "sequence.seqKey");
            final int step = KVParameterRequires.requireNonNegative(request.getStep(), "sequence.step");
            this.rawKVStore.getSequence(seqKey, step, new BaseKVStoreClosure() {

                @Override
                public void run(final Status status) {
                    if (status.isOk()) {
                        response.setValue((Sequence) getData());
                    } else {
                        setFailure(request, response, status, getError());
                    }
                    closure.sendResponse(response);
                }
            });
        } catch (final Throwable t) {
            LOG.error("Failed to handle: {}, {}.", request, StackTraceUtil.stackTrace(t));
            response.setError(Errors.forException(t));
            closure.sendResponse(response);
        }
    }

    @Override
    public void handleResetSequence(final ResetSequenceRequest request,
                                    final RequestProcessClosure<BaseRequest, BaseResponse<?>> closure) {
        final ResetSequenceResponse response = new ResetSequenceResponse();
        response.setRegionId(getRegionId());
        response.setRegionEpoch(getRegionEpoch());
        try {
            KVParameterRequires.requireSameEpoch(request, getRegionEpoch());
            final byte[] seqKey = KVParameterRequires.requireNonNull(request.getSeqKey(), "sequence.seqKey");
            this.rawKVStore.resetSequence(seqKey, new BaseKVStoreClosure() {

                @Override
                public void run(final Status status) {
                    if (status.isOk()) {
                        response.setValue((Boolean) getData());
                    } else {
                        setFailure(request, response, status, getError());
                    }
                    closure.sendResponse(response);
                }
            });
        } catch (final Throwable t) {
            LOG.error("Failed to handle: {}, {}.", request, StackTraceUtil.stackTrace(t));
            response.setError(Errors.forException(t));
            closure.sendResponse(response);
        }
    }

    @Override
    public void handleKeyLockRequest(final KeyLockRequest request,
                                     final RequestProcessClosure<BaseRequest, BaseResponse<?>> closure) {
        final KeyLockResponse response = new KeyLockResponse();
        response.setRegionId(getRegionId());
        response.setRegionEpoch(getRegionEpoch());
        try {
            KVParameterRequires.requireSameEpoch(request, getRegionEpoch());
            final byte[] key = KVParameterRequires.requireNonNull(request.getKey(), "lock.key");
            final byte[] fencingKey = this.regionEngine.getRegion().getStartKey();
            final DistributedLock.Acquirer acquirer = KVParameterRequires.requireNonNull(request.getAcquirer(),
                "lock.acquirer");
            KVParameterRequires.requireNonNull(acquirer.getId(), "lock.id");
            KVParameterRequires.requirePositive(acquirer.getLeaseMillis(), "lock.leaseMillis");
            this.rawKVStore.tryLockWith(key, fencingKey, request.isKeepLease(), acquirer, new BaseKVStoreClosure() {

                @Override
                public void run(final Status status) {
                    if (status.isOk()) {
                        response.setValue((DistributedLock.Owner) getData());
                    } else {
                        setFailure(request, response, status, getError());
                    }
                    closure.sendResponse(response);
                }
            });
        } catch (final Throwable t) {
            LOG.error("Failed to handle: {}, {}.", request, StackTraceUtil.stackTrace(t));
            response.setError(Errors.forException(t));
            closure.sendResponse(response);
        }
    }

    @Override
    public void handleKeyUnlockRequest(final KeyUnlockRequest request,
                                       final RequestProcessClosure<BaseRequest, BaseResponse<?>> closure) {
        final KeyUnlockResponse response = new KeyUnlockResponse();
        response.setRegionId(getRegionId());
        response.setRegionEpoch(getRegionEpoch());
        try {
            KVParameterRequires.requireSameEpoch(request, getRegionEpoch());
            final byte[] key = KVParameterRequires.requireNonNull(request.getKey(), "unlock.key");
            final DistributedLock.Acquirer acquirer = KVParameterRequires.requireNonNull(request.getAcquirer(),
                "lock.acquirer");
            KVParameterRequires.requireNonNull(acquirer.getId(), "lock.id");
            this.rawKVStore.releaseLockWith(key, acquirer, new BaseKVStoreClosure() {

                @Override
                public void run(final Status status) {
                    if (status.isOk()) {
                        response.setValue((DistributedLock.Owner) getData());
                    } else {
                        setFailure(request, response, status, getError());
                    }
                    closure.sendResponse(response);
                }
            });
        } catch (final Throwable t) {
            LOG.error("Failed to handle: {}, {}.", request, StackTraceUtil.stackTrace(t));
            response.setError(Errors.forException(t));
            closure.sendResponse(response);
        }
    }

    @Override
    public void handleNodeExecuteRequest(final NodeExecuteRequest request,
                                         final RequestProcessClosure<BaseRequest, BaseResponse<?>> closure) {
        final NodeExecuteResponse response = new NodeExecuteResponse();
        response.setRegionId(getRegionId());
        response.setRegionEpoch(getRegionEpoch());
        try {
            KVParameterRequires.requireSameEpoch(request, getRegionEpoch());
            final NodeExecutor executor = KVParameterRequires
                .requireNonNull(request.getNodeExecutor(), "node.executor");
            this.rawKVStore.execute(executor, true, new BaseKVStoreClosure() {

                @Override
                public void run(final Status status) {
                    if (status.isOk()) {
                        response.setValue((Boolean) getData());
                    } else {
                        setFailure(request, response, status, getError());
                    }
                    closure.sendResponse(response);
                }
            });
        } catch (final Throwable t) {
            LOG.error("Failed to handle: {}, {}.", request, StackTraceUtil.stackTrace(t));
            response.setError(Errors.forException(t));
            closure.sendResponse(response);
        }
    }

    @Override
    public void handleRangeSplitRequest(final RangeSplitRequest request,
                                        final RequestProcessClosure<BaseRequest, BaseResponse<?>> closure) {
        final RangeSplitResponse response = new RangeSplitResponse();
        response.setRegionId(getRegionId());
        response.setRegionEpoch(getRegionEpoch());
        try {
            // do not need to check the region epoch
            final Long newRegionId = KVParameterRequires.requireNonNull(request.getNewRegionId(),
                "rangeSplit.newRegionId");
            this.regionEngine.getStoreEngine().applySplit(request.getRegionId(), newRegionId, new BaseKVStoreClosure() {

                @Override
                public void run(final Status status) {
                    if (status.isOk()) {
                        response.setValue((Boolean) getData());
                    } else {
                        setFailure(request, response, status, getError());
                    }
                    closure.sendResponse(response);
                }
            });
        } catch (final Throwable t) {
            LOG.error("Failed to handle: {}, {}.", request, StackTraceUtil.stackTrace(t));
            response.setError(Errors.forException(t));
            closure.sendResponse(response);
        }
    }

    @Override
    public void handleCompareAndPutAll(final CASAllRequest request,
                                       final RequestProcessClosure<BaseRequest, BaseResponse<?>> closure) {
        final CASAllResponse response = new CASAllResponse();
        response.setRegionId(getRegionId());
        response.setRegionEpoch(getRegionEpoch());
        try {
            KVParameterRequires.requireSameEpoch(request, getRegionEpoch());
            final List<CASEntry> casEntries = KVParameterRequires.requireNonEmpty(request.getCasEntries(),
                "casAll.casEntries");
            this.rawKVStore.compareAndPutAll(casEntries, new BaseKVStoreClosure() {

                @Override
                public void run(final Status status) {
                    if (status.isOk()) {
                        response.setValue((Boolean) getData());
                    } else {
                        setFailure(request, response, status, getError());
                    }
                    closure.sendResponse(response);
                }
            });
        } catch (final Throwable t) {
            LOG.error("Failed to handle: {}, {}.", request, StackTraceUtil.stackTrace(t));
            response.setError(Errors.forException(t));
            closure.sendResponse(response);
        }

    }

    private static void setFailure(final BaseRequest request, final BaseResponse<?> response, final Status status,
                                   final Errors error) {
        response.setError(error == null ? Errors.STORAGE_ERROR : error);
        LOG.error("Failed to handle: {}, status: {}, error: {}.", request, status, error);
    }
}
