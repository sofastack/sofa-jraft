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
import com.alipay.sofa.jraft.rhea.cmd.store.BatchPutRequest;
import com.alipay.sofa.jraft.rhea.cmd.store.BatchPutResponse;
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
import com.alipay.sofa.jraft.rhea.errors.InvalidParameterException;
import com.alipay.sofa.jraft.rhea.metadata.RegionEpoch;
import com.alipay.sofa.jraft.rhea.storage.BaseKVStoreClosure;
import com.alipay.sofa.jraft.rhea.storage.KVEntry;
import com.alipay.sofa.jraft.rhea.storage.NodeExecutor;
import com.alipay.sofa.jraft.rhea.storage.RawKVStore;
import com.alipay.sofa.jraft.rhea.storage.Sequence;
import com.alipay.sofa.jraft.rhea.util.ByteArray;
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
            checkRegionEpoch(request);
            final byte[] key = requireNonNull(request.getKey(), "put.key");
            final byte[] value = requireNonNull(request.getValue(), "put.value");
            this.rawKVStore.put(key, value, new BaseKVStoreClosure() {

                @Override
                public void run(Status status) {
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
            checkRegionEpoch(request);
            final List<KVEntry> kvEntries = requireNonEmpty(request.getKvEntries(), "put.kvEntries");
            this.rawKVStore.put(kvEntries, new BaseKVStoreClosure() {

                @Override
                public void run(Status status) {
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
            checkRegionEpoch(request);
            final byte[] key = requireNonNull(request.getKey(), "putIfAbsent.key");
            final byte[] value = requireNonNull(request.getValue(), "putIfAbsent.value");
            this.rawKVStore.putIfAbsent(key, value, new BaseKVStoreClosure() {

                @Override
                public void run(Status status) {
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
            checkRegionEpoch(request);
            final byte[] key = requireNonNull(request.getKey(), "getAndPut.key");
            final byte[] value = requireNonNull(request.getValue(), "getAndPut.value");
            this.rawKVStore.getAndPut(key, value, new BaseKVStoreClosure() {

                @Override
                public void run(Status status) {
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
    public void handleDeleteRequest(final DeleteRequest request,
                                    final RequestProcessClosure<BaseRequest, BaseResponse<?>> closure) {
        final DeleteResponse response = new DeleteResponse();
        response.setRegionId(getRegionId());
        response.setRegionEpoch(getRegionEpoch());
        try {
            checkRegionEpoch(request);
            final byte[] key = requireNonNull(request.getKey(), "delete.key");
            this.rawKVStore.delete(key, new BaseKVStoreClosure() {

                @Override
                public void run(Status status) {
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
            checkRegionEpoch(request);
            final byte[] startKey = requireNonNull(request.getStartKey(), "deleteRange.startKey");
            final byte[] endKey = requireNonNull(request.getEndKey(), "deleteRange.endKey");
            this.rawKVStore.deleteRange(startKey, endKey, new BaseKVStoreClosure() {

                @Override
                public void run(Status status) {
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
            checkRegionEpoch(request);
            final byte[] key = requireNonNull(request.getKey(), "merge.key");
            final byte[] value = requireNonNull(request.getValue(), "merge.value");
            this.rawKVStore.merge(key, value, new BaseKVStoreClosure() {

                @Override
                public void run(Status status) {
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
            checkRegionEpoch(request);
            final byte[] key = requireNonNull(request.getKey(), "get.key");
            this.rawKVStore.get(key, request.isReadOnlySafe(), new BaseKVStoreClosure() {

                @Override
                public void run(Status status) {
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
            checkRegionEpoch(request);
            final List<byte[]> keys = requireNonEmpty(request.getKeys(), "multiGet.keys");
            this.rawKVStore.multiGet(keys, request.isReadOnlySafe(), new BaseKVStoreClosure() {

                @SuppressWarnings("unchecked")
                @Override
                public void run(Status status) {
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
    public void handleScanRequest(final ScanRequest request,
                                  final RequestProcessClosure<BaseRequest, BaseResponse<?>> closure) {
        final ScanResponse response = new ScanResponse();
        response.setRegionId(getRegionId());
        response.setRegionEpoch(getRegionEpoch());
        try {
            checkRegionEpoch(request);
            this.rawKVStore.scan(request.getStartKey(), request.getEndKey(), request.getLimit(),
                request.isReadOnlySafe(), new BaseKVStoreClosure() {

                    @SuppressWarnings("unchecked")
                    @Override
                    public void run(Status status) {
                        if (status.isOk()) {
                            response.setValue((List<KVEntry>) getData());
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
    public void handleGetSequence(final GetSequenceRequest request,
                                  final RequestProcessClosure<BaseRequest, BaseResponse<?>> closure) {
        final GetSequenceResponse response = new GetSequenceResponse();
        response.setRegionId(getRegionId());
        response.setRegionEpoch(getRegionEpoch());
        try {
            checkRegionEpoch(request);
            final byte[] seqKey = requireNonNull(request.getSeqKey(), "sequence.seqKey");
            final int step = requirePositive(request.getStep(), "sequence.step");
            this.rawKVStore.getSequence(seqKey, step, new BaseKVStoreClosure() {

                @Override
                public void run(Status status) {
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
            checkRegionEpoch(request);
            final byte[] seqKey = requireNonNull(request.getSeqKey(), "sequence.seqKey");
            this.rawKVStore.resetSequence(seqKey, new BaseKVStoreClosure() {

                @Override
                public void run(Status status) {
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
            checkRegionEpoch(request);
            final byte[] key = requireNonNull(request.getKey(), "lock.key");
            final byte[] fencingKey = this.regionEngine.getRegion().getStartKey();
            final DistributedLock.Acquirer acquirer = requireNonNull(request.getAcquirer(), "lock.acquirer");
            requireNonNull(acquirer.getId(), "lock.id");
            requirePositive(acquirer.getLeaseMillis(), "lock.leaseMillis");
            this.rawKVStore.tryLockWith(key, fencingKey, request.isKeepLease(), acquirer, new BaseKVStoreClosure() {

                @Override
                public void run(Status status) {
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
            checkRegionEpoch(request);
            final byte[] key = requireNonNull(request.getKey(), "unlock.key");
            final DistributedLock.Acquirer acquirer = requireNonNull(request.getAcquirer(), "lock.acquirer");
            requireNonNull(acquirer.getId(), "lock.id");
            this.rawKVStore.releaseLockWith(key, acquirer, new BaseKVStoreClosure() {

                @Override
                public void run(Status status) {
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
            checkRegionEpoch(request);
            final NodeExecutor executor = requireNonNull(request.getNodeExecutor(), "node.executor");
            this.rawKVStore.execute(executor, true, new BaseKVStoreClosure() {

                @Override
                public void run(Status status) {
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
            final Long newRegionId = requireNonNull(request.getNewRegionId(), "rangeSplit.newRegionId");
            this.regionEngine.getStoreEngine().applySplit(request.getRegionId(), newRegionId, new BaseKVStoreClosure() {

                @Override
                public void run(Status status) {
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

    void checkRegionEpoch(final BaseRequest request) {
        RegionEpoch currentEpoch = getRegionEpoch();
        RegionEpoch requestEpoch = request.getRegionEpoch();
        if (currentEpoch.equals(requestEpoch)) {
            return;
        }
        if (currentEpoch.getConfVer() != requestEpoch.getConfVer()) {
            throw Errors.INVALID_REGION_MEMBERSHIP.exception();
        }
        if (currentEpoch.getVersion() != requestEpoch.getVersion()) {
            throw Errors.INVALID_REGION_VERSION.exception();
        }
        throw Errors.INVALID_REGION_EPOCH.exception();
    }

    private static <T> T requireNonNull(final T target, final String message) {
        if (target == null) {
            throw new InvalidParameterException(message);
        }
        return target;
    }

    private static <T> List<T> requireNonEmpty(final List<T> target, final String message) {
        requireNonNull(target, message);
        if (target.isEmpty()) {
            throw new InvalidParameterException(message);
        }
        return target;
    }

    @SuppressWarnings("SameParameterValue")
    private static int requirePositive(final int value, final String message) {
        if (value <= 0) {
            throw new InvalidParameterException(message);
        }
        return value;
    }

    @SuppressWarnings("SameParameterValue")
    private static long requirePositive(final long value, final String message) {
        if (value <= 0) {
            throw new InvalidParameterException(message);
        }
        return value;
    }

    private static void setFailure(final BaseRequest request, final BaseResponse<?> response, final Status status,
                                   final Errors error) {
        response.setError(error);
        LOG.error("Failed to handle: {}, status: {}, error: {}.", request, status, error);
    }
}
