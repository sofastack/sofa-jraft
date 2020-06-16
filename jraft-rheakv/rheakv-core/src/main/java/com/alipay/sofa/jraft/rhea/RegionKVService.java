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

import com.alipay.sofa.jraft.rhea.cmd.proto.RheakvRpc;
import com.alipay.sofa.jraft.rhea.metadata.RegionEpoch;

/**
 * Request processing service on the KV server side.
 * <p>
 * A {@link StoreEngine} contains many {@link RegionKVService}s,
 * each {@link RegionKVService} corresponds to a region, and it
 * only processes BaseRequest keys within its own region.
 *
 * @author jiachun.fjc
 */
public interface RegionKVService {

    long getRegionId();

    RegionEpoch getRegionEpoch();

    /**
     * {@link RheakvRpc.PutRequest}
     */
    void handlePutRequest(final RheakvRpc.BaseRequest baseRequest, final RheakvRpc.PutRequest request,
                          final RequestProcessClosure<RheakvRpc.BaseRequest, RheakvRpc.BaseResponse> closure);

    /**
     * {@link RheakvRpc.BatchPutRequest}
     */
    void handleBatchPutRequest(final RheakvRpc.BaseRequest baseRequest, final RheakvRpc.BatchPutRequest request,
                               final RequestProcessClosure<RheakvRpc.BaseRequest, RheakvRpc.BaseResponse> closure);

    /**
     * {@link RheakvRpc.PutIfAbsentRequest}
     */
    void handlePutIfAbsentRequest(final RheakvRpc.BaseRequest baseRequest, final RheakvRpc.PutIfAbsentRequest request,
                                  final RequestProcessClosure<RheakvRpc.BaseRequest, RheakvRpc.BaseResponse> closure);

    /**
     * {@link RheakvRpc.GetAndPutRequest}
     */
    void handleGetAndPutRequest(final RheakvRpc.BaseRequest baseRequest, final RheakvRpc.GetAndPutRequest request,
                                final RequestProcessClosure<RheakvRpc.BaseRequest, RheakvRpc.BaseResponse> closure);

    /**
     * {@link RheakvRpc.CompareAndPutRequest}
     */
    void handleCompareAndPutRequest(final RheakvRpc.BaseRequest baseRequest,
                                    final RheakvRpc.CompareAndPutRequest request,
                                    final RequestProcessClosure<RheakvRpc.BaseRequest, RheakvRpc.BaseResponse> closure);

    /**
     * {@link RheakvRpc.DeleteRequest}
     */
    void handleDeleteRequest(final RheakvRpc.BaseRequest baseRequest, final RheakvRpc.DeleteRequest request,
                             final RequestProcessClosure<RheakvRpc.BaseRequest, RheakvRpc.BaseResponse> closure);

    /**
     * {@link RheakvRpc.DeleteRangeRequest}
     */
    void handleDeleteRangeRequest(final RheakvRpc.BaseRequest baseRequest, final RheakvRpc.DeleteRangeRequest request,
                                  final RequestProcessClosure<RheakvRpc.BaseRequest, RheakvRpc.BaseResponse> closure);

    /**
     * {@link RheakvRpc.BatchDeleteRequest}
     */
    void handleBatchDeleteRequest(final RheakvRpc.BaseRequest baseRequest, final RheakvRpc.BatchDeleteRequest request,
                                  final RequestProcessClosure<RheakvRpc.BaseRequest, RheakvRpc.BaseResponse> closure);

    /**
     * {@link RheakvRpc.MergeRequest}
     */
    void handleMergeRequest(final RheakvRpc.BaseRequest baseRequest, final RheakvRpc.MergeRequest request,
                            final RequestProcessClosure<RheakvRpc.BaseRequest, RheakvRpc.BaseResponse> closure);

    /**
     * {@link RheakvRpc.GetRequest}
     */
    void handleGetRequest(final RheakvRpc.BaseRequest baseRequest, final RheakvRpc.GetRequest request,
                          final RequestProcessClosure<RheakvRpc.BaseRequest, RheakvRpc.BaseResponse> closure);

    /**
     * {@link RheakvRpc.MultiGetRequest}
     */
    void handleMultiGetRequest(final RheakvRpc.BaseRequest baseRequest, final RheakvRpc.MultiGetRequest request,
                               final RequestProcessClosure<RheakvRpc.BaseRequest, RheakvRpc.BaseResponse> closure);

    /**
     * {@link RheakvRpc.ContainsKeyRequest}
     */
    void handleContainsKeyRequest(final RheakvRpc.BaseRequest baseRequest, final RheakvRpc.ContainsKeyRequest request,
                                  final RequestProcessClosure<RheakvRpc.BaseRequest, RheakvRpc.BaseResponse> closure);

    /**
     * {@link RheakvRpc.ScanRequest}
     */
    void handleScanRequest(final RheakvRpc.BaseRequest baseRequest, final RheakvRpc.ScanRequest request,
                           final RequestProcessClosure<RheakvRpc.BaseRequest, RheakvRpc.BaseResponse> closure);

    /**
     * {@link RheakvRpc.GetSequenceRequest}
     */
    void handleGetSequence(final RheakvRpc.BaseRequest baseRequest, final RheakvRpc.GetSequenceRequest request,
                           final RequestProcessClosure<RheakvRpc.BaseRequest, RheakvRpc.BaseResponse> closure);

    /**
     * {@link RheakvRpc.ResetSequenceRequest}
     */
    void handleResetSequence(final RheakvRpc.BaseRequest baseRequest, final RheakvRpc.ResetSequenceRequest request,
                             final RequestProcessClosure<RheakvRpc.BaseRequest, RheakvRpc.BaseResponse> closure);

    /**
     * {@link RheakvRpc.KeyLockRequest}
     */
    void handleKeyLockRequest(final RheakvRpc.BaseRequest baseRequest, final RheakvRpc.KeyLockRequest request,
                              final RequestProcessClosure<RheakvRpc.BaseRequest, RheakvRpc.BaseResponse> closure);

    /**
     * {@link RheakvRpc.KeyUnlockRequest}
     */
    void handleKeyUnlockRequest(final RheakvRpc.BaseRequest baseRequest, final RheakvRpc.KeyUnlockRequest request,
                                final RequestProcessClosure<RheakvRpc.BaseRequest, RheakvRpc.BaseResponse> closure);

    /**
     * {@link RheakvRpc.NodeExecuteRequest}
     */
    void handleNodeExecuteRequest(final RheakvRpc.BaseRequest baseRequest, final RheakvRpc.NodeExecuteRequest request,
                                  final RequestProcessClosure<RheakvRpc.BaseRequest, RheakvRpc.BaseResponse> closure);

    /**
     * {@link RheakvRpc.RangeSplitRequest}
     */
    void handleRangeSplitRequest(final RheakvRpc.BaseRequest baseRequest, final RheakvRpc.RangeSplitRequest request,
                                 final RequestProcessClosure<RheakvRpc.BaseRequest, RheakvRpc.BaseResponse> closure);
}
