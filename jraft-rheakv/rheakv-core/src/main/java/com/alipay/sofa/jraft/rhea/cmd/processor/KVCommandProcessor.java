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
package com.alipay.sofa.jraft.rhea.cmd.processor;

import java.util.concurrent.Executor;

import com.alipay.sofa.jraft.rhea.RegionKVService;
import com.alipay.sofa.jraft.rhea.RequestProcessClosure;
import com.alipay.sofa.jraft.rhea.StoreEngine;
import com.alipay.sofa.jraft.rhea.cmd.proto.RheakvRpc;
import com.alipay.sofa.jraft.rhea.errors.Errors;
import com.alipay.sofa.jraft.rhea.errors.RheaRuntimeException;
import com.alipay.sofa.jraft.rhea.serialization.JavaSerializer;
import com.alipay.sofa.jraft.rpc.RpcContext;
import com.alipay.sofa.jraft.rpc.RpcProcessor;
import com.alipay.sofa.jraft.util.Requires;

/**
 * Rhea KV store RPC request processing service.
 *
 * @author baozi
 */
public class KVCommandProcessor implements RpcProcessor<RheakvRpc.BaseRequest> {

    private final StoreEngine storeEngine;

    public KVCommandProcessor(StoreEngine storeEngine) {
        this.storeEngine = Requires.requireNonNull(storeEngine, "storeEngine");
    }

    @Override
    public void handleRequest(RpcContext rpcCtx, RheakvRpc.BaseRequest baseRequest) {

        Requires.requireNonNull(baseRequest, "request");
        final RequestProcessClosure<RheakvRpc.BaseRequest, RheakvRpc.BaseResponse> closure = new RequestProcessClosure<>(
            baseRequest, rpcCtx);
        final RegionKVService regionKVService = this.storeEngine.getRegionKVService(baseRequest.getRegionId());
        if (regionKVService == null) {
            RheakvRpc.BaseResponse response = RheakvRpc.BaseResponse.newBuilder()
                .setRegionId(baseRequest.getRegionId())
                .setError(JavaSerializer.serializeByteString(Errors.NO_REGION_FOUND))
                .setResponseType(RheakvRpc.BaseResponse.ResponseType.noRegionFund)
                .setValue(JavaSerializer.serializeByteString(false)).build();
            closure.sendResponse(response);
            return;
        }
        RheakvRpc.BaseRequest.RequestType requestType = baseRequest.getRequestType();
        switch (requestType) {
            case get:
                RheakvRpc.GetRequest getRequest = baseRequest.getExtension(RheakvRpc.GetRequest.body);
                regionKVService.handleGetRequest(baseRequest, getRequest, closure);
                break;
            case getAndPut:
                RheakvRpc.GetAndPutRequest getAndPutRequest = baseRequest.getExtension(RheakvRpc.GetAndPutRequest.body);
                regionKVService.handleGetAndPutRequest(baseRequest, getAndPutRequest, closure);
                break;
            case put:
                RheakvRpc.PutRequest putRequest = baseRequest.getExtension(RheakvRpc.PutRequest.body);
                regionKVService.handlePutRequest(baseRequest, putRequest, closure);
                break;
            case batchDelete:
                RheakvRpc.BatchDeleteRequest batchDeleteRequest = baseRequest
                    .getExtension(RheakvRpc.BatchDeleteRequest.body);
                regionKVService.handleBatchDeleteRequest(baseRequest, batchDeleteRequest, closure);
                break;
            case batchPut:
                RheakvRpc.BatchPutRequest batchPutRequest = baseRequest.getExtension(RheakvRpc.BatchPutRequest.body);
                regionKVService.handleBatchPutRequest(baseRequest, batchPutRequest, closure);
                break;
            case compareAndPut:
                RheakvRpc.CompareAndPutRequest compareAndPutRequest = baseRequest
                    .getExtension(RheakvRpc.CompareAndPutRequest.body);
                regionKVService.handleCompareAndPutRequest(baseRequest, compareAndPutRequest, closure);
                break;
            case containsKey:
                RheakvRpc.ContainsKeyRequest containsKeyRequest = baseRequest
                    .getExtension(RheakvRpc.ContainsKeyRequest.body);
                regionKVService.handleContainsKeyRequest(baseRequest, containsKeyRequest, closure);
                break;
            case deleteRange:
                RheakvRpc.DeleteRangeRequest deleteRangeRequest = baseRequest
                    .getExtension(RheakvRpc.DeleteRangeRequest.body);
                regionKVService.handleDeleteRangeRequest(baseRequest, deleteRangeRequest, closure);
                break;
            case delete:
                RheakvRpc.DeleteRequest deleteRequest = baseRequest.getExtension(RheakvRpc.DeleteRequest.body);
                regionKVService.handleDeleteRequest(baseRequest, deleteRequest, closure);
                break;
            case getSequence:
                RheakvRpc.GetSequenceRequest getSequenceRequest = baseRequest
                    .getExtension(RheakvRpc.GetSequenceRequest.body);
                regionKVService.handleGetSequence(baseRequest, getSequenceRequest, closure);
                break;
            case keyLock:
                RheakvRpc.KeyLockRequest keyLockRequest = baseRequest.getExtension(RheakvRpc.KeyLockRequest.body);
                regionKVService.handleKeyLockRequest(baseRequest, keyLockRequest, closure);
                break;
            case keyUnlock:
                RheakvRpc.KeyUnlockRequest keyUnlockRequest = baseRequest.getExtension(RheakvRpc.KeyUnlockRequest.body);
                regionKVService.handleKeyUnlockRequest(baseRequest, keyUnlockRequest, closure);
                break;
            case merge:
                RheakvRpc.MergeRequest mergeRequest = baseRequest.getExtension(RheakvRpc.MergeRequest.body);
                regionKVService.handleMergeRequest(baseRequest, mergeRequest, closure);
                break;
            case multiGet:
                RheakvRpc.MultiGetRequest multiGetRequest = baseRequest.getExtension(RheakvRpc.MultiGetRequest.body);
                regionKVService.handleMultiGetRequest(baseRequest, multiGetRequest, closure);
                break;
            case nodeExecute:
                RheakvRpc.NodeExecuteRequest nodeExecuteRequest = baseRequest
                    .getExtension(RheakvRpc.NodeExecuteRequest.body);
                regionKVService.handleNodeExecuteRequest(baseRequest, nodeExecuteRequest, closure);
                break;
            case putIfAbsent:
                RheakvRpc.PutIfAbsentRequest putIfAbsentRequest = baseRequest
                    .getExtension(RheakvRpc.PutIfAbsentRequest.body);
                regionKVService.handlePutIfAbsentRequest(baseRequest, putIfAbsentRequest, closure);
                break;
            case rangeSplit:
                RheakvRpc.RangeSplitRequest rangeSplitRequest = baseRequest
                    .getExtension(RheakvRpc.RangeSplitRequest.body);
                regionKVService.handleRangeSplitRequest(baseRequest, rangeSplitRequest, closure);
                break;
            case resetSequence:
                RheakvRpc.ResetSequenceRequest resetSequenceRequest = baseRequest
                    .getExtension(RheakvRpc.ResetSequenceRequest.body);
                regionKVService.handleResetSequence(baseRequest, resetSequenceRequest, closure);
                break;
            case scan:
                RheakvRpc.ScanRequest scanRequest = baseRequest.getExtension(RheakvRpc.ScanRequest.body);
                regionKVService.handleScanRequest(baseRequest, scanRequest, closure);
                break;
            default:
                throw new RheaRuntimeException("Unsupported request type: " + requestType.name());
        }
    }

    @Override
    public String interest() {
        return RheakvRpc.BaseRequest.class.getName();
    }

    @Override
    public Executor executor() {
        return this.storeEngine.getKvRpcExecutor();
    }
}
