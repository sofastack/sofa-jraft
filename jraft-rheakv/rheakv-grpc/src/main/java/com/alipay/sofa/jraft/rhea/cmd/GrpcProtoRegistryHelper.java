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
package com.alipay.sofa.jraft.rhea.cmd;

import com.alipay.sofa.jraft.rhea.cmd.pd.CreateRegionIdRequest;
import com.alipay.sofa.jraft.rhea.cmd.pd.CreateRegionIdResponse;
import com.alipay.sofa.jraft.rhea.cmd.pd.GetClusterInfoRequest;
import com.alipay.sofa.jraft.rhea.cmd.pd.GetClusterInfoResponse;
import com.alipay.sofa.jraft.rhea.cmd.pd.GetStoreIdRequest;
import com.alipay.sofa.jraft.rhea.cmd.pd.GetStoreIdResponse;
import com.alipay.sofa.jraft.rhea.cmd.pd.GetStoreInfoRequest;
import com.alipay.sofa.jraft.rhea.cmd.pd.GetStoreInfoResponse;
import com.alipay.sofa.jraft.rhea.cmd.pd.RegionHeartbeatRequest;
import com.alipay.sofa.jraft.rhea.cmd.pd.RegionHeartbeatResponse;
import com.alipay.sofa.jraft.rhea.cmd.pd.SetStoreInfoRequest;
import com.alipay.sofa.jraft.rhea.cmd.pd.SetStoreInfoResponse;
import com.alipay.sofa.jraft.rhea.cmd.pd.StoreHeartbeatRequest;
import com.alipay.sofa.jraft.rhea.cmd.pd.StoreHeartbeatResponse;
import com.alipay.sofa.jraft.rhea.cmd.pd.proto.RheakvPDRpc;
import com.alipay.sofa.jraft.rhea.cmd.pd.transfer.CreateRegionIdRequestTransfer;
import com.alipay.sofa.jraft.rhea.cmd.pd.transfer.CreateRegionIdResponseTransfer;
import com.alipay.sofa.jraft.rhea.cmd.pd.transfer.GetClusterInfoRequestTransfer;
import com.alipay.sofa.jraft.rhea.cmd.pd.transfer.GetClusterInfoResponseTransfer;
import com.alipay.sofa.jraft.rhea.cmd.pd.transfer.GetStoreIdRequestTransfer;
import com.alipay.sofa.jraft.rhea.cmd.pd.transfer.GetStoreIdResponseTransfer;
import com.alipay.sofa.jraft.rhea.cmd.pd.transfer.GetStoreInfoRequestTransfer;
import com.alipay.sofa.jraft.rhea.cmd.pd.transfer.GetStoreInfoResponseTransfer;
import com.alipay.sofa.jraft.rhea.cmd.pd.transfer.RegionHeartbeatRequestTransfer;
import com.alipay.sofa.jraft.rhea.cmd.pd.transfer.RegionHeartbeatResponseTransfer;
import com.alipay.sofa.jraft.rhea.cmd.pd.transfer.SetStoreInfoRequestTransfer;
import com.alipay.sofa.jraft.rhea.cmd.pd.transfer.SetStoreInfoResponseTransfer;
import com.alipay.sofa.jraft.rhea.cmd.pd.transfer.StoreHeartbeatRequestTransfer;
import com.alipay.sofa.jraft.rhea.cmd.pd.transfer.StoreHeartbeatResponseTransfer;
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
import com.alipay.sofa.jraft.rhea.cmd.store.proto.RheakvRpc;
import com.alipay.sofa.jraft.rhea.cmd.store.transfer.BatchDeleteRequestProtobufTransfer;
import com.alipay.sofa.jraft.rhea.cmd.store.transfer.BatchDeleteResponseProtobufTransfer;
import com.alipay.sofa.jraft.rhea.cmd.store.transfer.BatchPutRequestProtobufTransfer;
import com.alipay.sofa.jraft.rhea.cmd.store.transfer.BatchPutResponseProtobufTransfer;
import com.alipay.sofa.jraft.rhea.cmd.store.transfer.CompareAndPutRequestProtobufTransfer;
import com.alipay.sofa.jraft.rhea.cmd.store.transfer.CompareAndPutResponseProtobufTransfer;
import com.alipay.sofa.jraft.rhea.cmd.store.transfer.ContainsKeyRequestProtobufTransfer;
import com.alipay.sofa.jraft.rhea.cmd.store.transfer.ContainsKeyResponseProtobufTransfer;
import com.alipay.sofa.jraft.rhea.cmd.store.transfer.DeleteRangeRequestProtobufTransfer;
import com.alipay.sofa.jraft.rhea.cmd.store.transfer.DeleteRangeResponseProtobufTransfer;
import com.alipay.sofa.jraft.rhea.cmd.store.transfer.DeleteRequestProtobufTransfer;
import com.alipay.sofa.jraft.rhea.cmd.store.transfer.DeleteResponseProtobufTransfer;
import com.alipay.sofa.jraft.rhea.cmd.store.transfer.GetAndPutRequestProtobufTransfer;
import com.alipay.sofa.jraft.rhea.cmd.store.transfer.GetAndPutResponseProtobufTransfer;
import com.alipay.sofa.jraft.rhea.cmd.store.transfer.GetRequestProtobufTransfer;
import com.alipay.sofa.jraft.rhea.cmd.store.transfer.GetResponseProtobufTransfer;
import com.alipay.sofa.jraft.rhea.cmd.store.transfer.GetSequenceRequestProtobufTransfer;
import com.alipay.sofa.jraft.rhea.cmd.store.transfer.GetSequenceResponseProtobufTransfer;
import com.alipay.sofa.jraft.rhea.cmd.store.transfer.KeyLockRequestProtobufTransfer;
import com.alipay.sofa.jraft.rhea.cmd.store.transfer.KeyLockResponseProtobufTransfer;
import com.alipay.sofa.jraft.rhea.cmd.store.transfer.KeyUnLockRequestProtobufTransfer;
import com.alipay.sofa.jraft.rhea.cmd.store.transfer.KeyUnLockResponseProtobufTransfer;
import com.alipay.sofa.jraft.rhea.cmd.store.transfer.MergeRequestProtobufTransfer;
import com.alipay.sofa.jraft.rhea.cmd.store.transfer.MergeResponseProtobufTransfer;
import com.alipay.sofa.jraft.rhea.cmd.store.transfer.MultiGetRequestProtobufTransfer;
import com.alipay.sofa.jraft.rhea.cmd.store.transfer.MultiGetResponseProtobufTransfer;
import com.alipay.sofa.jraft.rhea.cmd.store.transfer.NodeExecuteRequestProtobufTransfer;
import com.alipay.sofa.jraft.rhea.cmd.store.transfer.NodeExecuteResponseProtobufTransfer;
import com.alipay.sofa.jraft.rhea.cmd.store.transfer.PutIfAbsentRequestProtobufTransfer;
import com.alipay.sofa.jraft.rhea.cmd.store.transfer.PutIfAbsentResponseProtobufTransfer;
import com.alipay.sofa.jraft.rhea.cmd.store.transfer.PutRequestProtobufTransfer;
import com.alipay.sofa.jraft.rhea.cmd.store.transfer.PutResponseProtobufTransfer;
import com.alipay.sofa.jraft.rhea.cmd.store.transfer.RangeSplitRequestProtobufTransfer;
import com.alipay.sofa.jraft.rhea.cmd.store.transfer.RangeSplitResponseProtobufTransfer;
import com.alipay.sofa.jraft.rhea.cmd.store.transfer.ResetSequenceRequestProtobufTransfer;
import com.alipay.sofa.jraft.rhea.cmd.store.transfer.ResetSequenceResponseProtobufTransfer;
import com.alipay.sofa.jraft.rhea.cmd.store.transfer.ScanRequestProtobufTransfer;
import com.alipay.sofa.jraft.rhea.cmd.store.transfer.ScanResponseProtobufTransfer;
import com.alipay.sofa.jraft.rpc.impl.GrpcProtobufTransferHelper;
import com.alipay.sofa.jraft.rpc.impl.MarshallerHelper;
import com.alipay.sofa.jraft.util.RpcFactoryHelper;
import com.google.protobuf.Message;

/**
 * @Author: baozi
 * @Date: 2020/9/26 21:19
 */
public class GrpcProtoRegistryHelper {

    public static void registryAll() {
        registryMarshaller();
        registryProtobufTransfer();
        registryPdMarshaller();
        registryPdProtobufTransfer();
    }

    /**
     * registry rheakv pd grpc request protobuf marshaller
     */
    private static void registryPdMarshaller() {
        registryMarshaller(CreateRegionIdRequest.class, RheakvPDRpc.CreateRegionIdRequest.getDefaultInstance(),
            RheakvPDRpc.CreateRegionIdResponse.getDefaultInstance());
        registryMarshaller(GetClusterInfoRequest.class, RheakvPDRpc.GetClusterInfoRequest.getDefaultInstance(),
            RheakvPDRpc.GetClusterInfoResponse.getDefaultInstance());
        registryMarshaller(GetStoreIdRequest.class, RheakvPDRpc.GetStoreIdRequest.getDefaultInstance(),
            RheakvPDRpc.GetStoreIdResponse.getDefaultInstance());
        registryMarshaller(GetStoreInfoRequest.class, RheakvPDRpc.GetStoreInfoRequest.getDefaultInstance(),
            RheakvPDRpc.GetStoreInfoResponse.getDefaultInstance());
        registryMarshaller(RegionHeartbeatRequest.class, RheakvPDRpc.RegionHeartbeatRequest.getDefaultInstance(),
            RheakvPDRpc.RegionHeartbeatResponse.getDefaultInstance());
        registryMarshaller(SetStoreInfoRequest.class, RheakvPDRpc.SetStoreInfoRequest.getDefaultInstance(),
            RheakvPDRpc.SetStoreInfoResponse.getDefaultInstance());
        registryMarshaller(StoreHeartbeatRequest.class, RheakvPDRpc.StoreHeartbeatRequest.getDefaultInstance(),
            RheakvPDRpc.StoreHeartbeatResponse.getDefaultInstance());
    }

    /**
     * registry pd protobuf transfer
     */
    private static void registryPdProtobufTransfer() {

        GrpcProtobufTransferHelper.registryTransfer(CreateRegionIdRequest.class,
            RheakvPDRpc.CreateRegionIdRequest.class, new CreateRegionIdRequestTransfer());
        GrpcProtobufTransferHelper.registryTransfer(CreateRegionIdResponse.class,
            RheakvPDRpc.CreateRegionIdResponse.class, new CreateRegionIdResponseTransfer());

        GrpcProtobufTransferHelper.registryTransfer(GetClusterInfoRequest.class,
            RheakvPDRpc.GetClusterInfoRequest.class, new GetClusterInfoRequestTransfer());
        GrpcProtobufTransferHelper.registryTransfer(GetClusterInfoResponse.class,
            RheakvPDRpc.GetClusterInfoResponse.class, new GetClusterInfoResponseTransfer());

        GrpcProtobufTransferHelper.registryTransfer(GetStoreIdRequest.class, RheakvPDRpc.GetStoreIdRequest.class,
            new GetStoreIdRequestTransfer());
        GrpcProtobufTransferHelper.registryTransfer(GetStoreIdResponse.class, RheakvPDRpc.GetStoreIdResponse.class,
            new GetStoreIdResponseTransfer());

        GrpcProtobufTransferHelper.registryTransfer(GetStoreInfoRequest.class, RheakvPDRpc.GetStoreInfoRequest.class,
            new GetStoreInfoRequestTransfer());
        GrpcProtobufTransferHelper.registryTransfer(GetStoreInfoResponse.class, RheakvPDRpc.GetStoreInfoResponse.class,
            new GetStoreInfoResponseTransfer());

        GrpcProtobufTransferHelper.registryTransfer(RegionHeartbeatRequest.class,
            RheakvPDRpc.RegionHeartbeatRequest.class, new RegionHeartbeatRequestTransfer());
        GrpcProtobufTransferHelper.registryTransfer(RegionHeartbeatResponse.class,
            RheakvPDRpc.RegionHeartbeatResponse.class, new RegionHeartbeatResponseTransfer());

        GrpcProtobufTransferHelper.registryTransfer(SetStoreInfoRequest.class, RheakvPDRpc.SetStoreInfoRequest.class,
            new SetStoreInfoRequestTransfer());
        GrpcProtobufTransferHelper.registryTransfer(SetStoreInfoResponse.class, RheakvPDRpc.SetStoreInfoResponse.class,
            new SetStoreInfoResponseTransfer());

        GrpcProtobufTransferHelper.registryTransfer(StoreHeartbeatRequest.class,
            RheakvPDRpc.StoreHeartbeatRequest.class, new StoreHeartbeatRequestTransfer());
        GrpcProtobufTransferHelper.registryTransfer(StoreHeartbeatResponse.class,
            RheakvPDRpc.StoreHeartbeatResponse.class, new StoreHeartbeatResponseTransfer());
    }

    /**
     * registry rheakv grpc request protobuf marshaller
     */
    private static void registryMarshaller() {
        registryMarshaller(GetRequest.class, RheakvRpc.GetRequest.getDefaultInstance(),
            RheakvRpc.GetResponse.getDefaultInstance());
        registryMarshaller(MultiGetRequest.class, RheakvRpc.MultiGetRequest.getDefaultInstance(),
            RheakvRpc.MultiGetResponse.getDefaultInstance());
        registryMarshaller(ContainsKeyRequest.class, RheakvRpc.ContainsKeyRequest.getDefaultInstance(),
            RheakvRpc.ContainsKeyResponse.getDefaultInstance());
        registryMarshaller(GetSequenceRequest.class, RheakvRpc.GetSequenceRequest.getDefaultInstance(),
            RheakvRpc.GetSequenceResponse.getDefaultInstance());
        registryMarshaller(ResetSequenceRequest.class, RheakvRpc.ResetSequenceRequest.getDefaultInstance(),
            RheakvRpc.ResetSequenceResponse.getDefaultInstance());
        registryMarshaller(ScanRequest.class, RheakvRpc.ScanRequest.getDefaultInstance(),
            RheakvRpc.ScanResponse.getDefaultInstance());
        registryMarshaller(PutRequest.class, RheakvRpc.PutRequest.getDefaultInstance(),
            RheakvRpc.PutResponse.getDefaultInstance());
        registryMarshaller(GetAndPutRequest.class, RheakvRpc.GetAndPutRequest.getDefaultInstance(),
            RheakvRpc.GetAndPutResponse.getDefaultInstance());
        registryMarshaller(CompareAndPutRequest.class, RheakvRpc.CompareAndPutRequest.getDefaultInstance(),
            RheakvRpc.CompareAndPutResponse.getDefaultInstance());
        registryMarshaller(MergeRequest.class, RheakvRpc.MergeRequest.getDefaultInstance(),
            RheakvRpc.MergeResponse.getDefaultInstance());
        registryMarshaller(PutIfAbsentRequest.class, RheakvRpc.PutIfAbsentRequest.getDefaultInstance(),
            RheakvRpc.PutIfAbsentResponse.getDefaultInstance());
        registryMarshaller(KeyLockRequest.class, RheakvRpc.KeyLockRequest.getDefaultInstance(),
            RheakvRpc.KeyLockResponse.getDefaultInstance());
        registryMarshaller(KeyUnlockRequest.class, RheakvRpc.KeyUnlockRequest.getDefaultInstance(),
            RheakvRpc.KeyUnlockResponse.getDefaultInstance());
        registryMarshaller(BatchPutRequest.class, RheakvRpc.BatchPutRequest.getDefaultInstance(),
            RheakvRpc.BatchPutResponse.getDefaultInstance());
        registryMarshaller(DeleteRequest.class, RheakvRpc.DeleteRequest.getDefaultInstance(),
            RheakvRpc.DeleteResponse.getDefaultInstance());
        registryMarshaller(DeleteRangeRequest.class, RheakvRpc.DeleteRangeRequest.getDefaultInstance(),
            RheakvRpc.DeleteRangeResponse.getDefaultInstance());
        registryMarshaller(BatchDeleteRequest.class, RheakvRpc.BatchDeleteRequest.getDefaultInstance(),
            RheakvRpc.BatchDeleteResponse.getDefaultInstance());
        registryMarshaller(NodeExecuteRequest.class, RheakvRpc.NodeExecuteRequest.getDefaultInstance(),
            RheakvRpc.NodeExecuteResponse.getDefaultInstance());
        registryMarshaller(RangeSplitRequest.class, RheakvRpc.RangeSplitRequest.getDefaultInstance(),
            RheakvRpc.RangeSplitResponse.getDefaultInstance());
    }

    /**
     * registry protobuf transfer
     */
    private static void registryProtobufTransfer() {

        GrpcProtobufTransferHelper.registryTransfer(GetRequest.class, RheakvRpc.GetRequest.class,
            new GetRequestProtobufTransfer());
        GrpcProtobufTransferHelper.registryTransfer(GetResponse.class, RheakvRpc.GetResponse.class,
            new GetResponseProtobufTransfer());

        GrpcProtobufTransferHelper.registryTransfer(MultiGetRequest.class, RheakvRpc.MultiGetRequest.class,
            new MultiGetRequestProtobufTransfer());
        GrpcProtobufTransferHelper.registryTransfer(MultiGetResponse.class, RheakvRpc.MultiGetResponse.class,
            new MultiGetResponseProtobufTransfer());

        GrpcProtobufTransferHelper.registryTransfer(ContainsKeyRequest.class, RheakvRpc.ContainsKeyRequest.class,
            new ContainsKeyRequestProtobufTransfer());
        GrpcProtobufTransferHelper.registryTransfer(ContainsKeyResponse.class, RheakvRpc.ContainsKeyResponse.class,
            new ContainsKeyResponseProtobufTransfer());

        GrpcProtobufTransferHelper.registryTransfer(GetSequenceRequest.class, RheakvRpc.GetSequenceRequest.class,
            new GetSequenceRequestProtobufTransfer());
        GrpcProtobufTransferHelper.registryTransfer(GetSequenceResponse.class, RheakvRpc.GetSequenceResponse.class,
            new GetSequenceResponseProtobufTransfer());

        GrpcProtobufTransferHelper.registryTransfer(ResetSequenceRequest.class, RheakvRpc.ResetSequenceRequest.class,
            new ResetSequenceRequestProtobufTransfer());
        GrpcProtobufTransferHelper.registryTransfer(ResetSequenceResponse.class, RheakvRpc.ResetSequenceResponse.class,
            new ResetSequenceResponseProtobufTransfer());

        GrpcProtobufTransferHelper.registryTransfer(ScanRequest.class, RheakvRpc.ScanRequest.class,
            new ScanRequestProtobufTransfer());
        GrpcProtobufTransferHelper.registryTransfer(ScanResponse.class, RheakvRpc.ScanResponse.class,
            new ScanResponseProtobufTransfer());

        GrpcProtobufTransferHelper.registryTransfer(PutRequest.class, RheakvRpc.PutRequest.class,
            new PutRequestProtobufTransfer());
        GrpcProtobufTransferHelper.registryTransfer(PutResponse.class, RheakvRpc.PutResponse.class,
            new PutResponseProtobufTransfer());

        GrpcProtobufTransferHelper.registryTransfer(GetAndPutRequest.class, RheakvRpc.GetAndPutRequest.class,
            new GetAndPutRequestProtobufTransfer());
        GrpcProtobufTransferHelper.registryTransfer(GetAndPutResponse.class, RheakvRpc.GetAndPutResponse.class,
            new GetAndPutResponseProtobufTransfer());

        GrpcProtobufTransferHelper.registryTransfer(CompareAndPutRequest.class, RheakvRpc.CompareAndPutRequest.class,
            new CompareAndPutRequestProtobufTransfer());
        GrpcProtobufTransferHelper.registryTransfer(CompareAndPutResponse.class, RheakvRpc.CompareAndPutResponse.class,
            new CompareAndPutResponseProtobufTransfer());

        GrpcProtobufTransferHelper.registryTransfer(MergeRequest.class, RheakvRpc.MergeRequest.class,
            new MergeRequestProtobufTransfer());
        GrpcProtobufTransferHelper.registryTransfer(MergeResponse.class, RheakvRpc.MergeResponse.class,
            new MergeResponseProtobufTransfer());

        GrpcProtobufTransferHelper.registryTransfer(PutIfAbsentRequest.class, RheakvRpc.PutIfAbsentRequest.class,
            new PutIfAbsentRequestProtobufTransfer());
        GrpcProtobufTransferHelper.registryTransfer(PutIfAbsentResponse.class, RheakvRpc.PutIfAbsentResponse.class,
            new PutIfAbsentResponseProtobufTransfer());

        GrpcProtobufTransferHelper.registryTransfer(KeyLockRequest.class, RheakvRpc.KeyLockRequest.class,
            new KeyLockRequestProtobufTransfer());
        GrpcProtobufTransferHelper.registryTransfer(KeyLockResponse.class, RheakvRpc.KeyLockResponse.class,
            new KeyLockResponseProtobufTransfer());

        GrpcProtobufTransferHelper.registryTransfer(KeyUnlockRequest.class, RheakvRpc.KeyUnlockRequest.class,
            new KeyUnLockRequestProtobufTransfer());
        GrpcProtobufTransferHelper.registryTransfer(KeyUnlockResponse.class, RheakvRpc.KeyUnlockResponse.class,
            new KeyUnLockResponseProtobufTransfer());

        GrpcProtobufTransferHelper.registryTransfer(BatchPutRequest.class, RheakvRpc.BatchPutRequest.class,
            new BatchPutRequestProtobufTransfer());
        GrpcProtobufTransferHelper.registryTransfer(BatchPutResponse.class, RheakvRpc.BatchPutResponse.class,
            new BatchPutResponseProtobufTransfer());

        GrpcProtobufTransferHelper.registryTransfer(DeleteRequest.class, RheakvRpc.DeleteRequest.class,
            new DeleteRequestProtobufTransfer());
        GrpcProtobufTransferHelper.registryTransfer(DeleteResponse.class, RheakvRpc.DeleteResponse.class,
            new DeleteResponseProtobufTransfer());

        GrpcProtobufTransferHelper.registryTransfer(DeleteRangeRequest.class, RheakvRpc.DeleteRangeRequest.class,
            new DeleteRangeRequestProtobufTransfer());
        GrpcProtobufTransferHelper.registryTransfer(DeleteRangeResponse.class, RheakvRpc.DeleteRangeResponse.class,
            new DeleteRangeResponseProtobufTransfer());

        GrpcProtobufTransferHelper.registryTransfer(BatchDeleteRequest.class, RheakvRpc.BatchDeleteRequest.class,
            new BatchDeleteRequestProtobufTransfer());
        GrpcProtobufTransferHelper.registryTransfer(BatchDeleteResponse.class, RheakvRpc.BatchDeleteResponse.class,
            new BatchDeleteResponseProtobufTransfer());

        GrpcProtobufTransferHelper.registryTransfer(NodeExecuteRequest.class, RheakvRpc.NodeExecuteRequest.class,
            new NodeExecuteRequestProtobufTransfer());
        GrpcProtobufTransferHelper.registryTransfer(NodeExecuteResponse.class, RheakvRpc.NodeExecuteResponse.class,
            new NodeExecuteResponseProtobufTransfer());

        GrpcProtobufTransferHelper.registryTransfer(RangeSplitRequest.class, RheakvRpc.RangeSplitRequest.class,
            new RangeSplitRequestProtobufTransfer());
        GrpcProtobufTransferHelper.registryTransfer(RangeSplitResponse.class, RheakvRpc.RangeSplitResponse.class,
            new RangeSplitResponseProtobufTransfer());
    }

    /**
     * @param requestCls
     * @param requestInstance
     * @param responseInstance
     */
    private static void registryMarshaller(final Class<?> requestCls, final Message requestInstance,
                                           final Message responseInstance) {
        RpcFactoryHelper.rpcFactory().registerProtobufSerializer(requestCls.getName(), requestInstance);
        MarshallerHelper.registerRespInstance(requestCls.getName(), responseInstance);
    }

}
