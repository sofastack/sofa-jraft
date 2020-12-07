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
package com.alipay.sofa.jraft.rhea.cmd.store.transfer;

import com.alipay.remoting.exception.CodecException;
import com.alipay.remoting.serialization.SerializerManager;
import com.alipay.sofa.jraft.rhea.cmd.store.NodeExecuteRequest;
import com.alipay.sofa.jraft.rhea.cmd.store.proto.RheakvRpc;
import com.alipay.sofa.jraft.rhea.storage.NodeExecutor;
import com.alipay.sofa.jraft.rpc.impl.GrpcSerializationTransfer;
import com.google.protobuf.ByteString;

/**
 * @author: baozi
 */
public class NodeExecuteRequestProtobufTransfer
                                               implements
                                               GrpcSerializationTransfer<NodeExecuteRequest, RheakvRpc.NodeExecuteRequest> {

    @Override
    public NodeExecuteRequest protoBufTransJavaBean(final RheakvRpc.NodeExecuteRequest nodeExecuteRequest) {
        final NodeExecuteRequest request = new NodeExecuteRequest();
        BaseRequestProtobufTransfer.protoBufTransJavaBean(request, nodeExecuteRequest.getBaseRequest());
        try {
            request.setNodeExecutor(SerializerManager.getSerializer(SerializerManager.Hessian2).deserialize(
                nodeExecuteRequest.getNodeExecutor().toByteArray(), NodeExecutor.class.getName()));
        } catch (CodecException e) {
            e.printStackTrace();
        }
        return request;
    }

    @Override
    public RheakvRpc.NodeExecuteRequest javaBeanTransProtobufBean(final NodeExecuteRequest nodeExecuteRequest) {
        RheakvRpc.NodeExecuteRequest.Builder builder = RheakvRpc.NodeExecuteRequest.newBuilder().setBaseRequest(
            BaseRequestProtobufTransfer.javaBeanTransProtobufBean(nodeExecuteRequest));
        try {
            builder.setNodeExecutor(ByteString.copyFrom(SerializerManager.getSerializer(SerializerManager.Hessian2)
                .serialize(nodeExecuteRequest.getNodeExecutor())));
        } catch (CodecException e) {
            e.printStackTrace();
        }
        return builder.build();
    }
}
