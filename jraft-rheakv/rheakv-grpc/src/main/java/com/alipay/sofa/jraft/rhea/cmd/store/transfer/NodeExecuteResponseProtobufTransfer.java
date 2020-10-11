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
import com.alipay.sofa.jraft.rhea.cmd.store.NodeExecuteResponse;
import com.alipay.sofa.jraft.rhea.cmd.store.proto.RheakvRpc;
import com.alipay.sofa.jraft.rpc.impl.GRpcSerializationTransfer;

/**
 * @Author: baozi
 * @Date: 2020/9/26 20:10
 */
public class NodeExecuteResponseProtobufTransfer
                                                implements
                                                GRpcSerializationTransfer<NodeExecuteResponse, RheakvRpc.NodeExecuteResponse> {

    @Override
    public NodeExecuteResponse protoBufTransJavaBean(final RheakvRpc.NodeExecuteResponse nodeExecuteResponse)
                                                                                                             throws CodecException {
        final NodeExecuteResponse response = new NodeExecuteResponse();
        BaseResponseProtobufTransfer.protoBufTransJavaBean(response, nodeExecuteResponse.getBaseResponse());
        response.setValue(nodeExecuteResponse.getValue());
        return response;
    }

    @Override
    public RheakvRpc.NodeExecuteResponse javaBeanTransProtobufBean(final NodeExecuteResponse nodeExecuteResponse)
                                                                                                                 throws CodecException {
        final RheakvRpc.BaseResponse baseResponse = BaseResponseProtobufTransfer
            .javaBeanTransProtobufBean(nodeExecuteResponse);
        final RheakvRpc.NodeExecuteResponse.Builder response = RheakvRpc.NodeExecuteResponse.newBuilder()
            .setBaseResponse(baseResponse);
        response.setValue(nodeExecuteResponse.getValue());
        return response.build();
    }

}
