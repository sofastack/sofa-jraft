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
package com.alipay.sofa.jraft.rhea.cmd.pd.transfer;

import com.alipay.remoting.exception.CodecException;
import com.alipay.sofa.jraft.rhea.cmd.pd.GetClusterInfoRequest;
import com.alipay.sofa.jraft.rhea.cmd.pd.proto.RheakvPDRpc;
import com.alipay.sofa.jraft.rpc.impl.GRpcSerializationTransfer;

/**
 * @Author: baozi
 * @Date: 2020/10/16 18:33
 */
public class GetClusterInfoRequestTransfer
                                          implements
                                          GRpcSerializationTransfer<GetClusterInfoRequest, RheakvPDRpc.GetClusterInfoRequest> {

    @Override
    public GetClusterInfoRequest protoBufTransJavaBean(final RheakvPDRpc.GetClusterInfoRequest getClusterInfoRequest)
                                                                                                                     throws CodecException {
        final GetClusterInfoRequest request = new GetClusterInfoRequest();
        BaseRequestProtobufTransfer.protoBufTransJavaBean(request, getClusterInfoRequest.getBaseRequest());
        return request;
    }

    @Override
    public RheakvPDRpc.GetClusterInfoRequest javaBeanTransProtobufBean(final GetClusterInfoRequest getClusterInfoRequest)
                                                                                                                         throws CodecException {
        return RheakvPDRpc.GetClusterInfoRequest.newBuilder()
            .setBaseRequest(BaseRequestProtobufTransfer.javaBeanTransProtobufBean(getClusterInfoRequest)).build();
    }
}
