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

import com.alipay.sofa.jraft.rhea.cmd.store.BaseRequest;
import com.alipay.sofa.jraft.rhea.cmd.store.proto.RheakvRpc;
import com.alipay.sofa.jraft.rhea.metadata.RegionEpoch;

/**
 * @Author: baozi
 * @Date: 2020/9/27 20:09
 */
public class BaseRequestProtobufTransfer {

    /**
     *
     * @param baseRequest
     * @param protoBaseRequest
     */
    public static void protoBufTransJavaBean(final BaseRequest baseRequest, final RheakvRpc.BaseRequest protoBaseRequest) {
        baseRequest.setRegionEpoch(new RegionEpoch(protoBaseRequest.getRegionEpoch().getConfVer(), protoBaseRequest
            .getRegionEpoch().getVersion()));
        baseRequest.setRegionId(protoBaseRequest.getRegionId());
    }

    /**
     *
     * @param baseRequest
     * @return
     */
    public static RheakvRpc.BaseRequest javaBeanTransProtobufBean(final BaseRequest baseRequest) {
        return RheakvRpc.BaseRequest
            .newBuilder()
            .setRegionId(baseRequest.getRegionId())
            .setRegionEpoch(
                RheakvRpc.RegionEpoch.newBuilder().setConfVer(baseRequest.getRegionEpoch().getConfVer())
                    .setVersion(baseRequest.getRegionEpoch().getVersion()).build()).build();
    }
}
