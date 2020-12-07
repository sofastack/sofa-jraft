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

import java.util.ArrayList;
import java.util.List;

import com.alipay.remoting.exception.CodecException;
import com.alipay.remoting.serialization.SerializerManager;
import com.alipay.sofa.jraft.rhea.cmd.pd.RegionHeartbeatResponse;
import com.alipay.sofa.jraft.rhea.cmd.pd.proto.RheakvPDRpc;
import com.alipay.sofa.jraft.rhea.metadata.Instruction;
import com.alipay.sofa.jraft.rpc.impl.GrpcSerializationTransfer;
import com.google.protobuf.ByteString;

/**
 * @author: baozi
 */
public class RegionHeartbeatResponseTransfer
                                            implements
                                            GrpcSerializationTransfer<RegionHeartbeatResponse, RheakvPDRpc.RegionHeartbeatResponse> {

    @Override
    public RegionHeartbeatResponse protoBufTransJavaBean(final RheakvPDRpc.RegionHeartbeatResponse regionHeartbeatResponse)
                                                                                                                           throws CodecException {
        final RegionHeartbeatResponse response = new RegionHeartbeatResponse();
        BaseResponseProtobufTransfer.protoBufTransJavaBean(response, regionHeartbeatResponse.getBaseResponse());
        List<ByteString> valueList = regionHeartbeatResponse.getValueList();
        if (valueList != null && !valueList.isEmpty()) {
            List<Instruction> instructions = new ArrayList<>();
            for (ByteString bytes : valueList) {
                Instruction instruction = SerializerManager.getSerializer(SerializerManager.Hessian2).deserialize(
                    bytes.toByteArray(), Instruction.class.getName());
                instructions.add(instruction);
            }
            response.setValue(instructions);
        }
        return response;
    }

    @Override
    public RheakvPDRpc.RegionHeartbeatResponse javaBeanTransProtobufBean(final RegionHeartbeatResponse regionHeartbeatResponse)
                                                                                                                               throws CodecException {
        RheakvPDRpc.RegionHeartbeatResponse.Builder builder = RheakvPDRpc.RegionHeartbeatResponse.newBuilder()
            .setBaseResponse(BaseResponseProtobufTransfer.javaBeanTransProtobufBean(regionHeartbeatResponse));

        List<Instruction> values = regionHeartbeatResponse.getValue();
        if (values != null && !values.isEmpty()) {
            byte[] bytes = SerializerManager.getSerializer(SerializerManager.Hessian2).serialize(
                regionHeartbeatResponse.getValue());
            builder.addValue(ByteString.copyFrom(bytes));
        }
        return builder.build();
    }
}
