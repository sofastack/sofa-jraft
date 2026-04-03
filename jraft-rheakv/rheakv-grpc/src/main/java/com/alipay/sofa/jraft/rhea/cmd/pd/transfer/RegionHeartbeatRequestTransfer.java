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
import com.alipay.sofa.jraft.rhea.cmd.pd.RegionHeartbeatRequest;
import com.alipay.sofa.jraft.rhea.cmd.pd.proto.RheakvPDRpc;
import com.alipay.sofa.jraft.rhea.metadata.Region;
import com.alipay.sofa.jraft.rhea.metadata.RegionStats;
import com.alipay.sofa.jraft.rhea.util.Pair;
import com.alipay.sofa.jraft.rpc.impl.GrpcSerializationTransfer;
import com.google.protobuf.ByteString;

/**
 * @author: baozi
 */
public class RegionHeartbeatRequestTransfer
                                           implements
                                           GrpcSerializationTransfer<RegionHeartbeatRequest, RheakvPDRpc.RegionHeartbeatRequest> {

    @Override
    public RegionHeartbeatRequest protoBufTransJavaBean(RheakvPDRpc.RegionHeartbeatRequest regionHeartbeatRequest)
                                                                                                                  throws CodecException {
        final RegionHeartbeatRequest request = new RegionHeartbeatRequest();
        BaseRequestProtobufTransfer.protoBufTransJavaBean(request, regionHeartbeatRequest.getBaseRequest());
        request.setStoreId(regionHeartbeatRequest.getStoreId());
        request.setLeastKeysOnSplit(regionHeartbeatRequest.getLeastKeysOnSplit());
        List<ByteString> regionStats = regionHeartbeatRequest.getRegionStatsListList();
        if (regionStats != null && !regionStats.isEmpty()) {
            List<Pair<Region, RegionStats>> regionStatsList = new ArrayList<>();
            for (ByteString byteString : regionStats) {
                Pair pair = SerializerManager.getSerializer(SerializerManager.Hessian2).deserialize(
                    byteString.toByteArray(), Pair.class.getName());
                regionStatsList.add(pair);
            }
            request.setRegionStatsList(regionStatsList);
        }
        return request;
    }

    @Override
    public RheakvPDRpc.RegionHeartbeatRequest javaBeanTransProtobufBean(RegionHeartbeatRequest regionHeartbeatRequest)
                                                                                                                      throws CodecException {
        RheakvPDRpc.RegionHeartbeatRequest.Builder builder = RheakvPDRpc.RegionHeartbeatRequest.newBuilder()
            .setBaseRequest(BaseRequestProtobufTransfer.javaBeanTransProtobufBean(regionHeartbeatRequest))
            .setStoreId(regionHeartbeatRequest.getStoreId()).setLeastKeysOnSplit(regionHeartbeatRequest.getStoreId());

        List<Pair<Region, RegionStats>> regionStatsList = regionHeartbeatRequest.getRegionStatsList();
        if (regionStatsList != null && !regionStatsList.isEmpty()) {
            for (Pair<Region, RegionStats> regionRegionStatsPair : regionStatsList) {
                byte[] bytes = SerializerManager.getSerializer(SerializerManager.Hessian2).serialize(
                    regionRegionStatsPair);
                builder.addRegionStatsList(ByteString.copyFrom(bytes));
            }
        }
        return builder.build();
    }

}
