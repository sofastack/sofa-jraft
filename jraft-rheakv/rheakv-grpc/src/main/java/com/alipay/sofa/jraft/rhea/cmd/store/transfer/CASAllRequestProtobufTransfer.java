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

import java.util.ArrayList;
import java.util.List;

import com.alipay.sofa.jraft.rhea.cmd.store.CASAllRequest;
import com.alipay.sofa.jraft.rhea.cmd.store.proto.RheakvRpc;
import com.alipay.sofa.jraft.rhea.storage.CASEntry;
import com.alipay.sofa.jraft.rpc.impl.GrpcSerializationTransfer;
import com.google.protobuf.ByteString;

/**
 * @author: baozi
 */
public class CASAllRequestProtobufTransfer
                                              implements
                                              GrpcSerializationTransfer<CASAllRequest, RheakvRpc.CASAllRequest> {

    @Override
    public CASAllRequest protoBufTransJavaBean(final RheakvRpc.CASAllRequest casAllRequest) {
        final CASAllRequest request = new CASAllRequest();
        BaseRequestProtobufTransfer.protoBufTransJavaBean(request, casAllRequest.getBaseRequest());
        final List<CASEntry> casEntries = new ArrayList<>();
        for (final RheakvRpc.CASEntry casEntry : casAllRequest.getCasEntriesList()) {
            final CASEntry entry = new CASEntry();
            entry.setKey(casEntry.getKey().toByteArray());
            entry.setExpect(casEntry.getExpect().toByteArray());
            entry.setUpdate(casEntry.getUpdate().toByteArray());
            casEntries.add(entry);
        }
        request.setCasEntries(casEntries);
        return request;
    }

    @Override
    public RheakvRpc.CASAllRequest javaBeanTransProtobufBean(final CASAllRequest casAllRequest) {
        final RheakvRpc.CASAllRequest.Builder builder = RheakvRpc.CASAllRequest.newBuilder()
            .setBaseRequest(BaseRequestProtobufTransfer.javaBeanTransProtobufBean(casAllRequest));
        if (casAllRequest.getCasEntries() != null) {
            for (final CASEntry casEntry : casAllRequest.getCasEntries()) {
                builder.addCasEntries(RheakvRpc.CASEntry.newBuilder()
                    .setKey(ByteString.copyFrom(casEntry.getKey() != null ? casEntry.getKey() : new byte[0]))
                    .setExpect(ByteString.copyFrom(casEntry.getExpect() != null ? casEntry.getExpect() : new byte[0]))
                    .setUpdate(ByteString.copyFrom(casEntry.getUpdate() != null ? casEntry.getUpdate() : new byte[0]))
                    .build());
            }
        }
        return builder.build();
    }
}
