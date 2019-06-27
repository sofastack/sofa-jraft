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
package com.alipay.sofa.jraft.rhea.pipeline.handler;

import com.alipay.sofa.jraft.rhea.MetadataStore;
import com.alipay.sofa.jraft.rhea.cmd.pd.StoreHeartbeatRequest;
import com.alipay.sofa.jraft.rhea.pipeline.event.StorePingEvent;
import com.alipay.sofa.jraft.rhea.util.pipeline.Handler;
import com.alipay.sofa.jraft.rhea.util.pipeline.HandlerContext;
import com.alipay.sofa.jraft.rhea.util.pipeline.InboundHandlerAdapter;
import com.alipay.sofa.jraft.util.SPI;

/**
 *
 * @author jiachun.fjc
 */
@SPI(name = "storeStatsPersistence", priority = 80)
@Handler.Sharable
public class StoreStatsPersistenceHandler extends InboundHandlerAdapter<StorePingEvent> {

    @Override
    public void readMessage(final HandlerContext ctx, final StorePingEvent event) throws Exception {
        final MetadataStore metadataStore = event.getMetadataStore();
        final StoreHeartbeatRequest request = event.getMessage();
        metadataStore.updateStoreStats(request.getClusterId(), request.getStats()).get(); // sync
    }
}
