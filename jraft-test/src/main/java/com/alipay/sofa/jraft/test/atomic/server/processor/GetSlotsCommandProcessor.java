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
package com.alipay.sofa.jraft.test.atomic.server.processor;

import com.alipay.remoting.BizContext;
import com.alipay.remoting.rpc.protocol.SyncUserProcessor;
import com.alipay.sofa.jraft.test.atomic.command.GetSlotsCommand;
import com.alipay.sofa.jraft.test.atomic.command.SlotsResponseCommand;
import com.alipay.sofa.jraft.test.atomic.server.AtomicServer;

public class GetSlotsCommandProcessor extends SyncUserProcessor<GetSlotsCommand> {
    private AtomicServer server;

    public GetSlotsCommandProcessor(AtomicServer server) {
        super();
        this.server = server;
    }

    @Override
    public Object handleRequest(BizContext bizCtx, GetSlotsCommand request) throws Exception {
        SlotsResponseCommand response = new SlotsResponseCommand();
        response.setMap(this.server.getGroups());
        return response;
    }

    @Override
    public String interest() {
        return GetSlotsCommand.class.getName();
    }

}
