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

import com.alipay.sofa.jraft.rpc.RpcContext;
import com.alipay.sofa.jraft.test.atomic.KeyNotFoundException;
import com.alipay.sofa.jraft.test.atomic.command.GetCommand;
import com.alipay.sofa.jraft.test.atomic.command.ValueCommand;
import com.alipay.sofa.jraft.test.atomic.server.AtomicRangeGroup;
import com.alipay.sofa.jraft.test.atomic.server.AtomicServer;
import com.alipay.sofa.jraft.test.atomic.server.CommandType;

/**
 * Get command processor
 * @author dennis
 *
 */
public class GetCommandProcessor extends BaseAsyncUserProcessor<GetCommand> {

    @Override
    protected CommandType getCmdType() {
        return CommandType.GET;
    }

    public GetCommandProcessor(AtomicServer server) {
        super(server);
    }

    @Override
    public void handleRequest(RpcContext rpcCtx, GetCommand request) {
        if (request.isReadByStateMachine()) {
            super.handleRequest(rpcCtx, request);
        } else {
            try {
                final AtomicRangeGroup group = server.getGroupBykey(request.getKey());
                if (!request.isReadFromQuorum()) {
                    rpcCtx.sendResponse(new ValueCommand(group.getFsm().getValue(request.getKey())));
                } else {
                    group.readFromQuorum(request.getKey(), rpcCtx);
                }
            } catch (final KeyNotFoundException e) {
                rpcCtx.sendResponse(createKeyNotFoundResponse());
            }
        }
    }

    public static ValueCommand createKeyNotFoundResponse() {
        return new ValueCommand(false, "key not found");
    }

    @Override
    public String interest() {
        return GetCommand.class.getName();
    }

}
