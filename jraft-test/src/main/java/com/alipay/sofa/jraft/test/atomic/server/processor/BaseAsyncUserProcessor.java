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

import java.nio.ByteBuffer;

import com.alipay.sofa.jraft.entity.Task;
import com.alipay.sofa.jraft.rpc.RpcContext;
import com.alipay.sofa.jraft.rpc.RpcProcessor;
import com.alipay.sofa.jraft.test.atomic.command.BaseRequestCommand;
import com.alipay.sofa.jraft.test.atomic.command.BooleanCommand;
import com.alipay.sofa.jraft.test.atomic.command.CommandCodec;
import com.alipay.sofa.jraft.test.atomic.server.AtomicRangeGroup;
import com.alipay.sofa.jraft.test.atomic.server.AtomicServer;
import com.alipay.sofa.jraft.test.atomic.server.CommandType;
import com.alipay.sofa.jraft.test.atomic.server.LeaderTaskClosure;
import com.alipay.sofa.jraft.util.BufferUtils;

public abstract class BaseAsyncUserProcessor<T extends BaseRequestCommand> implements RpcProcessor<T> {
    protected AtomicServer server;

    public BaseAsyncUserProcessor(AtomicServer server) {
        super();
        this.server = server;
    }

    @Override
    public void handleRequest(final RpcContext rpcCtx, final T request) {
        final AtomicRangeGroup group = server.getGroupBykey(request.getKey());
        if (!group.getFsm().isLeader()) {
            rpcCtx.sendResponse(group.redirect());
            return;
        }

        final CommandType cmdType = getCmdType();
        final Task task = createTask(rpcCtx, request, cmdType);
        group.getNode().apply(task);
    }

    protected abstract CommandType getCmdType();

    private Task createTask(RpcContext asyncCtx, T request, CommandType cmdType) {
        final LeaderTaskClosure closure = new LeaderTaskClosure();
        closure.setCmd(request);
        closure.setCmdType(cmdType);
        closure.setDone(status -> {
            if (status.isOk()) {
                asyncCtx.sendResponse(closure.getResponse());
            } else {
                asyncCtx.sendResponse(new BooleanCommand(false, status.getErrorMsg()));
            }
        });
        final byte[] cmdBytes = CommandCodec.encodeCommand(request);
        final ByteBuffer data = ByteBuffer.allocate(cmdBytes.length + 1);
        data.put(cmdType.toByte());
        data.put(cmdBytes);
        BufferUtils.flip(data);
        return new Task(data, closure);
    }
}
