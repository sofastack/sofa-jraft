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

import com.alipay.remoting.AsyncContext;
import com.alipay.remoting.BizContext;
import com.alipay.remoting.rpc.protocol.AsyncUserProcessor;
import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.entity.Task;
import com.alipay.sofa.jraft.test.atomic.command.BaseRequestCommand;
import com.alipay.sofa.jraft.test.atomic.command.BooleanCommand;
import com.alipay.sofa.jraft.test.atomic.command.CommandCodec;
import com.alipay.sofa.jraft.test.atomic.server.AtomicRangeGroup;
import com.alipay.sofa.jraft.test.atomic.server.AtomicServer;
import com.alipay.sofa.jraft.test.atomic.server.CommandType;
import com.alipay.sofa.jraft.test.atomic.server.LeaderTaskClosure;

public abstract class BaseAsyncUserProcessor<T extends BaseRequestCommand> extends AsyncUserProcessor<T> {
    protected AtomicServer server;

    public BaseAsyncUserProcessor(AtomicServer server) {
        super();
        this.server = server;
    }

    @Override
    public void handleRequest(BizContext bizCtx, AsyncContext asyncCtx, T request) {
        final AtomicRangeGroup group = server.getGroupBykey(request.getKey());
        if (!group.getFsm().isLeader()) {
            asyncCtx.sendResponse(group.redirect());
            return;
        }

        final CommandType cmdType = getCmdType();
        final Task task = createTask(asyncCtx, request, cmdType);
        group.getNode().apply(task);
    }

    protected abstract CommandType getCmdType();

    private Task createTask(AsyncContext asyncCtx, T request, CommandType cmdType) {
        final LeaderTaskClosure closure = new LeaderTaskClosure();
        closure.setCmd(request);
        closure.setCmdType(cmdType);
        closure.setDone(new Closure() {

            @Override
            public void run(Status status) {
                if (status.isOk()) {
                    asyncCtx.sendResponse(closure.getResponse());
                } else {
                    asyncCtx.sendResponse(new BooleanCommand(false, status.getErrorMsg()));
                }
            }
        });
        final byte[] cmdBytes = CommandCodec.encodeCommand(request);
        final ByteBuffer data = ByteBuffer.allocate(cmdBytes.length + 1);
        data.put(cmdType.toByte());
        data.put(cmdBytes);
        data.flip();
        final Task task = new Task(data, closure);
        return task;
    }

}
