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

import com.alipay.sofa.jraft.entity.Task;
import com.alipay.sofa.jraft.test.atomic.command.CommandCodec;
import com.alipay.sofa.jraft.test.atomic.command.RpcCommand.SetCommand;
import com.alipay.sofa.jraft.test.atomic.command.RpcCommand.IncrementAndGetCommand;
import com.alipay.sofa.jraft.test.atomic.command.RpcCommand.GetSlotsCommand;
import com.alipay.sofa.jraft.test.atomic.command.RpcCommand.CompareAndSetCommand;
import com.alipay.sofa.jraft.test.atomic.command.RpcCommand.GetCommand;
import com.alipay.sofa.jraft.test.atomic.command.RpcCommand.BaseResponseCommand;
import com.alipay.sofa.jraft.test.atomic.command.RpcCommand.BaseRequestCommand;
import com.alipay.sofa.jraft.test.atomic.KeyNotFoundException;
import com.alipay.sofa.jraft.test.atomic.server.AtomicRangeGroup;
import com.alipay.sofa.jraft.test.atomic.server.AtomicServer;
import com.alipay.sofa.jraft.test.atomic.server.LeaderTaskClosure;
import com.alipay.sofa.jraft.test.atomic.server.RequestCommandType;

import java.nio.ByteBuffer;

public class DefaultKVService implements KVService {

    private AtomicServer server;

    public DefaultKVService(AtomicServer server) {
        this.server = server;
    }

    @Override
    public void handleGetCommand(BaseRequestCommand baseReqCmd, GetCommand getCommand,
                                 RequestProcessClosure<BaseRequestCommand, BaseResponseCommand> closure) {
        final BaseResponseCommand.Builder response = BaseResponseCommand.newBuilder();

        if (getCommand.getReadByStateMachine()) {
            handleReqCmd(baseReqCmd, closure);
        } else {
            try {
                final AtomicRangeGroup group = server.getGroupBykey(baseReqCmd.getKey());

                if (!getCommand.getReadFromQuorum()) {
                    response.setVlaue(group.getFsm().getValue(baseReqCmd.getKey()));
                    closure.sendResponse(response.build());
                } else {
                    group.readFromQuorum(baseReqCmd.getKey(), closure.getRpcCtx());
                }
            } catch (final KeyNotFoundException e) {
                response.setSuccess(false).setErrorMsg("key not found");
                closure.sendResponse(response.build());
            }
        }
    }

    @Override
    public void handleCompareAndSetCommand(BaseRequestCommand baseReqCmd, CompareAndSetCommand compAndSetCmd,
                                           RequestProcessClosure<BaseRequestCommand, BaseResponseCommand> closure) {
        handleReqCmd(baseReqCmd, closure);
    }

    @Override
    public void handleGetSlotsCommand(BaseRequestCommand baseReqCmd, GetSlotsCommand getSlotsCmd,
                                      RequestProcessClosure<BaseRequestCommand, BaseResponseCommand> closure) {

        final BaseResponseCommand.Builder response = BaseResponseCommand.newBuilder();
        response.putAllMap(this.server.getGroups());
        response.setSuccess(true);
        closure.sendResponse(response.build());
    }

    @Override
    public void handleIncrementAndGetCommand(BaseRequestCommand baseReqCmd, IncrementAndGetCommand increAndGetCmd,
                                             RequestProcessClosure<BaseRequestCommand, BaseResponseCommand> closure) {
        handleReqCmd(baseReqCmd, closure);
    }

    @Override
    public void handleSetCommand(BaseRequestCommand baseReqCmd, SetCommand setCmd,
                                 RequestProcessClosure<BaseRequestCommand, BaseResponseCommand> closure) {
        handleReqCmd(baseReqCmd, closure);
    }

    private Task createTask(RequestProcessClosure<BaseRequestCommand, BaseResponseCommand> responseClosure,
            BaseRequestCommand command) {
        
        final BaseResponseCommand.Builder response = BaseResponseCommand.newBuilder();
        
        final LeaderTaskClosure closure = new LeaderTaskClosure();
        closure.setCmd(command);
        closure.setRequestType(command.getRequestType());
        closure.setDone(status -> {
            if (status.isOk()) {
                responseClosure.sendResponse(closure.getResponse());
            } else {
                response.setErrorMsg(status.getErrorMsg());
                response.setSuccess(false);
                responseClosure.sendResponse(response.build());
            }
        });

        final byte[] cmdBytes = CommandCodec.encodeCommand(command);
        final byte cmdByte = RequestCommandType.toByte(command.getRequestType());
        final ByteBuffer data = ByteBuffer.allocate(cmdBytes.length + 1);
        data.put(cmdByte);
        data.put(cmdBytes);
        data.flip();
        return new Task(data, closure);
    }

    private void handleReqCmd(final BaseRequestCommand baseReqCmd,
                              RequestProcessClosure<BaseRequestCommand, BaseResponseCommand> closure) {

        final AtomicRangeGroup group = server.getGroupBykey(baseReqCmd.getKey());
        if (!group.getFsm().isLeader()) {
            closure.sendResponse(group.redirect());
            return;
        }

        final Task task = createTask(closure, baseReqCmd);
        group.getNode().apply(task);
    }

}
