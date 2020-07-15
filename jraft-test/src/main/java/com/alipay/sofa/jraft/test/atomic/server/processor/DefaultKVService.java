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
import com.alipay.sofa.jraft.test.atomic.KeyNotFoundException;
import com.alipay.sofa.jraft.test.atomic.command.RpcCommand.SetCommand;
import com.alipay.sofa.jraft.test.atomic.command.RpcCommand.IncrementAndGetCommand;
import com.alipay.sofa.jraft.test.atomic.command.RpcCommand.GetSlotsCommand;
import com.alipay.sofa.jraft.test.atomic.command.RpcCommand.CompareAndSetCommand;
import com.alipay.sofa.jraft.test.atomic.command.RpcCommand.GetCommand;
import com.alipay.sofa.jraft.test.atomic.command.RpcCommand.BaseResponseCommand;
import com.alipay.sofa.jraft.test.atomic.command.RpcCommand.BaseRequestCommand;
import com.alipay.sofa.jraft.test.atomic.command.RpcCommand.BaseRequestCommand.RequestType;
import com.alipay.sofa.jraft.test.atomic.server.AtomicRangeGroup;
import com.alipay.sofa.jraft.test.atomic.server.AtomicServer;
import com.alipay.sofa.jraft.test.atomic.server.LeaderTaskClosure;

import java.nio.ByteBuffer;


public class DefaultKVService implements KVService {
    
    private AtomicServer server;
    
    public DefaultKVService(AtomicServer server) {
        this.server = server;
    }
    
    @Override
    public void handleGetCommand(BaseRequestCommand baseRequestCommand, GetCommand getCommand,
            RequestProcessClosure<BaseRequestCommand, BaseResponseCommand> closure) {
        final BaseResponseCommand.Builder response = BaseResponseCommand.newBuilder();
        
        if (getCommand.getReadByStateMachine()) {
            //super.handleRequest(rpcCtx, request);
            final AtomicRangeGroup group = server.getGroupBykey(baseRequestCommand.getKey());
            if (!group.getFsm().isLeader()) {
                // rpcCtx.sendResponse(group.redirect());
                closure.sendResponse(group.redirect());
                return;
            }
            
            final Task task = createTask(closure, baseRequestCommand);
            group.getNode().apply(task);
            
        } else {
            try {
                final AtomicRangeGroup group = server.getGroupBykey(baseRequestCommand.getKey());
                
                if (!getCommand.getReadFromQuorum()) {
                    //rpcCtx.sendResponse(new ValueCommand(group.getFsm().getValue(request.getKey())));
                    response.setVlaue(group.getFsm().getValue(baseRequestCommand.getKey()));
                    closure.sendResponse(response.build());
                } else {
                    //group.readFromQuorum(request.getKey(), rpcCtx);
                    group.readFromQuorum(baseRequestCommand.getKey(), closure.getRpcCtx());
                }
            } catch (final KeyNotFoundException e) {
                response.setSuccess(false).setErrorMsg("key not found");
                closure.sendResponse(response.build());
            }
        }
    }
    
    @Override
    public void handleCompareAndSetCommand(BaseRequestCommand baseRequestCommand,
            CompareAndSetCommand compareAndSetCommand,
            RequestProcessClosure<BaseRequestCommand, BaseResponseCommand> closure) {
    }
    
    @Override
    public void handleGetSlotsCommand(BaseRequestCommand baseRequestCommand, GetSlotsCommand getSlotsCommand,
            RequestProcessClosure<BaseRequestCommand, BaseResponseCommand> closure) {
        
        //final SlotsResponseCommand response = new SlotsResponseCommand();
        //response.setMap(this.server.getGroups());
        //rpcCtx.sendResponse(response);
        
        final BaseResponseCommand.Builder response = BaseResponseCommand.newBuilder();
        response.putAllMap(this.server.getGroups());
        closure.sendResponse(response.build());
    }
    
    @Override
    public void handleIncrementAndGetCommand(BaseRequestCommand baseRequestCommand,
            IncrementAndGetCommand incrementAndGetCommand,
            RequestProcessClosure<BaseRequestCommand, BaseResponseCommand> closure) {
    }
    
    @Override
    public void handleSetCommand(BaseRequestCommand baseRequestCommand, SetCommand setCommand,
            RequestProcessClosure<BaseRequestCommand, BaseResponseCommand> closure) {
    }
    
    private Task createTask(RequestProcessClosure<BaseRequestCommand, BaseResponseCommand> responseClosure,
            BaseRequestCommand command) {
        
        final BaseResponseCommand.Builder response = BaseResponseCommand.newBuilder();
        
        final LeaderTaskClosure closure = new LeaderTaskClosure();
        closure.setCmd(command);
        closure.setRequestType(command.getRequestType());
        closure.setDone(status -> {
            if (status.isOk()) {
                //asyncCtx.sendResponse(closure.getResponse());
                responseClosure.sendResponse(closure.getResponse());
            } else {
                //asyncCtx.sendResponse(new BooleanCommand(false, status.getErrorMsg()));
                response.setErrorMsg(status.getErrorMsg());
                response.setSuccess(false);
                responseClosure.sendResponse(response.build());
            }
        });
        //final byte[] cmdBytes = CommandCodec.encodeCommand(request);
        final byte[] cmdBytes = command.toByteArray();
        final byte cmdByte = toByte(command.getRequestType());
        final ByteBuffer data = ByteBuffer.allocate(cmdBytes.length + 1);
        data.put(cmdByte);
        data.put(cmdBytes);
        data.flip();
        return new Task(data, closure);
    }
    
    public byte toByte(RequestType requestType) {
        switch (requestType) {
            case set:
                return (byte) 0;
            case compareAndSet:
                return (byte) 1;
            case incrementAndGet:
                return (byte) 2;
            case get:
                return (byte) 3;
        }
        throw new IllegalArgumentException();
    }
}
