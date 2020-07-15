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
import com.alipay.sofa.jraft.rpc.RpcProcessor;
import com.alipay.sofa.jraft.test.atomic.command.RpcCommand.IncrementAndGetCommand;
import com.alipay.sofa.jraft.test.atomic.command.RpcCommand.SetCommand;
import com.alipay.sofa.jraft.test.atomic.command.RpcCommand.GetSlotsCommand;
import com.alipay.sofa.jraft.test.atomic.command.RpcCommand.CompareAndSetCommand;
import com.alipay.sofa.jraft.test.atomic.command.RpcCommand.GetCommand;
import com.alipay.sofa.jraft.test.atomic.command.RpcCommand.BaseResponseCommand;
import com.alipay.sofa.jraft.test.atomic.command.RpcCommand.BaseRequestCommand;
import com.alipay.sofa.jraft.util.Requires;

public class KVCommandProcessor implements RpcProcessor<BaseRequestCommand> {
    
    private KVService kvService;
    
    public KVCommandProcessor(final KVService kvService) {
        this.kvService = kvService;
    }
    
    @Override
    public void handleRequest(RpcContext rpcCtx, BaseRequestCommand baseCommand) {
        Requires.requireNonNull(baseCommand, "command");
        
        final RequestProcessClosure<BaseRequestCommand, BaseResponseCommand> closure = new RequestProcessClosure<>(
                baseCommand, rpcCtx);
        
        BaseRequestCommand.RequestType requestType = baseCommand.getRequestType();
        switch (requestType) {
            case get:
                GetCommand getCommand = baseCommand.getExtension(GetCommand.body);
                kvService.handleGetCommand(baseCommand, getCommand, closure);
                break;
            case compareAndSet:
                CompareAndSetCommand compareAndSetCommand = baseCommand.getExtension(CompareAndSetCommand.body);
                kvService.handleCompareAndSetCommand(baseCommand, compareAndSetCommand, closure);
                break;
            case getSlots:
                GetSlotsCommand getSlotsCommand = baseCommand.getExtension(GetSlotsCommand.body);
                kvService.handleGetSlotsCommand(baseCommand, getSlotsCommand, closure);
                break;
            case set:
                SetCommand setCommand = baseCommand.getExtension(SetCommand.body);
                kvService.handleSetCommand(baseCommand, setCommand, closure);
                break;
            case incrementAndGet:
                IncrementAndGetCommand incrementAndGetCommand = baseCommand.getExtension(IncrementAndGetCommand.body);
                kvService.handleIncrementAndGetCommand(baseCommand, incrementAndGetCommand, closure);
                break;
            default:
                throw new RuntimeException("Unsupported request type: " + requestType.name());
        }
    }
    
    
    @Override
    public String interest() {
        return BaseRequestCommand.class.getName();
    }
    
}
