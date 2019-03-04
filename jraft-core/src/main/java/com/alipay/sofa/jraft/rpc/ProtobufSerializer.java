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
package com.alipay.sofa.jraft.rpc;

import com.alipay.remoting.CustomSerializer;
import com.alipay.remoting.InvokeContext;
import com.alipay.remoting.exception.DeserializationException;
import com.alipay.remoting.exception.SerializationException;
import com.alipay.remoting.rpc.RequestCommand;
import com.alipay.remoting.rpc.ResponseCommand;
import com.alipay.remoting.rpc.protocol.RpcRequestCommand;
import com.alipay.remoting.rpc.protocol.RpcResponseCommand;
import com.google.protobuf.Message;

/**
 * RPC custom serializer based on protobuf
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Mar-26 4:43:21 PM
 */
public class ProtobufSerializer implements CustomSerializer {

    public static final ProtobufSerializer INSTANCE = new ProtobufSerializer();

    @Override
    public <T extends RequestCommand> boolean serializeHeader(T request, InvokeContext invokeContext)
                                                                                                     throws SerializationException {

        final RpcRequestCommand cmd = (RpcRequestCommand) request;
        final Message msg = (Message) cmd.getRequestObject();
        if (msg instanceof RpcRequests.AppendEntriesRequest) {
            final RpcRequests.AppendEntriesRequest req = (RpcRequests.AppendEntriesRequest) msg;
            final RpcRequests.AppendEntriesRequestHeader.Builder hb = RpcRequests.AppendEntriesRequestHeader
                .newBuilder() //
                .setGroupId(req.getGroupId()) //
                .setPeerId(req.getPeerId()) //
                .setServerId(req.getServerId());
            cmd.setHeader(hb.build().toByteArray());
            return true;
        }

        return false;
    }

    @Override
    public <T extends ResponseCommand> boolean serializeHeader(T response) throws SerializationException {
        return false;
    }

    @Override
    public <T extends RequestCommand> boolean deserializeHeader(T request) throws DeserializationException {
        final RpcRequestCommand cmd = (RpcRequestCommand) request;
        final String className = cmd.getRequestClass();
        if (className.equals(RpcRequests.AppendEntriesRequest.class.getName())) {
            final byte[] header = cmd.getHeader();
            cmd.setRequestHeader(ProtobufMsgFactory.newMessageByJavaClassName(
                RpcRequests.AppendEntriesRequestHeader.class.getName(), header));
            return true;
        }
        return false;
    }

    @Override
    public <T extends ResponseCommand> boolean deserializeHeader(T response, InvokeContext invokeContext)
                                                                                                         throws DeserializationException {
        return false;
    }

    @Override
    public <T extends RequestCommand> boolean serializeContent(T request, InvokeContext invokeContext)
                                                                                                      throws SerializationException {
        final RpcRequestCommand cmd = (RpcRequestCommand) request;
        final Message msg = (Message) cmd.getRequestObject();
        cmd.setContent(msg.toByteArray());
        return true;
    }

    @Override
    public <T extends ResponseCommand> boolean serializeContent(T response) throws SerializationException {
        final RpcResponseCommand cmd = (RpcResponseCommand) response;
        final Message msg = (Message) cmd.getResponseObject();
        cmd.setContent(msg.toByteArray());
        return true;
    }

    @Override
    public <T extends RequestCommand> boolean deserializeContent(T request) throws DeserializationException {
        final RpcRequestCommand cmd = (RpcRequestCommand) request;
        final String className = cmd.getRequestClass();

        cmd.setRequestObject(ProtobufMsgFactory.newMessageByJavaClassName(className, request.getContent()));
        return true;
    }

    @Override
    public <T extends ResponseCommand> boolean deserializeContent(T response, InvokeContext invokeContext)
                                                                                                          throws DeserializationException {
        final RpcResponseCommand cmd = (RpcResponseCommand) response;
        final String className = cmd.getResponseClass();

        cmd.setResponseObject(ProtobufMsgFactory.newMessageByJavaClassName(className, response.getContent()));
        return true;
    }

}
