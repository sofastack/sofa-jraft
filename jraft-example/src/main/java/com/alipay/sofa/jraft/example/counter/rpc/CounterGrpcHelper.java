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
package com.alipay.sofa.jraft.example.counter.rpc;

import com.alipay.sofa.jraft.rpc.RpcServer;
import com.alipay.sofa.jraft.util.RpcFactoryHelper;
import com.google.protobuf.Message;

import java.lang.reflect.Method;

public class CounterGrpcHelper {

    public static RpcServer rpcServer;

    public static void initGRpc() {
        if ("com.alipay.sofa.jraft.rpc.impl.GrpcRaftRpcFactory".equals(RpcFactoryHelper.rpcFactory().getClass()
            .getName())) {
            RpcFactoryHelper.rpcFactory().registerProtobufSerializer(CounterOutter.GetValueRequest.class.getName(),
                CounterOutter.GetValueRequest.getDefaultInstance());
            RpcFactoryHelper.rpcFactory().registerProtobufSerializer(
                CounterOutter.IncrementAndGetRequest.class.getName(),
                CounterOutter.IncrementAndGetRequest.getDefaultInstance());
            RpcFactoryHelper.rpcFactory().registerProtobufSerializer(CounterOutter.ValueResponse.class.getName(),
                CounterOutter.ValueResponse.getDefaultInstance());

            try {
                Class<?> clazz = Class.forName("com.alipay.sofa.jraft.rpc.impl.MarshallerHelper");
                Method method = clazz.getMethod("registerRespInstance", String.class, Message.class);
                method.invoke(null, CounterOutter.GetValueRequest.class.getName(),
                    CounterOutter.ValueResponse.getDefaultInstance());
                method.invoke(null, CounterOutter.IncrementAndGetRequest.class.getName(),
                    CounterOutter.ValueResponse.getDefaultInstance());
            } catch (Exception ignore) {
            }
        }
    }

    public static void setRpcServer(RpcServer rpcServer) {
        CounterGrpcHelper.rpcServer = rpcServer;
    }

    public static void blockUntilShutdown() {
        if (rpcServer == null) {
            return;
        }
        if ("com.alipay.sofa.jraft.rpc.impl.GrpcRaftRpcFactory".equals(RpcFactoryHelper.rpcFactory().getClass()
            .getName())) {
            try {
                Method method = rpcServer.getClass().getMethod("getServer");
                Object grpcServer = method.invoke(rpcServer);
                Method method1 = grpcServer.getClass().getMethod("awaitTermination");
                method1.invoke(grpcServer);
            } catch (Exception ignore) {
            }
        }
    }

}
