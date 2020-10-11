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
package com.alipay.sofa.jraft.rpc.impl;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.alipay.remoting.exception.CodecException;
import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.Message;

/**
 * @Author: baozi
 * @Date: 2020/9/24 09:19
 */
public abstract class GrpcProtobufTransferHelper<A, L extends GeneratedMessageV3> {

    private final static Map<String, GRpcSerializationTransfer> protobufTransfers = new ConcurrentHashMap<>();
    private final static Map<String, GRpcSerializationTransfer> javaBeanTransfers = new ConcurrentHashMap<>();

    /**
     * @param javaBeanCls
     * @param protobufBeanCls
     * @param transfer
     */
    public static void registryTransfer(Class<?> javaBeanCls, Class<? extends Message> protobufBeanCls,
                                        GRpcSerializationTransfer transfer) {
        javaBeanTransfers.put(javaBeanCls.getName(), transfer);
        protobufTransfers.put(protobufBeanCls.getName(), transfer);
    }

    /**
     * @param object
     * @return
     */
    public static Object transferJavaBean(Message object) throws GrpcSerializationTransferException {
        GRpcSerializationTransfer gRpcJavaBeanTransfer = protobufTransfers.get(object.getClass().getName());
        if (gRpcJavaBeanTransfer != null) {
            try {
                return gRpcJavaBeanTransfer.protoBufTransJavaBean(object);
            } catch (CodecException e) {
                throw new GrpcSerializationTransferException(String.format("transfer %s fail", object.getClass()
                    .getName()), e);
            }
        }
        return object;
    }

    /**
     * @param object
     * @return
     */
    public static Message transferProtoBean(final Object object) throws GrpcSerializationTransferException {
        GRpcSerializationTransfer gRpcJavaBeanTransfer = javaBeanTransfers.get(object.getClass().getName());
        if (gRpcJavaBeanTransfer != null) {
            try {
                return gRpcJavaBeanTransfer.javaBeanTransProtobufBean(object);
            } catch (CodecException e) {
                throw new GrpcSerializationTransferException(String.format("transfer %s fail", object.getClass()
                    .getName()), e);
            }
        }
        return (Message) object;
    }
}
