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
import com.alipay.sofa.jraft.util.Requires;
import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.Message;

/**
 * Helper to use grpc and keep use normal java object.
 * se the protobuf object only when serialize
 *
 * @author: baozi
 */
public abstract class GrpcProtobufTransferHelper {

    private final static Map<String, GrpcSerializationTransfer> protobufTransfers = new ConcurrentHashMap<>();
    private final static Map<String, GrpcSerializationTransfer> javaBeanTransfers = new ConcurrentHashMap<>();

    /**
     * registry grpc serialization transfer
     *
     * @param javaBeanCls java bean class
     * @param protobufBeanCls protobuf bean class
     * @param transfer serialization transfer
     */
    public static void registryTransfer(final Class<?> javaBeanCls, final Class<? extends Message> protobufBeanCls,
                                        final GrpcSerializationTransfer transfer) {
        javaBeanTransfers.put(javaBeanCls.getName(), transfer);
        protobufTransfers.put(protobufBeanCls.getName(), transfer);
    }

    /**
     * protobuf java object transfer to ordinary java object
     *
     * @param object protobuf java object
     * @return ordinary java object
     * @throws GrpcSerializationTransferException
     */
    public static Object toJavaBean(final Message object) throws GrpcSerializationTransferException {
        Requires.requireNonNull(object, "transfer java bean fail, object is null");
        final GrpcSerializationTransfer gRpcJavaBeanTransfer = protobufTransfers.get(object.getClass().getName());
        if (gRpcJavaBeanTransfer != null) {
            try {
                return gRpcJavaBeanTransfer.protoBufTransJavaBean(object);
            } catch (final CodecException e) {
                throw new GrpcSerializationTransferException(String.format("transfer %s fail", object.getClass()
                    .getName()), e);
            }
        }
        return object;
    }

    /**
     * ordinary java object transfer to protobuf java object
     *
     * @param object ordinary java object
     * @return protobuf java object
     * @throws GrpcSerializationTransferException
     */
    public static Message toProtoBean(final Object object) throws GrpcSerializationTransferException {
        Requires.requireNonNull(object, "transfer protobuf bean fail, object is null");
        if (object instanceof Message) {
            return (Message) object;
        }
        final GrpcSerializationTransfer gRpcJavaBeanTransfer = javaBeanTransfers.get(object.getClass().getName());
        if (gRpcJavaBeanTransfer != null) {
            try {
                return gRpcJavaBeanTransfer.javaBeanTransProtobufBean(object);
            } catch (final CodecException e) {
                throw new GrpcSerializationTransferException(String.format("transfer %s fail", object.getClass()
                    .getName()), e);
            }
        } else {
            throw new GrpcSerializationTransferException(String.format(
                "not found serialization transfer by %s, please call registryTransfer()", object.getClass().getName()));
        }
    }
}
