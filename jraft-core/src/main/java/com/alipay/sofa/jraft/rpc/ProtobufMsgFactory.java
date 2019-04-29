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

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.SerializationException;

import com.alipay.remoting.CustomSerializerManager;
import com.alipay.sofa.jraft.error.MessageClassNotFoundException;
import com.alipay.sofa.jraft.storage.io.ProtoBufFile;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Message;

import static java.lang.invoke.MethodType.methodType;

/**
 * Protobuf message factory.
 *
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Mar-26 4:44:20 PM
 */
public class ProtobufMsgFactory {

    private static Map<String/* class name in proto file */, MethodHandle> PARSE_METHODS_4PROTO = new HashMap<>();
    private static Map<String/* class name in java file */, MethodHandle>  PARSE_METHODS_4J     = new HashMap<>();

    static {
        try {
            FileDescriptorSet descriptorSet = FileDescriptorSet.parseFrom(ProtoBufFile.class
                .getResourceAsStream("/raft.desc"));
            List<FileDescriptor> resolveFDs = new ArrayList<>();
            for (FileDescriptorProto fdp : descriptorSet.getFileList()) {

                FileDescriptor[] dependencies = new FileDescriptor[resolveFDs.size()];
                resolveFDs.toArray(dependencies);

                FileDescriptor fd = FileDescriptor.buildFrom(fdp, dependencies);
                resolveFDs.add(fd);
                for (Descriptor descriptor : fd.getMessageTypes()) {

                    String className = fdp.getOptions().getJavaPackage() + "."
                                       + fdp.getOptions().getJavaOuterClassname() + "$" + descriptor.getName();
                    Class<?> clazz = Class.forName(className);
                    MethodHandle methodHandle = MethodHandles.lookup().findStatic(clazz, "parseFrom",
                        methodType(clazz, byte[].class));
                    PARSE_METHODS_4PROTO.put(descriptor.getFullName(), methodHandle);
                    PARSE_METHODS_4J.put(className, methodHandle);
                    CustomSerializerManager.registerCustomSerializer(className, ProtobufSerializer.INSTANCE);
                }

            }

        } catch (Exception e) {
            e.printStackTrace(); // NOPMD
        }
    }

    public static void load() {
        if (PARSE_METHODS_4J.isEmpty() || PARSE_METHODS_4PROTO.isEmpty()) {
            throw new IllegalStateException("Parse protocol file failed.");
        }
    }

    @SuppressWarnings("unchecked")
    public static <T extends Message> T newMessageByJavaClassName(String className, byte[] bs) {
        MethodHandle handle = PARSE_METHODS_4J.get(className);
        if (handle == null) {
            throw new MessageClassNotFoundException(className + " not found");
        }
        try {
            return (T) handle.invoke(bs);
        } catch (Throwable t) {
            throw new SerializationException(t);
        }
    }

    @SuppressWarnings("unchecked")
    public static <T extends Message> T newMessageByProtoClassName(String className, byte[] bs) {
        MethodHandle handle = PARSE_METHODS_4PROTO.get(className);
        if (handle == null) {
            throw new MessageClassNotFoundException(className + " not found");
        }
        try {
            return (T) handle.invoke(bs);
        } catch (Throwable t) {
            throw new SerializationException(t);
        }
    }
}
