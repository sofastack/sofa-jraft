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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import io.grpc.MethodDescriptor;

import com.alipay.sofa.jraft.util.internal.ThrowUtil;

public class ObjectMarshaller implements MethodDescriptor.Marshaller<Object> {

    public static ObjectMarshaller INSTANCE = new ObjectMarshaller();

    @Override
    public InputStream stream(final Object value) {
        final ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try {
            final ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(value);
            oos.flush();
            oos.close();
        } catch (final IOException e) {
            ThrowUtil.throwException(e);
        }
        return new ByteArrayInputStream(bos.toByteArray());
    }

    @Override
    public Object parse(final InputStream in) {
        try {
            final ObjectInputStream ois = new ObjectInputStream(in);
            return ois.readObject();
        } catch (final IOException | ClassNotFoundException e) {
            ThrowUtil.throwException(e);
        }
        return null;
    }
}
