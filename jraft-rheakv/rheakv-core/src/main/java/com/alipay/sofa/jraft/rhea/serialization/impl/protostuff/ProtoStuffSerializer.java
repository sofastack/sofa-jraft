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
package com.alipay.sofa.jraft.rhea.serialization.impl.protostuff;

import java.io.IOException;
import java.nio.ByteBuffer;

import io.protostuff.Input;
import io.protostuff.LinkedBuffer;
import io.protostuff.Output;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;

import com.alipay.sofa.jraft.rhea.serialization.Serializer;
import com.alipay.sofa.jraft.rhea.serialization.impl.protostuff.io.Inputs;
import com.alipay.sofa.jraft.rhea.serialization.impl.protostuff.io.LinkedBuffers;
import com.alipay.sofa.jraft.rhea.serialization.impl.protostuff.io.Outputs;
import com.alipay.sofa.jraft.rhea.serialization.io.InputBuf;
import com.alipay.sofa.jraft.rhea.serialization.io.OutputBuf;
import com.alipay.sofa.jraft.util.internal.ThrowUtil;
import com.alipay.sofa.jraft.util.SystemPropertyUtil;

/**
 *
 * @author jiachun.fjc
 */
public class ProtoStuffSerializer extends Serializer {

    static {
        // see io.protostuff.runtime.RuntimeEnv

        // If true, the constructor will always be obtained from {@code ReflectionFactory.newConstructorFromSerialization}.
        //
        // Enable this if you intend to avoid deserialize objects whose no-args constructor initializes (unwanted)
        // internal state. This applies to complex/framework objects.
        //
        // If you intend to fill default field values using your default constructor, leave this disabled. This normally
        // applies to java beans/data objects.
        //
        final String always_use_sun_reflection_factory = SystemPropertyUtil.get(
            "rhea.serializer.protostuff.always_use_sun_reflection_factory", "false");
        SystemPropertyUtil.setProperty("protostuff.runtime.always_use_sun_reflection_factory",
            always_use_sun_reflection_factory);

        // Disabled by default.  Writes a sentinel value (uint32) in place of null values.
        //
        // default is false
        final String allow_null_array_element = SystemPropertyUtil.get(
            "rhea.serializer.protostuff.allow_null_array_element", "false");
        SystemPropertyUtil.setProperty("protostuff.runtime.allow_null_array_element", allow_null_array_element);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> OutputBuf writeObject(final OutputBuf outputBuf, final T obj) {
        final Schema<T> schema = RuntimeSchema.getSchema((Class<T>) obj.getClass());

        final Output output = Outputs.getOutput(outputBuf);
        try {
            schema.writeTo(output, obj);
        } catch (final IOException e) {
            ThrowUtil.throwException(e);
        }

        return outputBuf;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> byte[] writeObject(final T obj) {
        final Schema<T> schema = RuntimeSchema.getSchema((Class<T>) obj.getClass());

        final LinkedBuffer buf = LinkedBuffers.getLinkedBuffer();
        final Output output = Outputs.getOutput(buf);
        try {
            schema.writeTo(output, obj);
            return Outputs.toByteArray(output);
        } catch (final IOException e) {
            ThrowUtil.throwException(e);
        } finally {
            LinkedBuffers.resetBuf(buf); // for reuse
        }

        return null; // never get here
    }

    @Override
    public <T> T readObject(final InputBuf inputBuf, final Class<T> clazz) {
        final Schema<T> schema = RuntimeSchema.getSchema(clazz);
        final T msg = schema.newMessage();

        final Input input = Inputs.getInput(inputBuf);
        try {
            schema.mergeFrom(input, msg);
            Inputs.checkLastTagWas(input, 0);
        } catch (final IOException e) {
            ThrowUtil.throwException(e);
        } finally {
            inputBuf.release();
        }

        return msg;
    }

    @Override
    public <T> T readObject(final ByteBuffer buf, final Class<T> clazz) {
        final Schema<T> schema = RuntimeSchema.getSchema(clazz);
        final T msg = schema.newMessage();

        final Input input = Inputs.getInput(buf);
        try {
            schema.mergeFrom(input, msg);
            Inputs.checkLastTagWas(input, 0);
        } catch (final IOException e) {
            ThrowUtil.throwException(e);
        }

        return msg;
    }

    @Override
    public <T> T readObject(final byte[] bytes, final int offset, final int length, final Class<T> clazz) {
        final Schema<T> schema = RuntimeSchema.getSchema(clazz);
        final T msg = schema.newMessage();

        final Input input = Inputs.getInput(bytes, offset, length);
        try {
            schema.mergeFrom(input, msg);
            Inputs.checkLastTagWas(input, 0);
        } catch (final IOException e) {
            ThrowUtil.throwException(e);
        }

        return msg;
    }

    @Override
    public String toString() {
        return "proto_stuff";
    }
}
