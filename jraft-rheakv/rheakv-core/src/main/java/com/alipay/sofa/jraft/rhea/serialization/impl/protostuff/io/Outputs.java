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
package com.alipay.sofa.jraft.rhea.serialization.impl.protostuff.io;

import io.protostuff.LinkedBuffer;
import io.protostuff.Output;
import io.protostuff.ProtostuffOutput;
import io.protostuff.WriteSession;

import com.alipay.sofa.jraft.rhea.serialization.io.OutputBuf;
import com.alipay.sofa.jraft.util.internal.UnsafeUtil;

/**
 *
 * @author jiachun.fjc
 */
public final class Outputs {

    public static Output getOutput(final OutputBuf outputBuf) {
        if (UnsafeUtil.hasUnsafe() && outputBuf.hasMemoryAddress()) {
            return new UnsafeNioBufOutput(outputBuf, -1, Integer.MAX_VALUE);
        }
        return new NioBufOutput(outputBuf, -1, Integer.MAX_VALUE);
    }

    public static Output getOutput(final LinkedBuffer buf) {
        return new ProtostuffOutput(buf);
    }

    public static byte[] toByteArray(final Output output) {
        if (output instanceof WriteSession) {
            return ((WriteSession) output).toByteArray();
        }
        throw new IllegalArgumentException("Output [" + output.getClass().getName()
                                           + "] must be a WriteSession's implementation");
    }

    private Outputs() {
    }
}
