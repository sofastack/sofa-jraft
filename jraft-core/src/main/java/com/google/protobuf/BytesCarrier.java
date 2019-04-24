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
package com.google.protobuf;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * 
 * @author jiachun.fjc
 */
public class BytesCarrier extends ByteOutput {

    private byte[]  value;
    private boolean valid;

    public byte[] getValue() {
        return value;
    }

    public boolean isValid() {
        return valid;
    }

    @Override
    public void write(final byte value) {
        this.valid = false;
    }

    @Override
    public void write(final byte[] value, final int offset, final int length) {
        doWrite(value, offset, length);
    }

    @Override
    public void writeLazy(final byte[] value, final int offset, final int length) {
        doWrite(value, offset, length);
    }

    @Override
    public void write(final ByteBuffer value) throws IOException {
        this.valid = false;
    }

    @Override
    public void writeLazy(final ByteBuffer value) throws IOException {
        this.valid = false;
    }

    private void doWrite(final byte[] value, final int offset, final int length) {
        if (this.value != null) {
            this.valid = false;
            return;
        }
        if (offset == 0 && length == value.length) {
            this.value = value;
            this.valid = true;
        }
    }
}
