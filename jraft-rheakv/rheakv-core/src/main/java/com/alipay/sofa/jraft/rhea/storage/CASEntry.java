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
package com.alipay.sofa.jraft.rhea.storage;

import java.io.Serializable;

import com.alipay.sofa.jraft.util.BytesUtil;

/**
 *
 * @author jiachun.fjc
 */
public class CASEntry implements Serializable {

    private static final long serialVersionUID = -8024887237976722615L;

    private byte[]            key;
    private byte[]            expect;
    private byte[]            update;

    public CASEntry() {
    }

    public CASEntry(byte[] key, byte[] expect, byte[] update) {
        this.key = key;
        this.expect = expect;
        this.update = update;
    }

    public byte[] getKey() {
        return key;
    }

    public void setKey(byte[] key) {
        this.key = key;
    }

    public byte[] getExpect() {
        return expect;
    }

    public void setExpect(byte[] expect) {
        this.expect = expect;
    }

    public byte[] getUpdate() {
        return update;
    }

    public void setUpdate(byte[] update) {
        this.update = update;
    }

    public int length() {
        return (this.key == null ? 0 : this.key.length) + (this.expect == null ? 0 : this.expect.length)
               + (this.update == null ? 0 : this.update.length);
    }

    @Override
    public String toString() {
        return "CASEntry{" + "key=" + BytesUtil.toHex(key) + ", expect=" + BytesUtil.toHex(expect) + ", update="
               + BytesUtil.toHex(update) + '}';
    }
}
