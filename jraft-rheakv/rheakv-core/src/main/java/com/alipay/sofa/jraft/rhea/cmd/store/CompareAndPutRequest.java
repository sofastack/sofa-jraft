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
package com.alipay.sofa.jraft.rhea.cmd.store;

import com.alipay.sofa.jraft.util.BytesUtil;

/**
 *
 * @author nicholas.jxf
 */
public class CompareAndPutRequest extends BaseRequest {

    private static final long serialVersionUID = -6395140862740192718L;

    private byte[]            key;
    private byte[]            expect;
    private byte[]            update;

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

    @Override
    public byte magic() {
        return COMPARE_PUT;
    }

    @Override
    public String toString() {
        return "CompareAndPutRequest{" + "key=" + BytesUtil.toHex(key) + ", expect=" + BytesUtil.toHex(expect)
               + ", update=" + BytesUtil.toHex(update) + "} " + super.toString();
    }
}
