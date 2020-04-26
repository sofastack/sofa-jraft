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
 * @author jiachun.fjc
 */
public class ScanRequest extends BaseRequest {

    private static final long serialVersionUID = -7229126480671449199L;

    private byte[]            startKey;
    private byte[]            endKey;
    // If limit == 0, it will be modified to Integer.MAX_VALUE on the server
    // and then queried.  So 'limit == 0' means that the number of queries is
    // not limited. This is because serialization uses varint to compress
    // numbers.  In the case of 0, only 1 byte is occupied, and Integer.MAX_VALUE
    // takes 5 bytes.
    private int               limit;
    private boolean           readOnlySafe     = true;
    private boolean           returnValue      = true;
    private boolean           reverse          = false;

    public boolean isReverse() {
        return reverse;
    }

    public void setReverse(boolean reverse) {
        this.reverse = reverse;
    }

    public byte[] getStartKey() {
        return startKey;
    }

    public void setStartKey(byte[] startKey) {
        this.startKey = startKey;
    }

    public byte[] getEndKey() {
        return endKey;
    }

    public void setEndKey(byte[] endKey) {
        this.endKey = endKey;
    }

    public int getLimit() {
        return limit;
    }

    public void setLimit(int limit) {
        this.limit = limit;
    }

    public boolean isReadOnlySafe() {
        return readOnlySafe;
    }

    public void setReadOnlySafe(boolean readOnlySafe) {
        this.readOnlySafe = readOnlySafe;
    }

    public boolean isReturnValue() {
        return returnValue;
    }

    public void setReturnValue(boolean returnValue) {
        this.returnValue = returnValue;
    }

    @Override
    public byte magic() {
        return SCAN;
    }

    @Override
    public String toString() {
        return "ScanRequest{" + "startKey=" + BytesUtil.toHex(startKey) + ", endKey=" + BytesUtil.toHex(endKey)
               + ", limit=" + limit + ", reverse=" + reverse + ", readOnlySafe=" + readOnlySafe + ", returnValue="
               + returnValue + "} " + super.toString();
    }
}
