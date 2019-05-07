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
public class GetSequenceRequest extends BaseRequest {

    private static final long serialVersionUID = 8409861577121335137L;

    private byte[]            seqKey;
    private int               step;

    public byte[] getSeqKey() {
        return seqKey;
    }

    public void setSeqKey(byte[] seqKey) {
        this.seqKey = seqKey;
    }

    public int getStep() {
        return step;
    }

    public void setStep(int step) {
        this.step = step;
    }

    @Override
    public byte magic() {
        return GET_SEQUENCE;
    }

    @Override
    public String toString() {
        return "GetSequenceRequest{" + "seqKey=" + BytesUtil.toHex(seqKey) + ", step=" + step + "} " + super.toString();
    }
}
