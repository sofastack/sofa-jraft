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
package com.alipay.sofa.jraft.example.flexibleRaft;

import java.io.Serializable;

/**
 * @author Akai
 */
public class Operation implements Serializable {

    private static final long serialVersionUID = -6597003954824547295L;

    /** Get value */
    public static final byte  GET              = 0x01;
    /** Increment and get value */
    public static final byte  INCREMENT        = 0x02;

    private byte              op;
    private long              delta;

    public static Operation createGet() {
        return new Operation(GET);
    }

    public static Operation createIncrement(final long delta) {
        return new Operation(INCREMENT, delta);
    }

    public Operation(byte op) {
        this(op, 0);
    }

    public Operation(byte op, long delta) {
        this.op = op;
        this.delta = delta;
    }

    public byte getOp() {
        return op;
    }

    public long getDelta() {
        return delta;
    }

    public boolean isReadOp() {
        return GET == this.op;
    }
}
