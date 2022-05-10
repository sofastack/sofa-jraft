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
package com.alipay.sofa.jraft.test.atomic.server;

/**
 * command type
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-25 1:34:15 PM
 */
public enum CommandType {
    SET, CAS, INC, GET;

    public byte toByte() {
        switch (this) {
            case SET:
                return (byte) 0;
            case CAS:
                return (byte) 1;
            case INC:
                return (byte) 2;
            case GET:
                return (byte) 3;
        }
        throw new IllegalArgumentException();
    }

    public static CommandType parseByte(byte b) {
        switch (b) {
            case 0:
                return SET;
            case 1:
                return CAS;
            case 2:
                return INC;
            case 3:
                return GET;
        }
        throw new IllegalArgumentException();
    }

    public boolean isReadOp() {
        return GET.equals(this);
    }
}
