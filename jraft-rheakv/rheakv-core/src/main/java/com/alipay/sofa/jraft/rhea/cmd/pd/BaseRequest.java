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
package com.alipay.sofa.jraft.rhea.cmd.pd;

import java.io.Serializable;

/**
 * RPC request header
 *
 * @author jiachun.fjc
 */
public abstract class BaseRequest implements Serializable {

    private static final long serialVersionUID = 1056021642901412112L;

    public static final byte  STORE_HEARTBEAT  = 0x01;
    public static final byte  REGION_HEARTBEAT = 0x02;
    public static final byte  GET_CLUSTER_INFO = 0x03;
    public static final byte  GET_STORE_INFO   = 0x04;
    public static final byte  SET_STORE_INFO   = 0x05;
    public static final byte  GET_STORE_ID     = 0x06;
    public static final byte  CREATE_REGION_ID = 0x07;

    private long              clusterId;

    public long getClusterId() {
        return clusterId;
    }

    public void setClusterId(long clusterId) {
        this.clusterId = clusterId;
    }

    public abstract byte magic();

    @Override
    public String toString() {
        return "BaseRequest{" + "clusterId=" + clusterId + '}';
    }
}
