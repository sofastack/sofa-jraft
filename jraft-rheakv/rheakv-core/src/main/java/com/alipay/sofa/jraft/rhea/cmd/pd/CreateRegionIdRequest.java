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

import com.alipay.sofa.jraft.util.Endpoint;

/**
 * @author jiachun.fjc
 */
public class CreateRegionIdRequest extends BaseRequest {

    private static final long serialVersionUID = -2166959562654571711L;

    private Endpoint          endpoint;

    public Endpoint getEndpoint() {
        return endpoint;
    }

    public void setEndpoint(Endpoint endpoint) {
        this.endpoint = endpoint;
    }

    @Override
    public byte magic() {
        return CREATE_REGION_ID;
    }

    @Override
    public String toString() {
        return "CreateRegionIdRequest{" + "endpoint=" + endpoint + "} " + super.toString();
    }
}
