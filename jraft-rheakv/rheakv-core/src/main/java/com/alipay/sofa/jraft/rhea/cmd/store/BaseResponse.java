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

import java.io.Serializable;

import com.alipay.sofa.jraft.rhea.errors.Errors;
import com.alipay.sofa.jraft.rhea.metadata.RegionEpoch;

/**
 * RPC response header
 *
 * @author jiachun.fjc
 */
public class BaseResponse<T> implements Serializable {

    private static final long serialVersionUID = 8411573936817037697L;

    private Errors            error            = Errors.NONE;
    private long              regionId;
    private RegionEpoch       regionEpoch;
    private T                 value;

    public boolean isSuccess() {
        return error == Errors.NONE;
    }

    public Errors getError() {
        return error;
    }

    public void setError(Errors error) {
        this.error = error;
    }

    public long getRegionId() {
        return regionId;
    }

    public void setRegionId(long regionId) {
        this.regionId = regionId;
    }

    public RegionEpoch getRegionEpoch() {
        return regionEpoch;
    }

    public void setRegionEpoch(RegionEpoch regionEpoch) {
        this.regionEpoch = regionEpoch;
    }

    public T getValue() {
        return value;
    }

    public void setValue(T value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "BaseResponse{" + "error=" + error + ", regionId=" + regionId + ", regionEpoch=" + regionEpoch
               + ", value=" + value + '}';
    }
}
