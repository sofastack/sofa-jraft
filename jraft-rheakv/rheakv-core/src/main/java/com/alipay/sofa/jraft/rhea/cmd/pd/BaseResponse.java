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

import com.alipay.sofa.jraft.rhea.errors.Errors;

/**
 * RPC response header
 *
 * @author jiachun.fjc
 */
public abstract class BaseResponse<T> implements Serializable {

    private static final long serialVersionUID = 7595480549808409921L;

    private Errors            error            = Errors.NONE;
    private long              clusterId;
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

    public long getClusterId() {
        return clusterId;
    }

    public void setClusterId(long clusterId) {
        this.clusterId = clusterId;
    }

    public T getValue() {
        return value;
    }

    public void setValue(T value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "BaseResponse{" + "error=" + error + ", clusterId=" + clusterId + ", value=" + value + '}';
    }
}
