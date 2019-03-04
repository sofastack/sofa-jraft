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
package com.alipay.sofa.jraft.example.counter.rpc;

import java.io.Serializable;

/**
 * Value response.
 *
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-09 4:55:35 PM
 */
public class ValueResponse implements Serializable {

    private static final long serialVersionUID = -4220017686727146773L;

    private long              value;
    private boolean           success;

    /**
     * redirect peer id
     */
    private String            redirect;

    private String            errorMsg;

    public String getErrorMsg() {
        return this.errorMsg;
    }

    public void setErrorMsg(String errorMsg) {
        this.errorMsg = errorMsg;
    }

    public String getRedirect() {
        return this.redirect;
    }

    public void setRedirect(String redirect) {
        this.redirect = redirect;
    }

    public boolean isSuccess() {
        return this.success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public long getValue() {
        return this.value;
    }

    public void setValue(long value) {
        this.value = value;
    }

    public ValueResponse(long value, boolean success, String redirect, String errorMsg) {
        super();
        this.value = value;
        this.success = success;
        this.redirect = redirect;
        this.errorMsg = errorMsg;
    }

    public ValueResponse() {
        super();
    }

    @Override
    public String toString() {
        return "ValueResponse [value=" + this.value + ", success=" + this.success + ", redirect=" + this.redirect
               + ", errorMsg=" + this.errorMsg + "]";
    }

}
