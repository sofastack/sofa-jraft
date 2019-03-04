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
package com.alipay.sofa.jraft.test.atomic.command;

import java.io.Serializable;

/**
 * Boolean command represents true or false.
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-25 1:25:10 PM
 */
public class BooleanCommand implements Serializable {
    private static final long serialVersionUID = 2776110757482798187L;
    private boolean           success;
    private String            errorMsg;
    private String            redirect;

    public String getRedirect() {
        return this.redirect;
    }

    public void setRedirect(String redirect) {
        this.redirect = redirect;
    }

    public BooleanCommand() {
        super();
    }

    public BooleanCommand(boolean result) {
        this(result, null);
    }

    public BooleanCommand(boolean result, String errorMsg) {
        super();
        this.success = result;
        this.errorMsg = errorMsg;
    }

    public String getErrorMsg() {
        return this.errorMsg;
    }

    public void setErrorMsg(String errorMsg) {
        this.errorMsg = errorMsg;
    }

    public boolean isSuccess() {
        return this.success;
    }

    public void setSuccess(boolean result) {
        this.success = result;
    }

}
