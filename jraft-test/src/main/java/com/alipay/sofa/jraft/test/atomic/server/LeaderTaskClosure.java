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

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.test.atomic.command.RpcCommand.BaseResponseCommand;
import com.alipay.sofa.jraft.test.atomic.command.RpcCommand.BaseRequestCommand.RequestType;

/**
 * Leader closure to apply task.
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-25 1:35:11 PM
 */
public class LeaderTaskClosure implements Closure {

    private Object              cmd;
    private RequestType         requestType;
    private Closure             done;
    private BaseResponseCommand response;

    @Override
    public void run(Status status) {
        if (this.done != null) {
            done.run(status);
        }
    }

    public Object getCmd() {
        return this.cmd;
    }

    public void setCmd(Object cmd) {
        this.cmd = cmd;
    }

    public Closure getDone() {
        return this.done;
    }

    public void setDone(Closure done) {
        this.done = done;
    }

    public RequestType getRequestType() {
        return requestType;
    }

    public void setRequestType(RequestType requestType) {
        this.requestType = requestType;
    }

    public BaseResponseCommand getResponse() {
        return response;
    }

    public void setResponse(BaseResponseCommand response) {
        this.response = response;
    }
}
