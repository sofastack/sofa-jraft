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
package com.alipay.sofa.jraft.example.counter;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.example.counter.rpc.IncrementAndGetRequest;
import com.alipay.sofa.jraft.example.counter.rpc.ValueResponse;

/**
 * RPC request closure
 *
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-09 5:01:25 PM
 */
public class IncrementAndAddClosure implements Closure {

    @SuppressWarnings({ "FieldCanBeLocal", "unused" })
    private CounterServer          counterServer;
    private IncrementAndGetRequest request;
    private ValueResponse          response;
    private Closure                done;

    public IncrementAndAddClosure(CounterServer counterServer, IncrementAndGetRequest request, ValueResponse response,
                                  Closure done) {
        super();
        this.counterServer = counterServer;
        this.request = request;
        this.response = response;
        this.done = done;
    }

    @Override
    public void run(Status status) {
        if (this.done != null) {
            done.run(status);
        }
    }

    public IncrementAndGetRequest getRequest() {
        return this.request;
    }

    public void setRequest(IncrementAndGetRequest request) {
        this.request = request;
    }

    public ValueResponse getResponse() {
        return this.response;
    }

}
