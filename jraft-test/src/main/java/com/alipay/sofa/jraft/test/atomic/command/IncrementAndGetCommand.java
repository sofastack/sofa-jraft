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
 * Increment with detal and get the latest value.
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-25 1:28:42 PM
 */
public class IncrementAndGetCommand extends BaseRequestCommand implements Serializable {
    private static final long serialVersionUID = -1232443841104358771L;
    private long              detal;

    public long getDetal() {
        return this.detal;
    }

    public void setDetal(long detal) {
        this.detal = detal;
    }

}
