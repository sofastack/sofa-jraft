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
 * Get value command
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-25 1:28:37 PM
 */
public class GetCommand extends BaseRequestCommand implements Serializable {

    /**
     * 
     */
    private static final long serialVersionUID   = -512223854448447263L;

    //Whether to use ReadIndex
    private boolean           readFromQuorum     = false;

    private boolean           readByStateMachine = false;

    public boolean isReadByStateMachine() {
        return readByStateMachine;
    }

    public void setReadByStateMachine(boolean readByStateMachine) {
        this.readByStateMachine = readByStateMachine;
    }

    public boolean isReadFromQuorum() {
        return readFromQuorum;
    }

    public void setReadFromQuorum(boolean readFromQuorum) {
        this.readFromQuorum = readFromQuorum;
    }

    public GetCommand() {

    }

    public GetCommand(String key) {
        setKey(key);
    }
}
