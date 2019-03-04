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
package com.alipay.sofa.jraft.option;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.StateMachine;
import com.alipay.sofa.jraft.closure.ClosureQueue;
import com.alipay.sofa.jraft.core.NodeImpl;
import com.alipay.sofa.jraft.entity.LogId;
import com.alipay.sofa.jraft.storage.LogManager;

/**
 * FSM caller options.
 *
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-04 2:59:02 PM
 */
public class FSMCallerOptions {
    private LogManager   logManager;
    private StateMachine fsm;
    private Closure      afterShutdown;
    private LogId        bootstrapId;
    private ClosureQueue closureQueue;
    private NodeImpl     node;
    /**
     * disruptor buffer size.
     */
    private int          disruptorBufferSize = 1024;

    public int getDisruptorBufferSize() {
        return this.disruptorBufferSize;
    }

    public void setDisruptorBufferSize(int disruptorBufferSize) {
        this.disruptorBufferSize = disruptorBufferSize;
    }

    public NodeImpl getNode() {
        return this.node;
    }

    public void setNode(NodeImpl node) {
        this.node = node;
    }

    public ClosureQueue getClosureQueue() {
        return this.closureQueue;
    }

    public void setClosureQueue(ClosureQueue closureQueue) {
        this.closureQueue = closureQueue;
    }

    public LogManager getLogManager() {
        return this.logManager;
    }

    public void setLogManager(LogManager logManager) {
        this.logManager = logManager;
    }

    public StateMachine getFsm() {
        return this.fsm;
    }

    public void setFsm(StateMachine fsm) {
        this.fsm = fsm;
    }

    public Closure getAfterShutdown() {
        return this.afterShutdown;
    }

    public void setAfterShutdown(Closure afterShutdown) {
        this.afterShutdown = afterShutdown;
    }

    public LogId getBootstrapId() {
        return this.bootstrapId;
    }

    public void setBootstrapId(LogId bootstrapId) {
        this.bootstrapId = bootstrapId;
    }
}
