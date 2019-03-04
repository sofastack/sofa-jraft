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
package com.alipay.sofa.jraft.rhea.storage;

import java.io.Serializable;

import com.alipay.sofa.jraft.Status;

/**
 * This is a callback interface, {@link NodeExecutor#execute(Status, boolean)}
 * will be triggered when each node's state machine is applied.
 *
 * Note that any element contained in the implementation of this interface must
 * implement the {@link Serializable} interface.
 *
 * @author jiachun.fjc
 */
public interface NodeExecutor extends Serializable {

    /**
     * This callback method will be triggered when each node's state machine
     * is applied.
     *
     * @param status   The execution state of the current node
     * @param isLeader Whether the current node is a leader
     */
    void execute(Status status, boolean isLeader);
}
