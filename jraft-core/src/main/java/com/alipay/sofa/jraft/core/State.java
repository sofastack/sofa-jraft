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
package com.alipay.sofa.jraft.core;

/**
 * Node state
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-08 5:41:54 PM
 */
public enum State {
    /** The node is a leader */
    STATE_LEADER,

    /** The node is transferring leadership */
    STATE_TRANSFERRING,

    /** The node is a candidate */
    STATE_CANDIDATE,

    /** The node is a follower */
    STATE_FOLLOWER,

    /** The node is in error */
    STATE_ERROR,

    /** The node is uninitialized */
    STATE_UNINITIALIZED,

    /** The node is shutting down */
    STATE_SHUTTING,

    /** The node is shut down */
    STATE_SHUTDOWN,

    /** The node is ending */
    STATE_END;

    public boolean isActive() {
        return this.ordinal() < STATE_ERROR.ordinal();
    }
}
