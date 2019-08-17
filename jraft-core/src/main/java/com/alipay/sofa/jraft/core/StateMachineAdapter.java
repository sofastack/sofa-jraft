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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.StateMachine;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.entity.LeaderChangeContext;
import com.alipay.sofa.jraft.error.RaftException;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotReader;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotWriter;

/**
 * State machine adapter that implements all methods with default behavior
 * except {@link #onApply(com.alipay.sofa.jraft.Iterator)}.
 *
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Mar-30 10:32:27 AM
 */
public abstract class StateMachineAdapter implements StateMachine {

    private static final Logger LOG = LoggerFactory.getLogger(StateMachineAdapter.class);

    @Override
    public void onShutdown() {
        LOG.info("onShutdown.");
    }

    @Override
    public void onSnapshotSave(final SnapshotWriter writer, final Closure done) {
        error("onSnapshotSave");
        runClosure(done, "onSnapshotSave");
    }

    @Override
    public boolean onSnapshotLoad(final SnapshotReader reader) {
        error("onSnapshotLoad", "while a snapshot is saved in " + reader.getPath());
        return false;
    }

    @Override
    public void onLeaderStart(final long term) {
        LOG.info("onLeaderStart: term={}.", term);
    }

    @Override
    public void onLeaderStop(final Status status) {
        LOG.info("onLeaderStop: status={}.", status);
    }

    @Override
    public void onError(final RaftException e) {
        LOG.error(
            "Encountered an error={} on StateMachine {}, it's highly recommended to implement this method as raft stops working since some error occurs, you should figure out the cause and repair or remove this node.",
            e.getStatus(), getClassName(), e);
    }

    @Override
    public void onConfigurationCommitted(final Configuration conf) {
        LOG.info("onConfigurationCommitted: {}.", conf);
    }

    @Override
    public void onStopFollowing(final LeaderChangeContext ctx) {
        LOG.info("onStopFollowing: {}.", ctx);
    }

    @Override
    public void onStartFollowing(final LeaderChangeContext ctx) {
        LOG.info("onStartFollowing: {}.", ctx);
    }

    @SuppressWarnings("SameParameterValue")
    private void runClosure(final Closure done, final String methodName) {
        done.run(new Status(-1, "%s doesn't implement %s", getClassName(), methodName));
    }

    private String getClassName() {
        return getClass().getName();
    }

    @SuppressWarnings("SameParameterValue")
    private void error(final String methodName) {
        error(methodName, "");
    }

    private void error(final String methodName, final String msg) {
        LOG.error("{} doesn't implement {} {}.", getClassName(), methodName, msg);
    }
}
