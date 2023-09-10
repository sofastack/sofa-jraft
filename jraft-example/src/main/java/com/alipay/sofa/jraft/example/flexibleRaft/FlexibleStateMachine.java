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
package com.alipay.sofa.jraft.example.flexibleRaft;

import com.alipay.remoting.exception.CodecException;
import com.alipay.remoting.serialization.SerializerManager;
import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Iterator;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.core.StateMachineAdapter;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.error.RaftException;
import com.alipay.sofa.jraft.example.counter.CounterOperation;
import com.alipay.sofa.jraft.example.flexibleRaft.snapshot.FlexibleSnapshotFile;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotReader;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotWriter;
import com.alipay.sofa.jraft.util.NamedThreadFactory;
import com.alipay.sofa.jraft.util.ThreadPoolUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;

import static com.alipay.sofa.jraft.example.counter.CounterOperation.GET;
import static com.alipay.sofa.jraft.example.counter.CounterOperation.INCREMENT;

/**
 * @author Akai
 */
public class FlexibleStateMachine extends StateMachineAdapter {
    private static final Logger       LOG        = LoggerFactory.getLogger(FlexibleStateMachine.class);
    private static ThreadPoolExecutor executor   = ThreadPoolUtil
                                                     .newBuilder()
                                                     .poolName("JRAFT_TEST_EXECUTOR")
                                                     .enableMetric(true)
                                                     .coreThreads(3)
                                                     .maximumThreads(5)
                                                     .keepAliveSeconds(60L)
                                                     .workQueue(new SynchronousQueue<>())
                                                     .threadFactory(
                                                         new NamedThreadFactory("JRaft-Test-Executor-", true)).build();
    /**
     * Counter value
     */
    private final AtomicLong          value      = new AtomicLong(0);
    /**
     * Leader term
     */
    private final AtomicLong          leaderTerm = new AtomicLong(-1);

    public boolean isLeader() {
        return this.leaderTerm.get() > 0;
    }

    /**
     * Returns current value.
     */
    public long getValue() {
        return this.value.get();
    }

    @Override
    public void onApply(final Iterator iter) {
        while (iter.hasNext()) {
            long current = 0;
            Operation operation = null;

            FlexibleRaftClosure closure = null;
            if (iter.done() != null) {
                // This task is applied by this node, get value from closure to avoid additional parsing.
                closure = (FlexibleRaftClosure) iter.done();
                operation = closure.getFlexibleOperation();
            } else {
                // Have to parse FetchAddRequest from this user log.
                final ByteBuffer data = iter.getData();
                try {
                    operation = SerializerManager.getSerializer(SerializerManager.Hessian2).deserialize(data.array(),
                        CounterOperation.class.getName());
                } catch (final CodecException e) {
                    LOG.error("Fail to decode IncrementAndGetRequest", e);
                }
                // follower ignore read operation
                if (operation != null && operation.isReadOp()) {
                    iter.next();
                    continue;
                }
            }
            if (operation != null) {
                switch (operation.getOp()) {
                    case GET:
                        current = this.value.get();
                        LOG.info("Get value={} at logIndex={}", current, iter.getIndex());
                        break;
                    case INCREMENT:
                        final long delta = operation.getDelta();
                        final long prev = this.value.get();
                        current = this.value.addAndGet(delta);
                        LOG.info("Added value={} by delta={} at logIndex={}", prev, delta, iter.getIndex());
                        break;
                }

                if (closure != null) {
                    closure.success(current);
                    closure.run(Status.OK());
                }
            }
            iter.next();
        }
    }

    @Override
    public void onSnapshotSave(final SnapshotWriter writer, final Closure done) {
        final long currVal = this.value.get();
        executor.submit(() -> {
            final FlexibleSnapshotFile snapshot = new FlexibleSnapshotFile(writer.getPath() + File.separator + "data");
            if (snapshot.save(currVal)) {
                if (writer.addFile("data")) {
                    done.run(Status.OK());
                } else {
                    done.run(new Status(RaftError.EIO, "Fail to add file to writer"));
                }
            } else {
                done.run(new Status(RaftError.EIO, "Fail to save counter snapshot %s", snapshot.getPath()));
            }
        });
    }

    @Override
    public void onError(final RaftException e) {
        LOG.error("Raft error: {}", e, e);
    }

    @Override
    public boolean onSnapshotLoad(final SnapshotReader reader) {
        if (isLeader()) {
            LOG.warn("Leader is not supposed to load snapshot");
            return false;
        }
        if (reader.getFileMeta("data") == null) {
            LOG.error("Fail to find data file in {}", reader.getPath());
            return false;
        }
        final FlexibleSnapshotFile snapshot = new FlexibleSnapshotFile(reader.getPath() + File.separator + "data");
        try {
            this.value.set(snapshot.load());
            return true;
        } catch (final IOException e) {
            LOG.error("Fail to load snapshot from {}", snapshot.getPath());
            return false;
        }

    }

    @Override
    public void onLeaderStart(final long term) {
        this.leaderTerm.set(term);
        super.onLeaderStart(term);

    }

    @Override
    public void onLeaderStop(final Status status) {
        this.leaderTerm.set(-1);
        super.onLeaderStop(status);
    }
}
