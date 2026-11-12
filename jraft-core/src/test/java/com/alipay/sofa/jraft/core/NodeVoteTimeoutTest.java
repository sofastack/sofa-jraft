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

import java.io.File;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.alipay.sofa.jraft.NodeManager;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.option.NodeOptions;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotExecutorImpl;
import com.alipay.sofa.jraft.test.TestUtils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class NodeVoteTimeoutTest {

    private NodeImpl node;
    private PeerId   peer;
    private String   dataPath;

    @Before
    public void setup() throws Exception {
        this.dataPath = TestUtils.mkTempDir();
        FileUtils.forceMkdir(new File(this.dataPath));
        this.peer = new PeerId(TestUtils.getMyIp(), TestUtils.INIT_PORT);
        NodeManager.getInstance().addAddress(this.peer.getEndpoint());

        final NodeOptions options = new NodeOptions();
        options.setFsm(new MockStateMachine(this.peer.getEndpoint()));
        options.setLogUri(this.dataPath + File.separator + "log");
        options.setRaftMetaUri(this.dataPath + File.separator + "meta");
        options.setSnapshotUri(this.dataPath + File.separator + "snapshot");
        options.setSnapshotIntervalSecs(0);
        options.setElectionTimeoutMs(600000);
        options.setInitialConf(new Configuration(Arrays.asList(this.peer, new PeerId(TestUtils.getMyIp(),
            TestUtils.INIT_PORT + 1))));
        this.node = new NodeImpl("vote-timeout-test", this.peer);
        assertTrue(this.node.init(options));
        ((com.alipay.sofa.jraft.util.RepeatedTimer) field("electionTimer").get(this.node)).stop();
    }

    @After
    public void teardown() throws Exception {
        try {
            if (this.node != null) {
                this.node.shutdown();
                this.node.join();
            }
        } finally {
            if (this.peer != null) {
                NodeManager.getInstance().removeAddress(this.peer.getEndpoint());
            }
            FileUtils.deleteDirectory(new File(this.dataPath));
        }
    }

    @Test
    public void testNotCandidateReleasesLock() throws Exception {
        assertNull(handleVoteTimeout());
        assertLocksAvailable();
    }

    @Test
    public void testStepDownReleasesLock() throws Exception {
        becomeCandidate();
        assertNull(handleVoteTimeout());
        assertLocksAvailable();
        assertEquals(State.STATE_FOLLOWER, field("state").get(this.node));
    }

    @Test
    public void testRetryVoteReleasesLock() throws Exception {
        becomeCandidate();
        this.node.getRaftOptions().setStepDownWhenVoteTimedout(false);
        final long term = this.node.getCurrentTerm();
        assertNull(handleVoteTimeout());
        assertLocksAvailable();
        assertEquals(term + 1, this.node.getCurrentTerm());
        assertEquals(State.STATE_CANDIDATE, field("state").get(this.node));
    }

    @Test
    public void testStepDownFailureReleasesLock() throws Exception {
        becomeCandidate();
        final Field termField = SnapshotExecutorImpl.class.getDeclaredField("term");
        termField.setAccessible(true);
        final SnapshotExecutorImpl snapshot = (SnapshotExecutorImpl) field("snapshotExecutor").get(this.node);
        final long term = this.node.getCurrentTerm();
        termField.set(snapshot, term + 1);
        try {
            assertTrue(handleVoteTimeout() instanceof IllegalArgumentException);
            assertLocksAvailable();
        } finally {
            termField.set(snapshot, term);
        }
    }

    private void becomeCandidate() throws Exception {
        this.node.tryElectSelf();
        assertEquals(State.STATE_CANDIDATE, field("state").get(this.node));
    }

    private Throwable handleVoteTimeout() throws Exception {
        final Method method = NodeImpl.class.getDeclaredMethod("handleVoteTimeout");
        method.setAccessible(true);
        try {
            method.invoke(this.node);
            return null;
        } catch (final InvocationTargetException e) {
            return e.getCause();
        }
    }

    private void assertLocksAvailable() throws InterruptedException {
        assertLockAvailable(this.node.readLock);
        assertLockAvailable(this.node.writeLock);
    }

    private void assertLockAvailable(final Lock lock) throws InterruptedException {
        final boolean acquired = lock.tryLock(200, TimeUnit.MILLISECONDS);
        try {
            assertTrue(acquired);
        } finally {
            if (acquired) {
                lock.unlock();
            }
        }
    }

    private static Field field(final String name) throws Exception {
        final Field field = NodeImpl.class.getDeclaredField(name);
        field.setAccessible(true);
        return field;
    }
}
