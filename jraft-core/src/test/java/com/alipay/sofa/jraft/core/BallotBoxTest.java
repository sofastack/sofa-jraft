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

import com.alipay.sofa.jraft.Quorum;
import com.alipay.sofa.jraft.entity.BallotFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.FSMCaller;
import com.alipay.sofa.jraft.JRaftUtils;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.closure.ClosureQueueImpl;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.option.BallotBoxOptions;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(value = MockitoJUnitRunner.class)
public class BallotBoxTest {
    private static final String GROUP_ID = "group001";
    private BallotBox           box;
    @Mock
    private FSMCaller           waiter;
    private ClosureQueueImpl    closureQueue;

    @Before
    public void setup() {
        BallotBoxOptions opts = new BallotBoxOptions();
        this.closureQueue = new ClosureQueueImpl(GROUP_ID);
        opts.setClosureQueue(this.closureQueue);
        opts.setWaiter(this.waiter);
        box = new BallotBox();
        assertTrue(box.init(opts));
    }

    @After
    public void teardown() {
        box.shutdown();
    }

    @Test
    public void testResetPendingIndex() {
        assertEquals(0, closureQueue.getFirstIndex());
        assertEquals(0, box.getPendingIndex());
        assertTrue(box.resetPendingIndex(1));
        assertEquals(1, closureQueue.getFirstIndex());
        assertEquals(1, box.getPendingIndex());
    }

    @Test
    public void testAppendPendingTask() {
        assertTrue(this.box.getPendingMetaQueue().isEmpty());
        assertTrue(this.closureQueue.getQueue().isEmpty());
        Quorum quorum = BallotFactory.buildMajorityQuorum(3);
        Quorum oldQuorum = BallotFactory.buildMajorityQuorum(1);
        assertFalse(this.box.appendPendingTask(
            JRaftUtils.getConfiguration("localhost:8081,localhost:8082,localhost:8083"),
            JRaftUtils.getConfiguration("localhost:8081"), quorum, oldQuorum, new Closure() {
                @Override
                public void run(Status status) {

                }
            }));
        assertTrue(box.resetPendingIndex(1));
        assertTrue(this.box.appendPendingTask(
            JRaftUtils.getConfiguration("localhost:8081,localhost:8082,localhost:8083"),
            JRaftUtils.getConfiguration("localhost:8081"), quorum, oldQuorum, new Closure() {
                @Override
                public void run(Status status) {

                }
            }));

        assertEquals(1, this.box.getPendingMetaQueue().size());
        assertEquals(1, this.closureQueue.getQueue().size());
    }

    @Test
    public void testClearPendingTasks() {
        testAppendPendingTask();
        this.box.clearPendingTasks();
        assertTrue(this.box.getPendingMetaQueue().isEmpty());
        assertTrue(this.closureQueue.getQueue().isEmpty());
        assertEquals(0, closureQueue.getFirstIndex());
    }

    @Test
    public void testCommitAt() {
        assertFalse(this.box.commitAt(1, 3, new PeerId("localhost", 8081)));
        assertTrue(box.resetPendingIndex(1));
        Quorum quorum = BallotFactory.buildMajorityQuorum(3);
        Quorum oldQuorum = BallotFactory.buildMajorityQuorum(1);
        assertTrue(this.box.appendPendingTask(
            JRaftUtils.getConfiguration("localhost:8081,localhost:8082,localhost:8083"),
            JRaftUtils.getConfiguration("localhost:8081"), quorum, oldQuorum, new Closure() {

                @Override
                public void run(Status status) {

                }
            }));
        assertEquals(0, this.box.getLastCommittedIndex());
        try {
            this.box.commitAt(1, 3, new PeerId("localhost", 8081));
            fail();
        } catch (ArrayIndexOutOfBoundsException e) {

        }
        assertTrue(this.box.commitAt(1, 1, new PeerId("localhost", 8081)));
        assertEquals(0, this.box.getLastCommittedIndex());
        assertEquals(1, this.box.getPendingIndex());
        assertTrue(this.box.commitAt(1, 1, new PeerId("localhost", 8082)));
        assertEquals(1, this.box.getLastCommittedIndex());
        assertEquals(2, this.box.getPendingIndex());
        Mockito.verify(this.waiter, Mockito.only()).onCommitted(1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetLastCommittedIndexHasPending() {
        assertTrue(box.resetPendingIndex(1));
        assertFalse(this.box.setLastCommittedIndex(1));
    }

    @Test
    public void testSetLastCommittedIndexLessThan() {
        assertFalse(this.box.setLastCommittedIndex(-1));
    }

    @Test
    public void testSetLastCommittedIndex() {
        Mockito.when(this.waiter.hasAvailableCapacity(1)).thenReturn(true);
        assertEquals(0, this.box.getLastCommittedIndex());
        assertTrue(this.box.setLastCommittedIndex(1));
        assertEquals(1, this.box.getLastCommittedIndex());
        Mockito.verify(this.waiter, Mockito.times(1)).onCommitted(1);
    }
}
