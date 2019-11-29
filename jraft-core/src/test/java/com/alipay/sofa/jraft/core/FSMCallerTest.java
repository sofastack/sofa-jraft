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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.CountDownLatch;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import com.alipay.sofa.jraft.Iterator;
import com.alipay.sofa.jraft.StateMachine;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.closure.ClosureQueueImpl;
import com.alipay.sofa.jraft.closure.LoadSnapshotClosure;
import com.alipay.sofa.jraft.closure.SaveSnapshotClosure;
import com.alipay.sofa.jraft.entity.EnumOutter.EntryType;
import com.alipay.sofa.jraft.entity.EnumOutter.ErrorType;
import com.alipay.sofa.jraft.entity.LeaderChangeContext;
import com.alipay.sofa.jraft.entity.LogEntry;
import com.alipay.sofa.jraft.entity.LogId;
import com.alipay.sofa.jraft.entity.RaftOutter.SnapshotMeta;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.error.RaftException;
import com.alipay.sofa.jraft.option.FSMCallerOptions;
import com.alipay.sofa.jraft.storage.LogManager;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotReader;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotWriter;
import com.alipay.sofa.jraft.test.TestUtils;

@RunWith(value = MockitoJUnitRunner.class)
public class FSMCallerTest {
    private FSMCallerImpl    fsmCaller;
    @Mock
    private NodeImpl         node;
    @Mock
    private StateMachine     fsm;
    @Mock
    private LogManager       logManager;
    private ClosureQueueImpl closureQueue;

    @Before
    public void setup() {
        this.fsmCaller = new FSMCallerImpl();
        this.closureQueue = new ClosureQueueImpl();
        final FSMCallerOptions opts = new FSMCallerOptions();
        Mockito.when(this.node.getNodeMetrics()).thenReturn(new NodeMetrics(false));
        opts.setNode(this.node);
        opts.setFsm(this.fsm);
        opts.setLogManager(this.logManager);
        opts.setBootstrapId(new LogId(10, 1));
        opts.setClosureQueue(this.closureQueue);
        assertTrue(this.fsmCaller.init(opts));
    }

    @After
    public void teardown() throws Exception {
        if (this.fsmCaller != null) {
            this.fsmCaller.shutdown();
            this.fsmCaller.join();
        }
    }

    @Test
    public void testShutdownJoin() throws Exception {
        this.fsmCaller.shutdown();
        this.fsmCaller.join();
        this.fsmCaller = null;
    }

    @Test
    public void testOnCommittedError() throws Exception {
        Mockito.when(this.logManager.getTerm(10)).thenReturn(1L);
        Mockito.when(this.logManager.getEntry(11)).thenReturn(null);

        assertTrue(this.fsmCaller.onCommitted(11));

        this.fsmCaller.flush();
        assertEquals(this.fsmCaller.getLastAppliedIndex(), 10);
        Mockito.verify(this.logManager).setAppliedId(new LogId(10, 1));
        assertFalse(this.fsmCaller.getError().getStatus().isOk());
        assertEquals("Fail to get entry at index=11 while committed_index=11", this.fsmCaller.getError().getStatus()
            .getErrorMsg());
    }

    @Test
    public void testOnCommitted() throws Exception {
        final LogEntry log = new LogEntry(EntryType.ENTRY_TYPE_DATA);
        log.getId().setIndex(11);
        log.getId().setTerm(1);
        Mockito.when(this.logManager.getTerm(11)).thenReturn(1L);
        Mockito.when(this.logManager.getEntry(11)).thenReturn(log);
        final ArgumentCaptor<Iterator> itArg = ArgumentCaptor.forClass(Iterator.class);

        assertTrue(this.fsmCaller.onCommitted(11));

        this.fsmCaller.flush();
        assertEquals(this.fsmCaller.getLastAppliedIndex(), 11);
        Mockito.verify(this.fsm).onApply(itArg.capture());
        final Iterator it = itArg.getValue();
        assertFalse(it.hasNext());
        assertEquals(it.getIndex(), 12);
        Mockito.verify(this.logManager).setAppliedId(new LogId(11, 1));
        assertTrue(this.fsmCaller.getError().getStatus().isOk());
    }

    @Test
    public void testOnSnapshotLoad() throws Exception {
        final SnapshotReader reader = Mockito.mock(SnapshotReader.class);

        final SnapshotMeta meta = SnapshotMeta.newBuilder().setLastIncludedIndex(12).setLastIncludedTerm(1).build();
        Mockito.when(reader.load()).thenReturn(meta);
        Mockito.when(this.fsm.onSnapshotLoad(reader)).thenReturn(true);
        final CountDownLatch latch = new CountDownLatch(1);
        this.fsmCaller.onSnapshotLoad(new LoadSnapshotClosure() {

            @Override
            public void run(final Status status) {
                assertTrue(status.isOk());
                latch.countDown();
            }

            @Override
            public SnapshotReader start() {
                return reader;
            }
        });
        latch.await();
        assertEquals(this.fsmCaller.getLastAppliedIndex(), 12);
        Mockito.verify(this.fsm).onConfigurationCommitted(Mockito.any());
    }

    @Test
    public void testOnSnapshotLoadFSMError() throws Exception {
        final SnapshotReader reader = Mockito.mock(SnapshotReader.class);

        final SnapshotMeta meta = SnapshotMeta.newBuilder().setLastIncludedIndex(12).setLastIncludedTerm(1).build();
        Mockito.when(reader.load()).thenReturn(meta);
        Mockito.when(this.fsm.onSnapshotLoad(reader)).thenReturn(false);
        final CountDownLatch latch = new CountDownLatch(1);
        this.fsmCaller.onSnapshotLoad(new LoadSnapshotClosure() {

            @Override
            public void run(final Status status) {
                assertFalse(status.isOk());
                assertEquals(-1, status.getCode());
                assertEquals("StateMachine onSnapshotLoad failed", status.getErrorMsg());
                latch.countDown();
            }

            @Override
            public SnapshotReader start() {
                return reader;
            }
        });
        latch.await();
        assertEquals(this.fsmCaller.getLastAppliedIndex(), 10);
    }

    @Test
    public void testOnSnapshotSaveEmptyConf() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);
        this.fsmCaller.onSnapshotSave(new SaveSnapshotClosure() {

            @Override
            public void run(final Status status) {
                assertFalse(status.isOk());
                assertEquals("Empty conf entry for lastAppliedIndex=10", status.getErrorMsg());
                latch.countDown();
            }

            @Override
            public SnapshotWriter start(final SnapshotMeta meta) {
                // TODO Auto-generated method stub
                return null;
            }
        });
        latch.await();
    }

    @Test
    public void testOnSnapshotSave() throws Exception {
        final SnapshotWriter writer = Mockito.mock(SnapshotWriter.class);
        Mockito.when(this.logManager.getConfiguration(10)).thenReturn(
            TestUtils.getConfEntry("localhost:8081,localhost:8082,localhost:8083", "localhost:8081"));
        final SaveSnapshotClosure done = new SaveSnapshotClosure() {

            @Override
            public void run(final Status status) {

            }

            @Override
            public SnapshotWriter start(final SnapshotMeta meta) {
                assertEquals(10, meta.getLastIncludedIndex());
                return writer;
            }
        };
        this.fsmCaller.onSnapshotSave(done);
        this.fsmCaller.flush();
        Mockito.verify(this.fsm).onSnapshotSave(writer, done);
    }

    @Test
    public void testOnLeaderStartStop() throws Exception {
        this.fsmCaller.onLeaderStart(11);
        this.fsmCaller.flush();
        Mockito.verify(this.fsm).onLeaderStart(11);

        final Status status = new Status(-1, "test");
        this.fsmCaller.onLeaderStop(status);
        this.fsmCaller.flush();
        Mockito.verify(this.fsm).onLeaderStop(status);
    }

    @Test
    public void testOnStartStopFollowing() throws Exception {
        final LeaderChangeContext ctx = new LeaderChangeContext(null, 11, Status.OK());
        this.fsmCaller.onStartFollowing(ctx);
        this.fsmCaller.flush();
        Mockito.verify(this.fsm).onStartFollowing(ctx);

        this.fsmCaller.onStopFollowing(ctx);
        this.fsmCaller.flush();
        Mockito.verify(this.fsm).onStopFollowing(ctx);
    }

    @Test
    public void testOnError() throws Exception {
        this.fsmCaller.onError(new RaftException(ErrorType.ERROR_TYPE_LOG, new Status(-1, "test")));
        this.fsmCaller.flush();
        assertFalse(this.fsmCaller.getError().getStatus().isOk());
        assertEquals(ErrorType.ERROR_TYPE_LOG, this.fsmCaller.getError().getType());
        Mockito.verify(this.node).onError(Mockito.any());
        Mockito.verify(this.fsm).onError(Mockito.any());
    }

    @Test
    public void testOnSnapshotLoadStale() throws Exception {
        final SnapshotReader reader = Mockito.mock(SnapshotReader.class);

        final SnapshotMeta meta = SnapshotMeta.newBuilder().setLastIncludedIndex(5).setLastIncludedTerm(1).build();
        Mockito.when(reader.load()).thenReturn(meta);

        final CountDownLatch latch = new CountDownLatch(1);
        this.fsmCaller.onSnapshotLoad(new LoadSnapshotClosure() {

            @Override
            public void run(final Status status) {
                assertFalse(status.isOk());
                assertEquals(RaftError.ESTALE, status.getRaftError());
                latch.countDown();
            }

            @Override
            public SnapshotReader start() {
                return reader;
            }
        });
        latch.await();
        assertEquals(this.fsmCaller.getLastAppliedIndex(), 10);
    }

}
