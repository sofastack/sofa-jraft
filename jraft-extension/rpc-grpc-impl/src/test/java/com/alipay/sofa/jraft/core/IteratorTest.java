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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Iterator;
import com.alipay.sofa.jraft.StateMachine;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.entity.EnumOutter;
import com.alipay.sofa.jraft.entity.LogEntry;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.storage.LogManager;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(value = MockitoJUnitRunner.class)
public class IteratorTest {

    private IteratorImpl  iterImpl;
    private Iterator      iter;

    @Mock
    private StateMachine  fsm;
    @Mock
    private LogManager    logManager;
    private List<Closure> closures;
    private AtomicLong    applyingIndex;

    @Before
    public void setup() {
        this.applyingIndex = new AtomicLong(0);
        this.closures = new ArrayList<>();
        for (int i = 0; i < 11; i++) {
            this.closures.add(new MockClosure());
            final LogEntry log = new LogEntry(EnumOutter.EntryType.ENTRY_TYPE_DATA);
            log.getId().setIndex(i);
            log.getId().setTerm(1);
            log.setData(ByteBuffer.allocate(i));
            Mockito.when(this.logManager.getEntry(i)).thenReturn(log);
        }
        this.iterImpl = new IteratorImpl(fsm, logManager, closures, 0L, 0L, 10L, applyingIndex);
        this.iter = new IteratorWrapper(iterImpl);
    }

    @Test
    public void testPredicates() {
        assertTrue(this.iter.hasNext());
    }

    @Test
    public void testNext() {
        int i = 1;
        while (iter.hasNext()) {
            assertEquals(i, iter.getIndex());
            assertNotNull(iter.done());
            assertEquals(i, iter.getIndex());
            assertEquals(1, iter.getTerm());
            assertEquals(i, iter.getData().remaining());
            iter.next();
            i++;
        }
        assertEquals(i, 11);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetErrorAndRollbackInvalid() {
        this.iter.setErrorAndRollback(-1, null);
    }

    @Test
    public void testSetErrorAndRollback() {
        testNext();
        assertFalse(iterImpl.hasError());
        this.iter.setErrorAndRollback(5, new Status(-1, "test"));
        assertTrue(iterImpl.hasError());
        Assert.assertEquals(EnumOutter.ErrorType.ERROR_TYPE_STATE_MACHINE, iterImpl.getError().getType());
        Assert.assertEquals(RaftError.ESTATEMACHINE.getNumber(), iterImpl.getError().getStatus().getCode());
        Assert
            .assertEquals(
                "StateMachine meet critical error when applying one or more tasks since index=6, Status[UNKNOWN<-1>: test]",
                iterImpl.getError().getStatus().getErrorMsg());
        assertEquals(6, iter.getIndex());
    }
}
