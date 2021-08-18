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
package com.alipay.sofa.jraft.rhea.fsm.pipe;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Iterator;
import com.alipay.sofa.jraft.StateMachine;
import com.alipay.sofa.jraft.core.IteratorImpl;
import com.alipay.sofa.jraft.core.IteratorWrapper;
import com.alipay.sofa.jraft.entity.EnumOutter;
import com.alipay.sofa.jraft.entity.LogEntry;
import com.alipay.sofa.jraft.rhea.storage.KVClosureAdapter;
import com.alipay.sofa.jraft.rhea.storage.KVOperation;
import com.alipay.sofa.jraft.rhea.storage.KVState;
import com.alipay.sofa.jraft.rhea.storage.TestClosure;
import com.alipay.sofa.jraft.storage.LogManager;
import com.alipay.sofa.jraft.util.BytesUtil;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

@RunWith(value = MockitoJUnitRunner.class)
public class PipeBaseTest {

    private IteratorImpl  iterImpl;
    protected Iterator    iter;

    @Mock
    private StateMachine  fsm;
    @Mock
    private LogManager    logManager;

    private List<Closure> closures;
    private AtomicLong    applyingIndex;

    public void setup() {
        this.applyingIndex = new AtomicLong(0);
        this.closures = new ArrayList<>();
        for (int i = 0; i <= 10; i++) {
            final KVClosureAdapter kvClosureAdapter = this.createPutOperation("key-" + i);
            this.closures.add(kvClosureAdapter);
            final LogEntry log = new LogEntry(EnumOutter.EntryType.ENTRY_TYPE_DATA);
            log.getId().setIndex(i);
            log.getId().setTerm(1);
            log.setData(null);
            Mockito.when(this.logManager.getEntry(i)).thenReturn(log);
        }
        this.iterImpl = new IteratorImpl(fsm, logManager, closures, 0L, 0L, 20L, applyingIndex);
        this.iter = new IteratorWrapper(iterImpl);
    }

    public KVClosureAdapter createPutOperation(final String cur) {
        final KVOperation op = KVOperation.createPut(BytesUtil.writeUtf8(cur), BytesUtil.writeUtf8(cur));
        final KVClosureAdapter adapter = new KVClosureAdapter(new TestClosure(), op);
        return adapter;
    }

    public KVState mockKVState(final String key) {
        final KVOperation op = KVOperation.createPut(BytesUtil.writeUtf8(key), BytesUtil.writeUtf8(key));
        final KVClosureAdapter adapter = new KVClosureAdapter(new TestClosure(), op);
        return KVState.of(op, adapter);
    }

}
