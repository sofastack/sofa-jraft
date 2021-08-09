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
package com.alipay.sofa.jraft.rhea.fsm.pipeline.KvPipe;

import com.alipay.sofa.jraft.rhea.fsm.pipeline.PipeBaseTest;
import com.alipay.sofa.jraft.rhea.storage.KVState;
import com.alipay.sofa.jraft.util.BytesUtil;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * @author hzh (642256541@qq.com)
 */
public class ReadKVOperationPipeTest extends PipeBaseTest {

    private ReadKVOperationPipe readPipe;
    private RecyclableKvTask    task;

    @Before
    public void init() {
        this.setup();
        this.readPipe = new ReadKVOperationPipe();
    }

    @Test
    public void testDoProcess() {
        int index = 0;
        this.task = readPipe.doProcess(this.iter);
        final List<KVState> kvStateList = this.task.getKvStateList();
        {
            assertEquals(kvStateList.size(), 10);
            for (final KVState kvState : kvStateList) {
                ++index;
                final String key = BytesUtil.readUtf8(kvState.getOp().getKey());
                assertEquals(key, "key-" + index);
            }
        }
    }
}