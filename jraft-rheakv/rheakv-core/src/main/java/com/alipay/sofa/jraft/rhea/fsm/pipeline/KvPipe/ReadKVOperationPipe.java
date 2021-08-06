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

import com.alipay.sofa.jraft.Iterator;
import com.alipay.sofa.jraft.rhea.fsm.pipeline.AbstractPipe;
import com.alipay.sofa.jraft.rhea.fsm.pipeline.PipeException;
import com.alipay.sofa.jraft.rhea.serialization.Serializer;
import com.alipay.sofa.jraft.rhea.serialization.Serializers;
import com.alipay.sofa.jraft.rhea.storage.KVClosureAdapter;
import com.alipay.sofa.jraft.rhea.storage.KVOperation;
import com.alipay.sofa.jraft.rhea.storage.KVState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Serialize KVOperation from jraft iterator and send batch KVState to next pipe
 * @author hzh (642256541@qq.com)
 */
public class ReadKVOperationPipe extends AbstractPipe<Iterator, List<KVState>> {
    private static final Logger LOG        = LoggerFactory.getLogger(ReadKVOperationPipe.class);

    private final Serializer    serializer = Serializers.getDefault();
    private final int           batchSize  = 10;

    @Override
    public List<KVState> doProcess(final Iterator it) {
        int cnt = 0;
        final List<KVState> kvStateList = new ArrayList<>();
        while (it.hasNext() && cnt < batchSize) {
            KVOperation kvOp = null;
            final KVClosureAdapter done = (KVClosureAdapter) it.done();
            if (done != null) {
                kvOp = done.getOperation();
            } else {
                final ByteBuffer buf = it.getData();
                try {
                    if (buf.hasArray()) {
                        kvOp = this.serializer.readObject(buf.array(), KVOperation.class);
                    } else {
                        kvOp = this.serializer.readObject(buf, KVOperation.class);
                    }
                } catch (final Throwable t) {
                    LOG.error("Error on serialize kvOp on index : {}, term: {}", it.getIndex(), it.getTerm());
                }
            }
            if (kvOp != null) {
                kvStateList.add(KVState.of(kvOp, done));
                cnt++;
            }
            it.next();
        }
        return kvStateList;
    }

}
