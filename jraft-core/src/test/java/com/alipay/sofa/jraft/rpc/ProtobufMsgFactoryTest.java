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
package com.alipay.sofa.jraft.rpc;

import org.junit.Test;

import com.alipay.sofa.jraft.entity.RaftOutter.SnapshotMeta;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;

public class ProtobufMsgFactoryTest {
    static {
        ProtobufMsgFactory.load();
    }

    @Test
    public void testNewMessageByJavaClassName() {
        SnapshotMeta meta = SnapshotMeta.newBuilder().setLastIncludedIndex(99).setLastIncludedTerm(1).build();
        SnapshotMeta pMeta = ProtobufMsgFactory
            .newMessageByJavaClassName(meta.getClass().getName(), meta.toByteArray());
        assertNotNull(pMeta);
        assertNotSame(pMeta, meta);
        assertEquals(pMeta, meta);
    }

    @Test
    public void testNewMessage() {
        SnapshotMeta meta = SnapshotMeta.newBuilder().setLastIncludedIndex(99).setLastIncludedTerm(1).build();
        SnapshotMeta pMeta = ProtobufMsgFactory.newMessageByProtoClassName("jraft.SnapshotMeta", meta.toByteArray());
        assertNotNull(pMeta);
        assertNotSame(pMeta, meta);
        assertEquals(pMeta, meta);
    }
}
