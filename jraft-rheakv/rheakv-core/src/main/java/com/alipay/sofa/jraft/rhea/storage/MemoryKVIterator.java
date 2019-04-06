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
package com.alipay.sofa.jraft.rhea.storage;

import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;

/**
 * @author jiachun.fjc
 */
public class MemoryKVIterator implements KVIterator {

    private final ConcurrentNavigableMap<byte[], byte[]> db;

    private Map.Entry<byte[], byte[]>                    cursorEntry;

    public MemoryKVIterator(ConcurrentNavigableMap<byte[], byte[]> db) {
        this.db = db;
    }

    @Override
    public boolean isValid() {
        return this.cursorEntry != null;
    }

    @Override
    public void seekToFirst() {
        this.cursorEntry = this.db.firstEntry();
    }

    @Override
    public void seekToLast() {
        this.cursorEntry = this.db.lastEntry();
    }

    @Override
    public void seek(final byte[] target) {
        this.cursorEntry = this.db.ceilingEntry(target);
    }

    @Override
    public void seekForPrev(final byte[] target) {
        this.cursorEntry = this.db.lowerEntry(target);
    }

    @Override
    public void next() {
        this.cursorEntry = this.db.higherEntry(this.cursorEntry.getKey());
    }

    @Override
    public void prev() {
        this.cursorEntry = this.db.lowerEntry(this.cursorEntry.getKey());
    }

    @Override
    public byte[] key() {
        return this.cursorEntry.getKey();
    }

    @Override
    public byte[] value() {
        return this.cursorEntry.getValue();
    }

    @Override
    public void close() throws Exception {
        // no-op
    }
}
