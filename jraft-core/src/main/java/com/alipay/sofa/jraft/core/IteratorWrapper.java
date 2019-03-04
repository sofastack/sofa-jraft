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

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Iterator;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.entity.EnumOutter;
import com.alipay.sofa.jraft.entity.LogEntry;

public class IteratorWrapper implements Iterator {

    private final IteratorImpl impl;

    public IteratorWrapper(IteratorImpl iterImpl) {
        super();
        this.impl = iterImpl;
    }

    @Override
    public boolean hasNext() {
        return this.impl.isGood() && impl.entry().getType() == EnumOutter.EntryType.ENTRY_TYPE_DATA;
    }

    @Override
    public ByteBuffer next() {
        ByteBuffer data = getData();
        if (hasNext()) {
            impl.next();
        }
        return data;
    }

    @Override
    public ByteBuffer getData() {
        LogEntry entry = impl.entry();
        return entry != null ? entry.getData() : null;
    }

    @Override
    public long getIndex() {
        return impl.getIndex();
    }

    @Override
    public long getTerm() {
        return impl.entry().getId().getTerm();
    }

    @Override
    public Closure done() {
        return impl.done();
    }

    @Override
    public void setErrorAndRollback(long ntail, Status st) {
        impl.setErrorAndRollback(ntail, st);
    }
}
