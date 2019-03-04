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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.RandomAccess;

import com.alipay.sofa.jraft.rhea.util.Recyclable;
import com.alipay.sofa.jraft.rhea.util.Recyclers;
import com.alipay.sofa.jraft.util.Requires;

/**
 * A simple kv state list which is recyclable.
 * This implementation does not allow {@code null} elements to be added.
 */
public final class KVStateOutputList extends ArrayList<KVState> implements Recyclable {

    private static final long serialVersionUID         = -8605125654176467947L;

    private static final int  DEFAULT_INITIAL_CAPACITY = 8;

    /**
     * Create a new empty {@link KVStateOutputList} instance
     */
    public static KVStateOutputList newInstance() {
        return newInstance(DEFAULT_INITIAL_CAPACITY);
    }

    /**
     * Create a new empty {@link KVStateOutputList} instance with the given capacity.
     */
    public static KVStateOutputList newInstance(int minCapacity) {
        KVStateOutputList ret = recyclers.get();
        ret.ensureCapacity(minCapacity);
        return ret;
    }

    public boolean isSingletonList() {
        return size() == 1;
    }

    /**
     * You must first check to make sure that {@link #isSingletonList()}
     * returns true.
     */
    public KVState getSingletonElement() {
        Requires.requireTrue(!isEmpty(), "empty");
        return get(0);
    }

    public KVState getFirstElement() {
        return isEmpty() ? null : get(0);
    }

    @Override
    public boolean addAll(Collection<? extends KVState> c) {
        checkNullElements(c);
        return super.addAll(c);
    }

    @Override
    public boolean addAll(int index, Collection<? extends KVState> c) {
        checkNullElements(c);
        return super.addAll(index, c);
    }

    private static void checkNullElements(Collection<?> c) {
        if (c instanceof RandomAccess && c instanceof List) {
            // produce less garbage
            List<?> list = (List<?>) c;
            int size = list.size();
            for (int i = 0; i < size; i++) {
                if (list.get(i) == null) {
                    throw new IllegalArgumentException("c contains null values");
                }
            }
        } else {
            for (Object element : c) {
                if (element == null) {
                    throw new IllegalArgumentException("c contains null values");
                }
            }
        }
    }

    @Override
    public boolean add(KVState element) {
        if (element == null) {
            throw new NullPointerException("element");
        }
        return super.add(element);
    }

    @Override
    public void add(int index, KVState element) {
        if (element == null) {
            throw new NullPointerException("element");
        }
        super.add(index, element);
    }

    @Override
    public KVState set(int index, KVState element) {
        if (element == null) {
            throw new NullPointerException("element");
        }
        return super.set(index, element);
    }

    @Override
    public boolean recycle() {
        clear();
        return recyclers.recycle(this, handle);
    }

    private KVStateOutputList(Recyclers.Handle handle) {
        this(handle, DEFAULT_INITIAL_CAPACITY);
    }

    private KVStateOutputList(Recyclers.Handle handle, int initialCapacity) {
        super(initialCapacity);
        this.handle = handle;
    }

    private transient final Recyclers.Handle          handle;

    private static final Recyclers<KVStateOutputList> recyclers = new Recyclers<KVStateOutputList>() {

                                                                    @Override
                                                                    protected KVStateOutputList newObject(Handle handle) {
                                                                        return new KVStateOutputList(handle);
                                                                    }
                                                                };
}
