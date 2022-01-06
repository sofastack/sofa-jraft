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
package com.alipay.sofa.jraft.util.concurrent;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

/**
 *
 * @author hzh (642256541@qq.com)
 */
public interface BlockingQueue2<E> {

    /**
     * Inserts the specified element at the tail of this queue, waiting
     * up to the specified wait time for space to become available if
     * the queue is full.
     *
     */
    void offer(E e, int timeout, TimeUnit unit) throws InterruptedException;

    /**
     * Removes all available elements from this queue and adds them
     * to the given collection, waiting up to the specified wait time
     * if necessary for an element to become available.
     *
     */
    int blockingDrainTo(Collection<? super E> c, int maxElements, int timeout, TimeUnit unit)
                                                                                             throws InterruptedException;
}
