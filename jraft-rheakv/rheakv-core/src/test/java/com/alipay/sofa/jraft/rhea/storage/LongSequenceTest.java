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

import java.util.concurrent.atomic.AtomicLong;

import org.junit.Assert;
import org.junit.Test;

/**
 * @author jiachun.fjc
 */
public class LongSequenceTest {

    private final AtomicLong index = new AtomicLong();

    @Test
    public void nextText() {
        final long base = 100;
        final LongSequence sequence = new LongSequence(base) {

            @Override
            public Sequence getNextSequence() {
                return new Sequence(index.getAndAdd(10), index.get());
            }
        };
        for (int i = 0; i < 100; i++) {
            Assert.assertTrue(sequence.next() == (long) i + base);
        }
    }
}
