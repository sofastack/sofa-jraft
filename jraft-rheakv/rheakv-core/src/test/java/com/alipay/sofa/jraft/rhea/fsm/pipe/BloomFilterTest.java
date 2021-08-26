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

import com.alipay.sofa.jraft.util.BytesUtil;
import junit.framework.TestCase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * @author hzh (642256541@qq.com)
 */
public class BloomFilterTest {

    private BloomFilter filter;
    private BloomFilter filter2;

    @Before
    public void setup() {
        this.filter = new BloomFilter(1024 * 1024);
        this.filter2 = new BloomFilter(1024 * 1024);
    }

    @Test
    public void testContains() {
        this.filter.add(BytesUtil.writeUtf8("key1"));
        this.filter.add(BytesUtil.writeUtf8("key2"));
        Assert.assertTrue(this.filter.contains(BytesUtil.writeUtf8("key1")));
        Assert.assertTrue(this.filter.contains(BytesUtil.writeUtf8("key2")));
    }

    @Test
    public void testIntersects() {
        this.filter.add(BytesUtil.writeUtf8("key1"));
        this.filter.add(BytesUtil.writeUtf8("key2"));
        this.filter2.add(BytesUtil.writeUtf8("key2"));
        this.filter2.add(BytesUtil.writeUtf8("key3"));
        Assert.assertTrue(this.filter.intersects(this.filter2));
    }
}