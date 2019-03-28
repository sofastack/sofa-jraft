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
package com.alipay.sofa.jraft.rhea.benchmark;

import java.util.concurrent.ThreadLocalRandom;

/**
 *
 * @author jiachun.fjc
 */
public class BenchmarkUtil {

    public static final int    CONCURRENCY = 32;
    public static final int    KEY_COUNT   = 1000000;

    public static final byte[] VALUE_BYTES;

    static {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        byte[] bytes = new byte[100];
        random.nextBytes(bytes);
        VALUE_BYTES = bytes;
    }
}
