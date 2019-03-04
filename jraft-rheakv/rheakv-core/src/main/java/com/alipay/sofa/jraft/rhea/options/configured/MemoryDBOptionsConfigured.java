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
package com.alipay.sofa.jraft.rhea.options.configured;

import com.alipay.sofa.jraft.rhea.options.MemoryDBOptions;
import com.alipay.sofa.jraft.rhea.util.Configured;

/**
 *
 * @author jiachun.fjc
 */
public final class MemoryDBOptionsConfigured implements Configured<MemoryDBOptions> {

    private final MemoryDBOptions opts;

    public static MemoryDBOptionsConfigured newConfigured() {
        return new MemoryDBOptionsConfigured(new MemoryDBOptions());
    }

    public MemoryDBOptionsConfigured withKeysPerSegment(final int keysPerSegment) {
        this.opts.setKeysPerSegment(keysPerSegment);
        return this;
    }

    @Override
    public MemoryDBOptions config() {
        return this.opts;
    }

    private MemoryDBOptionsConfigured(MemoryDBOptions opts) {
        this.opts = opts;
    }
}
