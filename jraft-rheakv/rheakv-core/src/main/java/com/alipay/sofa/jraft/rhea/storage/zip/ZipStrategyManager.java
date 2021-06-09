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
package com.alipay.sofa.jraft.rhea.storage.zip;

import com.alipay.sofa.jraft.rhea.options.RheaKVStoreOptions;

/**
 * @author hzh
 */
public final class ZipStrategyManager {
    private static ZipStrategy[] zipStrategies     = new ZipStrategy[5];
    private static byte          DEFAULT_STRATEGY  = 1;
    public static final byte     JDK_STRATEGY      = 1;
    public static final byte     PARALLEL_STRATEGY = 2;

    static {
        addZipStrategy(JDK_STRATEGY, new JDKZipStrategy());
    }

    public static void addZipStrategy(int idx, ZipStrategy zipStrategy) {
        if (zipStrategies.length <= idx) {
            final ZipStrategy[] newZipStrategies = new ZipStrategy[idx + 5];
            System.arraycopy(zipStrategies, 0, newZipStrategies, 0, zipStrategies.length);
            zipStrategies = newZipStrategies;
        }
        zipStrategies[idx] = zipStrategy;
    }

    public static ZipStrategy getZipStrategy(int idx) {
        return zipStrategies[idx];
    }

    public static ZipStrategy getDefault() {
        return zipStrategies[DEFAULT_STRATEGY];
    }

    private ZipStrategyManager() {
    }

    public static boolean init(RheaKVStoreOptions opts) {
        // add parallel zip strategy if necessary
        if (opts.isUseParallelCompress()) {
            ParallelZipStrategy parallelZipStrategy = new ParallelZipStrategy(opts.getCompressThreads(),
                opts.getDeCompressThreads());
            parallelZipStrategy.init();
            addZipStrategy(PARALLEL_STRATEGY, parallelZipStrategy);
            DEFAULT_STRATEGY = PARALLEL_STRATEGY;
        }
        return true;
    }

    public static void shutdown() {
        for (ZipStrategy zipStrategy : zipStrategies) {
            if (zipStrategy != null) {
                zipStrategy.shutdown();
            }
        }
    }
}
