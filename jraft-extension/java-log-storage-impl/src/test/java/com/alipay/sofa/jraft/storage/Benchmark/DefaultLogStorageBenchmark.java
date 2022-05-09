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
package com.alipay.sofa.jraft.storage.Benchmark;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;

import com.alipay.sofa.jraft.conf.ConfigurationManager;
import com.alipay.sofa.jraft.entity.LogEntry;
import com.alipay.sofa.jraft.entity.codec.v2.LogEntryV2CodecFactory;
import com.alipay.sofa.jraft.option.LogStorageOptions;
import com.alipay.sofa.jraft.option.StoreOptions;
import com.alipay.sofa.jraft.storage.LogStorage;
import com.alipay.sofa.jraft.storage.LogitLogStorage;
import com.alipay.sofa.jraft.test.TestUtils;
import com.alipay.sofa.jraft.util.Utils;

/**
 * benchmark result:
 * total data : 8G
 * batchSize:200
 * 1.Test write:
 *  *   Log number   :524288
 *  *   Log Size     :16384
 *  *   Batch Size   :200
 *  *   Cost time(s) :40s
 *  *   Total size   :8589934592
 *
 *
 * 2.Test read:
 *   Log number   :524288
 *   Log Size     :16384
 *   Batch Size   :200
 *   Cost time(s) :6s
 *   Total size   :8589934592
 *
 * If batchSize = 500
 * write cost times = 38s
 *
 * If batchSize = 1000
 * write cost times = 32s
 *
 * @author hzh (642256541@qq.com)
 */
public class DefaultLogStorageBenchmark {

    private final LogStorage logStorage;

    private final int        logSize;

    private final int        totalLogs;

    private final int        batchSize;

    public DefaultLogStorageBenchmark(final LogStorage logStorage, final int logSize, final int totalLogs,
                                      final int batchSize) {
        super();
        this.logStorage = logStorage;
        this.logSize = logSize;
        this.totalLogs = totalLogs;
        this.batchSize = batchSize;
    }

    private void write(final int batchSize, final int logSize, final int totalLogs) {
        List<LogEntry> entries = new ArrayList<>(batchSize);
        for (int i = 0; i < totalLogs; i += batchSize) {
            for (int j = i; j < i + batchSize; j++) {
                entries.add(TestUtils.mockEntry(j, j, logSize));
            }
            int ret = this.logStorage.appendEntries(entries);
            if (ret != batchSize) {
                System.err.println("Fatal error: write failures, expect " + batchSize + ", but was " + ret);
                System.exit(1);
            }
            entries.clear(); //reuse it
        }
    }

    private static void assertNotNull(final Object obj) {
        if (obj == null) {
            System.err.println("Null object");
            System.exit(1);
        }
    }

    private static void assertEquals(final long x, final long y) {
        if (x != y) {
            System.err.println("Expect " + x + " but was " + y);
            System.exit(1);
        }
    }

    private void read(final int logSize, final int totalLogs) {
        for (int i = 0; i < totalLogs; i++) {
            final LogEntry log = this.logStorage.getEntry(i);
            assertNotNull(log);
            assertEquals(i, log.getId().getIndex());
            assertEquals(i, log.getId().getTerm());
            assertEquals(logSize, log.getData().remaining());
        }
    }

    private void report(final String op, final long cost) {
        System.out.println("Test " + op + ":");
        System.out.println("  Log number   :" + this.totalLogs);
        System.out.println("  Log Size     :" + this.logSize);
        System.out.println("  Batch Size   :" + this.batchSize);
        System.out.println("  Cost time(s) :" + cost / 1000);
        System.out.println("  Total size   :" + (long) this.totalLogs * this.logSize);
    }

    private void doTest() throws InterruptedException {
        System.out.println("Begin test...");
        System.out.println("Start test...");
        {
            long start = Utils.monotonicMs();
            write(this.batchSize, this.logSize, this.totalLogs);
            System.out.println("write done");
            long cost = Utils.monotonicMs() - start;
            report("write", cost);
        }
        Thread.sleep(3000);
        {
            long start = Utils.monotonicMs();
            read(this.logSize, this.totalLogs);
            long cost = Utils.monotonicMs() - start;
            report("read", cost);
        }
        System.out.println("Test done!");
    }

    public static LogStorage newLogStorage(final String path) {
        // Init options
        final StoreOptions storeOptions = new StoreOptions();
        // Init
        return new LogitLogStorage(path, storeOptions);
    }

    public static void main(final String[] args) throws InterruptedException {
        String testPath = "D://temp";
        System.out.println("Test storage path:" + testPath);
        final File file = new File(testPath);
        file.mkdirs();
        int batchSize = 500;
        int logSize = 16 * 1024;
        int totalLogs = 1024 * 512;
        LogStorage logStorage = newLogStorage(testPath);
        LogStorageOptions opts = new LogStorageOptions();
        opts.setConfigurationManager(new ConfigurationManager());
        opts.setLogEntryCodecFactory(LogEntryV2CodecFactory.getInstance());
        logStorage.init(opts);
        // warm up files
        new DefaultLogStorageBenchmark(logStorage, logSize, totalLogs, batchSize).doTest();
        System.out.println("begin to shutdown");
        logStorage.shutdown();
        FileUtils.deleteQuietly(file);
    }
}
