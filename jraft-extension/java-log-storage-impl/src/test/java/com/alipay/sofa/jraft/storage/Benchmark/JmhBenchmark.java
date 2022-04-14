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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

import com.alipay.sofa.jraft.entity.LogEntry;
import com.alipay.sofa.jraft.option.StoreOptions;
import com.alipay.sofa.jraft.storage.LogStorage;
import com.alipay.sofa.jraft.storage.LogitLogStorage;
import com.alipay.sofa.jraft.test.TestUtils;

/**
 * @author hzh (642256541@qq.com)
 */
@State(Scope.Benchmark)
public class JmhBenchmark extends BaseLogStorageBenchmark {

    private static final int logSize = 1024 * 16;

    /**
     * test Write
     *
     * logitLogStorage:
     *
     * Benchmark        Mode  Cnt  Score   Error   Units
     * bigBatchSize     thrpt    3  0.024 ± 0.050  ops/ms
     * normalBatchSize  thrpt    3  0.045 ± 0.029  ops/ms
     * smallBatchSize   thrpt    3  0.100 ± 0.112  ops/ms
     *
     * rocksdbLogStorage:
     * bigBatchSize     thrpt    3  0.022 ± 0.021  ops/ms
     * normalBatchSize  thrpt    3  0.038 ± 0.031  ops/ms
     * smallBatchSize   thrpt    3  0.088 ± 0.220  ops/ms
     */

    @Setup
    public void setup() throws Exception {
        super.setup();
    }

    @TearDown
    public void teardown() throws Exception {
        super.teardown();
    }

    public void testWrite(final int batchSize, final int logSize) {
        final List<LogEntry> entries = new ArrayList<>(batchSize);
        for (int i = 0; i < batchSize; i++) {
            entries.add(TestUtils.mockEntry(i, i, logSize));
        }
        int ret = this.logStorage.appendEntries(entries);
        if (ret != batchSize) {
            System.err.println("Fatal error: write failures, expect " + batchSize + ", but was " + ret);
            System.exit(1);
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void bigBatchSize() {
        testWrite(500, logSize);
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void normalBatchSize() {
        testWrite(250, logSize);
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void smallBatchSize() {
        testWrite(100, logSize);
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder() //
            .include(JmhBenchmark.class.getSimpleName()) //
            .warmupIterations(1) //
            .warmupTime(TimeValue.seconds(10)) //
            .measurementIterations(3) //
            .measurementTime(TimeValue.seconds(10)) //
            .threads(1) //
            .forks(1) //
            .build();

        new Runner(opt).run();
    }

    @Override
    protected LogStorage newLogStorage() {
        final StoreOptions storeOptions = new StoreOptions();
        storeOptions.setSegmentFileSize(1024 * 1024 * 64);
        //return new RocksDBLogStorage(this.path, new RaftOptions());
        return new LogitLogStorage(this.path, storeOptions);
    }
}
