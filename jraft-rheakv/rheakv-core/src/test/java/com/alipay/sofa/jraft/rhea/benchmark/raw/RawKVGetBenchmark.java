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
package com.alipay.sofa.jraft.rhea.benchmark.raw;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
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

import com.alipay.sofa.jraft.rhea.storage.KVEntry;
import com.alipay.sofa.jraft.rhea.util.Lists;
import com.alipay.sofa.jraft.util.BytesUtil;

import static com.alipay.sofa.jraft.rhea.benchmark.BenchmarkUtil.CONCURRENCY;
import static com.alipay.sofa.jraft.rhea.benchmark.BenchmarkUtil.KEY_COUNT;
import static com.alipay.sofa.jraft.rhea.benchmark.BenchmarkUtil.VALUE_BYTES;

/**
 * @author jiachun.fjc
 */
@State(Scope.Benchmark)
public class RawKVGetBenchmark extends BaseRawStoreBenchmark {
    /**
     //
     // 100w keys, each value is 100 bytes.
     //
     // get tps = 1034.823 * 1000 = 1034823 ops/second
     //
     Benchmark                            Mode       Cnt     Score     Error   Units
     RawKVGetBenchmark.get               thrpt         3  1034.823 ± 914.022  ops/ms
     RawKVGetBenchmark.get                avgt         3     0.032 ±   0.007   ms/op
     RawKVGetBenchmark.get              sample  14885516     0.047 ±   0.001   ms/op
     RawKVGetBenchmark.get:get·p0.00    sample               0.003             ms/op
     RawKVGetBenchmark.get:get·p0.50    sample               0.007             ms/op
     RawKVGetBenchmark.get:get·p0.90    sample               0.008             ms/op
     RawKVGetBenchmark.get:get·p0.95    sample               0.009             ms/op
     RawKVGetBenchmark.get:get·p0.99    sample               0.016             ms/op
     RawKVGetBenchmark.get:get·p0.999   sample               4.526             ms/op
     RawKVGetBenchmark.get:get·p0.9999  sample              69.599             ms/op
     RawKVGetBenchmark.get:get·p1.00    sample             248.775             ms/op
     RawKVGetBenchmark.get                  ss         3     0.039 ±   0.100   ms/op
     */

    @Setup
    public void setup() {
        try {
            super.setup();
        } catch (Exception e) {
            e.printStackTrace();
        }

        // insert data first
        put();
        long dbSize = this.kvStore.getApproximateKeysInRange(null, null);
        System.out.println("db size = " + dbSize);
    }

    @TearDown
    public void tearDown() {
        try {
            super.tearDown();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.All)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void get() {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        byte[] key = BytesUtil.writeUtf8("benchmark_" + random.nextInt(KEY_COUNT));
        this.kvStore.get(key, false, null);
    }

    public void put() {
        final List<KVEntry> batch = Lists.newArrayListWithCapacity(100);
        for (int i = 0; i < KEY_COUNT; i++) {
            byte[] key = BytesUtil.writeUtf8("benchmark_" + i);
            batch.add(new KVEntry(key, VALUE_BYTES));
            if (batch.size() >= 100) {
                this.kvStore.put(batch, null);
                batch.clear();
            }
        }
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder() //
            .include(RawKVGetBenchmark.class.getSimpleName()) //
            .warmupIterations(1) //
            .warmupTime(TimeValue.seconds(10)) //
            .measurementIterations(3) //
            .measurementTime(TimeValue.seconds(10)) //
            .threads(CONCURRENCY) //
            .forks(1) //
            .build();

        new Runner(opt).run();
    }
}
