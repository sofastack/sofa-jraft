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

import com.alipay.sofa.jraft.util.BytesUtil;

import static com.alipay.sofa.jraft.rhea.benchmark.BenchmarkUtil.CONCURRENCY;
import static com.alipay.sofa.jraft.rhea.benchmark.BenchmarkUtil.KEY_COUNT;
import static com.alipay.sofa.jraft.rhea.benchmark.BenchmarkUtil.VALUE_BYTES;

/**
 * @author jiachun.fjc
 */
@State(Scope.Benchmark)
public class RawKVPutBenchmark extends BaseRawStoreBenchmark {

    /**
     //
     // 100w keys, each value is 100 bytes.
     //
     // put tps = 133.658 * 1000 = 133658 ops/second
     //
     Benchmark                            Mode      Cnt    Score    Error   Units
     RawKVPutBenchmark.put               thrpt        3  133.658 ± 26.637  ops/ms
     RawKVPutBenchmark.put                avgt        3    0.233 ±  0.023   ms/op
     RawKVPutBenchmark.put              sample  4057686    0.236 ±  0.001   ms/op
     RawKVPutBenchmark.put:put·p0.00    sample             0.013            ms/op
     RawKVPutBenchmark.put:put·p0.50    sample             0.219            ms/op
     RawKVPutBenchmark.put:put·p0.90    sample             0.332            ms/op
     RawKVPutBenchmark.put:put·p0.95    sample             0.375            ms/op
     RawKVPutBenchmark.put:put·p0.99    sample             0.499            ms/op
     RawKVPutBenchmark.put:put·p0.999   sample             0.818            ms/op
     RawKVPutBenchmark.put:put·p0.9999  sample             2.726            ms/op
     RawKVPutBenchmark.put:put·p1.00    sample            24.707            ms/op
     RawKVPutBenchmark.put                  ss        3    0.364 ±  0.737   ms/op

     */

    @Setup
    public void setup() {
        try {
            super.setup();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @TearDown
    public void tearDown() {
        try {
            super.tearDown();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder() //
            .include(RawKVPutBenchmark.class.getSimpleName()) //
            .warmupIterations(1) //
            .warmupTime(TimeValue.seconds(10)) //
            .measurementIterations(3) //
            .measurementTime(TimeValue.seconds(10)) //
            .threads(CONCURRENCY) //
            .forks(1) //
            .build();

        new Runner(opt).run();
    }

    @Benchmark
    @BenchmarkMode(Mode.All)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void put() {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        byte[] key = BytesUtil.writeUtf8("benchmark_" + random.nextInt(KEY_COUNT));
        super.kvStore.put(key, VALUE_BYTES, null);
    }
}
