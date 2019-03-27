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
package com.alipay.sofa.jraft.rhea.benchmark.rhea;

import java.io.IOException;
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

import com.alipay.sofa.jraft.rhea.client.RheaKVStore;
import com.alipay.sofa.jraft.util.BytesUtil;

import static com.alipay.sofa.jraft.rhea.benchmark.BenchmarkUtil.CONCURRENCY;
import static com.alipay.sofa.jraft.rhea.benchmark.BenchmarkUtil.KEY_COUNT;
import static com.alipay.sofa.jraft.rhea.benchmark.BenchmarkUtil.VALUE_BYTES;

/**
 * @author jiachun.fjc
 */
@State(Scope.Benchmark)
public class RheaKVPutBenchmark extends RheaBenchmarkCluster {

    /**
     //
     // 100w keys, each value is 100 bytes.
     //
     // put tps = 24.548 * 1000 = 24548 ops/second
     //
     Benchmark                             Mode     Cnt   Score    Error   Units
     RheaKVPutBenchmark.put               thrpt       3  24.548 ± 20.413  ops/ms
     RheaKVPutBenchmark.put                avgt       3   1.282 ±  0.651   ms/op
     RheaKVPutBenchmark.put              sample  750138   1.279 ±  0.005   ms/op
     RheaKVPutBenchmark.put:put·p0.00    sample           0.403            ms/op
     RheaKVPutBenchmark.put:put·p0.50    sample           1.163            ms/op
     RheaKVPutBenchmark.put:put·p0.90    sample           1.798            ms/op
     RheaKVPutBenchmark.put:put·p0.95    sample           2.032            ms/op
     RheaKVPutBenchmark.put:put·p0.99    sample           2.712            ms/op
     RheaKVPutBenchmark.put:put·p0.999   sample           7.717            ms/op
     RheaKVPutBenchmark.put:put·p0.9999  sample          71.303            ms/op
     RheaKVPutBenchmark.put:put·p1.00    sample          85.197            ms/op
     RheaKVPutBenchmark.put                  ss       3   4.422 ±  4.274   ms/op

     */

    private RheaKVStore kvStore;

    @Setup
    public void setup() {
        try {
            super.start();
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
        this.kvStore = getLeaderStore(1);
    }

    @TearDown
    public void tearDown() {
        try {
            super.shutdown();
        } catch (final IOException e) {
            e.printStackTrace();
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.All)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void put() {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        byte[] key = BytesUtil.writeUtf8("benchmark_" + random.nextInt(KEY_COUNT));
        this.kvStore.bPut(key, VALUE_BYTES);
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder() //
            .include(RheaKVPutBenchmark.class.getSimpleName()) //
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
