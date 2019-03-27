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

import com.alipay.sofa.jraft.rhea.benchmark.BenchmarkUtil;
import com.alipay.sofa.jraft.rhea.client.RheaKVStore;
import com.alipay.sofa.jraft.rhea.storage.KVEntry;
import com.alipay.sofa.jraft.rhea.util.Lists;
import com.alipay.sofa.jraft.util.BytesUtil;

import static com.alipay.sofa.jraft.rhea.benchmark.BenchmarkUtil.KEY_COUNT;
import static com.alipay.sofa.jraft.rhea.benchmark.BenchmarkUtil.VALUE_BYTES;

/**
 * @author jiachun.fjc
 */
@State(Scope.Benchmark)
public class RheaKVGetBenchmark extends RheaBenchmarkCluster {
    /**
     //
     // 100w keys, each value is 100 bytes.
     //
     // get tps (mem_table命中率100%, 所以才会这么高的tps) = 925.409 * 1000 = 925409 ops/second
     // getReadOnlySafe tps                             = 62.949 * 1000 = 62949 ops/second
     // getReadOnlySafe tps (based raft log)            = 23.170 * 1000 = 23170 ops/second
     //
     Benchmark                                                     Mode       Cnt    Score     Error   Units
     RheaKVGetBenchmark.get                                       thrpt         3  925.409 ± 538.093  ops/ms
     RheaKVGetBenchmark.getReadOnlySafe                           thrpt         3   62.949 ±   0.762  ops/ms
     RheaKVGetBenchmark.get                                        avgt         3    0.034 ±   0.004   ms/op
     RheaKVGetBenchmark.getReadOnlySafe                            avgt         3    0.510 ±   0.073   ms/op
     RheaKVGetBenchmark.get                                      sample  13892415    0.048 ±   0.001   ms/op
     RheaKVGetBenchmark.get:get·p0.00                            sample              0.004             ms/op
     RheaKVGetBenchmark.get:get·p0.50                            sample              0.008             ms/op
     RheaKVGetBenchmark.get:get·p0.90                            sample              0.009             ms/op
     RheaKVGetBenchmark.get:get·p0.95                            sample              0.010             ms/op
     RheaKVGetBenchmark.get:get·p0.99                            sample              0.015             ms/op
     RheaKVGetBenchmark.get:get·p0.999                           sample             20.283             ms/op
     RheaKVGetBenchmark.get:get·p0.9999                          sample             54.460             ms/op
     RheaKVGetBenchmark.get:get·p1.00                            sample            194.773             ms/op
     RheaKVGetBenchmark.getReadOnlySafe                          sample   1871951    0.513 ±   0.001   ms/op
     RheaKVGetBenchmark.getReadOnlySafe:getReadOnlySafe·p0.00    sample              0.188             ms/op
     RheaKVGetBenchmark.getReadOnlySafe:getReadOnlySafe·p0.50    sample              0.494             ms/op
     RheaKVGetBenchmark.getReadOnlySafe:getReadOnlySafe·p0.90    sample              0.637             ms/op
     RheaKVGetBenchmark.getReadOnlySafe:getReadOnlySafe·p0.95    sample              0.689             ms/op
     RheaKVGetBenchmark.getReadOnlySafe:getReadOnlySafe·p0.99    sample              0.852             ms/op
     RheaKVGetBenchmark.getReadOnlySafe:getReadOnlySafe·p0.999   sample              1.747             ms/op
     RheaKVGetBenchmark.getReadOnlySafe:getReadOnlySafe·p0.9999  sample             12.976             ms/op
     RheaKVGetBenchmark.getReadOnlySafe:getReadOnlySafe·p1.00    sample             18.383             ms/op
     RheaKVGetBenchmark.get                                          ss         3    0.044 ±   0.055   ms/op
     RheaKVGetBenchmark.getReadOnlySafe                              ss         3    2.668 ±  13.052   ms/op
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

        // insert data first
        put();
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
    public void get() {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        byte[] key = BytesUtil.writeUtf8("benchmark_" + random.nextInt(KEY_COUNT));
        this.kvStore.get(key, false);
    }

    @Benchmark
    @BenchmarkMode(Mode.All)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void getReadOnlySafe() {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        byte[] key = BytesUtil.writeUtf8("benchmark_" + random.nextInt(KEY_COUNT));
        this.kvStore.get(key, true);
    }

    public void put() {
        final List<KVEntry> batch = Lists.newArrayListWithCapacity(100);
        for (int i = 0; i < KEY_COUNT; i++) {
            byte[] key = BytesUtil.writeUtf8("benchmark_" + i);
            batch.add(new KVEntry(key, VALUE_BYTES));
            if (batch.size() >= 10) {
                this.kvStore.bPut(batch);
                batch.clear();
            }
        }
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder() //
            .include(RheaKVGetBenchmark.class.getSimpleName()) //
            .warmupIterations(1) //
            .warmupTime(TimeValue.seconds(10)) //
            .measurementIterations(3) //
            .measurementTime(TimeValue.seconds(10)) //
            .threads(BenchmarkUtil.CONCURRENCY) //
            .forks(1) //
            .build();

        new Runner(opt).run();
    }
}
