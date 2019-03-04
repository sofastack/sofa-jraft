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
package com.alipay.sofa.jraft.rhea.util;

import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

/**
 * @author jiachun.fjc
 */
@SuppressWarnings("all")
@State(Scope.Benchmark)
public class VarIntsBenchmark {

    /**
    Benchmark                                     Mode     Cnt       Score      Error   Units
    VarIntsBenchmark.int64                       thrpt       3       3.048 ±    1.002  ops/ns
    VarIntsBenchmark.varInt64                    thrpt       3       0.931 ±    0.020  ops/ns
    VarIntsBenchmark.int64                        avgt       3       0.319 ±    0.023   ns/op
    VarIntsBenchmark.varInt64                     avgt       3       1.074 ±    0.080   ns/op
    VarIntsBenchmark.int64                      sample  799721      51.160 ±    3.511   ns/op
    VarIntsBenchmark.int64:int64·p0.00          sample               6.000              ns/op
    VarIntsBenchmark.int64:int64·p0.50          sample              39.000              ns/op
    VarIntsBenchmark.int64:int64·p0.90          sample              41.000              ns/op
    VarIntsBenchmark.int64:int64·p0.95          sample              45.000              ns/op
    VarIntsBenchmark.int64:int64·p0.99          sample              92.000              ns/op
    VarIntsBenchmark.int64:int64·p0.999         sample             760.278              ns/op
    VarIntsBenchmark.int64:int64·p0.9999        sample           17313.779              ns/op
    VarIntsBenchmark.int64:int64·p1.00          sample          697344.000              ns/op
    VarIntsBenchmark.varInt64                   sample  861436      46.786 ±    1.051   ns/op
    VarIntsBenchmark.varInt64:varInt64·p0.00    sample               1.000              ns/op
    VarIntsBenchmark.varInt64:varInt64·p0.50    sample              39.000              ns/op
    VarIntsBenchmark.varInt64:varInt64·p0.90    sample              42.000              ns/op
    VarIntsBenchmark.varInt64:varInt64·p0.95    sample              42.000              ns/op
    VarIntsBenchmark.varInt64:varInt64·p0.99    sample              53.000              ns/op
    VarIntsBenchmark.varInt64:varInt64·p0.999   sample             301.126              ns/op
    VarIntsBenchmark.varInt64:varInt64·p0.9999  sample           13407.906              ns/op
    VarIntsBenchmark.varInt64:varInt64·p1.00    sample           40448.000              ns/op
    VarIntsBenchmark.int64                          ss       3    2254.333 ± 6750.580   ns/op
    VarIntsBenchmark.varInt64                       ss       3    2859.333 ± 3305.354   ns/op
     */

    static long SMALL_VAL = 64;

    @Benchmark
    @BenchmarkMode(Mode.All)
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    public void varInt64() {
        byte[] bytes = VarInts.writeVarInt64(SMALL_VAL);
        VarInts.readVarInt64(bytes);
    }

    @Benchmark
    @BenchmarkMode(Mode.All)
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    public void int64() {
        byte[] bytes = new byte[8];
        putLong(bytes, 0, SMALL_VAL);
        getLong(bytes, 0);
    }

    public static long getLong(byte[] b, int off) {
        return (b[off + 7] & 0xFFL) + ((b[off + 6] & 0xFFL) << 8) + ((b[off + 5] & 0xFFL) << 16)
               + ((b[off + 4] & 0xFFL) << 24) + ((b[off + 3] & 0xFFL) << 32) + ((b[off + 2] & 0xFFL) << 40)
               + ((b[off + 1] & 0xFFL) << 48) + ((long) b[off] << 56);
    }

    public static void putLong(byte[] b, int off, long val) {
        b[off + 7] = (byte) val;
        b[off + 6] = (byte) (val >>> 8);
        b[off + 5] = (byte) (val >>> 16);
        b[off + 4] = (byte) (val >>> 24);
        b[off + 3] = (byte) (val >>> 32);
        b[off + 2] = (byte) (val >>> 40);
        b[off + 1] = (byte) (val >>> 48);
        b[off] = (byte) (val >>> 56);
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder() //
            .include(VarIntsBenchmark.class.getSimpleName()) //
            .warmupIterations(3) //
            .warmupTime(TimeValue.seconds(10)) //
            .measurementIterations(3) //
            .measurementTime(TimeValue.seconds(10)) //
            .forks(1) //
            .build();

        new Runner(opt).run();
    }
}
