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

import java.util.Collections;
import java.util.List;
import java.util.function.Function;

/**
 * Batch tasks are split and executed in the smallest unit,
 * can be used to perform parallel tasks, and can also be
 * used to perform serial batch tasks.
 *
 * @author jiachun.fjc
 */
public class Partitions {

    /**
     * Batch tasks are split and executed in the smallest unit,
     * and the callback function (input -> output) is in a
     * many-to-one manner.
     *
     * @param inputList input parameter list
     * @param unitSize  the minimum unit
     * @param func      the callback function
     * @param <IN>      input type
     * @param <OUT>     output type
     */
    public static <IN, OUT> List<OUT> manyToOne(final List<IN> inputList, final int unitSize,
                                                final Function<List<IN>, OUT> func) {
        if (inputList == null || inputList.isEmpty()) {
            return Collections.emptyList();
        }

        final int inputSize = inputList.size();

        final List<OUT> outputList = Lists.newArrayListWithCapacity((int) Math.ceil((double) inputSize / unitSize));

        if (inputSize <= unitSize) {
            add(outputList, func.apply(inputList));
            return outputList;
        }

        List<IN> segment = Lists.newArrayListWithCapacity(unitSize);
        for (final IN input : inputList) {
            segment.add(input);
            if (segment.size() >= unitSize) {
                add(outputList, func.apply(segment));
                segment = Lists.newArrayListWithCapacity(unitSize);
            }
        }

        if (!segment.isEmpty()) {
            add(outputList, func.apply(segment));
        }

        return outputList;
    }

    /**
     * Batch tasks are split and executed in the smallest unit,
     * and the callback function (input -> output) is in a
     * one-to-one manner.
     *
     * @param inputList input parameter list
     * @param unitSize  the minimum unit
     * @param func      the callback function
     * @param <IN>      input type
     * @param <OUT>     output type
     */
    public static <IN, OUT> List<OUT> oneToOne(final List<IN> inputList, final int unitSize,
                                               final Function<List<IN>, List<OUT>> func) {
        if (inputList == null || inputList.isEmpty()) {
            return Collections.emptyList();
        }

        final int inputSize = inputList.size();

        if (inputSize <= unitSize) {
            return func.apply(inputList);
        }

        final List<OUT> outputList = Lists.newArrayListWithCapacity(inputSize);

        List<IN> segment = Lists.newArrayListWithCapacity(unitSize);
        for (final IN input : inputList) {
            segment.add(input);
            if (segment.size() >= unitSize) {
                addAll(outputList, func.apply(segment));
                segment = Lists.newArrayListWithCapacity(unitSize);
            }
        }

        if (!segment.isEmpty()) {
            addAll(outputList, func.apply(segment));
        }

        return outputList;
    }

    private static <T> void add(final List<T> list, final T element) {
        if (list != null && element != null) {
            list.add(element);
        }
    }

    private static <T> void addAll(final List<T> list, final List<T> elements) {
        if (list != null && elements != null && !elements.isEmpty()) {
            list.addAll(elements);
        }
    }
}
