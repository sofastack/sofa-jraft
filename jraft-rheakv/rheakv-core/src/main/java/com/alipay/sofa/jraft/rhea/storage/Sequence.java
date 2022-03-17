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
package com.alipay.sofa.jraft.rhea.storage;

import java.io.Serializable;

/**
 * Self-increasing sequence, each value is globally unique,
 * range: [startValue, endValue)
 *
 * @author jiachun.fjc
 */
public class Sequence implements Serializable {

    private static final long serialVersionUID = 25761738530535127L;

    private final long        startValue;                           // inclusive
    private final long        endValue;                             // exclusive

    public Sequence(long startValue, long endValue) {
        this.startValue = startValue;
        this.endValue = endValue;
    }

    /**
     * The available minimum value, including [startValue, endValue)
     */
    public long getStartValue() {
        return startValue;
    }

    /**
     * The available maximum value, not including [startValue, endValue)
     */
    public long getEndValue() {
        return endValue;
    }

    @Override
    public String toString() {
        return "Sequence{" + "startValue=" + startValue + ", endValue=" + endValue + '}';
    }
}
