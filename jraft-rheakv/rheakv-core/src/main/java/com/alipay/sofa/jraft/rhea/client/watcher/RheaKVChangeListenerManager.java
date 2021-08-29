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
package com.alipay.sofa.jraft.rhea.client.watcher;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Function;

/**
 * @author funkye
 */
public class RheaKVChangeListenerManager {

    private static final Map<byte[], Queue<RheaKVChangeListener>> KV_BYTE_LISTENER_QUEUE = new ConcurrentHashMap<>();

    public static void push(byte[] key, RheaKVChangeListener listener) {
        Queue<RheaKVChangeListener> queue = computeIfAbsent(KV_BYTE_LISTENER_QUEUE, key,
            value -> new ConcurrentLinkedQueue<>());
        if (queue.contains(listener)) {
            synchronized (queue) {
                if (queue.contains(listener)) {
                    queue.add(listener);
                }
            }
        }
    }

    public static void notify(byte[] key, byte opType) {
        Queue<RheaKVChangeListener> queue = KV_BYTE_LISTENER_QUEUE.remove(key);
        if (queue != null && queue.size() > 0) {
            synchronized (queue) {
                if (queue.size() > 0) {
                    RheaKVChangeEvent<byte[]> rheaKVChangeEvent = new RheaKVChangeEvent<>();
                    rheaKVChangeEvent.setOpType(opType);
                    queue.parallelStream().forEach(listener -> listener.onChangeEvent(rheaKVChangeEvent));
                }
            }
        }
    }

    /**
     * Compute if absent.
     * Use this method if you are frequently using the same key,
     * because the get method has no lock.
     *
     * @param map             the map
     * @param key             the key
     * @param mappingFunction the mapping function
     * @param <K>             the type of key
     * @param <V>             the type of value
     * @return the value
     */
    public static <K, V> V computeIfAbsent(Map<K, V> map, K key, Function<? super K, ? extends V> mappingFunction) {
        V value = map.get(key);
        if (value != null) {
            return value;
        }
        return map.computeIfAbsent(key, mappingFunction);
    }

}
