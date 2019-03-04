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
package com.alipay.sofa.jraft.example.rheakv;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.rhea.client.RheaKVStore;
import com.alipay.sofa.jraft.rhea.util.ByteArray;
import com.alipay.sofa.jraft.rhea.util.Lists;

import static com.alipay.sofa.jraft.util.BytesUtil.readUtf8;
import static com.alipay.sofa.jraft.util.BytesUtil.writeUtf8;

/**
 *
 * @author jiachun.fjc
 */
public class MultiGetExample {

    private static final Logger LOG = LoggerFactory.getLogger(MultiGetExample.class);

    public static void main(final String[] args) throws Exception {
        final Client client = new Client();
        client.init();
        multiGet(client.getRheaKVStore());
        client.shutdown();
    }

    public static void multiGet(final RheaKVStore rheaKVStore) {
        final List<byte[]> keys = Lists.newArrayList();
        for (int i = 0; i < 10; i++) {
            final byte[] bytes = writeUtf8("multi_get_demo_" + i);
            keys.add(bytes);
            rheaKVStore.bPut(bytes, bytes);
        }

        // async multiGet
        final CompletableFuture<Map<ByteArray, byte[]>> f1 = rheaKVStore.multiGet(keys);
        final CompletableFuture<Map<ByteArray, byte[]>> f2 = rheaKVStore.multiGet(keys, false);
        CompletableFuture.allOf(f1, f2).join();
        for (Map.Entry<ByteArray, byte[]> entry : f1.join().entrySet()) {
            LOG.info("Async multiGet: key={}, value={}", readUtf8(entry.getKey().getBytes()),
                readUtf8(entry.getValue()));
        }
        for (Map.Entry<ByteArray, byte[]> entry : f2.join().entrySet()) {
            LOG.info("Async multiGet: key={}, value={}", readUtf8(entry.getKey().getBytes()),
                readUtf8(entry.getValue()));
        }

        // async multiGet
        final Map<ByteArray, byte[]> map1 = rheaKVStore.bMultiGet(keys);
        Map<ByteArray, byte[]> map2 = rheaKVStore.bMultiGet(keys, false);
        for (Map.Entry<ByteArray, byte[]> entry : map1.entrySet()) {
            LOG.info("Sync multiGet: key={}, value={}", readUtf8(entry.getKey().getBytes()), readUtf8(entry.getValue()));
        }
        for (Map.Entry<ByteArray, byte[]> entry : map2.entrySet()) {
            LOG.info("Sync multiGet: key={}, value={}", readUtf8(entry.getKey().getBytes()), readUtf8(entry.getValue()));
        }
    }
}
