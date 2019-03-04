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
import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.rhea.client.RheaKVStore;
import com.alipay.sofa.jraft.rhea.storage.KVEntry;
import com.alipay.sofa.jraft.rhea.util.Lists;

import static com.alipay.sofa.jraft.util.BytesUtil.readUtf8;
import static com.alipay.sofa.jraft.util.BytesUtil.writeUtf8;

/**
 *
 * @author jiachun.fjc
 */
public class ScanExample {

    private static final Logger LOG = LoggerFactory.getLogger(ScanExample.class);

    public static void main(final String[] args) throws Exception {
        final Client client = new Client();
        client.init();
        scan(client.getRheaKVStore());
        client.shutdown();
    }

    @SuppressWarnings("unchecked")
    public static void scan(final RheaKVStore rheaKVStore) {
        final List<byte[]> keys = Lists.newArrayList();
        for (int i = 0; i < 10; i++) {
            final byte[] bytes = writeUtf8("scan_demo_" + i);
            keys.add(bytes);
            rheaKVStore.bPut(bytes, bytes);
        }

        final byte[] firstKey = keys.get(0);
        final byte[] lastKey = keys.get(keys.size() - 1);
        final String firstKeyString = readUtf8(firstKey);
        final String lastKeyString = readUtf8(lastKey);

        // async scan
        final CompletableFuture<List<KVEntry>> f1 = rheaKVStore.scan(firstKey, lastKey);
        final CompletableFuture<List<KVEntry>> f2 = rheaKVStore.scan(firstKey, lastKey, false);
        final CompletableFuture<List<KVEntry>> f3 = rheaKVStore.scan(firstKeyString, lastKeyString);
        final CompletableFuture<List<KVEntry>> f4 = rheaKVStore.scan(firstKeyString, lastKeyString, false);
        CompletableFuture.allOf(f1, f2, f3, f4).join();
        for (final CompletableFuture<List<KVEntry>> f : new CompletableFuture[] { f1, f2, f3, f4 }) {
            for (final KVEntry kv : f.join()) {
                LOG.info("Async scan: key={}, value={}", readUtf8(kv.getKey()), readUtf8(kv.getValue()));
            }
        }

        // sync scan
        final List<KVEntry> l1 = rheaKVStore.bScan(firstKey, lastKey);
        final List<KVEntry> l2 = rheaKVStore.bScan(firstKey, lastKey, false);
        final List<KVEntry> l3 = rheaKVStore.bScan(firstKeyString, lastKeyString);
        final List<KVEntry> l4 = rheaKVStore.bScan(firstKeyString, lastKeyString, false);
        for (final List<KVEntry> l : new List[] { l1, l2, l3, l4 }) {
            for (final KVEntry kv : l) {
                LOG.info("Async scan: key={}, value={}", readUtf8(kv.getKey()), readUtf8(kv.getValue()));
            }
        }
    }
}
