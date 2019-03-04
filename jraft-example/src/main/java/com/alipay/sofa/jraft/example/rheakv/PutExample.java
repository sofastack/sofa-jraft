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

import com.alipay.sofa.jraft.rhea.client.FutureHelper;
import com.alipay.sofa.jraft.rhea.client.RheaKVStore;
import com.alipay.sofa.jraft.rhea.storage.KVEntry;
import com.alipay.sofa.jraft.rhea.util.Lists;

import static com.alipay.sofa.jraft.util.BytesUtil.readUtf8;
import static com.alipay.sofa.jraft.util.BytesUtil.writeUtf8;

/**
 *
 * @author jiachun.fjc
 */
public class PutExample {

    private static final Logger LOG = LoggerFactory.getLogger(PutExample.class);

    public static void main(final String[] args) throws Exception {
        final Client client = new Client();
        client.init();
        put(client.getRheaKVStore());
        client.shutdown();
    }

    public static void put(final RheaKVStore rheaKVStore) {
        final byte[] value = writeUtf8("put_example_value");
        final CompletableFuture<Boolean> r1 = rheaKVStore.put("1", value);
        if (FutureHelper.get(r1)) {
            LOG.info("Async put 1 {} success.", readUtf8(rheaKVStore.bGet("1")));
        }

        final CompletableFuture<Boolean> r2 = rheaKVStore.put(writeUtf8("2"), value);
        if (FutureHelper.get(r2)) {
            LOG.info("Async put 2 {} success.", readUtf8(rheaKVStore.bGet("2")));
        }

        final boolean r3 = rheaKVStore.bPut("3", value);
        if (r3) {
            LOG.info("Sync put 3 {} success.", readUtf8(rheaKVStore.bGet("3")));
        }

        final boolean r4 = rheaKVStore.bPut(writeUtf8("4"), value);
        if (r4) {
            LOG.info("Sync put 4 {} success.", readUtf8(rheaKVStore.bGet("4")));
        }

        // put list
        final KVEntry kv1 = new KVEntry(writeUtf8("10"), value);
        final KVEntry kv2 = new KVEntry(writeUtf8("11"), value);
        final KVEntry kv3 = new KVEntry(writeUtf8("12"), value);
        final KVEntry kv4 = new KVEntry(writeUtf8("13"), value);
        final KVEntry kv5 = new KVEntry(writeUtf8("14"), value);

        List<KVEntry> entries = Lists.newArrayList(kv1, kv2, kv3);

        final CompletableFuture<Boolean> r5 = rheaKVStore.put(entries);
        if (FutureHelper.get(r5)) {
            for (final KVEntry entry : entries) {
                LOG.info("Async put list {} with value {} success.", readUtf8(entry.getKey()),
                    readUtf8(entry.getValue()));
            }
        }

        entries = Lists.newArrayList(kv3, kv4, kv5);
        final boolean r6 = rheaKVStore.bPut(entries);
        if (r6) {
            for (final KVEntry entry : entries) {
                LOG.info("Sync put list {} with value {} success.", readUtf8(entry.getKey()),
                    readUtf8(entry.getValue()));
            }
        }
    }
}
