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

import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.rhea.client.RheaKVStore;

import static com.alipay.sofa.jraft.util.BytesUtil.readUtf8;

/**
 *
 * @author jiachun.fjc
 */
public class MergeExample {

    private static final Logger LOG = LoggerFactory.getLogger(MergeExample.class);

    public static void main(final String[] args) throws Exception {
        final Client client = new Client();
        client.init();
        merge(client.getRheaKVStore());
        client.shutdown();
    }

    public static void merge(final RheaKVStore rheaKVStore) {
        final CompletableFuture<Boolean> f1 = rheaKVStore.merge("merge_example", "1");
        final CompletableFuture<Boolean> f2 = rheaKVStore.merge("merge_example", "2");
        final CompletableFuture<Boolean> f3 = rheaKVStore.merge("merge_example", "3");
        final CompletableFuture<Boolean> f4 = rheaKVStore.merge("merge_example", "4");
        final CompletableFuture<Boolean> f5 = rheaKVStore.merge("merge_example", "5");

        CompletableFuture.allOf(f1, f2, f3, f4, f5).join();
        LOG.info("Merge result is: {}", readUtf8(rheaKVStore.bGet("merge_example")));

        rheaKVStore.bMerge("merge_example1", "1");
        rheaKVStore.bMerge("merge_example1", "2");
        rheaKVStore.bMerge("merge_example1", "3");
        rheaKVStore.bMerge("merge_example1", "4");
        rheaKVStore.bMerge("merge_example1", "5");
        LOG.info("Merge result is: {}", readUtf8(rheaKVStore.bGet("merge_example1")));
    }
}
