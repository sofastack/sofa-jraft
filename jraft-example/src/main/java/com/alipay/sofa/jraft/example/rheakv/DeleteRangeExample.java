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

import com.alipay.sofa.jraft.rhea.client.FutureHelper;
import com.alipay.sofa.jraft.rhea.client.RheaKVStore;

import static com.alipay.sofa.jraft.util.BytesUtil.writeUtf8;

/**
 *
 * @author jiachun.fjc
 */
public class DeleteRangeExample {

    public static void main(final String[] args) throws Exception {
        final Client client = new Client();
        client.init();
        deleteRange(client.getRheaKVStore());
        client.shutdown();
    }

    public static void deleteRange(final RheaKVStore rheaKVStore) {
        for (int i = 0; i < 10; i++) {
            rheaKVStore.bPut("delete_range_example_" + i, writeUtf8("1"));
        }
        final byte[] start = writeUtf8("delete_range_example_0");
        final byte[] end = writeUtf8("delete_range_example_9");
        final CompletableFuture<Boolean> f1 = rheaKVStore.deleteRange(start, end);
        FutureHelper.get(f1);

        for (int i = 0; i < 10; i++) {
            rheaKVStore.bPut("delete_range_example_" + i, writeUtf8("1"));
        }
        final CompletableFuture<Boolean> f2 = rheaKVStore.deleteRange("delete_range_example_0",
            "delete_range_example_9");
        FutureHelper.get(f2);

        for (int i = 0; i < 10; i++) {
            rheaKVStore.bPut("delete_range_example_" + i, writeUtf8("1"));
        }
        rheaKVStore.bDeleteRange(start, end);

        for (int i = 0; i < 10; i++) {
            rheaKVStore.bPut("delete_range_example_" + i, writeUtf8("1"));
        }
        rheaKVStore.bDeleteRange("delete_range_example_0", "delete_range_example_9");
    }
}
