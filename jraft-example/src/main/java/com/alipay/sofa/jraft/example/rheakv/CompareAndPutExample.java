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

import com.alipay.sofa.jraft.rhea.client.FutureHelper;
import com.alipay.sofa.jraft.rhea.client.RheaKVStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

import static com.alipay.sofa.jraft.util.BytesUtil.readUtf8;
import static com.alipay.sofa.jraft.util.BytesUtil.writeUtf8;

/**
 *
 * @author nicholas.jxf
 */
public class CompareAndPutExample {

    private static final Logger LOG = LoggerFactory.getLogger(CompareAndPutExample.class);

    public static void main(final String[] args) throws Exception {
        final Client client = new Client();
        client.init();
        put(client.getRheaKVStore());
        client.shutdown();
    }

    private static void put(final RheaKVStore rheaKVStore) {
        final CompletableFuture<Boolean> r1 = rheaKVStore.put("compareAndPut", writeUtf8("compareAndPutExpect"));
        if (FutureHelper.get(r1)) {
            LOG.info("Async put compareAndPut {} success.", readUtf8(rheaKVStore.bGet("compareAndPut")));
        }

        final CompletableFuture<Boolean> f1 = rheaKVStore.compareAndPut(writeUtf8("compareAndPut"),
            writeUtf8("compareAndPutExpect"), writeUtf8("compareAndPutUpdate"));
        if (FutureHelper.get(f1)) {
            LOG.info("Compare compareAndPutExpect and set {} success.", readUtf8(rheaKVStore.bGet("compareAndPut")));
        }

        final CompletableFuture<Boolean> f2 = rheaKVStore.compareAndPut("compareAndPut",
            writeUtf8("compareAndPutUpdate"), writeUtf8("compareAndPutUpdate2"));
        if (FutureHelper.get(f2)) {
            LOG.info("Compare compareAndPutUpdate and set {} success.", readUtf8(rheaKVStore.bGet("compareAndPut")));
        }

        final Boolean b1 = rheaKVStore.bCompareAndPut(writeUtf8("compareAndPut1"), writeUtf8("compareAndPutUpdate2"),
            writeUtf8("compareAndPutUpdate3"));
        if (b1) {
            LOG.info("Compare compareAndPutUpdate2 and set {} success.", readUtf8(rheaKVStore.bGet("compareAndPut")));
        }

        final Boolean b2 = rheaKVStore.bCompareAndPut(writeUtf8("compareAndPut1"), writeUtf8("compareAndPutUpdate3"),
            writeUtf8("compareAndPutUpdate4"));
        if (b2) {
            LOG.info("Compare compareAndPutUpdate3 and set {} success.", readUtf8(rheaKVStore.bGet("compareAndPut")));
        }
    }
}
