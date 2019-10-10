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

import java.io.File;
import java.util.EnumMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.alipay.sofa.jraft.entity.LocalFileMetaOutter.LocalFileMeta;
import com.alipay.sofa.jraft.rhea.metadata.Region;

/**
 * KV store test helper
 *
 * @author jiachun.fjc
 */
public final class KVStoreAccessHelper {

    private static final ExecutorService EXECUTOR = Executors.newSingleThreadExecutor();

    public static void createSstFiles(final RocksRawKVStore store, final EnumMap<SstColumnFamily, File> sstFileTable,
                                      final byte[] startKey, final byte[] endKey) {
        store.createSstFiles(sstFileTable, startKey, endKey, EXECUTOR).join();
    }

    public static void ingestSstFiles(final RocksRawKVStore store, final EnumMap<SstColumnFamily, File> sstFileTable) {
        store.ingestSstFiles(sstFileTable);
    }

    public static LocalFileMeta.Builder saveSnapshot(final BaseRawKVStore<?> store, final String snapshotPath,
                                                     final Region region) throws Exception {
        final KVStoreSnapshotFile snapshotFile = KVStoreSnapshotFileFactory.getKVStoreSnapshotFile(store);
        return ((AbstractKVStoreSnapshotFile) snapshotFile).doSnapshotSave(snapshotPath, region, EXECUTOR).get();
    }

    public static void loadSnapshot(final BaseRawKVStore<?> store, final String snapshotPath, final LocalFileMeta meta,
                                    final Region region) throws Exception {
        final KVStoreSnapshotFile snapshotFile = KVStoreSnapshotFileFactory.getKVStoreSnapshotFile(store);
        ((AbstractKVStoreSnapshotFile) snapshotFile).doSnapshotLoad(snapshotPath, meta, region);
    }
}
