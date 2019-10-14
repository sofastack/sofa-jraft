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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

import com.alipay.sofa.jraft.rhea.errors.StorageException;
import com.alipay.sofa.jraft.rhea.metadata.Region;
import com.alipay.sofa.jraft.rhea.util.RegionHelper;

import static com.alipay.sofa.jraft.entity.LocalFileMetaOutter.LocalFileMeta;

/**
 *
 * @author jiachun.fjc
 */
public class RocksKVStoreSnapshotFile extends AbstractKVStoreSnapshotFile {

    private final RocksRawKVStore kvStore;

    RocksKVStoreSnapshotFile(RocksRawKVStore kvStore) {
        this.kvStore = kvStore;
    }

    @Override
    CompletableFuture<LocalFileMeta.Builder> doSnapshotSave(final String snapshotPath, final Region region,
                                                            final ExecutorService executor) throws Exception {
        if (RegionHelper.isMultiGroup(region)) {
            final CompletableFuture<Void> snapshotFuture = this.kvStore.writeSstSnapshot(snapshotPath, region, executor);
            final CompletableFuture<LocalFileMeta.Builder> metaFuture = new CompletableFuture<>();
            snapshotFuture.whenComplete((aVoid, throwable) -> {
                if (throwable == null) {
                    metaFuture.complete(writeMetadata(region));
                } else {
                    metaFuture.completeExceptionally(throwable);
                }
            });
            return metaFuture;
        }
        if (this.kvStore.isFastSnapshot()) {
            // Checkpoint is fast enough, no need to asynchronous
            this.kvStore.writeSnapshot(snapshotPath);
            return CompletableFuture.completedFuture(writeMetadata(null));
        }
        final RocksDBBackupInfo backupInfo = this.kvStore.backupDB(snapshotPath);
        return CompletableFuture.completedFuture(writeMetadata(backupInfo));
    }

    @Override
    void doSnapshotLoad(final String snapshotPath, final LocalFileMeta meta, final Region region) throws Exception {
        if (RegionHelper.isMultiGroup(region)) {
            final Region snapshotRegion = readMetadata(meta, Region.class);
            if (!RegionHelper.isSameRange(region, snapshotRegion)) {
                throw new StorageException("Invalid snapshot region: " + snapshotRegion + " current region is: "
                                           + region);
            }
            this.kvStore.readSstSnapshot(snapshotPath);
            return;
        }
        if (this.kvStore.isFastSnapshot()) {
            this.kvStore.readSnapshot(snapshotPath);
            return;
        }
        final RocksDBBackupInfo rocksBackupInfo = readMetadata(meta, RocksDBBackupInfo.class);
        this.kvStore.restoreBackup(snapshotPath, rocksBackupInfo);
    }
}
