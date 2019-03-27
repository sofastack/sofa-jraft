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
package com.alipay.sofa.jraft.benchmark.kv.raw;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.zip.ZipOutputStream;

import org.apache.commons.io.FileUtils;

import com.alipay.sofa.jraft.entity.LocalFileMetaOutter;
import com.alipay.sofa.jraft.rhea.options.RocksDBOptions;
import com.alipay.sofa.jraft.rhea.storage.KVEntry;
import com.alipay.sofa.jraft.rhea.storage.RocksRawKVStore;
import com.alipay.sofa.jraft.rhea.util.Lists;
import com.alipay.sofa.jraft.rhea.util.ZipUtil;
import com.alipay.sofa.jraft.util.BytesUtil;

import static com.alipay.sofa.jraft.benchmark.kv.BenchmarkUtil.KEY_COUNT;
import static com.alipay.sofa.jraft.benchmark.kv.BenchmarkUtil.VALUE_BYTES;

/**
 * @author jiachun.fjc
 */
public class SnapshotBenchmark extends BaseRawStoreBenchmark {

    private static final String SNAPSHOT_DIR     = "kv";
    private static final String SNAPSHOT_ARCHIVE = "kv.zip";

    public void setup() {
        try {
            super.setup();
        } catch (Exception e) {
            e.printStackTrace();
        }

        // insert data first
        put();
        long dbSize = this.kvStore.getApproximateKeysInRange(null, null);
        System.out.println("db size = " + dbSize);
    }

    public void tearDown() {
        try {
            super.tearDown();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void put() {
        final List<KVEntry> batch = Lists.newArrayListWithCapacity(100);
        for (int i = 0; i < KEY_COUNT * 100; i++) {
            byte[] key = BytesUtil.writeUtf8("benchmark_" + i);
            batch.add(new KVEntry(key, VALUE_BYTES));
            if (batch.size() >= 100) {
                this.kvStore.put(batch, null);
                batch.clear();
            }
        }
    }

    /*
    100 million keys, the time unit is milliseconds

     slow save snapshot time cost: 8265
     slow compress time cost: 46517
     slow load snapshot time cost: 21907

     slow save snapshot time cost: 7424
     slow compress time cost: 45040
     slow load snapshot time cost: 19257

     slow save snapshot time cost: 7025
     slow compress time cost: 44410
     slow load snapshot time cost: 20087

     -----------------------------------------------------

     fast save snapshot time cost: 742
     fast compress time cost: 37548
     fast load snapshot time cost: 13100

     fast save snapshot time cost: 743
     fast compress time cost: 43864
     fast load snapshot time cost: 14176

     fast save snapshot time cost: 755
     fast compress time cost: 45789
     fast load snapshot time cost: 14308
     */

    public static void main(String[] args) throws IOException {
        for (int i = 0; i < 3; i++) {
            SnapshotBenchmark snapshot = new SnapshotBenchmark();
            snapshot.setup();
            snapshot.snapshot(false);
            snapshot.tearDown();
        }

        for (int i = 0; i < 3; i++) {
            SnapshotBenchmark snapshot = new SnapshotBenchmark();
            snapshot.setup();
            snapshot.snapshot(true);
            snapshot.tearDown();
        }
    }

    public void snapshot(boolean isFastSnapshot) throws IOException {
        final File backupDir = new File("backup");
        if (backupDir.exists()) {
            FileUtils.deleteDirectory(backupDir);
        }
        FileUtils.forceMkdir(backupDir);

        final LocalFileMetaOutter.LocalFileMeta meta = doSnapshotSave(backupDir.getAbsolutePath(), isFastSnapshot);

        this.kvStore.shutdown();
        FileUtils.deleteDirectory(new File(this.tempPath));
        FileUtils.forceMkdir(new File(this.tempPath));
        this.kvStore = new RocksRawKVStore();
        final RocksDBOptions dbOpts = new RocksDBOptions();
        dbOpts.setDbPath(this.tempPath);
        this.kvStore.init(dbOpts);

        final long loadStart = System.nanoTime();
        doSnapshotLoad(backupDir.getAbsolutePath(), meta, isFastSnapshot);
        System.out.println((isFastSnapshot ? "fast" : "slow") + " load snapshot time cost: "
                           + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - loadStart));
        FileUtils.deleteDirectory(backupDir);
    }

    private LocalFileMetaOutter.LocalFileMeta doSnapshotSave(final String path, final boolean isFastSnapshot) {
        final String snapshotPath = path + File.separator + SNAPSHOT_DIR;
        try {
            final long saveStart = System.nanoTime();
            final LocalFileMetaOutter.LocalFileMeta meta;
            if (isFastSnapshot) {
                doFastSnapshotSave(snapshotPath);
                meta = null;
            } else {
                meta = doSlowSnapshotSave(snapshotPath);
            }
            System.out.println((isFastSnapshot ? "fast" : "slow") + " save snapshot time cost: "
                               + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - saveStart));
            final long compressStart = System.nanoTime();
            doCompressSnapshot(path);
            System.out.println((isFastSnapshot ? "fast" : "slow") + " compress time cost: "
                               + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - compressStart));
            return meta;
        } catch (final Throwable t) {
            t.printStackTrace();
        }
        return null;
    }

    public boolean doSnapshotLoad(final String path, final LocalFileMetaOutter.LocalFileMeta meta,
                                  final boolean isFastSnapshot) {
        try {
            ZipUtil.unzipFile(path + File.separator + SNAPSHOT_ARCHIVE, path);
            if (isFastSnapshot) {
                doFastSnapshotLoad(path + File.separator + SNAPSHOT_DIR);
            } else {
                doSlowSnapshotLoad(path + File.separator + SNAPSHOT_DIR, meta);
            }
            return true;
        } catch (final Throwable t) {
            t.printStackTrace();
            return false;
        }
    }

    private void doFastSnapshotSave(final String snapshotPath) throws Exception {
        this.dbOptions.setFastSnapshot(true);
        this.kvStore.onSnapshotSave(snapshotPath);
    }

    private LocalFileMetaOutter.LocalFileMeta doSlowSnapshotSave(final String snapshotPath) throws Exception {
        this.dbOptions.setFastSnapshot(false);
        return this.kvStore.onSnapshotSave(snapshotPath);
    }

    private void doCompressSnapshot(final String path) {
        try {
            try (final ZipOutputStream out = new ZipOutputStream(new FileOutputStream(path + File.separator
                                                                                      + SNAPSHOT_ARCHIVE))) {
                ZipUtil.compressDirectoryToZipFile(path, SNAPSHOT_DIR, out);
            }
        } catch (final Throwable t) {
            t.printStackTrace();
        }
    }

    private void doFastSnapshotLoad(final String snapshotPath) {
        try {
            this.dbOptions.setFastSnapshot(true);
            this.kvStore.onSnapshotLoad(snapshotPath, null);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void doSlowSnapshotLoad(final String snapshotPath, final LocalFileMetaOutter.LocalFileMeta meta) {
        try {
            this.dbOptions.setFastSnapshot(false);
            this.kvStore.onSnapshotLoad(snapshotPath, meta);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
