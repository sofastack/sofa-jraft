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
package com.alipay.sofa.jraft.rhea.benchmark.raw;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.zip.Checksum;

import org.apache.commons.io.FileUtils;

import com.alipay.sofa.jraft.rhea.metadata.Region;
import com.alipay.sofa.jraft.rhea.storage.KVEntry;
import com.alipay.sofa.jraft.rhea.storage.KVStoreAccessHelper;
import com.alipay.sofa.jraft.rhea.storage.RocksRawKVStore;
import com.alipay.sofa.jraft.rhea.util.Lists;
import com.alipay.sofa.jraft.rhea.util.ZipUtil;
import com.alipay.sofa.jraft.util.BytesUtil;
import com.alipay.sofa.jraft.util.CRC64;

import static com.alipay.sofa.jraft.entity.LocalFileMetaOutter.LocalFileMeta;
import static com.alipay.sofa.jraft.rhea.benchmark.BenchmarkUtil.KEY_COUNT;
import static com.alipay.sofa.jraft.rhea.benchmark.BenchmarkUtil.VALUE_BYTES;

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
        for (int i = 0; i < KEY_COUNT * 10; i++) {
            final byte[] key = BytesUtil.writeUtf8("benchmark_" + i);
            batch.add(new KVEntry(key, VALUE_BYTES));
            if (batch.size() >= 100) {
                this.kvStore.put(batch, null);
                batch.clear();
            }
        }
    }

    /**
        -----------------------------------------------
        db size = 10000000
        slow save snapshot time cost: 2552
        slow compressed file size: 41915298
        slow compress time cost: 9173
        slow load snapshot time cost: 5119
        -----------------------------------------------
        db size = 10000000
        fast save snapshot time cost: 524
        fast compressed file size: 41920248
        fast compress time cost: 8807
        fast load snapshot time cost: 3090
        -----------------------------------------------
        db size = 10000000
        sst save snapshot time cost: 4296
        sst compressed file size: 10741032
        sst compress time cost: 2005
        sst load snapshot time cost: 593
        -----------------------------------------------
        db size = 10000000
        slow save snapshot time cost: 2248
        slow compressed file size: 41918551
        slow compress time cost: 8705
        slow load snapshot time cost: 4485
        -----------------------------------------------
        db size = 10000000
        fast save snapshot time cost: 508
        fast compressed file size: 41914702
        fast compress time cost: 8736
        fast load snapshot time cost: 3047
        -----------------------------------------------
        db size = 10000000
        sst save snapshot time cost: 4206
        sst compressed file size: 10741032
        sst compress time cost: 1950
        sst load snapshot time cost: 599
        -----------------------------------------------
        db size = 10000000
        slow save snapshot time cost: 2327
        slow compressed file size: 41916640
        slow compress time cost: 8643
        slow load snapshot time cost: 4590
        -----------------------------------------------
        db size = 10000000
        fast save snapshot time cost: 511
        fast compressed file size: 41914533
        fast compress time cost: 8704
        fast load snapshot time cost: 3013
        -----------------------------------------------
        db size = 10000000
        sst save snapshot time cost: 4253
        sst compressed file size: 10741032
        sst compress time cost: 1947
        sst load snapshot time cost: 590
        -----------------------------------------------
     */

    public static void main(String[] args) throws IOException {
        for (int i = 0; i < 3; i++) {
            SnapshotBenchmark snapshot = new SnapshotBenchmark();
            snapshot.setup();
            snapshot.snapshot(false, false);
            snapshot.tearDown();

            snapshot = new SnapshotBenchmark();
            snapshot.setup();
            snapshot.snapshot(false, true);
            snapshot.tearDown();

            snapshot = new SnapshotBenchmark();
            snapshot.setup();
            snapshot.snapshot(true, true);
            snapshot.tearDown();
        }
    }

    public void snapshot(final boolean isSstSnapshot, final boolean isFastSnapshot) throws IOException {
        final File backupDir = new File("backup");
        if (backupDir.exists()) {
            FileUtils.deleteDirectory(backupDir);
        }
        FileUtils.forceMkdir(backupDir);

        final LocalFileMeta meta = doSnapshotSave(backupDir.getAbsolutePath(), isSstSnapshot, isFastSnapshot);

        this.kvStore.shutdown();
        FileUtils.deleteDirectory(new File(this.tempPath));
        FileUtils.forceMkdir(new File(this.tempPath));
        this.kvStore = new RocksRawKVStore();
        this.kvStore.init(this.dbOptions);

        final String name;
        if (isSstSnapshot) {
            name = "sst";
        } else {
            if (isFastSnapshot) {
                name = "fast";
            } else {
                name = "slow";
            }
        }

        final long decompressStart = System.nanoTime();
        final String sourceFile = Paths.get(backupDir.getAbsolutePath(), SNAPSHOT_ARCHIVE).toString();
        ZipUtil.decompress(sourceFile, backupDir.getAbsolutePath(), new CRC64());
        System.out.println(name + " decompress time cost: "
                           + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - decompressStart));

        final long loadStart = System.nanoTime();
        doSnapshotLoad(backupDir.getAbsolutePath(), meta, isFastSnapshot);

        System.out.println(name + " load snapshot time cost: "
                           + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - loadStart));
        FileUtils.deleteDirectory(backupDir);
    }

    private LocalFileMeta doSnapshotSave(final String path, final boolean isSstSnapshot, final boolean isFastSnapshot) {
        final String snapshotPath = Paths.get(path, SNAPSHOT_DIR).toString();
        try {
            final long saveStart = System.nanoTime();
            LocalFileMeta.Builder metaBuilder = null;
            if (isSstSnapshot) {
                doSstSnapshotSave(snapshotPath);
            } else {
                if (isFastSnapshot) {
                    metaBuilder = doFastSnapshotSave(snapshotPath);
                } else {
                    metaBuilder = doSlowSnapshotSave(snapshotPath);
                }
            }
            metaBuilder = metaBuilder == null ? LocalFileMeta.newBuilder() : metaBuilder;
            final String name;
            if (isSstSnapshot) {
                name = "sst";
            } else {
                if (isFastSnapshot) {
                    name = "fast";
                } else {
                    name = "slow";
                }
            }
            System.out.println(name + " save snapshot time cost: "
                               + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - saveStart));
            final long compressStart = System.nanoTime();
            doCompressSnapshot(path, metaBuilder);
            System.out.println(name + " compressed file size: "
                               + FileUtils.sizeOf(Paths.get(path, SNAPSHOT_ARCHIVE).toFile()));
            System.out.println(name + " compress time cost: "
                               + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - compressStart));
            return metaBuilder.build();
        } catch (final Throwable t) {
            t.printStackTrace();
        }
        return null;
    }

    public boolean doSnapshotLoad(final String path, final LocalFileMeta meta, final boolean isFastSnapshot) {
        final String snapshotPath = Paths.get(path, SNAPSHOT_DIR).toString();
        try {
            if (isFastSnapshot) {
                doFastSnapshotLoad(snapshotPath);
            } else {
                doSlowSnapshotLoad(snapshotPath, meta);
            }
            return true;
        } catch (final Throwable t) {
            t.printStackTrace();
            return false;
        }
    }

    private LocalFileMeta.Builder doFastSnapshotSave(final String snapshotPath) throws Exception {
        this.dbOptions.setFastSnapshot(true);
        final Region region = new Region();
        return KVStoreAccessHelper.saveSnapshot(this.kvStore, snapshotPath, region);
    }

    private LocalFileMeta.Builder doSlowSnapshotSave(final String snapshotPath) throws Exception {
        this.dbOptions.setFastSnapshot(false);
        final Region region = new Region();
        return KVStoreAccessHelper.saveSnapshot(this.kvStore, snapshotPath, region);
    }

    private void doSstSnapshotSave(final String snapshotPath) throws Exception {
        FileUtils.forceMkdir(new File(snapshotPath));
        final List<Region> regions = Lists.newArrayList();
        for (int i = 0; i < 10; i++) {
            final Region r = new Region();
            r.setId(i);
            r.setStartKey(BytesUtil.writeUtf8("benchmark_" + i));
            if (i < 9) {
                r.setEndKey(BytesUtil.writeUtf8("benchmark_" + (i + 1)));
            }
            regions.add(r);
        }
        final ExecutorService executor = Executors.newFixedThreadPool(10);
        final List<Future<?>> futures = Lists.newArrayList();
        for (final Region r : regions) {
            final Future<?> f = executor.submit(() -> {
                try {
                    KVStoreAccessHelper.saveSnapshot(this.kvStore, Paths.get(snapshotPath, String.valueOf(r.getId())).toString(), r);
                } catch (final Exception e) {
                    e.printStackTrace();
                }
            });
            futures.add(f);
        }
        for (final Future<?> f : futures) {
            try {
                f.get();
            } catch (final InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }
        executor.shutdownNow();
    }

    private void doCompressSnapshot(final String path, final LocalFileMeta.Builder metaBuilder) {
        final String outputFile = Paths.get(path, SNAPSHOT_ARCHIVE).toString();
        try {
            final Checksum checksum = new CRC64();
            ZipUtil.compress(path, SNAPSHOT_DIR, outputFile, checksum);
            metaBuilder.setChecksum(Long.toHexString(checksum.getValue()));
        } catch (final Throwable t) {
            t.printStackTrace();
        }
    }

    private void doFastSnapshotLoad(final String snapshotPath) {
        try {
            this.dbOptions.setFastSnapshot(true);
            final Region region = new Region();
            KVStoreAccessHelper.loadSnapshot(this.kvStore, snapshotPath, null, region);
        } catch (final Exception e) {
            e.printStackTrace();
        }
    }

    private void doSlowSnapshotLoad(final String snapshotPath, final LocalFileMeta meta) {
        try {
            this.dbOptions.setFastSnapshot(false);
            final Region region = new Region();
            KVStoreAccessHelper.loadSnapshot(this.kvStore, snapshotPath, meta, region);
        } catch (final Exception e) {
            e.printStackTrace();
        }
    }

    private void doSstSnapshotLoad(final String snapshotPath) {
        final List<Region> regions = Lists.newArrayList();
        for (int i = 0; i < 10; i++) {
            final Region r = new Region();
            r.setId(i);
            r.setStartKey(BytesUtil.writeUtf8("benchmark_" + i));
            if (i < 9) {
                r.setEndKey(BytesUtil.writeUtf8("benchmark_" + (i + 1)));
            }
            regions.add(r);
        }
        final ExecutorService executor = Executors.newFixedThreadPool(10);
        final List<Future<?>> futures = Lists.newArrayList();
        for (final Region r : regions) {
            final Future<?> f = executor.submit(() -> {
                try {
                    KVStoreAccessHelper.loadSnapshot(kvStore, Paths.get(snapshotPath, String.valueOf(r.getId())).toString(), null, r);
                } catch (final Exception e) {
                    e.printStackTrace();
                }
            });
            futures.add(f);
        }
        for (final Future<?> f : futures) {
            try {
                f.get();
            } catch (final InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }
        executor.shutdownNow();
    }
}
