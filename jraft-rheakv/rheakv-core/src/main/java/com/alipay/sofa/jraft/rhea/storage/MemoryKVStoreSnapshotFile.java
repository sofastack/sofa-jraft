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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

import com.alipay.sofa.jraft.rhea.errors.StorageException;
import com.alipay.sofa.jraft.rhea.metadata.Region;
import com.alipay.sofa.jraft.rhea.util.ByteArray;
import com.alipay.sofa.jraft.rhea.util.Pair;
import com.alipay.sofa.jraft.rhea.util.RegionHelper;
import com.alipay.sofa.jraft.rhea.util.concurrent.DistributedLock;
import com.alipay.sofa.jraft.util.Bits;

import static com.alipay.sofa.jraft.entity.LocalFileMetaOutter.LocalFileMeta;

/**
 *
 * @author jiachun.fjc
 */
public class MemoryKVStoreSnapshotFile extends AbstractKVStoreSnapshotFile {

    private final MemoryRawKVStore kvStore;

    MemoryKVStoreSnapshotFile(MemoryRawKVStore kvStore) {
        this.kvStore = kvStore;
    }

    @Override
    CompletableFuture<LocalFileMeta.Builder> doSnapshotSave(final String snapshotPath, final Region region,
                                                            final ExecutorService executor) throws Exception {
        this.kvStore.doSnapshotSave(this, snapshotPath, region);
        return CompletableFuture.completedFuture(writeMetadata(region));
    }

    @Override
    void doSnapshotLoad(final String snapshotPath, final LocalFileMeta meta, final Region region) throws Exception {
        final File file = new File(snapshotPath);
        if (!file.exists()) {
            throw new StorageException("Snapshot file [" + snapshotPath + "] not exists");
        }
        final Region snapshotRegion = readMetadata(meta, Region.class);
        if (!RegionHelper.isSameRange(region, snapshotRegion)) {
            throw new StorageException("Invalid snapshot region: " + snapshotRegion + ", current region is: " + region);
        }
        this.kvStore.doSnapshotLoad(this, snapshotPath);
    }

    <T> void writeToFile(final String rootPath, final String fileName, final Persistence<T> persist) throws Exception {
        final Path path = Paths.get(rootPath, fileName);
        try (final FileOutputStream out = new FileOutputStream(path.toFile());
                final BufferedOutputStream bufOutput = new BufferedOutputStream(out)) {
            final byte[] bytes = this.serializer.writeObject(persist);
            final byte[] lenBytes = new byte[4];
            Bits.putInt(lenBytes, 0, bytes.length);
            bufOutput.write(lenBytes);
            bufOutput.write(bytes);
            bufOutput.flush();
            out.getFD().sync();
        }
    }

    <T> T readFromFile(final String rootPath, final String fileName, final Class<T> clazz) throws Exception {
        final Path path = Paths.get(rootPath, fileName);
        final File file = path.toFile();
        if (!file.exists()) {
            throw new NoSuchFieldException(path.toString());
        }
        try (final FileInputStream in = new FileInputStream(file);
                final BufferedInputStream bufInput = new BufferedInputStream(in)) {
            final byte[] lenBytes = new byte[4];
            int read = bufInput.read(lenBytes);
            if (read != lenBytes.length) {
                throw new IOException("fail to read snapshot file length, expects " + lenBytes.length
                                      + " bytes, but read " + read);
            }
            final int len = Bits.getInt(lenBytes, 0);
            final byte[] bytes = new byte[len];
            read = bufInput.read(bytes);
            if (read != bytes.length) {
                throw new IOException("fail to read snapshot file, expects " + bytes.length + " bytes, but read "
                                      + read);
            }
            return this.serializer.readObject(bytes, clazz);
        }
    }

    static class Persistence<T> {

        private final T data;

        public Persistence(T data) {
            this.data = data;
        }

        public T data() {
            return data;
        }
    }

    /**
     * The data of sequences
     */
    static class SequenceDB extends Persistence<Map<ByteArray, Long>> {

        public SequenceDB(Map<ByteArray, Long> data) {
            super(data);
        }
    }

    /**
     * The data of fencing token keys
     */
    static class FencingKeyDB extends Persistence<Map<ByteArray, Long>> {

        public FencingKeyDB(Map<ByteArray, Long> data) {
            super(data);
        }
    }

    /**
     * The data of lock info
     */
    static class LockerDB extends Persistence<Map<ByteArray, DistributedLock.Owner>> {

        public LockerDB(Map<ByteArray, DistributedLock.Owner> data) {
            super(data);
        }
    }

    /**
     * The data will be cut into many small portions, each called a segment
     */
    static class Segment extends Persistence<List<Pair<byte[], byte[]>>> {

        public Segment(List<Pair<byte[], byte[]>> data) {
            super(data);
        }
    }

    /**
     * The 'tailIndex' records the largest segment number (the segment number starts from 0)
     */
    static class TailIndex extends Persistence<Integer> {

        public TailIndex(Integer data) {
            super(data);
        }
    }
}
