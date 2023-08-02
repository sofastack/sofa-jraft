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
package com.alipay.sofa.jraft.storage.snapshot.local;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.option.RaftOptions;
import com.alipay.sofa.jraft.option.SnapshotCopierOptions;
import com.alipay.sofa.jraft.storage.SnapshotStorage;
import com.alipay.sofa.jraft.storage.SnapshotThrottle;
import com.alipay.sofa.jraft.storage.snapshot.Snapshot;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotCopier;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotReader;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotWriter;
import com.alipay.sofa.jraft.util.Endpoint;
import com.alipay.sofa.jraft.util.Requires;
import com.alipay.sofa.jraft.util.Utils;

/**
 * Snapshot storage based on local file storage.
 *
 * @author boyan (boyan@alibaba-inc.com)
 * <p>
 * 2018-Mar-13 2:11:30 PM
 */
public class LocalSnapshotStorage implements SnapshotStorage {

    private static final Logger                      LOG       = LoggerFactory.getLogger(LocalSnapshotStorage.class);

    private static final String                      TEMP_PATH = "temp";
    private final ConcurrentMap<Long, AtomicInteger> refMap    = new ConcurrentHashMap<>();
    private final String                             path;
    private final String                             tempPath;
    private Endpoint                                 addr;
    private boolean                                  filterBeforeCopyRemote;
    private long                                     lastSnapshotIndex;
    private final Lock                               lock;
    private final RaftOptions                        raftOptions;
    private SnapshotThrottle                         snapshotThrottle;

    @Override
    public void setSnapshotThrottle(SnapshotThrottle snapshotThrottle) {
        this.snapshotThrottle = snapshotThrottle;
    }

    public boolean hasServerAddr() {
        return this.addr != null;
    }

    public void setServerAddr(Endpoint addr) {
        this.addr = addr;
    }

    public LocalSnapshotStorage(String path, String tempPath, RaftOptions raftOptions) {
        super();
        this.path = path;
        if (StringUtils.isEmpty(tempPath)) {
            this.tempPath = buildTempPath(this.path);
        } else {
            File pathFile = new File(path);
            File tempPathFile = new File(tempPath);

            String pathAbsolutePath = pathFile.getAbsolutePath();
            String tempPathAbsolutePath = tempPathFile.getAbsolutePath();
            if (pathAbsolutePath.equals(tempPathAbsolutePath) || pathAbsolutePath.startsWith(tempPathAbsolutePath)) {
                this.tempPath = buildTempPath(this.path);
            } else {
                this.tempPath = tempPath;
            }
            LOG.info("The snapshot temp path is {}", this.tempPath);
        }
        this.lastSnapshotIndex = 0;
        this.raftOptions = raftOptions;
        this.lock = new ReentrantLock();
    }

    private String buildTempPath(String path) {
        return Paths.get(path, TEMP_PATH).toString();
    }

    public long getLastSnapshotIndex() {
        this.lock.lock();
        try {
            return this.lastSnapshotIndex;
        } finally {
            this.lock.unlock();
        }
    }

    @Override
    public boolean init(final Void v) {
        final File dir = new File(this.path);

        try {
            FileUtils.forceMkdir(dir);
        } catch (final IOException e) {
            LOG.error("Fail to create directory {}.", this.path, e);
            return false;
        }

        // delete temp snapshot
        if (!this.filterBeforeCopyRemote) {
            final File tempFile = new File(this.tempPath);
            if (tempFile.exists()) {
                try {
                    FileUtils.forceDelete(tempFile);
                } catch (final IOException e) {
                    LOG.error("Fail to delete temp snapshot path {}.", this.tempPath, e);
                    return false;
                }
            }
        }
        // delete old snapshot
        final List<Long> snapshots = new ArrayList<>();
        final File[] oldFiles = dir.listFiles();
        if (oldFiles != null) {
            for (final File sFile : oldFiles) {
                final String name = sFile.getName();
                if (!name.startsWith(Snapshot.JRAFT_SNAPSHOT_PREFIX)) {
                    continue;
                }
                final long index = Long.parseLong(name.substring(Snapshot.JRAFT_SNAPSHOT_PREFIX.length()));
                snapshots.add(index);
            }
        }

        // TODO: add snapshot watcher

        // get last_snapshot_index
        if (!snapshots.isEmpty()) {
            Collections.sort(snapshots);
            final int snapshotCount = snapshots.size();

            for (int i = 0; i < snapshotCount - 1; i++) {
                final long index = snapshots.get(i);
                final String snapshotPath = getSnapshotPath(index);
                if (!destroySnapshot(snapshotPath)) {
                    return false;
                }
            }
            this.lastSnapshotIndex = snapshots.get(snapshotCount - 1);
            ref(this.lastSnapshotIndex);
        }

        return true;
    }

    private String getSnapshotPath(final long index) {
        return this.path + File.separator + Snapshot.JRAFT_SNAPSHOT_PREFIX + index;
    }

    void ref(final long index) {
        final AtomicInteger refs = getRefs(index);
        refs.incrementAndGet();
    }

    private boolean destroySnapshot(final String path) {
        LOG.info("Deleting snapshot {}.", path);
        final File file = new File(path);
        try {
            FileUtils.deleteDirectory(file);
            return true;
        } catch (final IOException e) {
            LOG.error("Fail to destroy snapshot {}.", path, e);
            return false;
        }
    }

    void unref(final long index) {
        final AtomicInteger refs = getRefs(index);
        if (refs.decrementAndGet() == 0) {
            if (this.refMap.remove(index, refs)) {
                destroySnapshot(getSnapshotPath(index));
            }
        }
    }

    AtomicInteger getRefs(final long index) {
        AtomicInteger refs = this.refMap.get(index);
        if (refs == null) {
            refs = new AtomicInteger(0);
            final AtomicInteger eRefs = this.refMap.putIfAbsent(index, refs);
            if (eRefs != null) {
                refs = eRefs;
            }
        }
        return refs;
    }

    void close(final LocalSnapshotWriter writer, final boolean keepDataOnError) throws IOException {
        int ret = writer.getCode();
        IOException ioe = null;

        // noinspection ConstantConditions
        do {
            if (ret != 0) {
                break;
            }
            try {
                if (!writer.sync()) {
                    ret = RaftError.EIO.getNumber();
                    break;
                }
            } catch (final IOException e) {
                LOG.error("Fail to sync writer {}.", writer.getPath(), e);
                ret = RaftError.EIO.getNumber();
                ioe = e;
                break;
            }
            final long oldIndex = getLastSnapshotIndex();
            final long newIndex = writer.getSnapshotIndex();
            if (oldIndex == newIndex) {
                ret = RaftError.EEXISTS.getNumber();
                break;
            }
            // rename temp to new
            final String newPath = getSnapshotPath(newIndex);

            if (!destroySnapshot(newPath)) {
                LOG.warn("Delete new snapshot path failed, path is {}.", newPath);
                ret = RaftError.EIO.getNumber();
                ioe = new IOException("Fail to delete new snapshot path: " + newPath);
                break;
            }
            LOG.info("Renaming {} to {}.", this.tempPath, newPath);
            if (!Utils.atomicMoveFile(new File(this.tempPath), new File(newPath), true)) {
                LOG.error("Renamed temp snapshot failed, from path {} to path {}.", this.tempPath, newPath);
                ret = RaftError.EIO.getNumber();
                ioe = new IOException("Fail to rename temp snapshot from: " + this.tempPath + " to: " + newPath);
                break;
            }
            ref(newIndex);
            this.lock.lock();
            try {
                Requires.requireTrue(oldIndex == this.lastSnapshotIndex);
                this.lastSnapshotIndex = newIndex;
            } finally {
                this.lock.unlock();
            }
            unref(oldIndex);
        } while (false);

        if (ret != 0) {
            LOG.warn("Close snapshot writer {} with exit code: {}.", writer.getPath(), ret);
            if (!keepDataOnError) {
                destroySnapshot(writer.getPath());
            }
        }

        if (ioe != null) {
            throw ioe;
        }

    }

    @Override
    public void shutdown() {
        // ignore
    }

    @Override
    public boolean setFilterBeforeCopyRemote() {
        this.filterBeforeCopyRemote = true;
        return true;
    }

    @Override
    public SnapshotWriter create() {
        return create(true);
    }

    public SnapshotWriter create(final boolean fromEmpty) {
        LocalSnapshotWriter writer = null;
        // noinspection ConstantConditions
        do {
            // delete temp
            // TODO: Notify watcher before deleting
            if (new File(this.tempPath).exists() && fromEmpty) {
                if (!destroySnapshot(this.tempPath)) {
                    break;
                }
            }
            writer = new LocalSnapshotWriter(this.tempPath, this, this.raftOptions);
            if (!writer.init(null)) {
                LOG.error("Fail to init snapshot writer.");
                writer = null;
                break;
            }
        } while (false);
        return writer;
    }

    @Override
    public SnapshotReader open() {
        long lsIndex = 0;
        this.lock.lock();
        try {
            if (this.lastSnapshotIndex != 0) {
                lsIndex = this.lastSnapshotIndex;
                ref(lsIndex);
            }
        } finally {
            this.lock.unlock();
        }
        if (lsIndex == 0) {
            LOG.warn("No data for snapshot reader {}.", this.path);
            return null;
        }
        final String snapshotPath = getSnapshotPath(lsIndex);
        final SnapshotReader reader = new LocalSnapshotReader(this, this.snapshotThrottle, this.addr, this.raftOptions,
            snapshotPath);
        if (!reader.init(null)) {
            LOG.error("Fail to init reader for path {}.", snapshotPath);
            unref(lsIndex);
            return null;
        }
        return reader;
    }

    @Override
    public SnapshotReader copyFrom(final String uri, final SnapshotCopierOptions opts) {
        final SnapshotCopier copier = startToCopyFrom(uri, opts);
        if (copier == null) {
            return null;
        }
        try {
            copier.join();
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.error("Join on snapshot copier was interrupted.");
            return null;
        }
        final SnapshotReader reader = copier.getReader();
        Utils.closeQuietly(copier);
        return reader;
    }

    @Override
    public SnapshotCopier startToCopyFrom(final String uri, final SnapshotCopierOptions opts) {
        final LocalSnapshotCopier copier = new LocalSnapshotCopier();
        copier.setStorage(this);
        copier.setSnapshotThrottle(this.snapshotThrottle);
        copier.setFilterBeforeCopyRemote(this.filterBeforeCopyRemote);
        if (!copier.init(uri, opts)) {
            LOG.error("Fail to init copier to {}.", uri);
            return null;
        }
        copier.start();
        return copier;
    }

}
