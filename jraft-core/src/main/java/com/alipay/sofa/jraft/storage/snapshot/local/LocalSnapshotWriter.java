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
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.entity.LocalFileMetaOutter.LocalFileMeta;
import com.alipay.sofa.jraft.entity.LocalFileMetaOutter.LocalFileMeta.Builder;
import com.alipay.sofa.jraft.entity.RaftOutter.SnapshotMeta;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.option.RaftOptions;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotWriter;
import com.alipay.sofa.jraft.util.Utils;
import com.google.protobuf.Message;

/**
 * Snapshot writer to write snapshot into local file system.
 *
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-08 11:51:43 AM
 */
public class LocalSnapshotWriter extends SnapshotWriter {

    private static final Logger          LOG = LoggerFactory.getLogger(LocalSnapshotWriter.class);

    private final LocalSnapshotMetaTable metaTable;
    private final String                 path;
    private final LocalSnapshotStorage   snapshotStorage;

    public LocalSnapshotWriter(String path, LocalSnapshotStorage snapshotStorage, RaftOptions raftOptions) {
        super();
        this.snapshotStorage = snapshotStorage;
        this.path = path;
        this.metaTable = new LocalSnapshotMetaTable(raftOptions);
    }

    @Override
    public boolean init(final Void v) {
        final File dir = new File(this.path);
        try {
            FileUtils.forceMkdir(dir);
        } catch (final IOException e) {
            LOG.error("Fail to create directory {}.", this.path, e);
            setError(RaftError.EIO, "Fail to create directory  %s", this.path);
            return false;
        }
        final String metaPath = path + File.separator + JRAFT_SNAPSHOT_META_FILE;
        final File metaFile = new File(metaPath);
        try {
            if (metaFile.exists()) {
                return metaTable.loadFromFile(metaPath);
            }
        } catch (final IOException e) {
            LOG.error("Fail to load snapshot meta from {}.", metaPath, e);
            setError(RaftError.EIO, "Fail to load snapshot meta from %s", metaPath);
            return false;
        }
        return true;
    }

    public long getSnapshotIndex() {
        return this.metaTable.hasMeta() ? this.metaTable.getMeta().getLastIncludedIndex() : 0;
    }

    @Override
    public void shutdown() {
        Utils.closeQuietly(this);
    }

    @Override
    public void close() throws IOException {
        close(false);
    }

    @Override
    public void close(final boolean keepDataOnError) throws IOException {
        this.snapshotStorage.close(this, keepDataOnError);
    }

    @Override
    public boolean saveMeta(final SnapshotMeta meta) {
        this.metaTable.setMeta(meta);
        return true;
    }

    public boolean sync() throws IOException {
        return this.metaTable.saveToFile(this.path + File.separator + JRAFT_SNAPSHOT_META_FILE);
    }

    @Override
    public boolean addFile(final String fileName, final Message fileMeta) {
        final Builder metaBuilder = LocalFileMeta.newBuilder();
        if (fileMeta != null) {
            metaBuilder.mergeFrom(fileMeta);
        }
        final LocalFileMeta meta = metaBuilder.build();
        return this.metaTable.addFile(fileName, meta);
    }

    @Override
    public boolean removeFile(final String fileName) {
        return this.metaTable.removeFile(fileName);
    }

    @Override
    public String getPath() {
        return this.path;
    }

    @Override
    public Set<String> listFiles() {
        return this.metaTable.listFiles();
    }

    @Override
    public Message getFileMeta(final String fileName) {
        return this.metaTable.getFileMeta(fileName);
    }
}
