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

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.alipay.sofa.jraft.entity.LocalFileMetaOutter;
import com.alipay.sofa.jraft.entity.RaftOutter;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotWriter;
import com.google.protobuf.Message;

/**
 *
 * @author jiachun.fjc
 */
public class TestSnapshotWriter extends SnapshotWriter {

    public final Map<String, LocalFileMetaOutter.LocalFileMeta> metaTable = new ConcurrentHashMap<>();
    public final String                                         path;

    public TestSnapshotWriter(String path) {
        this.path = path;
    }

    @Override
    public boolean saveMeta(RaftOutter.SnapshotMeta meta) {
        return false;
    }

    @Override
    public boolean addFile(String fileName, Message fileMeta) {
        final LocalFileMetaOutter.LocalFileMeta.Builder metaBuilder = LocalFileMetaOutter.LocalFileMeta.newBuilder();
        if (fileMeta != null) {
            metaBuilder.mergeFrom(fileMeta);
        }
        final LocalFileMetaOutter.LocalFileMeta meta = metaBuilder.build();
        return this.metaTable.putIfAbsent(fileName, meta) == null;
    }

    @Override
    public boolean removeFile(String fileName) {
        return false;
    }

    @Override
    public void close(boolean keepDataOnError) throws IOException {

    }

    @Override
    public boolean init(Void opts) {
        return false;
    }

    @Override
    public void shutdown() {

    }

    @Override
    public String getPath() {
        return this.path;
    }

    @Override
    public Set<String> listFiles() {
        return null;
    }

    @Override
    public Message getFileMeta(String fileName) {
        return this.metaTable.get(fileName);
    }

    @Override
    public void close() throws IOException {

    }
}
