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

import java.util.Set;

import com.alipay.sofa.jraft.option.RaftOptions;
import com.alipay.sofa.jraft.storage.snapshot.Snapshot;
import com.google.protobuf.Message;

/**
 * Describe the Snapshot on another machine.
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-07 10:21:55 AM
 */
public class LocalSnapshot extends Snapshot {

    private final LocalSnapshotMetaTable metaTable;

    public LocalSnapshot(RaftOptions raftOptions) {
        this.metaTable = new LocalSnapshotMetaTable(raftOptions);
    }

    public LocalSnapshotMetaTable getMetaTable() {
        return this.metaTable;
    }

    @Override
    public String getPath() {
        throw new UnsupportedOperationException();
    }

    /**
     * List all the existing files in the Snapshot currently
     *
     * @return the existing file list
     */
    @Override
    public Set<String> listFiles() {
        return this.metaTable.listFiles();
    }

    @Override
    public Message getFileMeta(final String fileName) {
        return this.metaTable.getFileMeta(fileName);
    }
}
