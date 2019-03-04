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
package com.alipay.sofa.jraft.storage;

import org.apache.commons.lang.StringUtils;

import com.alipay.sofa.jraft.core.NodeMetrics;
import com.alipay.sofa.jraft.option.RaftOptions;
import com.alipay.sofa.jraft.storage.impl.LocalRaftMetaStorage;
import com.alipay.sofa.jraft.storage.impl.LogManagerImpl;
import com.alipay.sofa.jraft.storage.impl.RocksDBLogStorage;
import com.alipay.sofa.jraft.storage.snapshot.local.LocalSnapshotStorage;
import com.alipay.sofa.jraft.util.Requires;

/**
 * Storage factory.
 *
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Mar-28 11:13:26 AM
 */
public class StorageFactory {

    /**
     * Creates a log storage.
     */
    public static LogStorage createLogStorage(String uri, RaftOptions raftOptions) {
        Requires.requireTrue(!StringUtils.isBlank(uri), "Blank log storage uri.");
        return new RocksDBLogStorage(uri, raftOptions);
    }

    /**
     * Creates a log manager.
     */
    public static LogManager createLogManager() {
        return new LogManagerImpl();
    }

    /**
     * Creates a raft snapshot storage by uri.
     */
    public static SnapshotStorage createSnapshotStorage(String uri, RaftOptions raftOptions) {
        Requires.requireTrue(!StringUtils.isBlank(uri), "Blank snapshot storage uri.");
        return new LocalSnapshotStorage(uri, raftOptions);
    }

    /**
     * Creates a raft meta storage by uri.
     */
    public static RaftMetaStorage createRaftMetaStorage(String uri, RaftOptions raftOptions, NodeMetrics nodeMetrics) {
        Requires.requireTrue(!StringUtils.isBlank(uri), "Blank raft meta storage uri.");
        return new LocalRaftMetaStorage(uri, raftOptions, nodeMetrics);
    }
}
