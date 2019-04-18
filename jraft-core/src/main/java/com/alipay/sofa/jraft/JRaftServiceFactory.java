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
package com.alipay.sofa.jraft;

import org.apache.commons.lang.StringUtils;

import com.alipay.sofa.jraft.entity.codec.DefaultLogEntryCodecFactory;
import com.alipay.sofa.jraft.entity.codec.LogEntryCodecFactory;
import com.alipay.sofa.jraft.option.NodeOptions;
import com.alipay.sofa.jraft.option.RaftOptions;
import com.alipay.sofa.jraft.storage.LogStorage;
import com.alipay.sofa.jraft.storage.RaftMetaStorage;
import com.alipay.sofa.jraft.storage.SnapshotStorage;
import com.alipay.sofa.jraft.storage.impl.LocalRaftMetaStorage;
import com.alipay.sofa.jraft.storage.impl.RocksDBLogStorage;
import com.alipay.sofa.jraft.storage.snapshot.local.LocalSnapshotStorage;
import com.alipay.sofa.jraft.util.Requires;

/**
 * Abstract factory to create services for SOFAJRaft.
 * @author boyan(boyan@antfin.com)
 * @since  1.2.6
 */
public interface JRaftServiceFactory {
    /**
     * Creates a raft log storage.
     * @param uri  The log storage uri from {@link NodeOptions#getSnapshotUri()}
     * @param raftOptions  the raft options.
     * @return storage to store raft log entires.
     */
    default LogStorage createLogStorage(final String uri, final RaftOptions raftOptions) {
        Requires.requireTrue(!StringUtils.isBlank(uri), "Blank log storage uri.");
        return new RocksDBLogStorage(uri, raftOptions);
    }

    /**
     * Creates a raft snapshot storage
     * @param uri  The snapshot storage uri from {@link NodeOptions#getSnapshotUri()}
     * @param raftOptions  the raft options.
     * @return storage to store state machine snapshot.
     */
    default SnapshotStorage createSnapshotStorage(final String uri, final RaftOptions raftOptions) {
        Requires.requireTrue(!StringUtils.isBlank(uri), "Blank snapshot storage uri.");
        return new LocalSnapshotStorage(uri, raftOptions);
    }

    /**
     * Creates a raft meta storage.
     * @param uri  The meta storage uri from {@link NodeOptions#getRaftMetaUri()}
     * @param raftOptions  the raft options.
     * @return meta storage to store raft meta info.
     */
    default RaftMetaStorage createRaftMetaStorage(final String uri, final RaftOptions raftOptions) {
        Requires.requireTrue(!StringUtils.isBlank(uri), "Blank raft meta storage uri.");
        return new LocalRaftMetaStorage(uri, raftOptions);
    }

    /**
     * Creates a log entry codec factory.
     * @return a codec factory to create encoder/decoder for raft log entry.
     */
    default LogEntryCodecFactory createLogEntryCodecFactory() {
        return DefaultLogEntryCodecFactory.getInstance();
    }

}
