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

import com.alipay.sofa.jraft.entity.codec.LogEntryCodecFactory;
import com.alipay.sofa.jraft.option.NodeOptions;
import com.alipay.sofa.jraft.option.RaftOptions;
import com.alipay.sofa.jraft.storage.LogStorage;
import com.alipay.sofa.jraft.storage.RaftMetaStorage;
import com.alipay.sofa.jraft.storage.SnapshotStorage;

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
     * @return storage to store raft log entries.
     */
    LogStorage createLogStorage(final String uri, final RaftOptions raftOptions);

    /**
     * Creates a raft snapshot storage
     * @param nodeOptions  the node options.
     * @return storage to store state machine snapshot.
     */
    SnapshotStorage createSnapshotStorage(final NodeOptions nodeOptions);

    /**
     * Creates a raft meta storage.
     * @param uri  The meta storage uri from {@link NodeOptions#getRaftMetaUri()}
     * @param raftOptions  the raft options.
     * @return meta storage to store raft meta info.
     */
    RaftMetaStorage createRaftMetaStorage(final String uri, final RaftOptions raftOptions);

    /**
     * Creates a log entry codec factory.
     * @return a codec factory to create encoder/decoder for raft log entry.
     */
    LogEntryCodecFactory createLogEntryCodecFactory();
}
