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
package com.alipay.sofa.jraft.core;

import com.alipay.sofa.jraft.option.NodeOptions;
import org.apache.commons.lang.StringUtils;

import com.alipay.sofa.jraft.JRaftServiceFactory;
import com.alipay.sofa.jraft.entity.codec.LogEntryCodecFactory;
import com.alipay.sofa.jraft.entity.codec.v2.LogEntryV2CodecFactory;
import com.alipay.sofa.jraft.option.RaftOptions;
import com.alipay.sofa.jraft.storage.LogStorage;
import com.alipay.sofa.jraft.storage.RaftMetaStorage;
import com.alipay.sofa.jraft.storage.SnapshotStorage;
import com.alipay.sofa.jraft.storage.impl.LocalRaftMetaStorage;
import com.alipay.sofa.jraft.storage.impl.RocksDBLogStorage;
import com.alipay.sofa.jraft.storage.snapshot.local.LocalSnapshotStorage;
import com.alipay.sofa.jraft.util.Requires;
import com.alipay.sofa.jraft.util.SPI;

/**
 * The default factory for JRaft services.
 * @author boyan(boyan@antfin.com)
 * @since 1.2.6
 *
 */
@SPI
public class DefaultJRaftServiceFactory implements JRaftServiceFactory {

    public static DefaultJRaftServiceFactory newInstance() {
        return new DefaultJRaftServiceFactory();
    }

    @Override
    public LogStorage createLogStorage(final String uri, final RaftOptions raftOptions) {
        Requires.requireTrue(StringUtils.isNotBlank(uri), "Blank log storage uri.");
        return new RocksDBLogStorage(uri, raftOptions);
    }

    @Override
    public SnapshotStorage createSnapshotStorage(final NodeOptions nodeOptions) {
        String uri = nodeOptions.getSnapshotUri();
        String tempUri = nodeOptions.getSnapshotTempUri();
        Requires.requireTrue(!StringUtils.isBlank(uri), "Blank snapshot storage uri.");
        return new LocalSnapshotStorage(uri, tempUri, nodeOptions.getRaftOptions());
    }

    @Override
    public RaftMetaStorage createRaftMetaStorage(final String uri, final RaftOptions raftOptions) {
        Requires.requireTrue(!StringUtils.isBlank(uri), "Blank raft meta storage uri.");
        return new LocalRaftMetaStorage(uri, raftOptions);
    }

    @Override
    public LogEntryCodecFactory createLogEntryCodecFactory() {
        return LogEntryV2CodecFactory.getInstance();
    }
}
