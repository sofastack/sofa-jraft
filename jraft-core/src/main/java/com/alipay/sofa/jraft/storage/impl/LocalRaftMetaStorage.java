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
package com.alipay.sofa.jraft.storage.impl;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.core.NodeImpl;
import com.alipay.sofa.jraft.core.NodeMetrics;
import com.alipay.sofa.jraft.entity.EnumOutter.ErrorType;
import com.alipay.sofa.jraft.entity.LocalStorageOutter.StablePBMeta;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.error.RaftException;
import com.alipay.sofa.jraft.option.RaftMetaStorageOptions;
import com.alipay.sofa.jraft.option.RaftOptions;
import com.alipay.sofa.jraft.storage.RaftMetaStorage;
import com.alipay.sofa.jraft.storage.io.ProtoBufFile;
import com.alipay.sofa.jraft.util.Utils;

/**
 * Raft meta storage.
 *
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Mar-26 7:30:36 PM
 */
public class LocalRaftMetaStorage implements RaftMetaStorage {

    private static final Logger LOG       = LoggerFactory.getLogger(LocalRaftMetaStorage.class);
    private static final String RAFT_META = "raft_meta";

    private boolean             isInited;
    private final String        path;
    private long                term;
    /** blank votedFor information*/
    private PeerId              votedFor  = new PeerId();
    private final RaftOptions   raftOptions;
    private final NodeMetrics   nodeMetrics;
    private NodeImpl            node;

    public LocalRaftMetaStorage(String path, RaftOptions raftOptions, NodeMetrics nodeMetrics) {
        super();
        this.path = path;
        this.raftOptions = raftOptions;
        this.nodeMetrics = nodeMetrics;
    }

    @Override
    public synchronized boolean init(RaftMetaStorageOptions opts) {
        if (this.isInited) {
            LOG.warn("Raft meta storage is already inited.");
            return true;
        }
        this.node = opts.getNode();
        try {
            FileUtils.forceMkdir(new File(this.path));
        } catch (final IOException e) {
            LOG.error("Fail to mkdir {}", this.path);
            return false;
        }
        if (load()) {
            this.isInited = true;
            return true;
        } else {
            return false;
        }
    }

    private boolean load() {
        final ProtoBufFile pbFile = newPbFile();
        try {
            final StablePBMeta meta = pbFile.load();
            if (meta != null) {
                this.term = meta.getTerm();
                return this.votedFor.parse(meta.getVotedfor());
            }
            return true;
        } catch (final FileNotFoundException e) {
            return true;
        } catch (final IOException e) {
            LOG.error("Fail to load raft meta storage", e);
            return false;
        }
    }

    private ProtoBufFile newPbFile() {
        final String mPath = this.path + File.separator + RAFT_META;
        return new ProtoBufFile(mPath);
    }

    private boolean save() {
        final long start = Utils.monotonicMs();
        final StablePBMeta meta = StablePBMeta.newBuilder(). //
            setTerm(this.term). //
            setVotedfor(this.votedFor.toString()). //
            build();
        final ProtoBufFile pbFile = newPbFile();
        try {
            if (!pbFile.save(meta, this.raftOptions.isSyncMeta())) {
                this.node.onError(new RaftException(ErrorType.ERROR_TYPE_META, RaftError.EIO,
                    "Fail to save raft meta, path=%s", this.path));
                return false;
            }
            return true;
        } catch (final IOException e) {
            LOG.error("Fail to save raft meta", e);
            this.node.onError(new RaftException(ErrorType.ERROR_TYPE_META, RaftError.EIO,
                "Fail to save raft meta, path=%s", this.path));
            return false;
        } finally {
            final long cost = Utils.monotonicMs() - start;
            if (this.nodeMetrics != null) {
                this.nodeMetrics.recordLatency("save-raft-meta", cost);
            }
            LOG.info("Save raft meta, path={}, term={}, votedFor={}, cost time={} ms", this.path, this.term,
                this.votedFor, cost);
        }
    }

    @Override
    public void shutdown() {
        save();
    }

    @Override
    public boolean setTerm(long term) {
        if (this.isInited) {
            this.term = term;
            return save();
        } else {
            LOG.warn("LocalRaftMetaStorage not init(), path={}", this.path);
            return false;
        }
    }

    @Override
    public long getTerm() {
        if (this.isInited) {
            return this.term;
        } else {
            LOG.warn("LocalRaftMetaStorage not init(), path={}", this.path);
            return -1L;
        }
    }

    @Override
    public boolean setVotedFor(PeerId peerId) {
        if (this.isInited) {
            this.votedFor = peerId;
            return save();
        } else {
            LOG.warn("LocalRaftMetaStorage not init(), path={}", this.path);
            return false;
        }
    }

    @Override
    public PeerId getVotedFor() {
        if (this.isInited) {
            return this.votedFor;
        } else {
            LOG.warn("LocalRaftMetaStorage not init(), path={}", this.path);
            return null;
        }
    }

    @Override
    public boolean setTermAndVotedFor(long term, PeerId peerId) {
        if (this.isInited) {
            this.votedFor = peerId;
            this.term = term;
            return save();
        } else {
            LOG.warn("LocalRaftMetaStorage not init(), path={}", this.path);
            return false;
        }
    }

    @Override
    public String toString() {
        return "RaftMetaStorageImpl [path=" + this.path + ", term=" + this.term + ", votedFor=" + this.votedFor + "]";
    }
}
