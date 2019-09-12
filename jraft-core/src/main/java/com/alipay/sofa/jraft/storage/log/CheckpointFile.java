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
package com.alipay.sofa.jraft.storage.log;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;

import com.alipay.sofa.jraft.entity.LocalFileMetaOutter.LocalFileMeta;
import com.alipay.sofa.jraft.storage.io.ProtoBufFile;
import com.alipay.sofa.jraft.util.Bits;
import com.google.protobuf.ZeroByteStringHelper;

/**
 * Segments checkpoint file.
 *
 * @author boyan(boyan@antfin.com)
 */
public class CheckpointFile {

    /**
     * firstLogIndex(8 B) + commitPos (4 B)
     */
    private static final int CHECKPOINT_METADATA_SIZE = 12;

    /**
     * Checkpoint metadata info.
     *
     * @author boyan(boyan@antfin.com)
     */
    public static final class Checkpoint {
        // Segment file start offset
        public final long firstLogIndex;
        // Segment file current commit position.
        public final int  committedPos;

        public Checkpoint(final long firstLogIndex, final int committedPos) {
            super();
            this.firstLogIndex = firstLogIndex;
            this.committedPos = committedPos;
        }

        @Override
        public String toString() {
            return "Checkpoint [firstLogIndex=" + this.firstLogIndex + ", committedPos=" + this.committedPos + "]";
        }
    }

    public void destroy() {
        FileUtils.deleteQuietly(new File(this.path));
    }

    public String getPath() {
        return this.path;
    }

    private final String path;

    public CheckpointFile(final String path) {
        super();
        this.path = path;
    }

    public synchronized boolean save(final Checkpoint checkpoint) throws IOException {
        final ProtoBufFile file = new ProtoBufFile(this.path);
        final byte[] data = new byte[CHECKPOINT_METADATA_SIZE];
        Bits.putLong(data, 0, checkpoint.firstLogIndex);
        Bits.putInt(data, 8, checkpoint.committedPos);

        final LocalFileMeta meta = LocalFileMeta.newBuilder() //
            .setUserMeta(ZeroByteStringHelper.wrap(data)) //
            .build();

        return file.save(meta, true);
    }

    public Checkpoint load() throws IOException {
        final ProtoBufFile file = new ProtoBufFile(this.path);
        final LocalFileMeta meta = file.load();
        if (meta != null) {
            final byte[] data = meta.getUserMeta().toByteArray();
            assert (data.length == CHECKPOINT_METADATA_SIZE);
            return new Checkpoint(Bits.getLong(data, 0), Bits.getInt(data, 8));
        }
        return null;
    }
}
