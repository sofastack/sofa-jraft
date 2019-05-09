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
 * @author boyan(boyan@antfin.com)
 *
 */
public class CheckpointFile {

    private static final int CHECKPOINT_LENGTH = 12;

    /**
     * Checkpoint metadata info.
     * @author boyan(boyan@antfin.com)
     *
     */
    public static class Checkpoint {
        /// segment file start offset
        public final long startOffset;
        // segment file current wrote position.
        public final int  pos;

        public Checkpoint(final long startOffset, final int pos) {
            super();
            this.startOffset = startOffset;
            this.pos = pos;
        }

        @Override
        public String toString() {
            return "Checkpoint [startOffset=" + this.startOffset + ", pos=" + this.pos + "]";
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

    public boolean save(final Checkpoint checkpoint) throws IOException {
        ProtoBufFile file = new ProtoBufFile(this.path);
        byte[] data = new byte[CHECKPOINT_LENGTH];
        Bits.putLong(data, 0, checkpoint.startOffset);
        Bits.putInt(data, 8, checkpoint.pos);

        LocalFileMeta meta = LocalFileMeta.newBuilder() //
            .setUserMeta(ZeroByteStringHelper.wrap(data)) //
            .build();

        return file.save(meta, true);
    }

    public Checkpoint load() throws IOException {
        ProtoBufFile file = new ProtoBufFile(this.path);
        LocalFileMeta meta = file.load();
        if (meta != null) {
            byte[] data = meta.getUserMeta().toByteArray();
            assert (data.length == 12);
            return new Checkpoint(Bits.getLong(data, 0), Bits.getInt(data, 8));
        }
        return null;
    }

}
