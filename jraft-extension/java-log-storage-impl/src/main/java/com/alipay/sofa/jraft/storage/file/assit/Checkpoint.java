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
package com.alipay.sofa.jraft.storage.file.assit;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;

import com.alipay.sofa.jraft.entity.LocalFileMetaOutter.LocalFileMeta;
import com.alipay.sofa.jraft.storage.io.ProtoBufFile;
import com.google.protobuf.ZeroByteStringHelper;

/**
 * Abstract checkpoint file
 * @author boyan(boyan@antfin.com)
 */
public abstract class Checkpoint {

    private final String path;

    public Checkpoint(final String path) {
        super();
        this.path = path;
    }

    /**
     * Encode metadata
     */
    public abstract byte[] encode();

    /**
     * Decode file data
     */
    public abstract boolean decode(final byte[] bs);

    public synchronized boolean save() throws IOException {
        final ProtoBufFile file = new ProtoBufFile(this.path);
        final byte[] data = this.encode();
        final LocalFileMeta meta = LocalFileMeta.newBuilder() //
            .setUserMeta(ZeroByteStringHelper.wrap(data)) //
            .build();
        return file.save(meta, true);
    }

    public void load() throws IOException {
        final ProtoBufFile file = new ProtoBufFile(this.path);
        final LocalFileMeta meta = file.load();
        if (meta != null) {
            final byte[] data = meta.getUserMeta().toByteArray();
            decode(data);
        }
    }

    public void destroy() {
        FileUtils.deleteQuietly(new File(this.path));
    }

    public String getPath() {
        return this.path;
    }

}
