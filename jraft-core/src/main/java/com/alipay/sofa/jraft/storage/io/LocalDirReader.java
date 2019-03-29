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
package com.alipay.sofa.jraft.storage.io;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.error.RetryAgainException;
import com.alipay.sofa.jraft.util.ByteBufferCollector;
import com.google.protobuf.Message;

/**
 * read a file data form local dir by fileName.
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-06 9:25:12 PM
 */
public class LocalDirReader implements FileReader {
    private static final Logger LOG = LoggerFactory.getLogger(LocalDirReader.class);

    private final String        path;

    public LocalDirReader(String path) {
        super();
        this.path = path;
    }

    @Override
    public String getPath() {
        return this.path;
    }

    @Override
    public int readFile(ByteBufferCollector buf, String fileName, long offset, long maxCount) throws IOException,
                                                                                             RetryAgainException {
        return this.readFileWithMeta(buf, fileName, null, offset, maxCount);
    }

    @SuppressWarnings("unused")
    protected int readFileWithMeta(ByteBufferCollector buf, String fileName, Message fileMeta, long offset,
                                   long maxCount) throws IOException, RetryAgainException {
        buf.expandIfNecessary();
        final String filePath = this.path + File.separator + fileName;
        final File file = new File(filePath);
        try (FileInputStream input = new FileInputStream(file); FileChannel fc = input.getChannel()) {
            int totalRead = 0;
            while (true) {
                final int nread = fc.read(buf.getBuffer(), offset);
                if (nread <= 0) {
                    return -1;
                }
                totalRead += nread;
                if (totalRead < maxCount) {
                    if (buf.hasRemaining()) {
                        return -1;
                    } else {
                        buf.expandAtMost((int) (maxCount - totalRead));
                        offset += nread;
                    }
                } else {
                    final long fsize = file.length();
                    if (fsize < 0) {
                        LOG.warn("Invalid file length {}", filePath);
                        return -1;
                    }
                    if (fsize == offset + nread) {
                        return -1;
                    } else {
                        return totalRead;
                    }
                }
            }
        }
    }
}
