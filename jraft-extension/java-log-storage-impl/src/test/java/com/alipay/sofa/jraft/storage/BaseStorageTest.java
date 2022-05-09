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

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.junit.After;

import com.alipay.sofa.jraft.conf.ConfigurationManager;
import com.alipay.sofa.jraft.entity.LogEntry;
import com.alipay.sofa.jraft.entity.codec.LogEntryCodecFactory;
import com.alipay.sofa.jraft.entity.codec.v2.LogEntryV2CodecFactory;
import com.alipay.sofa.jraft.option.LogStorageOptions;
import com.alipay.sofa.jraft.option.StoreOptions;
import com.alipay.sofa.jraft.storage.factory.LogStoreFactory;
import com.alipay.sofa.jraft.storage.file.FileHeader;
import com.alipay.sofa.jraft.storage.file.index.IndexFile.IndexEntry;
import com.alipay.sofa.jraft.storage.file.index.IndexType;
import com.alipay.sofa.jraft.test.TestUtils;

import static com.alipay.sofa.jraft.test.TestUtils.mockEntry;

public class BaseStorageTest {
    protected String               path;
    protected StoreOptions         storeOptions = new StoreOptions();
    protected int                  indexEntrySize;
    protected int                  headerSize;
    protected int                  indexFileSize;
    protected int                  segmentFileSize;
    protected ConfigurationManager confManager;
    protected LogEntryCodecFactory logEntryCodecFactory;

    protected LogStoreFactory      logStoreFactory;

    protected final byte           segmentIndex = IndexType.IndexSegment.getType();

    public void setup() throws Exception {
        this.path = TestUtils.mkTempDir();
        indexEntrySize = IndexEntry.INDEX_SIZE;
        headerSize = FileHeader.HEADER_SIZE;
        indexFileSize = headerSize + 10 * indexEntrySize;
        this.segmentFileSize = 300;

        FileUtils.forceMkdir(new File(this.path));

        storeOptions.setIndexFileSize(indexFileSize);
        storeOptions.setSegmentFileSize(segmentFileSize);
        storeOptions.setConfFileSize(segmentFileSize);
        this.logStoreFactory = new LogStoreFactory(storeOptions);

        this.confManager = new ConfigurationManager();
        this.logEntryCodecFactory = LogEntryV2CodecFactory.getInstance();
    }

    @After
    public void teardown() throws Exception {
        try {
            FileUtils.deleteDirectory(new File(this.path));
        } catch (final IOException ignored) {
        }
    }

    protected String writeData() throws IOException {
        File file = new File(this.path + File.separator + "data");
        String data = "jraft is great!";
        FileUtils.writeStringToFile(file, data);
        return data;
    }

    protected byte[] genData(final int index, final int term, int size) {
        final LogEntry entry = mockEntry(index, term, size - 14);
        final byte[] data = LogEntryV2CodecFactory.getInstance().encoder().encode(entry);
        return data;
    }

    protected LogStorageOptions newLogStorageOptions() {
        final LogStorageOptions opts = new LogStorageOptions();
        opts.setConfigurationManager(this.confManager);
        opts.setLogEntryCodecFactory(this.logEntryCodecFactory);
        return opts;
    }
}
