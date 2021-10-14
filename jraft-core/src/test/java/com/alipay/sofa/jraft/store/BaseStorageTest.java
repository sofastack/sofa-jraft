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
package com.alipay.sofa.jraft.store;

import com.alipay.sofa.jraft.option.StoreOptions;
import com.alipay.sofa.jraft.store.db.AbstractDB;
import com.alipay.sofa.jraft.store.factory.LogStoreFactory;
import com.alipay.sofa.jraft.store.file.FileHeader;
import com.alipay.sofa.jraft.store.file.index.IndexFile.IndexEntry;
import com.alipay.sofa.jraft.store.file.index.IndexType;
import com.alipay.sofa.jraft.store.service.FlushRequest;
import com.alipay.sofa.jraft.test.TestUtils;
import org.apache.commons.io.FileUtils;
import org.junit.After;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;

public class BaseStorageTest {
    protected String          path;
    protected StoreOptions    storeOptions = new StoreOptions();
    protected int             indexEntrySize;
    protected int             headerSize;
    protected int             indexFileSize;
    protected int             segmentFileSize;

    protected LogStoreFactory logStoreFactory;

    protected final byte      segmentIndex = IndexType.IndexSegment.getType();

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

    protected byte[] genData(final int size) {
        final byte[] bs = new byte[size];
        ThreadLocalRandom.current().nextBytes(bs);
        return bs;
    }

    public void waitForFlush(final AbstractDB db, final long expectedFlushPosition) {
        try {
            final FlushRequest request = FlushRequest.buildRequest(expectedFlushPosition);
            db.registerFlushRequest(request);
            request.getFuture().get();
        } catch (final Exception e) {
            e.printStackTrace();
        }
    }

}
