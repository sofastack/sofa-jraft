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
package com.alipay.sofa.jraft.storage.Benchmark;

import org.junit.After;
import org.junit.Before;

import com.alipay.sofa.jraft.conf.ConfigurationManager;
import com.alipay.sofa.jraft.entity.codec.LogEntryCodecFactory;
import com.alipay.sofa.jraft.entity.codec.v2.LogEntryV2CodecFactory;
import com.alipay.sofa.jraft.storage.LogitLogStorage;
import com.alipay.sofa.jraft.storage.file.FileHeader;
import com.alipay.sofa.jraft.storage.file.index.IndexFile;
import com.alipay.sofa.jraft.option.LogStorageOptions;
import com.alipay.sofa.jraft.option.StoreOptions;
import com.alipay.sofa.jraft.storage.LogStorage;
import com.alipay.sofa.jraft.test.TestUtils;

/**
 * @author hzh (642256541@qq.com)
 */
public abstract class BaseLogStorageBenchmark {

    protected LogStorage         logStorage;
    protected String             path;
    protected StoreOptions       storeOptions;
    private ConfigurationManager confManager;
    private LogEntryCodecFactory logEntryCodecFactory;

    @Before
    public void setup() throws Exception {
        this.path = TestUtils.mkTempDir();
        this.storeOptions = new StoreOptions();
        this.confManager = new ConfigurationManager();
        this.logEntryCodecFactory = LogEntryV2CodecFactory.getInstance();
        this.logStorage = newLogStorage();

        final LogStorageOptions opts = newLogStorageOptions();

        this.logStorage.init(opts);
    }

    protected LogStorage newLogStorage() {

        final StoreOptions storeOptions = new StoreOptions();
        storeOptions.setSegmentFileSize(1024 * 1024 * 256);
        storeOptions.setConfFileSize(1024 * 1024 * 256);
        storeOptions.setIndexFileSize(FileHeader.HEADER_SIZE + 50000 * IndexFile.IndexEntry.INDEX_SIZE);
        return new LogitLogStorage(this.path, storeOptions);
    }

    protected LogStorageOptions newLogStorageOptions() {
        final LogStorageOptions opts = new LogStorageOptions();
        opts.setConfigurationManager(this.confManager);
        opts.setLogEntryCodecFactory(this.logEntryCodecFactory);
        return opts;
    }

    @After
    public void teardown() throws Exception {
        this.logStorage.shutdown();
    }

}
