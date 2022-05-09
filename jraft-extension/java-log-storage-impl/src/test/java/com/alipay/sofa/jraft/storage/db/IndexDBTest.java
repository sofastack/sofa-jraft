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
package com.alipay.sofa.jraft.storage.db;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.alipay.sofa.jraft.storage.BaseStorageTest;
import com.alipay.sofa.jraft.storage.file.index.IndexFile;
import com.alipay.sofa.jraft.storage.file.index.IndexType;
import com.alipay.sofa.jraft.storage.log.AbortFile;
import com.alipay.sofa.jraft.util.Pair;

import static org.junit.Assert.assertEquals;

/**
 * @author hzh (642256541@qq.com)
 */
public class IndexDBTest extends BaseStorageTest {
    private IndexDB   indexDB;
    private String    indexStorePath;
    private AbortFile abortFile;

    @Before
    @Override
    public void setup() throws Exception {
        super.setup();
        this.indexStorePath = this.path + File.separator + "index";
        this.abortFile = new AbortFile(this.indexStorePath + File.separator + "Abort");
        FileUtils.forceMkdir(new File(this.indexStorePath));
        this.init();
    }

    public void init() {
        this.indexDB = new IndexDB(this.indexStorePath);
        this.indexDB.init(this.logStoreFactory);
    }

    @After
    public void teardown() throws Exception {
        this.indexDB.shutdown();
        super.teardown();
    }

    /**
     * When call testAppendIndex
     * The FileManager's file state is :
     *
     * fileId   fileFromOffset    firstLogIndex  lastLogIndex  fileLastOffset         wrotePosition
     * 0        0                 0              9             26 + 100 = 126         26 + 100
     * 1        26 + 100          10             15            26 + 26 + 160 = 212    26 + 60
     */

    /**
     * Test for Service , which enables auto flush
     */
    @Test
    public void testAppendIndex() throws Exception {
        this.indexDB.startServiceManager();
        {
            // Append 10 index to first file , and come to the file end (size:130)
            for (int i = 0; i < 10; i++) {
                this.indexDB.appendIndexAsync(i, i, IndexType.IndexSegment);
            }
            // Write 5 index to second file , wrotePosition = 30 + 50
            Pair<Integer, Long> posPair = null;
            for (int i = 10; i <= 15; i++) {
                posPair = this.indexDB.appendIndexAsync(i, i, IndexType.IndexSegment);
            }

            this.indexDB.waitForFlush(posPair.getSecond(), 100);

            assertEquals(this.indexDB.lookupIndex(15).getOffset(), 5);
            assertEquals(this.indexDB.getFlushedPosition(), 212);
        }
    }

    @Test
    public void testLookupFirstLogPosFromLogIndex() {
        this.indexDB.startServiceManager();
        {
            this.indexDB.appendIndexAsync(1, 1, IndexType.IndexSegment);
            this.indexDB.appendIndexAsync(2, 2, IndexType.IndexSegment);
            final Pair<Integer, Long> posPair = this.indexDB.appendIndexAsync(3, 3, IndexType.IndexConf);
            this.indexDB.waitForFlush(posPair.getSecond(), 100);
        }

        final Pair<Integer, Integer> posPair = this.indexDB.lookupFirstLogPosFromLogIndex(1);
        final int firstSegmentPos = posPair.getFirst();
        final int firstConfPos = posPair.getSecond();
        assertEquals(firstSegmentPos, 1);
        assertEquals(firstConfPos, 3);
    }

    @Test
    public void testLookupLastLogIndexAndPosFromTail() {
        this.indexDB.startServiceManager();
        {
            this.indexDB.appendIndexAsync(1, 1, IndexType.IndexSegment);
            this.indexDB.appendIndexAsync(2, 2, IndexType.IndexSegment);
            final Pair<Integer, Long> posPair = this.indexDB.appendIndexAsync(3, 3, IndexType.IndexConf);
            this.indexDB.appendIndexAsync(4, 4, IndexType.IndexSegment);
            this.indexDB.waitForFlush(posPair.getSecond(), 100);
        }
        final Pair<IndexFile.IndexEntry, IndexFile.IndexEntry> indexPair = this.indexDB
            .lookupLastLogIndexAndPosFromTail();
        final IndexFile.IndexEntry lastSegmentIndex = indexPair.getFirst();
        final IndexFile.IndexEntry lastConfIndex = indexPair.getSecond();
        assert (lastSegmentIndex.getLogIndex() == 4);
        assert (lastConfIndex.getLogIndex() == 3);
    }

    @Test
    public void testRecoverNormal() throws Exception {
        this.testAppendIndex();
        {
            // Try to shutdown and recover , check flush position
            this.indexDB.shutdown();
            this.init();
            this.indexDB.recover();
            assertEquals(this.indexDB.getFlushedPosition(), 212);
        }
    }

    @Test
    public void testRecoverAbNormal() throws Exception {
        this.testAppendIndex();
        {
            // Try to shutdown and recover , check flush position
            this.indexDB.shutdown();
            this.init();
            // Create abort file
            this.abortFile.create();
            this.indexDB.recover();
            assertEquals(this.indexDB.getFlushedPosition(), 212);
        }
    }

}