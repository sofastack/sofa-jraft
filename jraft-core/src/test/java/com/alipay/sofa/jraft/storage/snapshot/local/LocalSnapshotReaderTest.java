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
package com.alipay.sofa.jraft.storage.snapshot.local;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import com.alipay.sofa.jraft.entity.LocalFileMetaOutter;
import com.alipay.sofa.jraft.option.RaftOptions;
import com.alipay.sofa.jraft.storage.BaseStorageTest;
import com.alipay.sofa.jraft.storage.FileService;
import com.alipay.sofa.jraft.storage.snapshot.Snapshot;
import com.alipay.sofa.jraft.util.Endpoint;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(value = MockitoJUnitRunner.class)
public class LocalSnapshotReaderTest extends BaseStorageTest {

    private LocalSnapshotReader    reader;
    @Mock
    private LocalSnapshotStorage   snapshotStorage;
    private LocalSnapshotMetaTable table;
    private final int              snapshotIndex = 99;

    @Override
    @Before
    public void setup() throws Exception {
        super.setup();
        this.path = this.path + File.separator + Snapshot.JRAFT_SNAPSHOT_PREFIX + snapshotIndex;
        FileUtils.forceMkdir(new File(path));
        this.table = new LocalSnapshotMetaTable(new RaftOptions());
        this.table.addFile("testFile", LocalFileMetaOutter.LocalFileMeta.newBuilder().setChecksum("test").build());
        table.saveToFile(path + File.separator + Snapshot.JRAFT_SNAPSHOT_META_FILE);
        this.reader = new LocalSnapshotReader(snapshotStorage, null, new Endpoint("localhost", 8081),
            new RaftOptions(), path);
        assertTrue(this.reader.init(null));
    }

    @Override
    @After
    public void teardown() throws Exception {
        super.teardown();
        this.reader.close();
        Mockito.verify(this.snapshotStorage, Mockito.only()).unref(this.snapshotIndex);
        assertFalse(FileService.getInstance().removeReader(this.reader.getReaderId()));
    }

    @Test
    public void testListFiles() {
        assertTrue(this.reader.listFiles().contains("testFile"));
    }

    @Test
    public void testGenerateUriForCopy() throws Exception {
        final String uri = this.reader.generateURIForCopy();
        assertNotNull(uri);
        assertTrue(uri.startsWith("remote://localhost:8081/"));
        final long readerId = Long.valueOf(uri.substring("remote://localhost:8081/".length()));
        assertTrue(FileService.getInstance().removeReader(readerId));
    }
}
