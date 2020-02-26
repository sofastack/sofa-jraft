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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.File;

import org.junit.Before;
import org.junit.Test;

import com.alipay.sofa.jraft.storage.BaseStorageTest;

public class CheckpointFileTest extends BaseStorageTest {
    private CheckpointFile checkpointFile;

    @Override
    @Before
    public void setup() throws Exception {
        super.setup();
        this.checkpointFile = new CheckpointFile(this.path + File.separator + "checkpoint");
    }

    @Test
    public void testMisc() throws Exception {
        assertNull(this.checkpointFile.load());

        this.checkpointFile.save(new CheckpointFile.Checkpoint("test1", 99));
        CheckpointFile.Checkpoint cp = this.checkpointFile.load();
        assertNotNull(cp);
        assertEquals("test1", cp.segFilename);
        assertEquals(99, cp.committedPos);

        this.checkpointFile.destroy();
        assertNull(this.checkpointFile.load());

        this.checkpointFile.save(new CheckpointFile.Checkpoint("test2", 299));
        cp = this.checkpointFile.load();
        assertNotNull(cp);
        assertEquals("test2", cp.segFilename);
        assertEquals(299, cp.committedPos);
    }
}
