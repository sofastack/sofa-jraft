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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;

import org.junit.Before;
import org.junit.Test;

import com.alipay.sofa.jraft.storage.BaseStorageTest;

public class AbortFileTest extends BaseStorageTest {
    private AbortFile abortFile;

    @Override
    @Before
    public void setup() throws Exception {
        super.setup();
        this.abortFile = new AbortFile(this.path + File.separator + "abort");
    }

    @Test
    public void testMisc() throws Exception {
        assertFalse(this.abortFile.exists());
        assertTrue(this.abortFile.create());
        assertTrue(this.abortFile.exists());
        assertFalse(this.abortFile.create());
        this.abortFile.destroy();
        assertFalse(this.abortFile.exists());
        assertTrue(this.abortFile.create());
        assertTrue(this.abortFile.exists());
    }
}
