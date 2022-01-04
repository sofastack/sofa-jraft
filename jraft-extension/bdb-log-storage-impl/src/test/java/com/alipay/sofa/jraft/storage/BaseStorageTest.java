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

import com.alipay.sofa.jraft.test.TestUtils;

public class BaseStorageTest {
    protected String path;

    public void setup() throws Exception {
        this.path = TestUtils.mkTempDir();
        FileUtils.forceMkdir(new File(this.path));
    }

    @After
    public void teardown() throws Exception {
        FileUtils.deleteDirectory(new File(this.path));
    }

    protected String writeData() throws IOException {
        File file = new File(this.path + File.separator + "data");
        String data = "jraft is great!";
        FileUtils.writeStringToFile(file, data);
        return data;
    }

}
