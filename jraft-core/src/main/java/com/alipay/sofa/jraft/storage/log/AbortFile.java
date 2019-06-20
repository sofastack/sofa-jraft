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

import java.io.File;
import java.io.IOException;

import com.alipay.sofa.jraft.util.Utils;

/**
 * Abort file
 *
 * @author boyan(boyan@antfin.com)
 */
public class AbortFile {

    private final String path;

    public String getPath() {
        return this.path;
    }

    public AbortFile(final String path) {
        super();
        this.path = path;
    }

    public boolean create() throws IOException {
        return new File(this.path) //
            .createNewFile();
    }

    public boolean touch() {
        return new File(this.path) //
            .setLastModified(Utils.nowMs());
    }

    public boolean exists() {
        final File file = new File(this.path);
        return file.isFile() && file.exists();
    }

    public boolean destroy() {
        return new File(this.path) //
            .delete();
    }
}
