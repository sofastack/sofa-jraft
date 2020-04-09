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
import java.io.FileWriter;
import java.io.IOException;
import java.util.Date;

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
        return writeDate();
    }

    @SuppressWarnings("deprecation")
    private boolean writeDate() throws IOException {
        final File file = new File(this.path);
        if (file.createNewFile()) {
            try (FileWriter writer = new FileWriter(file, false)) {
                writer.write(new Date().toGMTString());
                writer.write(System.lineSeparator());
            }
            return true;
        } else {
            return false;
        }
    }

    public boolean touch() throws IOException {
        return writeDate();
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
