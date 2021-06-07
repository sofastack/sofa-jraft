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
package com.alipay.sofa.jraft.rhea.storage.zip;

import java.io.IOException;
import java.util.zip.Checksum;

/**
 * @author hzh
 */
public interface ZipStrategy {

    /**
     * Compress files to zip
     *
     * @param rootDir    the origin file root dir
     * @param sourceDir  the origin file source dir
     * @param outputZipFile the target zip file
     * @param checksum   checksum
     */
    public void compress(final String rootDir, final String sourceDir, final String outputZipFile,
                         final Checksum checksum) throws IOException;

    /**
     * Decompress zip to files
     *
     * @param sourceZipFile the origin zip file
     * @param outputDir  the target file dir
     * @param checksum   checksum
     */
    public void deCompress(final String sourceZipFile, final String outputDir, final Checksum checksum)
                                                                                                       throws IOException;

}
