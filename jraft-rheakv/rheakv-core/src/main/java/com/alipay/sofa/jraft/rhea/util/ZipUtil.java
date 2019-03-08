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
package com.alipay.sofa.jraft.rhea.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;

import com.alipay.sofa.jraft.util.Requires;

/**
 *
 * @author jiachun.fjc
 */
public final class ZipUtil {

    public static void compressDirectoryToZipFile(final String rootDir, final String sourceDir,
                                                  final ZipOutputStream zos) throws IOException {
        final String dir = Paths.get(rootDir, sourceDir).toString();
        final File[] files = Requires.requireNonNull(new File(dir).listFiles(), "files");
        for (final File file : files) {
            if (file.isDirectory()) {
                compressDirectoryToZipFile(rootDir, Paths.get(sourceDir, file.getName()).toString(), zos);
            } else {
                zos.putNextEntry(new ZipEntry(Paths.get(sourceDir, file.getName()).toString()));
                try (final FileInputStream in = new FileInputStream(Paths.get(rootDir, sourceDir, file.getName())
                    .toString())) {
                    IOUtils.copy(in, zos);
                }
            }
        }
    }

    public static void unzipFile(final String sourceFile, final String outputDir) throws IOException {
        try (final ZipInputStream zis = new ZipInputStream(new FileInputStream(sourceFile))) {
            ZipEntry zipEntry = zis.getNextEntry();
            while (zipEntry != null) {
                final String fileName = zipEntry.getName();
                final File entryFile = new File(outputDir + File.separator + fileName);
                FileUtils.forceMkdir(entryFile.getParentFile());
                try (final FileOutputStream fos = new FileOutputStream(entryFile)) {
                    IOUtils.copy(zis, fos);
                }
                zipEntry = zis.getNextEntry();
            }
            zis.closeEntry();
        }
    }

    private ZipUtil() {
    }
}
