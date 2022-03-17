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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.zip.CheckedInputStream;
import java.util.zip.CheckedOutputStream;
import java.util.zip.Checksum;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.NullOutputStream;

import com.alipay.sofa.jraft.util.Requires;
import com.alipay.sofa.jraft.util.Utils;

/**
 *
 * @author jiachun.fjc
 */
public final class ZipUtil {

    public static void compress(final String rootDir, final String sourceDir, final String outputFile,
                                final Checksum checksum) throws IOException {
        try (final FileOutputStream fos = new FileOutputStream(outputFile);
                final CheckedOutputStream cos = new CheckedOutputStream(fos, checksum);
                final ZipOutputStream zos = new ZipOutputStream(new BufferedOutputStream(cos))) {
            ZipUtil.compressDirectoryToZipFile(rootDir, sourceDir, zos);
            zos.flush();
            fos.getFD().sync();
        }
        Utils.fsync(new File(outputFile));
    }

    private static void compressDirectoryToZipFile(final String rootDir, final String sourceDir,
                                                   final ZipOutputStream zos) throws IOException {
        final String dir = Paths.get(rootDir, sourceDir).toString();
        final File[] files = Requires.requireNonNull(new File(dir).listFiles(), "files");
        for (final File file : files) {
            final String child = Paths.get(sourceDir, file.getName()).toString();
            if (file.isDirectory()) {
                compressDirectoryToZipFile(rootDir, child, zos);
            } else {
                zos.putNextEntry(new ZipEntry(child));
                try (final FileInputStream fis = new FileInputStream(file);
                        final BufferedInputStream bis = new BufferedInputStream(fis)) {
                    IOUtils.copy(bis, zos);
                }
            }
        }
    }

    public static void decompress(final String sourceFile, final String outputDir, final Checksum checksum)
                                                                                                           throws IOException {
        try (final FileInputStream fis = new FileInputStream(sourceFile);
                final CheckedInputStream cis = new CheckedInputStream(fis, checksum);
                final ZipInputStream zis = new ZipInputStream(new BufferedInputStream(cis))) {
            ZipEntry entry;
            while ((entry = zis.getNextEntry()) != null) {
                final String fileName = entry.getName();
                final File entryFile = new File(Paths.get(outputDir, fileName).toString());
                FileUtils.forceMkdir(entryFile.getParentFile());
                try (final FileOutputStream fos = new FileOutputStream(entryFile);
                        final BufferedOutputStream bos = new BufferedOutputStream(fos)) {
                    IOUtils.copy(zis, bos);
                    bos.flush();
                    fos.getFD().sync();
                }
            }
            // Continue to read all remaining bytes(extra metadata of ZipEntry) directly from the checked stream,
            // Otherwise, the checksum value maybe unexpected.
            //
            // See https://coderanch.com/t/279175/java/ZipInputStream
            IOUtils.copy(cis, NullOutputStream.NULL_OUTPUT_STREAM);
        }
    }
}
