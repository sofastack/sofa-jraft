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
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.concurrent.ExecutionException;
import java.util.zip.CheckedInputStream;
import java.util.zip.CheckedOutputStream;
import java.util.zip.Checksum;
import java.util.zip.ZipEntry;

import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.zip.ParallelScatterZipCreator;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipArchiveInputStream;
import org.apache.commons.compress.archivers.zip.ZipArchiveOutputStream;
import org.apache.commons.compress.parallel.InputStreamSupplier;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.NullOutputStream;

import com.alipay.sofa.jraft.util.Requires;

/**
 *
 * @author jiachun.fjc
 */
public final class ZipUtil {

    public static void compress(final String rootDir, final String sourceDir, final String outputFile,
                                final Checksum checksum) throws IOException, ExecutionException, InterruptedException {
        try (final FileOutputStream fos = new FileOutputStream(outputFile);
                final CheckedOutputStream cos = new CheckedOutputStream(fos, checksum);
                final ZipArchiveOutputStream zaos = new ZipArchiveOutputStream(new BufferedOutputStream(cos));) {
            final ParallelScatterZipCreator pszc = new ParallelScatterZipCreator();
            ZipUtil.compressDirectoryToZipFile(rootDir, sourceDir, zaos, pszc);
            pszc.writeTo(zaos);
        }
    }

    private static void compressDirectoryToZipFile(final String rootDir, final String sourceDir,
                                                   final ZipArchiveOutputStream zaos, final ParallelScatterZipCreator pszc) {
        final String dir = Paths.get(rootDir, sourceDir).toString();
        final File[] files = Requires.requireNonNull(new File(dir).listFiles(), "files");
        for (final File file : files) {
            final String child = Paths.get(sourceDir, file.getName()).toString();
            if (file.isDirectory()) {
                compressDirectoryToZipFile(rootDir, child, zaos, pszc);
            } else {
                InputStreamSupplier supplier = () -> {
                    try {
                        return new FileInputStream(file);
                    } catch (FileNotFoundException e) {
                        e.printStackTrace();
                    }
                    return null;
                };
                ZipArchiveEntry zipArchiveEntry = new ZipArchiveEntry(child);
                zipArchiveEntry.setMethod(ZipEntry.DEFLATED);
                pszc.addArchiveEntry(zipArchiveEntry, supplier);
            }
        }
    }

    public static void decompress(final String sourceFile, final String outputDir, final Checksum checksum)
                                                                                                           throws IOException {
        try (final FileInputStream fis = new FileInputStream(sourceFile);
                final CheckedInputStream cis = new CheckedInputStream(fis, checksum);
                final ZipArchiveInputStream zais = new ZipArchiveInputStream(new BufferedInputStream(cis))) {
            ArchiveEntry entry;
            while ((entry = zais.getNextEntry()) != null) {
                final String fileName = entry.getName();
                final File entryFile = new File(Paths.get(outputDir, fileName).toString());
                FileUtils.forceMkdir(entryFile.getParentFile());
                try (final FileOutputStream fos = new FileOutputStream(entryFile);
                        final BufferedOutputStream bos = new BufferedOutputStream(fos)) {
                    IOUtils.copy(zais, bos);
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
