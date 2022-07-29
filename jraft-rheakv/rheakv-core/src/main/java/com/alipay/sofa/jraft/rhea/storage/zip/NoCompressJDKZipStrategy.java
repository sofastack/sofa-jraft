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

import com.alipay.sofa.jraft.util.Requires;
import com.alipay.sofa.jraft.util.Utils;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Paths;
import java.util.zip.CheckedOutputStream;
import java.util.zip.Checksum;
import java.util.zip.Deflater;
import java.util.zip.ZipOutputStream;

public class NoCompressJDKZipStrategy extends JDKZipStrategy {

    private static final int BUFFER_SIZE = 2097152;

    @Override
    public void compress(String rootDir, String sourceDir, String outputFile, Checksum checksum) throws Throwable {
        try (final FileOutputStream fos = new FileOutputStream(outputFile);
             final CheckedOutputStream cos = new CheckedOutputStream(fos, checksum);
             ZipOutputStream zipOutputStream = new ZipOutputStream(new BufferedOutputStream(cos, BUFFER_SIZE));
        ) {
            WritableByteChannel writableByteChannel = Channels.newChannel(zipOutputStream);
            zipOutputStream.setLevel(Deflater.NO_COMPRESSION);
            compressDirectoryToZipFile(rootDir, sourceDir, zipOutputStream, writableByteChannel);
            zipOutputStream.flush();
            fos.getFD().sync();
        }
        Utils.fsync(new File(outputFile));
    }

    private static void compressDirectoryToZipFile(final String rootDir, final String sourceDir,
                                                   final ZipOutputStream zos, WritableByteChannel writableByteChannel) throws IOException {
        final String dir = Paths.get(rootDir, sourceDir).toString();
        final File[] files = Requires.requireNonNull(new File(dir).listFiles(), "files");
        for (final File file : files) {
            final String child = Paths.get(sourceDir, file.getName()).toString();
            if (file.isDirectory()) {
                compressDirectoryToZipFile(rootDir, child, zos, writableByteChannel);
            } else {
                long length = file.length();
                if (length <= 0) {
                    return;
                }
                ZipArchiveEntry entry = new ZipArchiveEntry(child);
                zos.putNextEntry(entry);
                try (FileChannel fileChannel = new FileInputStream(file).getChannel()) {
                    fileChannel.transferTo(0, length, writableByteChannel);
                }
            }
        }
    }
}
