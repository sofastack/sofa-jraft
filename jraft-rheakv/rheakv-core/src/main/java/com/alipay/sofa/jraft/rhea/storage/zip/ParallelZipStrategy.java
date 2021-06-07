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

import com.alipay.sofa.jraft.Lifecycle;
import com.alipay.sofa.jraft.rhea.util.Lists;
import com.alipay.sofa.jraft.util.ExecutorServiceHelper;
import com.alipay.sofa.jraft.util.Requires;
import org.apache.commons.compress.archivers.zip.*;
import org.apache.commons.compress.parallel.FileBasedScatterGatherBackingStore;
import org.apache.commons.compress.parallel.InputStreamSupplier;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.NullInputStream;

import java.io.*;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.zip.Checksum;
import java.util.zip.ZipEntry;

/**
 * @author hzh
 */
public class ParallelZipStrategy implements Lifecycle<ParallelZipStrategy>, ZipStrategy {

    private final int             deCompressCoreThreads;
    private final int             compressCoreThreads;
    private final ExecutorService deCompressExecutor;

    public ParallelZipStrategy(int deCompressThreadSize, int compressCoreThreads) {
        this.compressCoreThreads = compressCoreThreads;
        this.deCompressCoreThreads = deCompressThreadSize;
        this.deCompressExecutor = Executors.newFixedThreadPool(deCompressThreadSize);
    }

    // parallel output streams controller
    private static class ZipArchiveScatterOutputStream {

        private final ParallelScatterZipCreator creator;

        public ZipArchiveScatterOutputStream(int threadSize) {
            this.creator = new ParallelScatterZipCreator(Executors.newFixedThreadPool(threadSize));
        }

        public void addEntry(ZipArchiveEntry entry, InputStreamSupplier supplier) {
            creator.addArchiveEntry(entry, supplier);
        }

        public void writeTo(ZipArchiveOutputStream archiveOutput) throws Exception {
            creator.writeTo(archiveOutput);
        }

    }

    @Override
    public void compress(String rootDir, String sourceDir, String outputZipFile, Checksum checksum) throws IOException {
        final File rootFile = new File(Paths.get(rootDir, sourceDir).toString());
        final File zipFile = new File(outputZipFile);
        FileUtils.forceMkdir(zipFile.getParentFile());

        //parallel compress
        ZipArchiveScatterOutputStream scatterOutput = new ZipArchiveScatterOutputStream(this.compressCoreThreads);
        compressDirectoryToZipFile(rootFile, scatterOutput, sourceDir, ZipEntry.DEFLATED);

        //write to and flush
        try (final BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(zipFile));
                final ZipArchiveOutputStream archiveOutput = new ZipArchiveOutputStream(bos)) {
            scatterOutput.writeTo(archiveOutput);
            archiveOutput.flush();
        } catch (Exception e) {
            e.printStackTrace();
        }

        //compute checksum
        computeZipFileChecksumValue(outputZipFile, checksum);
    }

    @Override
    public void deCompress(final String sourceZipFile, final String outputDir, final Checksum checksum) throws IOException {
        try (final ZipFile zipFile = new ZipFile(new File(sourceZipFile))) {
            final ArrayList<ZipArchiveEntry> zipEntries = Collections.list(zipFile.getEntries());
            final List<Future<?>> futures = Lists.newArrayList();
            //parallel deCompress
            for (final ZipArchiveEntry zipEntry : zipEntries) {
                final Future<?> future = deCompressExecutor.submit(() -> {
                    try {
                        unZipFile(zipFile, zipEntry, outputDir);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
                futures.add(future);
            }
            //blocking and caching exception
            for (final Future<?> future : futures) {
                try {
                    future.get();
                } catch (Throwable t) {
                    t.printStackTrace();
                }
            }
        }
        //compute checksum
        computeZipFileChecksumValue(sourceZipFile,checksum);
    }

    private void compressDirectoryToZipFile(File dir, ZipArchiveScatterOutputStream scatterOutput, String sourceDir,
                                            int method) throws IOException {
        if (dir == null) {
            return;
        }
        if (dir.isFile()) {
            addEntry(sourceDir, dir, scatterOutput, method);
            return;
        }
        final File[] files = Requires.requireNonNull(dir.listFiles(), "files");
        for (final File file : files) {
            final String child = Paths.get(sourceDir, file.getName()).toString();
            if (file.isDirectory()) {
                compressDirectoryToZipFile(file, scatterOutput, child, method);
            } else {
                addEntry(child, file, scatterOutput, method);
            }
        }
    }

    /**
     * add archive entry to the scatterOutputStream
     */
    private void addEntry(String filePath, File file, ZipArchiveScatterOutputStream scatterOutputStream, int method) {
        final ZipArchiveEntry archiveEntry = new ZipArchiveEntry(filePath);
        archiveEntry.setMethod(method);
        scatterOutputStream.addEntry(archiveEntry, () -> {
            try {
                return file.isDirectory() ? new NullInputStream(0) :
                                                    new BufferedInputStream(new FileInputStream(file));
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
            return null;
        });
    }

    /**
     * unzip the archive entry to targetDir
     */
    private void unZipFile(ZipFile zipFile, ZipArchiveEntry entry, String targetDir) throws IOException {
        final File targetFile = new File(Paths.get(targetDir, entry.getName()).toString());
        FileUtils.forceMkdir(targetFile.getParentFile());
        try (final InputStream is = zipFile.getInputStream(entry);
                final BufferedInputStream fis = new BufferedInputStream(is);
                final BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(targetFile))) {
            IOUtils.copy(fis, bos);
        }
    }

    /**
     * compute the value of checksum
     */
    private void computeZipFileChecksumValue(String zipPath, Checksum checksum) throws IOException {
        File file = new File(zipPath);
        if (!file.exists())
            return;
        byte[] buffer = new byte[8192];
        try (final ZipFile zipFile = new ZipFile(file)) {
            final ArrayList<ZipArchiveEntry> zipEntries = Collections.list(zipFile.getEntries());
            for (ZipArchiveEntry zipEntry : zipEntries) {
                try (final InputStream is = zipFile.getRawInputStream(zipEntry);
                        final BufferedInputStream fis = new BufferedInputStream(is)) {
                    int length = 0;
                    while (-1 != (length = fis.read(buffer))) {
                        checksum.update(buffer, 0, length);
                    }
                }
            }
        }
    }

    @Override
    public boolean init(ParallelZipStrategy opts) {
        return true;
    }

    @Override
    public void shutdown() {
        this.deCompressExecutor.shutdown();
        ExecutorServiceHelper.shutdownAndAwaitTermination(this.deCompressExecutor);
    }
}
