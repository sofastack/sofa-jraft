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

import com.alipay.sofa.jraft.rhea.util.Lists;
import com.alipay.sofa.jraft.rhea.util.StackTraceUtil;
import com.alipay.sofa.jraft.rhea.util.concurrent.CallerRunsPolicyWithReport;
import com.alipay.sofa.jraft.rhea.util.concurrent.NamedThreadFactory;
import com.alipay.sofa.jraft.util.ExecutorServiceHelper;
import com.alipay.sofa.jraft.util.Requires;
import com.alipay.sofa.jraft.util.ThreadPoolUtil;
import com.alipay.sofa.jraft.util.Utils;
import org.apache.commons.compress.archivers.zip.ParallelScatterZipCreator;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipArchiveInputStream;
import org.apache.commons.compress.archivers.zip.ZipArchiveOutputStream;
import org.apache.commons.compress.archivers.zip.ZipFile;
import org.apache.commons.compress.parallel.InputStreamSupplier;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.NullInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.Enumeration;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.zip.CheckedInputStream;
import java.util.zip.CheckedOutputStream;
import java.util.zip.Checksum;
import java.util.zip.ZipEntry;

/**
 * @author hzh
 */
public class ParallelZipStrategy implements ZipStrategy {

    private static final Logger LOG = LoggerFactory.getLogger(ParallelZipStrategy.class);
    private final int           compressThreads;
    private final int           deCompressThreads;

    public ParallelZipStrategy(final int compressThreads, final int deCompressThreads) {
        this.compressThreads = compressThreads;
        this.deCompressThreads = deCompressThreads;
    }

    /**
     * Parallel output streams controller
     */
    private static class ZipArchiveScatterOutputStream {

        private final ParallelScatterZipCreator creator;

        public ZipArchiveScatterOutputStream(final ExecutorService executorService) {
            this.creator = new ParallelScatterZipCreator(executorService);
        }

        public void addEntry(final ZipArchiveEntry entry, final InputStreamSupplier supplier) {
            creator.addArchiveEntry(entry, supplier);
        }

        public void writeTo(final ZipArchiveOutputStream archiveOutput) throws Exception {
            creator.writeTo(archiveOutput);
        }

    }

    @Override
    public void compress(final String rootDir, final String sourceDir, final String outputZipFile,
                         final Checksum checksum) throws Throwable {
        final File rootFile = new File(Paths.get(rootDir, sourceDir).toString());
        final File zipFile = new File(outputZipFile);
        FileUtils.forceMkdir(zipFile.getParentFile());

        // parallel compress
        final ExecutorService compressExecutor = newFixedPool(this.compressThreads, "rheakv-raft-compress-executor");
        final ZipArchiveScatterOutputStream scatterOutput = new ZipArchiveScatterOutputStream(compressExecutor);
        compressDirectoryToZipFile(rootFile, scatterOutput, sourceDir, ZipEntry.DEFLATED);

        // write and flush
        try (final FileOutputStream fos = new FileOutputStream(zipFile);
                final BufferedOutputStream bos = new BufferedOutputStream(fos);
                final CheckedOutputStream cos = new CheckedOutputStream(bos, checksum);
                final ZipArchiveOutputStream archiveOutputStream = new ZipArchiveOutputStream(cos)) {
            scatterOutput.writeTo(archiveOutputStream);
            archiveOutputStream.flush();
            fos.getFD().sync();
        }
        Utils.fsync(zipFile);
        // shutdown executor
        ExecutorServiceHelper.shutdownAndAwaitTermination(compressExecutor);
    }

    @Override
    public void deCompress(final String sourceZipFile, final String outputDir, final Checksum checksum) throws Throwable {
        final ExecutorService deCompressExecutor = newFixedPool(this.deCompressThreads, "rheakv-raft-decompress-executor");
        // compute the checksum in a single thread
        final Future<Boolean> checksumFuture = deCompressExecutor.submit(() -> {
            computeZipFileChecksumValue(sourceZipFile, checksum);
            return true;
        });
        // decompress zip file in thread pool
        try (final ZipFile zipFile = new ZipFile(sourceZipFile)) {
            final List<Future<Boolean>> futures = Lists.newArrayList();
            for (final Enumeration<ZipArchiveEntry> e = zipFile.getEntries(); e.hasMoreElements(); ) {
                final ZipArchiveEntry zipEntry = e.nextElement();
                final Future<Boolean> future = deCompressExecutor.submit(() -> {
                    unZipFile(zipFile, zipEntry, outputDir);
                    return true;
                });
                futures.add(future);
            }
            // blocking and caching exception
            for (final Future<Boolean> future : futures) {
                future.get();
            }
        }
        // wait for checksum to be calculated
        checksumFuture.get();
        // shutdown executor
        ExecutorServiceHelper.shutdownAndAwaitTermination(deCompressExecutor);
    }

    private void compressDirectoryToZipFile(final File dir, final ZipArchiveScatterOutputStream scatterOutput,
                                            final String sourceDir, final int method) {
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
     * Add archive entry to the scatterOutputStream
     */
    private void addEntry(final String filePath, final File file, final ZipArchiveScatterOutputStream scatterOutputStream, final int method) {
        final ZipArchiveEntry archiveEntry = new ZipArchiveEntry(filePath);
        archiveEntry.setMethod(method);
        scatterOutputStream.addEntry(archiveEntry, () -> {
            try {
                return file.isDirectory() ? new NullInputStream(0) :
                                                    new BufferedInputStream(new FileInputStream(file));
            } catch (final FileNotFoundException e) {
                LOG.error("Can't find file, path={}, {}", file.getPath(), StackTraceUtil.stackTrace(e));
            }
            return new NullInputStream(0) ;
        });
    }

    /**
     * Unzip the archive entry to targetDir
     */
    private void unZipFile(final ZipFile zipFile, final ZipArchiveEntry entry, final String targetDir) throws Exception {
        final File targetFile = new File(Paths.get(targetDir, entry.getName()).toString());
        FileUtils.forceMkdir(targetFile.getParentFile());
        try (final InputStream is = zipFile.getInputStream(entry);
                final BufferedInputStream fis = new BufferedInputStream(is);
                final BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(targetFile))) {
            IOUtils.copy(fis, bos);
        }
    }

    /**
     * Compute the value of checksum
     */
    private void computeZipFileChecksumValue(final String zipPath, final Checksum checksum) throws Exception {
        try (final BufferedInputStream bis = new BufferedInputStream(new FileInputStream(zipPath));
                final CheckedInputStream cis = new CheckedInputStream(bis, checksum);
                final ZipArchiveInputStream zis = new ZipArchiveInputStream(cis)) {
            // checksum is calculated in the process
            while ((zis.getNextZipEntry()) != null)
                ;
        }
    }

    private static ExecutorService newFixedPool(final int coreThreads, final String poolName) {
        return ThreadPoolUtil.newBuilder() //
            .poolName(poolName) //
            .enableMetric(true) //
            .coreThreads(coreThreads) //
            .maximumThreads(coreThreads) //
            .keepAliveSeconds(60L) //
            .workQueue(new LinkedBlockingQueue<>()) //
            .threadFactory(new NamedThreadFactory(poolName, true)) //
            .rejectedHandler(new CallerRunsPolicyWithReport(poolName, poolName)) //
            .build();
    }

}
