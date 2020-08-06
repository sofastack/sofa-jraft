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

import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.conf.ConfigurationEntry;
import com.alipay.sofa.jraft.conf.ConfigurationManager;
import com.alipay.sofa.jraft.entity.EnumOutter;
import com.alipay.sofa.jraft.entity.LogEntry;
import com.alipay.sofa.jraft.entity.LogId;
import com.alipay.sofa.jraft.entity.codec.LogEntryDecoder;
import com.alipay.sofa.jraft.entity.codec.LogEntryEncoder;
import com.alipay.sofa.jraft.option.LogStorageOptions;
import com.alipay.sofa.jraft.option.RaftOptions;
import com.alipay.sofa.jraft.storage.LogStorage;
import com.alipay.sofa.jraft.storage.impl.RocksDBLogStorage;
import com.alipay.sofa.jraft.util.*;
import javafx.util.Pair;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author: caobiao
 * @date: 2020/07/17
 * @Description:
 */
public class ArrayDequeLogStorage implements LogStorage, Describer {


    private static final Logger LOG = LoggerFactory.getLogger(RocksDBLogStorage.class);

    private final String path;

    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private final Lock readLock      = this.readWriteLock.readLock();
    private final Lock writeLock     = this.readWriteLock.writeLock();

//    private final boolean openStatistics;

    protected ArrayDeque<Pair<byte[],byte[]>> datalogEntries ;
    protected ArrayDeque<Pair<byte[],byte[]>> conflogEntries ;

    protected LogEntryEncoder logEntryEncoder;
    protected LogEntryDecoder logEntryDecoder;

    private volatile long                   firstLogIndex = 1;
    private volatile boolean                hasLoadFirstLogIndex;
    private final boolean sync;

    /**
     * First log index and last log index key in configuration column family.
     */
    public static final byte[] FIRST_LOG_IDX_KEY = Utils.getBytes("meta/firstLogIndex");
//    private DebugStatistics statistics;


    public ArrayDequeLogStorage(final String path, final RaftOptions raftOptions) {
        super();
        this.path = path;
        this.sync = raftOptions.isSync();
//        this.openStatistics = raftOptions.isOpenStatistics();
    }

    /**
     * Write batch template.
     *
     * @author boyan (boyan@alibaba-inc.com)
     *
     * 2017-Nov-08 11:19:22 AM
     */
    private interface WriteBatchTemplate {

        void execute(WriteBatch batch) throws RocksDBException, IOException, InterruptedException;
    }



    /**
     * A write context
     * @author boyan(boyan@antfin.com)
     *
     */
    public interface WriteContext {
        /**
         * Start a sub job.
         */
        default void startJob() {
        }

        /**
         * Finish a sub job
         */
        default void finishJob() {
        }

        /**
         * Adds a callback that will be invoked after all sub jobs finish.
         */
        default void addFinishHook(final Runnable r) {

        }

        /**
         * Set an exception to context.
         * @param e
         */
        default void setError(final Exception e) {
        }

        /**
         * Wait for all sub jobs finish.
         */
        default void joinAll() throws InterruptedException, IOException {
        }
    }

    /**
     * An empty write context
     * @author boyan(boyan@antfin.com)
     *
     */
    protected static class EmptyWriteContext implements RocksDBLogStorage.WriteContext {
        static EmptyWriteContext INSTANCE = new EmptyWriteContext();
    }

    private boolean initAndLoad(final ConfigurationManager confManager) {
        this.hasLoadFirstLogIndex = false;
        this.firstLogIndex = 1;
        initDeque();
        load(confManager);
        return onInitLoaded();
    }

    private void initDeque() {
        this.datalogEntries = new ArrayDeque<>();
        this.conflogEntries = new ArrayDeque<>();
    }

    private void load(ConfigurationManager confManager) {
        checkState();
        Iterator<Pair<byte[], byte[]>> iterator = this.conflogEntries.iterator();
        while (iterator.hasNext()) {
            Pair<byte[], byte[]> next = iterator.next();
            final byte[] ks = next.getKey();
            final byte[] bs = next.getValue();
            // LogEntry index
            if (ks.length == 8) {
                final LogEntry entry = this.logEntryDecoder.decode(bs);
                if (entry != null) {
                    if (entry.getType() == EnumOutter.EntryType.ENTRY_TYPE_CONFIGURATION) {
                        final ConfigurationEntry confEntry = new ConfigurationEntry();
                        confEntry.setId(new LogId(entry.getId().getIndex(), entry.getId().getTerm()));
                        confEntry.setConf(new Configuration(entry.getPeers(), entry.getLearners()));
                        if (entry.getOldPeers() != null) {
                            confEntry.setOldConf(new Configuration(entry.getOldPeers(), entry.getOldLearners()));
                        }
                        if (confManager != null) {
                            confManager.add(confEntry);
                        }
                    }
                } else {
                    LOG.warn("Fail to decode conf entry at index {}, the log data is: {}.", Bits.getLong(ks, 0),
                            BytesUtil.toHex(bs));
                }
            } else {
                if (Arrays.equals(FIRST_LOG_IDX_KEY, ks)) {
                    setFirstLogIndex(Bits.getLong(bs, 0));
                    truncatePrefixInBackground(0L, this.firstLogIndex);
                } else {
                    LOG.warn("Unknown entry in configuration storage key={}, value={}.", BytesUtil.toHex(ks),
                            BytesUtil.toHex(bs));
                }
            }
        }
    }


    @Override
    public long getFirstLogIndex() {
        this.readLock.lock();
        try {
            if (this.hasLoadFirstLogIndex) {
                return this.firstLogIndex;
            }
            checkState();
            if (this.datalogEntries.size() > 0){
                final long ret = Bits.getLong(this.datalogEntries.peekFirst().getKey(),0);
                saveFirstLogIndex(ret);
                setFirstLogIndex(ret);
                return ret;
            }
            return 1L;
        } finally {
            this.readLock.unlock();
        }
    }

    @Override
    public long getLastLogIndex() {
        this.readLock.lock();
        checkState();
        try {
            if (datalogEntries.size() > 0){
                return Bits.getLong(datalogEntries.peekLast().getKey(),0);
            }
            return 0L;
        } finally {
            this.readLock.unlock();
        }
    }

    @Override
    public LogEntry getEntry(long index) {
        this.readLock.lock();
        try {
            if (this.hasLoadFirstLogIndex && index < this.firstLogIndex) {
                return null;
            }
            long firstLogIndex = getFirstLogIndex();
            final byte[] bs = onDataGet(index, getValueFromDeque(index , firstLogIndex));
            if (bs != null) {
                final LogEntry entry = this.logEntryDecoder.decode(bs);
                if (entry != null) {
                    return entry;
                } else {
                    LOG.error("Bad log entry format for index={}, the log data is: {}.", index, BytesUtil.toHex(bs));
                    // invalid data remove? TODO
                    return null;
                }
            }
        } catch (final IOException e) {
            LOG.error("Fail to get log entry at index {}.", index, e);
        } finally {
            this.readLock.unlock();
        }
        return null;
    }

    protected byte[] getValueFromDeque(long index , long firstLogIndex) {
        checkState();
        if (index >= firstLogIndex && index <= getLastLogIndex()){
            long indexOffset = index - firstLogIndex;
            if(indexOffset < datalogEntries.size()){
                Pair<byte[], byte[]> pair = datalogEntries.get((int) indexOffset);
                if (Bits.getLong(pair.getKey(),0) == index){
                    return datalogEntries.get((int) indexOffset).getValue();
                }
            }
            for (Pair<byte[], byte[]> datalogEntry : datalogEntries) {
                if (Bits.getLong(datalogEntry.getKey(),0) == index){
                    return datalogEntry.getValue();
                }
            }
        }
        return null;
    }

    @Override
    public long getTerm(long index) {
        final LogEntry entry = getEntry(index);
        if (entry != null) {
            return entry.getId().getTerm();
        }
        return 0;
    }

    @Override
    public boolean appendEntry(LogEntry entry) {
        if (entry.getType() == EnumOutter.EntryType.ENTRY_TYPE_CONFIGURATION) {
            return addConf(entry);
        } else {
            this.readLock.lock();
            try {
                if (this.datalogEntries == null) {
                    LOG.warn("Deque not initialized or destroyed.");
                    return false;
                }
                final RocksDBLogStorage.WriteContext writeCtx = newWriteContext();
                final long logIndex = entry.getId().getIndex();
                final byte[] valueBytes = this.logEntryEncoder.encode(entry);
                final byte[] newValueBytes = onDataAppend(logIndex, valueBytes, writeCtx);
                writeCtx.startJob();
                addData(getKeyBytes(logIndex),newValueBytes);
//                this.db.put(this.defaultHandle, this.writeOptions, getKeyBytes(logIndex), newValueBytes);
                writeCtx.joinAll();
                if (newValueBytes != valueBytes) {
                    doSync();
                }
                return true;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            } catch (IOException e) {
                LOG.error("Fail to append entry.", e);
                return false;
            } finally {
                this.readLock.unlock();
            }
        }
    }

    protected void addData(byte[] keyBytes, byte[] newValueBytes) {
        long index = Bits.getLong(keyBytes, 0);
        long indexOffset = index - getFirstLogIndex();
        Pair<byte[],byte[]> pair = new Pair<>(keyBytes,newValueBytes);
        for (Pair<byte[], byte[]> datalogEntry : datalogEntries) {
            if (Bits.getLong(datalogEntry.getKey(),0) > index){
                break;
            }else if(Bits.getLong(datalogEntry.getKey(),0) == index){
                //覆盖
                datalogEntries.set((int)indexOffset,pair);
                return;
            }
        }
        this.datalogEntries.add(pair);
    }


    @Override
    public int appendEntries(List<LogEntry> entries) {
        if (entries == null || entries.isEmpty()) {
            return 0;
        }
        final int entriesCount = entries.size();
        final RocksDBLogStorage.WriteContext writeCtx = newWriteContext();
        try {
            for (final LogEntry entry : entries) {
                if (entry.getType() == EnumOutter.EntryType.ENTRY_TYPE_CONFIGURATION) {
                    addConf(entry);
                } else {
                    writeCtx.startJob();
                    addDataBatch(entry, writeCtx);
                }
            }
            writeCtx.joinAll();
            doSync();
        } catch (InterruptedException | IOException e) {
            e.printStackTrace();
        }
        return entriesCount;
    }

    private void addDataBatch(final LogEntry entry,
                              final RocksDBLogStorage.WriteContext ctx) throws IOException, InterruptedException {
        final long logIndex = entry.getId().getIndex();
        final byte[] content = this.logEntryEncoder.encode(entry);
        final byte[] newValueBytes = onDataAppend(logIndex, content, ctx);
        addData(getKeyBytes(logIndex),newValueBytes);
    }



    private void truncatePrefixInBackground(final long startIndex, final long firstIndexKept) {
        // delete logs in background.
        Utils.runInThread(() -> {
            this.writeLock.lock();
            try {
                System.out.println("truncatePrefixInBackground");
                if (datalogEntries == null) {
                    return;
                }
                onTruncatePrefix(startIndex, firstIndexKept);
                synchronized (datalogEntries){
                    datalogEntries.removeRange(0, (int) (firstIndexKept-startIndex));
                }
//                conflogEntries.removeRange(0, (int) (firstIndexKept-startIndex));
            } catch (final RocksDBException | IOException e) {
                LOG.error("Fail to truncatePrefix {}.", firstIndexKept, e);
            } finally {
                this.writeLock.unlock();
            }
        });
    }

    private void doSync() throws IOException, InterruptedException {
        onSync();
    }

    @Override
    public boolean truncatePrefix(long firstIndexKept) {
        this.readLock.lock();
        try {
            final long startIndex = getFirstLogIndex();
            final boolean ret = saveFirstLogIndex(firstIndexKept);
            if (ret) {
                setFirstLogIndex(firstIndexKept);
            }
//            truncatePrefixInBackground(startIndex, firstIndexKept);
            datalogEntries.removeRange(0, (int) (firstIndexKept-startIndex));
            return ret;
        } finally {
            this.readLock.unlock();
        }
    }


    private void checkState() {
        Requires.requireNonNull(this.datalogEntries, "datalogEntries not initialized or destroyed");
        Requires.requireNonNull(this.conflogEntries, "conflogEntries not initialized or destroyed");
    }

    private void setFirstLogIndex(final long index) {
        this.firstLogIndex = index;
        this.hasLoadFirstLogIndex = true;
    }

    @Override
    public boolean truncateSuffix(long lastIndexKept) {
        this.readLock.lock();
        try {
            try {
                onTruncateSuffix(lastIndexKept);
            } finally {
                final long startIndex = getFirstLogIndex();
                this.datalogEntries.removeRange((int)(lastIndexKept-startIndex+1),(int)(getLastLogIndex()-startIndex+1));
//                this.conflogEntries.removeRange((int)(lastIndexKept-startIndex),(int)(getLastLogIndex()-startIndex+1));
            }
            return true;
        } catch (final RocksDBException | IOException e) {
            LOG.error("Fail to truncateSuffix {}.", lastIndexKept, e);
        } finally {
            this.readLock.unlock();
        }
        return false;
    }

    @Override
    public boolean reset(long nextLogIndex) {
        if (nextLogIndex <= 0) {
            throw new IllegalArgumentException("Invalid next log index.");
        }
        this.writeLock.lock();
        try {
            LogEntry entry = getEntry(nextLogIndex);
            this.conflogEntries.clear();
            this.datalogEntries.clear();
            this.conflogEntries = null;
            this.datalogEntries = null;
            onReset(nextLogIndex);
            if (initAndLoad(null)) {
                if (entry == null) {
                    entry = new LogEntry();
                    entry.setType(EnumOutter.EntryType.ENTRY_TYPE_NO_OP);
                    entry.setId(new LogId(nextLogIndex, 0));
                    LOG.warn("Entry not found for nextLogIndex {} when reset.", nextLogIndex);
                }
                return appendEntry(entry);
            } else {
                return false;
            }
        }finally {
            this.writeLock.unlock();
        }
    }

    @Override
    public boolean init(final LogStorageOptions opts) {
        Requires.requireNonNull(opts.getConfigurationManager(), "Null conf manager");
        Requires.requireNonNull(opts.getLogEntryCodecFactory(), "Null log entry codec factory");
        this.writeLock.lock();
        try {
            if (this.datalogEntries != null && this.conflogEntries != null) {
                LOG.warn("MyLogStorage init() already.");
                return true;
            }
            this.logEntryDecoder = opts.getLogEntryCodecFactory().decoder();
            this.logEntryEncoder = opts.getLogEntryCodecFactory().encoder();
            Requires.requireNonNull(this.logEntryDecoder, "Null log entry decoder");
            Requires.requireNonNull(this.logEntryEncoder, "Null log entry encoder");
//            if (this.openStatistics) {
//                this.statistics = new DebugStatistics();
//            }
            return initAndLoad(opts.getConfigurationManager());
        }  finally {
            this.writeLock.unlock();
        }
    }

    @Override
    public void shutdown() {
        this.writeLock.lock();
        try {
            this.conflogEntries.clear();
            this.datalogEntries.clear();
            this.conflogEntries = null;
            this.datalogEntries = null;
            onShutdown();
//            if (this.statistics != null) {
//                this.statistics.close();
//            }
//            this.statistics = null;
            LOG.info("DB destroyed, the db path is.");
        } finally {
            this.writeLock.unlock();
        }
    }

    @Override
    public void describe(Printer out) {

    }

    protected byte[] getKeyBytes(final long index) {
        final byte[] ks = new byte[8];
        Bits.putLong(ks, 0, index);
        return ks;
    }



    private boolean addConf(final LogEntry entry){
        final byte[] ks = getKeyBytes(entry.getId().getIndex());
        final byte[] content = this.logEntryEncoder.encode(entry);
        Pair<byte[] , byte[]> pair = new Pair<>(ks,content);
        conflogEntries.add(pair);
        return true;
    }




    private byte[] concatByteArray(byte[] a, byte[] b) {
        byte[] c= new byte[a.length+b.length];
        System.arraycopy(a, 0, c, 0, a.length);
        System.arraycopy(b, 0, c, a.length, b.length);
        return c;
    }

    /**
     * Save the first log index into conf column family.
     */
    private boolean saveFirstLogIndex(final long firstLogIndex) {
        this.readLock.lock();
        try {
            final byte[] vs = new byte[8];
            Bits.putLong(vs, 0, firstLogIndex);
            checkState();
            this.conflogEntries.add(new Pair<>(FIRST_LOG_IDX_KEY,vs));
            return true;
        } finally {
            this.readLock.unlock();
        }
    }







    // Hooks for {@link RocksDBSegmentLogStorage}

    /**
     * Called after opening RocksDB and loading configuration into conf manager.
     */
    protected boolean onInitLoaded() {
        return true;
    }

    /**
     * Called after closing db.
     */
    protected void onShutdown() {
    }

    /**
     * Called after resetting db.
     *
     * @param nextLogIndex next log index
     */
    protected void onReset(final long nextLogIndex) {
    }

    /**
     * Called after truncating prefix logs in rocksdb.
     *
     * @param startIndex     the start index
     * @param firstIndexKept the first index to kept
     */
    protected void onTruncatePrefix(final long startIndex, final long firstIndexKept) throws RocksDBException,
            IOException {
        System.out.println("onTruncatePrefix");
    }

    /**
     * Called when sync data into file system.
     */
    protected void onSync() throws IOException, InterruptedException {
    }

    protected boolean isSync() {
        return this.sync;
    }

    /**
     * Called after truncating suffix logs in rocksdb.
     *
     * @param lastIndexKept the last index to kept
     */
    protected void onTruncateSuffix(final long lastIndexKept) throws RocksDBException, IOException {
    }

    protected RocksDBLogStorage.WriteContext newWriteContext() {
        return EmptyWriteContext.INSTANCE;
    }

    /**
     * Called before appending data entry.
     *
     * @param logIndex the log index
     * @param value    the data value in log entry.
     * @return the new value
     */
    protected byte[] onDataAppend(final long logIndex, final byte[] value,
                                  final RocksDBLogStorage.WriteContext ctx) throws IOException, InterruptedException {
        ctx.finishJob();
        return value;
    }

    /**
     * Called after getting data from rocksdb.
     *
     * @param logIndex the log index
     * @param value    the value in rocksdb
     * @return the new value
     */
    protected byte[] onDataGet(final long logIndex, final byte[] value) throws IOException {
        return value;
    }

    protected byte[] onConfAppend(final byte[] keyBytes , final byte[] value , final RocksDBLogStorage.WriteContext ctx) throws IOException , InterruptedException{
        ctx.finishJob();
        return value;
    }
}
