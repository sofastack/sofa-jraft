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
package com.alipay.sofa.jraft.storage.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.FSMCaller;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.conf.ConfigurationEntry;
import com.alipay.sofa.jraft.conf.ConfigurationManager;
import com.alipay.sofa.jraft.core.NodeMetrics;
import com.alipay.sofa.jraft.entity.EnumOutter.EntryType;
import com.alipay.sofa.jraft.entity.EnumOutter.ErrorType;
import com.alipay.sofa.jraft.entity.LogEntry;
import com.alipay.sofa.jraft.entity.LogId;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.entity.RaftOutter.SnapshotMeta;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.error.RaftException;
import com.alipay.sofa.jraft.option.LogManagerOptions;
import com.alipay.sofa.jraft.option.RaftOptions;
import com.alipay.sofa.jraft.storage.LogManager;
import com.alipay.sofa.jraft.storage.LogStorage;
import com.alipay.sofa.jraft.util.ArrayDeque;
import com.alipay.sofa.jraft.util.LogExceptionHandler;
import com.alipay.sofa.jraft.util.NamedThreadFactory;
import com.alipay.sofa.jraft.util.Requires;
import com.alipay.sofa.jraft.util.Utils;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;

/**
 * LogManager implementation.
 *
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-04 4:42:20 PM
 */
public class LogManagerImpl implements LogManager {
    private static final Logger                              LOG                   = LoggerFactory
                                                                                       .getLogger(LogManagerImpl.class);

    private LogStorage                                       logStorage;
    private ConfigurationManager                             configManager;
    private FSMCaller                                        fsmCaller;
    private final ReadWriteLock                              lock                  = new ReentrantReadWriteLock();
    private final Lock                                       writeLock             = lock.writeLock();
    private final Lock                                       readLock              = lock.readLock();
    private volatile boolean                                 stopped;
    private volatile boolean                                 hasError;
    private long                                             nextWaitId;
    private LogId                                            diskId                = new LogId(0, 0);
    private LogId                                            appliedId             = new LogId(0, 0);
    //TODO  use a lock-free concurrent list instead?
    private ArrayDeque<LogEntry>                             logsInMemory          = new ArrayDeque<>();
    private volatile long                                    firstLogIndex;
    private volatile long                                    lastLogIndex;
    private volatile LogId                                   lastSnapshotId        = new LogId(0, 0);
    private final Map<Long, WaitMeta>                        waitMap               = new HashMap<>();
    private Disruptor<StableClosureEvent>                    disruptor;
    private RingBuffer<StableClosureEvent>                   diskQueue;
    private RaftOptions                                      raftOptions;
    private volatile CountDownLatch                          shutDownLatch;
    private NodeMetrics                                      nodeMetrics;
    private final CopyOnWriteArrayList<LastLogIndexListener> lastLogIndexListeners = new CopyOnWriteArrayList<>();

    private enum EventType {
        OTHER, // other event type.
        RESET, // reset
        TRUNCATE_PREFIX, // truncate log from prefix
        TRUNCATE_SUFFIX, // truncate log from suffix
        SHUTDOWN, LAST_LOG_ID // get last log id
    }

    private static class StableClosureEvent {
        StableClosure done;
        EventType     type;

        void reset() {
            this.done = null;
            this.type = null;
        }
    }

    private static class StableClosureEventFactory implements EventFactory<StableClosureEvent> {

        @Override
        public StableClosureEvent newInstance() {
            return new StableClosureEvent();
        }
    }

    /**
     * Waiter metadata
     * @author boyan (boyan@alibaba-inc.com)
     *
     * 2018-Apr-04 5:05:04 PM
     */
    private static class WaitMeta {
        /** callback when new log come in*/
        onNewLogCallback onNewLog;
        /** callback error code*/
        int              errorCode;
        /** the waiter pass-in argument */
        Object           arg;

        public WaitMeta(onNewLogCallback onNewLog, Object arg, int errorCode) {
            super();
            this.onNewLog = onNewLog;
            this.arg = arg;
            this.errorCode = errorCode;
        }

    }

    @Override
    public void addLastLogIndexListener(LastLogIndexListener listener) {
        this.lastLogIndexListeners.add(listener);

    }

    @Override
    public void removeLastLogIndexListener(LastLogIndexListener listener) {
        this.lastLogIndexListeners.remove(listener);
    }

    @Override
    public boolean init(LogManagerOptions opts) {
        writeLock.lock();
        try {
            if (opts.getLogStorage() == null) {
                LOG.error("Fail to init log manager, log storage is null");
                return false;
            }
            this.raftOptions = opts.getRaftOptions();
            this.nodeMetrics = opts.getNodeMetrics();
            this.logStorage = opts.getLogStorage();
            this.configManager = opts.getConfigurationManager();
            if (!this.logStorage.init(this.configManager)) {
                LOG.error("Fail to init logStorage");
                return false;
            }
            this.firstLogIndex = this.logStorage.getFirstLogIndex();
            this.lastLogIndex = this.logStorage.getLastLogIndex();
            this.diskId = new LogId(this.lastLogIndex, this.logStorage.getTerm(this.lastLogIndex));
            this.fsmCaller = opts.getFsmCaller();
            this.disruptor = new Disruptor<>(new StableClosureEventFactory(), opts.getDisruptorBufferSize(),
                    new NamedThreadFactory("JRaft-LogManager-Disruptor-", true));
            this.disruptor.handleEventsWith(new StableClosureEventHandler());
            this.disruptor.setDefaultExceptionHandler(
                new LogExceptionHandler<Object>(this.getClass().getSimpleName(),
                    (event, ex) -> reportError(-1, "LogManager handle event error")));
            this.diskQueue = this.disruptor.getRingBuffer();
            this.disruptor.start();
        } finally {
            writeLock.unlock();
        }
        return true;
    }

    private void stopDiskThread() {
        shutDownLatch = new CountDownLatch(1);
        this.diskQueue.publishEvent((event, sequence) -> {
            event.reset();
            event.type = EventType.SHUTDOWN;
        });
    }

    @Override
    public void join() throws InterruptedException {
        if (this.shutDownLatch == null) {
            return;
        }
        shutDownLatch.await();
        this.disruptor.shutdown();
    }

    @Override
    public void shutdown() {
        boolean doUnlock = true;
        writeLock.lock();
        try {
            if (this.stopped) {
                return;
            }
            this.stopped = true;
            doUnlock = false;
            this.wakeupAllWaiter(this.writeLock);
        } finally {
            if (doUnlock) {
                writeLock.unlock();
            }
        }
        this.stopDiskThread();
    }

    private void clearMemoryLogs(LogId id) {
        writeLock.lock();
        try {
            int index = 0;
            for (final int size = this.logsInMemory.size(); index < size; index++) {
                final LogEntry entry = this.logsInMemory.get(index);
                if (entry.getId().compareTo(id) > 0) {
                    break;
                }
            }
            if (index > 0) {
                this.logsInMemory.removeRange(0, index);
            }
        } finally {
            writeLock.unlock();
        }
    }

    private static class LastLogIdClosure extends StableClosure {

        public LastLogIdClosure() {
            super(null);
        }

        private LogId lastLogId;

        void setLastLogId(LogId logId) {
            Requires.requireTrue(logId.getIndex() == 0 || logId.getTerm() != 0);
            this.lastLogId = logId;
        }

        private final CountDownLatch latch = new CountDownLatch(1);

        @Override
        public void run(Status status) {
            latch.countDown();
        }

        void await() throws InterruptedException {
            latch.await();
        }

    }

    @Override
    public void appendEntries(List<LogEntry> entries, final StableClosure done) {
        Requires.requireNonNull(done, "done");
        if (this.hasError) {
            entries.clear();
            Utils.runClosureInThread(done, new Status(RaftError.EIO, "Corrupted LogStorage"));
            return;
        }
        boolean doUnlock = true;
        writeLock.lock();
        try {
            if (!entries.isEmpty() && !checkAndResolveConflict(entries, done)) {
                entries.clear();
                Utils.runClosureInThread(done, new Status(RaftError.EINTERNAL, "Fail to checkAndResolveConflict."));
                return;
            }
            for (int i = 0; i < entries.size(); i++) {
                final LogEntry entry = entries.get(i);
                if (entry.getType() == EntryType.ENTRY_TYPE_CONFIGURATION) {
                    Configuration oldConf = new Configuration();
                    if (entry.getOldPeers() != null) {
                        oldConf = new Configuration(entry.getOldPeers());
                    }
                    final ConfigurationEntry conf = new ConfigurationEntry(entry.getId(), new Configuration(
                        entry.getPeers()), oldConf);
                    this.configManager.add(conf);
                }
            }
            if (!entries.isEmpty()) {
                done.setFirstLogIndex(entries.get(0).getId().getIndex());
                this.logsInMemory.addAll(entries);
            }
            done.setEntries(entries);
            offerEvent(done, EventType.OTHER);
            doUnlock = false;
            if (!wakeupAllWaiter(this.writeLock)) {
                notifyLastLogIndexListeners();
            }
        } finally {
            if (doUnlock) {
                writeLock.unlock();
            }
        }
    }

    private void offerEvent(final StableClosure done, final EventType type) {
        if (this.stopped) {
            Utils.runClosureInThread(done, new Status(RaftError.ESTOP, "Log manager is stopped."));
            return;
        }
        this.diskQueue.publishEvent((event, sequence) -> {
            event.reset();
            event.type = type;
            event.done = done;
        });
    }

    private void notifyLastLogIndexListeners() {
        for (int i = 0; i < this.lastLogIndexListeners.size(); i++) {
            final LastLogIndexListener listener = this.lastLogIndexListeners.get(i);
            if (listener != null) {
                try {
                    listener.onLastLogIndexChanged(this.lastLogIndex);
                } catch (final Exception e) {
                    LOG.error("Fail to notify LastLogIndexListener, listener={}, index={}", listener, this.lastLogIndex);
                }
            }
        }
    }

    private boolean wakeupAllWaiter(Lock lock) {
        if (waitMap.isEmpty()) {
            lock.unlock();
            return false;
        }
        final List<WaitMeta> wms = new ArrayList<>(this.waitMap.values());
        final int errCode = this.stopped ? RaftError.ESTOP.getNumber() : RaftError.SUCCESS.getNumber();
        this.waitMap.clear();
        lock.unlock();

        final int waiterCount = wms.size();
        for (int i = 0; i < waiterCount; i++) {
            final WaitMeta wm = wms.get(i);
            wm.errorCode = errCode;
            Utils.runInThread(() -> runOnNewLog(wm));
        }
        return true;
    }

    private LogId appendToStorage(List<LogEntry> toAppend) {
        LogId lastId = null;
        if (!hasError) {
            final long startMs = Utils.monotonicMs();
            final int entriesCount = toAppend.size();
            this.nodeMetrics.recordSize("append-logs-count", entriesCount);
            try {
                int writtenSize = 0;
                for (int i = 0; i < entriesCount; i++) {
                    final LogEntry entry = toAppend.get(i);
                    writtenSize += entry.getData() != null ? entry.getData().remaining() : 0;
                }
                this.nodeMetrics.recordSize("append-logs-bytes", writtenSize);
                final int nAppent = this.logStorage.appendEntries(toAppend);
                if (nAppent != entriesCount) {
                    LOG.error("**Critical error**, fail to appendEntries, nAppent={}, toAppend={}", nAppent,
                        toAppend.size());
                    reportError(RaftError.EIO.getNumber(), "Fail to append log entries");
                }
                if (nAppent > 0) {
                    lastId = toAppend.get(nAppent - 1).getId();
                }
                toAppend.clear();
            } finally {
                this.nodeMetrics.recordLatency("append-logs", Utils.monotonicMs() - startMs);
            }
        }
        return lastId;
    }

    private class AppendBatcher {
        List<StableClosure> storage;
        int                 cap;
        int                 size;
        int                 bufferSize;
        List<LogEntry>      toAppend;
        LogId               lastId;

        public AppendBatcher(List<StableClosure> storage, int cap, List<LogEntry> toAppend, LogId lastId) {
            super();
            this.storage = storage;
            this.cap = cap;
            this.toAppend = toAppend;
            this.lastId = lastId;
        }

        LogId flush() {
            if (this.size > 0) {
                this.lastId = appendToStorage(toAppend);
                for (int i = 0; i < size; i++) {
                    storage.get(i).getEntries().clear();
                    if (hasError) {
                        storage.get(i).run(new Status(RaftError.EIO, "Corrupted LogStorage"));
                    } else {
                        storage.get(i).run(Status.OK());
                    }
                }
                toAppend.clear();
                storage.clear();
            }
            this.size = 0;
            this.bufferSize = 0;
            return this.lastId;
        }

        void append(StableClosure done) {
            if (size == cap || this.bufferSize >= raftOptions.getMaxAppendBufferSize()) {
                flush();
            }
            storage.add(done);
            size++;
            toAppend.addAll(done.getEntries());
            for (final LogEntry entry : done.getEntries()) {
                this.bufferSize += entry.getData() != null ? entry.getData().remaining() : 0;
            }
        }
    }

    private class StableClosureEventHandler implements EventHandler<StableClosureEvent> {
        LogId               lastId  = diskId;
        List<StableClosure> storage = new ArrayList<>(256);
        AppendBatcher       ab      = new AppendBatcher(storage, 256, new ArrayList<>(), diskId);

        @Override
        public void onEvent(StableClosureEvent event, long sequence, boolean endOfBatch) throws Exception {
            if (event.type == EventType.SHUTDOWN) {
                lastId = ab.flush();
                setDiskId(lastId);
                shutDownLatch.countDown();
                return;
            }
            final StableClosure done = event.done;

            if (done.getEntries() != null && !done.getEntries().isEmpty()) {
                ab.append(done);
            } else {
                this.lastId = ab.flush();
                boolean ret = true;
                switch (event.type) {
                    case LAST_LOG_ID:
                        ((LastLogIdClosure) done).setLastLogId(this.lastId.copy());
                        break;
                    case TRUNCATE_PREFIX:
                        long startMs = Utils.monotonicMs();
                        try {
                            final TruncatePrefixClosure tpc = (TruncatePrefixClosure) done;
                            LOG.debug("Truncating storage to firstIndexKept={}", tpc.firstIndexKept);
                            ret = logStorage.truncatePrefix(tpc.firstIndexKept);
                        } finally {
                            nodeMetrics.recordLatency("truncate-log-prefix", Utils.monotonicMs() - startMs);
                        }
                        break;
                    case TRUNCATE_SUFFIX:
                        startMs = Utils.monotonicMs();
                        try {
                            final TruncateSuffixClosure tsc = (TruncateSuffixClosure) done;
                            LOG.warn("Truncating storage to lastIndexKept={}", tsc.lastIndexKept);
                            ret = logStorage.truncateSuffix(tsc.lastIndexKept);
                            if (ret) {
                                this.lastId.setIndex(tsc.lastIndexKept);
                                this.lastId.setTerm(tsc.lastTermKept);
                                Requires.requireTrue(lastId.getIndex() == 0 || lastId.getTerm() != 0);
                            }
                        } finally {
                            nodeMetrics.recordLatency("truncate-log-suffix", Utils.monotonicMs() - startMs);
                        }
                        break;
                    case RESET:
                        final ResetClosure rc = (ResetClosure) done;
                        LOG.info("Reseting storage to nextLogIndex={}", rc.nextLogIndex);
                        ret = logStorage.reset(rc.nextLogIndex);
                        break;
                    default:
                        break;
                }

                if (!ret) {
                    reportError(RaftError.EIO.getNumber(), "Failed operation in LogStorage");
                } else {
                    done.run(Status.OK());
                }
            }
            if (endOfBatch) {
                lastId = ab.flush();
                setDiskId(lastId);
            }
        }

    }

    private void reportError(int code, String fmt, Object... args) {
        this.hasError = true;
        final RaftException error = new RaftException(ErrorType.ERROR_TYPE_LOG);
        error.setStatus(new Status(code, fmt, args));
        this.fsmCaller.onError(error);
    }

    private void setDiskId(LogId id) {
        LogId clearId;
        writeLock.lock();
        try {
            if (id.compareTo(this.diskId) < 0) {
                return;
            }
            this.diskId = id;
            clearId = this.diskId.compareTo(this.appliedId) <= 0 ? this.diskId : this.appliedId;
        } finally {
            writeLock.unlock();
        }
        if (clearId != null) {
            this.clearMemoryLogs(clearId);
        }
    }

    @Override
    public void setSnapshot(SnapshotMeta meta) {
        LOG.debug("set snapshot: {}", meta);
        writeLock.lock();
        try {
            if (meta.getLastIncludedIndex() <= this.lastSnapshotId.getIndex()) {
                return;
            }
            final Configuration conf = new Configuration();
            for (int i = 0; i < meta.getPeersCount(); i++) {
                final PeerId peer = new PeerId();
                peer.parse(meta.getPeers(i));
                conf.addPeer(peer);
            }
            final Configuration oldConf = new Configuration();
            for (int i = 0; i < meta.getOldPeersCount(); i++) {
                final PeerId peer = new PeerId();
                peer.parse(meta.getOldPeers(i));
                oldConf.addPeer(peer);
            }

            final ConfigurationEntry entry = new ConfigurationEntry(new LogId(meta.getLastIncludedIndex(),
                meta.getLastIncludedTerm()), conf, oldConf);
            this.configManager.setSnapshot(entry);
            final long term = unsafeGetTerm(meta.getLastIncludedIndex());
            final long savedLastSnapshotIndex = this.lastSnapshotId.getIndex();

            lastSnapshotId.setIndex(meta.getLastIncludedIndex());
            lastSnapshotId.setTerm(meta.getLastIncludedTerm());

            if (lastSnapshotId.compareTo(this.appliedId) > 0) {
                this.appliedId = lastSnapshotId.copy();
            }

            if (term == 0) {
                // last_included_index is larger than last_index
                // FIXME: what if last_included_index is less than first_index?
                truncatePrefix(meta.getLastIncludedIndex() + 1);
            } else if (term == meta.getLastIncludedTerm()) {
                // Truncating log to the index of the last snapshot.
                // We don't truncate log before the last snapshot immediately since
                // some log around last_snapshot_index is probably needed by some
                // followers
                // TODO if there are still be need?
                if (savedLastSnapshotIndex > 0) {
                    truncatePrefix(savedLastSnapshotIndex + 1);
                }
            } else {
                if (!reset(meta.getLastIncludedIndex() + 1)) {
                    LOG.warn("Reset log manager failed, nextLogIndex={}", meta.getLastIncludedIndex() + 1);
                }
            }
        } finally {
            writeLock.unlock();
        }

    }

    @Override
    public void clearBufferedLogs() {
        writeLock.lock();
        try {
            if (lastSnapshotId.getIndex() != 0) {
                truncatePrefix(lastSnapshotId.getIndex() + 1);
            }
        } finally {
            writeLock.unlock();
        }
    }

    private String descLogsInMemory() {
        final StringBuilder sb = new StringBuilder();
        boolean wasFirst = true;
        for (final LogEntry logEntry : this.logsInMemory) {
            if (!wasFirst) {
                sb.append(",");
            } else {
                wasFirst = false;
            }
            sb.append("<id:(").append(logEntry.getId().getTerm()).append(",").append(logEntry.getId().getIndex())
                .append("),type:").append(logEntry.getType()).append(">");
        }
        return sb.toString();
    }

    protected LogEntry getEntryFromMemory(long index) {
        LogEntry entry = null;
        if (!this.logsInMemory.isEmpty()) {
            final long firstIndex = this.logsInMemory.peekFirst().getId().getIndex();
            final long lastIndex = this.logsInMemory.peekLast().getId().getIndex();
            if (lastIndex - firstIndex + 1 != this.logsInMemory.size()) {
                throw new IllegalStateException(String.format("lastIndex=%d,firstIndex=%d,logsInMemory=[%s]",
                    lastIndex, firstIndex, descLogsInMemory()));
            }
            if (index >= firstIndex && index <= lastIndex) {
                entry = this.logsInMemory.get((int) (index - firstIndex));
            }
        }
        return entry;
    }

    @Override
    public LogEntry getEntry(long index) {
        readLock.lock();
        try {
            if (index > lastLogIndex || index < firstLogIndex) {
                return null;
            }
            final LogEntry entry = getEntryFromMemory(index);
            if (entry != null) {
                return entry;
            }
        } finally {
            readLock.unlock();
        }
        final LogEntry entry = this.logStorage.getEntry(index);
        if (entry == null) {
            this.reportError(RaftError.EIO.getNumber(), "Corrupted entry at index=%d", index);
        }
        return entry;
    }

    @Override
    public long getTerm(long index) {
        if (index == 0) {
            return 0;
        }
        readLock.lock();
        try {
            // out of range, direct return NULL
            if (index > this.lastLogIndex) {
                return 0;
            }
            // check index equal snapshot_index, return snapshot_term
            if (index == lastSnapshotId.getIndex()) {
                return lastSnapshotId.getTerm();
            }
            final LogEntry entry = getEntryFromMemory(index);
            if (entry != null) {
                return entry.getId().getTerm();
            }
        } finally {
            readLock.unlock();
        }
        return this.logStorage.getTerm(index);
    }

    @Override
    public long getFirstLogIndex() {
        readLock.lock();
        try {
            return this.firstLogIndex;
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public long getLastLogIndex() {
        return this.getLastLogIndex(false);
    }

    @Override
    public long getLastLogIndex(boolean isFlush) {
        LastLogIdClosure c;
        readLock.lock();
        try {
            if (!isFlush) {
                return this.lastLogIndex;
            } else {
                if (lastLogIndex == this.lastSnapshotId.getIndex()) {
                    return this.lastLogIndex;
                }
                c = new LastLogIdClosure();
                offerEvent(c, EventType.LAST_LOG_ID);
            }
        } finally {
            readLock.unlock();
        }
        try {
            c.await();
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
        return c.lastLogId.getIndex();
    }

    private long unsafeGetTerm(long index) {
        if (index == 0) {
            return 0;
        }
        if (index > this.lastLogIndex) {
            return 0;
        }
        final LogId lss = lastSnapshotId;
        if (index == lss.getIndex()) {
            return lss.getTerm();
        }
        final LogEntry entry = getEntryFromMemory(index);
        if (entry != null) {
            return entry.getId().getTerm();
        }
        return this.logStorage.getTerm(index);
    }

    @Override
    public LogId getLastLogId(boolean isFlush) {
        LastLogIdClosure c;
        readLock.lock();
        try {
            if (!isFlush) {
                if (this.lastLogIndex >= this.firstLogIndex) {
                    return new LogId(this.lastLogIndex, unsafeGetTerm(this.lastLogIndex));
                }
                return this.lastSnapshotId;
            } else {
                if (lastLogIndex == this.lastSnapshotId.getIndex()) {
                    return this.lastSnapshotId;
                }
                c = new LastLogIdClosure();
                offerEvent(c, EventType.LAST_LOG_ID);
            }
        } finally {
            readLock.unlock();
        }
        try {
            c.await();
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
        return c.lastLogId;
    }

    private static class TruncatePrefixClosure extends StableClosure {
        long firstIndexKept;

        public TruncatePrefixClosure(long firstIndexKept) {
            super(null);
            this.firstIndexKept = firstIndexKept;
        }

        @Override
        public void run(Status status) {

        }

    }

    private static class TruncateSuffixClosure extends StableClosure {
        long lastIndexKept;
        long lastTermKept;

        public TruncateSuffixClosure(long lastIndexKept, long lastTermKept) {
            super(null);
            this.lastIndexKept = lastIndexKept;
            this.lastTermKept = lastTermKept;
        }

        @Override
        public void run(Status status) {

        }

    }

    private static class ResetClosure extends StableClosure {
        long nextLogIndex;

        public ResetClosure(long nextLogIndex) {
            super(null);
            this.nextLogIndex = nextLogIndex;
        }

        @Override
        public void run(Status status) {

        }
    }

    private boolean truncatePrefix(long firstIndexKept) {
        int index = 0;
        for (final int size = this.logsInMemory.size(); index < size; index++) {
            final LogEntry entry = this.logsInMemory.get(index);
            if (entry.getId().getIndex() >= firstIndexKept) {
                break;
            }
        }
        if (index > 0) {
            this.logsInMemory.removeRange(0, index);
        }

        // TODO  maybe it's fine here
        Requires.requireTrue(firstIndexKept >= this.firstLogIndex,
            "Try to truncate logs before %d, but the firstLogIndex is %d", firstIndexKept, firstLogIndex);

        this.firstLogIndex = firstIndexKept;
        if (firstIndexKept > this.lastLogIndex) {
            // The entry log is dropped
            this.lastLogIndex = firstIndexKept - 1;
        }
        LOG.debug("Truncate prefix, firstIndexKept is :{}", firstIndexKept);
        this.configManager.truncatePrefix(firstIndexKept);
        final TruncatePrefixClosure c = new TruncatePrefixClosure(firstIndexKept);
        offerEvent(c, EventType.TRUNCATE_PREFIX);
        return true;
    }

    private boolean reset(long nextLogIndex) {
        writeLock.lock();
        try {
            this.logsInMemory = new ArrayDeque<>();
            this.firstLogIndex = nextLogIndex;
            this.lastLogIndex = nextLogIndex - 1;
            configManager.truncatePrefix(this.firstLogIndex);
            configManager.truncateSuffix(this.lastLogIndex);
            final ResetClosure c = new ResetClosure(nextLogIndex);
            offerEvent(c, EventType.RESET);
            return true;
        } finally {
            writeLock.unlock();
        }
    }

    private void unsafeTruncateSuffix(long lastIndexKept) {
        if (lastIndexKept < this.appliedId.getIndex()) {
            LOG.error("FATAL ERROR: Can't truncate logs before appliedId={}, lastIndexKept={}", this.appliedId,
                lastIndexKept);
            return;
        }
        while (!logsInMemory.isEmpty()) {
            final LogEntry entry = logsInMemory.peekLast();
            if (entry.getId().getIndex() > lastIndexKept) {
                logsInMemory.pollLast();
            } else {
                break;
            }
        }
        this.lastLogIndex = lastIndexKept;
        final long lastTermKept = unsafeGetTerm(lastIndexKept);
        Requires.requireTrue(this.lastLogIndex == 0 || lastTermKept != 0);
        LOG.debug("Truncate suffix :{}", lastIndexKept);
        this.configManager.truncateSuffix(lastIndexKept);
        final TruncateSuffixClosure c = new TruncateSuffixClosure(lastIndexKept, lastTermKept);
        offerEvent(c, EventType.TRUNCATE_SUFFIX);
    }

    @SuppressWarnings("NonAtomicOperationOnVolatileField")
    private boolean checkAndResolveConflict(List<LogEntry> entries, StableClosure done) {
        final LogEntry firstLogEntry = ArrayDeque.peekFirst(entries);
        if (firstLogEntry.getId().getIndex() == 0) {
            // Node is currently the leader and |entries| are from the user who
            // don't know the correct indexes the logs should assign to. So we have
            // to assign indexes to the appending entries
            for (int i = 0; i < entries.size(); i++) {
                entries.get(i).getId().setIndex(++this.lastLogIndex);
            }
            return true;
        } else {
            // Node is currently a follower and |entries| are from the leader. We
            // should check and resolve the conflicts between the local logs and
            // |entries|
            if (firstLogEntry.getId().getIndex() > this.lastLogIndex + 1) {
                Utils.runClosureInThread(done, new Status(RaftError.EINVAL,
                    "There's gap between first_index=%d and last_log_index=%d", firstLogEntry.getId().getIndex(),
                    this.lastLogIndex));
                return false;
            }
            final long appliedIndex = appliedId.getIndex();
            final LogEntry lastLogEntry = ArrayDeque.peekLast(entries);
            if (lastLogEntry.getId().getIndex() <= appliedIndex) {
                LOG.warn(
                    "Received entries of which the lastLog={} is not greater than appliedIndex={}, return immediately with nothing changed.",
                    lastLogEntry.getId().getIndex(), appliedIndex);
                return false;
            }
            if (firstLogEntry.getId().getIndex() == this.lastLogIndex + 1) {
                // fast path
                lastLogIndex = lastLogEntry.getId().getIndex();
            } else {
                // Appending entries overlap the local ones. We should find if there
                // is a conflicting index from which we should truncate the local
                // ones.
                int conflictingIndex = 0;
                for (; conflictingIndex < entries.size(); conflictingIndex++) {
                    if (unsafeGetTerm(entries.get(conflictingIndex).getId().getIndex()) != entries
                        .get(conflictingIndex).getId().getTerm()) {
                        break;
                    }
                }
                if (conflictingIndex != entries.size()) {
                    if (entries.get(conflictingIndex).getId().getIndex() <= this.lastLogIndex) {
                        // Truncate all the conflicting entries to make local logs
                        // consensus with the leader.
                        unsafeTruncateSuffix(entries.get(conflictingIndex).getId().getIndex() - 1);
                    }
                    this.lastLogIndex = lastLogEntry.getId().getIndex();
                } // else this is a duplicated AppendEntriesRequest, we have
                  // nothing to do besides releasing all the entries
                if (conflictingIndex > 0) {
                    // Remove duplication
                    entries.subList(0, conflictingIndex).clear();
                }
            }
            return true;
        }
    }

    @Override
    public ConfigurationEntry getConfiguration(long index) {
        readLock.lock();
        try {
            return this.configManager.get(index);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public ConfigurationEntry checkAndSetConfiguration(ConfigurationEntry current) {
        if (current == null) {
            return null;
        }
        readLock.lock();
        try {
            final ConfigurationEntry lastConf = this.configManager.getLastConfiguration();
            if (lastConf != null && !lastConf.isEmpty() && !current.getId().equals(lastConf.getId())) {
                return lastConf;
            }
        } finally {
            readLock.unlock();
        }
        return current;
    }

    @Override
    public long wait(long expectedLastLogIndex, onNewLogCallback cb, Object arg) {
        final WaitMeta wm = new WaitMeta(cb, arg, 0);
        return notifyOnNewLog(expectedLastLogIndex, wm);
    }

    private long notifyOnNewLog(long expectedLastLogIndex, WaitMeta wm) {
        writeLock.lock();
        try {
            if (expectedLastLogIndex != lastLogIndex || this.stopped) {
                wm.errorCode = this.stopped ? RaftError.ESTOP.getNumber() : 0;
                Utils.runInThread(() -> runOnNewLog(wm));
                return 0L;
            }
            if (nextWaitId == 0) { //skip 0
                ++nextWaitId;
            }
            final long waitId = nextWaitId++;
            this.waitMap.put(waitId, wm);
            return waitId;
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public boolean removeWaiter(long id) {
        writeLock.lock();
        try {
            return waitMap.remove(id) != null;
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void setAppliedId(LogId appliedId) {
        LogId clearId;
        writeLock.lock();
        try {
            if (appliedId.compareTo(this.appliedId) < 0) {
                return;
            }
            this.appliedId = appliedId.copy();
            clearId = this.diskId.compareTo(this.appliedId) <= 0 ? this.diskId : this.appliedId;
        } finally {
            writeLock.unlock();
        }
        if (clearId != null) {
            this.clearMemoryLogs(clearId);
        }
    }

    void runOnNewLog(WaitMeta wm) {
        wm.onNewLog.onNewLog(wm.arg, wm.errorCode);
    }

    @Override
    public Status checkConsistency() {
        readLock.lock();
        try {
            Requires.requireTrue(this.firstLogIndex > 0);
            Requires.requireTrue(lastLogIndex >= 0);
            if (lastSnapshotId.equals(new LogId(0, 0))) {
                if (firstLogIndex == 1) {
                    return Status.OK();
                }
                return new Status(RaftError.EIO, "Missing logs in (0, %d)", firstLogIndex);
            } else {
                if (lastSnapshotId.getIndex() >= firstLogIndex - 1 && lastSnapshotId.getIndex() <= lastLogIndex) {
                    return Status.OK();
                }
                return new Status(RaftError.EIO, "There's a gap between snapshot={%d, %d} and log=[%d, %d] ",
                    lastSnapshotId.toString(), lastSnapshotId.getTerm(), firstLogIndex, lastLogIndex);
            }
        } finally {
            readLock.unlock();
        }
    }
}
