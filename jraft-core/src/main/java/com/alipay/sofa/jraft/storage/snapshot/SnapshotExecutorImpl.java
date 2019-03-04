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
package com.alipay.sofa.jraft.storage.snapshot;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.FSMCaller;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.closure.LoadSnapshotClosure;
import com.alipay.sofa.jraft.closure.SaveSnapshotClosure;
import com.alipay.sofa.jraft.core.NodeImpl;
import com.alipay.sofa.jraft.entity.EnumOutter.ErrorType;
import com.alipay.sofa.jraft.entity.RaftOutter.SnapshotMeta;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.error.RaftException;
import com.alipay.sofa.jraft.option.SnapshotCopierOptions;
import com.alipay.sofa.jraft.option.SnapshotExecutorOptions;
import com.alipay.sofa.jraft.rpc.RpcRequestClosure;
import com.alipay.sofa.jraft.rpc.RpcRequests.InstallSnapshotRequest;
import com.alipay.sofa.jraft.rpc.RpcRequests.InstallSnapshotResponse;
import com.alipay.sofa.jraft.rpc.RpcResponseFactory;
import com.alipay.sofa.jraft.storage.LogManager;
import com.alipay.sofa.jraft.storage.SnapshotExecutor;
import com.alipay.sofa.jraft.storage.SnapshotStorage;
import com.alipay.sofa.jraft.storage.StorageFactory;
import com.alipay.sofa.jraft.storage.snapshot.local.LocalSnapshotStorage;
import com.alipay.sofa.jraft.util.CountDownEvent;
import com.alipay.sofa.jraft.util.OnlyForTest;
import com.alipay.sofa.jraft.util.Requires;
import com.alipay.sofa.jraft.util.Utils;

/**
 * Snapshot executor implementation.
 *
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Mar-22 5:38:56 PM
 */
public class SnapshotExecutorImpl implements SnapshotExecutor {

    private static final Logger                        LOG                 = LoggerFactory
                                                                               .getLogger(SnapshotExecutorImpl.class);

    private final Lock                                 lock                = new ReentrantLock();

    private long                                       lastSnapshotTerm;
    private long                                       lastSnapshotIndex;
    private long                                       term;
    private volatile boolean                           savingSnapshot;
    private volatile boolean                           loadingSnapshot;
    private volatile boolean                           stopped;
    private SnapshotStorage                            snapshotStorage;
    private SnapshotCopier                             curCopier;
    private FSMCaller                                  fsmCaller;
    private NodeImpl                                   node;
    private LogManager                                 logManager;
    private final AtomicReference<DownloadingSnapshot> downloadingSnapshot = new AtomicReference<>(null);
    private SnapshotMeta                               loadingSnapshotMeta;
    private final CountDownEvent                       runningJobs         = new CountDownEvent();

    /**
     * Downloading snapshot job.
     *
     * @author boyan (boyan@alibaba-inc.com)
     *
     * 2018-Apr-08 3:07:19 PM
     */
    static class DownloadingSnapshot {
        InstallSnapshotRequest          request;
        InstallSnapshotResponse.Builder responseBuilder;
        RpcRequestClosure               done;

        public DownloadingSnapshot(InstallSnapshotRequest request, InstallSnapshotResponse.Builder responseBuilder,
                                   RpcRequestClosure done) {
            super();
            this.request = request;
            this.responseBuilder = responseBuilder;
            this.done = done;
        }

    }

    /**
     * Only for test
     */
    @OnlyForTest
    public long getLastSnapshotTerm() {
        return this.lastSnapshotTerm;
    }

    /**
     * Only for test
     */
    @OnlyForTest
    public long getLastSnapshotIndex() {
        return this.lastSnapshotIndex;
    }

    /**
     * Save snapshot done closure
     * @author boyan (boyan@alibaba-inc.com)
     *
     * 2018-Apr-08 3:07:52 PM
     */
    private class SaveSnapshotDone implements SaveSnapshotClosure {

        SnapshotWriter writer;
        Closure        done;
        SnapshotMeta   meta;

        public SaveSnapshotDone(SnapshotWriter writer, Closure done, SnapshotMeta meta) {
            super();
            this.writer = writer;
            this.done = done;
            this.meta = meta;
        }

        @Override
        public void run(final Status status) {
            Utils.runInThread(() -> continueRun(status));
        }

        void continueRun(Status st) {
            final int ret = onSnapshotSaveDone(st, meta, writer);
            if (ret != 0 && st.isOk()) {
                st.setError(ret, "node call onSnapshotSaveDone failed");
            }
            if (this.done != null) {
                Utils.runClosureInThread(this.done, st);
            }
        }

        @Override
        public SnapshotWriter start(SnapshotMeta meta) {
            this.meta = meta;
            return this.writer;

        }

    }

    /**
     * Install snapshot done closure
     * @author boyan (boyan@alibaba-inc.com)
     *
     * 2018-Apr-08 3:08:09 PM
     */
    private class InstallSnapshotDone implements LoadSnapshotClosure {

        SnapshotReader reader;

        public InstallSnapshotDone(SnapshotReader reader) {
            super();
            this.reader = reader;
        }

        @Override
        public void run(Status status) {
            onSnapshotLoadDone(status);
        }

        @Override
        public SnapshotReader start() {
            return this.reader;
        }

    }

    /**
     * Load snapshot at first time closure
     * @author boyan (boyan@alibaba-inc.com)
     *
     * 2018-Apr-16 2:57:46 PM
     */
    private class FirstSnapshotLoadDone implements LoadSnapshotClosure {

        SnapshotReader reader;
        CountDownLatch eventLatch;
        Status         status;

        public FirstSnapshotLoadDone(SnapshotReader reader) {
            super();
            this.reader = reader;
            this.eventLatch = new CountDownLatch(1);
        }

        @Override
        public void run(Status status) {
            this.status = status;
            onSnapshotLoadDone(this.status);
            this.eventLatch.countDown();
        }

        public void waitForRun() throws InterruptedException {
            eventLatch.await();
        }

        @Override
        public SnapshotReader start() {
            return this.reader;
        }

    }

    @Override
    public boolean init(SnapshotExecutorOptions opts) {
        if (StringUtils.isBlank(opts.getUri())) {
            LOG.error("Snapshot uri is empty");
            return false;
        }
        this.logManager = opts.getLogManager();
        this.fsmCaller = opts.getFsmCaller();
        this.node = opts.getNode();
        this.term = opts.getInitTerm();
        this.snapshotStorage = StorageFactory.createSnapshotStorage(opts.getUri(), this.node.getRaftOptions());
        if (opts.isFilterBeforeCopyRemote()) {
            this.snapshotStorage.setFilterBeforeCopyRemote();
        }
        if (opts.getSnapshotThrottle() != null) {
            this.snapshotStorage.setSnapshotThrottle(opts.getSnapshotThrottle());
        }
        if (!this.snapshotStorage.init(null)) {
            LOG.error("Fail to init snapshot storage.");
            return false;
        }
        final LocalSnapshotStorage tmp = (LocalSnapshotStorage) this.snapshotStorage;
        if (tmp != null && !tmp.hasServerAddr()) {
            tmp.setServerAddr(opts.getAddr());
        }
        final SnapshotReader reader = this.snapshotStorage.open();
        if (reader == null) {
            return true;
        }
        this.loadingSnapshotMeta = reader.load();
        if (this.loadingSnapshotMeta == null) {
            LOG.error("Fail to load meta from {}", opts.getUri());
            Utils.closeQuietly(reader);
            return false;
        }
        this.loadingSnapshot = true;
        this.runningJobs.incrementAndGet();
        final FirstSnapshotLoadDone done = new FirstSnapshotLoadDone(reader);
        Requires.requireTrue(this.fsmCaller.onSnapshotLoad(done));
        try {
            done.waitForRun();
        } catch (final InterruptedException e) {
            LOG.warn("Wait for FirstSnapshotLoadDone run is interrupted.");
            Thread.currentThread().interrupt();
            return false;
        } finally {
            Utils.closeQuietly(reader);
        }
        if (!done.status.isOk()) {
            LOG.error("Fail to load snapshot from {},FirstSnapshotLoadDone status is {}", opts.getUri(), done.status);
            return false;
        }
        return true;
    }

    @Override
    public void shutdown() {
        long savedTerm;
        this.lock.lock();
        try {
            savedTerm = this.term;
            this.stopped = true;
        } finally {
            lock.unlock();
        }
        this.interruptDownloadingSnapshots(savedTerm);
    }

    @Override
    public NodeImpl getNode() {
        return this.node;
    }

    @Override
    public void doSnapshot(Closure done) {
        boolean doUnlock = true;
        lock.lock();
        try {
            if (this.stopped) {
                Utils.runClosureInThread(done, new Status(RaftError.EPERM, "Is stopped."));
                return;
            }
            if (this.downloadingSnapshot.get() != null) {
                Utils.runClosureInThread(done, new Status(RaftError.EBUSY, "Is loading another snapshot."));
                return;
            }

            if (this.savingSnapshot) {
                Utils.runClosureInThread(done, new Status(RaftError.EBUSY, "Is saving another snapshot."));
                return;
            }

            if (fsmCaller.getLastAppliedIndex() == this.lastSnapshotIndex) {
                // There might be false positive as the getLastAppliedIndex() is being
                // updated. But it's fine since we will do next snapshot saving in a
                // predictable time.
                doUnlock = false;
                lock.unlock();
                this.logManager.clearBufferedLogs();
                Utils.runClosureInThread(done);
                return;
            }
            final SnapshotWriter writer = this.snapshotStorage.create();
            if (writer == null) {
                Utils.runClosureInThread(done, new Status(RaftError.EIO, "Fail to create writer."));
                reportError(RaftError.EIO.getNumber(), "Fail to create snapshot writer.");
                return;
            }
            this.savingSnapshot = true;
            final SaveSnapshotDone saveSnapshotDone = new SaveSnapshotDone(writer, done, null);
            if (!this.fsmCaller.onSnapshotSave(saveSnapshotDone)) {
                Utils.runClosureInThread(done, new Status(RaftError.EHOSTDOWN, "The raft node is down."));
                return;
            }
            this.runningJobs.incrementAndGet();
        } finally {
            if (doUnlock) {
                lock.unlock();
            }
        }

    }

    int onSnapshotSaveDone(Status st, SnapshotMeta meta, SnapshotWriter writer) {
        int ret;
        lock.lock();
        try {
            ret = st.getCode();
            // InstallSnapshot can break SaveSnapshot, check InstallSnapshot when SaveSnapshot
            // because upstream Snapshot maybe newer than local Snapshot.
            if (st.isOk()) {
                if (meta.getLastIncludedIndex() <= this.lastSnapshotIndex) {
                    ret = RaftError.ESTALE.getNumber();
                    if (node != null) {
                        LOG.warn("Node {} discards an stale snapshot lastIncludedIndex={}, lastSnapshotIndex={}",
                            this.node.getNodeId(), meta.getLastIncludedIndex(), this.lastSnapshotIndex);
                    }
                    writer.setError(RaftError.ESTALE, "Installing snapshot is older than local snapshot");
                }
            }
        } finally {
            lock.unlock();
        }

        if (ret == 0) {
            if (!writer.saveMeta(meta)) {
                LOG.warn("Fail to save snapshot {}", writer.getPath());
                ret = RaftError.EIO.getNumber();
            }
        } else {
            if (writer.isOk()) {
                writer.setError(ret, "Fail to do snapshot.");
            }
        }
        try {
            writer.close();
        } catch (final IOException e) {
            LOG.error("Fail to close writer", e);
            ret = RaftError.EIO.getNumber();
        }
        boolean doUnlock = true;
        lock.lock();
        try {
            if (ret == 0) {
                this.lastSnapshotIndex = meta.getLastIncludedIndex();
                this.lastSnapshotTerm = meta.getLastIncludedTerm();
                doUnlock = false;
                this.lock.unlock();
                this.logManager.setSnapshot(meta); //should be out of lock
                doUnlock = true;
                lock.lock();
            }
            if (ret == RaftError.EIO.getNumber()) {
                this.reportError(RaftError.EIO.getNumber(), "Fail to save snapshot.");
            }
            this.savingSnapshot = false;
            this.runningJobs.countDown();
            return ret;

        } finally {
            if (doUnlock) {
                lock.unlock();
            }
        }
    }

    private void onSnapshotLoadDone(Status st) {
        DownloadingSnapshot m;
        boolean doUnlock = true;
        lock.lock();
        try {
            Requires.requireTrue(this.loadingSnapshot, "Not loading snapshot");
            m = this.downloadingSnapshot.get();
            if (st.isOk()) {
                this.lastSnapshotIndex = this.loadingSnapshotMeta.getLastIncludedIndex();
                this.lastSnapshotTerm = this.loadingSnapshotMeta.getLastIncludedTerm();
                doUnlock = false;
                this.lock.unlock();
                this.logManager.setSnapshot(this.loadingSnapshotMeta); //should be out of lock
                doUnlock = true;
                lock.lock();
            }
            final StringBuilder sb = new StringBuilder();
            if (this.node != null) {
                sb.append("Node ").append(node.getNodeId()).append(" ");
            }
            sb.append("onSnapshotLoadDone, ").append(this.loadingSnapshotMeta);
            LOG.info(sb.toString());
            doUnlock = false;
            this.lock.unlock();
            if (node != null) {
                node.updateConfigurationAfterInstallingSnapshot();
            }
            doUnlock = true;
            lock.lock();
            this.loadingSnapshot = false;
            this.downloadingSnapshot.set(null);

        } finally {
            if (doUnlock) {
                lock.unlock();
            }
        }
        if (m != null) {
            // Respond RPC
            if (!st.isOk()) {
                m.done.run(st);
            } else {
                m.responseBuilder.setSuccess(true);
                m.done.sendResponse(m.responseBuilder.build());
            }
        }
        this.runningJobs.countDown();
    }

    @Override
    public void installSnapshot(InstallSnapshotRequest request, InstallSnapshotResponse.Builder response,
                                RpcRequestClosure done) {
        final SnapshotMeta meta = request.getMeta();
        final DownloadingSnapshot ds = new DownloadingSnapshot(request, response, done);
        //DON'T access request, response, and done after this point
        //as the retry snapshot will replace this one.
        if (!registerDownloadingSnapshot(ds)) {
            LOG.warn("Fail to register downloading snapshot");
            // This RPC will be responded by the previous session
            return;
        }
        Requires.requireNonNull(this.curCopier, "curCopier");
        try {
            this.curCopier.join();
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.warn("Install snapshot copy job was canceled.");
            return;
        }

        loadDownloadingSnapshot(ds, meta);
    }

    void loadDownloadingSnapshot(DownloadingSnapshot ds, SnapshotMeta meta) {
        SnapshotReader reader;
        this.lock.lock();
        try {
            if (ds != this.downloadingSnapshot.get()) {
                //It is interrupted and response by other request,just return
                return;
            }
            Requires.requireNonNull(this.curCopier, "curCopier");
            reader = this.curCopier.getReader();
            if (!this.curCopier.isOk()) {
                if (this.curCopier.getCode() == RaftError.EIO.getNumber()) {
                    reportError(this.curCopier.getCode(), this.curCopier.getErrorMsg());
                }
                Utils.closeQuietly(reader);
                ds.done.run(curCopier);
                Utils.closeQuietly(this.curCopier);
                this.curCopier = null;
                this.downloadingSnapshot.set(null);
                this.runningJobs.countDown();
                return;
            }
            Utils.closeQuietly(this.curCopier);
            this.curCopier = null;
            if (reader == null || !reader.isOk()) {
                Utils.closeQuietly(reader);
                this.downloadingSnapshot.set(null);
                ds.done.sendResponse(RpcResponseFactory.newResponse(RaftError.EINTERNAL,
                    "Fail to copy snapshot from %s", ds.request.getUri()));
                this.runningJobs.countDown();
                return;
            }
            this.loadingSnapshot = true;
            this.loadingSnapshotMeta = meta;
        } finally {
            lock.unlock();
        }
        final InstallSnapshotDone installSnapshotDone = new InstallSnapshotDone(reader);
        if (!this.fsmCaller.onSnapshotLoad(installSnapshotDone)) {
            LOG.warn("Fail to  call fsm onSnapshotLoad");
            installSnapshotDone.run(new Status(RaftError.EHOSTDOWN, "This raft node is down"));
        }
    }

    @SuppressWarnings("all")
    boolean registerDownloadingSnapshot(DownloadingSnapshot ds) {
        DownloadingSnapshot saved = null;
        boolean result = true;

        lock.lock();
        try {
            if (this.stopped) {
                LOG.warn("Register DownloadingSnapshot failed: node is stopped");
                ds.done.sendResponse(RpcResponseFactory.newResponse(RaftError.EHOSTDOWN, "Node is stopped."));
                return false;
            }
            if (this.savingSnapshot) {
                LOG.warn("Register DownloadingSnapshot failed: is saving snapshot");
                ds.done.sendResponse(RpcResponseFactory.newResponse(RaftError.EBUSY, "Node is saving snapshot."));
                return false;
            }

            ds.responseBuilder.setTerm(this.term);
            if (ds.request.getTerm() != this.term) {
                LOG.warn("Register DownloadingSnapshot failed: term mismatch, expect {} but {}.", this.term,
                    ds.request.getTerm());
                ds.responseBuilder.setSuccess(false);
                ds.done.sendResponse(ds.responseBuilder.build());
                return false;
            }
            if (ds.request.getMeta().getLastIncludedIndex() <= this.lastSnapshotIndex) {
                LOG.warn(
                    "Register DownloadingSnapshot failed: snapshot is not newer, request lastIncludedIndex={}, lastSnapshotIndex={}.",
                    ds.request.getMeta().getLastIncludedIndex(), this.lastSnapshotIndex);
                ds.responseBuilder.setSuccess(true);
                ds.done.sendResponse(ds.responseBuilder.build());
                return false;
            }
            final DownloadingSnapshot m = this.downloadingSnapshot.get();
            if (m == null) {
                this.downloadingSnapshot.set(ds);
                Requires.requireTrue(this.curCopier == null, "Current copier is not null");
                this.curCopier = this.snapshotStorage.startToCopyFrom(ds.request.getUri(), newCopierOpts());
                if (this.curCopier == null) {
                    this.downloadingSnapshot.set(null);
                    LOG.warn("Register DownloadingSnapshot failed: fail to copy file from {}", ds.request.getUri());
                    ds.done.sendResponse(RpcResponseFactory.newResponse(RaftError.EINVAL, "Fail to copy from: %s",
                        ds.request.getUri()));
                    return false;
                }
                this.runningJobs.incrementAndGet();
                return true;
            }

            // A previous snapshot is under installing, check if this is the same
            // snapshot and resume it, otherwise drop previous snapshot as this one is
            // newer

            if (m.request.getMeta().getLastIncludedIndex() == ds.request.getMeta().getLastIncludedIndex()) {
                // m is a retry
                // Copy |*ds| to |*m| so that the former session would respond
                // this RPC.
                saved = m;
                downloadingSnapshot.set(ds);
                result = false;
            } else if (m.request.getMeta().getLastIncludedIndex() > ds.request.getMeta().getLastIncludedIndex()) {
                // |is| is older
                LOG.warn("Register DownloadingSnapshot failed:  is installing a newer one, lastIncludeIndex={}",
                    m.request.getMeta().getLastIncludedIndex());
                ds.done.sendResponse(RpcResponseFactory.newResponse(RaftError.EINVAL,
                    "A newer snapshot is under installing"));
                return false;
            } else {
                // |is| is newer
                if (this.loadingSnapshot) {
                    LOG.warn("Register DownloadingSnapshot failed: is loading an older snapshot, lastIncludeIndex={}",
                        m.request.getMeta().getLastIncludedIndex());
                    ds.done.sendResponse(RpcResponseFactory.newResponse(RaftError.EBUSY,
                        "A former snapshot is under loading"));
                    return false;
                }
                Requires.requireNonNull(this.curCopier, "curCopier");
                this.curCopier.cancel();
                LOG.warn(
                    "Register DownloadingSnapshot failed:an older snapshot is under installing, cancel downloading, lastIncludeIndex={}",
                    m.request.getMeta().getLastIncludedIndex());
                ds.done.sendResponse(RpcResponseFactory.newResponse(RaftError.EBUSY,
                    "A former snapshot is under installing, trying to cancel"));
                return false;
            }
        } finally {
            lock.unlock();
        }
        if (saved != null) {
            // Respond replaced session
            LOG.warn("Register DownloadingSnapshot failed:  interrupted by retry installling request.");
            saved.done.sendResponse(RpcResponseFactory.newResponse(RaftError.EINTR,
                "Interrupted by the retry InstallSnapshotRequest"));
        }
        return result;
    }

    private SnapshotCopierOptions newCopierOpts() {
        final SnapshotCopierOptions copierOpts = new SnapshotCopierOptions();
        copierOpts.setNodeOptions(this.node.getOptions());
        copierOpts.setRaftClientService(this.node.getRpcService());
        copierOpts.setTimerManager(this.node.getTimerManager());
        copierOpts.setRaftOptions(this.node.getRaftOptions());
        return copierOpts;
    }

    @Override
    public void interruptDownloadingSnapshots(long newTerm) {
        lock.lock();
        try {
            Requires.requireTrue(newTerm >= this.term);
            this.term = newTerm;
            if (this.downloadingSnapshot.get() == null) {
                return;
            }
            if (this.loadingSnapshot) {
                // We can't interrupt loading
                return;
            }
            Requires.requireNonNull(this.curCopier, "curCopier");
            this.curCopier.cancel();
            LOG.info("Trying to cancel downloading snapshot: {}", downloadingSnapshot.get().request);
        } finally {
            lock.unlock();
        }
    }

    private void reportError(int errCode, String fmt, Object... args) {
        final RaftException error = new RaftException(ErrorType.ERROR_TYPE_SNAPSHOT);
        error.setStatus(new Status(errCode, fmt, args));
        this.fsmCaller.onError(error);
    }

    @Override
    public boolean isInstallingSnapshot() {
        return this.downloadingSnapshot.get() != null;
    }

    @Override
    public SnapshotStorage getSnapshotStorage() {
        return this.snapshotStorage;
    }

    @Override
    public void join() throws InterruptedException {
        this.runningJobs.await();
    }

}
