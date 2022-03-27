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
package com.alipay.sofa.jraft.storage.service;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.Lifecycle;
import com.alipay.sofa.jraft.storage.db.AbstractDB;
import com.alipay.sofa.jraft.storage.factory.LogStoreFactory;
import com.alipay.sofa.jraft.util.concurrent.ShutdownAbleThread;

/**
 * Manager of service like allocateService
 * @author hzh (642256541@qq.com)
 */
public class ServiceManager implements Lifecycle<LogStoreFactory> {
    private static final Logger      LOG   = LoggerFactory.getLogger(ServiceManager.class);
    private final AbstractDB         abstractDB;
    private AllocateFileService      allocateService;
    // Maybe we will add more services in the future
    private List<ShutdownAbleThread> serviceList;
    private final AtomicBoolean      start = new AtomicBoolean(false);

    public ServiceManager(final AbstractDB abstractDB) {
        this.abstractDB = abstractDB;
    }

    @Override
    public boolean init(final LogStoreFactory logStoreFactory) {
        this.allocateService = logStoreFactory.newAllocateService(this.abstractDB);
        this.serviceList = new ArrayList<>(1);
        this.serviceList.add(allocateService);
        return true;
    }

    public void start() {
        if (!this.start.compareAndSet(false, true)) {
            return;
        }
        for (final ShutdownAbleThread serviceThread : this.serviceList) {
            serviceThread.start();
        }
    }

    @Override
    public void shutdown() {
        if (!this.start.compareAndSet(true, false)) {
            return;
        }
        try {
            this.allocateService.shutdown(true);
        } catch (final Exception e) {
            LOG.error("Error on shutdown {}'s serviceManager,", this.abstractDB.getDBName(), e);
        }
    }

    public AllocateFileService getAllocateService() {
        return this.allocateService;
    }

}
