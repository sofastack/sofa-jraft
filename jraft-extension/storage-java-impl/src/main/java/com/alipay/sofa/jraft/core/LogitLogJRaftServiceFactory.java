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
package com.alipay.sofa.jraft.core;

import com.alipay.sofa.jraft.logStore.LogitLogStorage;
import com.alipay.sofa.jraft.option.RaftOptions;
import com.alipay.sofa.jraft.option.StoreOptions;
import com.alipay.sofa.jraft.storage.LogStorage;

import java.nio.file.Paths;

/**
 * Extends from DefaultJRaftServiceFactory, Overwrite createLogStorage() to create a logitLogStorage
 * @author hzh (642256541@qq.com)
 */
public class LogitLogJRaftServiceFactory extends DefaultJRaftServiceFactory {

    @Override
    public LogStorage createLogStorage(final String uri, final RaftOptions raftOptions) {
        final StoreOptions storeOptions = new StoreOptions();
        final String path = Paths.get(uri, storeOptions.getStoragePath()).toString();
        return new LogitLogStorage(path, storeOptions);
    }
}
