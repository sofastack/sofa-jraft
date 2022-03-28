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

import java.nio.file.Paths;

import org.apache.commons.lang.StringUtils;

import com.alipay.sofa.jraft.option.RaftOptions;
import com.alipay.sofa.jraft.option.StoreOptions;
import com.alipay.sofa.jraft.storage.HybridLogStorage;
import com.alipay.sofa.jraft.storage.LogStorage;
import com.alipay.sofa.jraft.util.Requires;
import com.alipay.sofa.jraft.util.SPI;

/**
 * Extends from DefaultJRaftServiceFactory, Overwrite createLogStorage() to create a HybridLogStorage
 * @author hzh (642256541@qq.com)
 */
@SPI(priority = 10)
public class HybridLogJRaftServiceFactory extends DefaultJRaftServiceFactory {
    public static final String NEW_STORAGE_PATH = "LogitStorage";

    @Override
    public LogStorage createLogStorage(final String uri, final RaftOptions raftOptions) {
        Requires.requireTrue(StringUtils.isNotBlank(uri), "Blank log storage uri.");

        // Create old storage if needed
        LogStorage oldStorage = null;
        if (raftOptions.isStartupOldStorage()) {
            oldStorage = super.createLogStorage(uri, raftOptions);
        }

        String newStoragePath = Paths.get(uri, NEW_STORAGE_PATH).toString();
        return new HybridLogStorage(newStoragePath, new StoreOptions(), oldStorage);
    }

}
