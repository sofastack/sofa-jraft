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
package com.alipay.sofa.jraft.storage.factory;

import com.alipay.sofa.jraft.option.StoreOptions;
import com.alipay.sofa.jraft.storage.db.AbstractDB;
import com.alipay.sofa.jraft.storage.file.AbstractFile;
import com.alipay.sofa.jraft.storage.file.FileManager;
import com.alipay.sofa.jraft.storage.file.FileType;
import com.alipay.sofa.jraft.storage.file.index.IndexFile;
import com.alipay.sofa.jraft.storage.file.segment.SegmentFile;
import com.alipay.sofa.jraft.storage.service.AllocateFileService;
import com.alipay.sofa.jraft.storage.service.ServiceManager;

/**
 * The factory that provides uniform construction functions
 * @author hzh (642256541@qq.com)
 */
public class LogStoreFactory {
    private final StoreOptions storeOptions;

    public LogStoreFactory(final StoreOptions opts) {
        this.storeOptions = opts;
    }

    /**
     * Create new file(index/segment/conf)
     */
    public AbstractFile newFile(final FileType fileType, final String filePath) {
        return isIndex(fileType) ? new IndexFile(filePath, this.storeOptions.getIndexFileSize()) : //
            isConf(fileType) ? new SegmentFile(filePath, this.storeOptions.getConfFileSize()) : //
                new SegmentFile(filePath, this.storeOptions.getSegmentFileSize());
    }

    /**
     * Create new fileManager(index/segment/conf)
     */
    public FileManager newFileManager(final FileType fileType, final String storePath,
                                      final AllocateFileService allocateService) {
        final FileManager.FileManagerBuilder fileManagerBuilder = FileManager.newBuilder() //
            .fileType(fileType) //
            .fileSize(getFileSize(fileType)) //
            .storePath(storePath) //
            .logStoreFactory(this) //
            .allocateService(allocateService);
        return fileManagerBuilder.build();
    }

    /**
     * Create new serviceManager
     */
    public ServiceManager newServiceManager(final AbstractDB abstractDB) {
        return new ServiceManager(abstractDB);
    }

    /**
     * Create new allocateFileService
     */
    public AllocateFileService newAllocateService(final AbstractDB abstractDB) {
        return new AllocateFileService(abstractDB, this);
    }

    public int getFileSize(final FileType fileType) {
        return isIndex(fileType) ? this.storeOptions.getIndexFileSize() : //
            isConf(fileType) ? this.storeOptions.getConfFileSize() : //
                this.storeOptions.getSegmentFileSize();
    }

    private boolean isIndex(final FileType fileType) {
        return fileType == FileType.FILE_INDEX;
    }

    private boolean isConf(final FileType fileType) {
        return fileType == FileType.FILE_CONFIGURATION;
    }

    public StoreOptions getStoreOptions() {
        return this.storeOptions;
    }

}
