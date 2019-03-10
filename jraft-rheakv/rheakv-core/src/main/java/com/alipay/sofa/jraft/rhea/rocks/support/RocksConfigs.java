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
package com.alipay.sofa.jraft.rhea.rocks.support;

import com.alipay.sofa.jraft.util.SystemPropertyUtil;

/**
 * @author jiachun.fjc
 */
public final class RocksConfigs {

    /** Env options ***************************************************************************************************/

    // Sets the number of background worker threads of the flush pool for this
    // environment.
    public static int        ENV_BACKGROUND_FLUSH_THREADS      = SystemPropertyUtil.getInt(
                                                                   "rhea.rocks.env.env_background_flush_threads", 2);

    // Sets the number of background worker threads of the compaction pool for this
    // environment.
    public static int        ENV_BACKGROUND_COMPACTION_THREADS = SystemPropertyUtil.getInt(
                                                                   "rhea.rocks.env.env_background_compaction_threads",
                                                                   4);

    /** RocksDB options ***********************************************************************************************/

    // max_open_files -- RocksDB keeps all file descriptors in a table cache. If number
    // of file descriptors exceeds max_open_files, some files are evicted from table cache
    // and their file descriptors closed. This means that every read must go through the
    // table cache to lookup the file needed. Set max_open_files to -1 to always keep all
    // files open, which avoids expensive table cache calls.
    public static final int  MAX_OPEN_FILES                    = SystemPropertyUtil.getInt(
                                                                   "rhea.rocks.options.max_open_files", -1);

    // 1. max_background_flushes:
    // is the maximum number of concurrent flush operations.
    //
    // 2. max_background_compactions:
    // is the maximum number of concurrent background compactions. The default is 1,
    // but to fully utilize your CPU and storage you might want to increase this to
    // approximately number of cores in the system.
    //
    // 3. max_background_jobs:
    // is the maximum number of concurrent background jobs (both flushes and compactions
    // combined).
    public static final int  MAX_BACKGROUND_JOBS               = SystemPropertyUtil.getInt(
                                                                   "rhea.rocks.options.max_background_jobs", 6);

    // Specifies the maximum size of RocksDB's info log file. If the current log
    // file is larger than `max_log_file_size`, a new info log file will be created.
    // If 0, all logs will be written to one log file.
    public static final long MAX_LOG_FILE_SIZE                 = SystemPropertyUtil.getLong(
                                                                   "rhea.rocks.options.max_log_file_size",
                                                                   1024 * 1024 * 1024L);

    /** User-Defined options ******************************************************************************************/
    // The maximum number of keys in once batch write
    public static final int  MAX_BATCH_WRITE_SIZE              = SystemPropertyUtil.getInt(
                                                                   "rhea.rocks.user.max_batch_write_size", 128);

    private RocksConfigs() {
    }
}
