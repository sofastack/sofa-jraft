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

import org.rocksdb.util.SizeUnit;

import com.alipay.sofa.jraft.util.SystemPropertyUtil;

/**
 * @author jiachun.fjc
 */
public final class RocksConfigs {

    /** Env options ***************************************************************************************************/

    // Sets the number of background worker threads of the flush pool for this
    // environment.
    public static int        ENV_BACKGROUND_FLUSH_THREADS       = SystemPropertyUtil.getInt(
                                                                    "rhea.rocks.env.env_background_flush_threads", 2);

    // Sets the number of background worker threads of the compaction pool for this
    // environment.
    public static int        ENV_BACKGROUND_COMPACTION_THREADS  = SystemPropertyUtil.getInt(
                                                                    "rhea.rocks.env.env_background_compaction_threads",
                                                                    4);

    /** RocksDB options ***********************************************************************************************/

    // max_open_files -- RocksDB keeps all file descriptors in a table cache. If number
    // of file descriptors exceeds max_open_files, some files are evicted from table cache
    // and their file descriptors closed. This means that every read must go through the
    // table cache to lookup the file needed. Set max_open_files to -1 to always keep all
    // files open, which avoids expensive table cache calls.
    public static final int  MAX_OPEN_FILES                     = SystemPropertyUtil.getInt(
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
    public static final int  MAX_BACKGROUND_JOBS                = SystemPropertyUtil.getInt(
                                                                    "rhea.rocks.options.max_background_jobs", 6);

    // Specifies the maximum size of RocksDB's info log file. If the current log
    // file is larger than `max_log_file_size`, a new info log file will be created.
    // If 0, all logs will be written to one log file.
    public static final long MAX_LOG_FILE_SIZE                  = SystemPropertyUtil.getLong(
                                                                    "rhea.rocks.options.max_log_file_size",
                                                                    1024 * 1024 * 1024L);

    // Flushing options:
    // write_buffer_size sets the size of a single mem_table. Once mem_table exceeds
    // this size, it is marked immutable and a new one is created.
    public static final long WRITE_BUFFER_SIZE                  = SystemPropertyUtil.getLong(
                                                                    "rhea.rocks.options.write_buffer_size",
                                                                    64 * SizeUnit.MB);

    // Flushing options:
    // max_write_buffer_number sets the maximum number of mem_tables, both active
    // and immutable.  If the active mem_table fills up and the total number of
    // mem_tables is larger than max_write_buffer_number we stall further writes.
    // This may happen if the flush process is slower than the write rate.
    public static final int  MAX_WRITE_BUFFER_NUMBER            = SystemPropertyUtil.getInt(
                                                                    "rhea.rocks.options.max_write_buffer_number", 5);

    // Flushing options:
    // min_write_buffer_number_to_merge is the minimum number of mem_tables to be
    // merged before flushing to storage. For example, if this option is set to 2,
    // immutable mem_tables are only flushed when there are two of them - a single
    // immutable mem_table will never be flushed.  If multiple mem_tables are merged
    // together, less data may be written to storage since two updates are merged to
    // a single key. However, every Get() must traverse all immutable mem_tables
    // linearly to check if the key is there. Setting this option too high may hurt
    // read performance.
    public static final int  MIN_WRITE_BUFFER_NUMBER_TO_MERGE   = SystemPropertyUtil
                                                                    .getInt(
                                                                        "rhea.rocks.options.min_write_buffer_number_to_merge",
                                                                        1);

    // Level Style Compaction:
    // level0_file_num_compaction_trigger -- Once level 0 reaches this number of
    // files, L0->L1 compaction is triggered. We can therefore estimate level 0
    // size in stable state as
    // write_buffer_size * min_write_buffer_number_to_merge * level0_file_num_compaction_trigger.
    public static final int  LEVEL0_FILE_NUM_COMPACTION_TRIGGER = SystemPropertyUtil
                                                                    .getInt(
                                                                        "rhea.rocks.options.level0_file_num_compaction_trigger",
                                                                        10);

    // Level Style Compaction:
    // max_bytes_for_level_base and max_bytes_for_level_multiplier
    //  -- max_bytes_for_level_base is total size of level 1. As mentioned, we
    // recommend that this be around the size of level 0. Each subsequent level
    // is max_bytes_for_level_multiplier larger than previous one. The default
    // is 10 and we do not recommend changing that.
    public static final long MAX_BYTES_FOR_LEVEL_BASE           = SystemPropertyUtil.getLong(
                                                                    "rhea.rocks.options.max_bytes_for_level_base",
                                                                    512 * SizeUnit.MB);

    // Level Style Compaction:
    // target_file_size_base and target_file_size_multiplier
    //  -- Files in level 1 will have target_file_size_base bytes. Each next
    // level's file size will be target_file_size_multiplier bigger than previous
    // one. However, by default target_file_size_multiplier is 1, so files in all
    // L1..LMax levels are equal. Increasing target_file_size_base will reduce total
    // number of database files, which is generally a good thing. We recommend setting
    // target_file_size_base to be max_bytes_for_level_base / 10, so that there are
    // 10 files in level 1.
    public static final long TARGET_FILE_SIZE_BASE              = SystemPropertyUtil.getLong(
                                                                    "rhea.rocks.options.target_file_size_base",
                                                                    64 * SizeUnit.MB);

    // Soft limit on number of level-0 files. We start slowing down writes at this
    // point. A value 0 means that no writing slow down will be triggered by number
    // of files in level-0.
    public static final int  LEVEL0_SLOWDOWN_WRITES_TRIGGER     = SystemPropertyUtil
                                                                    .getInt(
                                                                        "rhea.rocks.options.level0_slowdown_writes_trigger",
                                                                        20);

    // Maximum number of level-0 files.  We stop writes at this point.
    public static final int  LEVEL0_STOP_WRITES_TRIGGER         = SystemPropertyUtil
                                                                    .getInt(
                                                                        "rhea.rocks.options.level0_stop_writes_trigger",
                                                                        40);

    /** User-Defined options ******************************************************************************************/
    // The maximum number of keys in once batch write
    public static final int  MAX_BATCH_WRITE_SIZE               = SystemPropertyUtil.getInt(
                                                                    "rhea.rocks.user.max_batch_write_size", 128);

    private RocksConfigs() {
    }
}
