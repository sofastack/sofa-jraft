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

import org.rocksdb.HistogramData;
import org.rocksdb.HistogramType;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.Statistics;
import org.rocksdb.TickerType;

import com.alipay.sofa.jraft.rhea.storage.RocksRawKVStore;
import com.alipay.sofa.jraft.util.internal.ThrowUtil;
import com.alipay.sofa.jraft.util.DebugStatistics;
import com.alipay.sofa.jraft.util.internal.ReferenceFieldUpdater;
import com.alipay.sofa.jraft.util.internal.Updaters;

/**
 * @author jiachun.fjc
 */
public final class RocksStatistics {

    private static final ReferenceFieldUpdater<RocksRawKVStore, DebugStatistics> statisticsGetter = Updaters
                                                                                                      .newReferenceFieldUpdater(
                                                                                                          RocksRawKVStore.class,
                                                                                                          "statistics");
    private static final ReferenceFieldUpdater<RocksRawKVStore, RocksDB>         dbGetter         = Updaters
                                                                                                      .newReferenceFieldUpdater(
                                                                                                          RocksRawKVStore.class,
                                                                                                          "db");

    /**
     * Get the count for a ticker.
     */
    public static long getTickerCount(final RocksRawKVStore rocksRawKVStore, final TickerType tickerType) {
        final Statistics statistics = statistics(rocksRawKVStore);
        if (statistics == null) {
            return -1L;
        }
        return statistics.getTickerCount(tickerType);
    }

    /**
     * Get the count for a ticker and reset the tickers count.
     */
    public static long getAndResetTickerCount(final RocksRawKVStore rocksRawKVStore, final TickerType tickerType) {
        final Statistics statistics = statistics(rocksRawKVStore);
        if (statistics == null) {
            return -1L;
        }
        return statistics.getAndResetTickerCount(tickerType);
    }

    /**
     * Gets the histogram data for a particular histogram.
     */
    public static HistogramData getHistogramData(final RocksRawKVStore rocksRawKVStore,
                                                 final HistogramType histogramType) {
        final Statistics statistics = statistics(rocksRawKVStore);
        if (statistics == null) {
            return null;
        }
        return statistics.getHistogramData(histogramType);
    }

    /**
     * Gets a string representation of a particular histogram.
     */
    public String getHistogramString(final RocksRawKVStore rocksRawKVStore, final HistogramType histogramType) {
        final Statistics statistics = statistics(rocksRawKVStore);
        if (statistics == null) {
            return "";
        }
        return statistics.getHistogramString(histogramType);
    }

    /**
     * String representation of the statistic.
     */
    public static String getStatisticsString(final RocksRawKVStore rocksRawKVStore) {
        final Statistics statistics = statistics(rocksRawKVStore);
        if (statistics == null) {
            return "";
        }
        return statistics.toString();
    }

    /**
     * DB implementations can export properties about their state
     * via this method.
     */
    public static String getProperty(final RocksRawKVStore rocksRawKVStore, final String name) {
        final RocksDB db = db(rocksRawKVStore);
        if (db != null) {
            try {
                return db.getProperty(name);
            } catch (final RocksDBException e) {
                ThrowUtil.throwException(e);
            }
        }
        throw new NullPointerException("db");
    }

    private static Statistics statistics(final RocksRawKVStore rocksRawKVStore) {
        return statisticsGetter.get(rocksRawKVStore);
    }

    private static RocksDB db(final RocksRawKVStore rocksRawKVStore) {
        return dbGetter.get(rocksRawKVStore);
    }

    private RocksStatistics() {
    }
}
