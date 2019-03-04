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
package com.alipay.sofa.jraft.rhea;

import com.alipay.sofa.jraft.rhea.util.StringBuilderHelper;
import com.alipay.sofa.jraft.util.Endpoint;

/**
 * @author jiachun.fjc
 */
public final class MetadataKeyHelper {

    private static final char DELIMITER = '-';

    public static String getClusterInfoKey(final long clusterId) {
        return StringBuilderHelper.get() //
            .append("pd_cluster") //
            .append(DELIMITER) //
            .append(clusterId) //
            .toString();
    }

    public static String getStoreIdKey(final long clusterId, final Endpoint endpoint) {
        return StringBuilderHelper.get() //
            .append("pd_store_id_map") //
            .append(DELIMITER) //
            .append(clusterId) //
            .append(DELIMITER) //
            .append(endpoint) //
            .toString();
    }

    public static String getStoreSeqKey(final long clusterId) {
        return StringBuilderHelper.get() //
            .append("pd_store_id_seq") //
            .append(DELIMITER) //
            .append(clusterId) //
            .toString();
    }

    public static String getStoreInfoKey(final long clusterId, final long storeId) {
        return StringBuilderHelper.get() //
            .append("pd_store_info") //
            .append(DELIMITER) //
            .append(clusterId) //
            .append(DELIMITER) //
            .append(storeId) //
            .toString();
    }

    public static String getRegionSeqKey(final long clusterId) {
        return StringBuilderHelper.get() //
            .append("pd_region_id_seq") //
            .append(DELIMITER) //
            .append(clusterId) //
            .toString();
    }

    public static String getStoreStatsKey(final long clusterId, final long storeId) {
        return StringBuilderHelper.get() //
            .append("pd_store_stats") //
            .append(DELIMITER) //
            .append(clusterId) //
            .append(DELIMITER) //
            .append(storeId).toString();
    }

    public static String getRegionStatsKey(final long clusterId, final long regionId) {
        return StringBuilderHelper.get() //
            .append("pd_region_stats") //
            .append(DELIMITER) //
            .append(clusterId) //
            .append(DELIMITER) //
            .append(regionId) //
            .toString();
    }

    private MetadataKeyHelper() {
    }
}
