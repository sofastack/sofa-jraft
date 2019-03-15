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
package com.alipay.sofa.jraft.rhea.util;

import com.alipay.sofa.jraft.rhea.metadata.Region;
import com.alipay.sofa.jraft.util.BytesUtil;

/**
 *
 * @author jiachun.fjc
 */
public final class RegionHelper {

    public static boolean isSingleGroup(final Region region) {
        return BytesUtil.nullToEmpty(region.getStartKey()).length == 0
               && BytesUtil.nullToEmpty(region.getEndKey()).length == 0;
    }

    public static boolean isMultiGroup(final Region region) {
        return !isSingleGroup(region);
    }

    public static boolean isSameRange(final Region r1, final Region r2) {
        if (BytesUtil.compare(BytesUtil.nullToEmpty(r1.getStartKey()), BytesUtil.nullToEmpty(r2.getStartKey())) != 0) {
            return false;
        }
        return BytesUtil.compare(BytesUtil.nullToEmpty(r1.getEndKey()), BytesUtil.nullToEmpty(r2.getEndKey())) == 0;
    }

    public static boolean isKeyInRegion(final byte[] key, final Region region) {
        final byte[] startKey = BytesUtil.nullToEmpty(region.getStartKey());
        if (BytesUtil.compare(key, startKey) < 0) {
            return false;
        }
        final byte[] endKey = region.getEndKey();
        return endKey == null || BytesUtil.compare(key, endKey) < 0;
    }

    private RegionHelper() {
    }
}
