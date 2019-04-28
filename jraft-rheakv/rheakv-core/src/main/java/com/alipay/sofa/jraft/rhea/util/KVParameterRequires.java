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

import java.util.List;

import com.alipay.sofa.jraft.rhea.cmd.store.BaseRequest;
import com.alipay.sofa.jraft.rhea.errors.Errors;
import com.alipay.sofa.jraft.rhea.errors.InvalidParameterException;
import com.alipay.sofa.jraft.rhea.metadata.RegionEpoch;

/**
 *
 * @author jiachun.fjc
 */
public final class KVParameterRequires {

    public static void requireSameEpoch(final BaseRequest request, final RegionEpoch current) {
        RegionEpoch requestEpoch = request.getRegionEpoch();
        if (current.equals(requestEpoch)) {
            return;
        }
        if (current.getConfVer() != requestEpoch.getConfVer()) {
            throw Errors.INVALID_REGION_MEMBERSHIP.exception();
        }
        if (current.getVersion() != requestEpoch.getVersion()) {
            throw Errors.INVALID_REGION_VERSION.exception();
        }
        throw Errors.INVALID_REGION_EPOCH.exception();
    }

    public static <T> T requireNonNull(final T target, final String message) {
        if (target == null) {
            throw new InvalidParameterException(message);
        }
        return target;
    }

    public static <T> List<T> requireNonEmpty(final List<T> target, final String message) {
        requireNonNull(target, message);
        if (target.isEmpty()) {
            throw new InvalidParameterException(message);
        }
        return target;
    }

    public static int requireNonNegative(final int value, final String message) {
        if (value < 0) {
            throw new InvalidParameterException(message);
        }
        return value;
    }

    public static long requireNonNegative(final long value, final String message) {
        if (value < 0) {
            throw new InvalidParameterException(message);
        }
        return value;
    }

    public static int requirePositive(final int value, final String message) {
        if (value <= 0) {
            throw new InvalidParameterException(message);
        }
        return value;
    }

    public static long requirePositive(final long value, final String message) {
        if (value <= 0) {
            throw new InvalidParameterException(message);
        }
        return value;
    }

    private KVParameterRequires() {
    }
}
