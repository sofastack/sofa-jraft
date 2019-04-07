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
package com.alipay.sofa.jraft.rhea.errors;

/**
 * @author jiachun.fjc
 */
public final class ErrorsHelper {

    // require refresh leader or peer
    public static boolean isInvalidPeer(final Errors error) {
        return error == Errors.CALL_SELF_ENDPOINT_ERROR //
               || error == Errors.NOT_LEADER //
               || error == Errors.NO_REGION_FOUND //
               || error == Errors.LEADER_NOT_AVAILABLE;
    }

    // require refresh region route table
    public static boolean isInvalidEpoch(final Errors error) {
        return error == Errors.INVALID_REGION_MEMBERSHIP //
               || error == Errors.INVALID_REGION_VERSION //
               || error == Errors.INVALID_REGION_EPOCH;
    }

    private ErrorsHelper() {
    }
}
