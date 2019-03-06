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

import java.util.Formatter;

import com.alipay.sofa.jraft.util.SystemPropertyUtil;

/**
 * Rhea's constants
 *
 * @author jiachun.fjc
 */
public final class Constants {

    /** 换行符 */
    public static final String  NEWLINE;

    static {
        String newLine;
        try {
            newLine = new Formatter().format("%n").toString();
        } catch (Exception e) {
            newLine = "\n";
        }
        NEWLINE = newLine;
    }

    /** ANY IP address 0.0.0.0 */
    // TODO support ipv6
    public static final String  IP_ANY                  = "0.0.0.0";

    /** CPU cores */
    public static final int     AVAILABLE_PROCESSORS    = Runtime.getRuntime().availableProcessors();

    public static final boolean THREAD_AFFINITY_ENABLED = SystemPropertyUtil.getBoolean("rhea.thread.affinity.enabled",
                                                            false);

    public static final long    DEFAULT_REGION_ID       = -1L;

    private Constants() {
    }
}
