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

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author jiachun.fjc
 */
public class UniqueIdUtil {

    private static final Logger     logger         = LoggerFactory.getLogger(UniqueIdUtil.class);

    // maximal value for 64bit systems is 2^22, see man 5 proc.
    private static final int        MAX_PROCESS_ID = 4194304;
    private static final char       PID_FLAG       = 'd';
    private static final String     IP_16;
    private static final String     PID;
    private static final long       ID_BASE        = 1000;
    private static final long       ID_MASK        = (1 << 13) - 1;                              // 8192 - 1
    private static final AtomicLong sequence       = new AtomicLong();

    static {
        String ip16;
        try {
            final String ip = NetUtil.getLocalAddress();
            ip16 = getIp16(ip);
        } catch (final Throwable t) {
            ip16 = "ffffffff";
        }
        IP_16 = ip16;

        String pid;
        try {
            pid = getHexProcessId(getProcessId());
        } catch (final Throwable t) {
            pid = "0000";
        }
        PID = pid;
    }

    public static String generateId() {
        return getId(IP_16, Clock.defaultClock().getTime(), getNextId());
    }

    private static String getHexProcessId(int pid) {
        // unsigned short 0 to 65535
        if (pid < 0) {
            pid = 0;
        }
        if (pid > 65535) {
            String strPid = Integer.toString(pid);
            strPid = strPid.substring(strPid.length() - 4);
            pid = Integer.parseInt(strPid);
        }
        final StringBuilder buf = new StringBuilder(Integer.toHexString(pid));
        while (buf.length() < 4) {
            buf.insert(0, "0");
        }
        return buf.toString();
    }

    /**
     * Gets current pid, max pid 32 bit systems 32768, for 64 bit 4194304
     * http://unix.stackexchange.com/questions/16883/what-is-the-maximum-value-of-the-pid-of-a-process
     * http://stackoverflow.com/questions/35842/how-can-a-java-program-get-its-own-process-id
     */
    private static int getProcessId() {
        String value = "";
        try {
            final RuntimeMXBean runtime = ManagementFactory.getRuntimeMXBean();
            value = runtime.getName();
        } catch (final Throwable t) {
            if (logger.isDebugEnabled()) {
                logger.debug("Could not invoke ManagementFactory.getRuntimeMXBean().getName(), {}.",
                    StackTraceUtil.stackTrace(t));
            }
        }

        // something like '<pid>@<hostname>', at least in SUN / Oracle JVMs
        final int atIndex = value.indexOf('@');
        if (atIndex >= 0) {
            value = value.substring(0, atIndex);
        }

        int pid = -1;
        try {
            pid = Integer.parseInt(value);
        } catch (final NumberFormatException ignored) {
            // value did not contain an integer
        }

        if (pid < 0 || pid > MAX_PROCESS_ID) {
            pid = ThreadLocalRandom.current().nextInt(MAX_PROCESS_ID + 1);

            logger.warn("Failed to find the current process ID from '{}'; using a random value: {}.", value, pid);
        }

        return pid;
    }

    private static String getIp16(final String ip) {
        final String[] segments = ip.split("\\.");
        final StringBuilder buf = StringBuilderHelper.get();
        for (final String s : segments) {
            final String hex = Integer.toHexString(Integer.parseInt(s));
            if (hex.length() == 1) {
                buf.append('0');
            }
            buf.append(hex);
        }
        return buf.toString();
    }

    @SuppressWarnings("SameParameterValue")
    private static String getId(final String ip16, final long timestamp, final long nextId) {
        return StringBuilderHelper.get() //
            .append(ip16) //
            .append(timestamp) //
            .append(nextId) //
            .append(PID_FLAG) //
            .append(PID) //
            .toString();
    }

    private static long getNextId() {
        // (1000 + 1) ~ (1000 + 8191)
        return (sequence.incrementAndGet() & ID_MASK) + ID_BASE;
    }
}
