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
package com.alipay.sofa.jraft.test;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import com.alipay.sofa.jraft.JRaftUtils;
import com.alipay.sofa.jraft.conf.ConfigurationEntry;
import com.alipay.sofa.jraft.entity.EnumOutter;
import com.alipay.sofa.jraft.entity.LogEntry;
import com.alipay.sofa.jraft.entity.LogId;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.rpc.RpcRequests;
import com.alipay.sofa.jraft.util.Endpoint;

/**
 * Test helper
 *
 * @author boyan (boyan@alibaba-inc.com)
 *
 *         2018-Apr-11 10:16:07 AM
 */
public class TestUtils {

    public static ConfigurationEntry getConfEntry(final String confStr, final String oldConfStr) {
        ConfigurationEntry entry = new ConfigurationEntry();
        entry.setConf(JRaftUtils.getConfiguration(confStr));
        entry.setOldConf(JRaftUtils.getConfiguration(oldConfStr));
        return entry;
    }

    public static void dumpThreads() {
        try {
            ThreadMXBean bean = ManagementFactory.getThreadMXBean();
            ThreadInfo[] infos = bean.dumpAllThreads(true, true);
            for (ThreadInfo info : infos) {
                System.out.println(info);
            }
        } catch (Throwable t) {
            t.printStackTrace(); // NOPMD
        }
    }

    public static String mkTempDir() {
        return Paths.get(System.getProperty("java.io.tmpdir", "/tmp"), "jraft_test_" + System.nanoTime()).toString();
    }

    public static LogEntry mockEntry(final int index, final int term) {
        return mockEntry(index, term, 0);
    }

    public static LogEntry mockEntry(final int index, final int term, final int dataSize) {
        LogEntry entry = new LogEntry(EnumOutter.EntryType.ENTRY_TYPE_NO_OP);
        entry.setId(new LogId(index, term));
        if (dataSize > 0) {
            byte[] bs = new byte[dataSize];
            ThreadLocalRandom.current().nextBytes(bs);
            entry.setData(ByteBuffer.wrap(bs));
        }
        return entry;
    }

    public static List<LogEntry> mockEntries() {
        return mockEntries(10);
    }

    public static String getMyIp() {
        String ip = null;
        try {
            Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
            while (interfaces.hasMoreElements()) {
                NetworkInterface iface = interfaces.nextElement();
                // filters out 127.0.0.1 and inactive interfaces
                if (iface.isLoopback() || !iface.isUp()) {
                    continue;
                }

                Enumeration<InetAddress> addresses = iface.getInetAddresses();
                while (addresses.hasMoreElements()) {
                    InetAddress addr = addresses.nextElement();
                    if (addr instanceof Inet4Address) {
                        ip = addr.getHostAddress();
                        break;
                    }
                }
            }
            return ip;
        } catch (SocketException e) {
            return "localhost";
        }
    }

    public static List<LogEntry> mockEntries(final int n) {
        List<LogEntry> entries = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            LogEntry entry = mockEntry(i, i);
            if (i > 0) {
                entry.setData(ByteBuffer.wrap(String.valueOf(i).getBytes()));
            }
            entries.add(entry);
        }
        return entries;
    }

    public static RpcRequests.PingRequest createPingRequest() {
        RpcRequests.PingRequest reqObject = RpcRequests.PingRequest.newBuilder()
            .setSendTimestamp(System.currentTimeMillis()).build();
        return reqObject;
    }

    public static final int INIT_PORT = 5003;

    public static List<PeerId> generatePeers(final int n) {
        List<PeerId> ret = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            ret.add(new PeerId(getMyIp(), INIT_PORT + i));
        }
        return ret;
    }

    public static List<PeerId> generatePriorityPeers(final int n, final List<Integer> priorities) {
        List<PeerId> ret = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            Endpoint endpoint = new Endpoint(getMyIp(), INIT_PORT + i);
            PeerId peerId = new PeerId(endpoint, 0, priorities.get(i));
            ret.add(peerId);
        }
        return ret;
    }

    public static byte[] getRandomBytes() {
        final byte[] requestContext = new byte[ThreadLocalRandom.current().nextInt(10) + 1];
        ThreadLocalRandom.current().nextBytes(requestContext);
        return requestContext;
    }
}
