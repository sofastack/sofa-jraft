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

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.UnknownHostException;
import java.util.Enumeration;
import java.util.regex.Pattern;

import com.alipay.sofa.jraft.util.internal.ThrowUtil;

/**
 *
 * @author jiachun.fjc
 */
public final class NetUtil {

    private static final Pattern IP_PATTERN = Pattern.compile("\\d{1,3}(\\.\\d{1,3}){3}$");
    private static final String  LOCAL_IP_ADDRESS;

    static {
        InetAddress localAddress;
        try {
            localAddress = InetAddress.getLocalHost();
        } catch (UnknownHostException e) {
            localAddress = null;
        }

        if (localAddress != null && isValidAddress(localAddress)) {
            LOCAL_IP_ADDRESS = localAddress.getHostAddress();
        } else {
            LOCAL_IP_ADDRESS = getFirstLocalAddress();
        }
    }

    public static String getLocalAddress() {
        return LOCAL_IP_ADDRESS;
    }

    public static String getLocalHostName() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (final UnknownHostException e) {
            ThrowUtil.throwException(e);
        }
        return null; // never get here
    }

    /**
     * Gets the fully qualified domain name for this IP address.
     * Best effort method, meaning we may not be able to return
     * the FQDN depending on the underlying system configuration.
     *
     * @return the fully qualified domain name for this IP address,
     * or if the operation is not allowed by the security check,
     * the textual representation of the IP address.
     */
    public static String getLocalCanonicalHostName() {
        try {
            return InetAddress.getLocalHost().getCanonicalHostName();
        } catch (final UnknownHostException ignored) {
            return getLocalHostName();
        }
    }

    /**
     * Gets the first valid IP in the NIC
     */
    private static String getFirstLocalAddress() {
        try {
            final Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
            while (interfaces.hasMoreElements()) {
                final NetworkInterface ni = interfaces.nextElement();
                final Enumeration<InetAddress> addresses = ni.getInetAddresses();
                while (addresses.hasMoreElements()) {
                    InetAddress address = addresses.nextElement();
                    if (!address.isLoopbackAddress() && !address.getHostAddress().contains(":")) {
                        return address.getHostAddress();
                    }
                }
            }
        } catch (final Throwable ignored) {
            // ignored
        }

        return "127.0.0.1";
    }

    private static boolean isValidAddress(final InetAddress address) {
        if (address.isLoopbackAddress()) {
            return false;
        }

        final String name = address.getHostAddress();
        return (name != null && !"0.0.0.0".equals(name) && !"127.0.0.1".equals(name) && IP_PATTERN.matcher(name)
            .matches());
    }

    private NetUtil() {
    }
}
