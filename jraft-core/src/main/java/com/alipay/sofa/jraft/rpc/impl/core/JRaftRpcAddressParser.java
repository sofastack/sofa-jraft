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
package com.alipay.sofa.jraft.rpc.impl.core;

import java.lang.ref.SoftReference;
import java.util.Properties;

import com.alipay.remoting.RemotingAddressParser;
import com.alipay.remoting.Url;
import com.alipay.remoting.rpc.RpcAddressParser;
import com.alipay.remoting.util.StringUtils;
import com.alipay.sofa.jraft.util.Requires;

/**
 * @author jiachun.fjc
 */
public class JRaftRpcAddressParser extends RpcAddressParser {

    /**
     * @see com.alipay.remoting.RemotingAddressParser#parse(java.lang.String)
     */
    @Override
    public Url parse(final String url) {
        if (StringUtils.isBlank(url)) {
            throw new IllegalArgumentException("Illegal format address string [" + url + "], should not be blank! ");
        }
        Url parsedUrl = tryGet(url);
        if (parsedUrl != null) {
            return parsedUrl;
        }
        String ip = null;
        String port = null;
        Properties properties = null;

        final int size = url.length();
        int pos = 0;
        for (int i = 0; i < size; ++i) {
            if (COLON == url.charAt(i)) {
                ip = url.substring(pos, i);
                pos = i;
                // should not end with COLON
                if (i == size - 1) {
                    throw new IllegalArgumentException("Illegal format address string [" + url
                                                       + "], should not end with COLON[:]! ");
                }
                break;
            }
            // must have one COLON
            if (i == size - 1) {
                throw new IllegalArgumentException("Illegal format address string [" + url
                                                   + "], must have one COLON[:]! ");
            }
        }

        for (int i = pos; i < size; ++i) {
            if (QUES == url.charAt(i)) {
                port = url.substring(pos + 1, i);
                pos = i;
                if (i == size - 1) {
                    // should not end with QUES
                    throw new IllegalArgumentException("Illegal format address string [" + url
                                                       + "], should not end with QUES[?]! ");
                }
                break;
            }
            // end without a QUES
            if (i == size - 1) {
                port = url.substring(pos + 1, i + 1);
                pos = size;
            }
        }

        if (pos < (size - 1)) {
            properties = new Properties();
            while (pos < (size - 1)) {
                String key = null;
                String value = null;
                for (int i = pos; i < size; ++i) {
                    if (EQUAL == url.charAt(i)) {
                        key = url.substring(pos + 1, i);
                        pos = i;
                        if (i == size - 1) {
                            // should not end with EQUAL
                            throw new IllegalArgumentException("Illegal format address string [" + url
                                                               + "], should not end with EQUAL[=]! ");
                        }
                        break;
                    }
                    if (i == size - 1) {
                        // must have one EQUAL
                        throw new IllegalArgumentException("Illegal format address string [" + url
                                                           + "], must have one EQUAL[=]! ");
                    }
                }
                for (int i = pos; i < size; ++i) {
                    if (AND == url.charAt(i)) {
                        value = url.substring(pos + 1, i);
                        pos = i;
                        if (i == size - 1) {
                            // should not end with AND
                            throw new IllegalArgumentException("Illegal format address string [" + url
                                                               + "], should not end with AND[&]! ");
                        }
                        break;
                    }
                    // end without more AND
                    if (i == size - 1) {
                        value = url.substring(pos + 1, i + 1);
                        pos = size;
                    }
                }
                if (key != null && value != null) {
                    properties.put(key, value);
                }
            }
        }
        Requires.requireNonNull(port, "port is null");
        // isolated the bolt connection pool
        // uniqueKey = ip:port:jraft
        final String uniqueKey = ip + RemotingAddressParser.COLON + port + RemotingAddressParser.COLON + "jraft";
        parsedUrl = new Url(url, ip, Integer.parseInt(port), uniqueKey, properties);
        initUrlArgs(parsedUrl);
        Url.parsedUrls.put(url, new SoftReference<>(parsedUrl));
        return parsedUrl;
    }

    /**
     * try get from cache
     */
    private Url tryGet(final String url) {
        final SoftReference<Url> softRef = Url.parsedUrls.get(url);
        return (softRef == null) ? null : softRef.get();
    }
}
