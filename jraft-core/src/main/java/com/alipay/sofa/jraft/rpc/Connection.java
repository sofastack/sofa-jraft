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
package com.alipay.sofa.jraft.rpc;

/**
 *
 * RPC connection
 * @author jiachun.fjc
 */
public interface Connection {

    /**
     * Get the attribute that bound to the connection.
     *
     * @param key the attribute key
     * @return the attribute value
     */
    Object getAttribute(final String key);

    /**
     * Set the attribute to the connection.
     *
     * @param key   the attribute key
     * @param value the attribute value
     */
    void setAttribute(final String key, final Object value);

    /**
     * Set the attribute to the connection if the key's item doesn't exist, otherwise returns the present item.
     *
     * @param key   the attribute key
     * @param value the attribute value
     * @return the previous value associated with the specified key, or
     *         <tt>null</tt> if there was no mapping for the key.
     */
    Object setAttributeIfAbsent(final String key, final Object value);

    /**
     * Close the connection.
     */
    void close();
}
