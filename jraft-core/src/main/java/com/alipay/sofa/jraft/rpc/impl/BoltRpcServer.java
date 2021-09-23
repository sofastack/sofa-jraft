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
package com.alipay.sofa.jraft.rpc.impl;

import java.util.concurrent.Executor;

import com.alipay.remoting.AsyncContext;
import com.alipay.remoting.BizContext;
import com.alipay.remoting.ConnectionEventType;
import com.alipay.remoting.config.BoltClientOption;
import com.alipay.remoting.config.switches.GlobalSwitch;
import com.alipay.remoting.rpc.protocol.AsyncUserProcessor;
import com.alipay.sofa.jraft.rpc.Connection;
import com.alipay.sofa.jraft.rpc.RpcContext;
import com.alipay.sofa.jraft.rpc.RpcProcessor;
import com.alipay.sofa.jraft.rpc.RpcServer;
import com.alipay.sofa.jraft.util.Requires;

/**
 * Bolt RPC server impl.
 *
 * @author jiachun.fjc
 */
public class BoltRpcServer implements RpcServer {

    private final com.alipay.remoting.rpc.RpcServer rpcServer;

    public BoltRpcServer(final com.alipay.remoting.rpc.RpcServer rpcServer) {
        this.rpcServer = Requires.requireNonNull(rpcServer, "rpcServer");
    }

    @Override
    public boolean init(final Void opts) {
        this.rpcServer.option(BoltClientOption.NETTY_FLUSH_CONSOLIDATION, true);
        this.rpcServer.initWriteBufferWaterMark(BoltRaftRpcFactory.CHANNEL_WRITE_BUF_LOW_WATER_MARK,
            BoltRaftRpcFactory.CHANNEL_WRITE_BUF_HIGH_WATER_MARK);
        this.rpcServer.startup();
        return this.rpcServer.isStarted();
    }

    @Override
    public void shutdown() {
        this.rpcServer.shutdown();
    }

    @Override
    public void registerConnectionClosedEventListener(final ConnectionClosedEventListener listener) {
        this.rpcServer.addConnectionEventProcessor(ConnectionEventType.CLOSE, (remoteAddress, conn) -> {
            final Connection proxyConn = conn == null ? null : new Connection() {

                @Override
                public Object getAttribute(final String key) {
                    return conn.getAttribute(key);
                }

                @Override
                public Object setAttributeIfAbsent(final String key, final Object value) {
                    return conn.setAttributeIfAbsent(key, value);
                }

                @Override
                public void setAttribute(final String key, final Object value) {
                    conn.setAttribute(key, value);
                }

                @Override
                public void close() {
                    conn.close();
                }
            };

            listener.onClosed(remoteAddress, proxyConn);
        });
    }

    @Override
    public int boundPort() {
        return this.rpcServer.port();
    }

    @Override
    public void registerProcessor(final RpcProcessor processor) {
        this.rpcServer.registerUserProcessor(new AsyncUserProcessor<Object>() {

            @SuppressWarnings("unchecked")
            @Override
            public void handleRequest(final BizContext bizCtx, final AsyncContext asyncCtx, final Object request) {
                final RpcContext rpcCtx = new RpcContext() {

                    @Override
                    public void sendResponse(final Object responseObj) {
                        asyncCtx.sendResponse(responseObj);
                    }

                    @Override
                    public Connection getConnection() {
                        com.alipay.remoting.Connection conn = bizCtx.getConnection();
                        if (conn == null) {
                            return null;
                        }
                        return new BoltConnection(conn);
                    }

                    @Override
                    public String getRemoteAddress() {
                        return bizCtx.getRemoteAddress();
                    }
                };

                processor.handleRequest(rpcCtx, request);
            }

            @Override
            public String interest() {
                return processor.interest();
            }

            @Override
            public ExecutorSelector getExecutorSelector() {
                final RpcProcessor.ExecutorSelector realSelector = processor.executorSelector();
                if (realSelector == null) {
                    return null;
                }
                return realSelector::select;
            }

            @Override
            public Executor getExecutor() {
                return processor.executor();
            }
        });
    }

    public com.alipay.remoting.rpc.RpcServer getServer() {
        return this.rpcServer;
    }

    private static class BoltConnection implements Connection {

        private final com.alipay.remoting.Connection conn;

        private BoltConnection(final com.alipay.remoting.Connection conn) {
            this.conn = Requires.requireNonNull(conn, "conn");
        }

        @Override
        public Object getAttribute(final String key) {
            return this.conn.getAttribute(key);
        }

        @Override
        public Object setAttributeIfAbsent(final String key, final Object value) {
            return this.conn.setAttributeIfAbsent(key, value);
        }

        @Override
        public void setAttribute(final String key, final Object value) {
            this.conn.setAttribute(key, value);
        }

        @Override
        public void close() {
            this.conn.close();
        }
    }
}
