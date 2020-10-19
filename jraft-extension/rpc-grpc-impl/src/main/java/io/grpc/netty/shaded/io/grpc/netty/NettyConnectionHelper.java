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
package io.grpc.netty.shaded.io.grpc.netty;

import java.util.List;
import com.alipay.sofa.jraft.rpc.Connection;
import com.alipay.sofa.jraft.rpc.impl.ConnectionClosedEventListener;
import com.alipay.sofa.jraft.util.internal.ReferenceFieldUpdater;
import com.alipay.sofa.jraft.util.internal.Updaters;
import io.grpc.internal.ServerStream;
import io.grpc.netty.shaded.io.netty.channel.Channel;
import io.grpc.netty.shaded.io.netty.util.Attribute;
import io.grpc.netty.shaded.io.netty.util.AttributeKey;

/**
 * Get netty channel.
 *
 * @author jiachun.fjc
 */
public class NettyConnectionHelper {

    private static final ReferenceFieldUpdater<NettyServerStream, Channel> CHANNEL_GETTER = Updaters
                                                                                              .newReferenceFieldUpdater(
                                                                                                  NettyServerStream.class,
                                                                                                  "channel");

    private static final AttributeKey<NettyConnection>                     NETTY_CONN_KEY = AttributeKey
                                                                                              .valueOf("netty.conn");

    public static Connection getOrCreateConnection(final ServerStream stream,
                                                   final List<ConnectionClosedEventListener> listeners) {
        if (stream instanceof NettyServerStream) {
            return attachChannel(CHANNEL_GETTER.get((NettyServerStream) stream), listeners);
        }
        return null;
    }

    private static Connection attachChannel(final Channel channel, final List<ConnectionClosedEventListener> listeners) {
        if (channel == null) {
            return null;
        }

        final Attribute<NettyConnection> attr = channel.attr(NETTY_CONN_KEY);
        NettyConnection conn = attr.get();
        if (conn == null) {
            final NettyConnection newConn = new NettyConnection(channel);
            conn = attr.setIfAbsent(newConn);
            if (conn == null) {
                conn = newConn;
                for (final ConnectionClosedEventListener l : listeners) {
                    conn.addClosedEventListener(l);
                }
            }
        }

        return conn;
    }
}

class NettyConnection implements Connection {

    private final Channel ch;

    NettyConnection(final Channel ch) {
        this.ch = ch;
    }

    @Override
    public Object setAttributeIfAbsent(final String key, final Object value) {
        return this.ch.attr(AttributeKey.valueOf(key)).setIfAbsent(value);
    }

    @Override
    public Object getAttribute(final String key) {
        return this.ch.attr(AttributeKey.valueOf(key)).get();
    }

    @Override
    public void setAttribute(final String key, final Object value) {
        this.ch.attr(AttributeKey.valueOf(key)).set(value);
    }

    @Override
    public void close() {
        this.ch.close();
    }

    void addClosedEventListener(final ConnectionClosedEventListener listener) {
      this.ch.closeFuture() //
      .addListener(
          future -> listener.onClosed(this.ch.remoteAddress().toString(), NettyConnection.this));
    }
}
