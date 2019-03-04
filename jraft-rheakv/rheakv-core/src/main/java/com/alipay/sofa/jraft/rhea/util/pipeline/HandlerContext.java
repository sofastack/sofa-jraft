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
package com.alipay.sofa.jraft.rhea.util.pipeline;

import com.alipay.sofa.jraft.rhea.util.pipeline.event.InboundMessageEvent;
import com.alipay.sofa.jraft.rhea.util.pipeline.event.MessageEvent;
import com.alipay.sofa.jraft.rhea.util.pipeline.event.OutboundMessageEvent;

/**
 * Enables a {@link Handler} to interact with its {@link Pipeline}
 * and other handlers. Among other things a handler can notify the next {@link Handler} in the
 * {@link Pipeline} as well as modify the {@link Pipeline} it belongs to dynamically.
 *
 * Most of the code references the pipeline design of
 * <a href="https://github.com/netty/netty">Netty</a>.
 *
 * @author jiachun.fjc
 */
public interface HandlerContext {

    /**
     * Return the assigned {@link Pipeline}.
     */
    Pipeline pipeline();

    /**
     * Returns the {@link HandlerInvoker} which is used to trigger an event for the associated
     * {@link Handler}. Note that the methods in {@link HandlerInvoker} are not intended to be called
     * by a user. Use this method only to obtain the reference to the {@link HandlerInvoker}
     * (and not calling its methods) unless you know what you are doing.
     */
    HandlerInvoker invoker();

    /**
     * The unique name of the {@link HandlerContext}.The name was used when then {@link Handler}
     * was added to the {@link Pipeline}. This name can also be used to access the registered
     * {@link Handler} from the {@link Pipeline}.
     */
    String name();

    /**
     * The {@link Handler} that is bound this {@link HandlerContext}.
     */
    Handler handler();

    /**
     * Return {@code true} if the {@link Handler} which belongs to this context was removed
     * from the {@link Pipeline}.
     */
    boolean isRemoved();

    /**
     * Received a message.
     */
    HandlerContext fireInbound(final InboundMessageEvent<?> event);

    /**
     * Request to write a message via this {@link HandlerContext} through the {@link Pipeline}.
     */
    HandlerContext fireOutbound(final OutboundMessageEvent<?> event);

    /**
     * Received an {@link Throwable} in one of its inbound operations.
     */
    HandlerContext fireExceptionCaught(final MessageEvent<?> event, final Throwable cause);
}
