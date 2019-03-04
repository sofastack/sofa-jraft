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

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;

import com.alipay.sofa.jraft.rhea.util.pipeline.event.InboundMessageEvent;
import com.alipay.sofa.jraft.rhea.util.pipeline.event.MessageEvent;
import com.alipay.sofa.jraft.rhea.util.pipeline.event.OutboundMessageEvent;

/**
 * Most of the code references the pipeline design of
 * <a href="https://github.com/netty/netty">Netty</a>.
 *
 * @author jiachun.fjc
 */
public interface HandlerInvoker {

    /**
     * Returns the {@link Executor} which is used to execute an arbitrary task.
     */
    ExecutorService executor();

    /**
     * Invokes {@link InboundHandler#handleInbound(HandlerContext, InboundMessageEvent)}. This method is not for a user
     * but for the internal {@link HandlerContext} implementation. To trigger an event, use the methods in
     * {@link HandlerContext} instead.
     */
    void invokeInbound(final HandlerContext ctx, final InboundMessageEvent<?> event);

    /**
     * Invokes {@link OutboundHandler#handleOutbound(HandlerContext, OutboundMessageEvent)}. This method is not for a user
     * but for the internal {@link HandlerContext} implementation. To trigger an event, use the methods in
     * {@link HandlerContext} instead.
     */
    void invokeOutbound(final HandlerContext ctx, final OutboundMessageEvent<?> event);

    /**
     * Received an {@link Throwable} in one of its inbound operations.
     *
     * This will result in having the {@link InboundHandler#exceptionCaught(HandlerContext, MessageEvent, Throwable)}
     * method called of the next {@link InboundHandler} contained in the {@link Pipeline}.
     */
    void invokeExceptionCaught(final HandlerContext ctx, final MessageEvent<?> event, final Throwable cause);

    /**
     * Attempts to stop all actively executing tasks.
     */
    void shutdown();
}
