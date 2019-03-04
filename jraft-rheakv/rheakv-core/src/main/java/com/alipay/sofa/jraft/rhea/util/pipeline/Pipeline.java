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
import com.alipay.sofa.jraft.rhea.util.pipeline.future.PipelineFuture;

/**
 * A list of {@link Handler}s which handles or intercepts
 * inbound events and outbound operations.
 *
 * Most of the code references the pipeline design of
 * <a href="https://github.com/netty/netty">Netty</a>.
 *
 * @author jiachun.fjc
 */
public interface Pipeline {

    /**
     * Inserts a {@link Handler} at the first position of this pipeline.
     *
     * @param name      the name of the handler to insert first.
     * @param handler   the handler to insert first.
     *
     * @return itself
     */
    Pipeline addFirst(final String name, final Handler handler);

    /**
     * Inserts a {@link Handler} at the first position of this pipeline.
     *
     * @param invoker   the {@link HandlerInvoker} which will be used to execute the {@link Handler}'s methods.
     * @param name      the name of the handler to insert first.
     * @param handler   the handler to insert first.
     *
     * @return itself
     */
    Pipeline addFirst(final HandlerInvoker invoker, final String name, final Handler handler);

    /**
     * Appends a {@link Handler} at the last position of this pipeline.
     *
     * @param name      the name of the handler to append.
     * @param handler   the handler to append.
     *
     * @return itself
     */
    Pipeline addLast(final String name, final Handler handler);

    /**
     * Appends a {@link Handler} at the last position of this pipeline.
     *
     * @param invoker   the {@link HandlerInvoker} which will be used to execute the {@link Handler}'s methods.
     * @param name      the name of the handler to append.
     * @param handler   the handler to append.
     *
     * @return itself
     */
    Pipeline addLast(final HandlerInvoker invoker, final String name, final Handler handler);

    /**
     * Inserts a {@link Handler} before an existing handler of this pipeline.
     *
     * @param baseName  the name of the existing handler.
     * @param name      the name of the handler to insert before.
     * @param handler   the handler to insert before.
     *
     * @return itself
     */
    Pipeline addBefore(final String baseName, final String name, final Handler handler);

    /**
     * Inserts a {@link Handler} before an existing handler of this pipeline.
     *
     * @param invoker   the {@link HandlerInvoker} which will be used to execute the {@link Handler}'s methods.
     * @param baseName  the name of the existing handler.
     * @param name      the name of the handler to insert before.
     * @param handler   the handler to insert before.
     *
     * @return itself
     */
    Pipeline addBefore(final HandlerInvoker invoker, final String baseName, final String name, final Handler handler);

    /**
     * Inserts a {@link Handler} after an existing handler of this pipeline.
     *
     * @param baseName  the name of the existing handler.
     * @param name      the name of the handler to insert after.
     * @param handler   the handler to insert after.
     *
     * @return itself
     */
    Pipeline addAfter(final String baseName, final String name, final Handler handler);

    /**
     * Inserts a {@link Handler} after an existing handler of this pipeline.
     *
     * @param invoker   the {@link HandlerInvoker} which will be used to execute the {@link Handler}'s methods.
     * @param baseName  the name of the existing handler.
     * @param name      the name of the handler to insert after.
     * @param handler   the handler to insert after.
     *
     * @return itself
     */
    Pipeline addAfter(final HandlerInvoker invoker, final String baseName, final String name, final Handler handler);

    /**
     * Inserts {@link Handler}s at the first position of this pipeline.
     *
     * @param handlers  the handlers to insert first.
     *
     * @return itself
     */
    Pipeline addFirst(final Handler... handlers);

    /**
     * Inserts {@link Handler}s at the first position of this pipeline.
     *
     * @param invoker   the {@link HandlerInvoker} which will be used to execute the {@link Handler}s's methods.
     * @param handlers  the handlers to insert first.
     */
    Pipeline addFirst(final HandlerInvoker invoker, final Handler... handlers);

    /**
     * Inserts {@link Handler}s at the last position of this pipeline.
     *
     * @param handlers  the handlers to insert last.
     *
     * @return itself
     */
    Pipeline addLast(final Handler... handlers);

    /**
     * Inserts {@link Handler}s at the last position of this pipeline.
     *
     * @param invoker   the {@link HandlerInvoker} which will be used to execute the {@link Handler}s's methods.
     * @param handlers  the handlers to insert last.
     *
     * @return itself
     */
    Pipeline addLast(final HandlerInvoker invoker, final Handler... handlers);

    /**
     * Removes the specified {@link Handler} from this pipeline.
     *
     * @param  handler  the {@link Handler} to remove
     *
     * @return the removed handler
     */
    Pipeline remove(final Handler handler);

    /**
     * Removes the {@link Handler} with the specified name from this pipeline.
     *
     * @param name  the name under which the {@link Handler} was stored.
     *
     * @return the removed handler
     */
    Handler remove(final String name);

    /**
     * Removes the {@link Handler} of the specified type from this pipeline.
     *
     * @param handlerType   the type of the handler.
     * @param <T>           the type of the handler.
     *
     * @return the removed handler
     */
    <T extends Handler> T remove(final Class<T> handlerType);

    /**
     * Removes the first {@link Handler} in this pipeline.
     *
     * @return the removed handler
     */
    Handler removeFirst();

    /**
     * Removes the last {@link Handler} in this pipeline.
     *
     * @return the removed handler
     */
    Handler removeLast();

    /**
     * Replaces the specified {@link Handler} with a new handler in this pipeline.
     *
     * @param  oldHandler    the {@link Handler} to be replaced
     * @param  newName       the name under which the replacement should be added.
     *                       {@code null} to use the same name with the handler being replaced.
     * @param  newHandler    the {@link Handler} which is used as replacement
     *
     * @return itself
     */
    Pipeline replace(final Handler oldHandler, final String newName, final Handler newHandler);

    /**
     * Replaces the {@link Handler} of the specified name with a new handler in this pipeline.
     *
     * @param oldName       the name of the {@link Handler} to be replaced.
     * @param newName       the name under which the replacement should be added.
     *                      {@code null} to use the same name with the handler being replaced.
     * @param newHandler    the {@link Handler} which is used as replacement.
     *
     * @return the removed handler
     */
    Handler replace(final String oldName, final String newName, final Handler newHandler);

    /**
     * Replaces the {@link Handler} of the specified type with a new handler in this pipeline.
     *
     * @param  oldHandlerType   the type of the handler to be removed
     * @param  newName          the name under which the replacement should be added.
     *                          {@code null} to use the same name with the handler being replaced.
     * @param  newHandler       the {@link Handler} which is used as replacement
     *
     * @return the removed handler
     */
    <T extends Handler> T replace(final Class<T> oldHandlerType, final String newName, final Handler newHandler);

    /**
     * Returns the {@link Handler} with the specified name in this
     * pipeline.
     *
     * @return the handler with the specified name.
     *         {@code null} if there's no such handler in this pipeline.
     */
    Handler get(final String name);

    /**
     * Returns the {@link Handler} of the specified type in this
     * pipeline.
     *
     * @return the handler of the specified handler type.
     *         {@code null} if there's no such handler in this pipeline.
     */
    <T extends Handler> T get(final Class<T> handlerType);

    /**
     * Returns the context object of the specified {@link Handler} in
     * this pipeline.
     *
     * @return the context object of the specified handler.
     *         {@code null} if there's no such handler in this pipeline.
     */
    HandlerContext context(final Handler handler);

    /**
     * Returns the context object of the {@link Handler} with the
     * specified name in this pipeline.
     *
     * @return the context object of the handler with the specified name.
     *         {@code null} if there's no such handler in this pipeline.
     */
    HandlerContext context(final String name);

    /**
     * Returns the context object of the {@link Handler} of the
     * specified type in this pipeline.
     *
     * @return the context object of the handler of the specified type.
     *         {@code null} if there's no such handler in this pipeline.
     */
    HandlerContext context(final Class<? extends Handler> handlerType);

    /**
     * Sends the specified {@link MessageEvent} to the first {@link InboundHandler} in this pipeline.
     */
    Pipeline fireInbound(final InboundMessageEvent<?> event);

    /**
     * Sends the specified {@link MessageEvent} to the first {@link OutboundHandler} in this pipeline.
     */
    Pipeline fireOutbound(final OutboundMessageEvent<?> event);

    /**
     * Received an {@link Throwable} in one of its inbound operations.
     *
     * This will result in having the  {@link InboundHandler#exceptionCaught(HandlerContext, MessageEvent, Throwable)}
     * method  called of the next  {@link InboundHandler} contained in the  {@link Pipeline}.
     */
    Pipeline fireExceptionCaught(final MessageEvent<?> event, final Throwable cause);

    /**
     * Invoke in the pipeline.
     *
     * @param event an inbound message event
     * @param <R>   expected return type
     * @param <M>   message type
     */
    <R, M> PipelineFuture<R> invoke(final InboundMessageEvent<M> event);

    /**
     *
     * @param event an inbound message event
     * @param timeoutMillis timeout
     * @param <R>   expected return type
     * @param <M>   message type
     */
    <R, M> PipelineFuture<R> invoke(final InboundMessageEvent<M> event, final long timeoutMillis);
}
