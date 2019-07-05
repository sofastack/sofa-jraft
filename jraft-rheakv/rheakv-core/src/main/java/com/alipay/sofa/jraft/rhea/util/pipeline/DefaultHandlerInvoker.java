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

import java.util.concurrent.ExecutorService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.rhea.util.StackTraceUtil;
import com.alipay.sofa.jraft.rhea.util.pipeline.event.InboundMessageEvent;
import com.alipay.sofa.jraft.rhea.util.pipeline.event.MessageEvent;
import com.alipay.sofa.jraft.rhea.util.pipeline.event.OutboundMessageEvent;
import com.alipay.sofa.jraft.util.ExecutorServiceHelper;

/**
 *
 * @author jiachun.fjc
 */
public class DefaultHandlerInvoker implements HandlerInvoker {

    private static final Logger   LOG = LoggerFactory.getLogger(DefaultHandlerInvoker.class);

    private final ExecutorService executor;

    public DefaultHandlerInvoker() {
        this(null);
    }

    public DefaultHandlerInvoker(ExecutorService executor) {
        this.executor = executor;
    }

    @Override
    public ExecutorService executor() {
        return executor;
    }

    @Override
    public void invokeInbound(final HandlerContext ctx, final InboundMessageEvent<?> event) {
        if (this.executor == null) {
            HandlerInvokerUtil.invokeInboundNow(ctx, event);
        } else {
            this.executor.execute(() -> HandlerInvokerUtil.invokeInboundNow(ctx, event));
        }
    }

    @Override
    public void invokeOutbound(final HandlerContext ctx, final OutboundMessageEvent<?> event) {
        if (this.executor == null) {
            HandlerInvokerUtil.invokeOutboundNow(ctx, event);
        } else {
            this.executor.execute(() -> HandlerInvokerUtil.invokeOutboundNow(ctx, event));
        }
    }

    @Override
    public void invokeExceptionCaught(final HandlerContext ctx, final MessageEvent<?> event, final Throwable cause) {
        if (cause == null) {
            throw new NullPointerException("cause");
        }

        if (this.executor == null) {
            HandlerInvokerUtil.invokeExceptionCaughtNow(ctx, event, cause);
        } else {
            try {
                this.executor.execute(() -> HandlerInvokerUtil.invokeExceptionCaughtNow(ctx, event, cause));
            } catch (final Throwable t) {
                if (LOG.isWarnEnabled()) {
                    LOG.warn("Failed to submit an exceptionCaught() event: {}.", StackTraceUtil.stackTrace(t));
                    LOG.warn("The exceptionCaught() event that was failed to submit was: {}.",
                        StackTraceUtil.stackTrace(cause));
                }
            }
        }
    }

    @Override
    public void shutdown() {
        ExecutorServiceHelper.shutdownAndAwaitTermination(this.executor);
    }
}
