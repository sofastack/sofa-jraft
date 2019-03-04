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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.rhea.util.StackTraceUtil;
import com.alipay.sofa.jraft.rhea.util.pipeline.event.InboundMessageEvent;
import com.alipay.sofa.jraft.rhea.util.pipeline.event.MessageEvent;
import com.alipay.sofa.jraft.rhea.util.pipeline.event.OutboundMessageEvent;

/**
 * Most of the code references the pipeline design of
 * <a href="https://github.com/netty/netty">Netty</a>.
 *
 * @author jiachun.fjc
 */
public final class HandlerInvokerUtil {

    private static final Logger LOG = LoggerFactory.getLogger(HandlerInvokerUtil.class);

    public static void invokeInboundNow(final HandlerContext ctx, final InboundMessageEvent<?> event) {
        try {
            ((InboundHandler) ctx.handler()).handleInbound(ctx, event);
        } catch (final Throwable t) {
            notifyHandlerException(ctx, event, t);
        }
    }

    public static void invokeOutboundNow(final HandlerContext ctx, final OutboundMessageEvent<?> event) {
        try {
            ((OutboundHandler) ctx.handler()).handleOutbound(ctx, event);
        } catch (final Throwable t) {
            notifyHandlerException(ctx, event, t);
        }
    }

    public static void invokeExceptionCaughtNow(final HandlerContext ctx, final MessageEvent<?> event,
                                                final Throwable cause) {
        try {
            ctx.handler().exceptionCaught(ctx, event, cause);
        } catch (final Throwable t) {
            if (LOG.isWarnEnabled()) {
                LOG.warn("An exception was thrown by a user handler's exceptionCaught() method: {}.",
                    StackTraceUtil.stackTrace(t));
                LOG.warn(".. and the cause of the exceptionCaught() was: {}.", StackTraceUtil.stackTrace(cause));
            }
        }
    }

    private static void notifyHandlerException(final HandlerContext ctx, final MessageEvent<?> event,
                                               final Throwable cause) {
        if (inExceptionCaught(cause)) {
            if (LOG.isWarnEnabled()) {
                LOG.warn("An exception was thrown by a user handler " + "while handling an exceptionCaught event: {}.",
                    StackTraceUtil.stackTrace(cause));
            }
            return;
        }

        invokeExceptionCaughtNow(ctx, event, cause);
    }

    private static boolean inExceptionCaught(Throwable cause) {
        do {
            final StackTraceElement[] trace = cause.getStackTrace();
            if (trace != null) {
                for (final StackTraceElement t : trace) {
                    if (t == null) {
                        break;
                    }
                    if ("exceptionCaught".equals(t.getMethodName())) {
                        return true;
                    }
                }
            }

            cause = cause.getCause();
        } while (cause != null);

        return false;
    }

    private HandlerInvokerUtil() {
    }
}
