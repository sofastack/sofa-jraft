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

import java.util.Map;
import java.util.NoSuchElementException;
import java.util.WeakHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.rhea.util.StackTraceUtil;
import com.alipay.sofa.jraft.rhea.util.pipeline.event.InboundMessageEvent;
import com.alipay.sofa.jraft.rhea.util.pipeline.event.MessageEvent;
import com.alipay.sofa.jraft.rhea.util.pipeline.event.OutboundMessageEvent;
import com.alipay.sofa.jraft.rhea.util.pipeline.future.DefaultPipelineFuture;
import com.alipay.sofa.jraft.rhea.util.pipeline.future.PipelineFuture;

/**
 * Most of the code references the pipeline design of
 * <a href="https://github.com/netty/netty">Netty</a>.
 *
 * @author jiachun.fjc
 */
public final class DefaultPipeline implements Pipeline {

    private static final Logger                             LOG        = LoggerFactory.getLogger(DefaultPipeline.class);

    private static final ThreadLocal<Map<Class<?>, String>> nameCaches = ThreadLocal.withInitial(WeakHashMap::new);

    final AbstractHandlerContext                            head;
    final AbstractHandlerContext                            tail;

    public DefaultPipeline() {
        tail = new TailContext(this);
        head = new HeadContext(this);

        head.next = tail;
        tail.prev = head;
    }

    @Override
    public Pipeline addFirst(String name, Handler handler) {
        return addFirst(null, name, handler);
    }

    @Override
    public Pipeline addFirst(HandlerInvoker invoker, String name, Handler handler) {
        name = filterName(name, handler);
        addFirst0(new DefaultHandlerContext(this, invoker, name, handler));
        return this;
    }

    private void addFirst0(AbstractHandlerContext newCtx) {
        checkMultiplicity(newCtx);

        AbstractHandlerContext nextCtx = head.next;
        newCtx.prev = head;
        newCtx.next = nextCtx;
        head.next = newCtx;
        nextCtx.prev = newCtx;

        callHandlerAdded(newCtx);
    }

    @Override
    public Pipeline addLast(String name, Handler handler) {
        return addLast(null, name, handler);
    }

    @Override
    public Pipeline addLast(HandlerInvoker invoker, String name, Handler handler) {
        name = filterName(name, handler);
        addLast0(new DefaultHandlerContext(this, invoker, name, handler));
        return this;
    }

    private void addLast0(AbstractHandlerContext newCtx) {
        checkMultiplicity(newCtx);

        AbstractHandlerContext prev = tail.prev;
        newCtx.prev = prev;
        newCtx.next = tail;
        prev.next = newCtx;
        tail.prev = newCtx;

        callHandlerAdded(newCtx);
    }

    @Override
    public Pipeline addBefore(String baseName, String name, Handler handler) {
        return addBefore(null, baseName, name, handler);
    }

    @Override
    public Pipeline addBefore(HandlerInvoker invoker, String baseName, String name, Handler handler) {
        AbstractHandlerContext ctx = getContextOrDie(baseName);
        name = filterName(name, handler);
        addBefore0(ctx, new DefaultHandlerContext(this, invoker, name, handler));
        return this;
    }

    private void addBefore0(AbstractHandlerContext ctx, AbstractHandlerContext newCtx) {
        checkMultiplicity(newCtx);

        newCtx.prev = ctx.prev;
        newCtx.next = ctx;
        ctx.prev.next = newCtx;
        ctx.prev = newCtx;

        callHandlerAdded(newCtx);
    }

    @Override
    public Pipeline addAfter(String baseName, String name, Handler handler) {
        return addAfter(null, baseName, name, handler);
    }

    @Override
    public Pipeline addAfter(HandlerInvoker invoker, String baseName, String name, Handler handler) {
        AbstractHandlerContext ctx = getContextOrDie(baseName);
        name = filterName(name, handler);
        addAfter0(ctx, new DefaultHandlerContext(this, invoker, name, handler));
        return this;
    }

    private void addAfter0(AbstractHandlerContext ctx, AbstractHandlerContext newCtx) {
        checkMultiplicity(newCtx);

        newCtx.prev = ctx;
        newCtx.next = ctx.next;
        ctx.next.prev = newCtx;
        ctx.next = newCtx;

        callHandlerAdded(newCtx);
    }

    @Override
    public Pipeline addFirst(Handler... handlers) {
        return addFirst(null, handlers);
    }

    @Override
    public Pipeline addFirst(HandlerInvoker invoker, Handler... handlers) {
        if (handlers == null) {
            throw new NullPointerException("handlers");
        }
        if (handlers.length == 0 || handlers[0] == null) {
            return this;
        }

        int size;
        for (size = 1; size < handlers.length; size ++) {
            if (handlers[size] == null) {
                break;
            }
        }

        for (int i = size - 1; i >= 0; i --) {
            Handler h = handlers[i];
            addFirst(invoker, null, h);
        }

        return this;
    }

    @Override
    public Pipeline addLast(Handler... handlers) {
        return addLast(null, handlers);
    }

    @Override
    public Pipeline addLast(HandlerInvoker invoker, Handler... handlers) {
        if (handlers == null) {
            throw new NullPointerException("handlers");
        }

        for (Handler h: handlers) {
            if (h == null) {
                break;
            }
            addLast(invoker, null, h);
        }

        return this;
    }

    @Override
    public Pipeline remove(Handler handler) {
        remove(getContextOrDie(handler));
        return this;
    }

    @Override
    public Handler remove(String name) {
        return remove(getContextOrDie(name)).handler();
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T extends Handler> T remove(Class<T> handlerType) {
        return (T) remove(getContextOrDie(handlerType)).handler();
    }

    private AbstractHandlerContext remove(final AbstractHandlerContext ctx) {
        assert ctx != head && ctx != tail;

        synchronized (this) {
            remove0(ctx);
            return ctx;
        }
    }

    private void remove0(AbstractHandlerContext ctx) {
        AbstractHandlerContext prev = ctx.prev;
        AbstractHandlerContext next = ctx.next;
        prev.next = next;
        next.prev = prev;
        callHandlerRemoved(ctx);
    }

    @Override
    public Handler removeFirst() {
        if (head.next == tail) {
            throw new NoSuchElementException();
        }
        return remove(head.next).handler();
    }

    @Override
    public Handler removeLast() {
        if (head.next == tail) {
            throw new NoSuchElementException();
        }
        return remove(tail.prev).handler();
    }

    @Override
    public Pipeline replace(Handler oldHandler, String newName, Handler newHandler) {
        replace(getContextOrDie(oldHandler), newName, newHandler);
        return this;
    }

    @Override
    public Handler replace(String oldName, String newName, Handler newHandler) {
        return replace(getContextOrDie(oldName), newName, newHandler);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T extends Handler> T replace(Class<T> oldHandlerType, String newName, Handler newHandler) {
        return (T) replace(getContextOrDie(oldHandlerType), newName, newHandler);
    }

    private Handler replace(
            final AbstractHandlerContext ctx, String newName, Handler newHandler) {

        assert ctx != head && ctx != tail;

        synchronized (this) {
            if (newName == null) {
                newName = ctx.name();
            } else if (!ctx.name().equals(newName)) {
                newName = filterName(newName, newHandler);
            }

            final AbstractHandlerContext newCtx =
                    new DefaultHandlerContext(this, ctx.invoker, newName, newHandler);

            replace0(ctx, newCtx);
            return ctx.handler();
        }
    }

    private void replace0(AbstractHandlerContext oldCtx, AbstractHandlerContext newCtx) {
        checkMultiplicity(newCtx);

        AbstractHandlerContext prev = oldCtx.prev;
        AbstractHandlerContext next = oldCtx.next;
        newCtx.prev = prev;
        newCtx.next = next;

        // Finish the replacement of oldCtx with newCtx in the linked list.
        // Note that this doesn't mean events will be sent to the new handler immediately
        // because we are currently at the event handler thread and no more than one handler methods can be invoked
        // at the same time (we ensured that in replace().)
        prev.next = newCtx;
        next.prev = newCtx;

        // update the reference to the replacement so forward of buffered content will work correctly
        oldCtx.prev = newCtx;
        oldCtx.next = newCtx;

        // Invoke newHandler.handlerAdded() first (i.e. before oldHandler.handlerRemoved() is invoked)
        // because callHandlerRemoved() will trigger inboundBufferUpdated() or flush() on newHandler and those
        // event handlers must be called after handlerAdded().
        callHandlerAdded(newCtx);
        callHandlerRemoved(oldCtx);
    }

    @Override
    public Handler get(String name) {
        HandlerContext ctx = context(name);
        if (ctx == null) {
            return null;
        } else {
            return ctx.handler();
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T extends Handler> T get(Class<T> handlerType) {
        HandlerContext ctx = context(handlerType);
        if (ctx == null) {
            return null;
        } else {
            return (T) ctx.handler();
        }
    }

    @Override
    public HandlerContext context(Handler handler) {
        if (handler == null) {
            throw new NullPointerException("handler");
        }

        AbstractHandlerContext ctx = head.next;
        for (;;) {

            if (ctx == null) {
                return null;
            }

            if (ctx.handler() == handler) {
                return ctx;
            }

            ctx = ctx.next;
        }
    }

    @Override
    public HandlerContext context(String name) {
        if (name == null) {
            throw new NullPointerException("name");
        }

        return context0(name);
    }

    @Override
    public HandlerContext context(Class<? extends Handler> handlerType) {
        if (handlerType == null) {
            throw new NullPointerException("handlerType");
        }

        AbstractHandlerContext ctx = head.next;
        for (;;) {
            if (ctx == null) {
                return null;
            }
            if (handlerType.isAssignableFrom(ctx.handler().getClass())) {
                return ctx;
            }
            ctx = ctx.next;
        }
    }

    @Override
    public Pipeline fireInbound(InboundMessageEvent<?> event) {
        head.fireInbound(event);
        return this;
    }

    @Override
    public Pipeline fireOutbound(OutboundMessageEvent<?> event) {
        tail.fireOutbound(event);
        return this;
    }

    @Override
    public Pipeline fireExceptionCaught(MessageEvent<?> event, Throwable cause) {
        head.fireExceptionCaught(event, cause);
        return this;
    }

    @Override
    public <R, M> PipelineFuture<R> invoke(InboundMessageEvent<M> event) {
        return invoke(event, -1);
    }

    @Override
    public <R, M> PipelineFuture<R> invoke(InboundMessageEvent<M> event, long timeoutMillis) {
        PipelineFuture<R> future = DefaultPipelineFuture.with(event.getInvokeId(), timeoutMillis);
        head.fireInbound(event);
        return future;
    }

    private void callHandlerAdded(final AbstractHandlerContext ctx) {
        try {
            ctx.handler().handlerAdded(ctx);
        } catch (Throwable t) {
            boolean removed = false;
            try {
                remove(ctx);
                removed = true;
            } catch (Throwable t2) {
                if (LOG.isWarnEnabled()) {
                    LOG.warn("Failed to remove a handler: {}, {}.", ctx.name(), StackTraceUtil.stackTrace(t2));
                }
            }

        fireExceptionCaught(null, new PipelineException(
                ctx.handler().getClass().getName() +
                        ".handlerAdded() has thrown an exception; " + (removed ? "removed." :  "also failed to remove."), t));
        }
    }

    private void callHandlerRemoved(final AbstractHandlerContext ctx) {
        // Notify the complete removal.
        try {
            ctx.handler().handlerRemoved(ctx);
            ctx.setRemoved();
        } catch (Throwable t) {
            fireExceptionCaught(null, new PipelineException(
                    ctx.handler().getClass().getName() + ".handlerRemoved() has thrown an exception.", t));
        }
    }

    private static void checkMultiplicity(HandlerContext ctx) {
        Handler handler = ctx.handler();
        if (handler instanceof HandlerAdapter) {
            HandlerAdapter h = (HandlerAdapter) handler;
            if (!h.isSharable() && h.added) {
                throw new PipelineException(
                        h.getClass().getName() +
                                " is not a @Sharable handler, so can't be added or removed multiple times.");
            }
            h.added = true;
        }
    }

    private String filterName(String name, Handler handler) {
        if (name == null) {
            return generateName(handler);
        }

        if (context0(name) == null) {
            return name;
        }

        throw new IllegalArgumentException("Duplicate handler name: " + name);
    }

    private AbstractHandlerContext context0(String name) {
        AbstractHandlerContext context = head.next;
        while (context != tail) {
            if (context.name().equals(name)) {
                return context;
            }
            context = context.next;
        }
        return null;
    }

    private AbstractHandlerContext getContextOrDie(String name) {
        AbstractHandlerContext ctx = (AbstractHandlerContext) context(name);
        if (ctx == null) {
            throw new NoSuchElementException(name);
        } else {
            return ctx;
        }
    }

    private AbstractHandlerContext getContextOrDie(Handler handler) {
        AbstractHandlerContext ctx = (AbstractHandlerContext) context(handler);
        if (ctx == null) {
            throw new NoSuchElementException(handler.getClass().getName());
        } else {
            return ctx;
        }
    }

    private AbstractHandlerContext getContextOrDie(Class<? extends Handler> handlerType) {
        AbstractHandlerContext ctx = (AbstractHandlerContext) context(handlerType);
        if (ctx == null) {
            throw new NoSuchElementException(handlerType.getName());
        } else {
            return ctx;
        }
    }

    private String generateName(Handler handler) {
        Map<Class<?>, String> cache = nameCaches.get();
        Class<?> handlerType = handler.getClass();
        String name = cache.get(handlerType);
        if (name == null) {
            name = generateName0(handlerType);
            cache.put(handlerType, name);
        }

        synchronized (this) {
            // It's not very likely for a user to put more than one handler of the same type, but make sure to avoid
            // any name conflicts.  Note that we don't cache the names generated here.
            if (context0(name) != null) {
                String baseName = name.substring(0, name.length() - 1); // Strip the trailing '0'.
                for (int i = 1;; i ++) {
                    String newName = baseName + i;
                    if (context0(newName) == null) {
                        name = newName;
                        break;
                    }
                }
            }
        }

        return name;
    }

    private static String generateName0(Class<?> handlerType) {
        if (handlerType == null) {
            throw new NullPointerException("handlerType");
        }

        String className = handlerType.getName();
        final int lastDotIdx = className.lastIndexOf('.');
        if (lastDotIdx > -1) {
            className = className.substring(lastDotIdx + 1);
        }
        return className + "#0";
    }

    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder()
                .append(getClass().getSimpleName())
                .append('{');
        AbstractHandlerContext ctx = head.next;
        for (;;) {
            if (ctx == tail) {
                break;
            }

            buf.append('(')
                    .append(ctx.name())
                    .append(" = ")
                    .append(ctx.handler().getClass().getName())
                    .append(')');

            ctx = ctx.next;
            if (ctx == tail) {
                break;
            }

            buf.append(", ");
        }
        buf.append('}');
        return buf.toString();
    }

    // A special catch-all handler that handles both messages.
    static final class TailContext extends AbstractHandlerContext implements InboundHandler {

        private static final String TAIL_NAME = generateName0(TailContext.class);

        public TailContext(DefaultPipeline pipeline) {
            super(pipeline, null, TAIL_NAME, true, false);
        }

        @Override
        public boolean isAcceptable(MessageEvent<?> event) {
            return true;
        }

        @Override
        public void handleInbound(HandlerContext ctx, InboundMessageEvent<?> event) throws Exception {}

        @Override
        public void exceptionCaught(HandlerContext ctx, MessageEvent<?> event, Throwable cause) throws Exception {
            LOG.warn("An exceptionCaught() event was fired, {}.", StackTraceUtil.stackTrace(cause));

            if (event != null) {
                DefaultPipelineFuture.received(event.getInvokeId(), cause);
            }
        }

        @Override
        public void handlerAdded(HandlerContext ctx) throws Exception {}

        @Override
        public void handlerRemoved(HandlerContext ctx) throws Exception {}

        @Override
        public Handler handler() {
            return this;
        }
    }

    static final class HeadContext extends AbstractHandlerContext implements OutboundHandler {

        private static final String HEAD_NAME = generateName0(HeadContext.class);

        public HeadContext(DefaultPipeline pipeline) {
            super(pipeline, null, HEAD_NAME, false, true);
        }

        @Override
        public boolean isAcceptable(MessageEvent<?> event) {
            return true;
        }

        @Override
        public void handlerAdded(HandlerContext ctx) throws Exception {}

        @Override
        public void handlerRemoved(HandlerContext ctx) throws Exception {}

        @Override
        public void exceptionCaught(HandlerContext ctx, MessageEvent<?> event, Throwable cause) throws Exception {
            ctx.fireExceptionCaught(event, cause);
        }

        @Override
        public Handler handler() {
            return this;
        }

        @Override
        public void handleOutbound(HandlerContext ctx, OutboundMessageEvent<?> event) throws Exception {
            if (event != null) {
                DefaultPipelineFuture.received(event.getInvokeId(), event.getMessage());
            }
        }
    }
}
