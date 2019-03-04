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

import java.util.concurrent.ConcurrentMap;

import com.alipay.sofa.jraft.rhea.util.Maps;
import com.alipay.sofa.jraft.rhea.util.pipeline.event.MessageEvent;

/**
 * Most of the code references the pipeline design of
 * <a href="https://github.com/netty/netty">Netty</a>.
 *
 * @author jiachun.fjc
 */
public abstract class HandlerAdapter implements Handler {

    // Not using volatile because it's used only for a sanity check.
    boolean                                               added;

    private static final ConcurrentMap<Class<?>, Boolean> cache = Maps.newConcurrentMap();

    /**
     * Do nothing by default, sub-classes may override this method.
     */
    @Override
    public void handlerAdded(final HandlerContext ctx) throws Exception {
        // NOOP
    }

    /**
     * Do nothing by default, sub-classes may override this method.
     */
    @Override
    public void handlerRemoved(final HandlerContext ctx) throws Exception {
        // NOOP
    }

    /**
     * Return {@code true} if the implementation is {@link Sharable} and so can be added
     * to different {@link Pipeline}s.
     */
    public boolean isSharable() {
        final Class<?> clazz = getClass();
        Boolean sharable = cache.get(clazz);
        if (sharable == null) {
            sharable = clazz.isAnnotationPresent(Sharable.class);
            cache.put(clazz, sharable);
        }
        return sharable;
    }

    /**
     * Calls {@link HandlerContext#fireExceptionCaught(MessageEvent, Throwable)} to forward
     * to the next {@link Handler} in the {@link Pipeline}.
     *
     * Sub-classes may override this method to change behavior.
     */
    @Override
    public void exceptionCaught(final HandlerContext ctx, final MessageEvent<?> event, final Throwable cause)
                                                                                                             throws Exception {
        ctx.fireExceptionCaught(event, cause);
    }
}
