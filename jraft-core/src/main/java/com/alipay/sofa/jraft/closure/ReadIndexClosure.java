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
package com.alipay.sofa.jraft.closure;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.Status;

/**
 * Read index closure
 *
 * @author dennis
 */
public abstract class ReadIndexClosure implements Closure {

    /**
     * Invalid log index -1.
     */
    public static final long INVALID_LOG_INDEX = -1;
    private long             index             = INVALID_LOG_INDEX;
    private byte[]           requestContext;

    /**
     * Called when ReadIndex can be executed.
     *
     * @param status the readIndex status.
     * @param index  the committed index when starts readIndex.
     * @param reqCtx the request context passed by {@link Node#readIndex(byte[], ReadIndexClosure)}.
     * @see Node#readIndex(byte[], ReadIndexClosure)
     */
    public abstract void run(final Status status, final long index, final byte[] reqCtx);

    /**
     * Set callback result, called by jraft.
     *
     * @param index  the committed index.
     * @param reqCtx the request context passed by {@link Node#readIndex(byte[], ReadIndexClosure)}.
     */
    public void setResult(final long index, final byte[] reqCtx) {
        this.index = index;
        this.requestContext = reqCtx;
    }

    /**
     * The committed log index when starts readIndex request. return -1 if fails.
     *
     * @return returns the committed index.  returns -1 if fails.
     */
    public long getIndex() {
        return index;
    }

    /**
     * Returns the request context.
     *
     * @return the request context.
     */
    public byte[] getRequestContext() {
        return requestContext;
    }

    @Override
    public void run(final Status status) {
        run(status, this.index, this.requestContext);
    }
}
