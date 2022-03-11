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
package com.alipay.sofa.jraft.core;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.error.RaftError;
import static org.junit.Assert.assertEquals;

public class ExpectClosure implements Closure {
    private final int            expectedErrCode;
    private final String         expectErrMsg;
    private final CountDownLatch latch;
    private AtomicInteger        successCount;

    public ExpectClosure(final CountDownLatch latch) {
        this(RaftError.SUCCESS, latch);
    }

    public ExpectClosure(final RaftError expectedErrCode, final CountDownLatch latch) {
        this(expectedErrCode, null, latch);

    }

    public ExpectClosure(final RaftError expectedErrCode, final String expectErrMsg, final CountDownLatch latch) {
        super();
        this.expectedErrCode = expectedErrCode.getNumber();
        this.expectErrMsg = expectErrMsg;
        this.latch = latch;
    }

    public ExpectClosure(final int code, final String expectErrMsg, final CountDownLatch latch) {
        this(code, expectErrMsg, latch, null);
    }

    public ExpectClosure(final int code, final String expectErrMsg, final CountDownLatch latch,
                         final AtomicInteger successCount) {
        super();
        this.expectedErrCode = code;
        this.expectErrMsg = expectErrMsg;
        this.latch = latch;
        this.successCount = successCount;
    }

    @Override
    public void run(final Status status) {
        if (this.expectedErrCode >= 0) {
            assertEquals(this.expectedErrCode, status.getCode());
        }
        if (this.expectErrMsg != null) {
            assertEquals(this.expectErrMsg, status.getErrorMsg());
        }
        if (status.isOk() && this.successCount != null) {
            this.successCount.incrementAndGet();
        }
        this.latch.countDown();
    }

}
