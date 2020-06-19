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
package com.alipay.sofa.jraft.rhea.storage;

import java.util.List;
import java.util.Map;

import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.rhea.errors.Errors;
import com.alipay.sofa.jraft.rhea.metrics.KVMetrics;
import com.alipay.sofa.jraft.rhea.util.ByteArray;
import com.codahale.metrics.Timer;

import static com.alipay.sofa.jraft.rhea.metrics.KVMetricNames.REGION_BYTES_READ;
import static com.alipay.sofa.jraft.rhea.metrics.KVMetricNames.REGION_BYTES_WRITTEN;
import static com.alipay.sofa.jraft.rhea.metrics.KVMetricNames.REGION_KEYS_READ;
import static com.alipay.sofa.jraft.rhea.metrics.KVMetricNames.REGION_KEYS_WRITTEN;
import static com.alipay.sofa.jraft.rhea.metrics.KVMetricNames.RPC_REQUEST_HANDLE_TIMER;

/**
 *
 * @author jiachun.fjc
 */
public class MetricsKVClosureAdapter implements KVStoreClosure {

    private final KVStoreClosure done;
    private final String         regionId;
    private final byte           kvOp;
    private final long           keysCount;
    private final long           bytesWritten;
    private final Timer.Context  ctx;
    private final Timer.Context  opCtx;

    public MetricsKVClosureAdapter(KVStoreClosure done, String regionId, byte kvOp, long keysCount, long bytesWritten,
                                   Timer.Context ctx) {
        this.done = done;
        this.regionId = regionId;
        this.kvOp = kvOp;
        this.keysCount = keysCount;
        this.bytesWritten = bytesWritten;
        this.ctx = ctx;
        this.opCtx = opTimeCtx(kvOp);
    }

    @Override
    public Errors getError() {
        if (this.done != null) {
            return this.done.getError();
        }
        return null;
    }

    @Override
    public void setError(Errors error) {
        if (this.done != null) {
            this.done.setError(error);
        }
    }

    @Override
    public Object getData() {
        if (this.done != null) {
            return this.done.getData();
        }
        return null;
    }

    @Override
    public void setData(Object data) {
        if (this.done != null) {
            this.done.setData(data);
        }
    }

    @Override
    public void run(final Status status) {
        try {
            if (this.done != null) {
                this.done.run(status);
            }
        } finally {
            if (status.isOk()) {
                doStatistics();
            }
            this.ctx.stop();
            this.opCtx.stop();
        }
    }

    private Timer.Context opTimeCtx(final byte op) {
        return KVMetrics.timer(RPC_REQUEST_HANDLE_TIMER, this.regionId, KVOperation.opName(op)).time();
    }

    @SuppressWarnings("unchecked")
    private void doStatistics() {
        final String id = this.regionId; // stack copy
        switch (this.kvOp) {
            case KVOperation.PUT:
            case KVOperation.MERGE: {
                KVMetrics.counter(REGION_KEYS_WRITTEN, id).inc();
                KVMetrics.counter(REGION_BYTES_WRITTEN, id).inc(this.bytesWritten);
                break;
            }
            case KVOperation.PUT_IF_ABSENT: {
                KVMetrics.counter(REGION_KEYS_READ, id).inc();
                byte[] bytes = (byte[]) getData();
                if (bytes != null) {
                    KVMetrics.counter(REGION_BYTES_READ, id).inc(bytes.length);
                } else {
                    KVMetrics.counter(REGION_KEYS_WRITTEN, id).inc();
                    KVMetrics.counter(REGION_BYTES_WRITTEN, id).inc(this.bytesWritten);
                }
                break;
            }
            case KVOperation.DELETE:
            case KVOperation.RESET_SEQUENCE:
            case KVOperation.KEY_LOCK_RELEASE: {
                KVMetrics.counter(REGION_KEYS_WRITTEN, id).inc();
                break;
            }
            case KVOperation.PUT_LIST: {
                KVMetrics.counter(REGION_KEYS_WRITTEN, id).inc(keysCount);
                KVMetrics.counter(REGION_BYTES_WRITTEN, id).inc(this.bytesWritten);
                break;
            }
            case KVOperation.DELETE_RANGE: {
                // TODO if this is needed?
                break;
            }
            case KVOperation.DELETE_LIST: {
                KVMetrics.counter(REGION_KEYS_WRITTEN, id).inc(keysCount);
                break;
            }
            case KVOperation.GET_SEQUENCE:
            case KVOperation.KEY_LOCK: {
                KVMetrics.counter(REGION_KEYS_READ, id).inc();
                KVMetrics.counter(REGION_KEYS_WRITTEN, id).inc(); // maybe 0
                KVMetrics.counter(REGION_BYTES_READ, id).inc(this.bytesWritten);
                KVMetrics.counter(REGION_BYTES_WRITTEN, id).inc(this.bytesWritten);
                break;
            }
            case KVOperation.GET: {
                KVMetrics.counter(REGION_KEYS_READ, id).inc();
                final byte[] data = (byte[]) getData();
                if (data != null) {
                    KVMetrics.counter(REGION_BYTES_READ, id).inc(data.length);
                }
                break;
            }
            case KVOperation.MULTI_GET: {
                KVMetrics.counter(REGION_KEYS_READ, id).inc(this.keysCount);
                final Map<ByteArray, byte[]> data = (Map<ByteArray, byte[]>) getData();
                if (data != null) {
                    long bytesRead = 0;
                    for (final byte[] bytes : data.values()) {
                        if (bytes == null) {
                            continue;
                        }
                        bytesRead += bytes.length;
                    }
                    KVMetrics.counter(REGION_BYTES_READ, id).inc(bytesRead);
                }
                break;
            }
            case KVOperation.CONTAINS_KEY: {
                KVMetrics.counter(REGION_KEYS_READ, id).inc();
                break;
            }
            case KVOperation.SCAN: {
                final List<KVEntry> data = (List<KVEntry>) getData();
                if (data != null) {
                    long bytesRead = 0;
                    for (final KVEntry kvEntry : data) {
                        final byte[] value = kvEntry.getValue();
                        if (value == null) {
                            continue;
                        }
                        bytesRead += value.length;
                    }
                    KVMetrics.counter(REGION_KEYS_READ, id).inc(data.size());
                    KVMetrics.counter(REGION_BYTES_READ, id).inc(bytesRead);
                }
                break;
            }
            case KVOperation.GET_PUT: {
                KVMetrics.counter(REGION_KEYS_READ, id).inc();
                KVMetrics.counter(REGION_KEYS_WRITTEN, id).inc();
                final byte[] data = (byte[]) getData();
                if (data != null) {
                    KVMetrics.counter(REGION_BYTES_READ, id).inc(data.length);
                }
                KVMetrics.counter(REGION_BYTES_WRITTEN, id).inc(this.bytesWritten);
                break;
            }
            case KVOperation.COMPARE_PUT: {
                KVMetrics.counter(REGION_KEYS_READ, id).inc();
                final Boolean data = (Boolean) getData();
                if (data != null && data) {
                    KVMetrics.counter(REGION_KEYS_WRITTEN, id).inc();
                    KVMetrics.counter(REGION_BYTES_WRITTEN, id).inc(this.bytesWritten);
                }
                break;
            }
        }
    }
}
