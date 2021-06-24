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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.rhea.errors.Errors;

/**
 * @author jiachun.fjc
 */
public class KVClosureAdapter implements KVStoreClosure {

    private static final Logger LOG = LoggerFactory.getLogger(KVClosureAdapter.class);

    private KVStoreClosure      done;
    private KVOperation         operation;

    public KVClosureAdapter(final KVStoreClosure done, final KVOperation operation) {
        this.done = done;
        this.operation = operation;
    }

    public KVStoreClosure getDone() {
        return done;
    }

    public KVOperation getOperation() {
        return operation;
    }

    @Override
    public void run(final Status status) {
        if (status.isOk()) {
            setError(Errors.NONE);
        } else {
            LOG.error("Fail status: {}.", status);
            if (getError() == null) {
                switch (status.getRaftError()) {
                    case SUCCESS:
                        setError(Errors.NONE);
                        break;
                    case EINVAL:
                        setError(Errors.INVALID_REQUEST);
                        break;
                    case EIO:
                        setError(Errors.STORAGE_ERROR);
                        break;
                    default:
                        setError(Errors.LEADER_NOT_AVAILABLE);
                        break;
                }
            }
        }
        if (done != null) {
            done.run(status);
        }
        reset();
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

    private void reset() {
        done = null;
        operation = null;
    }
}
