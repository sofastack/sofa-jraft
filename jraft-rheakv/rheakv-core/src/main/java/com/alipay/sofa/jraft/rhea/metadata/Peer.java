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
package com.alipay.sofa.jraft.rhea.metadata;

import java.io.Serializable;
import java.util.Objects;

import com.alipay.sofa.jraft.util.Copiable;
import com.alipay.sofa.jraft.util.Endpoint;

/**
 *
 * @author jiachun.fjc
 */
public class Peer implements Copiable<Peer>, Serializable {

    private static final long serialVersionUID = -266370017635677437L;

    private long              id;
    private long              storeId;
    private Endpoint          endpoint;

    public Peer() {
    }

    public Peer(long id, long storeId, Endpoint endpoint) {
        this.id = id;
        this.storeId = storeId;
        this.endpoint = endpoint;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public long getStoreId() {
        return storeId;
    }

    public void setStoreId(long storeId) {
        this.storeId = storeId;
    }

    public Endpoint getEndpoint() {
        return endpoint;
    }

    public void setEndpoint(Endpoint endpoint) {
        this.endpoint = endpoint;
    }

    @Override
    public Peer copy() {
        Endpoint endpoint = null;
        if (this.endpoint != null) {
            endpoint = this.endpoint.copy();
        }
        return new Peer(this.id, this.storeId, endpoint);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        Peer peer = (Peer) o;
        return id == peer.id && storeId == peer.storeId && Objects.equals(endpoint, peer.endpoint);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, storeId, endpoint);
    }

    @Override
    public String toString() {
        return "Peer{" + "id=" + id + ", storeId=" + storeId + ", endpoint=" + endpoint + '}';
    }
}
