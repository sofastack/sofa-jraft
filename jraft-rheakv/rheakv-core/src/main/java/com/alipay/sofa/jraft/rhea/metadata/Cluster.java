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
import java.util.List;
import java.util.Objects;

import com.alipay.sofa.jraft.rhea.util.Lists;
import com.alipay.sofa.jraft.util.Copiable;

/**
 *
 * @author jiachun.fjc
 */
public class Cluster implements Copiable<Cluster>, Serializable {

    private static final long serialVersionUID = 3291666486933960310L;

    private long              clusterId;
    private List<Store>       stores;

    public Cluster(long clusterId, List<Store> stores) {
        this.clusterId = clusterId;
        this.stores = stores;
    }

    public long getClusterId() {
        return clusterId;
    }

    public void setClusterId(long clusterId) {
        this.clusterId = clusterId;
    }

    public List<Store> getStores() {
        return stores;
    }

    public void setStores(List<Store> stores) {
        this.stores = stores;
    }

    @Override
    public Cluster copy() {
        List<Store> stores = null;
        if (this.stores != null) {
            stores = Lists.newArrayListWithCapacity(this.stores.size());
            for (Store store : this.stores) {
                stores.add(store.copy());
            }
        }
        return new Cluster(this.clusterId, stores);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        Cluster cluster = (Cluster) o;
        return clusterId == cluster.clusterId && Objects.equals(stores, cluster.stores);
    }

    @Override
    public int hashCode() {
        return Objects.hash(clusterId, stores);
    }

    @Override
    public String toString() {
        return "Cluster{" + "clusterId=" + clusterId + ", stores=" + stores + '}';
    }
}
