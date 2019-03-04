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
import com.alipay.sofa.jraft.util.Endpoint;

/**
 * A physical node in the cluster, embedded in an app java process,
 * store corresponds to a local file based rocksDB, a store contains
 * multiple regions, all regions share a rocksDB.
 *
 * @author jiachun.fjc
 */
public class Store implements Copiable<Store>, Serializable {

    private static final long serialVersionUID = 8566110829366373797L;

    private long              id;                                     // store id
    private Endpoint          endpoint;                               // address
    private StoreState        state;                                  // store's state
    private List<Region>      regions;                                // list of included regions
    private List<StoreLabel>  labels;                                 // key/value label

    public Store() {
    }

    public Store(long id, Endpoint endpoint, StoreState state, List<Region> regions, List<StoreLabel> labels) {
        this.id = id;
        this.endpoint = endpoint;
        this.state = state;
        this.regions = regions;
        this.labels = labels;
    }

    public boolean isEmpty() {
        return this.endpoint == null || this.regions == null || this.regions.isEmpty();
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public Endpoint getEndpoint() {
        return endpoint;
    }

    public void setEndpoint(Endpoint endpoint) {
        this.endpoint = endpoint;
    }

    public List<Region> getRegions() {
        return regions;
    }

    public void setRegions(List<Region> regions) {
        this.regions = regions;
    }

    @Override
    public Store copy() {
        Endpoint endpoint = null;
        if (this.endpoint != null) {
            endpoint = this.endpoint.copy();
        }
        List<Region> regions = null;
        if (this.regions != null) {
            regions = Lists.newArrayListWithCapacity(this.regions.size());
            for (Region region : this.regions) {
                regions.add(region.copy());
            }
        }
        List<StoreLabel> labels = null;
        if (this.labels != null) {
            labels = Lists.newArrayListWithCapacity(this.labels.size());
            for (StoreLabel label : this.labels) {
                labels.add(label.copy());
            }
        }
        return new Store(this.id, endpoint, this.state, regions, labels);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        Store store = (Store) o;
        return id == store.id && Objects.equals(endpoint, store.endpoint);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, endpoint);
    }

    @Override
    public String toString() {
        return "Store{" + "id=" + id + ", endpoint=" + endpoint + ", state=" + state + ", regions=" + regions
               + ", labels=" + labels + '}';
    }
}
