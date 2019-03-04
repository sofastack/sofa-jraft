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
package com.alipay.sofa.jraft.rhea.options;

import java.util.List;

import com.alipay.sofa.jraft.option.CliOptions;

/**
 *
 * @author jiachun.fjc
 */
public class PlacementDriverOptions {

    private boolean                       fake;
    private CliOptions                    cliOptions;
    private RpcOptions                    pdRpcOptions;
    // placement driver raft group id
    private String                        pdGroupId;
    private List<RegionRouteTableOptions> regionRouteTableOptionsList;
    private String                        initialServerList;
    // placement driver server address list, with ',' as a separator
    private String                        initialPdServerList;

    public boolean isFake() {
        return fake;
    }

    public void setFake(boolean fake) {
        this.fake = fake;
    }

    public CliOptions getCliOptions() {
        return cliOptions;
    }

    public void setCliOptions(CliOptions cliOptions) {
        this.cliOptions = cliOptions;
    }

    public RpcOptions getPdRpcOptions() {
        return pdRpcOptions;
    }

    public void setPdRpcOptions(RpcOptions pdRpcOptions) {
        this.pdRpcOptions = pdRpcOptions;
    }

    public String getPdGroupId() {
        return pdGroupId;
    }

    public void setPdGroupId(String pdGroupId) {
        this.pdGroupId = pdGroupId;
    }

    public List<RegionRouteTableOptions> getRegionRouteTableOptionsList() {
        return regionRouteTableOptionsList;
    }

    public void setRegionRouteTableOptionsList(List<RegionRouteTableOptions> regionRouteTableOptionsList) {
        this.regionRouteTableOptionsList = regionRouteTableOptionsList;
    }

    public String getInitialServerList() {
        return initialServerList;
    }

    public void setInitialServerList(String initialServerList) {
        this.initialServerList = initialServerList;
    }

    public String getInitialPdServerList() {
        return initialPdServerList;
    }

    public void setInitialPdServerList(String initialPdServerList) {
        this.initialPdServerList = initialPdServerList;
    }

    @Override
    public String toString() {
        return "PlacementDriverOptions{" + "fake=" + fake + ", cliOptions=" + cliOptions + ", pdRpcOptions="
               + pdRpcOptions + ", pdGroupId='" + pdGroupId + '\'' + ", regionRouteTableOptionsList="
               + regionRouteTableOptionsList + ", initialServerList='" + initialServerList + '\''
               + ", initialPdServerList='" + initialPdServerList + '\'' + '}';
    }
}
