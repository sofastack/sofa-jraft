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
package com.alipay.sofa.jraft.rhea;

import com.alipay.sofa.jraft.Lifecycle;
import com.alipay.sofa.jraft.rhea.cmd.pd.BaseRequest;
import com.alipay.sofa.jraft.rhea.cmd.pd.BaseResponse;
import com.alipay.sofa.jraft.rhea.cmd.pd.CreateRegionIdRequest;
import com.alipay.sofa.jraft.rhea.cmd.pd.GetClusterInfoRequest;
import com.alipay.sofa.jraft.rhea.cmd.pd.GetStoreIdRequest;
import com.alipay.sofa.jraft.rhea.cmd.pd.GetStoreInfoRequest;
import com.alipay.sofa.jraft.rhea.cmd.pd.RegionHeartbeatRequest;
import com.alipay.sofa.jraft.rhea.cmd.pd.SetStoreInfoRequest;
import com.alipay.sofa.jraft.rhea.cmd.pd.StoreHeartbeatRequest;
import com.alipay.sofa.jraft.rhea.options.PlacementDriverServerOptions;

/**
 * @author jiachun.fjc
 */
public interface PlacementDriverService extends Lifecycle<PlacementDriverServerOptions> {

    /**
     * {@link BaseRequest#STORE_HEARTBEAT}
     */
    void handleStoreHeartbeatRequest(final StoreHeartbeatRequest request,
                                     final RequestProcessClosure<BaseRequest, BaseResponse> closure);

    /**
     * {@link BaseRequest#REGION_HEARTBEAT}
     */
    void handleRegionHeartbeatRequest(final RegionHeartbeatRequest request,
                                      final RequestProcessClosure<BaseRequest, BaseResponse> closure);

    /**
     * {@link BaseRequest#GET_CLUSTER_INFO}
     */
    void handleGetClusterInfoRequest(final GetClusterInfoRequest request,
                                     final RequestProcessClosure<BaseRequest, BaseResponse> closure);

    /**
     * {@link BaseRequest#GET_STORE_INFO}
     */
    void handleGetStoreInfoRequest(final GetStoreInfoRequest request,
                                   final RequestProcessClosure<BaseRequest, BaseResponse> closure);

    /**
     * {@link BaseRequest#SET_STORE_INFO}
     */
    void handleSetStoreInfoRequest(final SetStoreInfoRequest request,
                                   final RequestProcessClosure<BaseRequest, BaseResponse> closure);

    /**
     * {@link BaseRequest#GET_STORE_ID}
     */
    void handleGetStoreIdRequest(final GetStoreIdRequest request,
                                 final RequestProcessClosure<BaseRequest, BaseResponse> closure);

    /**
     * {@link BaseRequest#CREATE_REGION_ID}
     */
    void handleCreateRegionIdRequest(final CreateRegionIdRequest request,
                                     final RequestProcessClosure<BaseRequest, BaseResponse> closure);
}
