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

import java.util.concurrent.Executor;

import com.alipay.sofa.jraft.rhea.cmd.pd.BaseRequest;
import com.alipay.sofa.jraft.rhea.cmd.pd.BaseResponse;
import com.alipay.sofa.jraft.rhea.cmd.pd.CreateRegionIdRequest;
import com.alipay.sofa.jraft.rhea.cmd.pd.GetClusterInfoRequest;
import com.alipay.sofa.jraft.rhea.cmd.pd.GetStoreIdRequest;
import com.alipay.sofa.jraft.rhea.cmd.pd.GetStoreInfoRequest;
import com.alipay.sofa.jraft.rhea.cmd.pd.RegionHeartbeatRequest;
import com.alipay.sofa.jraft.rhea.cmd.pd.SetStoreInfoRequest;
import com.alipay.sofa.jraft.rhea.cmd.pd.StoreHeartbeatRequest;
import com.alipay.sofa.jraft.rhea.errors.RheaRuntimeException;
import com.alipay.sofa.jraft.rpc.RpcContext;
import com.alipay.sofa.jraft.rpc.RpcProcessor;
import com.alipay.sofa.jraft.util.Requires;

/**
 * The PD server does not actively push information to any RheaKV-node,
 * and all information about the cluster is reported by the RheaKV-node.
 *
 * <pre>
 * PD server needs to handle two types of heartbeats:
 *  1. One case is the heartbeat of 'Store'. The PD is processed in the
 *      'handleStoreHeartbeat' method.  The main task is to save the
 *      state of current 'Store', including the number of regions, and
 *      how many region-leader in the 'Store', etc. This information will
 *      be used as scheduling reference.
 *
 *  2. The other case is the heartbeat reported by the region-leader. The
 *      PD is processed in the 'handleRegionHeartbeat' method. Note that
 *      only the region-leader can report the 'Region' information, and
 *      the flower cant not.
 * </pre>
 *
 * @author jiachun.fjc
 */
public class PlacementDriverProcessor<T extends BaseRequest> implements RpcProcessor<T> {

    private final Class<T>               reqClazz;
    private final PlacementDriverService placementDriverService;
    private final Executor               executor;

    public PlacementDriverProcessor(Class<T> reqClazz, PlacementDriverService placementDriverService, Executor executor) {
        this.reqClazz = Requires.requireNonNull(reqClazz, "reqClazz");
        this.placementDriverService = Requires.requireNonNull(placementDriverService, "placementDriverService");
        this.executor = executor;
    }

    @Override
    public void handleRequest(final RpcContext rpcCtx, final T request) {
        Requires.requireNonNull(request, "request");
        final RequestProcessClosure<BaseRequest, BaseResponse> closure = new RequestProcessClosure<>(request, rpcCtx);
        switch (request.magic()) {
            case BaseRequest.STORE_HEARTBEAT:
                this.placementDriverService.handleStoreHeartbeatRequest((StoreHeartbeatRequest) request, closure);
                break;
            case BaseRequest.REGION_HEARTBEAT:
                this.placementDriverService.handleRegionHeartbeatRequest((RegionHeartbeatRequest) request, closure);
                break;
            case BaseRequest.GET_CLUSTER_INFO:
                this.placementDriverService.handleGetClusterInfoRequest((GetClusterInfoRequest) request, closure);
                break;
            case BaseRequest.GET_STORE_INFO:
                this.placementDriverService.handleGetStoreInfoRequest((GetStoreInfoRequest) request, closure);
                break;
            case BaseRequest.SET_STORE_INFO:
                this.placementDriverService.handleSetStoreInfoRequest((SetStoreInfoRequest) request, closure);
                break;
            case BaseRequest.GET_STORE_ID:
                this.placementDriverService.handleGetStoreIdRequest((GetStoreIdRequest) request, closure);
                break;
            case BaseRequest.CREATE_REGION_ID:
                this.placementDriverService.handleCreateRegionIdRequest((CreateRegionIdRequest) request, closure);
                break;
            default:
                throw new RheaRuntimeException("Unsupported request type: " + request.getClass().getName());
        }
    }

    @Override
    public String interest() {
        return reqClazz.getName();
    }

    @Override
    public Executor executor() {
        return this.executor;
    }
}
