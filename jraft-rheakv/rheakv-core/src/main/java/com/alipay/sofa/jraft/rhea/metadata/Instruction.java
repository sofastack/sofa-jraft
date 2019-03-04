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

import com.alipay.sofa.jraft.util.Endpoint;

/**
 * Instruction of the placement driver server.
 *
 * @author jiachun.fjc
 */
public class Instruction implements Serializable {

    private static final long serialVersionUID = 2675841162817080976L;

    private Region            region;
    private ChangePeer        changePeer;
    private TransferLeader    transferLeader;
    private RangeSplit        rangeSplit;

    public Region getRegion() {
        return region;
    }

    public void setRegion(Region region) {
        this.region = region;
    }

    public ChangePeer getChangePeer() {
        return changePeer;
    }

    public void setChangePeer(ChangePeer changePeer) {
        this.changePeer = changePeer;
    }

    public TransferLeader getTransferLeader() {
        return transferLeader;
    }

    public void setTransferLeader(TransferLeader transferLeader) {
        this.transferLeader = transferLeader;
    }

    public RangeSplit getRangeSplit() {
        return rangeSplit;
    }

    public void setRangeSplit(RangeSplit rangeSplit) {
        this.rangeSplit = rangeSplit;
    }

    @Override
    public String toString() {
        return "Instruction{" + "region=" + region + ", changePeer=" + changePeer + ", transferLeader="
               + transferLeader + ", rangeSplit=" + rangeSplit + '}';
    }

    public static class ChangePeer implements Serializable {

        private static final long serialVersionUID = -6753587746283650702L;

        // TODO support add/update peer
    }

    public static class TransferLeader implements Serializable {

        private static final long serialVersionUID = 7483209239871846301L;

        private long              moveToStoreId;
        private Endpoint          moveToEndpoint;

        public long getMoveToStoreId() {
            return moveToStoreId;
        }

        public void setMoveToStoreId(long moveToStoreId) {
            this.moveToStoreId = moveToStoreId;
        }

        public Endpoint getMoveToEndpoint() {
            return moveToEndpoint;
        }

        public void setMoveToEndpoint(Endpoint moveToEndpoint) {
            this.moveToEndpoint = moveToEndpoint;
        }

        @Override
        public String toString() {
            return "TransferLeader{" + "moveToStoreId=" + moveToStoreId + ", moveToEndpoint=" + moveToEndpoint + '}';
        }
    }

    public static class RangeSplit implements Serializable {

        private static final long serialVersionUID = -3451109819719367744L;

        private Long              newRegionId;

        public Long getNewRegionId() {
            return newRegionId;
        }

        public void setNewRegionId(Long newRegionId) {
            this.newRegionId = newRegionId;
        }

        @Override
        public String toString() {
            return "RangeSplit{" + "newRegionId=" + newRegionId + '}';
        }
    }
}
