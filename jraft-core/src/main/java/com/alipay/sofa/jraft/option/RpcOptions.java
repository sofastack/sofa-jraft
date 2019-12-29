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
package com.alipay.sofa.jraft.option;

import com.codahale.metrics.MetricRegistry;

public class RpcOptions {

    /**
     * Rpc connect timeout in milliseconds
     * Default: 1000(1s)
     */
    private int            rpcConnectTimeoutMs        = 1000;

    /**
     * RPC request default timeout in milliseconds
     * Default: 5000(5s)
     */
    private int            rpcDefaultTimeout          = 5000;

    /**
     * Install snapshot RPC request default timeout in milliseconds
     * Default: 5 * 60 * 1000(5min)
     */
    private int            rpcInstallSnapshotTimeout  = 5 * 60 * 1000;

    /**
     * RPC process thread pool size
     * Default: 80
     */
    private int            rpcProcessorThreadPoolSize = 80;

    /**
     * Whether to enable checksum for RPC.
     * Default: false
     */
    private boolean        enableRpcChecksum          = false;

    /**
     * Whether to enable server SSL support.
     * Default: false
     */
    private boolean        enableServerSsl            = false;

    /**
     * Whether to enable Client SSL support.
     * Default: false
     */
    private boolean        enableClientSsl            = false;

    /**
     * Whether to enable server SSL client auth.
     * Default: false
     */
    private boolean        serverSslClientAuth        = false;

    /**
     * Server SSL keystore file path.
     */
    private String         serverSslKeystore;

    /**
     * Server SSL keystore password.
     */
    private String         serverSslKeystorePassword;

    /**
     * Server SSL keystore type, JKS or pkcs12 for example.
     */
    private String         serverSslKeystoreType;

    /**
     * Server SSL KeyManagerFactory algorithm.
     */
    private String         serverSslKmfAlgorithm;

    /**
     * Client SSL keystore file path.
     */
    private String         clientSslKeystore;

    /**
     * Client SSL keystore password.
     */
    private String         clientSslKeystorePassword;

    /**
     * Client SSL keystore type, JKS or pkcs12 for example.
     */
    private String         clientSslKeystoreType;

    /**
     * Client SSL TrustManagerFactory algorithm.
     */
    private String         clientSslTmfAlgorithm;

    /**
     * Metric registry for RPC services, user should not use this field.
     */
    private MetricRegistry metricRegistry;

    public int getRpcConnectTimeoutMs() {
        return this.rpcConnectTimeoutMs;
    }

    public void setRpcConnectTimeoutMs(int rpcConnectTimeoutMs) {
        this.rpcConnectTimeoutMs = rpcConnectTimeoutMs;
    }

    public int getRpcDefaultTimeout() {
        return this.rpcDefaultTimeout;
    }

    public void setRpcDefaultTimeout(int rpcDefaultTimeout) {
        this.rpcDefaultTimeout = rpcDefaultTimeout;
    }

    public int getRpcInstallSnapshotTimeout() {
        return rpcInstallSnapshotTimeout;
    }

    public void setRpcInstallSnapshotTimeout(int rpcInstallSnapshotTimeout) {
        this.rpcInstallSnapshotTimeout = rpcInstallSnapshotTimeout;
    }

    public int getRpcProcessorThreadPoolSize() {
        return this.rpcProcessorThreadPoolSize;
    }

    public void setRpcProcessorThreadPoolSize(int rpcProcessorThreadPoolSize) {
        this.rpcProcessorThreadPoolSize = rpcProcessorThreadPoolSize;
    }

    public boolean isEnableRpcChecksum() {
        return enableRpcChecksum;
    }

    public void setEnableRpcChecksum(boolean enableRpcChecksum) {
        this.enableRpcChecksum = enableRpcChecksum;
    }

    public boolean isEnableServerSsl() {
        return enableServerSsl;
    }

    public void setEnableServerSsl(boolean enableServerSsl) {
        this.enableServerSsl = enableServerSsl;
    }

    public boolean isEnableClientSsl() {
        return enableClientSsl;
    }

    public void setEnableClientSsl(boolean enableClientSsl) {
        this.enableClientSsl = enableClientSsl;
    }

    public boolean isServerSslClientAuth() {
        return serverSslClientAuth;
    }

    public void setServerSslClientAuth(boolean serverSslClientAuth) {
        this.serverSslClientAuth = serverSslClientAuth;
    }

    public String getServerSslKeystore() {
        return serverSslKeystore;
    }

    public void setServerSslKeystore(String serverSslKeystore) {
        this.serverSslKeystore = serverSslKeystore;
    }

    public String getServerSslKeystorePassword() {
        return serverSslKeystorePassword;
    }

    public void setServerSslKeystorePassword(String serverSslKeystorePassword) {
        this.serverSslKeystorePassword = serverSslKeystorePassword;
    }

    public String getServerSslKeystoreType() {
        return serverSslKeystoreType;
    }

    public void setServerSslKeystoreType(String serverSslKeystoreType) {
        this.serverSslKeystoreType = serverSslKeystoreType;
    }

    public String getServerSslKmfAlgorithm() {
        return serverSslKmfAlgorithm;
    }

    public void setServerSslKmfAlgorithm(String serverSslKmfAlgorithm) {
        this.serverSslKmfAlgorithm = serverSslKmfAlgorithm;
    }

    public String getClientSslKeystore() {
        return clientSslKeystore;
    }

    public void setClientSslKeystore(String clientSslKeystore) {
        this.clientSslKeystore = clientSslKeystore;
    }

    public String getClientSslKeystorePassword() {
        return clientSslKeystorePassword;
    }

    public void setClientSslKeystorePassword(String clientSslKeystorePassword) {
        this.clientSslKeystorePassword = clientSslKeystorePassword;
    }

    public String getClientSslKeystoreType() {
        return clientSslKeystoreType;
    }

    public void setClientSslKeystoreType(String clientSslKeystoreType) {
        this.clientSslKeystoreType = clientSslKeystoreType;
    }

    public String getClientSslTmfAlgorithm() {
        return clientSslTmfAlgorithm;
    }

    public void setClientSslTmfAlgorithm(String clientSslTmfAlgorithm) {
        this.clientSslTmfAlgorithm = clientSslTmfAlgorithm;
    }

    public MetricRegistry getMetricRegistry() {
        return metricRegistry;
    }

    public void setMetricRegistry(MetricRegistry metricRegistry) {
        this.metricRegistry = metricRegistry;
    }

    @Override
    public String toString() {
        return "RpcOptions{" + "rpcConnectTimeoutMs=" + rpcConnectTimeoutMs + ", rpcDefaultTimeout="
               + rpcDefaultTimeout + ", rpcInstallSnapshotTimeout=" + rpcInstallSnapshotTimeout
               + ", rpcProcessorThreadPoolSize=" + rpcProcessorThreadPoolSize + ", enableRpcChecksum="
               + enableRpcChecksum + ", enableServerSsl=" + enableServerSsl + ", enableClientSsl=" + enableClientSsl
               + ", serverSslClientAuth=" + serverSslClientAuth + ", serverSslKeystore='" + serverSslKeystore
               + ", serverSslKeystorePassword='" + serverSslKeystorePassword + ", serverSslKeystoreType='"
               + serverSslKeystoreType + ", serverSslKmfAlgorithm='" + serverSslKmfAlgorithm + ", clientSslKeystore='"
               + clientSslKeystore + ", clientSslKeystorePassword='" + clientSslKeystorePassword
               + ", clientSslKeystoreType='" + clientSslKeystoreType + ", clientSslTmfAlgorithm='"
               + clientSslTmfAlgorithm + ", metricRegistry=" + metricRegistry + '}';
    }
}
