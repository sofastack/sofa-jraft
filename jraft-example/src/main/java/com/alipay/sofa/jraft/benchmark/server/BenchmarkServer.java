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
package com.alipay.sofa.jraft.benchmark.server;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.benchmark.Yaml;
import com.alipay.sofa.jraft.example.rheakv.Node;
import com.alipay.sofa.jraft.rhea.metrics.KVMetrics;
import com.alipay.sofa.jraft.rhea.options.RheaKVStoreOptions;
import com.codahale.metrics.ConsoleReporter;

/**
 *
 * @author jiachun.fjc
 */
public class BenchmarkServer {

    private static final Logger LOG = LoggerFactory.getLogger(BenchmarkServer.class);

    public static void main(final String[] args) {
        if (args.length < 3) {
            LOG.error("[initialServerList], [configPath] are needed.");
        }
        final String initialServerList = args[1];
        final String configPath = args[2];

        final RheaKVStoreOptions opts = Yaml.readConfig(configPath);
        opts.setInitialServerList(initialServerList);

        final Node node = new Node(opts);
        node.start();

        ConsoleReporter.forRegistry(KVMetrics.metricRegistry()) //
                .build() //
                .start(30, TimeUnit.SECONDS);

        Runtime.getRuntime().addShutdownHook(new Thread(node::stop));
        LOG.info("BenchmarkServer start OK, options: {}", opts);
    }
}
