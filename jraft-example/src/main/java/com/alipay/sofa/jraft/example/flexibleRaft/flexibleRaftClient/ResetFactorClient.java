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
package com.alipay.sofa.jraft.example.flexibleRaft.flexibleRaftClient;

import com.alipay.sofa.jraft.CliService;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.core.CliServiceImpl;
import com.alipay.sofa.jraft.option.CliOptions;

/**
 * @author Akai
 */
public class ResetFactorClient {
    public static void main(String[] args) {
        CliService cliService = new CliServiceImpl();
        cliService.init(new CliOptions());
        //n=5,w=2,r=4  writeFactor=4,readFactor=6
        //n=5,w=4,r=2  writeFactor=8,readFactor=2
        //n=6,w=3,r=4, writeFactor=4,readFactor=6
        //n=6,w=5,r=2, writeFactor=8,readFactor=2
        int writeFactor = 8;
        int readFactor = 2;
        String groupId = "counter";
        String confStr = "127.0.0.1:8081,127.0.0.1:8082,127.0.0.1:8083,127.0.0.1:8084,127.0.0.1:8085";
        final Configuration conf = new Configuration();
        if (!conf.parse(confStr)) {
            throw new IllegalArgumentException("Fail to parse conf:" + confStr);
        }
        cliService.resetFactor(groupId, conf, readFactor, writeFactor);
    }
}
