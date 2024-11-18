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
package com.alipay.sofa.jraft.example.counter;

import com.alipay.sofa.jraft.CliService;
import com.alipay.sofa.jraft.JRaftUtils;
import com.alipay.sofa.jraft.RaftServiceFactory;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.option.CliOptions;

/**
 *
 */
public class TestCli {

    private static final String GROUP = "jraft-example-group";

    public static void main(String[] args) {
        // 创建并初始化 CliService
        CliService cliService = RaftServiceFactory.createAndInitCliService(new CliOptions());
        // 使用CliService
        Configuration conf = JRaftUtils.getConfiguration("127.0.0.1:8080");
        Status status = cliService.removePeer(GROUP, conf, new PeerId("127.0.0.1", 8082));
        if (status.isOk()) {
            System.out.println("添加节点成功");
            System.exit(0);
        }
        System.err.println("添加节点失败");
        System.exit(-1);
    }
}
