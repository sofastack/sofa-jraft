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

import com.alipay.sofa.jraft.option.CliOptions;
import com.alipay.sofa.jraft.rhea.client.DefaultRheaKVCliService;
import com.alipay.sofa.jraft.rhea.client.RheaKVCliService;

/**
 * Service factory to create rheaKV services, such as RheaKVCliService etc.
 *
 * @author jiachun.fjc
 */
public final class RheaKVServiceFactory {

    /**
     * Create and initialize a RheaKVCliService instance.
     */
    public static RheaKVCliService createAndInitRheaKVCliService(final CliOptions opts) {
        final RheaKVCliService cliService = new DefaultRheaKVCliService();
        if (!cliService.init(opts)) {
            throw new IllegalStateException("Fail to init RheaKVCliService");
        }
        return cliService;
    }

    private RheaKVServiceFactory() {
    }
}
