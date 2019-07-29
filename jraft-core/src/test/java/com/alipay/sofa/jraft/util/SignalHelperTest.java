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
package com.alipay.sofa.jraft.util;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author jiachun.fjc
 */
public class SignalHelperTest {

    public static void main(String[] args) throws InterruptedException {
        // test with:
        //
        // kill -s USR2 pid

        final List<JRaftSignalHandler> handlers = new ArrayList<>();
        handlers.add((signalName) -> System.out.println("signal test: " + signalName));

        if (SignalHelper.supportSignal()) {
            SignalHelper.addSignal(SignalHelper.SIG_USR2, handlers);
        }

        Thread.sleep(300000);
    }
}
