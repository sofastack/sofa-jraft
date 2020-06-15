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
package com.alipay.sofa.jraft.util.timer;

import com.alipay.sofa.jraft.core.Scheduler;

/**
 * @author jiachun.fjc
 */
public interface RaftTimerFactory {

    Timer getElectionTimer(final boolean shared, final String name);

    Timer getVoteTimer(final boolean shared, final String name);

    Timer getStepDownTimer(final boolean shared, final String name);

    Timer getSnapshotTimer(final boolean shared, final String name);

    Scheduler getRaftScheduler(final boolean shared, final int workerNum, final String name);

    Timer createTimer(final String name);

    Scheduler createScheduler(final int workerNum, final String name);
}
