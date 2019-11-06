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

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import com.alipay.sofa.jraft.rhea.util.Lists;
import com.alipay.sofa.jraft.util.Describer;

/**
 *
 * @author jiachun.fjc
 */
public final class DescriberManager {

    private static final DescriberManager INSTANCE   = new DescriberManager();

    private final List<Describer>         describers = new CopyOnWriteArrayList<>();

    public static DescriberManager getInstance() {
        return INSTANCE;
    }

    public void addDescriber(final Describer describer) {
        this.describers.add(describer);
    }

    public List<Describer> getAllDescribers() {
        return Lists.newArrayList(this.describers);
    }
}
