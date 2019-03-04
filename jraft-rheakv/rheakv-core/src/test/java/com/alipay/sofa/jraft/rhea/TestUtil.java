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

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import com.alipay.sofa.jraft.entity.PeerId;

/**
 * @author jiachun.fjc
 */
public class TestUtil {

    public static final int INIT_PORT = 18800;

    public static List<PeerId> generatePeers(int n) {
        final List<PeerId> peers = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            peers.add(new PeerId("127.0.0.1", INIT_PORT + i));
        }
        return peers;
    }

    public static <T> T getByName(final Object obj, final String name, final Class<T> type) {
        try {
            final Field f = obj.getClass().getDeclaredField(name);
            f.setAccessible(true);
            return type.cast(f.get(obj));
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }
}
