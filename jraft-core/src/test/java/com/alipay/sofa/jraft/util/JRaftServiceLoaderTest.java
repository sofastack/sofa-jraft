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

import java.util.List;
import java.util.ServiceConfigurationError;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

/**
 *
 * @author jiachun.fjc
 */
public class JRaftServiceLoaderTest {

    @Test
    public void serviceNotFoundTest() {
        try {
            JRaftServiceLoader.load(NoImplTest.class).first();
            fail("fail");
        } catch (final ServiceConfigurationError e) {
            assertEquals(
                "com.alipay.sofa.jraft.util.JRaftServiceLoaderTest$NoImplTest: could not find any implementation for class",
                e.getMessage());
        }
    }

    @Test
    public void serviceSortTest() {
        final List<SortTest> serviceList = JRaftServiceLoader.load(SortTest.class) //
            .sort();
        assertEquals(3, serviceList.size());
        assertEquals(SortImpl9Test.class, serviceList.get(0).getClass());
        assertEquals(SortImpl2Test.class, serviceList.get(1).getClass());
        assertEquals(SortImpl1Test.class, serviceList.get(2).getClass());
    }

    @Test
    public void serviceGetFirstTest() {
        final SortTest service = JRaftServiceLoader.load(SortTest.class) //
            .first();
        assertNotNull(service);
        assertEquals(SortImpl9Test.class, service.getClass());
    }

    @Test
    public void serviceFindTest() {
        final SortTest service = JRaftServiceLoader.load(SortTest.class) //
            .find("sort2");
        assertNotNull(service);
        assertEquals(SortImpl2Test.class, service.getClass());
    }

    public interface NoImplTest {
    }

    public interface SortTest {
    }

    @SPI(name = "sort1", priority = 1)
    public static class SortImpl1Test implements SortTest {
    }

    @SPI(name = "sort2", priority = 2)
    public static class SortImpl2Test implements SortTest {
    }

    @SPI(name = "sort9", priority = 9)
    public static class SortImpl9Test implements SortTest {
    }
}
