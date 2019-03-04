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
package com.alipay.sofa.jraft.rhea.client;

import java.util.List;

import org.junit.Test;

import com.alipay.sofa.jraft.rhea.KeyValueTool;
import com.alipay.sofa.jraft.rhea.errors.RouteTableException;
import com.alipay.sofa.jraft.rhea.metadata.Region;
import com.alipay.sofa.jraft.rhea.metadata.RegionEpoch;
import com.alipay.sofa.jraft.util.BytesUtil;

import static org.junit.Assert.assertEquals;

/**
 * @author jiachun.fjc
 */
public class RegionRouteTableTest {

    @Test
    public void splitRegionTest() {
        RegionRouteTable table = new RegionRouteTable();
        Region region = makeRegion(-1, null, null);
        table.addOrUpdateRegion(region);
        Region newRegion = makeRegion(1, BytesUtil.writeUtf8("t"), null);
        table.splitRegion(-1, newRegion);
        Region found = table.findRegionByKey(BytesUtil.writeUtf8("a"));
        assertEquals(-1, found.getId());
        found = table.findRegionByKey(BytesUtil.writeUtf8("w"));
        assertEquals(1, found.getId());
    }

    @Test
    public void findRegionByKeyTest() {
        // case-1
        {
            RegionRouteTable table = new RegionRouteTable();
            Region region = makeRegion(-1, null, null);
            table.addOrUpdateRegion(region);
            Region found = table.findRegionByKey(new byte[0]);
            assertEquals(region, found);
        }
        // case-2
        {
            RegionRouteTable table = new RegionRouteTable();
            Region r1 = makeRegion(1, null, KeyValueTool.makeKey("c"));
            Region r2 = makeRegion(2, KeyValueTool.makeKey("c"), KeyValueTool.makeKey("e"));
            Region r3 = makeRegion(3, KeyValueTool.makeKey("e"), null);
            table.addOrUpdateRegion(r1);
            table.addOrUpdateRegion(r2);
            table.addOrUpdateRegion(r3);
            Region found = table.findRegionByKey(KeyValueTool.makeKey("abs"));
            assertEquals(r1.getId(), found.getId());
            found = table.findRegionByKey(KeyValueTool.makeKey("cde"));
            assertEquals(r2.getId(), found.getId());
            found = table.findRegionByKey(KeyValueTool.makeKey("efg"));
            assertEquals(r3.getId(), found.getId());
        }
    }

    @Test
    public void findRegionsByKeyRangeTest() {
        // case-1
        {
            RegionRouteTable table = new RegionRouteTable();
            Region region = makeRegion(-1, null, null);
            table.addOrUpdateRegion(region);
            List<Region> regionList = table.findRegionsByKeyRange(KeyValueTool.makeKey("a"), KeyValueTool.makeKey("w"));
            assertEquals(1, regionList.size());
        }
        // case-2
        {
            RegionRouteTable table = new RegionRouteTable();
            Region r1 = makeRegion(1, null, KeyValueTool.makeKey("c"));
            Region r2 = makeRegion(2, KeyValueTool.makeKey("c"), KeyValueTool.makeKey("e"));
            Region r3 = makeRegion(3, KeyValueTool.makeKey("e"), null);
            table.addOrUpdateRegion(r1);
            table.addOrUpdateRegion(r2);
            table.addOrUpdateRegion(r3);
            List<Region> foundList = table.findRegionsByKeyRange(KeyValueTool.makeKey("adc"),
                KeyValueTool.makeKey("def"));
            assertEquals(2, foundList.size());
            assertEquals(r1.getId(), foundList.get(0).getId());
            assertEquals(r2.getId(), foundList.get(1).getId());

            foundList = table.findRegionsByKeyRange(KeyValueTool.makeKey("c"), KeyValueTool.makeKey("def"));
            assertEquals(1, foundList.size());
            assertEquals(r2.getId(), foundList.get(0).getId());
        }
        // case-3
        {
            RegionRouteTable table = new RegionRouteTable();
            Region r1 = makeRegion(1, null, KeyValueTool.makeKey("c"));
            Region r2 = makeRegion(2, KeyValueTool.makeKey("c"), KeyValueTool.makeKey("e"));
            Region r3 = makeRegion(3, KeyValueTool.makeKey("e"), KeyValueTool.makeKey("g"));
            Region r4 = makeRegion(4, KeyValueTool.makeKey("g"), KeyValueTool.makeKey("i"));
            Region r5 = makeRegion(5, KeyValueTool.makeKey("i"), KeyValueTool.makeKey("k"));
            Region r6 = makeRegion(6, KeyValueTool.makeKey("k"), KeyValueTool.makeKey("n"));
            Region r7 = makeRegion(7, KeyValueTool.makeKey("n"), null);
            table.addOrUpdateRegion(r1);
            table.addOrUpdateRegion(r2);
            table.addOrUpdateRegion(r3);
            table.addOrUpdateRegion(r4);
            table.addOrUpdateRegion(r5);
            table.addOrUpdateRegion(r6);
            table.addOrUpdateRegion(r7);
            List<Region> foundList = table.findRegionsByKeyRange(KeyValueTool.makeKey("adc"),
                KeyValueTool.makeKey("def"));
            assertEquals(2, foundList.size());
            assertEquals(r1.getId(), foundList.get(0).getId());
            assertEquals(r2.getId(), foundList.get(1).getId());

            foundList = table.findRegionsByKeyRange(KeyValueTool.makeKey("c"), KeyValueTool.makeKey("kf"));
            assertEquals(5, foundList.size());
            assertEquals(r2.getId(), foundList.get(0).getId());
            assertEquals(r3.getId(), foundList.get(1).getId());
            assertEquals(r4.getId(), foundList.get(2).getId());
            assertEquals(r5.getId(), foundList.get(3).getId());
            assertEquals(r6.getId(), foundList.get(4).getId());

        }
    }

    @Test(expected = RouteTableException.class)
    public void brokenTest() {
        RegionRouteTable table = new RegionRouteTable();
        // Region r1 = makeRegion(1, null, KeyValueTool.makeKey("c"));
        Region r2 = makeRegion(2, KeyValueTool.makeKey("c"), KeyValueTool.makeKey("e"));
        Region r3 = makeRegion(3, KeyValueTool.makeKey("e"), KeyValueTool.makeKey("g"));
        Region r4 = makeRegion(4, KeyValueTool.makeKey("g"), KeyValueTool.makeKey("i"));
        Region r5 = makeRegion(5, KeyValueTool.makeKey("i"), KeyValueTool.makeKey("k"));
        Region r6 = makeRegion(6, KeyValueTool.makeKey("k"), KeyValueTool.makeKey("n"));
        Region r7 = makeRegion(7, KeyValueTool.makeKey("n"), null);
        table.addOrUpdateRegion(r2);
        table.addOrUpdateRegion(r3);
        table.addOrUpdateRegion(r4);
        table.addOrUpdateRegion(r5);
        table.addOrUpdateRegion(r6);
        table.addOrUpdateRegion(r7);
        table.findRegionByKey(KeyValueTool.makeKey("a"));
    }

    Region makeRegion(long id, byte[] startKey, byte[] endKey) {
        Region region = new Region();
        region.setId(id);
        region.setStartKey(startKey);
        region.setEndKey(endKey);
        region.setRegionEpoch(new RegionEpoch(-1, -1));
        return region;
    }
}
