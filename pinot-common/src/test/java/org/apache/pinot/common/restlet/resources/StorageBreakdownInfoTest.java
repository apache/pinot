/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.common.restlet.resources;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class StorageBreakdownInfoTest {

  @Test
  public void testTierInfoGetters() {
    StorageBreakdownInfo.TierInfo tierInfo = new StorageBreakdownInfo.TierInfo(5, 1048576L);

    assertEquals(tierInfo.getCount(), 5);
    assertEquals(tierInfo.getSizePerReplicaInBytes(), 1048576L);
  }

  @Test
  public void testGetTiersMap() {
    Map<String, StorageBreakdownInfo.TierInfo> tiers = new HashMap<>();
    tiers.put("hotTier", new StorageBreakdownInfo.TierInfo(3, 2000000L));
    tiers.put("coldTier", new StorageBreakdownInfo.TierInfo(7, 8000000L));

    StorageBreakdownInfo info = new StorageBreakdownInfo(tiers);

    assertNotNull(info.getTiers());
    assertEquals(info.getTiers().size(), 2);
    assertEquals(info.getTiers().get("hotTier").getCount(), 3);
    assertEquals(info.getTiers().get("hotTier").getSizePerReplicaInBytes(), 2000000L);
    assertEquals(info.getTiers().get("coldTier").getCount(), 7);
    assertEquals(info.getTiers().get("coldTier").getSizePerReplicaInBytes(), 8000000L);
  }

  @Test
  public void testJsonRoundTripWithMultipleTiers()
      throws Exception {
    Map<String, StorageBreakdownInfo.TierInfo> tiers = new HashMap<>();
    tiers.put("tier1", new StorageBreakdownInfo.TierInfo(10, 5000000L));
    tiers.put("tier2", new StorageBreakdownInfo.TierInfo(4, 1500000L));

    StorageBreakdownInfo original = new StorageBreakdownInfo(tiers);

    String json = JsonUtils.objectToString(original);
    StorageBreakdownInfo deserialized = JsonUtils.stringToObject(json, StorageBreakdownInfo.class);

    assertNotNull(deserialized.getTiers());
    assertEquals(deserialized.getTiers().size(), 2);

    StorageBreakdownInfo.TierInfo tier1 = deserialized.getTiers().get("tier1");
    assertNotNull(tier1);
    assertEquals(tier1.getCount(), 10);
    assertEquals(tier1.getSizePerReplicaInBytes(), 5000000L);

    StorageBreakdownInfo.TierInfo tier2 = deserialized.getTiers().get("tier2");
    assertNotNull(tier2);
    assertEquals(tier2.getCount(), 4);
    assertEquals(tier2.getSizePerReplicaInBytes(), 1500000L);
  }

  @Test
  public void testJsonRoundTripEmptyTiers()
      throws Exception {
    StorageBreakdownInfo original = new StorageBreakdownInfo(Collections.emptyMap());

    String json = JsonUtils.objectToString(original);
    StorageBreakdownInfo deserialized = JsonUtils.stringToObject(json, StorageBreakdownInfo.class);

    assertNotNull(deserialized.getTiers());
    assertTrue(deserialized.getTiers().isEmpty());
  }

  @Test
  public void testJsonIgnoresUnknownFieldsOnStorageBreakdownInfo()
      throws Exception {
    String json = "{\"tiers\":{\"hotTier\":{\"count\":2,\"sizePerReplicaInBytes\":900000}},"
        + "\"unknownTopField\":\"ignored\"}";

    StorageBreakdownInfo deserialized = JsonUtils.stringToObject(json, StorageBreakdownInfo.class);

    assertNotNull(deserialized.getTiers());
    assertEquals(deserialized.getTiers().size(), 1);
    assertEquals(deserialized.getTiers().get("hotTier").getCount(), 2);
    assertEquals(deserialized.getTiers().get("hotTier").getSizePerReplicaInBytes(), 900000L);
  }

  @Test
  public void testJsonIgnoresUnknownFieldsOnTierInfo()
      throws Exception {
    String json = "{\"tiers\":{\"tier1\":{\"count\":3,\"sizePerReplicaInBytes\":4000000,"
        + "\"futureField\":\"ignored\"}}}";

    StorageBreakdownInfo deserialized = JsonUtils.stringToObject(json, StorageBreakdownInfo.class);

    assertNotNull(deserialized.getTiers());
    StorageBreakdownInfo.TierInfo tierInfo = deserialized.getTiers().get("tier1");
    assertNotNull(tierInfo);
    assertEquals(tierInfo.getCount(), 3);
    assertEquals(tierInfo.getSizePerReplicaInBytes(), 4000000L);
  }

  @Test
  public void testNullTiersMap()
      throws Exception {
    StorageBreakdownInfo original = new StorageBreakdownInfo(null);

    String json = JsonUtils.objectToString(original);
    StorageBreakdownInfo deserialized = JsonUtils.stringToObject(json, StorageBreakdownInfo.class);

    assertNull(deserialized.getTiers());
  }
}
