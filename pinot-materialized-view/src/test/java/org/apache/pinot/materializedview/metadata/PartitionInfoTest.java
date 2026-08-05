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
package org.apache.pinot.materializedview.metadata;

import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class PartitionInfoTest {

  @Test
  public void testToAndFromFieldMapValid() {
    PartitionInfo info = new PartitionInfo(
        PartitionState.VALID, new PartitionFingerprint(10, 5000L), 1700006400000L);
    Map<String, String> fieldMap = info.toFieldMap();
    assertEquals(fieldMap.get("state"), "V");
    assertEquals(fieldMap.get("segmentCount"), "10");
    assertEquals(fieldMap.get("crc"), "5000");
    assertEquals(fieldMap.get("lastRefreshTime"), "1700006400000");

    PartitionInfo decoded = PartitionInfo.fromFieldMap(fieldMap);
    assertEquals(decoded.getState(), PartitionState.VALID);
    assertEquals(decoded.getFingerprint().getSegmentCount(), 10);
    assertEquals(decoded.getFingerprint().getCrcChecksum(), 5000L);
    assertEquals(decoded.getLastRefreshTime(), 1700006400000L);
    assertEquals(decoded, info);
  }

  @Test
  public void testToAndFromFieldMapStale() {
    PartitionInfo info = new PartitionInfo(
        PartitionState.STALE, new PartitionFingerprint(3, -999L), 0L);
    Map<String, String> fieldMap = info.toFieldMap();
    assertEquals(fieldMap.get("state"), "S");

    PartitionInfo decoded = PartitionInfo.fromFieldMap(fieldMap);
    assertEquals(decoded.getState(), PartitionState.STALE);
    assertEquals(decoded.getFingerprint(), new PartitionFingerprint(3, -999L));
    assertEquals(decoded.getLastRefreshTime(), 0L);
    assertEquals(decoded, info);
  }

  @Test
  public void testToAndFromFieldMapZeroValues() {
    PartitionInfo info = new PartitionInfo(
        PartitionState.VALID, new PartitionFingerprint(0, 0L), 0L);
    PartitionInfo decoded = PartitionInfo.fromFieldMap(info.toFieldMap());
    assertEquals(decoded, info);
  }

  @Test
  public void testFromFieldMapIgnoresUnknownKeys() {
    Map<String, String> fieldMap = new HashMap<>();
    fieldMap.put("state", "V");
    fieldMap.put("segmentCount", "10");
    fieldMap.put("crc", "5000");
    fieldMap.put("lastRefreshTime", "1700006400000");
    fieldMap.put("unknownFutureField", "something");

    PartitionInfo decoded = PartitionInfo.fromFieldMap(fieldMap);
    assertEquals(decoded.getState(), PartitionState.VALID);
    assertEquals(decoded.getFingerprint().getSegmentCount(), 10);
  }

  @Test
  public void testWithState() {
    PartitionFingerprint fp = new PartitionFingerprint(5, 1234L);
    PartitionInfo valid = new PartitionInfo(PartitionState.VALID, fp, 1000L);
    PartitionInfo stale = valid.withState(PartitionState.STALE);

    assertEquals(stale.getState(), PartitionState.STALE);
    assertEquals(stale.getFingerprint(), fp);
    assertEquals(stale.getLastRefreshTime(), 1000L);
    assertNotEquals(stale, valid);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testFromFieldMapMissingState() {
    Map<String, String> map = new HashMap<>();
    map.put("segmentCount", "10");
    map.put("crc", "5000");
    map.put("lastRefreshTime", "1000");
    PartitionInfo.fromFieldMap(map);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testFromFieldMapInvalidState() {
    Map<String, String> map = new HashMap<>();
    map.put("state", "X");
    map.put("segmentCount", "10");
    map.put("crc", "5000");
    map.put("lastRefreshTime", "1000");
    PartitionInfo.fromFieldMap(map);
  }

  @Test
  public void testEqualsAndHashCode() {
    PartitionFingerprint fp = new PartitionFingerprint(5, 100L);
    PartitionInfo a = new PartitionInfo(PartitionState.VALID, fp, 1000L);
    PartitionInfo b = new PartitionInfo(PartitionState.VALID, fp, 1000L);
    PartitionInfo c = new PartitionInfo(PartitionState.STALE, fp, 1000L);
    PartitionInfo d = new PartitionInfo(PartitionState.VALID, fp, 2000L);

    assertEquals(a, b);
    assertEquals(a.hashCode(), b.hashCode());
    assertNotEquals(a, c);
    assertNotEquals(a, d);
    assertNotEquals(a, null);
  }

  @Test
  public void testToString() {
    PartitionInfo info = new PartitionInfo(
        PartitionState.VALID, new PartitionFingerprint(3, 42L), 999L);
    String str = info.toString();
    assertTrue(str.contains("VALID"));
    assertTrue(str.contains("lastRefreshTime=999"));
  }
}
