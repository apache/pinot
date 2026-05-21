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


public class PartitionFingerprintTest {

  @Test
  public void testEncodeAndDecode() {
    PartitionFingerprint fp = new PartitionFingerprint(5, 123456789L);
    String encoded = fp.encode();
    assertEquals(encoded, "5,123456789");

    PartitionFingerprint decoded = PartitionFingerprint.decode(encoded);
    assertEquals(decoded.getSegmentCount(), 5);
    assertEquals(decoded.getCrcChecksum(), 123456789L);
    assertEquals(decoded, fp);
  }

  @Test
  public void testEncodeAndDecodeZeroValues() {
    PartitionFingerprint fp = new PartitionFingerprint(0, 0L);
    String encoded = fp.encode();
    assertEquals(encoded, "0,0");

    PartitionFingerprint decoded = PartitionFingerprint.decode(encoded);
    assertEquals(decoded.getSegmentCount(), 0);
    assertEquals(decoded.getCrcChecksum(), 0L);
    assertEquals(decoded, fp);
  }

  @Test
  public void testEncodeAndDecodeNegativeCrc() {
    PartitionFingerprint fp = new PartitionFingerprint(3, -999L);
    PartitionFingerprint decoded = PartitionFingerprint.decode(fp.encode());
    assertEquals(decoded, fp);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testDecodeInvalidNoSeparator() {
    PartitionFingerprint.decode("12345");
  }

  @Test
  public void testEncodeMapEmpty() {
    assertEquals(PartitionFingerprint.encodeMap(new HashMap<>()), "");
    assertEquals(PartitionFingerprint.encodeMap(null), "");
  }

  @Test
  public void testEncodeMapSingleEntry() {
    Map<Long, PartitionFingerprint> map = new HashMap<>();
    map.put(1700006400000L, new PartitionFingerprint(10, 5000L));

    String encoded = PartitionFingerprint.encodeMap(map);
    assertEquals(encoded, "1700006400000=10,5000");

    Map<Long, PartitionFingerprint> decoded = PartitionFingerprint.decodeMap(encoded);
    assertEquals(decoded.size(), 1);
    assertEquals(decoded.get(1700006400000L), new PartitionFingerprint(10, 5000L));
  }

  @Test
  public void testEncodeMapMultipleEntries() {
    Map<Long, PartitionFingerprint> map = new HashMap<>();
    map.put(1000L, new PartitionFingerprint(1, 100L));
    map.put(2000L, new PartitionFingerprint(2, 200L));
    map.put(3000L, new PartitionFingerprint(3, 300L));

    String encoded = PartitionFingerprint.encodeMap(map);
    Map<Long, PartitionFingerprint> decoded = PartitionFingerprint.decodeMap(encoded);

    assertEquals(decoded.size(), 3);
    assertEquals(decoded.get(1000L), new PartitionFingerprint(1, 100L));
    assertEquals(decoded.get(2000L), new PartitionFingerprint(2, 200L));
    assertEquals(decoded.get(3000L), new PartitionFingerprint(3, 300L));
  }

  @Test
  public void testDecodeMapEmptyString() {
    assertTrue(PartitionFingerprint.decodeMap("").isEmpty());
    assertTrue(PartitionFingerprint.decodeMap(null).isEmpty());
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testDecodeMapInvalidEntry() {
    PartitionFingerprint.decodeMap("badentry");
  }

  @Test
  public void testEqualsAndHashCode() {
    PartitionFingerprint a = new PartitionFingerprint(5, 999L);
    PartitionFingerprint b = new PartitionFingerprint(5, 999L);
    PartitionFingerprint c = new PartitionFingerprint(5, 998L);
    PartitionFingerprint d = new PartitionFingerprint(4, 999L);

    assertEquals(a, b);
    assertEquals(a.hashCode(), b.hashCode());
    assertNotEquals(a, c);
    assertNotEquals(a, d);
    assertNotEquals(a, null);
  }

  @Test
  public void testToString() {
    PartitionFingerprint fp = new PartitionFingerprint(3, 42L);
    assertTrue(fp.toString().contains("segmentCount=3"));
    assertTrue(fp.toString().contains("crcChecksum=42"));
  }
}
