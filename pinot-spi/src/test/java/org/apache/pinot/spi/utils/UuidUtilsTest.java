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
package org.apache.pinot.spi.utils;

import java.util.Arrays;
import java.util.UUID;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


/**
 * Tests for {@link UuidUtils}.
 */
public class UuidUtilsTest {
  private static final String UUID_VALUE = "550e8400-e29b-41d4-a716-446655440000";

  @DataProvider(name = "invalidUuidStrings")
  public Object[][] invalidUuidStrings() {
    return new Object[][]{
        {"550e8400-e29b-41d4-a716-44665544000"},
        {"550e8400-e29b-41d4-a716-4466554400000"},
        {"550e8400-e29b-41d4-a716-44665544000g"},
        {""}
    };
  }

  @DataProvider(name = "invalidUuidBytes")
  public Object[][] invalidUuidBytes() {
    return new Object[][]{
        {new byte[15]},
        {new byte[17]}
    };
  }

  @Test
  public void testMixedCaseUuidStringRoundTrips() {
    String mixedCaseUuid = "550E8400-E29B-41D4-A716-446655440000";
    byte[] bytes = UuidUtils.toBytes(mixedCaseUuid);

    assertEquals(UuidUtils.toString(bytes), UUID_VALUE);
    assertEquals(UuidUtils.toUUID(mixedCaseUuid), UUID.fromString(UUID_VALUE));
  }

  @Test
  public void testCharSequenceRoundTrips() {
    CharSequence mixedCaseUuid = new StringBuilder("550E8400-E29B-41D4-A716-446655440000");

    assertEquals(UuidUtils.toBytes(mixedCaseUuid), UuidUtils.toBytes(UUID_VALUE));
    assertEquals(UuidUtils.toUUID(mixedCaseUuid), UUID.fromString(UUID_VALUE));
    assertTrue(UuidUtils.isUuid(mixedCaseUuid));
  }

  @Test
  public void testDashlessHexStringRoundTrips() {
    String hexUuid = "550e8400e29b41d4a716446655440000";

    assertEquals(UuidUtils.toBytes(hexUuid), UuidUtils.toBytes(UUID_VALUE));
    assertEquals(UuidUtils.toString(UuidUtils.toBytes(hexUuid)), UUID_VALUE);
    assertEquals(UuidUtils.toUUID(hexUuid), UUID.fromString(UUID_VALUE));
    assertTrue(UuidUtils.isUuid(hexUuid));
  }

  @Test(dataProvider = "invalidUuidStrings")
  public void testRejectsInvalidUuidStrings(String invalidUuid) {
    Assert.expectThrows(IllegalArgumentException.class, () -> UuidUtils.toBytes(invalidUuid));
    Assert.expectThrows(IllegalArgumentException.class, () -> UuidUtils.toUUID(invalidUuid));
    assertFalse(UuidUtils.isUuid(invalidUuid));
  }

  @Test(dataProvider = "invalidUuidBytes")
  public void testRejectsInvalidUuidBytes(byte[] invalidBytes) {
    Assert.expectThrows(IllegalArgumentException.class, () -> UuidUtils.toBytes(invalidBytes));
    Assert.expectThrows(IllegalArgumentException.class, () -> UuidUtils.toUUID(invalidBytes));
    assertFalse(UuidUtils.isUuid(invalidBytes));
  }

  @Test
  public void testIsUuidAcceptsValidStringAndBytes() {
    byte[] uuidBytes = UuidUtils.toBytes(UUID_VALUE);

    assertTrue(UuidUtils.isUuid(UUID_VALUE));
    assertTrue(UuidUtils.isUuid(uuidBytes));
    assertTrue(UuidUtils.isUuid(new ByteArray(uuidBytes)));
  }

  @Test
  public void testIsUuidRejectsNull() {
    assertFalse(UuidUtils.isUuid((String) null));
    assertFalse(UuidUtils.isUuid((byte[]) null));
    assertFalse(UuidUtils.isUuid((Object) null));
  }

  @Test
  public void testRandomV4HasVersionFourAndNoTimestamp() {
    UUID uuid = UuidUtils.randomV4();
    assertEquals(UuidUtils.getVersion(uuid), 4);
    assertEquals(UuidUtils.getVersion(UuidUtils.toBytes(uuid)), 4);
    // v4 is not time-based, so timestamp extraction must fail.
    Assert.expectThrows(IllegalArgumentException.class, () -> UuidUtils.getTimestampMillis(uuid));
  }

  @Test
  public void testRandomV7HasVersionSevenAndCurrentTimestamp() {
    long before = System.currentTimeMillis();
    UUID uuid = UuidUtils.randomV7();
    long after = System.currentTimeMillis();

    assertEquals(UuidUtils.getVersion(uuid), 7);
    assertEquals(UuidUtils.getVersion(UuidUtils.toBytes(uuid)), 7);
    long timestamp = UuidUtils.getTimestampMillis(uuid);
    assertTrue(timestamp >= before && timestamp <= after,
        "v7 timestamp " + timestamp + " not within [" + before + ", " + after + "]");
    // Byte-array and UUID overloads must agree.
    assertEquals(UuidUtils.getTimestampMillis(UuidUtils.toBytes(uuid)), timestamp);
  }

  @Test
  public void testGetTimestampMillisMatchesKnownVersionOneAndSevenLayouts() {
    // Fixed v7 UUID whose leading 48 bits encode 0x0000018F00000000 ms.
    UUID v7 = new UUID(0x018F00000000_7000L | 0x0123L, 0x8000000000000000L);
    assertEquals(UuidUtils.getVersion(v7), 7);
    assertEquals(UuidUtils.getTimestampMillis(v7), 0x018F00000000L);
  }

  @Test
  public void testNullUuidBytesReturnsDefensiveCopy() {
    byte[] first = UuidUtils.nullUuidBytes();
    byte[] second = UuidUtils.nullUuidBytes();

    first[0] = 1;
    assertEquals(second, new byte[UuidUtils.UUID_NUM_BYTES]);
  }

  @Test
  public void testComparePreservesUnsignedByteOrdering() {
    byte[] larger = UuidUtils.toBytes("80000000-0000-0000-0000-000000000000");
    byte[] smaller = UuidUtils.toBytes("7fffffff-ffff-ffff-ffff-ffffffffffff");

    assertTrue(ByteArray.compare(larger, smaller) > 0);
    assertEquals(UuidUtils.compare(larger, smaller), ByteArray.compare(larger, smaller));
    assertEquals(UuidUtils.compare(smaller, larger), ByteArray.compare(smaller, larger));
  }

  @Test
  public void testUuidEqualityHashAndBitExtraction() {
    UUID uuid = UUID.fromString(UUID_VALUE);
    byte[] uuidBytes = UuidUtils.toBytes(uuid);

    assertEquals(UuidUtils.getMostSignificantBits(uuidBytes), uuid.getMostSignificantBits());
    assertEquals(UuidUtils.getLeastSignificantBits(uuidBytes), uuid.getLeastSignificantBits());
    assertTrue(UuidUtils.equals(uuidBytes, Arrays.copyOf(uuidBytes, uuidBytes.length)));
    assertTrue(UuidUtils.equals(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits(),
        uuid.getMostSignificantBits(), uuid.getLeastSignificantBits()));
    assertEquals(UuidUtils.hashCode(uuidBytes), new ByteArray(uuidBytes).hashCode());
    assertEquals(UuidUtils.hashCode(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits()),
        new ByteArray(uuidBytes).hashCode());
  }

  @Test
  public void testUuidKeyNormalizesRepresentations() {
    UUID uuid = UUID.fromString(UUID_VALUE);
    byte[] uuidBytes = UuidUtils.toBytes(uuid);
    UuidKey uuidKeyFromBytes = UuidKey.fromBytes(uuidBytes);
    UuidKey uuidKeyFromObject = UuidKey.fromObject(new ByteArray(uuidBytes));

    assertEquals(uuidKeyFromBytes, UuidKey.fromUUID(uuid));
    assertEquals(uuidKeyFromBytes, uuidKeyFromObject);
    assertEquals(uuidKeyFromBytes.hashCode(), UuidUtils.hashCode(uuidBytes));
    assertEquals(uuidKeyFromBytes.toByteArray(), new ByteArray(uuidBytes));
    assertEquals(uuidKeyFromBytes.toString(), UUID_VALUE);
  }

  @Test
  public void testLongPairHelpersRoundTrip() {
    UUID uuid = UUID.fromString(UUID_VALUE);
    long mostSignificantBits = uuid.getMostSignificantBits();
    long leastSignificantBits = uuid.getLeastSignificantBits();

    assertEquals(UuidUtils.toBytes(mostSignificantBits, leastSignificantBits), UuidUtils.toBytes(uuid));
    assertEquals(UuidUtils.toUUID(mostSignificantBits, leastSignificantBits), uuid);
    assertEquals(UuidUtils.toString(mostSignificantBits, leastSignificantBits), UUID_VALUE);
    assertEquals(UuidKey.fromLongs(mostSignificantBits, leastSignificantBits), UuidKey.fromUUID(uuid));
  }
}
