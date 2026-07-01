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
package org.apache.pinot.common.function.scalar.uuid;

import java.util.UUID;
import org.apache.pinot.spi.utils.UuidUtils;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


public class UuidConversionFunctionsTest {
  private static final String UUID_VALUE = "550e8400-e29b-41d4-a716-446655440000";
  private static final String MIXED_CASE_UUID_VALUE = "550E8400-E29B-41D4-A716-446655440000";

  @DataProvider(name = "invalidUuidStrings")
  public Object[][] invalidUuidStrings() {
    return new Object[][]{
        {"550e8400-e29b-41d4-a716-44665544000"},
        {"550e8400-e29b-41d4-a716-4466554400000"},
        {"550e8400-e29b-41d4-a716-44665544000g"},
        {"550e8400e29b41d4a716446655440000"},
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
  public void testToUuidFromStringNormalizesToLowerCase() {
    assertEquals(ToUuidScalarFunction.toUuid(MIXED_CASE_UUID_VALUE), UUID.fromString(UUID_VALUE));
  }

  @Test
  public void testToUuidFromBytes() {
    byte[] bytes = UuidUtils.toBytes(UUID_VALUE);

    assertEquals(ToUuidScalarFunction.toUuid(bytes), UUID.fromString(UUID_VALUE));
    assertEquals(UuidConversionFunctions.bytesToUuid(bytes), UUID.fromString(UUID_VALUE));
  }

  @Test
  public void testUuidToBytesAndString() {
    UUID uuid = UUID.fromString(UUID_VALUE);

    assertEquals(UuidConversionFunctions.uuidToBytes(uuid), UuidUtils.toBytes(UUID_VALUE));
    assertEquals(UuidConversionFunctions.uuidToString(uuid), UUID_VALUE);
  }

  @Test
  public void testIsUuid() {
    assertTrue(IsUuidScalarFunction.isUuid(UUID_VALUE));
    assertTrue(IsUuidScalarFunction.isUuid(UuidUtils.toBytes(UUID_VALUE)));
    assertFalse(IsUuidScalarFunction.isUuid("not-a-uuid"));
    assertFalse(IsUuidScalarFunction.isUuid(new byte[15]));
  }

  @Test(dataProvider = "invalidUuidStrings")
  public void testToUuidRejectsInvalidString(String invalidUuid) {
    Assert.expectThrows(IllegalArgumentException.class, () -> ToUuidScalarFunction.toUuid(invalidUuid));
    assertFalse(IsUuidScalarFunction.isUuid(invalidUuid));
  }

  @Test(dataProvider = "invalidUuidBytes")
  public void testBytesToUuidRejectsInvalidBytes(byte[] invalidBytes) {
    Assert.expectThrows(IllegalArgumentException.class, () -> ToUuidScalarFunction.toUuid(invalidBytes));
    Assert.expectThrows(IllegalArgumentException.class, () -> UuidConversionFunctions.bytesToUuid(invalidBytes));
    assertFalse(IsUuidScalarFunction.isUuid(invalidBytes));
  }

  @Test
  public void testNullInputs() {
    assertNull(ToUuidScalarFunction.toUuid((String) null));
    assertNull(ToUuidScalarFunction.toUuid((byte[]) null));
    assertNull(UuidConversionFunctions.uuidToBytes(null));
    assertNull(UuidConversionFunctions.bytesToUuid(null));
    assertNull(UuidConversionFunctions.uuidToString(null));
    assertNull(UuidConversionFunctions.uuidVersion(null));
    assertNull(UuidConversionFunctions.uuidTimestamp(null));
    assertFalse(IsUuidScalarFunction.isUuid((String) null));
    assertFalse(IsUuidScalarFunction.isUuid((byte[]) null));
  }

  @Test
  public void testUuidV4ProducesValidVersion4Uuid() {
    UUID uuid = UuidConversionFunctions.uuidV4();
    assertEquals(uuid.version(), 4, "uuidV4 must produce version 4");
    assertEquals(UuidConversionFunctions.uuidVersion(uuid).intValue(), 4);

    // Two successive calls should not return the same value (probability of collision is ~0).
    UUID other = UuidConversionFunctions.uuidV4();
    assertFalse(uuid.equals(other), "Successive uuidV4 calls collided");
  }

  @Test
  public void testUuidV7EncodesCurrentUnixMillisAndIsKSortable() {
    long before = System.currentTimeMillis();
    UUID first = UuidConversionFunctions.uuidV7();
    UUID second = UuidConversionFunctions.uuidV7();
    long after = System.currentTimeMillis();

    assertEquals(first.version(), 7, "uuidV7 must produce version 7");
    assertEquals(UuidConversionFunctions.uuidVersion(first).intValue(), 7);

    long firstTs = UuidConversionFunctions.uuidTimestamp(first);
    long secondTs = UuidConversionFunctions.uuidTimestamp(second);

    assertTrue(firstTs >= before && firstTs <= after,
        "uuidV7 timestamp " + firstTs + " not within [" + before + ", " + after + "]");
    assertTrue(secondTs >= firstTs, "uuidV7 timestamps must be monotonically non-decreasing across calls");

    // Variant must be RFC 4122 (top two bits of LSB = 10).
    assertEquals(first.variant(), 2, "uuidV7 must use RFC 4122 variant");
  }

  @Test
  public void testUuidVersionForKnownVersions() {
    // Version 4 — random.
    assertEquals(UuidConversionFunctions.uuidVersion(UUID.fromString("550e8400-e29b-41d4-a716-446655440000"))
        .intValue(), 4);
    // Version 1 — Gregorian time-based.
    assertEquals(UuidConversionFunctions.uuidVersion(UUID.fromString("c232ab00-9414-11ec-b3c8-9e6bdeced846"))
        .intValue(), 1);
    // Version 7 — Unix-time-based.
    assertEquals(UuidConversionFunctions.uuidVersion(UUID.fromString("017f22e2-79b0-7cc3-98c4-dc0c0c07398f"))
        .intValue(), 7);
  }

  @Test
  public void testUuidTimestampRejectsNonTimeBasedVersions() {
    UUID v4 = UUID.fromString("550e8400-e29b-41d4-a716-446655440000");
    Assert.expectThrows(IllegalArgumentException.class, () -> UuidConversionFunctions.uuidTimestamp(v4));
  }

  @Test
  public void testUuidTimestampDecodesV7FixedSample() {
    // Construct a v7 UUID with known unix ms = 0x017F22E279B0L = 1645557742000 (2022-02-22T19:22:22Z).
    long unixMs = 0x017F22E279B0L;
    long msb = (unixMs << 16) | 0x7000L | 0x0CC3L;
    long lsb = 0x8000000000000000L | 0x18C4DC0C0C07398FL;
    UUID v7 = new UUID(msb, lsb);
    assertEquals(UuidConversionFunctions.uuidTimestamp(v7).longValue(), unixMs);
    assertEquals(UuidConversionFunctions.uuidVersion(v7).intValue(), 7);
  }
}
