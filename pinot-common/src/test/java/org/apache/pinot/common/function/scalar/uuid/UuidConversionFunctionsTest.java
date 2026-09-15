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
import org.apache.pinot.common.evaluator.InbuiltFunctionEvaluator;
import org.apache.pinot.common.function.PinotScalarFunction;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.UuidUtils;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
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
        // NOTE: the dash-less 32-hex form ("550e8400e29b41d4a716446655440000") is accepted since #18927 added a
        // hex-bytes fallback to UuidUtils.toBytes(String), so it is intentionally not listed as invalid.
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
  public void testSemanticFunctionsAcceptStringBytesAndUuid() {
    UUID uuid = UUID.fromString(UUID_VALUE);
    byte[] bytes = UuidUtils.toBytes(uuid);

    assertEquals(ToUuidScalarFunction.toUuid(uuid), uuid);
    assertEquals(UuidToStringScalarFunction.uuidToString(MIXED_CASE_UUID_VALUE), UUID_VALUE);
    assertEquals(UuidToStringScalarFunction.uuidToString(bytes), UUID_VALUE);
    assertEquals(UuidToStringScalarFunction.uuidToString(uuid), UUID_VALUE);
    assertEquals(UuidToBytesScalarFunction.uuidToBytes(UUID_VALUE), bytes);
    assertEquals(UuidToBytesScalarFunction.uuidToBytes(bytes), bytes);
    assertSame(UuidToBytesScalarFunction.uuidToBytes(bytes), bytes);
    assertEquals(UuidToBytesScalarFunction.uuidToBytes(uuid), bytes);
    assertEquals(UuidVersionScalarFunction.uuidVersion(UUID_VALUE).intValue(), 4);
    assertEquals(UuidVersionScalarFunction.uuidVersion(bytes).intValue(), 4);
    assertEquals(UuidVersionScalarFunction.uuidVersion(uuid).intValue(), 4);
    assertTrue(IsUuidScalarFunction.isUuid(uuid));
  }

  @Test
  public void testUuidToStringIngestionEvaluationForAllInputTypes() {
    GenericRow row = new GenericRow();
    row.putValue("uuidString", MIXED_CASE_UUID_VALUE);
    row.putValue("uuidBytes", UuidUtils.toBytes(UUID_VALUE));
    row.putValue("uuid", UUID.fromString(UUID_VALUE));

    assertEquals(new InbuiltFunctionEvaluator("UUID_TO_STRING(uuidString)").evaluate(row), UUID_VALUE);
    assertEquals(new InbuiltFunctionEvaluator("UUID_TO_STRING(uuidBytes)").evaluate(row), UUID_VALUE);
    assertEquals(new InbuiltFunctionEvaluator("UUID_TO_STRING(uuid)").evaluate(row), UUID_VALUE);
    assertEquals(new InbuiltFunctionEvaluator("UUID_VERSION(uuidString)").evaluate(row), 4);
    assertEquals(new InbuiltFunctionEvaluator("UUID_VERSION(uuidBytes)").evaluate(row), 4);
    assertEquals(new InbuiltFunctionEvaluator("UUID_VERSION(uuid)").evaluate(row), 4);
    assertEquals(new InbuiltFunctionEvaluator("IS_UUID(uuidString)").evaluate(row), true);
    assertEquals(new InbuiltFunctionEvaluator("IS_UUID(uuidBytes)").evaluate(row), true);
    assertEquals(new InbuiltFunctionEvaluator("IS_UUID(uuid)").evaluate(row), true);
  }

  @Test
  public void testIngestionEvaluationPropagatesNull() {
    GenericRow row = new GenericRow();
    row.putValue("value", null);

    assertNull(new InbuiltFunctionEvaluator("IS_UUID(value)").evaluate(row));
    assertNull(new InbuiltFunctionEvaluator("TO_UUID(value)").evaluate(row));
    assertNull(new InbuiltFunctionEvaluator("UUID_TO_STRING(value)").evaluate(row));
    assertNull(new InbuiltFunctionEvaluator("UUID_TO_BYTES(value)").evaluate(row));
    assertNull(new InbuiltFunctionEvaluator("UUID_VERSION(value)").evaluate(row));
    assertNull(new InbuiltFunctionEvaluator("UUID_TIMESTAMP(value)").evaluate(row));
    assertNull(new InbuiltFunctionEvaluator("BYTES_TO_UUID(value)").evaluate(row));
  }

  @Test
  public void testPolymorphicFunctionInfoDispatch() {
    PinotScalarFunction[] functions = {
        new IsUuidScalarFunction(), new ToUuidScalarFunction(), new UuidToStringScalarFunction(),
        new UuidToBytesScalarFunction(), new UuidVersionScalarFunction(), new UuidTimestampScalarFunction()
    };
    for (PinotScalarFunction function : functions) {
      assertEquals(function.getFunctionInfo(new ColumnDataType[]{ColumnDataType.STRING}).getMethod()
          .getParameterTypes()[0], String.class);
      assertEquals(function.getFunctionInfo(new ColumnDataType[]{ColumnDataType.BYTES}).getMethod()
          .getParameterTypes()[0], byte[].class);
      assertEquals(function.getFunctionInfo(new ColumnDataType[]{ColumnDataType.UUID}).getMethod()
          .getParameterTypes()[0], byte[].class);
    }
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
    assertNull(ToUuidScalarFunction.toUuid((UUID) null));
    assertNull(UuidToBytesScalarFunction.uuidToBytes((String) null));
    assertNull(UuidToBytesScalarFunction.uuidToBytes((byte[]) null));
    assertNull(UuidToBytesScalarFunction.uuidToBytes((UUID) null));
    assertNull(UuidConversionFunctions.bytesToUuid(null));
    assertNull(UuidToStringScalarFunction.uuidToString((String) null));
    assertNull(UuidToStringScalarFunction.uuidToString((byte[]) null));
    assertNull(UuidToStringScalarFunction.uuidToString((UUID) null));
    assertNull(UuidVersionScalarFunction.uuidVersion((String) null));
    assertNull(UuidVersionScalarFunction.uuidVersion((byte[]) null));
    assertNull(UuidVersionScalarFunction.uuidVersion((UUID) null));
    assertNull(UuidTimestampScalarFunction.uuidTimestamp((String) null));
    assertNull(UuidTimestampScalarFunction.uuidTimestamp((byte[]) null));
    assertNull(UuidTimestampScalarFunction.uuidTimestamp((UUID) null));
    assertFalse(IsUuidScalarFunction.isUuid((String) null));
    assertFalse(IsUuidScalarFunction.isUuid((byte[]) null));
    assertFalse(IsUuidScalarFunction.isUuid((UUID) null));
  }

  @Test
  public void testUuidV4ProducesValidVersion4Uuid() {
    UUID uuid = UuidConversionFunctions.uuidV4();
    assertEquals(uuid.version(), 4, "uuidV4 must produce version 4");
    assertEquals(UuidVersionScalarFunction.uuidVersion(uuid).intValue(), 4);

    // Two successive calls should not return the same value (probability of collision is ~0).
    UUID other = UuidConversionFunctions.uuidV4();
    assertFalse(uuid.equals(other), "Successive uuidV4 calls collided");
  }

  @Test
  public void testUuidV7ProducesValidVersion7Uuid() {
    UUID uuid = UuidConversionFunctions.uuidV7();
    assertEquals(uuid.version(), 7, "uuidV7 must produce version 7");
    assertEquals(UuidVersionScalarFunction.uuidVersion(uuid).intValue(), 7);
    // Variant must be RFC 4122 (top two bits of LSB = 10).
    assertEquals(uuid.variant(), 2, "uuidV7 must use RFC 4122 variant");
  }

  @Test
  public void testUuidVersionForKnownVersions() {
    // Version 4 — random.
    assertEquals(UuidVersionScalarFunction.uuidVersion(UUID.fromString("550e8400-e29b-41d4-a716-446655440000"))
        .intValue(), 4);
    // Version 1 — Gregorian time-based.
    assertEquals(UuidVersionScalarFunction.uuidVersion(UUID.fromString("c232ab00-9414-11ec-b3c8-9e6bdeced846"))
        .intValue(), 1);
    // Version 7 — Unix-time-based.
    assertEquals(UuidVersionScalarFunction.uuidVersion(UUID.fromString("017f22e2-79b0-7cc3-98c4-dc0c0c07398f"))
        .intValue(), 7);
  }

  @Test
  public void testUuidTimestampRejectsNonTimeBasedVersions() {
    UUID v4 = UUID.fromString("550e8400-e29b-41d4-a716-446655440000");
    Assert.expectThrows(IllegalArgumentException.class, () -> UuidTimestampScalarFunction.uuidTimestamp(v4));
  }

  @Test
  public void testUuidTimestampDecodesV7FixedSample() {
    // Construct a v7 UUID with known unix ms = 0x017F22E279B0L = 1645557742000 (2022-02-22T19:22:22Z).
    long unixMs = 0x017F22E279B0L;
    long msb = (unixMs << 16) | 0x7000L | 0x0CC3L;
    long lsb = 0x8000000000000000L | 0x18C4DC0C0C07398FL;
    UUID v7 = new UUID(msb, lsb);
    assertEquals(UuidTimestampScalarFunction.uuidTimestamp(v7).longValue(), unixMs);
    assertEquals(UuidTimestampScalarFunction.uuidTimestamp(UuidUtils.toBytes(v7)).longValue(), unixMs);
    assertEquals(UuidTimestampScalarFunction.uuidTimestamp(v7.toString()).longValue(), unixMs);
    assertEquals(UuidVersionScalarFunction.uuidVersion(v7).intValue(), 7);
  }
}
