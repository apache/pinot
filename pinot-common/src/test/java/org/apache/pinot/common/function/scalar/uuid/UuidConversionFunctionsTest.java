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
    assertFalse(IsUuidScalarFunction.isUuid((String) null));
    assertFalse(IsUuidScalarFunction.isUuid((byte[]) null));
  }
}
