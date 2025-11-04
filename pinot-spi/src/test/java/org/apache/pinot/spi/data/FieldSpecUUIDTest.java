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
package org.apache.pinot.spi.data;

import org.apache.pinot.spi.utils.ByteArray;
import org.apache.pinot.spi.utils.UUIDUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


public class FieldSpecUUIDTest {

  @Test
  public void testUUIDDataTypeProperties() {
    FieldSpec.DataType uuidType = FieldSpec.DataType.UUID;

    // Verify UUID is stored as BYTES
    assertEquals(uuidType.getStoredType(), FieldSpec.DataType.BYTES);

    // Verify UUID is not numeric
    assertFalse(uuidType.isNumeric());
  }

  @Test
  public void testUUIDDimensionFieldSpec() {
    DimensionFieldSpec fieldSpec = new DimensionFieldSpec("userId", FieldSpec.DataType.UUID, true);

    assertEquals(fieldSpec.getName(), "userId");
    assertEquals(fieldSpec.getDataType(), FieldSpec.DataType.UUID);
    assertTrue(fieldSpec.isSingleValueField());

    // Test default null value
    Object defaultNullValue = fieldSpec.getDefaultNullValue();
    assertNotNull(defaultNullValue);
    assertTrue(defaultNullValue instanceof byte[]);
    assertEquals(((byte[]) defaultNullValue).length, 0);
  }

  @Test
  public void testUUIDConvertString() {
    String uuidString = "550e8400-e29b-41d4-a716-446655440000";
    Object converted = FieldSpec.DataType.UUID.convert(uuidString);

    assertNotNull(converted);
    assertTrue(converted instanceof byte[]);
    assertEquals(((byte[]) converted).length, 16);

    // Verify round-trip
    String result = FieldSpec.DataType.UUID.toString(converted);
    assertEquals(result, uuidString);
  }

  @Test
  public void testUUIDCompare() {
    String uuid1String = "550e8400-e29b-41d4-a716-446655440000";
    String uuid2String = "6ba7b810-9dad-11d1-80b4-00c04fd430c8";

    byte[] uuid1Bytes = UUIDUtils.serialize(uuid1String);
    byte[] uuid2Bytes = UUIDUtils.serialize(uuid2String);

    int result = FieldSpec.DataType.UUID.compare(uuid1Bytes, uuid2Bytes);

    // Just verify comparison works, exact value doesn't matter
    assertNotEquals(result, 0); // Different UUIDs should not be equal

    // Same UUID should compare equal
    int sameResult = FieldSpec.DataType.UUID.compare(uuid1Bytes, uuid1Bytes);
    assertEquals(sameResult, 0);
  }

  @Test
  public void testUUIDToString() {
    String uuidString = "550e8400-e29b-41d4-a716-446655440000";
    byte[] uuidBytes = UUIDUtils.serialize(uuidString);

    String result = FieldSpec.DataType.UUID.toString(uuidBytes);
    assertEquals(result, uuidString);
  }

  @Test
  public void testUUIDConvertInternal() {
    String uuidString = "550e8400-e29b-41d4-a716-446655440000";
    Object converted = FieldSpec.DataType.UUID.convertInternal(uuidString);

    assertNotNull(converted);
    assertTrue(converted instanceof ByteArray);
    assertEquals(((ByteArray) converted).length(), 16);
  }

  @Test
  public void testMultiValueUUIDField() {
    DimensionFieldSpec fieldSpec = new DimensionFieldSpec("userIds", FieldSpec.DataType.UUID, false);

    assertEquals(fieldSpec.getName(), "userIds");
    assertEquals(fieldSpec.getDataType(), FieldSpec.DataType.UUID);
    assertFalse(fieldSpec.isSingleValueField());
  }

  @Test
  public void testUUIDMetricFieldSpec() {
    // UUIDs can technically be used as metrics (though uncommon)
    MetricFieldSpec fieldSpec = new MetricFieldSpec("transactionId", FieldSpec.DataType.UUID);

    assertEquals(fieldSpec.getName(), "transactionId");
    assertEquals(fieldSpec.getDataType(), FieldSpec.DataType.UUID);
    assertTrue(fieldSpec.isSingleValueField());
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testInvalidUUIDString() {
    FieldSpec.DataType.UUID.convert("not-a-uuid");
  }

  @Test
  public void testUUIDSerialization() {
    // Create a dimension field with UUID type
    DimensionFieldSpec fieldSpec = new DimensionFieldSpec("userId", FieldSpec.DataType.UUID, true);

    // Serialize to JSON
    String json = fieldSpec.toJsonObject().toString();

    // Should contain UUID data type
    assertTrue(json.contains("UUID"));
  }

  @Test
  public void testZeroUUID() {
    String zeroUUID = "00000000-0000-0000-0000-000000000000";
    byte[] bytes = (byte[]) FieldSpec.DataType.UUID.convert(zeroUUID);

    assertEquals(bytes.length, 16);

    String result = FieldSpec.DataType.UUID.toString(bytes);
    assertEquals(result, zeroUUID);
  }

  @Test
  public void testMaxUUID() {
    String maxUUID = "ffffffff-ffff-ffff-ffff-ffffffffffff";
    byte[] bytes = (byte[]) FieldSpec.DataType.UUID.convert(maxUUID);

    assertEquals(bytes.length, 16);

    String result = FieldSpec.DataType.UUID.toString(bytes);
    assertEquals(result, maxUUID);
  }
}
