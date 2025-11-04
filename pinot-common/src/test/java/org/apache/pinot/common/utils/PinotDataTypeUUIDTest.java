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
package org.apache.pinot.common.utils;

import org.apache.pinot.spi.utils.UUIDUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


public class PinotDataTypeUUIDTest {

  @Test
  public void testUUIDToString() {
    String uuidString = "550e8400-e29b-41d4-a716-446655440000";
    byte[] uuidBytes = UUIDUtils.serialize(uuidString);

    String result = PinotDataType.UUID.toString(uuidBytes);
    assertEquals(result, uuidString);
  }

  @Test
  public void testUUIDToBytes() {
    String uuidString = "550e8400-e29b-41d4-a716-446655440000";
    byte[] uuidBytes = UUIDUtils.serialize(uuidString);

    byte[] result = (byte[]) PinotDataType.UUID.toBytes(uuidBytes);
    assertEquals(result, uuidBytes);
  }

  @Test
  public void testConvertStringToUUID() {
    String uuidString = "550e8400-e29b-41d4-a716-446655440000";

    Object result = PinotDataType.UUID.convert(uuidString, PinotDataType.STRING);

    assertNotNull(result);
    assertTrue(result instanceof byte[]);
    assertEquals(((byte[]) result).length, 16);

    // Verify round-trip
    String convertedBack = PinotDataType.UUID.toString(result);
    assertEquals(convertedBack, uuidString);
  }

  @Test
  public void testConvertUUIDToString() {
    String uuidString = "550e8400-e29b-41d4-a716-446655440000";
    byte[] uuidBytes = UUIDUtils.serialize(uuidString);

    Object result = PinotDataType.STRING.convert(uuidBytes, PinotDataType.UUID);

    assertNotNull(result);
    assertTrue(result instanceof String);
    assertEquals(result, uuidString);
  }

  @Test(expectedExceptions = UnsupportedOperationException.class)
  public void testUUIDToInt() {
    byte[] uuidBytes = UUIDUtils.serialize("550e8400-e29b-41d4-a716-446655440000");
    PinotDataType.UUID.toInt(uuidBytes);
  }

  @Test(expectedExceptions = UnsupportedOperationException.class)
  public void testUUIDToLong() {
    byte[] uuidBytes = UUIDUtils.serialize("550e8400-e29b-41d4-a716-446655440000");
    PinotDataType.UUID.toLong(uuidBytes);
  }

  @Test(expectedExceptions = UnsupportedOperationException.class)
  public void testUUIDToFloat() {
    byte[] uuidBytes = UUIDUtils.serialize("550e8400-e29b-41d4-a716-446655440000");
    PinotDataType.UUID.toFloat(uuidBytes);
  }

  @Test(expectedExceptions = UnsupportedOperationException.class)
  public void testUUIDToDouble() {
    byte[] uuidBytes = UUIDUtils.serialize("550e8400-e29b-41d4-a716-446655440000");
    PinotDataType.UUID.toDouble(uuidBytes);
  }

  @Test(expectedExceptions = UnsupportedOperationException.class)
  public void testUUIDToBigDecimal() {
    byte[] uuidBytes = UUIDUtils.serialize("550e8400-e29b-41d4-a716-446655440000");
    PinotDataType.UUID.toBigDecimal(uuidBytes);
  }

  @Test(expectedExceptions = UnsupportedOperationException.class)
  public void testUUIDToBoolean() {
    byte[] uuidBytes = UUIDUtils.serialize("550e8400-e29b-41d4-a716-446655440000");
    PinotDataType.UUID.toBoolean(uuidBytes);
  }

  @Test(expectedExceptions = UnsupportedOperationException.class)
  public void testUUIDToTimestamp() {
    byte[] uuidBytes = UUIDUtils.serialize("550e8400-e29b-41d4-a716-446655440000");
    PinotDataType.UUID.toTimestamp(uuidBytes);
  }

  @Test
  public void testUUIDArrayConversion() {
    String[] uuidStrings = {
        "550e8400-e29b-41d4-a716-446655440000",
        "6ba7b810-9dad-11d1-80b4-00c04fd430c8"
    };

    Object result = PinotDataType.UUID_ARRAY.convert(uuidStrings, PinotDataType.STRING_ARRAY);

    assertNotNull(result);
    assertTrue(result instanceof byte[][]);
    byte[][] uuidBytesArray = (byte[][]) result;
    assertEquals(uuidBytesArray.length, 2);
    assertEquals(uuidBytesArray[0].length, 16);
    assertEquals(uuidBytesArray[1].length, 16);
  }

  @Test
  public void testGetPinotDataTypeForExecution() {
    DataSchema.ColumnDataType columnDataType = DataSchema.ColumnDataType.UUID;

    PinotDataType dataType = PinotDataType.getPinotDataTypeForExecution(columnDataType);

    assertEquals(dataType, PinotDataType.UUID);
  }

  @Test
  public void testGetPinotDataTypeForExecutionArray() {
    DataSchema.ColumnDataType columnDataType = DataSchema.ColumnDataType.UUID_ARRAY;

    PinotDataType dataType = PinotDataType.getPinotDataTypeForExecution(columnDataType);

    assertEquals(dataType, PinotDataType.UUID_ARRAY);
  }

  @Test
  public void testMultipleUUIDConversions() {
    String[] uuidStrings = {
        "550e8400-e29b-41d4-a716-446655440000",
        "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
        "00000000-0000-0000-0000-000000000001",
        "ffffffff-ffff-ffff-ffff-fffffffffffe"
    };

    for (String uuidString : uuidStrings) {
      Object converted = PinotDataType.UUID.convert(uuidString, PinotDataType.STRING);
      String convertedBack = PinotDataType.UUID.toString(converted);
      assertEquals(convertedBack, uuidString);
    }
  }
}
