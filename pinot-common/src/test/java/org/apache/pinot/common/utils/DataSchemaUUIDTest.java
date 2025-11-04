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

import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.utils.ByteArray;
import org.apache.pinot.spi.utils.UUIDUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


public class DataSchemaUUIDTest {

  @Test
  public void testUUIDColumnDataType() {
    DataSchema.ColumnDataType uuidType = DataSchema.ColumnDataType.UUID;

    // Verify UUID is stored as BYTES
    assertEquals(uuidType.getStoredType(), DataSchema.ColumnDataType.BYTES);

    // Verify conversion to DataType
    assertEquals(uuidType.toDataType(), FieldSpec.DataType.UUID);
  }

  @Test
  public void testUUIDArrayColumnDataType() {
    DataSchema.ColumnDataType uuidArrayType = DataSchema.ColumnDataType.UUID_ARRAY;

    // Verify UUID_ARRAY is stored as BYTES_ARRAY
    assertEquals(uuidArrayType.getStoredType(), DataSchema.ColumnDataType.BYTES_ARRAY);

    // Verify conversion to DataType
    assertEquals(uuidArrayType.toDataType(), FieldSpec.DataType.UUID);
  }

  @Test
  public void testUUIDToInternal() {
    String uuidString = "550e8400-e29b-41d4-a716-446655440000";
    byte[] uuidBytes = UUIDUtils.serialize(uuidString);

    Object internal = DataSchema.ColumnDataType.UUID.toInternal(uuidBytes);

    assertNotNull(internal);
    assertTrue(internal instanceof ByteArray);
    assertEquals(((ByteArray) internal).length(), 16);
  }

  @Test
  public void testUUIDToExternal() {
    String uuidString = "550e8400-e29b-41d4-a716-446655440000";
    byte[] uuidBytes = UUIDUtils.serialize(uuidString);
    ByteArray byteArray = new ByteArray(uuidBytes);

    Object external = DataSchema.ColumnDataType.UUID.toExternal(byteArray);

    assertNotNull(external);
    assertTrue(external instanceof byte[]);
    assertEquals((byte[]) external, uuidBytes);
  }

  @Test
  public void testUUIDFormat() {
    String uuidString = "550e8400-e29b-41d4-a716-446655440000";
    byte[] uuidBytes = UUIDUtils.serialize(uuidString);

    String formatted = DataSchema.ColumnDataType.UUID.format(uuidBytes).toString();

    assertEquals(formatted, uuidString);
  }

  @Test
  public void testUUIDConvertAndFormat() {
    String uuidString = "550e8400-e29b-41d4-a716-446655440000";
    byte[] uuidBytes = UUIDUtils.serialize(uuidString);
    ByteArray byteArray = new ByteArray(uuidBytes);

    String formatted = (String) DataSchema.ColumnDataType.UUID.convertAndFormat(byteArray);

    assertEquals(formatted, uuidString);
  }

  @Test
  public void testUUIDArrayFormat() {
    String[] uuidStrings = {
        "550e8400-e29b-41d4-a716-446655440000",
        "6ba7b810-9dad-11d1-80b4-00c04fd430c8"
    };

    byte[][] uuidBytesArray = new byte[2][];
    uuidBytesArray[0] = UUIDUtils.serialize(uuidStrings[0]);
    uuidBytesArray[1] = UUIDUtils.serialize(uuidStrings[1]);

    String formatted = DataSchema.ColumnDataType.UUID_ARRAY.format(uuidBytesArray).toString();

    assertTrue(formatted.contains(uuidStrings[0]));
    assertTrue(formatted.contains(uuidStrings[1]));
  }

  @Test
  public void testFromDataTypeSV() {
    DataSchema.ColumnDataType columnDataType = DataSchema.ColumnDataType.fromDataTypeSV(FieldSpec.DataType.UUID);

    assertEquals(columnDataType, DataSchema.ColumnDataType.UUID);
  }

  @Test
  public void testFromDataTypeMV() {
    DataSchema.ColumnDataType columnDataType = DataSchema.ColumnDataType.fromDataTypeMV(FieldSpec.DataType.UUID);

    assertEquals(columnDataType, DataSchema.ColumnDataType.UUID_ARRAY);
  }

  @Test
  public void testDataSchemaWithUUID() {
    String[] columnNames = {"userId", "userName"};
    DataSchema.ColumnDataType[] columnDataTypes = {
        DataSchema.ColumnDataType.UUID,
        DataSchema.ColumnDataType.STRING
    };

    DataSchema dataSchema = new DataSchema(columnNames, columnDataTypes);

    assertEquals(dataSchema.getColumnName(0), "userId");
    assertEquals(dataSchema.getColumnDataType(0), DataSchema.ColumnDataType.UUID);
    assertEquals(dataSchema.getColumnName(1), "userName");
    assertEquals(dataSchema.getColumnDataType(1), DataSchema.ColumnDataType.STRING);
  }

  @Test
  public void testDataSchemaWithUUIDArray() {
    String[] columnNames = {"userIds", "sessionIds"};
    DataSchema.ColumnDataType[] columnDataTypes = {
        DataSchema.ColumnDataType.UUID_ARRAY,
        DataSchema.ColumnDataType.UUID_ARRAY
    };

    DataSchema dataSchema = new DataSchema(columnNames, columnDataTypes);

    assertEquals(dataSchema.getColumnName(0), "userIds");
    assertEquals(dataSchema.getColumnDataType(0), DataSchema.ColumnDataType.UUID_ARRAY);
    assertEquals(dataSchema.getColumnName(1), "sessionIds");
    assertEquals(dataSchema.getColumnDataType(1), DataSchema.ColumnDataType.UUID_ARRAY);
  }
}
