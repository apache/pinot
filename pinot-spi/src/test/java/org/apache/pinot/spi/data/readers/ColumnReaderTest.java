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
package org.apache.pinot.spi.data.readers;

import java.sql.Timestamp;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.PinotDataType;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;


/// Tests for the [ColumnReader] static helpers: [ColumnReader#toValueType(DataType, boolean)] maps a logical
/// [DataType] to the value type a reader reports, and [ColumnReader#toLogicalValue(Object, DataType)] converts a
/// physically-stored value to the logical object [ColumnReader#getValue(int)] returns.
public class ColumnReaderTest {

  @Test
  public void testToValueType() {
    assertEquals(ColumnReader.toValueType(DataType.INT, true), PinotDataType.INT);
    assertEquals(ColumnReader.toValueType(DataType.INT, false), PinotDataType.INT_ARRAY);
    assertEquals(ColumnReader.toValueType(DataType.LONG, true), PinotDataType.LONG);
    assertEquals(ColumnReader.toValueType(DataType.FLOAT, true), PinotDataType.FLOAT);
    assertEquals(ColumnReader.toValueType(DataType.DOUBLE, true), PinotDataType.DOUBLE);
    assertEquals(ColumnReader.toValueType(DataType.BIG_DECIMAL, true), PinotDataType.BIG_DECIMAL);
    assertEquals(ColumnReader.toValueType(DataType.STRING, true), PinotDataType.STRING);
    assertEquals(ColumnReader.toValueType(DataType.BYTES, true), PinotDataType.BYTES);
    assertEquals(ColumnReader.toValueType(DataType.BYTES, false), PinotDataType.BYTES_ARRAY);
    // Logical types with no dedicated accessor are read through getValue().
    assertNull(ColumnReader.toValueType(DataType.BOOLEAN, true));
    assertNull(ColumnReader.toValueType(DataType.TIMESTAMP, true));
    assertNull(ColumnReader.toValueType(DataType.MAP, true));
    // JSON is stored and read as its text String.
    assertEquals(ColumnReader.toValueType(DataType.JSON, true), PinotDataType.STRING);
  }

  @Test
  public void testToLogicalValueScalar() {
    // BOOLEAN is stored as int 0/1 but surfaces as Boolean, so it stringifies as "true"/"false", not "1"/"0".
    assertEquals(ColumnReader.toLogicalValue(1, DataType.BOOLEAN), Boolean.TRUE);
    assertEquals(ColumnReader.toLogicalValue(0, DataType.BOOLEAN), Boolean.FALSE);
    // TIMESTAMP is stored as an epoch-millis long but surfaces as Timestamp.
    Object timestamp = ColumnReader.toLogicalValue(1000L, DataType.TIMESTAMP);
    assertEquals(timestamp, new Timestamp(1000L));
    assertEquals(String.valueOf(timestamp), new Timestamp(1000L).toString());
    // Types stored as their logical type pass through unchanged (JSON is read as its text String).
    assertEquals(ColumnReader.toLogicalValue(42, DataType.INT), 42);
    assertEquals(ColumnReader.toLogicalValue("foo", DataType.STRING), "foo");
    assertEquals(ColumnReader.toLogicalValue("{\"a\":1}", DataType.JSON), "{\"a\":1}");
    assertNull(ColumnReader.toLogicalValue(null, DataType.BOOLEAN));
  }

  @Test
  public void testToLogicalValueMultiValue() {
    assertEquals((Boolean[]) ColumnReader.toLogicalValue(new Object[]{1, 0, 1}, DataType.BOOLEAN),
        new Boolean[]{true, false, true});
    assertEquals((Timestamp[]) ColumnReader.toLogicalValue(new Object[]{1000L, 2000L}, DataType.TIMESTAMP),
        new Timestamp[]{new Timestamp(1000L), new Timestamp(2000L)});
  }
}
