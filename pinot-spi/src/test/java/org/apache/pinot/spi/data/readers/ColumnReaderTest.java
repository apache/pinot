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

import java.io.IOException;
import java.sql.Timestamp;
import java.util.Map;
import org.apache.pinot.spi.columntransformer.ColumnTransformer;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.PinotDataType;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.expectThrows;


/// Tests the [ColumnReader] logical-type helpers and default JSON conversion contract.
public class ColumnReaderTest {

  @DataProvider(name = "jsonValues")
  public Object[][] jsonValues() {
    return new Object[][]{
        {Map.of("key", "value"), "{\"key\":\"value\"}"},
        {"{\"key\":\"value\"}", "{\"key\":\"value\"}"},
        {null, "null"},
        {Map.of(), null}
    };
  }

  @Test(dataProvider = "jsonValues")
  public void testGetJsonValue(Object rawValue, Object jsonValue)
      throws IOException {
    ColumnReader reader = mock(ColumnReader.class, CALLS_REAL_METHODS);
    ColumnTransformer transformer = mock(ColumnTransformer.class);
    when(reader.getValue(42)).thenReturn(rawValue);
    when(transformer.transform(rawValue)).thenReturn(jsonValue);

    assertEquals(reader.getJsonValue(42, transformer), jsonValue);
    verify(reader).getValue(42);
    verify(transformer).transform(same(rawValue));
  }

  @Test
  public void testGetJsonValuePropagatesReadFailure()
      throws IOException {
    ColumnReader reader = mock(ColumnReader.class, CALLS_REAL_METHODS);
    ColumnTransformer transformer = mock(ColumnTransformer.class);
    IOException failure = new IOException("read failed");
    when(reader.getValue(42)).thenThrow(failure);

    assertSame(expectThrows(IOException.class, () -> reader.getJsonValue(42, transformer)), failure);
    verifyNoInteractions(transformer);
  }

  @Test
  public void testGetJsonValuePropagatesConversionFailure()
      throws IOException {
    ColumnReader reader = mock(ColumnReader.class, CALLS_REAL_METHODS);
    ColumnTransformer transformer = mock(ColumnTransformer.class);
    when(reader.getValue(42)).thenReturn("invalid JSON");
    IllegalArgumentException failure = new IllegalArgumentException("conversion failed");
    when(transformer.transform("invalid JSON")).thenThrow(failure);

    assertSame(expectThrows(IllegalArgumentException.class, () -> reader.getJsonValue(42, transformer)), failure);
  }

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
