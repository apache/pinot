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
package org.apache.pinot.core.segment.processing.genericrow;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class GenericRowSerDeTest {
  private List<FieldSpec> _fieldSpecs;
  private GenericRow _row;

  @BeforeClass
  public void setUp() {
    _fieldSpecs = Arrays.asList(new DimensionFieldSpec("intSV", DataType.INT, true),
        new DimensionFieldSpec("longSV", DataType.LONG, true), new DimensionFieldSpec("floatSV", DataType.FLOAT, true),
        new DimensionFieldSpec("doubleSV", DataType.DOUBLE, true),
        new DimensionFieldSpec("stringSV", DataType.STRING, true),
        new DimensionFieldSpec("bytesSV", DataType.BYTES, true), new DimensionFieldSpec("nullSV", DataType.INT, true),
        new DimensionFieldSpec("intMV", DataType.INT, false), new DimensionFieldSpec("longMV", DataType.LONG, false),
        new DimensionFieldSpec("floatMV", DataType.FLOAT, false),
        new DimensionFieldSpec("doubleMV", DataType.DOUBLE, false),
        new DimensionFieldSpec("stringMV", DataType.STRING, false),
        new DimensionFieldSpec("nullMV", DataType.LONG, false));

    _row = new GenericRow();
    _row.putValue("intSV", 123);
    _row.putValue("longSV", 123L);
    _row.putValue("floatSV", 123.0f);
    _row.putValue("doubleSV", 123.0);
    _row.putValue("stringSV", "123");
    _row.putValue("bytesSV", new byte[]{1, 2, 3});
    _row.putDefaultNullValue("nullSV", Integer.MAX_VALUE);
    _row.putValue("intMV", new Object[]{123, 456});
    _row.putValue("longMV", new Object[]{123L, 456L});
    _row.putValue("floatMV", new Object[]{123.0f, 456.0f});
    _row.putValue("doubleMV", new Object[]{123.0, 456.0});
    _row.putValue("stringMV", new Object[]{"123", "456"});
    _row.putDefaultNullValue("nullMV", new Object[]{Long.MIN_VALUE});
  }

  @Test
  public void testSerDeWithoutNullFields() {
    GenericRowSerializer serializer = new GenericRowSerializer(_fieldSpecs, false);
    byte[] bytes = serializer.serialize(_row);
    PinotDataBuffer dataBuffer = PinotDataBuffer.allocateDirect(bytes.length, PinotDataBuffer.NATIVE_ORDER, null);
    dataBuffer.readFrom(0L, bytes);
    GenericRowDeserializer deserializer = new GenericRowDeserializer(dataBuffer, _fieldSpecs, false);
    GenericRow reuse = new GenericRow();
    deserializer.deserialize(0L, reuse);
    Map<String, Object> actualValueMap = reuse.getFieldToValueMap();
    Map<String, Object> expectedValueMap = _row.getFieldToValueMap();
    // NOTE: Cannot directly assert equals on maps because they contain arrays
    assertEquals(actualValueMap.size(), expectedValueMap.size());
    for (Map.Entry<String, Object> entry : expectedValueMap.entrySet()) {
      assertEquals(actualValueMap.get(entry.getKey()), entry.getValue());
    }
    assertTrue(reuse.getNullValueFields().isEmpty());
  }

  @Test
  public void testSerDeWithNullFields() {
    GenericRowSerializer serializer = new GenericRowSerializer(_fieldSpecs, true);
    byte[] bytes = serializer.serialize(_row);
    PinotDataBuffer dataBuffer = PinotDataBuffer.allocateDirect(bytes.length, PinotDataBuffer.NATIVE_ORDER, null);
    dataBuffer.readFrom(0L, bytes);
    GenericRowDeserializer deserializer = new GenericRowDeserializer(dataBuffer, _fieldSpecs, true);
    GenericRow reuse = new GenericRow();
    deserializer.deserialize(0L, reuse);
    assertEquals(reuse, _row);
  }

  @Test
  public void testSerDeWithPartialFields() {
    List<FieldSpec> fieldSpecs = Arrays.asList(new DimensionFieldSpec("intSV", DataType.INT, true),
        new DimensionFieldSpec("nullSV", DataType.INT, true));
    GenericRowSerializer serializer = new GenericRowSerializer(fieldSpecs, true);
    byte[] bytes = serializer.serialize(_row);
    PinotDataBuffer dataBuffer = PinotDataBuffer.allocateDirect(bytes.length, PinotDataBuffer.NATIVE_ORDER, null);
    dataBuffer.readFrom(0L, bytes);
    GenericRowDeserializer deserializer = new GenericRowDeserializer(dataBuffer, fieldSpecs, true);
    GenericRow reuse = new GenericRow();
    deserializer.deserialize(0L, reuse);
    Map<String, Object> fieldToValueMap = reuse.getFieldToValueMap();
    assertEquals(fieldToValueMap.size(), 2);
    assertEquals(fieldToValueMap.get("intSV"), _row.getValue("intSV"));
    assertEquals(fieldToValueMap.get("nullSV"), _row.getValue("nullSV"));
    Set<String> nullValueFields = reuse.getNullValueFields();
    assertEquals(nullValueFields, Collections.singleton("nullSV"));
  }

  @Test
  public void testPartialDeserialize() {
    GenericRowSerializer serializer = new GenericRowSerializer(_fieldSpecs, true);
    byte[] bytes = serializer.serialize(_row);
    PinotDataBuffer dataBuffer = PinotDataBuffer.allocateDirect(bytes.length, PinotDataBuffer.NATIVE_ORDER, null);
    dataBuffer.readFrom(0L, bytes);
    GenericRowDeserializer deserializer = new GenericRowDeserializer(dataBuffer, _fieldSpecs, true);
    Object[] values = deserializer.partialDeserialize(0L, 7);
    assertEquals(values[0], _row.getValue("intSV"));
    assertEquals(values[1], _row.getValue("longSV"));
    assertEquals(values[2], _row.getValue("floatSV"));
    assertEquals(values[3], _row.getValue("doubleSV"));
    assertEquals(values[4], _row.getValue("stringSV"));
    assertEquals(values[5], _row.getValue("bytesSV"));
    assertEquals(values[6], _row.getValue("nullSV"));
  }
}
