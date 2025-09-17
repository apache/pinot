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

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.data.ComplexFieldSpec;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.MetricFieldSpec;
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
        new DimensionFieldSpec("bytesSV", DataType.BYTES, true),
        new MetricFieldSpec("bigDecimalSV", DataType.BIG_DECIMAL), new DimensionFieldSpec("nullSV", DataType.INT, true),
        new ComplexFieldSpec("mapSV", DataType.MAP, true, new HashMap<>()),
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
    _row.putValue("bigDecimalSV", new BigDecimal("122333"));
    _row.putDefaultNullValue("nullSV", Integer.MAX_VALUE);

    // Add MAP data
    Map<String, Object> mapData = new HashMap<>();
    mapData.put("key1", "value1");
    mapData.put("key2", 42);
    mapData.put("key3", 3.14);
    _row.putValue("mapSV", mapData);
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
    GenericRow buffer = new GenericRow();
    deserializer.deserialize(0L, buffer);
    Map<String, Object> actualValueMap = buffer.getFieldToValueMap();
    Map<String, Object> expectedValueMap = _row.getFieldToValueMap();
    // NOTE: Cannot directly assert equals on maps because they contain arrays
    assertEquals(actualValueMap.size(), expectedValueMap.size());
    for (Map.Entry<String, Object> entry : expectedValueMap.entrySet()) {
      assertEquals(actualValueMap.get(entry.getKey()), entry.getValue());
    }
    assertTrue(buffer.getNullValueFields().isEmpty());
  }

  @Test
  public void testSerDeWithNullFields() {
    GenericRowSerializer serializer = new GenericRowSerializer(_fieldSpecs, true);
    byte[] bytes = serializer.serialize(_row);
    PinotDataBuffer dataBuffer = PinotDataBuffer.allocateDirect(bytes.length, PinotDataBuffer.NATIVE_ORDER, null);
    dataBuffer.readFrom(0L, bytes);
    GenericRowDeserializer deserializer = new GenericRowDeserializer(dataBuffer, _fieldSpecs, true);
    GenericRow buffer = new GenericRow();
    deserializer.deserialize(0L, buffer);
    assertEquals(buffer, _row);
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
    GenericRow buffer = new GenericRow();
    deserializer.deserialize(0L, buffer);
    Map<String, Object> fieldToValueMap = buffer.getFieldToValueMap();
    assertEquals(fieldToValueMap.size(), 2);
    assertEquals(fieldToValueMap.get("intSV"), _row.getValue("intSV"));
    assertEquals(fieldToValueMap.get("nullSV"), _row.getValue("nullSV"));
    Set<String> nullValueFields = buffer.getNullValueFields();
    assertEquals(nullValueFields, Collections.singleton("nullSV"));
  }

  @Test
  public void testCompare() {
    GenericRowSerializer serializer = new GenericRowSerializer(_fieldSpecs, true);
    byte[] bytes = serializer.serialize(_row);
    long numBytes = bytes.length;
    PinotDataBuffer dataBuffer = PinotDataBuffer.allocateDirect(numBytes * 2, PinotDataBuffer.NATIVE_ORDER, null);
    dataBuffer.readFrom(0L, bytes);
    dataBuffer.readFrom(numBytes, bytes);
    GenericRowDeserializer deserializer = new GenericRowDeserializer(dataBuffer, _fieldSpecs, true);
    int numFields = _fieldSpecs.size();
    for (int i = 0; i < numFields; i++) {
      assertEquals(deserializer.compare(0L, numBytes, i), 0);
    }
  }

  @Test
  public void testMapSerDe() {
    List<FieldSpec> mapFieldSpecs = Arrays.asList(new ComplexFieldSpec("mapSV", DataType.MAP, true, new HashMap<>()));

    GenericRow mapRow = new GenericRow();
    Map<String, Object> testMap = new HashMap<>();
    testMap.put("stringKey", "stringValue");
    testMap.put("intKey", 123);
    testMap.put("doubleKey", 45.67);
    mapRow.putValue("mapSV", testMap);

    GenericRowSerializer serializer = new GenericRowSerializer(mapFieldSpecs, false);
    byte[] bytes = serializer.serialize(mapRow);
    PinotDataBuffer dataBuffer = PinotDataBuffer.allocateDirect(bytes.length, PinotDataBuffer.NATIVE_ORDER, null);
    dataBuffer.readFrom(0L, bytes);
    GenericRowDeserializer deserializer = new GenericRowDeserializer(dataBuffer, mapFieldSpecs, false);
    GenericRow buffer = new GenericRow();
    deserializer.deserialize(0L, buffer);

    @SuppressWarnings("unchecked")
    Map<String, Object> deserializedMap = (Map<String, Object>) buffer.getValue("mapSV");
    assertEquals(deserializedMap, testMap);
  }

  @Test
  public void testMapCompare() {
    // Test MAP comparison for both different and equal maps
    List<FieldSpec> mapFieldSpecs = Arrays.asList(new ComplexFieldSpec("mapSV", DataType.MAP, true, new HashMap<>()));

    GenericRow mapRow1 = new GenericRow();
    Map<String, Object> testMap1 = new HashMap<>();
    testMap1.put("key1", "value1");
    testMap1.put("key2", 123);
    mapRow1.putValue("mapSV", testMap1);

    GenericRow mapRow2 = new GenericRow();
    Map<String, Object> testMap2 = new HashMap<>();
    testMap2.put("key1", "value2");
    testMap2.put("key2", 123);
    mapRow2.putValue("mapSV", testMap2);

    GenericRowSerializer serializer = new GenericRowSerializer(mapFieldSpecs, false);
    byte[] bytes1 = serializer.serialize(mapRow1);
    byte[] bytes2 = serializer.serialize(mapRow2);

    long numBytes = Math.max(bytes1.length, bytes2.length);
    PinotDataBuffer dataBuffer = PinotDataBuffer.allocateDirect(numBytes * 2, PinotDataBuffer.NATIVE_ORDER, null);
    dataBuffer.readFrom(0L, bytes1);
    dataBuffer.readFrom(numBytes, bytes2);

    GenericRowDeserializer deserializer = new GenericRowDeserializer(dataBuffer, mapFieldSpecs, true);

    // This should return non-zero as values are different
    int result = deserializer.compare(0L, numBytes, 1);
    assertTrue(result != 0, "MAP comparison should return non-zero for different maps");

    // Test 2: Equal maps should return 0
    GenericRow mapRow3 = new GenericRow();
    Map<String, Object> testMap3 = new HashMap<>();
    testMap3.put("key1", "value1");
    testMap3.put("key2", 123);
    mapRow3.putValue("mapSV", testMap3);

    GenericRow mapRow4 = new GenericRow();
    Map<String, Object> testMap4 = new HashMap<>();
    testMap4.put("key1", "value1"); // Same value
    testMap4.put("key2", 123);      // Same value
    mapRow4.putValue("mapSV", testMap4);

    byte[] bytes3 = serializer.serialize(mapRow3);
    byte[] bytes4 = serializer.serialize(mapRow4);

    long numBytes2 = Math.max(bytes3.length, bytes4.length);
    PinotDataBuffer dataBuffer2 = PinotDataBuffer.allocateDirect(numBytes2 * 2, PinotDataBuffer.NATIVE_ORDER, null);
    dataBuffer2.readFrom(0L, bytes3);
    dataBuffer2.readFrom(numBytes2, bytes4);

    GenericRowDeserializer deserializer2 = new GenericRowDeserializer(dataBuffer2, mapFieldSpecs, true);

    int result2 = deserializer2.compare(0L, numBytes2, 1);
    assertEquals(result2, 0, "MAP comparison should return 0 for equal maps");
  }
}
