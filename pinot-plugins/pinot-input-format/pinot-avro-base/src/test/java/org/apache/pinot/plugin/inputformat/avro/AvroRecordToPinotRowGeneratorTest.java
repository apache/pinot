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
package org.apache.pinot.plugin.inputformat.avro;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertEqualsDeep;
import static org.testng.Assert.assertTrue;


public class AvroRecordToPinotRowGeneratorTest {

  @Test
  public void testIncomingTimeColumn() {
    Schema schema =
        SchemaBuilder.record("test").fields().name("incomingTime").type().longType().noDefault().endRecord();
    GenericData.Record avroRecord = new GenericData.Record(schema);
    avroRecord.put("incomingTime", 12345L);

    Set<String> sourceFields = Set.of("incomingTime", "outgoingTime");

    AvroRecordExtractor avroRecordExtractor = new AvroRecordExtractor();
    avroRecordExtractor.init(sourceFields, null);
    GenericRow genericRow = new GenericRow();
    avroRecordExtractor.extract(avroRecord, genericRow);

    assertTrue(genericRow.getFieldToValueMap().keySet().containsAll(Arrays.asList("incomingTime", "outgoingTime")));
    assertEquals(genericRow.getValue("incomingTime"), 12345L);
  }

  @Test
  public void testMultiValueField() {
    Schema schema = SchemaBuilder.record("test").fields().name("intMV").type().array().items().intType().noDefault()
        .name("stringMV").type().array().items().stringType().noDefault().endRecord();
    GenericRecord genericRecord = new GenericData.Record(schema);

    AvroRecordExtractor avroRecordExtractor = new AvroRecordExtractor();
    avroRecordExtractor.init(null, null);
    GenericRow genericRow = new GenericRow();

    // List
    genericRecord.put("intMV", new ArrayList(List.of(1, 2, 3)));
    genericRecord.put("stringMV", new ArrayList(List.of("value1", "value2", "value3")));
    avroRecordExtractor.extract(genericRecord, genericRow);
    assertEqualsDeep(genericRow.getFieldToValueMap(),
        Map.of("intMV", new Object[]{1, 2, 3}, "stringMV", new Object[]{"value1", "value2", "value3"}));

    // Object[]
    genericRow.clear();
    genericRecord.put("intMV", new Object[]{1, 2, 3});
    genericRecord.put("stringMV", new Object[]{"value1", "value2", "value3"});
    avroRecordExtractor.extract(genericRecord, genericRow);
    assertEqualsDeep(genericRow.getFieldToValueMap(),
        Map.of("intMV", new Object[]{1, 2, 3}, "stringMV", new Object[]{"value1", "value2", "value3"}));

    // Integer[] and String[]
    genericRow.clear();
    genericRecord.put("intMV", new Integer[]{1, 2, 3});
    genericRecord.put("stringMV", new String[]{"value1", "value2", "value3"});
    avroRecordExtractor.extract(genericRecord, genericRow);
    assertEqualsDeep(genericRow.getFieldToValueMap(),
        Map.of("intMV", new Object[]{1, 2, 3}, "stringMV", new Object[]{"value1", "value2", "value3"}));

    // Primitive array
    genericRow.clear();
    genericRecord.put("intMV", new int[]{1, 2, 3});
    genericRecord.put("stringMV", new String[]{"value1", "value2", "value3"});
    avroRecordExtractor.extract(genericRecord, genericRow);
    assertEqualsDeep(genericRow.getFieldToValueMap(),
        Map.of("intMV", new Object[]{1, 2, 3}, "stringMV", new Object[]{"value1", "value2", "value3"}));

    // Empty List
    genericRow.clear();
    genericRecord.put("intMV", new ArrayList<>());
    genericRecord.put("stringMV", new ArrayList<>());
    avroRecordExtractor.extract(genericRecord, genericRow);
    assertEqualsDeep(genericRow.getFieldToValueMap(), Map.of("intMV", new Object[0], "stringMV", new Object[0]));

    // Empty array
    genericRow.clear();
    genericRecord.put("intMV", new int[0]);
    genericRecord.put("stringMV", new String[0]);
    avroRecordExtractor.extract(genericRecord, genericRow);
    assertEqualsDeep(genericRow.getFieldToValueMap(), Map.of("intMV", new Object[0], "stringMV", new Object[0]));

    // null
    genericRow.clear();
    genericRecord.put("intMV", null);
    genericRecord.put("stringMV", null);
    avroRecordExtractor.extract(genericRecord, genericRow);
    Map<String, Object> expected = new HashMap<>();
    expected.put("intMV", null);
    expected.put("stringMV", null);
    assertEqualsDeep(genericRow.getFieldToValueMap(), expected);
  }

  @Test
  public void testMapField() {
    Schema schema = SchemaBuilder.record("test").fields().name("intMap").type().map().values().intType().noDefault()
        .name("stringMap").type().map().values().stringType().noDefault().endRecord();
    GenericRecord genericRecord = new GenericData.Record(schema);

    AvroRecordExtractor avroRecordExtractor = new AvroRecordExtractor();
    avroRecordExtractor.init(null, null);
    GenericRow genericRow = new GenericRow();

    Map<String, Integer> intMap = new HashMap(Map.of("v1", 1, "v2", 2, "v3", 3));
    genericRecord.put("intMap", intMap);
    Map<String, String> stringMap = new HashMap(Map.of("v1", "value1", "v2", "value2", "v3", "value3"));
    genericRecord.put("stringMap", stringMap);
    avroRecordExtractor.extract(genericRecord, genericRow);
    assertEqualsDeep(genericRow.getFieldToValueMap(), Map.of("intMap", intMap, "stringMap", stringMap));

    // Map with null
    genericRow.clear();
    intMap = new HashMap<>();
    intMap.put("v1", 1);
    intMap.put("v2", null);
    intMap.put("v3", null);
    genericRecord.put("intMap", intMap);
    stringMap = new HashMap<>();
    stringMap.put("v1", null);
    stringMap.put("v2", null);
    stringMap.put("v3", "value3");
    genericRecord.put("stringMap", stringMap);
    avroRecordExtractor.extract(genericRecord, genericRow);
    assertEqualsDeep(genericRow.getFieldToValueMap(), Map.of("intMap", intMap, "stringMap", stringMap));
  }
}
