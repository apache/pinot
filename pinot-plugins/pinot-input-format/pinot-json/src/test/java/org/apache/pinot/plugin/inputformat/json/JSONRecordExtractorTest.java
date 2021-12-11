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
package org.apache.pinot.plugin.inputformat.json;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Sets;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.spi.data.readers.AbstractRecordExtractorTest;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.utils.JsonUtils;


/**
 * Tests the {@link JSONRecordExtractor}
 */
public class JSONRecordExtractorTest extends AbstractRecordExtractorTest {

  private final File _dataFile = new File(_tempDir, "events.json");

  private static final String NULL_FIELD = "myNull";
  private static final String INT_FIELD = "myInt";
  private static final String LONG_FIELD = "myLong";
  private static final String DOUBLE_FIELD = "myDouble";
  private static final String STRING_FIELD = "myString";
  private static final String INT_ARRAY_FIELD = "myIntArray";
  private static final String DOUBLE_ARRAY_FIELD = "myDoubleArray";
  private static final String STRING_ARRAY_FIELD = "myStringArray";
  private static final String COMPLEX_ARRAY_1_FIELD = "myComplexArray1";
  private static final String COMPLEX_ARRAY_2_FIELD = "myComplexArray2";
  private static final String MAP_1_FIELD = "myMap1";
  private static final String MAP_2_FIELD = "myMap2";

  @Override
  protected List<Map<String, Object>> getInputRecords() {
    return Arrays.asList(createRecord1(), createRecord2());
  }

  @Override
  protected Set<String> getSourceFields() {
    return Sets
        .newHashSet(NULL_FIELD, INT_FIELD, LONG_FIELD, DOUBLE_FIELD, STRING_FIELD, INT_ARRAY_FIELD, DOUBLE_ARRAY_FIELD,
            STRING_ARRAY_FIELD, COMPLEX_ARRAY_1_FIELD, COMPLEX_ARRAY_2_FIELD, MAP_1_FIELD, MAP_2_FIELD);
  }

  /**
   * Create a JSONRecordReader
   */
  @Override
  protected RecordReader createRecordReader(Set<String> fieldsToRead)
      throws IOException {
    JSONRecordReader recordReader = new JSONRecordReader();
    recordReader.init(_dataFile, fieldsToRead, null);
    return recordReader;
  }

  /**
   * Create a JSON input file using the input records
   */
  @Override
  protected void createInputFile()
      throws IOException {
    try (FileWriter fileWriter = new FileWriter(_dataFile)) {
      for (Map<String, Object> inputRecord : _inputRecords) {
        ObjectNode jsonRecord = JsonUtils.newObjectNode();
        for (String key : inputRecord.keySet()) {
          jsonRecord.set(key, JsonUtils.objectToJsonNode(inputRecord.get(key)));
        }
        fileWriter.write(jsonRecord.toString());
      }
    }
  }

  private Map<String, Object> createRecord1() {
    Map<String, Object> record = new HashMap<>();
    record.put(NULL_FIELD, null);
    record.put(INT_FIELD, 10);
    record.put(LONG_FIELD, 1588469340000L);
    record.put(DOUBLE_FIELD, 10.2);
    record.put(STRING_FIELD, "foo");
    record.put(INT_ARRAY_FIELD, Arrays.asList(10, 20, 30));
    record.put(DOUBLE_ARRAY_FIELD, Arrays.asList(10.2, 12.1, 1.1));
    record.put(STRING_ARRAY_FIELD, Arrays.asList("foo", "bar"));

    Map<String, Object> map1 = new HashMap<>();
    map1.put("one", 1);
    map1.put("two", "too");
    Map<String, Object> map2 = new HashMap<>();
    map2.put("one", 11);
    map2.put("two", "roo");
    record.put(COMPLEX_ARRAY_1_FIELD, Arrays.asList(map1, map2));

    Map<String, Object> map3 = new HashMap<>();
    map3.put("one", 1);
    Map<String, Object> map31 = new HashMap<>();
    map31.put("sub1", 1.1);
    map31.put("sub2", 1.2);
    map3.put("two", map31);
    map3.put("three", Arrays.asList("a", "b"));

    Map<String, Object> map4 = new HashMap<>();
    map4.put("one", 11);
    Map<String, Object> map41 = new HashMap<>();
    map41.put("sub1", 11.1);
    map41.put("sub2", 11.2);
    map4.put("two", map41);
    map4.put("three", Arrays.asList("aa", "bb"));
    record.put(COMPLEX_ARRAY_2_FIELD, Arrays.asList(map3, map4));

    Map<String, Object> map5 = new HashMap<>();
    map5.put("k1", "foo");
    map5.put("k2", "bar");
    record.put(MAP_1_FIELD, map5);

    Map<String, Object> map6 = new HashMap<>();
    Map<String, Object> map61 = new HashMap<>();
    map61.put("sub1", 10);
    map61.put("sub2", 1.0);
    map6.put("k3", map61);
    map6.put("k4", "baz");
    map6.put("k5", Arrays.asList(1, 2, 3));
    record.put(MAP_2_FIELD, map6);

    return record;
  }

  private Map<String, Object> createRecord2() {
    Map<String, Object> record = new HashMap<>();
    record.put(NULL_FIELD, null);
    record.put(INT_FIELD, 20);
    record.put(LONG_FIELD, 998732130000L);
    record.put(DOUBLE_FIELD, 11.2);
    record.put(STRING_FIELD, "hello");
    record.put(INT_ARRAY_FIELD, Arrays.asList(100, 200, 300));
    record.put(DOUBLE_ARRAY_FIELD, Arrays.asList(20.1, 30.2, 40.3));
    record.put(STRING_ARRAY_FIELD, Arrays.asList("hello", "world!"));

    Map<String, Object> map1 = new HashMap<>();
    map1.put("two", 2);
    map1.put("three", "tree");
    Map<String, Object> map2 = new HashMap<>();
    map2.put("two", 22);
    map2.put("three", "free");
    record.put(COMPLEX_ARRAY_1_FIELD, Arrays.asList(map1, map2));

    Map<String, Object> map3 = new HashMap<>();
    map3.put("two", 2);
    Map<String, Object> map31 = new HashMap<>();
    map31.put("sub1", 1.2);
    map31.put("sub2", 2.3);
    map3.put("two", map31);
    map3.put("three", Arrays.asList("b", "c"));

    Map<String, Object> map4 = new HashMap<>();
    map4.put("one", 12);
    Map<String, Object> map41 = new HashMap<>();
    map41.put("sub1", 12.2);
    map41.put("sub2", 10.1);
    map4.put("two", map41);
    map4.put("three", Arrays.asList("cc", "cc"));
    record.put(COMPLEX_ARRAY_2_FIELD, Arrays.asList(map3, map4));

    Map<String, Object> map5 = new HashMap<>();
    map5.put("k1", "hello");
    map5.put("k2", "world");
    record.put(MAP_1_FIELD, map5);

    Map<String, Object> map6 = new HashMap<>();
    Map<String, Object> map61 = new HashMap<>();
    map61.put("sub1", 20);
    map61.put("sub2", 2.0);
    map6.put("k3", map61);
    map6.put("k4", "abc");
    map6.put("k5", Arrays.asList(3, 2, 1));
    record.put(MAP_2_FIELD, map6);

    return record;
  }
}
