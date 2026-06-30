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
package org.apache.pinot.plugin.inputformat.thrift;

import java.io.File;
import java.io.FileWriter;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.AbstractRecordReaderTest;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


/**
 * Test {@code org.apache.pinot.plugin.inputformat.thrift.data.ThriftRecordReader} for a given sample thrift
 * data.
 */
public class ThriftRecordReaderTest extends AbstractRecordReaderTest {
  private static final String THRIFT_DATA = "_test_sample_thrift_data.thrift";

  private ThriftRecordReaderConfig getThriftRecordReaderConfig() {
    ThriftRecordReaderConfig config = new ThriftRecordReaderConfig();
    config.setThriftClass("org.apache.pinot.plugin.inputformat.thrift.ThriftSampleData");
    return config;
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    if (_tempDir.exists()) {
      FileUtils.cleanDirectory(_tempDir);
    }
    FileUtils.forceMkdir(_tempDir);
    // Generate Pinot schema
    _pinotSchema = getPinotSchema();
    _sourceFields = getSourceFields(_pinotSchema);
    // Generate random records based on Pinot schema
    _records = generateRandomRecords(_pinotSchema);
    _primaryKeys = generatePrimaryKeys(_records, getPrimaryKeyColumns());
    // Write generated random records to file
    writeRecordsToFile(_records);
  }

  protected static List<Map<String, Object>> generateRandomRecords(Schema pinotSchema) {
    // TODO: instead of hardcoding some rows, change this to work with the AbstractRecordReader's random value generator
    List<Map<String, Object>> records = new ArrayList<>();
    Map<String, Object> record1 = new HashMap<>();
    record1.put("active", true);
    record1.put("created_at", 1515541280L);
    record1.put("id", 1);
    // `groups` is `i16` in thrift but the extractor widens to `Integer` per the contract — use Integer here so
    // round-trip equality matches what the reader produces.
    record1.put("groups", List.of(1, 4));
    Map<String, Long> mapValues1 = new HashMap<>();
    mapValues1.put("name1", 1L);
    record1.put("map_values", mapValues1);
    List<String> setValues1 = new ArrayList<>();
    setValues1.add("name1");
    record1.put("set_values", setValues1);
    records.add(record1);

    Map<String, Object> record2 = new HashMap<>();
    record2.put("active", false);
    record2.put("created_at", 1515541290L);
    record2.put("id", 1);
    record2.put("groups", List.of(2, 3));
    records.add(record2);

    return records;
  }

  @Override
  protected org.apache.pinot.spi.data.Schema getPinotSchema() {
    return new Schema.SchemaBuilder()
        .setSchemaName("ThriftSampleData")
        .addSingleValueDimension("id", DataType.INT)
        .addSingleValueDimension("name", DataType.STRING)
        .addSingleValueDimension("created_at", DataType.LONG)
        .addSingleValueDimension("active", DataType.BOOLEAN)
        .addMultiValueDimension("groups", DataType.INT)
        .addMultiValueDimension("set_values", DataType.STRING)
        .build();
  }

  private Set<String> getSourceFields() {
    return Set.of("id", "name", "created_at", "active", "groups", "set_values");
  }

  @Override
  protected RecordReader createRecordReader(File file)
      throws Exception {
    ThriftRecordReader recordReader = new ThriftRecordReader();
    recordReader.init(file, getSourceFields(), getThriftRecordReaderConfig());
    return recordReader;
  }

  @Override
  protected void writeRecordsToFile(List<Map<String, Object>> recordsToWrite)
      throws Exception {
    List<ThriftSampleData> dataList = new ArrayList<>(recordsToWrite.size());
    for (Map<String, Object> record : recordsToWrite) {
      ThriftSampleData data = new ThriftSampleData();
      data.setActive((Boolean) record.get("active"));
      data.setCreated_at(Math.abs(((Long) record.get("created_at")).longValue()));
      int i = Math.abs(((Integer) record.get("id")).intValue());
      data.setId(i);
      data.setName((String) record.get("name"));
      @SuppressWarnings("unchecked")
      List<Integer> groupsList = (List<Integer>) record.get("groups");
      if (groupsList != null) {
        // Thrift `i16` is `short` on the wire; narrow each Integer to Short for the generated setter.
        List<Short> shortList = new ArrayList<>(groupsList.size());
        for (Integer v : groupsList) {
          shortList.add(v.shortValue());
        }
        data.setGroups(shortList);
      }
      List<String> setValuesList = (List<String>) record.get("set_values");
      if (setValuesList != null) {
        Set<String> setValuesResult = new HashSet<>(setValuesList.size());
        for (String s : setValuesList) {
          setValuesResult.add(s);
        }
        data.setSet_values(setValuesResult);
      }
      dataList.add(data);
    }

    TSerializer binarySerializer = new TSerializer(new TBinaryProtocol.Factory());
    try (FileWriter writer = new FileWriter(_dataFile)) {
      for (ThriftSampleData d : dataList) {
        IOUtils.write(binarySerializer.serialize(d), writer, Charset.defaultCharset());
      }
    }
  }

  @Override
  protected String getDataFileName() {
    return THRIFT_DATA;
  }

  /// Verify thrift primitive types are extracted per the [ThriftRecordExtractor] contract end-to-end through
  /// the reader — `bool` → `Boolean`, `i16` (multi-value) → `Integer[]` (small ints widen), `i32` → `Integer`,
  /// `i64` → `Long`.
  @Test
  public void testPrimitiveTypePreservation()
      throws Exception {
    try (RecordReader reader = createRecordReader()) {
      GenericRow row = reader.next();
      assertEquals(row.getValue("active"), true);
      assertEquals(row.getValue("id"), 1);
      assertEquals(row.getValue("created_at"), 1515541280L);
      assertEquals(row.getValue("groups"), new Object[]{1, 4});
    }
  }
}
