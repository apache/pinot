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

import com.google.common.collect.Sets;
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
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.AbstractRecordReaderTest;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.testng.annotations.BeforeClass;


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
    // Create and init RecordReader
    _recordReader = createRecordReader();
  }

  protected static List<Map<String, Object>> generateRandomRecords(Schema pinotSchema) {
    // TODO: instead of hardcoding some rows, change this to work with the AbstractRecordReader's random value generator
    List<Map<String, Object>> records = new ArrayList<>();
    Map<String, Object> record1 = new HashMap<>();
    record1.put("active", "true");
    record1.put("created_at", 1515541280L);
    record1.put("id", 1);
    List<Integer> groups1 = new ArrayList<>();
    groups1.add(1);
    groups1.add(4);
    record1.put("groups", groups1);
    Map<String, Long> mapValues1 = new HashMap<>();
    mapValues1.put("name1", 1L);
    record1.put("map_values", mapValues1);
    List<String> setValues1 = new ArrayList<>();
    setValues1.add("name1");
    record1.put("set_values", setValues1);
    records.add(record1);

    Map<String, Object> record2 = new HashMap<>();
    record2.put("active", "false");
    record2.put("created_at", 1515541290L);
    record2.put("id", 1);
    List<Integer> groups2 = new ArrayList<>();
    groups2.add(2);
    groups2.add(3);
    record2.put("groups", groups2);
    records.add(record2);

    return records;
  }

  @Override
  protected org.apache.pinot.spi.data.Schema getPinotSchema() {
    return new Schema.SchemaBuilder().setSchemaName("ThriftSampleData")
        .addSingleValueDimension("id", FieldSpec.DataType.INT)
        .addSingleValueDimension("name", FieldSpec.DataType.STRING)
        .addSingleValueDimension("created_at", FieldSpec.DataType.LONG)
        .addSingleValueDimension("active", FieldSpec.DataType.BOOLEAN)
        .addMultiValueDimension("groups", FieldSpec.DataType.INT)
        .addMultiValueDimension("set_values", FieldSpec.DataType.STRING).build();
  }

  private Set<String> getSourceFields() {
    return Sets.newHashSet("id", "name", "created_at", "active", "groups", "set_values");
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
      data.setActive(Boolean.parseBoolean(record.get("active").toString()));
      data.setCreated_at(Math.abs(((Long) record.get("created_at")).longValue()));
      int i = Math.abs(((Integer) record.get("id")).intValue());
      data.setId(i);
      data.setName((String) record.get("name"));
      List<Integer> groupsList = (List<Integer>) record.get("groups");
      if (groupsList != null) {
        List<Short> groupsResult = new ArrayList<>(groupsList.size());
        for (Integer num : groupsList) {
          groupsResult.add(num.shortValue());
        }
        data.setGroups(groupsResult);
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
    FileWriter writer = new FileWriter(_dataFile);
    for (ThriftSampleData d : dataList) {
      IOUtils.write(binarySerializer.serialize(d), writer, Charset.defaultCharset());
    }
    writer.close();
  }

  @Override
  protected String getDataFileName() {
    return THRIFT_DATA;
  }
}
