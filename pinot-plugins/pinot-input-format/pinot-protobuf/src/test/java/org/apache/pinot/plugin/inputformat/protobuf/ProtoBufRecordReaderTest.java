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
package org.apache.pinot.plugin.inputformat.protobuf;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.AbstractRecordReaderTest;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class ProtoBufRecordReaderTest extends AbstractRecordReaderTest {
  private final static Random RANDOM = new Random(System.currentTimeMillis());
  private static final String PROTO_DATA = "_test_sample_proto_data.data";
  private static final String DESCRIPTOR_FILE = "sample.desc";
  private File _tempFile;
  private RecordReader _recordReader;
  private final static int SAMPLE_RECORDS_SIZE = 10000;

  @Override
  protected Schema getPinotSchema() {
    return new Schema.SchemaBuilder().setSchemaName("SampleRecord")
        .addSingleValueDimension("id", FieldSpec.DataType.INT)
        .addSingleValueDimension("name", FieldSpec.DataType.STRING)
        .addSingleValueDimension("email", FieldSpec.DataType.STRING)
        .addMultiValueDimension("friends", FieldSpec.DataType.STRING).build();
  }

  private static List<Map<String, Object>> generateRandomRecords(Schema pinotSchema) {
    List<Map<String, Object>> records = new ArrayList<>();

    for (int i = 0; i < SAMPLE_RECORDS_SIZE; i++) {
      Map<String, Object> recordMap = new HashMap<>();
      for (FieldSpec fieldSpec : pinotSchema.getAllFieldSpecs()) {
        recordMap.put(fieldSpec.getName(), generateRandomValue(fieldSpec));
      }
      records.add(recordMap);
    }
    return records;
  }

  private static Object generateRandomValue(FieldSpec fieldSpec) {
    if (fieldSpec.isSingleValueField()) {
      return generateRandomSingleValue(fieldSpec);
    }
    List list = new ArrayList();
    int listSize = 1 + RANDOM.nextInt(50);
    for (int i = 0; i < listSize; i++) {
      list.add(generateRandomSingleValue(fieldSpec));
    }
    return list;
  }

  private static Object generateRandomSingleValue(FieldSpec fieldSpec) {
    switch (fieldSpec.getDataType()) {
      case INT:
        return RANDOM.nextInt();
      case LONG:
        return RANDOM.nextLong();
      case FLOAT:
        return RANDOM.nextFloat();
      case DOUBLE:
        return RANDOM.nextDouble();
      case STRING:
        return RandomStringUtils.randomAscii(RANDOM.nextInt(50) + 1);
      default:
        throw new RuntimeException("Not supported fieldSpec - " + fieldSpec);
    }
  }

  @BeforeClass
  @Override
  public void setUp() throws Exception {
    FileUtils.forceMkdir(_tempDir);
    // Generate Pinot schema
    _pinotSchema = getPinotSchema();
    // Generate random records based on Pinot schema
    _records = generateRandomRecords(_pinotSchema);
    _primaryKeys = generatePrimaryKeys(_records, getPrimaryKeyColumns());
    // Write generated random records to file
    writeRecordsToFile(_records);
    // Create and init RecordReader
    _recordReader = createRecordReader();
  }

  @AfterClass
  @Override
  public void tearDown() throws Exception {
    FileUtils.forceDelete(_tempFile);
  }

  @Test
  public void testRecordReader() throws Exception {
    checkValue(_recordReader, _records, _primaryKeys);
    _recordReader.rewind();
    checkValue(_recordReader, _records, _primaryKeys);
  }

  @Override
  protected RecordReader createRecordReader() throws Exception {
    RecordReader recordReader = new ProtoBufRecordReader();
    Set<String> sourceFields = getSourceFields(getPinotSchema());
    recordReader.init(_tempFile, sourceFields, getProtoRecordReaderConfig());
    return recordReader;
  }

  @Override
  protected void writeRecordsToFile(List<Map<String, Object>> recordsToWrite) throws Exception {
    List<Sample.SampleRecord> lists = new ArrayList<>();
    for (Map<String, Object> record : recordsToWrite) {
      Sample.SampleRecord sampleRecord =
          Sample.SampleRecord.newBuilder().setEmail((String) record.get("email")).setName((String) record.get("name"))
              .setId((Integer) record.get("id")).addAllFriends((List<String>) record.get("friends")).build();

      lists.add(sampleRecord);
    }

    _tempFile = getSampleDataPath();
    try (FileOutputStream output = new FileOutputStream(_tempFile, true)) {
      for (Sample.SampleRecord record : lists) {
        record.writeDelimitedTo(output);
      }
    }
  }

  private File getSampleDataPath() throws IOException {
    return File.createTempFile(ProtoBufRecordReaderTest.class.getName(), PROTO_DATA);
  }

  private ProtoBufRecordReaderConfig getProtoRecordReaderConfig() throws URISyntaxException {
    ProtoBufRecordReaderConfig config = new ProtoBufRecordReaderConfig();
    URL descriptorFile = getClass().getClassLoader().getResource(DESCRIPTOR_FILE);
    config.setDescriptorFile(descriptorFile.toURI());
    return config;
  }
}
