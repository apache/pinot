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

import com.google.common.collect.Sets;
import java.io.File;
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
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public abstract class AbstractRecordReaderTest {
  private final static Random RANDOM = new Random(System.currentTimeMillis());
  private final static int SAMPLE_RECORDS_SIZE = 10000;

  protected final File _tempDir = new File(FileUtils.getTempDirectory(), "RecordReaderTest");
  protected List<Map<String, Object>> _records;
  protected org.apache.pinot.spi.data.Schema _pinotSchema;
  protected Set<String> _sourceFields;
  private RecordReader _recordReader;

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

  protected void checkValue(RecordReader recordReader, List<Map<String, Object>> expectedRecordsMap)
      throws Exception {
    for (Map<String, Object> expectedRecord : expectedRecordsMap) {
      GenericRow actualRecord = recordReader.next();
      org.apache.pinot.spi.data.Schema pinotSchema = recordReader.getSchema();
      for (FieldSpec fieldSpec : pinotSchema.getAllFieldSpecs()) {
        String fieldSpecName = fieldSpec.getName();
        if (fieldSpec.isSingleValueField()) {
          Assert.assertEquals(actualRecord.getValue(fieldSpecName), expectedRecord.get(fieldSpecName));
        } else {
          Object[] actualRecords = (Object[]) actualRecord.getValue(fieldSpecName);
          List expectedRecords = (List) expectedRecord.get(fieldSpecName);
          Assert.assertEquals(actualRecords.length, expectedRecords.size());
          for (int i = 0; i < actualRecords.length; i++) {
            Assert.assertEquals(actualRecords[i], expectedRecords.get(i));
          }
        }
      }
    }
    Assert.assertFalse(recordReader.hasNext());
  }

  protected org.apache.pinot.spi.data.Schema getPinotSchema() {
    return new org.apache.pinot.spi.data.Schema.SchemaBuilder()
        .addSingleValueDimension("dim_sv_int", FieldSpec.DataType.INT)
        .addSingleValueDimension("dim_sv_long", FieldSpec.DataType.LONG)
        .addSingleValueDimension("dim_sv_float", FieldSpec.DataType.FLOAT)
        .addSingleValueDimension("dim_sv_double", FieldSpec.DataType.DOUBLE)
        .addSingleValueDimension("dim_sv_string", FieldSpec.DataType.STRING)
        .addMultiValueDimension("dim_mv_int", FieldSpec.DataType.INT)
        .addMultiValueDimension("dim_mv_long", FieldSpec.DataType.LONG)
        .addMultiValueDimension("dim_mv_float", FieldSpec.DataType.FLOAT)
        .addMultiValueDimension("dim_mv_double", FieldSpec.DataType.DOUBLE)
        .addMultiValueDimension("dim_mv_string", FieldSpec.DataType.STRING).addMetric("met_int", FieldSpec.DataType.INT)
        .addMetric("met_long", FieldSpec.DataType.LONG).addMetric("met_float", FieldSpec.DataType.FLOAT)
        .addMetric("met_double", FieldSpec.DataType.DOUBLE).build();
  }

  protected Set<String> getSourceFields(Schema schema) {
    return Sets.newHashSet(schema.getColumnNames());
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    FileUtils.forceMkdir(_tempDir);
    // Generate Pinot schema
    _pinotSchema = getPinotSchema();
    _sourceFields = getSourceFields(_pinotSchema);
    // Generate random records based on Pinot schema
    _records = generateRandomRecords(_pinotSchema);
    // Write generated random records to file
    writeRecordsToFile(_records);
    // Create and init RecordReader
    _recordReader = createRecordReader();
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    FileUtils.forceDelete(_tempDir);
  }

  @Test
  public void testRecordReader()
      throws Exception {
    checkValue(_recordReader, _records);
    _recordReader.rewind();
    checkValue(_recordReader, _records);
  }

  /**
   * @return an implementation of RecordReader
   * @throws Exception
   */
  protected abstract RecordReader createRecordReader()
      throws Exception;

  /**
   * Write records into a file
   * @throws Exception
   */
  protected abstract void writeRecordsToFile(List<Map<String, Object>> recordsToWrite)
      throws Exception;
}
