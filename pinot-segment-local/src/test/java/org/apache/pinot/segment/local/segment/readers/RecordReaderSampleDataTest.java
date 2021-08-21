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
package org.apache.pinot.segment.local.segment.readers;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import java.io.File;
import java.util.HashSet;
import java.util.Map;
import org.apache.pinot.segment.local.recordtransformer.CompositeTransformer;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.FileFormat;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.data.readers.RecordReaderFactory;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


public class RecordReaderSampleDataTest {
  private static final File AVRO_SAMPLE_DATA_FILE = new File(Preconditions
      .checkNotNull(RecordReaderSampleDataTest.class.getClassLoader().getResource("data/test_sample_data.avro"))
      .getFile());
  private static final File CSV_SAMPLE_DATA_FILE = new File(Preconditions
      .checkNotNull(RecordReaderSampleDataTest.class.getClassLoader().getResource("data/test_sample_data.csv"))
      .getFile());
  private static final File JSON_SAMPLE_DATA_FILE = new File(Preconditions
      .checkNotNull(RecordReaderSampleDataTest.class.getClassLoader().getResource("data/test_sample_data.json"))
      .getFile());
  private static final File JSON_EMPTY_DATA_FILE = new File(Preconditions
      .checkNotNull(RecordReaderSampleDataTest.class.getClassLoader().getResource("data/test_empty_data.json"))
      .getFile());
  private static final Schema SCHEMA =
      new Schema.SchemaBuilder().addSingleValueDimension("column1", FieldSpec.DataType.LONG)
          .addSingleValueDimension("column2", FieldSpec.DataType.INT)
          .addSingleValueDimension("column3", FieldSpec.DataType.STRING)
          .addSingleValueDimension("column7", FieldSpec.DataType.STRING)
          .addSingleValueDimension("unknown_dimension", FieldSpec.DataType.STRING)
          .addMetric("met_impressionCount", FieldSpec.DataType.LONG)
          .addMetric("unknown_metric", FieldSpec.DataType.DOUBLE).build();
  private static final TableConfig TABLE_CONFIG =
      new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").build();

  @Test
  public void testRecordReaders()
      throws Exception {
    CompositeTransformer defaultTransformer = CompositeTransformer.getDefaultTransformer(TABLE_CONFIG, SCHEMA);
    try (RecordReader avroRecordReader = RecordReaderFactory
        .getRecordReader(FileFormat.AVRO, AVRO_SAMPLE_DATA_FILE, SCHEMA.getColumnNames(), null);
        RecordReader csvRecordReader = RecordReaderFactory
            .getRecordReader(FileFormat.CSV, CSV_SAMPLE_DATA_FILE, SCHEMA.getColumnNames(), null);
        RecordReader jsonRecordReader = RecordReaderFactory
            .getRecordReader(FileFormat.JSON, JSON_SAMPLE_DATA_FILE, SCHEMA.getColumnNames(), null)) {
      int numRecords = 0;
      while (avroRecordReader.hasNext()) {
        assertTrue(csvRecordReader.hasNext());
        assertTrue(jsonRecordReader.hasNext());
        numRecords++;

        GenericRow avroRecord = defaultTransformer.transform(avroRecordReader.next());
        GenericRow csvRecord = defaultTransformer.transform(csvRecordReader.next());
        GenericRow jsonRecord = defaultTransformer.transform(jsonRecordReader.next());
        checkEqualCSV(avroRecord, csvRecord);
        checkEqual(avroRecord, jsonRecord);

        // Check the values from the first record
        if (numRecords == 1) {
          // Dimensions
          assertEquals(avroRecord.getValue("column1"), 1840748525967736008L);
          assertEquals(avroRecord.getValue("column2"), 231355578);
          assertEquals(avroRecord.getValue("column3"), "CezOib");

          // Dimension empty string
          assertEquals(avroRecord.getValue("column7"), "");

          // Dimension default column
          assertEquals(avroRecord.getValue("unknown_dimension"), "null");

          // Metric
          assertEquals(avroRecord.getValue("met_impressionCount"), 4955241829510629137L);

          // Metric default column
          assertEquals(avroRecord.getValue("unknown_metric"), 0.0);
        }
      }
      assertEquals(numRecords, 10001);
    }
  }

  @Test
  public void testRecordReaderEmptyFile()
      throws Exception {
    try (RecordReader jsonRecordReader = RecordReaderFactory
        .getRecordReader(FileFormat.JSON, JSON_EMPTY_DATA_FILE, SCHEMA.getColumnNames(), null)) {
      Assert.assertFalse(jsonRecordReader.hasNext());
    }
  }

  /**
   * Tests that record extractor is able to handle missing fields correctly (incoming and outgoing are missing from
   * data)
   * @throws Exception
   */
  @Test
  public void testRecordExtractorAbsentFields()
      throws Exception {
    HashSet<String> sourceFields = Sets.newHashSet("incoming", "time_day", "outgoing", "column2");
    try (RecordReader avroRecordReader = RecordReaderFactory
        .getRecordReader(FileFormat.AVRO, AVRO_SAMPLE_DATA_FILE, sourceFields, null);
        RecordReader csvRecordReader = RecordReaderFactory
            .getRecordReader(FileFormat.CSV, CSV_SAMPLE_DATA_FILE, sourceFields, null);
        RecordReader jsonRecordReader = RecordReaderFactory
            .getRecordReader(FileFormat.JSON, JSON_SAMPLE_DATA_FILE, sourceFields, null)) {
      int numRecords = 0;
      while (avroRecordReader.hasNext()) {
        assertTrue(csvRecordReader.hasNext());
        assertTrue(jsonRecordReader.hasNext());
        numRecords++;

        GenericRow avroRecord = avroRecordReader.next();
        GenericRow csvRecord = csvRecordReader.next();
        GenericRow jsonRecord = jsonRecordReader.next();
        checkEqualCSV(avroRecord, csvRecord);
        checkEqual(avroRecord, jsonRecord);

        // Check the values from the first record
        if (numRecords == 1) {
          assertEquals(Long.valueOf(avroRecord.getValue("time_day").toString()), new Long(1072889503L));
          assertEquals(avroRecord.getValue("column2"), 231355578);
          assertNull(avroRecord.getValue("incoming"));
          assertNull(avroRecord.getValue("outgoing"));
        }
      }
      assertEquals(numRecords, 10001);
    }
  }

  /**
   * True data types are not achieved until DataType transformer. Hence, pure equality might not work in most cases
   * (Integer Long etc)
   */
  private void checkEqual(GenericRow row1, GenericRow row2) {
    for (Map.Entry<String, Object> entry : row1.getFieldToValueMap().entrySet()) {
      if (entry.getValue() == null) {
        Assert.assertNull(row2.getValue(entry.getKey()));
      } else {
        Assert.assertEquals(entry.getValue().toString(), row2.getValue(entry.getKey()).toString());
      }
    }
  }

  /**
   * True data types are not achieved until DataType transformer. Hence, pure equality might not work in most cases
   * (Integer Long etc)
   * Empty string gets treated as null value in CSV, because we no longer have data types
   */
  private void checkEqualCSV(GenericRow row, GenericRow csvRecord) {
    for (Map.Entry<String, Object> entry : row.getFieldToValueMap().entrySet()) {
      Object row1Value = entry.getValue();
      String row1Key = entry.getKey();
      if (row1Value == null) {
        Assert.assertNull(csvRecord.getValue(row1Key));
      } else if (row1Value.toString().isEmpty()) {
        Assert.assertEquals(csvRecord.getValue(row1Key), FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_STRING);
      } else {
        Assert.assertEquals(row1Value.toString(), csvRecord.getValue(row1Key).toString());
      }
    }
  }
}
