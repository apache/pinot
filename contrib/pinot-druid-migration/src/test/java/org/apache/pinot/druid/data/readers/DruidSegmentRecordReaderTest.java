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
package org.apache.pinot.druid.data.readers;

import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.pinot.common.data.FieldSpec;
import org.apache.pinot.common.data.Schema;
import org.apache.pinot.core.data.GenericRow;
import org.apache.pinot.core.data.readers.JSONRecordReader;
import org.apache.pinot.core.data.recordtransformer.CompositeTransformer;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class DruidSegmentRecordReaderTest {
  // test_sample_data is a Druid segment that was created from pinot-core/src/test/resources/data/test_sample_data.json
  private static final File TEST_SAMPLE_DATA = new File(Preconditions
      .checkNotNull(DruidSegmentRecordReaderTest.class.getClassLoader().getResource("test_sample_data"))
      .getFile());
  private static final File TEST_SAMPLE_DATA_JSON = new File(Preconditions
      .checkNotNull(DruidSegmentRecordReaderTest.class.getClassLoader().getResource("test_sample_data_dumped.json"))
      .getFile());
  // Same schema as in RecordReaderSampleDataTest
  private static final Schema TEST_SAMPLE_DATA_SCHEMA = new Schema.SchemaBuilder().addSingleValueDimension("column1", FieldSpec.DataType.LONG)
        .addSingleValueDimension("column2", FieldSpec.DataType.LONG)
        .addSingleValueDimension("column3", FieldSpec.DataType.STRING)
        .addSingleValueDimension("column7", FieldSpec.DataType.STRING)
        .addSingleValueDimension("unknown_dimension", FieldSpec.DataType.STRING)
        .addMetric("met_impressionCount", FieldSpec.DataType.LONG).addMetric("unknown_metric", FieldSpec.DataType.DOUBLE)
        .build();
  // Test based on CSVRecordReaderTest
  private static final File CSV_TEST = new File(Preconditions
      .checkNotNull(DruidSegmentRecordReaderTest.class.getClassLoader().getResource("test_druid_from_csv"))
      .getFile());
  private static final Schema CSV_TEST_SCHEMA = new Schema.SchemaBuilder().addSingleValueDimension("__time", FieldSpec.DataType.LONG)
      .addMultiValueDimension("STR_MV", FieldSpec.DataType.STRING)
      .addMetric("LONG_SV", FieldSpec.DataType.LONG)
      .build();
  private static final File ALL_TYPES_TEST = new File(Preconditions
      .checkNotNull(DruidSegmentRecordReaderTest.class.getClassLoader().getResource("test_druid_all_types"))
      .getFile());
  private static final Schema ALL_TYPES_TEST_SCHEMA = new Schema.SchemaBuilder().addSingleValueDimension("__time", FieldSpec.DataType.LONG)
      .addSingleValueDimension("stringval", FieldSpec.DataType.STRING)
      .addMultiValueDimension("tags", FieldSpec.DataType.STRING)
      .addMetric("longnum", FieldSpec.DataType.LONG)
      .addMetric("floatnum", FieldSpec.DataType.FLOAT)
      .addMetric("doublenum", FieldSpec.DataType.DOUBLE)
      .build();

  private void testReadRowsWithRecordList(DruidSegmentRecordReader reader, Object[][] records, String[] columns)
      throws IOException {
    CompositeTransformer defaultTransformer = CompositeTransformer.getDefaultTransformer(reader.getSchema());

    List<GenericRow> genericRows = new ArrayList<>();
    int row = 0;
    while (reader.hasNext()) {
      Assert.assertTrue(row < records.length);

      GenericRow currentRow = reader.next();
      GenericRow transformedRow = defaultTransformer.transform(currentRow);

      for (int col = 0; col < columns.length; col++) {
        if (records[row][col] != null) {
          if (transformedRow.getValue(columns[col]) != null && records[row][col] != null) {
            if (transformedRow.getValue(columns[col]) instanceof String[]) {
              Assert.assertTrue(compareMultiValueColumn(transformedRow.getValue(columns[col]), records[row][col]));
            } else {
              Assert.assertEquals(transformedRow.getValue(columns[col]), records[row][col]);
            }
          }
        }
      }
      genericRows.add(currentRow);
      row += 1;
    }
    Assert.assertFalse(reader.hasNext());
    Assert.assertEquals(genericRows.size(), records.length,
        String.format("Size of GenericRows (%d) must be equal to records size (%d).", genericRows.size(), records.length));
  }

  private boolean compareMultiValueColumn(Object value1, Object value2) {
    Object[] value1Array = (Object[]) value1;
    Object[] value2Array = (Object[]) value2;
    Set<Object> value1Set = new HashSet<>(Arrays.asList(value1Array));
    Set<Object> value2Set = new HashSet<>(Arrays.asList(value2Array));
    return value1Set.containsAll(value2Set);
  }

  @Test
  public void testRecordReaderWithJson()
      throws IOException {
    CompositeTransformer defaultTransformer = CompositeTransformer.getDefaultTransformer(TEST_SAMPLE_DATA_SCHEMA);
    try (DruidSegmentRecordReader druidRecordReader = new DruidSegmentRecordReader(TEST_SAMPLE_DATA, TEST_SAMPLE_DATA_SCHEMA);
        JSONRecordReader jsonRecordReader = new JSONRecordReader(TEST_SAMPLE_DATA_JSON, TEST_SAMPLE_DATA_SCHEMA)) {
      int numRecords = 0;
      while (druidRecordReader.hasNext()) {
        assertTrue(jsonRecordReader.hasNext());
        numRecords++;

        GenericRow druidRecord = defaultTransformer.transform(druidRecordReader.next());
        GenericRow jsonRecord = defaultTransformer.transform(jsonRecordReader.next());
        assertEquals(druidRecord, jsonRecord);

        // Check the values from the first record
        if (numRecords == 1) {
          // Dimensions
          Assert.assertEquals(druidRecord.getValue("column1"), 1840748525967736008L);
          Assert.assertEquals(druidRecord.getValue("column2"), 231355578L);
          Assert.assertEquals(druidRecord.getValue("column3"), "CezOib");

          // Dimension empty string
          Assert.assertEquals(druidRecord.getValue("column7").toString(), "null");

          // Dimension default column
          Assert.assertEquals(druidRecord.getValue("unknown_dimension"), "null");

          // Metric
          Assert.assertEquals(druidRecord.getValue("met_impressionCount"), 8637957270245933828L);

          // Metric default column
          Assert.assertEquals(druidRecord.getValue("unknown_metric"), 0.0);
        }
      }
      Assert.assertEquals(numRecords, 10001);
    }
  }

  @Test
  public void testCSVRecord()
      throws IOException {
    final String[] columns = {"LONG_SV", "STR_MV", "__time"};
    final Object[][] records = {
        {new Long(5), new String[]{"10", "15", "20"}, Instant.parse("2010-01-01T00:00:00.000Z").toEpochMilli()},
        {new Long(25), new String[]{"30", "35", "40"}, Instant.parse("2010-01-02T00:00:00.000Z").toEpochMilli()},
        {null, null, Instant.parse("2010-01-03T00:00:00.000Z").toEpochMilli()}};
    try (DruidSegmentRecordReader testReader = new DruidSegmentRecordReader(CSV_TEST, CSV_TEST_SCHEMA)) {
      testReadRowsWithRecordList(testReader, records, columns);
    }
  }

  @Test
  public void testAllColumnTypes()
      throws IOException {
    final String[] columns = {"__time", "longnum", "floatnum", "tags", "stringval", "doublenum"};
    final Object[][] records = {
        {Long.parseLong("1294704000000"), Long.parseLong("0"), null, null, null, Double.parseDouble("0.0")},
        {Long.parseLong("1294790400000"), Long.parseLong("1"), new Float(0.1), new String[]{"t1","t2","t3"}, "test1", Double.parseDouble("300.47179")},
        {Long.parseLong("1294876800000"), Long.parseLong("2"), new Float(10.02), new String[]{"t3","t4","t5"}, "test2", Double.parseDouble("102893.383401")},
        {Long.parseLong("1294963200000"), Long.parseLong("3"), new Float(100.003), new String[]{"t5","t6","t7"}, "test3", Double.parseDouble("0.5559")}};
    try (DruidSegmentRecordReader testReader = new DruidSegmentRecordReader(ALL_TYPES_TEST, ALL_TYPES_TEST_SCHEMA)) {
      testReadRowsWithRecordList(testReader, records, columns);
    }
  }

  @Test
  public void testRewind()
      throws IOException {
    final String[] columns = {"LONG_SV", "STR_MV", "__time"};
    final Object[][] records = {
        {new Long(5), new String[]{"10", "15", "20"}, Instant.parse("2010-01-01T00:00:00.000Z").toEpochMilli()},
        {new Long(25), new String[]{"30", "35", "40"}, Instant.parse("2010-01-02T00:00:00.000Z").toEpochMilli()},
        {null, null, Instant.parse("2010-01-03T00:00:00.000Z").toEpochMilli()}};

    try (DruidSegmentRecordReader testReader = new DruidSegmentRecordReader(CSV_TEST, CSV_TEST_SCHEMA)) {
      testReadRowsWithRecordList(testReader, records, columns);
      Assert.assertFalse(testReader.hasNext());

      testReader.rewind();
      Assert.assertTrue(testReader.hasNext());

      testReadRowsWithRecordList(testReader, records, columns);
    }
  }
}
