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
package org.apache.pinot.plugin.inputformat.csv;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.AbstractRecordReaderTest;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.PrimaryKey;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.testng.Assert;
import org.testng.annotations.Test;


public class CSVRecordReaderTest extends AbstractRecordReaderTest {
  private static final char CSV_MULTI_VALUE_DELIMITER = '\t';

  @Override
  protected RecordReader createRecordReader(File file)
      throws Exception {
    CSVRecordReaderConfig csvRecordReaderConfig = new CSVRecordReaderConfig();
    csvRecordReaderConfig.setMultiValueDelimiter(CSV_MULTI_VALUE_DELIMITER);
    CSVRecordReader csvRecordReader = new CSVRecordReader();
    csvRecordReader.init(file, _sourceFields, csvRecordReaderConfig);
    return csvRecordReader;
  }

  @Override
  protected void writeRecordsToFile(List<Map<String, Object>> recordsToWrite)
      throws Exception {
    Schema pinotSchema = getPinotSchema();
    String[] columns = pinotSchema.getColumnNames().toArray(new String[0]);
    try (FileWriter fileWriter = new FileWriter(_dataFile);
        CSVPrinter csvPrinter = new CSVPrinter(fileWriter, CSVFormat.DEFAULT.withHeader(columns))) {

      for (Map<String, Object> r : recordsToWrite) {
        Object[] record = new Object[columns.length];
        for (int i = 0; i < columns.length; i++) {
          if (pinotSchema.getFieldSpecFor(columns[i]).isSingleValueField()) {
            record[i] = r.get(columns[i]);
          } else {
            record[i] = StringUtils.join(((List) r.get(columns[i])).toArray(), CSV_MULTI_VALUE_DELIMITER);
          }
        }
        csvPrinter.printRecord(record);
      }
    }
  }

  @Override
  protected String getDataFileName() {
    return "data.csv";
  }

  @Override
  protected void checkValue(RecordReader recordReader, List<Map<String, Object>> expectedRecordsMap,
      List<Object[]> expectedPrimaryKeys)
      throws Exception {
    for (int i = 0; i < expectedRecordsMap.size(); i++) {
      Map<String, Object> expectedRecord = expectedRecordsMap.get(i);
      GenericRow actualRecord = recordReader.next();
      for (FieldSpec fieldSpec : _pinotSchema.getAllFieldSpecs()) {
        String fieldSpecName = fieldSpec.getName();
        if (fieldSpec.isSingleValueField()) {
          Assert.assertEquals(actualRecord.getValue(fieldSpecName).toString(),
              expectedRecord.get(fieldSpecName).toString());
        } else {
          List expectedRecords = (List) expectedRecord.get(fieldSpecName);
          if (expectedRecords.size() == 1) {
            Assert.assertEquals(actualRecord.getValue(fieldSpecName).toString(), expectedRecords.get(0).toString());
          } else {
            Object[] actualRecords = (Object[]) actualRecord.getValue(fieldSpecName);
            Assert.assertEquals(actualRecords.length, expectedRecords.size());
            for (int j = 0; j < actualRecords.length; j++) {
              Assert.assertEquals(actualRecords[j].toString(), expectedRecords.get(j).toString());
            }
          }
        }
        PrimaryKey primaryKey = actualRecord.getPrimaryKey(getPrimaryKeyColumns());
        for (int j = 0; j < primaryKey.getValues().length; j++) {
          Assert.assertEquals(primaryKey.getValues()[j].toString(), expectedPrimaryKeys.get(i)[j].toString());
        }
      }
    }
    Assert.assertFalse(recordReader.hasNext());
  }

  @Test
  public void testInvalidDelimiterInHeader() {
    //setup
    CSVRecordReaderConfig csvRecordReaderConfig = new CSVRecordReaderConfig();
    csvRecordReaderConfig.setMultiValueDelimiter(CSV_MULTI_VALUE_DELIMITER);
    csvRecordReaderConfig.setHeader("col1;col2;col3;col4;col5;col6;col7;col8;col9;col10");
    csvRecordReaderConfig.setDelimiter(',');
    CSVRecordReader csvRecordReader = new CSVRecordReader();

    //execute and assert
    Assert.assertThrows(IllegalArgumentException.class,
        () -> csvRecordReader.init(_dataFile, null, csvRecordReaderConfig));
  }

  @Test
  public void testValidDelimiterInHeader()
      throws IOException {
    //setup
    CSVRecordReaderConfig csvRecordReaderConfig = new CSVRecordReaderConfig();
    csvRecordReaderConfig.setMultiValueDelimiter(CSV_MULTI_VALUE_DELIMITER);
    csvRecordReaderConfig.setHeader("col1,col2,col3,col4,col5,col6,col7,col8,col9,col10");
    csvRecordReaderConfig.setDelimiter(',');
    CSVRecordReader csvRecordReader = new CSVRecordReader();

    //read all fields
    //execute and assert
    csvRecordReader.init(_dataFile, null, csvRecordReaderConfig);
    Assert.assertEquals(10, csvRecordReader.getCSVHeaderMap().size());
    Assert.assertTrue(csvRecordReader.getCSVHeaderMap().containsKey("col1"));
    Assert.assertTrue(csvRecordReader.hasNext());
  }

  /**
   * When CSV records contain a single value, then no exception should be throw while initialising.
   * This test requires a different setup from the rest of the tests as it requires a single-column
   * CSV. Therefore, we re-write already generated records into a new file, but only the first
   * column.
   *
   * @throws IOException
   */
  @Test
  public void testHeaderDelimiterSingleColumn()
      throws IOException {
    //setup

    //create a single value CSV
    Schema pinotSchema = getPinotSchema();
    //write only the first column in the schema
    String column = pinotSchema.getColumnNames().toArray(new String[0])[0];
    //use a different file name so that other tests aren't affected
    File file = new File(_tempDir, "data1.csv");
    try (FileWriter fileWriter = new FileWriter(file);
        CSVPrinter csvPrinter = new CSVPrinter(fileWriter, CSVFormat.DEFAULT.withHeader(column))) {
      for (Map<String, Object> r : _records) {
        Object[] record = new Object[1];
        record[0] = r.get(column);
        csvPrinter.printRecord(record);
      }
    }

    CSVRecordReaderConfig csvRecordReaderConfig = new CSVRecordReaderConfig();
    csvRecordReaderConfig.setMultiValueDelimiter(CSV_MULTI_VALUE_DELIMITER);
    csvRecordReaderConfig.setHeader("col1");
    CSVRecordReader csvRecordReader = new CSVRecordReader();

    //execute and assert
    csvRecordReader.init(file, null, csvRecordReaderConfig);
    Assert.assertTrue(csvRecordReader.hasNext());
  }

  @Test
  public void testNullValueString()
      throws IOException {
    //setup
    String nullString = "NULL";
    //create a single value CSV
    Schema pinotSchema = getPinotSchema();
    //write only the first column in the schema
    String column = pinotSchema.getColumnNames().toArray(new String[0])[0];
    //use a different file name so that other tests aren't affected
    File file = new File(_tempDir, "data1.csv");
    try (FileWriter fileWriter = new FileWriter(file);
        CSVPrinter csvPrinter = new CSVPrinter(fileWriter,
            CSVFormat.DEFAULT.withHeader("col1", "col2", "col3").withNullString(nullString))) {
      for (Map<String, Object> r : _records) {
        Object[] record = new Object[3];
        record[0] = r.get(column);
        csvPrinter.printRecord(record);
      }
    }

    CSVRecordReaderConfig csvRecordReaderConfig = new CSVRecordReaderConfig();
    csvRecordReaderConfig.setMultiValueDelimiter(CSV_MULTI_VALUE_DELIMITER);
    csvRecordReaderConfig.setHeader("col1,col2,col3");
    csvRecordReaderConfig.setNullStringValue(nullString);
    CSVRecordReader csvRecordReader = new CSVRecordReader();

    //execute and assert
    csvRecordReader.init(file, null, csvRecordReaderConfig);
    Assert.assertTrue(csvRecordReader.hasNext());
    csvRecordReader.next();

    GenericRow row = csvRecordReader.next();
    Assert.assertNotNull(row.getValue("col1"));
    Assert.assertNull(row.getValue("col2"));
    Assert.assertNull(row.getValue("col3"));
  }
}
