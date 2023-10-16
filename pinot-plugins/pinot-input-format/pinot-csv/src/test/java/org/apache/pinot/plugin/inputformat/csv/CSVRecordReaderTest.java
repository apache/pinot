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
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
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
    // setup
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

  @Test
  public void testReadingDataFileWithCommentedLines()
      throws IOException, URISyntaxException {
    URI uri = ClassLoader.getSystemResource("dataFileWithCommentedLines.csv").toURI();
    File dataFile = new File(uri);

    // test using line iterator
    CSVRecordReaderConfig readerConfig = new CSVRecordReaderConfig();
    readerConfig.setSkipUnParseableLines(true);
    readerConfig.setCommentMarker('#');
    List<GenericRow> genericRows = readCSVRecords(dataFile, readerConfig, null, false);
    Assert.assertEquals(3, genericRows.size());

    // test using default CSVRecordReader
    readerConfig.setSkipUnParseableLines(false);
    genericRows = readCSVRecords(dataFile, readerConfig, null, false);
    Assert.assertEquals(3, genericRows.size());
  }

  @Test
  public void testReadingDataFileWithEmptyLines()
      throws URISyntaxException, IOException {
    URI uri = ClassLoader.getSystemResource("dataFileWithEmptyLines.csv").toURI();
    File dataFile = new File(uri);

    // test using line iterator
    CSVRecordReaderConfig readerConfig = new CSVRecordReaderConfig();
    readerConfig.setSkipUnParseableLines(true);
    List<GenericRow> genericRows = readCSVRecords(dataFile, readerConfig, null, false);
    Assert.assertEquals(5, genericRows.size());

    // test using default CSVRecordReader
    readerConfig.setSkipUnParseableLines(false);
    genericRows = readCSVRecords(dataFile, readerConfig, null, false);
    Assert.assertEquals(5, genericRows.size());
  }

  @Test
  public void testReadingDataFileWithEscapedQuotes()
      throws URISyntaxException, IOException {
    URI uri = ClassLoader.getSystemResource("dataFileWithEscapedQuotes.csv").toURI();
    File dataFile = new File(uri);

    // test using line iterator
    CSVRecordReaderConfig readerConfig = new CSVRecordReaderConfig();
    readerConfig.setSkipUnParseableLines(true);
    List<GenericRow> genericRows = readCSVRecords(dataFile, readerConfig, null, false);
    Assert.assertEquals(2, genericRows.size());

    // test using default CSVRecordReader
    readerConfig.setSkipUnParseableLines(false);
    genericRows = readCSVRecords(dataFile, readerConfig, null, false);
    Assert.assertEquals(2, genericRows.size());
  }

  @Test
  public void testReadingDataFileWithNoHeader()
      throws URISyntaxException, IOException {
    URI uri = ClassLoader.getSystemResource("dataFileWithNoHeader.csv").toURI();
    File dataFile = new File(uri);

    // test using line iterator
    CSVRecordReaderConfig readerConfig = new CSVRecordReaderConfig();
    readerConfig.setSkipUnParseableLines(true);
    readerConfig.setHeader("id,name");
    List<GenericRow> genericRows = readCSVRecords(dataFile, readerConfig, null, false);
    Assert.assertEquals(3, genericRows.size());

    // test using default CSVRecordReader
    readerConfig.setSkipUnParseableLines(false);
    genericRows = readCSVRecords(dataFile, readerConfig, null, false);
    Assert.assertEquals(3, genericRows.size());
  }

  @Test
  public void testReadingDataFileWithQuotedHeaders()
      throws URISyntaxException, IOException {
    URI uri = ClassLoader.getSystemResource("dataFileWithQuotedHeaders.csv").toURI();
    File dataFile = new File(uri);

    // test using line iterator
    CSVRecordReaderConfig readerConfig = new CSVRecordReaderConfig();
    readerConfig.setSkipUnParseableLines(true);
    List<GenericRow> genericRows = readCSVRecords(dataFile, readerConfig, null, false);
    Assert.assertEquals(2, genericRows.size());

    // test using default CSVRecordReader
    readerConfig.setSkipUnParseableLines(false);
    genericRows = readCSVRecords(dataFile, readerConfig, null, false);
    Assert.assertEquals(2, genericRows.size());
  }

  @Test
  public void testLineIteratorReadingDataFileWithUnparseableLines()
      throws URISyntaxException, IOException {
    URI uri = ClassLoader.getSystemResource("dataFileWithUnparseableLines.csv").toURI();
    File dataFile = new File(uri);

    CSVRecordReaderConfig readerConfig = new CSVRecordReaderConfig();
    readerConfig.setSkipUnParseableLines(true);
    List<GenericRow> genericRows = readCSVRecords(dataFile, readerConfig, null, false);
    Assert.assertEquals(1, genericRows.size());
  }

  @Test (expectedExceptions = RuntimeException.class)
  public void testDefaultCsvReaderExceptionReadingDataFileWithUnparseableLines()
      throws URISyntaxException, IOException {
    URI uri = ClassLoader.getSystemResource("dataFileWithUnparseableLines.csv").toURI();
    File dataFile = new File(uri);

    CSVRecordReaderConfig readerConfig = new CSVRecordReaderConfig();
    readCSVRecords(dataFile, readerConfig, null, false);
  }

  @Test
  public void testLineIteratorReadingDataFileWithMultipleCombinations()
      throws URISyntaxException, IOException {
    URI uri = ClassLoader.getSystemResource("dataFileWithMultipleCombinations.csv").toURI();
    File dataFile = new File(uri);

    CSVRecordReaderConfig readerConfig = new CSVRecordReaderConfig();
    readerConfig.setSkipUnParseableLines(true);
    readerConfig.setCommentMarker('#');
    readerConfig.setIgnoreEmptyLines(true);

    List<GenericRow> genericRows = readCSVRecords(dataFile, readerConfig, null, false);
    Assert.assertEquals(7, genericRows.size());
  }

  @Test
  public void testDefaultCsvReaderReadingDataFileWithMultipleCombinations()
      throws URISyntaxException, IOException {
    URI uri = ClassLoader.getSystemResource("dataFileWithMultipleCombinationsParseable.csv").toURI();
    File dataFile = new File(uri);

    CSVRecordReaderConfig readerConfig = new CSVRecordReaderConfig();
    readerConfig.setCommentMarker('#');
    readerConfig.setIgnoreEmptyLines(true);

    List<GenericRow> genericRows = readCSVRecords(dataFile, readerConfig, new GenericRow(), false);
    Assert.assertEquals(7, genericRows.size());
  }

  @Test
  public void testLineIteratorRewindMethod()
      throws URISyntaxException, IOException {
    URI uri = ClassLoader.getSystemResource("dataFileWithMultipleCombinations.csv").toURI();
    File dataFile = new File(uri);

    CSVRecordReaderConfig readerConfig = new CSVRecordReaderConfig();
    readerConfig.setSkipUnParseableLines(true);
    readerConfig.setCommentMarker('#');
    readerConfig.setIgnoreEmptyLines(true);
    readCSVRecords(dataFile, readerConfig, null, true);

    // Start reading again; results should be same
    List<GenericRow> genericRows = readCSVRecords(dataFile, readerConfig, new GenericRow(), false);
    Assert.assertEquals(7, genericRows.size());
  }

  @Test
  public void testDefaultCsvReaderRewindMethod()
      throws URISyntaxException, IOException {
    URI uri = ClassLoader.getSystemResource("dataFileWithMultipleCombinationsParseable.csv").toURI();
    File dataFile = new File(uri);

    CSVRecordReaderConfig readerConfig = new CSVRecordReaderConfig();
    readerConfig.setCommentMarker('#');
    readerConfig.setIgnoreEmptyLines(true);
    readCSVRecords(dataFile, readerConfig, null, true);

    // Start reading again; results should be same
    List<GenericRow> genericRows = readCSVRecords(dataFile, readerConfig, null, false);
    Assert.assertEquals(7, genericRows.size());
  }

  @Test
  public void testReadingDataFileWithInvalidHeader()
      throws URISyntaxException, IOException {
    URI uri = ClassLoader.getSystemResource("dataFileWithInvalidHeader.csv").toURI();
    File dataFile = new File(uri);

    // test using line iterator
    CSVRecordReaderConfig readerConfig = new CSVRecordReaderConfig();
    readerConfig.setHeader("firstName,lastName,id");
    readerConfig.setSkipHeader(true);
    readerConfig.setSkipUnParseableLines(true);
    List<GenericRow> genericRows = readCSVRecords(dataFile, readerConfig, null, false);
    Assert.assertEquals(3, genericRows.size());

    // test using default CSVRecordReader
    readerConfig.setSkipUnParseableLines(false);
    genericRows = readCSVRecords(dataFile, readerConfig, null, false);
    Assert.assertEquals(3, genericRows.size());
  }

  @Test
  public void testReadingDataFileWithAlternateDelimiter()
      throws URISyntaxException, IOException {
    URI uri = ClassLoader.getSystemResource("dataFileWithAlternateDelimiter.csv").toURI();
    File dataFile = new File(uri);

    // test using line iterator
    CSVRecordReaderConfig readerConfig = new CSVRecordReaderConfig();
    readerConfig.setDelimiter('|');
    readerConfig.setSkipUnParseableLines(true);
    List<GenericRow> genericRows = readCSVRecords(dataFile, readerConfig, null, false);
    Assert.assertEquals(3, genericRows.size());

    // test using default CSVRecordReader
    readerConfig.setSkipUnParseableLines(false);
    genericRows = readCSVRecords(dataFile, readerConfig, null, false);
    Assert.assertEquals(3, genericRows.size());
  }

  @Test
  public void testReadingDataFileWithSpaceAroundHeaderFields()
      throws URISyntaxException, IOException {
    URI uri = ClassLoader.getSystemResource("dataFileWithSpaceAroundHeaders.csv").toURI();
    File dataFile = new File(uri);

    // test using line iterator
    CSVRecordReaderConfig readerConfig = new CSVRecordReaderConfig();
    readerConfig.setSkipUnParseableLines(true);
    readerConfig.setIgnoreSurroundingSpaces(true);
    List<GenericRow> genericRows = readCSVRecords(dataFile, readerConfig, null, false);
    Assert.assertEquals(3, genericRows.size());
    validateSpaceAroundHeadersAreTrimmed(dataFile, readerConfig);

    // test using default CSVRecordReader
    readerConfig.setSkipUnParseableLines(false);
    genericRows = readCSVRecords(dataFile, readerConfig, null, false);
    Assert.assertEquals(3, genericRows.size());
    validateSpaceAroundHeadersAreTrimmed(dataFile, readerConfig);
  }

  @Test
  public void testReadingDataFileWithSpaceAroundHeaderAreRetained()
      throws URISyntaxException, IOException {
    URI uri = ClassLoader.getSystemResource("dataFileWithSpaceAroundHeaders.csv").toURI();
    File dataFile = new File(uri);

    // test using line iterator
    CSVRecordReaderConfig readerConfig = new CSVRecordReaderConfig();
    readerConfig.setSkipUnParseableLines(true);
    readerConfig.setIgnoreSurroundingSpaces(false);
    List<GenericRow> genericRows = readCSVRecords(dataFile, readerConfig, null, false);
    Assert.assertEquals(3, genericRows.size());
    validateSpaceAroundHeadersAreRetained(dataFile, readerConfig);

    // test using default CSVRecordReader
    readerConfig.setSkipUnParseableLines(false);
    genericRows = readCSVRecords(dataFile, readerConfig, null, false);
    Assert.assertEquals(3, genericRows.size());
    validateSpaceAroundHeadersAreRetained(dataFile, readerConfig);
  }

  @Test
  public void testRewindMethodAndSkipHeader()
      throws URISyntaxException, IOException {
    URI uri = ClassLoader.getSystemResource("dataFileWithInvalidHeader.csv").toURI();
    File dataFile = new File(uri);

    CSVRecordReaderConfig readerConfig = new CSVRecordReaderConfig();
    readerConfig.setSkipUnParseableLines(true);
    readerConfig.setHeader("id,name");
    readerConfig.setSkipHeader(true);
    readCSVRecords(dataFile, readerConfig, new GenericRow(), true);

    // Start reading again; results should be same
    List<GenericRow> genericRows = readCSVRecords(dataFile, readerConfig, null, false);
    Assert.assertEquals(3, genericRows.size());

    // test using default CSVRecordReader
    readerConfig.setSkipUnParseableLines(false);
    readCSVRecords(dataFile, readerConfig, null, true);

    // Start reading again; results should be same
    genericRows = readCSVRecords(dataFile, readerConfig, null, false);
    Assert.assertEquals(3, genericRows.size());
  }

  @Test
  public void testReadingDataFileWithPartialLastRow()
      throws URISyntaxException, IOException {
    URI uri = ClassLoader.getSystemResource("dataFileWithPartialLastRow.csv").toURI();
    File dataFile = new File(uri);

    // test using line iterator
    CSVRecordReaderConfig readerConfig = new CSVRecordReaderConfig();
    readerConfig.setSkipUnParseableLines(true);
    List<GenericRow> genericRows = readCSVRecords(dataFile, readerConfig, null, false);
    Assert.assertEquals(2, genericRows.size());

    // Note: The default CSVRecordReader cannot handle unparseable rows
  }

  @Test
  public void testReadingDataFileWithNoRecords()
      throws URISyntaxException, IOException {
    URI uri = ClassLoader.getSystemResource("dataFileWithNoRecords.csv").toURI();
    File dataFile = new File(uri);

    // test using line iterator
    CSVRecordReaderConfig readerConfig = new CSVRecordReaderConfig();
    readerConfig.setSkipUnParseableLines(true);
    List<GenericRow> genericRows = readCSVRecords(dataFile, readerConfig, null, false);
    Assert.assertEquals(0, genericRows.size());

    // Note: The default CSVRecordReader cannot handle unparseable rows
  }

  @Test
  public void testReadingDataFileWithNoHeaderAndDataRecordsWithEmptyValues()
      throws URISyntaxException, IOException {
    URI uri = ClassLoader.getSystemResource("dataFileWithNoHeader2.csv").toURI();
    File dataFile = new File(uri);

    // test using line iterator
    CSVRecordReaderConfig readerConfig = new CSVRecordReaderConfig();
    readerConfig.setSkipUnParseableLines(true);
    readerConfig.setHeader("key,num0,num1");
    List<GenericRow> genericRows = readCSVRecords(dataFile, readerConfig, null, false);
    Assert.assertEquals(4, genericRows.size());

    // test using default CSVRecordReader
    readerConfig.setSkipUnParseableLines(false);
    genericRows = readCSVRecords(dataFile, readerConfig, null, false);
    Assert.assertEquals(4, genericRows.size());
  }

  @Test
  public void testReadingDataFileWithValidHeaders()
      throws URISyntaxException, IOException {
    URI uri = ClassLoader.getSystemResource("dataFileWithValidHeaders.csv").toURI();
    File dataFile = new File(uri);

    // test using line iterator
    CSVRecordReaderConfig readerConfig = new CSVRecordReaderConfig();
    readerConfig.setSkipUnParseableLines(true);
    // No explicit header is set and attempt to skip the header should be ignored. 1st line would be treated as the
    // header line.
    readerConfig.setSkipHeader(false);
    List<GenericRow> genericRows = readCSVRecords(dataFile, readerConfig, null, false);
    Assert.assertEquals(4, genericRows.size());

    // test using default CSVRecordReader
    readerConfig.setSkipUnParseableLines(false);
    genericRows = readCSVRecords(dataFile, readerConfig, null, false);
    Assert.assertEquals(4, genericRows.size());
  }

  private List<GenericRow> readCSVRecords(File dataFile,
      CSVRecordReaderConfig readerConfig, GenericRow genericRow, boolean rewind)
      throws IOException {
    List<GenericRow> genericRows = new ArrayList<>();

    try (CSVRecordReader recordReader = new CSVRecordReader()) {
      recordReader.init(dataFile, null, readerConfig);
      GenericRow reuse = new GenericRow();
      while (recordReader.hasNext()) {
        if (genericRow != null) {
          recordReader.next(reuse);
          genericRows.add(reuse);
        } else {
          GenericRow nextRow = recordReader.next();
          genericRows.add(nextRow);
        }
      }

      if (rewind) {
        // rewind the reader after reading all the lines
        recordReader.rewind();
      }
    }
    return genericRows;
  }

  private void validateSpaceAroundHeadersAreTrimmed(File dataFile, CSVRecordReaderConfig readerConfig)
      throws IOException {
    try (CSVRecordReader recordReader = new CSVRecordReader()) {
      recordReader.init(dataFile, null, readerConfig);
      Map<String, Integer> headerMap = recordReader.getCSVHeaderMap();
      Assert.assertEquals(3, headerMap.size());
      List<String> headers = List.of("firstName", "lastName", "id");
      for (String header : headers) {
        // surrounding spaces in headers are trimmed
        Assert.assertTrue(headerMap.containsKey(header));
      }
    }
  }

  private void validateSpaceAroundHeadersAreRetained(File dataFile, CSVRecordReaderConfig readerConfig)
      throws IOException {
    try (CSVRecordReader recordReader = new CSVRecordReader()) {
      recordReader.init(dataFile, null, readerConfig);
      Map<String, Integer> headerMap = recordReader.getCSVHeaderMap();
      Assert.assertEquals(3, headerMap.size());
      List<String> headers = List.of(" firstName ", " lastName ", " id");
      for (String header : headers) {
        // surrounding spaces in headers are trimmed
        Assert.assertTrue(headerMap.containsKey(header));
      }
    }
  }
}
